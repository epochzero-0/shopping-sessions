// Links: parse and intern link IDs in parallel, union-find on node ids serially, then each link ID's slot holds its
// person's key. A person's root is its byte-wise smallest ID.
#include <atomic>

#include "csv.hpp"
#include "pipeline.hpp"

void FTab::init(size_t n) {
    size_t c = 1024;
    while (c < n * 2) c <<= 1;
    s = (Slot*)big_alloc(c * sizeof(Slot));  // only the few fallback IDs touch it
    mask = c - 1;
    cap = (uint32_t)(c * 7 / 8);
    name_p = (const char**)big_alloc(cap * sizeof(const char*));
    name_l = (uint32_t*)big_alloc(cap * sizeof(uint32_t));
}

FTab::~FTab() {
    if (!s) return;
    big_free(s, (mask + 1) * sizeof(Slot));
    big_free(name_p, cap * sizeof(const char*));
    big_free(name_l, cap * sizeof(uint32_t));
}

void Links::init(size_t n) {
    size_t c = 1024;
    int b = 10;
    while (c < n * 2) c <<= 1, b++;
    s = (Slot*)big_alloc(c * sizeof(Slot));
    memset((void*)s, 0, c * sizeof(Slot));  // sequential first touch gets 2 MB pages
    mask = c - 1;
    shift = 64 - b;
}

Links::~Links() {
    if (s) big_free(s, (mask + 1) * sizeof(Slot));
}

void build_links(const Ctx& c, Links& L, size_t est_users) {
    const int T = c.T;
    const size_t NK = c.link_chunks.size();
    // lines bound the link count; size the tables before interning
    vector<size_t> klines(NK);
    {
        std::atomic<size_t> next{0};
        parallel(T, [&](int) {
            for (size_t k; (k = next++) < NK;) {
                const char* b = c.link_chunks[k].b;
                const char* e = c.link_chunks[k].e;
                size_t n = 0;
                while (b < e) {
                    const char* nl = (const char*)memchr(b, '\n', e - b);
                    n++;
                    if (!nl) break;
                    b = nl + 1;
                }
                klines[k] = n;
            }
        });
    }
    size_t lines = 0;
    for (size_t n : klines) lines += n;
    const size_t maxnodes = 2 * lines + 16;
    L.F.init(maxnodes + est_users);
    L.init(maxnodes);
    // node id -> key, ID bytes, slot; written by the inserting thread
    uint64_t* nkey = (uint64_t*)big_alloc(maxnodes * sizeof(uint64_t));
    const char** np = (const char**)big_alloc(maxnodes * sizeof(const char*));
    uint32_t* nl = (uint32_t*)big_alloc(maxnodes * sizeof(uint32_t));
    uint32_t* nslot = (uint32_t*)big_alloc(maxnodes * sizeof(uint32_t));
    std::atomic<uint32_t> nnodes{0};
    auto node = [&](const char* p, uint32_t n) -> uint32_t {
        const uint64_t k = L.F.key(p, n);
        for (size_t i = L.idx(k);; i = (i + 1) & L.mask) {
            uint64_t cur = L.s[i].key.load(std::memory_order_relaxed);
            if (cur == 0 && L.s[i].key.compare_exchange_strong(cur, k, std::memory_order_relaxed)) {
                uint32_t id = nnodes.fetch_add(1, std::memory_order_relaxed);
                nkey[id] = k;
                np[id] = p;
                nl[id] = n;
                nslot[id] = (uint32_t)i;
                L.s[i].val.store(id + 1, std::memory_order_release);
                return id;
            }
            if (cur == k) {
                uint64_t v;
                while (!(v = L.s[i].val.load(std::memory_order_acquire))) {
                }
                return (uint32_t)(v - 1);
            }
        }
    };
    vector<vector<uint32_t>> edges(T);  // node id pairs
    {
        std::atomic<size_t> next{0};
        parallel(T, [&](int t) {
            auto& out = edges[t];
            out.reserve(lines * 2 / T + 1024);
            for (size_t k; (k = next++) < NK;) {
                scan_lines<2>(c.link_chunks[k].b, c.link_chunks[k].e, [&](const char* const* fp, const uint32_t* fl) {
                    if (!fl[0] || !fl[1]) return;
                    out.push_back(node(fp[0], fl[0]));
                    out.push_back(node(fp[1], fl[1]));
                });
            }
        });
    }
    const uint32_t nodes = nnodes.load();
    vector<uint32_t> parent(nodes);
    for (uint32_t i = 0; i < nodes; i++) parent[i] = i;
    auto find = [&](uint32_t x) {
        uint32_t r = x;
        while (parent[r] != r) r = parent[r];
        while (parent[x] != r) {
            uint32_t nx = parent[x];
            parent[x] = r;
            x = nx;
        }
        return r;
    };
    for (auto& v : edges)
        for (size_t i = 0; i < v.size(); i += 2) {
            uint32_t a = find(v[i]), b = find(v[i + 1]);
            if (a == b) continue;
            // keep the byte-wise smallest ID as root
            if (bcmp3(np[b], nl[b], np[a], nl[a]) < 0) std::swap(a, b);
            parent[b] = a;
        }
    for (uint32_t i = 0; i < nodes; i++) parent[i] = find(i);
    parallel(T, [&](int t) {
        for (uint32_t i = (uint32_t)((uint64_t)nodes * t / T), e = (uint32_t)((uint64_t)nodes * (t + 1) / T); i < e; i++)
            L.s[nslot[i]].val.store(nkey[parent[i]], std::memory_order_relaxed);
    });
    big_free(nkey, maxnodes * sizeof(uint64_t));
    big_free(np, maxnodes * sizeof(const char*));
    big_free(nl, maxnodes * sizeof(uint32_t));
    big_free(nslot, maxnodes * sizeof(uint32_t));
}
