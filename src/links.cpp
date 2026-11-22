// Links: parse and intern link IDs in parallel, union-find on node ids serially, then each link ID's slot holds its
// person's key. A person's root is its byte-wise smallest ID.
#include <atomic>

#include "csv.hpp"
#include "pipeline.hpp"

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
    if (np) big_free(np, ncap * sizeof(const char*));
    if (nl) big_free(nl, ncap * sizeof(uint32_t));
}

// A pack_id key with its ten 6-bit codes reversed (first byte on top): orders as bcmp3 orders the IDs, since codes
// rise with the byte and a shorter ID pads with code 0. Its own inverse.
static inline uint64_t id_order(uint64_t k) {
    uint64_t r = 0;
    for (int i = 0; i < 10; i++) r = r << 6 | (k >> (6 * i) & 63);
    return r;
}

void build_links(const Ctx& c, Links& L) {
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
    for (size_t k = 0; k < NK; k++) lines += klines[k];
    const size_t maxnodes = 2 * lines + 16;
    L.init(maxnodes);
    // node id -> key, ID bytes, slot; written by the inserting thread. Each thread takes ids in blocks of IDBLK, so
    // threads share neither the counter nor cache lines of these arrays; ids never handed out keep nkey 0 (a hole).
    // The ID bytes (L.np, L.nl) outlive the build: they name persons keyed FBIT | node id.
    const uint32_t IDBLK = 1024;
    const size_t idcap = maxnodes + (size_t)T * IDBLK;
    uint64_t* nkey = (uint64_t*)big_alloc(idcap * sizeof(uint64_t));
    L.ncap = idcap;
    const char** np = L.np = (const char**)big_alloc(idcap * sizeof(const char*));
    uint32_t* nl = L.nl = (uint32_t*)big_alloc(idcap * sizeof(uint32_t));
    uint32_t* nslot = (uint32_t*)big_alloc(idcap * sizeof(uint32_t));
    std::atomic<uint32_t> nnodes{0};
    struct IdRange {
        uint32_t next = 0, end = 0;
    };
    auto node = [&](IdRange& ir, const char* p, uint32_t n) -> uint32_t {
        const uint64_t k = user_key(p, n);
        for (size_t i = L.idx(k);; i = (i + 1) & L.mask) {
            uint64_t cur = L.s[i].key.load(std::memory_order_relaxed);
            if (cur == 0 && L.s[i].key.compare_exchange_strong(cur, k, std::memory_order_relaxed)) {
                if (ir.next == ir.end) ir.end = (ir.next = nnodes.fetch_add(IDBLK, std::memory_order_relaxed)) + IDBLK;
                uint32_t id = ir.next++;
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
            IdRange ir;
            for (size_t k; (k = next++) < NK;) {
                scan_lines<2>(c.link_chunks[k].b, c.link_chunks[k].e, [&](const char* const* fp, const uint32_t* fl) {
                    if (!fl[0] || !fl[1]) return;
                    out.push_back(node(ir, fp[0], fl[0]));
                    out.push_back(node(ir, fp[1], fl[1]));
                });
            }
        });
    }
    const uint32_t nodes = nnodes.load();
    auto range = [&](int t, uint32_t& b, uint32_t& e) {
        b = (uint32_t)((uint64_t)nodes * t / T), e = (uint32_t)((uint64_t)nodes * (t + 1) / T);
    };
    // Parallel union-find: a root is linked under a smaller-numbered root by CAS, so links never form a cycle; path
    // halving only moves a pointer to an ancestor, so its races are harmless.
    const size_t nalloc = nodes ? nodes : 1;  // mmap rejects 0 bytes
    std::atomic<uint32_t>* par = (std::atomic<uint32_t>*)big_alloc(nalloc * sizeof(uint32_t));
    // per component: min of id_order over its packed IDs, or FBIT | node id once the serial pass picks an ID that does
    // not pack
    std::atomic<uint64_t>* best = (std::atomic<uint64_t>*)big_alloc(nalloc * sizeof(uint64_t));
    const uint64_t NONE = UINT64_MAX;
    parallel(T, [&](int t) {
        uint32_t b, e;
        range(t, b, e);
        for (uint32_t i = b; i < e; i++) {
            par[i].store(i, std::memory_order_relaxed);
            best[i].store(NONE, std::memory_order_relaxed);
        }
    });
    auto find = [&](uint32_t x) {
        while (true) {
            uint32_t p = par[x].load(std::memory_order_relaxed);
            if (p == x) return x;
            uint32_t g = par[p].load(std::memory_order_relaxed);
            if (g != p) par[x].store(g, std::memory_order_relaxed);
            x = g;
        }
    };
    parallel(T, [&](int t) {
        const auto& v = edges[t];
        for (size_t i = 0; i < v.size(); i += 2) {
            uint32_t a = v[i], b = v[i + 1];
            while (true) {
                a = find(a), b = find(b);
                if (a == b) break;
                if (a < b) std::swap(a, b);
                uint32_t x = a;
                if (par[a].compare_exchange_strong(x, b, std::memory_order_relaxed)) break;
            }
        }
    });
    // the name is the byte-wise smallest ID: packed IDs by an atomic min of id_order, then fallback IDs serially
    vector<vector<uint32_t>> fb(T);
    parallel(T, [&](int t) {
        uint32_t b, e;
        range(t, b, e);
        for (uint32_t i = b; i < e; i++) {
            if (!nkey[i]) continue;
            const uint32_t r = find(i);
            par[i].store(r, std::memory_order_relaxed);
            if (nkey[i] & FBIT) {
                fb[t].push_back(i);
                continue;
            }
            const uint64_t o = id_order(nkey[i]);
            uint64_t cur = best[r].load(std::memory_order_relaxed);
            while (o < cur && !best[r].compare_exchange_weak(cur, o, std::memory_order_relaxed)) {
            }
        }
    });
    for (auto& v : fb)
        for (uint32_t i : v) {
            const uint32_t r = par[i].load(std::memory_order_relaxed);
            const uint64_t cur = best[r].load(std::memory_order_relaxed);
            char buf[10];
            uint32_t cl = 0;
            const char* cp = nullptr;
            if (cur != NONE) {
                if (cur & FBIT) {
                    cl = nl[(uint32_t)cur], cp = np[(uint32_t)cur];
                } else {
                    cl = unpack_id(id_order(cur), buf), cp = buf;
                }
            }
            if (cur == NONE || bcmp3(np[i], nl[i], cp, cl) < 0) best[r].store(FBIT | i, std::memory_order_relaxed);
        }
    parallel(T, [&](int t) {
        uint32_t b, e;
        range(t, b, e);
        for (uint32_t i = b; i < e; i++) {
            if (!nkey[i]) continue;
            const uint64_t v = best[par[i].load(std::memory_order_relaxed)].load(std::memory_order_relaxed);
            L.s[nslot[i]].val.store(v & FBIT ? v : id_order(v), std::memory_order_relaxed);
        }
    });
    big_free(par, nalloc * sizeof(uint32_t));
    big_free(best, nalloc * sizeof(uint64_t));
    big_free(nkey, idcap * sizeof(uint64_t));
    big_free(nslot, idcap * sizeof(uint32_t));
}
