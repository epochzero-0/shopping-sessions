// Links: parse and intern in parallel, union-find on pids serially. A person's root is its byte-wise smallest ID.
#include <atomic>

#include "csv.hpp"
#include "pipeline.hpp"

void Persons::init(size_t n) {
    size_t c = 1024;
    while (c < n * 2) c <<= 1;
    s = (Slot*)big_alloc(c * sizeof(Slot));
    mask = c - 1;
    cap = (uint32_t)(c * 7 / 8);
    name_p = (const char**)big_alloc(cap * sizeof(const char*));
    name_l = (uint32_t*)big_alloc(cap * sizeof(uint32_t));
}

Persons::~Persons() {
    if (!s) return;
    big_free(s, (mask + 1) * sizeof(Slot));
    big_free(name_p, cap * sizeof(const char*));
    big_free(name_l, cap * sizeof(uint32_t));
}

void build_links(const Ctx& c, Persons& P, size_t est_users) {
    const int T = c.T;
    const size_t NK = c.link_chunks.size();
    // lines bound the link count; size the table before interning
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
    P.init(lines + est_users + 16);
    // Intern both sides in parallel. Only link IDs are inserted here, so their pids are 0..nodes-1.
    // nslot[pid] = the pid's slot, written by the inserting thread.
    const size_t maxnodes = 2 * lines + 16;
    size_t* nslot = (size_t*)big_alloc(maxnodes * sizeof(size_t));
    vector<vector<uint32_t>> edges(T);  // pid pairs
    {
        std::atomic<size_t> next{0};
        parallel(T, [&](int t) {
            auto& out = edges[t];
            out.reserve(lines * 2 / T + 1024);
            for (size_t k; (k = next++) < NK;) {
                scan_lines<2>(c.link_chunks[k].b, c.link_chunks[k].e, [&](const char* const* fp, const uint32_t* fl) {
                    if (!fl[0] || !fl[1]) return;
                    for (int j = 0; j < 2; j++) {
                        size_t slot;
                        uint32_t id = P.get(fp[j], fl[j], &slot);
                        // every thread finding this pid writes the same slot
                        __atomic_store_n(&nslot[id], slot, __ATOMIC_RELAXED);
                        out.push_back(id);
                    }
                });
            }
        });
    }
    const uint32_t nodes = P.next.load();
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
            if (bcmp3(P.name_p[b], P.name_l[b], P.name_p[a], P.name_l[a]) < 0) std::swap(a, b);
            parent[b] = a;
        }
    for (uint32_t i = 0; i < nodes; i++) parent[i] = find(i);
    parallel(T, [&](int t) {
        for (uint32_t i = (uint32_t)((uint64_t)nodes * t / T), e = (uint32_t)((uint64_t)nodes * (t + 1) / T); i < e; i++)
            P.s[nslot[i]].id1.store(parent[i] + 1, std::memory_order_relaxed);
    });
    big_free(nslot, maxnodes * sizeof(size_t));
}
