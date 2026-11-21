// Links: parse in parallel, union-find serially. A person's root is its byte-wise smallest ID.
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
    struct LinkSide {
        const char* p;
        uint32_t n;
    };
    vector<vector<LinkSide>> lparts(T);
    {
        std::atomic<size_t> next{0};
        parallel(T, [&](int t) {
            auto& out = lparts[t];
            for (size_t k; (k = next++) < c.link_chunks.size();) {
                for_lines(c.link_chunks[k].b, c.link_chunks[k].e, [&](const char* b, const char* e) {
                    const char* fp[2];
                    uint32_t fl[2];
                    if (!split<2>(b, e, fp, fl) || !fl[0] || !fl[1]) return;
                    out.push_back({fp[0], fl[0]});
                    out.push_back({fp[1], fl[1]});
                });
            }
        });
    }
    size_t nsides = 0;
    for (auto& v : lparts) nsides += v.size();
    P.init(nsides / 2 + est_users + 16);
    // node id == pid: link IDs are inserted first, so their pids are 0..nodes-1
    vector<uint32_t> parent;
    vector<size_t> nslot;
    parent.reserve(nsides / 2 + 16);
    nslot.reserve(nsides / 2 + 16);
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
    auto node = [&](const LinkSide& sd) {
        size_t slot;
        uint32_t id = P.get(sd.p, sd.n, &slot);
        if (id == parent.size()) {
            parent.push_back(id);
            nslot.push_back(slot);
        }
        return id;
    };
    for (auto& v : lparts)
        for (size_t i = 0; i < v.size(); i += 2) {
            uint32_t a = find(node(v[i])), b = find(node(v[i + 1]));
            if (a == b) continue;
            // keep the byte-wise smallest ID as root
            if (bcmp3(P.name_p[b], P.name_l[b], P.name_p[a], P.name_l[a]) < 0) std::swap(a, b);
            parent[b] = a;
        }
    for (uint32_t i = 0; i < parent.size(); i++) P.s[nslot[i]].id1.store(find(i) + 1, std::memory_order_relaxed);
}
