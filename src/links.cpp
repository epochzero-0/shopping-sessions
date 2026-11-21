// Links: parse in parallel, union-find serially. A person's root is its byte-wise smallest ID.
#include <atomic>

#include "csv.hpp"
#include "pipeline.hpp"

void build_links(const Ctx& c, Links& L) {
    const int T = c.T;
    struct LinkSide {
        uint64_t h;
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
                    out.push_back({hash_bytes(fp[0], fl[0]), fp[0], fl[0]});
                    out.push_back({hash_bytes(fp[1], fl[1]), fp[1], fl[1]});
                });
            }
        });
    }
    auto& parent = L.parent;
    auto& nodep = L.nodep;
    auto& nodel = L.nodel;
    size_t nsides = 0;
    for (auto& v : lparts) nsides += v.size();
    L.map.init(nsides / 2 + 16);
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
    auto node = [&](const LinkSide& s) {
        bool ins;
        uint32_t id = L.map.insert(s.h, s.p, s.n, (uint32_t)parent.size(), ins);
        if (ins) {
            parent.push_back(id);
            nodep.push_back(s.p);
            nodel.push_back(s.n);
        }
        return id;
    };
    for (auto& v : lparts)
        for (size_t i = 0; i < v.size(); i += 2) {
            uint32_t a = find(node(v[i])), b = find(node(v[i + 1]));
            if (a == b) continue;
            // keep the byte-wise smallest ID as root
            if (bcmp3(nodep[b], nodel[b], nodep[a], nodel[a]) < 0) std::swap(a, b);
            parent[b] = a;
        }
    L.personh.resize(parent.size());
    for (uint32_t i = 0; i < parent.size(); i++) {
        uint32_t r = find(i);
        L.personh[i] = (r == i) ? hash_bytes(nodep[i], nodel[i]) : 0;
    }
    for (uint32_t i = 0; i < parent.size(); i++) L.personh[i] = L.personh[parent[i]];
}
