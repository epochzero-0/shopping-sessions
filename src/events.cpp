// Events and clicks: parse into hash partitions, then dedup events per partition.
#include <algorithm>
#include <atomic>

#include "csv.hpp"
#include "pipeline.hpp"

namespace {
struct EvRec {
    uint64_t h;  // hash of event_id
    int64_t ingest;
    int64_t ts;
    uint64_t ph;  // person hash, resolved at parse so no later phase reads the CSV bytes back
    const char* eid;
    const char* name;
    uint32_t eidlen, namelen;
    int32_t price;
    uint8_t cat;
    uint8_t flags;  // 1 valid, 2 purchase
};
}  // namespace

void parse_and_dedup(const Ctx& c, const Links& L, EvParts& sp, CkParts& ckp) {
    const int T = c.T;
    const int64_t ws = c.ws, we = c.we;
    // events by event_id hash, clicks by person hash
    vector<vector<vector<EvRec>>> evp(T, vector<vector<EvRec>>(NPART));
    ckp.assign(T, vector<vector<ClickRec>>(NPART));
    {
        size_t est_ev = c.ev_bytes / 60 / T / NPART + 16, est_ck = c.ck_bytes / 40 / T / NPART + 16;
        std::atomic<size_t> next{0};
        parallel(T, [&](int t) {
            for (auto& v : evp[t]) v.reserve(est_ev);
            for (auto& v : ckp[t]) v.reserve(est_ck);
            auto& ev = evp[t];
            auto& ck = ckp[t];
            for (size_t k; (k = next++) < c.main_chunks.size();) {
                const Chunk& ch = c.main_chunks[k];
                if (ch.kind == 0) {
                    for_lines(ch.b, ch.e, [&](const char* b, const char* e) {
                        const char* fp[7];
                        uint32_t fl[7];
                        if (!split<7>(b, e, fp, fl) || !fl[0]) return;
                        EvRec r;
                        r.h = hash_bytes(fp[0], fl[0]);
                        r.eid = fp[0];
                        r.eidlen = fl[0];
                        if (!parse_ts(fp[3], fl[3], r.ingest)) r.ingest = INT64_MAX;
                        r.flags = 0;
                        r.name = nullptr;
                        r.namelen = 0;
                        r.price = 0;
                        r.cat = 0;
                        r.ph = 0;
                        r.ts = 0;
                        bool ok = fl[1] && fl[5] && parse_ts(fp[2], fl[2], r.ts) && r.ts >= ws && r.ts < we;
                        if (ok) {
                            const char* ty = fp[4];
                            uint32_t tl = fl[4];
                            if (tl == 8 && memcmp(ty, "purchase", 8) == 0) {
                                r.flags |= 2;
                                const char* pp = fp[6];
                                uint32_t pl = fl[6];
                                uint32_t i = 0;
                                for (uint32_t q = 0; q < pl; q++)
                                    if ((unsigned char)pp[q] - '0' > 9u) ok = false;
                                if (!pl) ok = false;
                                while (i < pl && pp[i] == '0') i++;
                                if (pl - i > 8) ok = false;
                                if (ok) {
                                    int64_t v = 0;
                                    for (; i < pl; i++) v = v * 10 + (pp[i] - '0');
                                    if (v < 1 || v > 10000000) ok = false;
                                    r.price = (int32_t)v;
                                }
                            } else if (!(tl == 4 && (memcmp(ty, "view", 4) == 0 || memcmp(ty, "cart", 4) == 0))) {
                                ok = false;
                            }
                        }
                        if (ok && r.ingest - r.ts > DAY_MS) ok = false;
                        if (ok) {
                            int64_t pi = c.prodmap.find(hash_bytes(fp[5], fl[5]), fp[5], fl[5]);
                            r.cat = pi < 0 ? c.unknown_cat : c.prodcat[pi];
                            L.person_of(hash_bytes(fp[1], fl[1]), fp[1], fl[1], r.ph, r.name, r.namelen);
                            r.flags |= 1;
                        }
                        ev[r.h >> (64 - PBITS)].push_back(r);
                    });
                } else {
                    for_lines(ch.b, ch.e, [&](const char* b, const char* e) {
                        const char* fp[4];
                        uint32_t fl[4];
                        if (!split<4>(b, e, fp, fl) || !fl[0] || !fl[1]) return;
                        int64_t camp = c.campmap.find(hash_bytes(fp[2], fl[2]), fp[2], fl[2]);
                        if (camp < 0) return;
                        int64_t ts;
                        if (!parse_ts(fp[3], fl[3], ts) || ts < ws - WEEK_MS || ts >= we) return;
                        ClickRec r;
                        const char* nm;
                        uint32_t nl;
                        L.person_of(hash_bytes(fp[1], fl[1]), fp[1], fl[1], r.ph, nm, nl);
                        r.ts = ts;
                        r.cid = fp[0];
                        r.cidlen = fl[0];
                        r.camp = (uint32_t)camp;
                        ck[r.ph >> (64 - PBITS)].push_back(r);
                    });
                }
            }
        });
    }
    phase("parse events+clicks");

    // dedup events per partition, emit survivors by person partition
    sp.assign(T, vector<vector<SessEv>>(NPART));
    vector<int> order(NPART);
    vector<size_t> psize(NPART, 0);
    for (int q = 0; q < NPART; q++) {
        order[q] = q;
        for (int t = 0; t < T; t++) psize[q] += evp[t][q].size();
    }
    std::sort(order.begin(), order.end(), [&](int a, int b) { return psize[a] > psize[b]; });
    size_t total = 0;
    for (auto s : psize) total += s;
    std::atomic<int> next{0};
    parallel(T, [&](int t) {
        for (auto& v : sp[t]) v.reserve(total / T / NPART + 16);
        vector<EvRec*> table;
        for (int k; (k = next++) < NPART;) {
            int q = order[k];
            size_t cap = 16;
            while (cap < psize[q] * 2) cap <<= 1;
            table.assign(cap, nullptr);
            size_t mask = cap - 1;
            for (int s = 0; s < T; s++)
                for (auto& r : evp[s][q]) {
                    size_t i = (r.h * 0x9E3779B97F4A7C15ull >> 20) & mask;
                    while (true) {
                        EvRec* x = table[i];
                        if (!x) {
                            table[i] = &r;
                            break;
                        }
                        if (x->h == r.h && same(x->eid, x->eidlen, r.eid, r.eidlen)) {
                            if (r.ingest < x->ingest) table[i] = &r;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            for (EvRec* x : table) {
                if (!x || !(x->flags & 1)) continue;
                SessEv e;
                e.ph = x->ph;
                e.name = x->name;
                e.namelen = x->namelen;
                e.ts = x->ts;
                e.price = x->price;
                e.cat = x->cat;
                e.purchase = (x->flags & 2) ? 1 : 0;
                sp[t][e.ph >> (64 - PBITS)].push_back(e);
            }
            for (int s = 0; s < T; s++) vector<EvRec>().swap(evp[s][q]);
        }
    });
    phase("dedup");
}
