// Events and clicks: parse into hash partitions, then dedup events per partition.
#include <algorithm>
#include <atomic>

#include "csv.hpp"
#include "pipeline.hpp"

namespace {
// 32 B. event_id is identified by 96 bits of hash, so dedup never reads the CSV bytes back.
struct EvRec {
    uint64_t h;      // hash_bytes(event_id)
    uint32_t h2;     // hash2(event_id)
    uint32_t ingest; // seconds since epoch
    uint32_t ts;     // ms since window_start
    uint32_t pid;    // person, resolved at parse
    uint32_t pc;     // price | cat << 24
    uint32_t flags;  // 1 valid
};
static_assert(sizeof(EvRec) == 32, "EvRec");
}  // namespace

void parse_and_dedup(const Ctx& c, Persons& P, EvParts& sp, CkParts& ckp) {
    const int T = c.T;
    const int64_t ws = c.ws, we = c.we;
    // events by event_id hash, clicks by person hash
    // Arenas are virtual reservations sized for the shortest possible lines; only touched pages count.
    const size_t slack = (size_t)T * NPART * (1 << 16);
    Arena eva;
    eva.init(c.ev_bytes / 16 * sizeof(EvRec) + slack);
    ckp.arena.init(c.ck_bytes / 12 * sizeof(ClickRec) + slack);
    vector<vector<BList<EvRec>>> evp(T, vector<BList<EvRec>>(NPART));
    ckp.p.assign(T, vector<BList<ClickRec>>(NPART));
    {
        std::atomic<size_t> next{0};
        parallel(T, [&](int t) {
            auto& ev = evp[t];
            auto& ck = ckp.p[t];
            for (size_t k; (k = next++) < c.main_chunks.size();) {
                const Chunk& ch = c.main_chunks[k];
                if (ch.kind == 0) {
                    scan_lines<7>(ch.b, ch.e, [&](const char* const* fp, const uint32_t* fl) {
                        if (!fl[0]) return;
                        EvRec r;
                        r.h = hash_bytes(fp[0], fl[0]);
                        r.h2 = hash2(fp[0], fl[0]);
                        int64_t ingest, ts = 0, price = 0;
                        if (!parse_ts(fp[3], fl[3], ingest)) ingest = INT64_MAX;
                        r.ingest = ingest < 0 ? 0 : (uint32_t)std::min<int64_t>(ingest / 1000, UINT32_MAX);
                        r.flags = 0;
                        r.pc = 0;
                        r.pid = 0;
                        r.ts = 0;
                        bool ok = fl[1] && fl[5] && parse_ts(fp[2], fl[2], ts) && ts >= ws && ts < we;
                        if (ok) {
                            const char* ty = fp[4];
                            uint32_t tl = fl[4];
                            if (tl == 8 && memcmp(ty, "purchase", 8) == 0) {
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
                                    price = v;
                                }
                            } else if (!(tl == 4 && (memcmp(ty, "view", 4) == 0 || memcmp(ty, "cart", 4) == 0))) {
                                ok = false;
                            }
                        }
                        if (ok && ingest - ts > DAY_MS) ok = false;
                        if (ok) {
                            int64_t pi = c.prodmap.find(hash_bytes(fp[5], fl[5]), fp[5], fl[5]);
                            uint32_t cat = pi < 0 ? c.unknown_cat : c.prodcat[pi];
                            r.pc = (uint32_t)price | cat << 24;
                            r.ts = (uint32_t)(ts - ws);
                            r.pid = P.get(fp[1], fl[1]);
                            r.flags = 1;
                        }
                        ev[r.h >> (64 - PBITS)].push(eva, r);
                    });
                } else {
                    scan_lines<4>(ch.b, ch.e, [&](const char* const* fp, const uint32_t* fl) {
                        if (!fl[0] || !fl[1]) return;
                        int64_t camp = c.campmap.find(hash_bytes(fp[2], fl[2]), fp[2], fl[2]);
                        if (camp < 0) return;
                        int64_t ts;
                        if (!parse_ts(fp[3], fl[3], ts) || ts < ws - WEEK_MS || ts >= we) return;
                        ClickRec r;
                        r.pid = P.get(fp[1], fl[1]);
                        r.ts = (uint32_t)(ts - (ws - WEEK_MS));
                        r.cid = fp[0];
                        r.cidlen = fl[0];
                        r.camp = (uint32_t)camp;
                        ck[ppart(r.pid)].push(ckp.arena, r);
                    });
                }
            }
        });
    }
    phase("parse events+clicks");

    // dedup events per partition, emit survivors by person partition
    sp.arena.init(c.ev_bytes / 16 * sizeof(SessEv) + slack);
    sp.p.assign(T, vector<BList<SessEv>>(NPART));
    vector<int> order(NPART);
    vector<size_t> psize(NPART, 0);
    for (int q = 0; q < NPART; q++) {
        order[q] = q;
        for (int t = 0; t < T; t++) psize[q] += evp[t][q].size();
    }
    std::sort(order.begin(), order.end(), [&](int a, int b) { return psize[a] > psize[b]; });
    std::atomic<int> next{0};
    parallel(T, [&](int t) {
        auto& out = sp.p[t];
        vector<EvRec*> table;
        for (int k; (k = next++) < NPART;) {
            int q = order[k];
            size_t cap = 16;
            while (cap < psize[q] * 2) cap <<= 1;
            table.assign(cap, nullptr);
            size_t mask = cap - 1;
            for (int s = 0; s < T; s++)
                evp[s][q].each([&](EvRec& r) {
                    size_t i = (r.h * 0x9E3779B97F4A7C15ull >> 20) & mask;
                    while (true) {
                        EvRec* x = table[i];
                        if (!x) {
                            table[i] = &r;
                            break;
                        }
                        if (x->h == r.h && x->h2 == r.h2) {
                            if (r.ingest < x->ingest) table[i] = &r;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                });
            for (EvRec* x : table) {
                if (!x || !(x->flags & 1)) continue;
                out[ppart(x->pid)].push(sp.arena, {x->ts, x->pid, x->pc});
            }
        }
    });
    phase("dedup");
}
