// Events and clicks: parse into hash partitions, then dedup events per partition.
#include <algorithm>
#include <atomic>

#include "csv.hpp"
#include "pipeline.hpp"

namespace {
// 32 B. event_id: exact pack_id key, or for other IDs 95 bits of hash; dedup never reads the CSV bytes back.
struct EvRec {
    uint64_t ek;      // pack_id(event_id), or FBIT | hash_bytes(event_id) >> 1
    uint32_t eh2;     // 0, or hash2(event_id) with a hashed ek
    uint32_t ingest;  // seconds since epoch
    uint32_t ts;      // ms since window_start; INVALID = dropped by validation
    uint32_t pc;      // price | cat << 24
    uint64_t uk;      // user key (FTab::key); the person is resolved after dedup
};
static_assert(sizeof(EvRec) == 32, "EvRec");
static const uint32_t INVALID = UINT32_MAX;  // the window is < 2^32 ms - 7 days
}  // namespace

void parse_and_dedup(const Ctx& c, Links& L, EvParts& sp, CkParts& ckp) {
    const int T = c.T;
    const int64_t ws = c.ws, we = c.we;
    DirectIdx dprod, dcamp;  // category / campaign index by ID number, when every ID has the p123 / c_123 form
    dprod.build(c.prodmap, "p", [&](uint32_t i) { return (uint32_t)c.prodcat[i]; });
    dcamp.build(c.campmap, "c_", [](uint32_t i) { return i; });
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
            WCLists<EvRec> ev(evp[t], eva);
            WCLists<ClickRec> ck(ckp.p[t], ckp.arena);
            // clicks wait RING lines between the link-slot prefetch and the probe; key holds the user key meanwhile
            const uint32_t RING = 8;
            ClickRec ring[RING];
            uint32_t rn = 0;
            auto click_out = [&](ClickRec& r) {
                r.key = L.person(r.key);
                ck.push(kpart(r.key), r);
            };
            for (size_t k; (k = next++) < c.main_chunks.size();) {
                const Chunk& ch = c.main_chunks[k];
                if (ch.kind == 0) {
                    scan_lines<7>(ch.b, ch.e, [&](const char* const* fp, const uint32_t* fl) {
                        if (!fl[0]) return;
                        EvRec r;
                        r.ek = pack_id(fp[0], fl[0]);
                        r.eh2 = 0;
                        if (!r.ek) {
                            r.ek = FBIT | hash_bytes(fp[0], fl[0]) >> 1;
                            r.eh2 = hash2(fp[0], fl[0]);
                        }
                        int64_t ingest, ts = 0, price = 0;
                        if (!parse_ts(fp[3], fl[3], ingest)) ingest = INT64_MAX;
                        r.ingest = ingest < 0 ? 0 : (uint32_t)std::min<int64_t>(ingest / 1000, UINT32_MAX);
                        r.pc = 0;
                        r.uk = 0;
                        r.ts = INVALID;
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
                            int64_t cat;
                            if (dprod.on) {
                                cat = dprod.find(fp[5], fl[5]);
                            } else {
                                cat = c.prodmap.find(hash_bytes(fp[5], fl[5]), fp[5], fl[5]);
                                if (cat >= 0) cat = c.prodcat[cat];
                            }
                            if (cat < 0) cat = c.unknown_cat;
                            r.pc = (uint32_t)price | (uint32_t)cat << 24;
                            r.ts = (uint32_t)(ts - ws);
                            r.uk = L.F.key(fp[1], fl[1]);
                        }
                        ev.push(kpart(r.ek), r);
                    });
                } else {
                    scan_lines<4>(ch.b, ch.e, [&](const char* const* fp, const uint32_t* fl) {
                        if (!fl[0] || !fl[1]) return;
                        const int64_t camp = dcamp.on ? dcamp.find(fp[2], fl[2])
                                                      : c.campmap.find(hash_bytes(fp[2], fl[2]), fp[2], fl[2]);
                        if (camp < 0) return;
                        int64_t ts;
                        if (!parse_ts(fp[3], fl[3], ts) || ts < ws - WEEK_MS || ts >= we) return;
                        ClickRec& r = ring[rn % RING];
                        if (rn >= RING) click_out(r);
                        rn++;
                        r.key = L.F.key(fp[1], fl[1]);
                        __builtin_prefetch(L.at(r.key));
                        r.ts = (uint32_t)(ts - (ws - WEEK_MS));
                        r.cid = fp[0];
                        r.cidlen = fl[0];
                        r.pad = 0;
                        r.camp = (uint32_t)camp;
                    });
                }
            }
            for (uint32_t i = rn > RING ? rn - RING : 0; i < rn; i++) click_out(ring[i % RING]);
            ev.flush();
            ck.flush();
        });
    }
    phase("parse events+clicks");

    // dedup events per partition; survivors resolve their person through the link table and go out by person partition
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
            int bits = 4;
            while (cap < psize[q] * 2) cap <<= 1, bits++;
            table.assign(cap, nullptr);
            const size_t mask = cap - 1;
            for (int s = 0; s < T; s++)
                evp[s][q].each([&](EvRec& r) {
                    size_t i = (size_t)((r.ek * 0xD6E8FEB86659FD93ull) >> (64 - bits));
                    while (true) {
                        EvRec* x = table[i];
                        if (!x) {
                            table[i] = &r;
                            break;
                        }
                        if (x->ek == r.ek && x->eh2 == r.eh2) {  // the loser is marked dropped in place
                            if (r.ingest < x->ingest) {
                                x->ts = INVALID;
                                table[i] = &r;
                            } else {
                                r.ts = INVALID;
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                });
            // survivors in record order (sequential), link slot prefetched D records ahead
            const uint32_t D = 16;
            for (int s = 0; s < T; s++)
                evp[s][q].each_block([&](const EvRec* p, uint32_t n) {
                    for (uint32_t x = 0; x < n; x++) {
                        if (x + D < n) __builtin_prefetch(L.at(p[x + D].uk));
                        if (p[x].ts == INVALID) continue;
                        const uint64_t pk = L.person(p[x].uk);
                        out[kpart(pk)].push(sp.arena, {p[x].ts, p[x].pc, pk});
                    }
                });
        }
    });
    phase("dedup");
}
