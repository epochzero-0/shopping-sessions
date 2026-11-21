// Sessions, click credit and aggregation, one person partition at a time.
#include <algorithm>
#include <atomic>

#include "pipeline.hpp"

void sessions(const Ctx& c, EvParts& sp, CkParts& ckp, Agg& out) {
    const int T = c.T;
    const int NC = c.NC;
    const uint32_t NCAMP = c.NCAMP;
    const int ndays = c.ndays;
    const int64_t day0 = c.day0;
    out.urows.assign(T, {});
    const size_t CD = (size_t)(NCAMP + 1) * ndays;
    out.c_sess.assign(T, vector<int64_t>(CD));
    out.c_purch.assign(T, vector<int64_t>(CD));
    out.c_rev.assign(T, vector<int64_t>(CD));
    out.c_assist.assign(T, vector<int64_t>(CD));
    vector<int> order(NPART);
    vector<size_t> psize(NPART, 0);
    for (int q = 0; q < NPART; q++) {
        order[q] = q;
        for (int t = 0; t < T; t++) psize[q] += sp[t][q].size();
    }
    std::sort(order.begin(), order.end(), [&](int a, int b) { return psize[a] > psize[b]; });
    std::atomic<int> next{0};
    parallel(T, [&](int t) {
        vector<SessEv> evs;
        vector<ClickRec> cks;
        vector<int64_t> catrev(NC, 0);
        vector<uint32_t> ccount(NCAMP, 0);
        vector<uint32_t> active;
        auto& rows = out.urows[t];
        int64_t* CS = out.c_sess[t].data();
        int64_t* CP = out.c_purch[t].data();
        int64_t* CR = out.c_rev[t].data();
        int64_t* CA = out.c_assist[t].data();
        for (int k; (k = next++) < NPART;) {
            int q = order[k];
            evs.clear();
            cks.clear();
            for (int s = 0; s < T; s++) {
                evs.insert(evs.end(), sp[s][q].begin(), sp[s][q].end());
                vector<SessEv>().swap(sp[s][q]);
                cks.insert(cks.end(), ckp[s][q].begin(), ckp[s][q].end());
                vector<ClickRec>().swap(ckp[s][q]);
            }
            std::sort(evs.begin(), evs.end(), [](const SessEv& a, const SessEv& b) {
                return a.pid != b.pid ? a.pid < b.pid : a.ts < b.ts;
            });
            std::sort(cks.begin(), cks.end(), [](const ClickRec& a, const ClickRec& b) {
                if (a.pid != b.pid) return a.pid < b.pid;
                if (a.ts != b.ts) return a.ts < b.ts;
                return bcmp3(a.cid, a.cidlen, b.cid, b.cidlen) < 0;
            });
            size_t ci = 0;
            size_t i = 0;
            while (i < evs.size()) {
                uint32_t pid = evs[i].pid;
                size_t j = i;
                while (j < evs.size() && evs[j].pid == pid) j++;
                while (ci < cks.size() && cks[ci].pid < pid) ci++;
                size_t cb = ci;
                while (ci < cks.size() && cks[ci].pid == pid) ci++;
                size_t ce = ci;
                size_t lo = cb, hi = cb;
                // per-day user accumulators
                int64_t cur_day = INT64_MIN, us = 0, ue = 0, ur = 0;
                bool anycat = false;
                auto flush = [&]() {
                    if (cur_day == INT64_MIN) return;
                    int32_t top = -1;
                    if (anycat) {
                        int64_t best = -1;
                        for (int x = 0; x < NC; x++)
                            if (catrev[x] > best && catrev[x] > 0) best = catrev[x], top = x;
                        std::fill(catrev.begin(), catrev.end(), 0);
                    }
                    rows.push_back({pid, (int32_t)cur_day, us, ue, ur, top});
                };
                size_t a = i;
                while (a < j) {
                    int64_t start = evs[a].ts;
                    size_t b = a + 1;
                    while (b < j && evs[b].ts - evs[b - 1].ts <= GAP_MS) b++;
                    int64_t day = start / DAY_MS;
                    if (day != cur_day) {
                        flush();
                        cur_day = day, us = ue = ur = 0, anycat = false;
                    }
                    int64_t rev = 0;
                    bool purchased = false;
                    for (size_t x = a; x < b; x++)
                        if (evs[x].purchase) {
                            purchased = true;
                            rev += evs[x].price;
                            catrev[evs[x].cat] += evs[x].price;
                            anycat = true;
                        }
                    us++, ue += (int64_t)(b - a), ur += rev;
                    // clicks: window [start - 7d, start]
                    while (hi < ce && cks[hi].ts <= start) {
                        if (ccount[cks[hi].camp]++ == 0) active.push_back(cks[hi].camp);
                        hi++;
                    }
                    while (lo < hi && cks[lo].ts < start - WEEK_MS) {
                        ccount[cks[lo].camp]--;
                        lo++;
                    }
                    uint32_t credit = NCAMP;
                    if (hi > lo) {
                        size_t m = hi - 1;
                        while (m > lo && cks[m - 1].ts == cks[hi - 1].ts) m--;
                        credit = cks[m].camp;
                    }
                    size_t di = (size_t)(day - day0);
                    size_t cidx = (size_t)credit * ndays + di;
                    CS[cidx]++;
                    CP[cidx] += purchased;
                    CR[cidx] += rev;
                    size_t w = 0;
                    for (size_t z = 0; z < active.size(); z++) {
                        uint32_t x = active[z];
                        if (ccount[x]) {
                            CA[(size_t)x * ndays + di]++;
                            active[w++] = x;
                        }
                    }
                    active.resize(w);
                    a = b;
                }
                flush();
                for (uint32_t x : active) ccount[x] = 0;
                active.clear();
                i = j;
            }
        }
    });
}
