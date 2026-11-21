// Sessions, click credit and aggregation, one person partition at a time.
#include <algorithm>
#include <atomic>

#include "pipeline.hpp"

// Sort one person's records: insertion sort for the usual handful, std::sort for heavy users.
template <class R, class Less>
static inline void sort_small(R* a, size_t n, Less less) {
    if (n > 32) {
        std::sort(a, a + n, less);
        return;
    }
    for (size_t i = 1; i < n; i++) {
        R v = a[i];
        size_t k = i;
        for (; k && less(v, a[k - 1]); k--) a[k] = a[k - 1];
        a[k] = v;
    }
}

// Person key -> dense local id within one partition; sized to stay in L2, doubles when half full.
struct Intern {
    vector<uint64_t> k;  // 0 = empty (no key is 0)
    vector<uint32_t> v;
    vector<uint64_t> keys;  // local id -> key
    size_t mask = 0;
    int shift = 64;
    void reset(size_t cap) {
        int b = 0;
        while (((size_t)1 << b) < cap) b++;
        k.assign((size_t)1 << b, 0);
        v.resize((size_t)1 << b);
        mask = ((size_t)1 << b) - 1;
        shift = 64 - b;
        keys.clear();
    }
    uint32_t get(uint64_t key) {
        for (size_t i = (size_t)((key * 0xC2B2AE3D27D4EB4Full) >> shift);; i = (i + 1) & mask) {
            const uint64_t x = k[i];
            if (x == key) return v[i];
            if (!x) {
                if ((keys.size() + 1) * 2 > mask + 1) {
                    grow();
                    return get(key);
                }
                k[i] = key;
                v[i] = (uint32_t)keys.size();
                keys.push_back(key);
                return v[i];
            }
        }
    }
    void grow() {
        vector<uint64_t> old;
        old.swap(keys);
        reset((mask + 1) * 2);
        for (uint64_t key : old) get(key);
    }
};

struct EvTs {
    uint32_t ts, pc;
};

void sessions(const Ctx& c, EvParts& sp, CkParts& ckp, Agg& out) {
    const int T = c.T;
    const int NC = c.NC;
    const uint32_t NCAMP = c.NCAMP;
    const int ndays = c.ndays;
    const int64_t day0 = c.day0, ws = c.ws;
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
        for (int t = 0; t < T; t++) psize[q] += sp.p[t][q].size();
    }
    std::sort(order.begin(), order.end(), [&](int a, int b) { return psize[a] > psize[b]; });
    std::atomic<int> next{0};
    parallel(T, [&](int t) {
        Intern I;
        I.reset(1 << 12);
        vector<EvTs> evs;
        vector<ClickRec> cks;
        vector<uint32_t> eoff, coff, le, lc;
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
            // local id per record (intern), then counting sort by it straight from the blocks
            I.reset(I.mask + 1);
            le.clear();
            lc.clear();
            for (int s = 0; s < T; s++) {
                sp.p[s][q].each_block([&](const SessEv* p, uint32_t n) {
                    for (uint32_t x = 0; x < n; x++) le.push_back(I.get(p[x].key));
                });
                ckp.p[s][q].each_block([&](const ClickRec* p, uint32_t n) {
                    for (uint32_t x = 0; x < n; x++) lc.push_back(I.get(p[x].key));
                });
            }
            const uint32_t nq = (uint32_t)I.keys.size();
            const size_t ne = le.size(), nck = lc.size();
            eoff.assign(nq + 1, 0);
            coff.assign(nq + 1, 0);
            for (uint32_t l : le) eoff[l + 1]++;
            for (uint32_t l : lc) coff[l + 1]++;
            for (uint32_t l = 0; l < nq; l++) eoff[l + 1] += eoff[l], coff[l + 1] += coff[l];
            evs.resize(ne);
            cks.resize(nck);
            size_t xe = 0, xc = 0;
            for (int s = 0; s < T; s++) {
                sp.p[s][q].each_block([&](const SessEv* p, uint32_t n) {
                    for (uint32_t x = 0; x < n; x++) evs[eoff[le[xe++]]++] = {p[x].ts, p[x].pc};
                });
                ckp.p[s][q].each_block([&](const ClickRec* p, uint32_t n) {
                    for (uint32_t x = 0; x < n; x++) cks[coff[lc[xc++]]++] = p[x];
                });
            }
            // after the scatter, eoff[l] / coff[l] is the end of person l
            size_t i = 0, cb = 0;
            for (uint32_t l = 0; l < nq; l++) {
                const size_t j = eoff[l], ce = coff[l];
                if (i == j) {
                    cb = ce;
                    continue;
                }
                sort_small(&evs[i], j - i, [](const EvTs& a, const EvTs& b) { return a.ts < b.ts; });
                sort_small(&cks[cb], ce - cb, [](const ClickRec& a, const ClickRec& b) {
                    if (a.ts != b.ts) return a.ts < b.ts;
                    return bcmp3(a.cid, a.cidlen, b.cid, b.cidlen) < 0;
                });
                const uint64_t key = I.keys[l];
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
                    rows.push_back({key, (int32_t)cur_day, us, ue, ur, top});
                };
                size_t a = i;
                while (a < j) {
                    const int64_t start = evs[a].ts;          // ms since window_start
                    const int64_t start_c = start + WEEK_MS;  // the same instant on the click scale
                    size_t b = a + 1;
                    while (b < j && evs[b].ts - evs[b - 1].ts <= GAP_MS) b++;
                    int64_t day = (ws + start) / DAY_MS;
                    if (day != cur_day) {
                        flush();
                        cur_day = day, us = ue = ur = 0, anycat = false;
                    }
                    int64_t rev = 0;
                    bool purchased = false;
                    for (size_t x = a; x < b; x++)
                        if (uint32_t price = evs[x].pc & PRICE_MASK) {
                            purchased = true;
                            rev += price;
                            catrev[evs[x].pc >> 24] += price;
                            anycat = true;
                        }
                    us++, ue += (int64_t)(b - a), ur += rev;
                    // clicks: window [start - 7d, start]
                    while (hi < ce && cks[hi].ts <= start_c) {
                        if (ccount[cks[hi].camp]++ == 0) active.push_back(cks[hi].camp);
                        hi++;
                    }
                    while (lo < hi && cks[lo].ts < start) {
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
                cb = ce;
            }
        }
    });
}
