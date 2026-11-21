// Parse ablation over the 20M events (N=7) and clicks (N=4).
// Usage: split_bench DATA_DIR THREADS [MODE] [RUNG]   Prints the min of 5 runs per mode.
//   MODE 0 for_lines only, 1 for_lines+split, 2 scan_lines, 3 ladder (all rungs, or only RUNG).
// Ladder, cumulative, copies of the events.cpp loop: 0 scan, 1 + hashes (eid h/h2, product, user h/h2),
// 2 + parse_ts and validation, 3 + product/campaign lookup, 4 + Persons::get, 5 + scatter into arenas (fresh
// each rep, so first-touch faults count), 6 = 5 with the person table and arenas pre-faulted outside the timer,
// 7 = 4 with only the person table pre-faulted.
// g++ -O3 -march=cascadelake -std=c++17 -pthread tests/split_bench.cpp
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "../src/csv.hpp"
#include "../src/pipeline.hpp"

void phase(const char*) {}
void* big_alloc(size_t bytes) {
    void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    madvise(p, bytes, MADV_HUGEPAGE);
    return p;
}
void big_free(void* p, size_t bytes) { munmap(p, bytes); }
void Persons::init(size_t n) {  // copy of links.cpp
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

struct EvRec {  // as events.cpp
    uint64_t h;
    uint32_t h2, ingest, ts, pid, pc, flags;
};

static const char* map_all(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    n = st.st_size;
    const char* p = (const char*)mmap(nullptr, n, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    return p;
}

static std::vector<std::string> csvs(const std::string& d) {
    std::vector<std::string> out;
    DIR* dp = opendir(d.c_str());
    for (dirent* de; (de = readdir(dp));) {
        std::string f = de->d_name;
        if (f.size() >= 4 && f.substr(f.size() - 4) == ".csv") out.push_back(d + "/" + f);
    }
    closedir(dp);
    return out;
}

static void add_dir(const std::string& d, int n, std::vector<Chunk>& out, size_t& bytes) {
    for (auto& path : csvs(d)) {
        size_t sz;
        const char* p = map_all(path, sz);
        const char* e = p + sz;
        p = (const char*)memchr(p, '\n', e - p) + 1;
        bytes += e - p;
        while (p < e) {
            const char* q = p + (4 << 20);
            q = q >= e ? e : (const char*)memchr(q, '\n', e - q) + 1;
            out.push_back({p, q, n});
            p = q;
        }
    }
}

// products / campaigns / window, as main.cpp load_small (categories by first-seen order: only lookups matter)
static void load_small(const std::string& d, Ctx& c) {
    size_t n;
    const char* m = map_all(d + "/manifest.json", n);
    std::string s(m, n);
    auto grab = [&](const char* key, int64_t& v) {
        size_t k = s.find(':', s.find(key));
        k = s.find('"', k) + 1;
        parse_ts(s.data() + k, (uint32_t)(s.find('"', k) - k), v);
    };
    grab("\"window_start\"", c.ws);
    grab("\"window_end\"", c.we);
    for (int which = 0; which < 2; which++) {
        const char* p = map_all(d + (which ? "/campaigns.csv" : "/products.csv"), n);
        std::vector<std::pair<const char*, uint32_t>> ids;
        for_lines((const char*)memchr(p, '\n', n) + 1, p + n, [&](const char* b, const char* e) {
            const char* fp[2];
            uint32_t fl[2];
            if (split<2>(b, e, fp, fl) && fl[0]) ids.push_back({fp[0], fl[0]});
        });
        StrMap& M = which ? c.campmap : c.prodmap;
        M.init(ids.size());
        for (size_t i = 0; i < ids.size(); i++) {
            bool ins;
            M.insert(hash_bytes(ids[i].first, ids[i].second), ids[i].first, ids[i].second, (uint32_t)i, ins);
            if (!which && ins) c.prodcat.push_back((uint8_t)(i % 20));
        }
    }
}

template <int N>
static uint64_t sum(const char* const* fp, const uint32_t* fl) {
    uint64_t s = 0;
    for (int i = 0; i < N; i++) s += fl[i] + (uintptr_t)fp[i];
    return s;
}

struct Lists {
    Arena eva, cka;
    std::vector<std::vector<BList<EvRec>>> ev;
    std::vector<std::vector<BList<ClickRec>>> ck;
};

// One rung over all chunks on T threads; returns seconds. Persons/arenas set up (and torn down) outside the timer.
static double ladder(const std::vector<Chunk>& ch, const Ctx& c, int T, int R, size_t ev_bytes, size_t ck_bytes,
                     size_t est_persons, uint64_t& check, uint64_t& lines) {
    Persons P;
    P.init(est_persons);
    Lists L;
    const size_t slack = (size_t)T * NPART * (1 << 16);
    L.eva.init(ev_bytes / 16 * sizeof(EvRec) + slack);
    L.cka.init(ck_bytes / 12 * sizeof(ClickRec) + slack);
    L.ev.assign(T, std::vector<BList<EvRec>>(NPART));
    L.ck.assign(T, std::vector<BList<ClickRec>>(NPART));
    if (R >= 6) {  // pre-fault the whole person table; rung 6 also ~what the arenas will touch
        auto p0 = std::chrono::steady_clock::now();
        memset((void*)P.s, 0, (P.mask + 1) * sizeof(Persons::Slot));
        memset(P.name_p, 0, P.cap * sizeof(const char*));
        memset(P.name_l, 0, P.cap * sizeof(uint32_t));
        if (R == 6) {
            memset(L.eva.base, 0, std::min(L.eva.cap, ev_bytes / 40 * sizeof(EvRec) + slack));
            memset(L.cka.base, 0, std::min(L.cka.cap, ck_bytes / 30 * sizeof(ClickRec) + slack));
        }
        fprintf(stderr, "prefault (1 thread) %.3f s, table %zu MB\n",
                std::chrono::duration<double>(std::chrono::steady_clock::now() - p0).count(),
                (P.mask + 1) * sizeof(Persons::Slot) >> 20);
        R = R == 6 ? 5 : 4;
    }
    const int64_t ws = c.ws, we = c.we;
    std::atomic<size_t> next{0};
    std::atomic<uint64_t> tot{0}, cnt{0};
    auto t0 = std::chrono::steady_clock::now();
    parallel(T, [&](int t) {
        uint64_t s = 0, n = 0;
        auto& ev = L.ev[t];
        auto& ck = L.ck[t];
        for (size_t k; (k = next++) < ch.size();) {
            const Chunk& x = ch[k];
            if (x.kind == 7) {
                scan_lines<7>(x.b, x.e, [&](const char* const* fp, const uint32_t* fl) {
                    n++;
                    if (R == 0) { s += sum<7>(fp, fl); return; }
                    if (!fl[0]) return;
                    EvRec r;
                    r.h = hash_bytes(fp[0], fl[0]);
                    r.h2 = hash2(fp[0], fl[0]);
                    const uint64_t hp = hash_bytes(fp[5], fl[5]);
                    if (R < 4) s += hash_bytes(fp[1], fl[1]) ^ hash2(fp[1], fl[1]);
                    if (R == 1) { s += r.h ^ r.h2 ^ hp; return; }
                    int64_t ingest, ts = 0, price = 0;
                    if (!parse_ts(fp[3], fl[3], ingest)) ingest = INT64_MAX;
                    r.ingest = ingest < 0 ? 0 : (uint32_t)std::min<int64_t>(ingest / 1000, UINT32_MAX);
                    r.flags = r.pc = r.pid = r.ts = 0;
                    bool ok = fl[1] && fl[5] && parse_ts(fp[2], fl[2], ts) && ts >= ws && ts < we;
                    if (ok) {
                        const char* ty = fp[4];
                        uint32_t tl = fl[4];
                        if (tl == 8 && memcmp(ty, "purchase", 8) == 0) {
                            const char* pp = fp[6];
                            uint32_t pl = fl[6], i = 0;
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
                        r.ts = (uint32_t)(ts - ws);
                        r.flags = 1;
                        if (R >= 3) {
                            int64_t pi = c.prodmap.find(hp, fp[5], fl[5]);
                            r.pc = (uint32_t)price | (uint32_t)(pi < 0 ? 255 : c.prodcat[pi]) << 24;
                        } else {
                            r.pc = (uint32_t)price ^ (uint32_t)hp;
                        }
                        if (R >= 4) r.pid = P.get(fp[1], fl[1]);
                    }
                    if (R < 5) { s += r.h ^ r.h2 ^ r.ingest ^ r.ts ^ r.pc ^ r.pid ^ r.flags; return; }
                    s += r.pid;
                    ev[r.h >> (64 - PBITS)].push(L.eva, r);
                });
            } else {
                scan_lines<4>(x.b, x.e, [&](const char* const* fp, const uint32_t* fl) {
                    n++;
                    if (R == 0) { s += sum<4>(fp, fl); return; }
                    if (!fl[0] || !fl[1]) return;
                    const uint64_t hc = hash_bytes(fp[2], fl[2]);
                    if (R < 4) s += hash_bytes(fp[1], fl[1]) ^ hash2(fp[1], fl[1]);
                    if (R == 1) { s += hc; return; }
                    int64_t ts;
                    if (!parse_ts(fp[3], fl[3], ts) || ts < ws - WEEK_MS || ts >= we) return;
                    int64_t camp = (int64_t)(hc & 7);
                    if (R >= 3 && (camp = c.campmap.find(hc, fp[2], fl[2])) < 0) return;
                    ClickRec r;
                    r.pid = R >= 4 ? P.get(fp[1], fl[1]) : 0;
                    r.ts = (uint32_t)(ts - (ws - WEEK_MS));
                    r.cid = fp[0];
                    r.cidlen = fl[0];
                    r.camp = (uint32_t)camp;
                    if (R < 5) { s += r.pid ^ r.ts ^ r.camp ^ r.cidlen; return; }
                    s += r.pid;
                    ck[ppart(r.pid)].push(L.cka, r);
                });
            }
        }
        tot += s;
        cnt += n;
    });
    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    check = tot;
    lines = cnt;
    if (R >= 4) check ^= (uint64_t)P.next.load() << 40;
    return sec;
}

int main(int argc, char** argv) {
    std::string d = argc > 1 ? argv[1] : "/data";
    int T = argc > 2 ? atoi(argv[2]) : 8;
    int only = argc > 3 ? atoi(argv[3]) : -1;  // run one mode (for perf stat)
    int rung = argc > 4 ? atoi(argv[4]) : -1;
    std::vector<Chunk> ch;
    size_t bytes = 0, evb = 0, ckb = 0;
    add_dir(d + "/events", 7, ch, evb);
    add_dir(d + "/clicks", 4, ch, ckb);
    bytes = evb + ckb;
    std::stable_sort(ch.begin(), ch.end(), [](const Chunk& a, const Chunk& b) { return (a.e - a.b) > (b.e - b.b); });
    printf("%.2f GB, %zu chunks, %d threads\n", bytes / 1e9, ch.size(), T);
    const char* names[] = {"for_lines only", "for_lines+split", "scan_lines"};
    for (int mode = 0; mode < 3; mode++) {
        if (only >= 0 && mode != only) continue;
        double best = 1e9;
        uint64_t check = 0, lines = 0;
        for (int rep = 0; rep < 5; rep++) {
            std::atomic<size_t> next{0};
            std::atomic<uint64_t> tot{0}, cnt{0};
            auto t0 = std::chrono::steady_clock::now();
            parallel(T, [&](int) {
                uint64_t s = 0, c = 0;
                for (size_t k; (k = next++) < ch.size();) {
                    const Chunk& x = ch[k];
                    if (mode == 0) {
                        for_lines(x.b, x.e, [&](const char* b, const char* e) { s += e - b; c++; });
                    } else if (mode == 1) {
                        for_lines(x.b, x.e, [&](const char* b, const char* e) {
                            const char* fp[7];
                            uint32_t fl[7];
                            if (x.kind == 7 ? split<7>(b, e, fp, fl) : split<4>(b, e, fp, fl))
                                s += x.kind == 7 ? sum<7>(fp, fl) : sum<4>(fp, fl), c++;
                        });
                    } else if (x.kind == 7) {
                        scan_lines<7>(x.b, x.e, [&](const char* const* fp, const uint32_t* fl) { s += sum<7>(fp, fl); c++; });
                    } else {
                        scan_lines<4>(x.b, x.e, [&](const char* const* fp, const uint32_t* fl) { s += sum<4>(fp, fl); c++; });
                    }
                }
                tot += s;
                cnt += c;
            });
            best = std::min(best, std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count());
            check = tot;
            lines = cnt;
        }
        printf("%-16s %.3f s  %.2f GB/s  lines %lu  check %lx\n", names[mode], best, bytes / best / 1e9,
               (unsigned long)lines, (unsigned long)check);
    }
    if (only >= 0 && only != 3) return 0;
    Ctx c;
    load_small(d, c);
    size_t link_lines = 0;
    for (auto& path : csvs(d + "/links")) {
        size_t n;
        const char* p = map_all(path, n);
        for (size_t i = 0; i < n; i++) link_lines += p[i] == '\n';
    }
    // as main.cpp + build_links: link lines + est_users
    const size_t est = link_lines + (evb / 50 + ckb / 35) / 8 + 16;
    const char* rn[] = {"scan", "+hashes", "+parse_ts", "+lookup", "+P.get", "+scatter", "L5 all prefault",
                        "L4 table prefault"};
    double prev = 0;
    for (int R = 0; R <= 7; R++) {
        if (rung >= 0 && R != rung) continue;
        double best = 1e9;
        uint64_t check = 0, lines = 0;
        for (int rep = 0; rep < 5; rep++) best = std::min(best, ladder(ch, c, T, R, evb, ckb, est, check, lines));
        printf("L%d %-18s %.3f s  delta %+.3f  ns/line/thread %.0f  lines %lu  check %lx\n", R, rn[R], best,
               R && R < 6 ? best - prev : 0.0, best * T * 1e9 / lines, (unsigned long)lines, (unsigned long)check);
        if (R < 6) prev = best;
    }
}
