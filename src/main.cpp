// Shopping Sessions pipeline. See RULES.md.
// BENCH=N runs the whole pipeline N times in one process and prints each phase's minimum.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>

#include "csv.hpp"
#include "parquet.hpp"
#include "pipeline.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

// ------------------------------------------------------------------ phase timing

static auto g_t0 = std::chrono::steady_clock::now();
static auto g_last = g_t0;
static vector<std::pair<string, double>> g_phases;  // this run

void phase(const char* name) {
    auto now = std::chrono::steady_clock::now();
    double s = std::chrono::duration<double>(now - g_t0).count();
    double d = std::chrono::duration<double>(now - g_last).count();
    g_last = now;
    g_phases.push_back({name, d});
    fprintf(stderr, "[%7.3f s] %-22s %6.3f\n", s, name, d);
}

// ------------------------------------------------------------------ files

struct File {
    const char* p = nullptr;
    size_t n = 0;
};

// Live big_alloc regions, released in parallel before _exit.
static std::mutex g_big_mu;
static vector<std::pair<void*, size_t>> g_big;

void* big_alloc(size_t bytes) {
    void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, bytes, MADV_HUGEPAGE);
    std::lock_guard<std::mutex> g(g_big_mu);
    g_big.push_back({p, bytes});
    return p;
}

void big_free(void* p, size_t bytes) {
    munmap(p, bytes);
    std::lock_guard<std::mutex> g(g_big_mu);
    for (auto& r : g_big)
        if (r.first == p) {
            r = g_big.back();
            g_big.pop_back();
            break;
        }
}

static vector<File> g_files;  // unmapped at the end of each run

static File map_file(const string& path) {
    File f;
#ifdef _WIN32
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) { perror(path.c_str()); exit(1); }
    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char* buf = (char*)malloc(sz + 1);
    if (sz && fread(buf, 1, sz, fp) != (size_t)sz) { perror(path.c_str()); exit(1); }
    fclose(fp);
    f.p = buf;
    f.n = sz;
#else
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    f.n = st.st_size;
    if (f.n) {
        void* m = mmap(nullptr, f.n, PROT_READ, MAP_PRIVATE, fd, 0);
        if (m == MAP_FAILED) { perror("mmap"); exit(1); }
        f.p = (const char*)m;
    }
    close(fd);
#endif
    g_files.push_back(f);
    return f;
}

static void unmap_all() {
    for (auto& f : g_files) {
#ifdef _WIN32
        free((void*)f.p);
#else
        if (f.n) munmap((void*)f.p, f.n);
#endif
    }
    g_files.clear();
}

static vector<string> list_csv(const string& dir) {
    vector<string> out;
    if (!fs::is_directory(dir)) return out;
    for (auto& e : fs::directory_iterator(dir)) {
        string name = e.path().filename().string();
        if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".csv") == 0 && e.is_regular_file())
            out.push_back(e.path().string());
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Split a file's data lines (header skipped) into chunks ending on '\n'.
static void add_chunks(const File& f, int kind, size_t target, vector<Chunk>& out) {
    if (!f.n) return;
    const char* p = f.p;
    const char* end = f.p + f.n;
    const char* nl = (const char*)memchr(p, '\n', f.n);
    if (!nl) return;  // header only
    p = nl + 1;
    while (p < end) {
        const char* q = p + target;
        if (q >= end) {
            q = end;
        } else {
            const char* n2 = (const char*)memchr(q, '\n', end - q);
            q = n2 ? n2 + 1 : end;
        }
        out.push_back({p, q, kind});
        p = q;
    }
}

// ------------------------------------------------------------------ setup

static void load_small(const string& data, Ctx& c) {
    // manifest
    {
        File f = map_file(data + "/manifest.json");
        string s(f.p, f.n);
        auto grab = [&](const char* key, int64_t& v) {
            size_t k = s.find(key);
            k = s.find(':', k);
            k = s.find('"', k) + 1;
            size_t e = s.find('"', k);
            if (!parse_ts(s.data() + k, (uint32_t)(e - k), v)) { fprintf(stderr, "bad manifest\n"); exit(1); }
        };
        grab("\"window_start\"", c.ws);
        grab("\"window_end\"", c.we);
    }
    if (c.we - c.ws + WEEK_MS >= (1LL << 32) || c.ws < WEEK_MS) {
        fprintf(stderr, "window does not fit 32-bit ms offsets\n");
        exit(1);
    }
    c.day0 = (c.ws >= 0 ? c.ws / DAY_MS : -((-c.ws + DAY_MS - 1) / DAY_MS));
    c.ndays = (int)(((c.we - 1) >= 0 ? (c.we - 1) / DAY_MS : -1) - c.day0 + 1);

    // products -> category id (ids in byte order of the category name, 'unknown' included)
    {
        File prodf = map_file(data + "/products.csv");
        vector<std::pair<const char*, uint32_t>> pid;
        vector<string> pcat;
        const char* nl = (const char*)memchr(prodf.p, '\n', prodf.n);
        if (nl)
            for_lines(nl + 1, prodf.p + prodf.n, [&](const char* b, const char* e) {
                const char* fp[2];
                uint32_t fl[2];
                if (!split<2>(b, e, fp, fl) || !fl[0]) return;
                pid.push_back({fp[0], fl[0]});
                pcat.emplace_back(fp[1], fl[1]);
            });
        c.catnames = pcat;
        c.catnames.push_back("unknown");
        std::sort(c.catnames.begin(), c.catnames.end());
        c.catnames.erase(std::unique(c.catnames.begin(), c.catnames.end()), c.catnames.end());
        c.prodmap.init(pid.size());
        for (size_t i = 0; i < pid.size(); i++) {
            uint8_t cat = (uint8_t)(std::lower_bound(c.catnames.begin(), c.catnames.end(), pcat[i]) -
                                    c.catnames.begin());
            bool ins;
            c.prodmap.insert(hash_bytes(pid[i].first, pid[i].second), pid[i].first, pid[i].second,
                             (uint32_t)c.prodcat.size(), ins);
            if (ins) c.prodcat.push_back(cat);
        }
    }
    c.unknown_cat = (uint8_t)(std::lower_bound(c.catnames.begin(), c.catnames.end(), string("unknown")) -
                              c.catnames.begin());
    c.NC = (int)c.catnames.size();

    // campaigns
    {
        File campf = map_file(data + "/campaigns.csv");
        const char* nl = (const char*)memchr(campf.p, '\n', campf.n);
        vector<std::pair<const char*, uint32_t>> ids;
        if (nl)
            for_lines(nl + 1, campf.p + campf.n, [&](const char* b, const char* e) {
                const char* fp[2];
                uint32_t fl[2];
                if (!split<2>(b, e, fp, fl) || !fl[0]) return;
                ids.push_back({fp[0], fl[0]});
                c.campch.emplace_back(fp[1], fl[1]);
            });
        c.campmap.init(ids.size());
        for (size_t i = 0; i < ids.size(); i++) {
            bool ins;
            c.campmap.insert(hash_bytes(ids[i].first, ids[i].second), ids[i].first, ids[i].second, (uint32_t)i,
                             ins);
            c.campid.emplace_back(ids[i].first, ids[i].second);
        }
    }
    c.NCAMP = (uint32_t)c.campid.size();
    phase("small tables");

    // map all CSV files, build chunks
    const size_t target = 4 << 20;
    for (auto& path : list_csv(data + "/links")) add_chunks(map_file(path), 2, target, c.link_chunks);
    for (auto& path : list_csv(data + "/events")) {
        File f = map_file(path);
        c.ev_bytes += f.n;
        add_chunks(f, 0, target, c.main_chunks);
    }
    for (auto& path : list_csv(data + "/clicks")) {
        File f = map_file(path);
        c.ck_bytes += f.n;
        add_chunks(f, 1, target, c.main_chunks);
    }
    std::stable_sort(c.main_chunks.begin(), c.main_chunks.end(),
                     [](const Chunk& a, const Chunk& b) { return (a.e - a.b) > (b.e - b.b); });
    phase("mapped files");
}

// ------------------------------------------------------------------ output

static void write_campaign(const Ctx& c, Agg& A, const string& outdir);

// Thread t's user_daily rows go to part-t; one more thread writes campaign_daily alongside.
static void write_outputs(const Ctx& c, const Links& L, Agg& A, const string& outdir) {
    const int T = c.T;
    fs::create_directories(outdir + "/user_daily");
    fs::create_directories(outdir + "/campaign_daily");
    parallel(T + 1, [&](int t) {
        if (t == T) return write_campaign(c, A, outdir);
        auto& rows = A.urows[t];
        if (rows.empty() && t != 0) return;
        vector<Col> cols = {Col{"user_id", P_BYTES, false, 0, 1, {}}, Col{"day", P_INT32, false, 6, 6, {}},
                            col_i64("sessions"), col_i64("events"), col_i64("revenue_cents"),
                            Col{"top_category", P_BYTES, true, 0, 1, {}}};
        const size_t n = rows.size();
        if (n) {
            // user_id as DELTA_BYTE_ARRAY: a person's rows are adjacent, so a repeated row is a whole-value prefix
            // with an empty suffix, and each person's name bytes are stored once. The other columns are dictionaries.
            vector<int32_t> pre(n), suf(n);
            string sufs, prev;
            uint64_t pkey = 0;
            for (size_t k = 0; k < n; k++) {
                if (k && rows[k].key == pkey) {
                    pre[k] = (int32_t)prev.size(), suf[k] = 0;
                    continue;
                }
                pkey = rows[k].key;
                char buf[16];
                uint32_t nl;
                const char* name = L.name(pkey, buf, nl);
                uint32_t p = 0;
                while (p < nl && p < prev.size() && name[p] == prev[p]) p++;
                pre[k] = (int32_t)p, suf[k] = (int32_t)(nl - p);
                sufs.append(name + p, nl - p);
                prev.assign(name, nl);
            }
            cols[0].enc = 7;
            delta_bytes(cols[0].data, pre, suf, sufs);
            vector<uint32_t> ix(n), tix;
            {
                IntDict d(4);
                for (size_t k = 0; k < n; k++) ix[k] = d.get(rows[k].day);
                set_dict(cols[1], std::move(d.dict), d.n, ix);
            }
            for (int col = 2; col < 5; col++) {
                IntDict d(8);
                for (size_t k = 0; k < n; k++)
                    ix[k] = d.get(col == 2 ? rows[k].sessions : col == 3 ? rows[k].events : rows[k].revenue);
                set_dict(cols[col], std::move(d.dict), d.n, ix);
            }
            vector<uint32_t> defs(n);
            for (size_t k = 0; k < n; k++)
                if ((defs[k] = rows[k].topcat >= 0)) tix.push_back((uint32_t)rows[k].topcat);
            string lev;
            rle_hybrid(lev, defs.data(), n, 1);
            put32(cols[5].data, (uint32_t)lev.size());
            cols[5].data += lev;
            string cats;
            for (auto& s : c.catnames) bytes_val(cats, s.data(), (uint32_t)s.size());
            set_dict(cols[5], std::move(cats), (uint32_t)c.catnames.size(), tix);
        }
        write_parquet(outdir + "/user_daily/part-" + std::to_string(t) + ".parquet", cols, (int64_t)n);
    });
}

static void write_campaign(const Ctx& c, Agg& A, const string& outdir) {
    const int T = c.T;
    const size_t CD = (size_t)(c.NCAMP + 1) * c.ndays;
    for (int t = 1; t < T; t++)
        for (size_t k = 0; k < CD; k++) {
            A.c_sess[0][k] += A.c_sess[t][k];
            A.c_purch[0][k] += A.c_purch[t][k];
            A.c_rev[0][k] += A.c_rev[t][k];
            A.c_assist[0][k] += A.c_assist[t][k];
        }
    vector<Col> cols = {Col{"campaign_id", P_BYTES, false, 0, 1, {}}, Col{"channel", P_BYTES, false, 0, 1, {}},
                        Col{"day", P_INT32, false, 6, 6, {}}, col_i64("sessions"),
                        col_i64("purchasing_sessions"), col_i64("revenue_cents"), col_i64("assisted_sessions")};
    int64_t n = 0;
    const string organic = "organic";
    for (uint32_t cc = 0; cc <= c.NCAMP; cc++)
        for (int d = 0; d < c.ndays; d++) {
            size_t k = (size_t)cc * c.ndays + d;
            if (!A.c_sess[0][k] && !A.c_assist[0][k]) continue;
            const string& id = cc == c.NCAMP ? organic : c.campid[cc];
            const string& ch = cc == c.NCAMP ? organic : c.campch[cc];
            bytes_val(cols[0].data, id.data(), (uint32_t)id.size());
            bytes_val(cols[1].data, ch.data(), (uint32_t)ch.size());
            put32(cols[2].data, (uint32_t)(c.day0 + d));
            put64(cols[3].data, A.c_sess[0][k]);
            put64(cols[4].data, A.c_purch[0][k]);
            put64(cols[5].data, A.c_rev[0][k]);
            put64(cols[6].data, A.c_assist[0][k]);
            n++;
        }
    write_parquet(outdir + "/campaign_daily/part-0.parquet", cols, n);
}

// ------------------------------------------------------------------ main

static int g_runs = 1, g_run = 0;
static vector<std::pair<string, double>> g_best;
static double g_best_total = 1e30;

static void record_run() {
    double total = 0;
    for (auto& p : g_phases) total += p.second;
    g_best_total = std::min(g_best_total, total);
    if (g_run == 0)
        g_best = g_phases;
    else
        for (size_t i = 0; i < g_best.size() && i < g_phases.size(); i++)
            g_best[i].second = std::min(g_best[i].second, g_phases[i].second);
    if (g_runs > 1) fprintf(stderr, "run %d: %.3f s\n", g_run + 1, total);
}

// MADV_DONTNEED every big region and input mapping, split into T equal 2 MB-aligned slices of their concatenation,
// so the unmap at exit has little left to tear down single-threaded.
static void release_parallel(int T) {
    const size_t G = 2 << 20, PG = 4096;
    struct R {
        char* p;
        size_t n;  // mapped bytes, rounded up to a page; never past the mapping
    };
    vector<R> r;
    for (auto& b : g_big) r.push_back({(char*)b.first, (b.second + PG - 1) / PG * PG});
    for (auto& f : g_files)
        if (f.n) r.push_back({(char*)f.p, (f.n + PG - 1) / PG * PG});
    size_t total = 0;  // each region takes a G-aligned span of the concatenation
    for (auto& x : r) total += (x.n + G - 1) / G * G;
    const size_t slice = (total / T + G - 1) / G * G;
    if (!slice) return;
    parallel(T, [&](int t) {
        const size_t lo = (size_t)t * slice, hi = lo + slice;
        size_t off = 0;
        for (auto& x : r) {
            const size_t a = std::max(lo, off), b = std::min(hi, off + x.n);
            if (a < b) madvise(x.p + (a - off), b - a, MADV_DONTNEED);
            off += (x.n + G - 1) / G * G;
        }
    });
}

static int g_T = 1;

// Outputs are closed; skip every destructor and free.
[[noreturn]] static void report_and_exit() {
    if (g_runs > 1) {
        fprintf(stderr, "BENCH min of %d:", g_runs);
        for (auto& p : g_best) fprintf(stderr, " %s %.3f ·", p.first.c_str(), p.second);
        fprintf(stderr, " total %.3f s\n", g_best_total);
    }
    if (FILE* st = fopen("/proc/self/status", "r")) {
        char line[256];
        while (fgets(line, sizeof line, st))
            if (!strncmp(line, "VmHWM", 5) || !strncmp(line, "RssFile", 7) || !strncmp(line, "RssAnon", 7))
                fputs(line, stderr);
        fclose(st);
    }
    release_parallel(g_T);
    _exit(0);
}

static void run_once(const string& data, const string& outdir, int T) {
    Ctx c;
    c.T = T;
    load_small(data, c);
    Links L;
    build_links(c, L, (c.ev_bytes / 50 + c.ck_bytes / 35) / 8);
    phase("links");
    EvParts sp;
    CkParts ckp;
    parse_and_dedup(c, L, sp, ckp);
    Agg A;
    sessions(c, sp, ckp, A);
    phase("sessions");
    write_outputs(c, L, A, outdir);
    phase("write");
    if (ckp.arena.spilled || sp.arena.spilled) fprintf(stderr, "arena spilled\n");
    record_run();
    if (g_run == g_runs - 1) report_and_exit();
    unmap_all();
}

int main(int argc, char** argv) {
    string data = getenv("DATA_DIR") ? getenv("DATA_DIR") : "/data";
    string outdir = getenv("OUTPUT_DIR") ? getenv("OUTPUT_DIR") : "/output";
    if (argc >= 3) data = argv[argc - 2], outdir = argv[argc - 1];
    const char* nc = getenv("NCPU");
    int T = std::max(1, std::min(nc ? atoi(nc) : (int)std::thread::hardware_concurrency(), 64));
    const char* bn = getenv("BENCH");
    g_runs = bn ? std::max(1, atoi(bn)) : 1;
    g_T = T;
    for (g_run = 0; g_run < g_runs; g_run++) {
        g_phases.clear();
        g_t0 = g_last = std::chrono::steady_clock::now();
        run_once(data, outdir, T);
    }
}
