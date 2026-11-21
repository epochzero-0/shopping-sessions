// Shopping Sessions pipeline. See RULES.md.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;
using std::string;
using std::vector;

static const int64_t DAY_MS = 86400000LL;
static const int64_t GAP_MS = 1800000LL;
static const int64_t WEEK_MS = 7 * DAY_MS;
static const int PBITS = 8;  // 256 hash partitions
static const int NPART = 1 << PBITS;

static int g_threads = 8;
static auto g_t0 = std::chrono::steady_clock::now();
static void phase(const char* name) {
    double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - g_t0).count();
    fprintf(stderr, "[%7.3f s] %s\n", s, name);
}

template <class F>
static void parallel(int n, F f) {
    vector<std::thread> ts;
    for (int t = 0; t < n; t++) ts.emplace_back(f, t);
    for (auto& t : ts) t.join();
}

// ------------------------------------------------------------------ hashing

static inline uint64_t mix(uint64_t a, uint64_t b) {
    __uint128_t r = (__uint128_t)a * b;
    return (uint64_t)r ^ (uint64_t)(r >> 64);
}

static inline uint64_t hash_bytes(const char* p, size_t n) {
    const uint64_t k0 = 0xa0761d6478bd642full, k1 = 0xe7037ed1a0b428dbull, k2 = 0x8ebc6af09c88c6e3ull;
    uint64_t h = 0x243f6a8885a308d3ull ^ (n * k2);
    while (n > 16) {
        uint64_t a, b;
        memcpy(&a, p, 8);
        memcpy(&b, p + 8, 8);
        h = mix(a ^ k0 ^ h, b ^ k1);
        p += 16;
        n -= 16;
    }
    uint64_t a = 0, b = 0;
    if (n > 8) {
        memcpy(&a, p, 8);
        memcpy(&b, p + n - 8, 8);
    } else if (n > 0) {
        memcpy(&a, p, n);
    }
    return mix(mix(a ^ k0 ^ h, b ^ k1), n ^ k2);
}

static inline bool same(const char* a, uint32_t al, const char* b, uint32_t bl) {
    return al == bl && memcmp(a, b, al) == 0;
}

// byte-wise compare, shorter prefix first
static inline int bcmp3(const char* a, uint32_t al, const char* b, uint32_t bl) {
    int c = memcmp(a, b, al < bl ? al : bl);
    if (c) return c;
    return al < bl ? -1 : (al > bl ? 1 : 0);
}

// String -> small id map, for products / campaigns / link IDs.
struct StrMap {
    vector<uint64_t> h;
    vector<uint32_t> v;  // value + 1, 0 = empty
    vector<const char*> sp;
    vector<uint32_t> sl;
    uint64_t mask = 0;
    void init(size_t n) {
        size_t cap = 16;
        while (cap < n * 2) cap <<= 1;
        h.assign(cap, 0);
        v.assign(cap, 0);
        sp.assign(cap, nullptr);
        sl.assign(cap, 0);
        mask = cap - 1;
    }
    // returns value, inserting `val` if missing; `inserted` set accordingly
    uint32_t insert(uint64_t hh, const char* p, uint32_t n, uint32_t val, bool& inserted) {
        size_t i = hh & mask;
        while (v[i]) {
            if (h[i] == hh && same(sp[i], sl[i], p, n)) {
                inserted = false;
                return v[i] - 1;
            }
            i = (i + 1) & mask;
        }
        h[i] = hh;
        v[i] = val + 1;
        sp[i] = p;
        sl[i] = n;
        inserted = true;
        return val;
    }
    int64_t find(uint64_t hh, const char* p, uint32_t n) const {
        size_t i = hh & mask;
        while (v[i]) {
            if (h[i] == hh && same(sp[i], sl[i], p, n)) return v[i] - 1;
            i = (i + 1) & mask;
        }
        return -1;
    }
};

// ------------------------------------------------------------------ files

struct File {
    const char* p = nullptr;
    size_t n = 0;
};

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
    return f;
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

struct Chunk {
    const char* b;
    const char* e;
    int kind;  // 0 events, 1 clicks, 2 links
};

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

// Iterate lines of a chunk; a trailing '\n' ends a line, it does not start an empty one.
template <class F>
static inline void for_lines(const char* p, const char* e, F f) {
    while (p < e) {
        const char* nl = (const char*)memchr(p, '\n', e - p);
        const char* le = nl ? nl : e;
        f(p, le);
        p = le + 1;
    }
}

// Split a line into exactly N fields. Quotes must wrap a whole field.
// A null (empty) field has length 0.
template <int N>
static inline bool split(const char* p, const char* e, const char** fp, uint32_t* fl) {
    for (int i = 0; i < N; i++) {
        const char* c = (const char*)memchr(p, ',', e - p);
        if (i == N - 1) {
            if (c) return false;
            c = e;
        } else if (!c) {
            return false;
        }
        size_t n = c - p;
        if (n && *p == '"') {
            if (n < 2 || c[-1] != '"' || memchr(p + 1, '"', n - 2)) return false;
            fp[i] = p + 1;
            fl[i] = (uint32_t)(n - 2);
        } else {
            if (memchr(p, '"', n)) return false;
            fp[i] = p;
            fl[i] = (uint32_t)n;
        }
        p = c + 1;
    }
    return true;
}

// ------------------------------------------------------------------ timestamps

static inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

static inline bool d2(const char* p, unsigned& v) {
    unsigned a = (unsigned char)p[0] - '0', b = (unsigned char)p[1] - '0';
    if (a > 9 || b > 9) return false;
    v = a * 10 + b;
    return true;
}

static bool parse_ts(const char* p, uint32_t n, int64_t& out) {
    if (n == 0) return false;
    if ((n == 20 || n == 25) && p[4] == '-') {
        unsigned y1, y2, mo, d, h, mi, s;
        if (!d2(p, y1) || !d2(p + 2, y2) || p[7] != '-' || !d2(p + 5, mo) || !d2(p + 8, d) || p[10] != 'T' ||
            !d2(p + 11, h) || p[13] != ':' || !d2(p + 14, mi) || p[16] != ':' || !d2(p + 17, s))
            return false;
        unsigned y = y1 * 100 + y2;
        if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
        static const unsigned char dim[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        unsigned maxd = dim[mo];
        if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) maxd = 29;
        if (d > maxd) return false;
        int64_t off = 0;
        if (n == 20) {
            if (p[19] != 'Z') return false;
        } else {
            unsigned oh, om;
            char sg = p[19];
            if ((sg != '+' && sg != '-') || !d2(p + 20, oh) || p[22] != ':' || !d2(p + 23, om) || oh > 23 || om > 59)
                return false;
            off = (int64_t)(oh * 60 + om) * 60000;
            if (sg == '-') off = -off;
        }
        out = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60000LL + s * 1000LL - off;
        return true;
    }
    // F3: digits only, epoch ms; too long clamps to INT64_MAX (outside every window)
    uint32_t i = 0;
    for (uint32_t k = 0; k < n; k++)
        if ((unsigned char)p[k] - '0' > 9u) return false;
    while (i < n && p[i] == '0') i++;
    if (n - i > 19) {
        out = INT64_MAX;
        return true;
    }
    uint64_t v = 0;
    for (; i < n; i++) v = v * 10 + (p[i] - '0');
    out = v > (uint64_t)INT64_MAX ? INT64_MAX : (int64_t)v;
    return true;
}

// ------------------------------------------------------------------ records

struct EvRec {
    uint64_t h;  // hash of event_id
    int64_t ingest;
    int64_t ts;
    uint64_t uh;  // hash of user_id
    const char* eid;
    const char* uid;
    uint32_t eidlen, uidlen;
    int32_t price;
    uint8_t cat;
    uint8_t flags;  // 1 valid, 2 purchase
};

struct SessEv {
    uint64_t ph;  // person hash
    int64_t ts;
    const char* name;
    uint32_t namelen;
    int32_t price;
    uint8_t cat;
    uint8_t purchase;
};

struct ClickRec {
    uint64_t ph;
    int64_t ts;
    const char* cid;
    uint32_t cidlen;
    uint32_t camp;
};

struct URow {
    const char* name;
    uint32_t namelen;
    int32_t day;
    int64_t sessions, events, revenue;
    int32_t topcat;  // -1 null
};

// ------------------------------------------------------------------ parquet writer

struct Thrift {
    string b;
    vector<int16_t> st;
    int16_t last = 0;
    void byte(uint8_t x) { b.push_back((char)x); }
    void varint(uint64_t v) {
        while (v >= 0x80) {
            byte((uint8_t)((v & 0x7f) | 0x80));
            v >>= 7;
        }
        byte((uint8_t)v);
    }
    void zz(int64_t v) { varint(((uint64_t)v << 1) ^ (uint64_t)(v >> 63)); }
    void fh(int16_t id, uint8_t type) {
        int d = id - last;
        if (d > 0 && d <= 15)
            byte((uint8_t)((d << 4) | type));
        else {
            byte(type);
            zz(id);
        }
        last = id;
    }
    void i32(int16_t id, int32_t v) { fh(id, 5), zz(v); }
    void i64(int16_t id, int64_t v) { fh(id, 6), zz(v); }
    void str(int16_t id, const string& s) { fh(id, 8), varint(s.size()), b += s; }
    void lst(int16_t id, uint8_t et, int n) {
        fh(id, 9);
        if (n < 15)
            byte((uint8_t)((n << 4) | et));
        else {
            byte(0xF0 | et);
            varint(n);
        }
    }
    void open(int16_t id) { fh(id, 12), push(); }
    void push() { st.push_back(last), last = 0; }  // struct begins (also list elements)
    void close() { byte(0), last = st.back(), st.pop_back(); }
};

enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTES = 6 };

struct Col {
    string name;
    int ptype;
    bool optional;
    int conv;     // -1 none, 0 UTF8, 6 DATE
    int logical;  // 0 none, 1 STRING, 6 DATE
    string data;  // page body
};

static void put32(string& s, uint32_t v) { s.append((const char*)&v, 4); }
static void put64(string& s, uint64_t v) { s.append((const char*)&v, 8); }

static void write_parquet(const string& path, vector<Col>& cols, int64_t nrows) {
    string out = "PAR1";
    vector<int64_t> offs, sizes;
    for (auto& c : cols) {
        Thrift ph;
        ph.i32(1, 0);
        ph.i32(2, (int32_t)c.data.size());
        ph.i32(3, (int32_t)c.data.size());
        ph.open(5);
        ph.i32(1, (int32_t)nrows);
        ph.i32(2, 0);
        ph.i32(3, 3);
        ph.i32(4, 3);
        ph.close();
        ph.byte(0);
        offs.push_back(out.size());
        out += ph.b;
        out += c.data;
        sizes.push_back(ph.b.size() + c.data.size());
        string().swap(c.data);
    }
    Thrift m;
    m.i32(1, 1);
    m.lst(2, 12, (int)cols.size() + 1);
    m.push();
    m.str(4, "schema");
    m.i32(5, (int32_t)cols.size());
    m.close();
    for (auto& c : cols) {
        m.push();
        m.i32(1, c.ptype);
        m.i32(3, c.optional ? 1 : 0);
        m.str(4, c.name);
        if (c.conv >= 0) m.i32(6, c.conv);
        if (c.logical) {
            m.open(10);
            m.open((int16_t)c.logical);
            m.close();
            m.close();
        }
        m.close();
    }
    m.i64(3, nrows);
    if (nrows > 0) {
        int64_t total = 0;
        for (auto s : sizes) total += s;
        m.lst(4, 12, 1);
        m.push();
        m.lst(1, 12, (int)cols.size());
        for (size_t i = 0; i < cols.size(); i++) {
            m.push();
            m.i64(2, offs[i]);
            m.open(3);
            m.i32(1, cols[i].ptype);
            m.lst(2, 5, 2);
            m.zz(0);
            m.zz(3);
            m.lst(3, 8, 1);
            m.varint(cols[i].name.size());
            m.b += cols[i].name;
            m.i32(4, 0);
            m.i64(5, nrows);
            m.i64(6, sizes[i]);
            m.i64(7, sizes[i]);
            m.i64(9, offs[i]);
            m.close();
            m.close();
        }
        m.i64(2, total);
        m.i64(3, nrows);
        m.close();
    } else {
        m.lst(4, 12, 0);
    }
    m.str(6, "infinity");
    m.byte(0);
    out += m.b;
    put32(out, (uint32_t)m.b.size());
    out += "PAR1";
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(1); }
    fwrite(out.data(), 1, out.size(), fp);
    fclose(fp);
}

static Col col_i64(const string& name) { return Col{name, P_INT64, false, -1, 0, {}}; }

static void bytes_val(string& s, const char* p, uint32_t n) {
    put32(s, n);
    s.append(p, n);
}

// ------------------------------------------------------------------ main

int main(int argc, char** argv) {
    string data = getenv("DATA_DIR") ? getenv("DATA_DIR") : "/data";
    string outdir = getenv("OUTPUT_DIR") ? getenv("OUTPUT_DIR") : "/output";
    if (argc >= 3) data = argv[argc - 2], outdir = argv[argc - 1];
    {
        const char* nc = getenv("NCPU");
        int n = nc ? atoi(nc) : (int)std::thread::hardware_concurrency();
        g_threads = std::max(1, std::min(n, 64));
    }
    const int T = g_threads;

    // manifest
    int64_t ws = 0, we = 0;
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
        grab("\"window_start\"", ws);
        grab("\"window_end\"", we);
    }
    const int64_t day0 = (ws >= 0 ? ws / DAY_MS : -((-ws + DAY_MS - 1) / DAY_MS));
    const int ndays = (int)(((we - 1) >= 0 ? (we - 1) / DAY_MS : -1) - day0 + 1);

    // products -> category id (ids in byte order of the category name, 'unknown' included)
    File prodf = map_file(data + "/products.csv");
    StrMap prodmap;
    vector<uint8_t> prodcat;
    vector<string> catnames;
    {
        vector<std::pair<const char*, uint32_t>> pid;
        vector<string> pcat;
        const char* p = prodf.p;
        const char* end = p + prodf.n;
        const char* nl = (const char*)memchr(p, '\n', prodf.n);
        if (nl)
            for_lines(nl + 1, end, [&](const char* b, const char* e) {
                const char* fp[2];
                uint32_t fl[2];
                if (!split<2>(b, e, fp, fl) || !fl[0]) return;
                pid.push_back({fp[0], fl[0]});
                pcat.emplace_back(fp[1], fl[1]);
            });
        catnames = pcat;
        catnames.push_back("unknown");
        std::sort(catnames.begin(), catnames.end());
        catnames.erase(std::unique(catnames.begin(), catnames.end()), catnames.end());
        prodmap.init(pid.size());
        for (size_t i = 0; i < pid.size(); i++) {
            uint8_t c = (uint8_t)(std::lower_bound(catnames.begin(), catnames.end(), pcat[i]) - catnames.begin());
            bool ins;
            uint32_t id = prodmap.insert(hash_bytes(pid[i].first, pid[i].second), pid[i].first, pid[i].second,
                                         (uint32_t)prodcat.size(), ins);
            if (ins) prodcat.push_back(c);
            (void)id;
        }
    }
    const uint8_t unknown_cat =
        (uint8_t)(std::lower_bound(catnames.begin(), catnames.end(), string("unknown")) - catnames.begin());
    const int NC = (int)catnames.size();

    // campaigns
    File campf = map_file(data + "/campaigns.csv");
    StrMap campmap;
    vector<string> campid, campch;
    {
        const char* nl = (const char*)memchr(campf.p, '\n', campf.n);
        vector<std::pair<const char*, uint32_t>> ids;
        if (nl)
            for_lines(nl + 1, campf.p + campf.n, [&](const char* b, const char* e) {
                const char* fp[2];
                uint32_t fl[2];
                if (!split<2>(b, e, fp, fl) || !fl[0]) return;
                ids.push_back({fp[0], fl[0]});
                campch.emplace_back(fp[1], fl[1]);
            });
        campmap.init(ids.size());
        for (size_t i = 0; i < ids.size(); i++) {
            bool ins;
            campmap.insert(hash_bytes(ids[i].first, ids[i].second), ids[i].first, ids[i].second, (uint32_t)i, ins);
            campid.emplace_back(ids[i].first, ids[i].second);
        }
    }
    const uint32_t NCAMP = (uint32_t)campid.size();  // index NCAMP = organic
    phase("small tables");

    // map all CSV files, build chunks
    vector<Chunk> link_chunks, main_chunks;
    size_t ev_bytes = 0, ck_bytes = 0;
    {
        const size_t target = 4 << 20;
        for (auto& path : list_csv(data + "/links")) add_chunks(map_file(path), 2, target, link_chunks);
        for (auto& path : list_csv(data + "/events")) {
            File f = map_file(path);
            ev_bytes += f.n;
            add_chunks(f, 0, target, main_chunks);
        }
        for (auto& path : list_csv(data + "/clicks")) {
            File f = map_file(path);
            ck_bytes += f.n;
            add_chunks(f, 1, target, main_chunks);
        }
        std::stable_sort(main_chunks.begin(), main_chunks.end(),
                         [](const Chunk& a, const Chunk& b) { return (a.e - a.b) > (b.e - b.b); });
    }
    phase("mapped files");

    // ---- links: parse in parallel, union-find serially
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
            for (size_t k; (k = next++) < link_chunks.size();) {
                for_lines(link_chunks[k].b, link_chunks[k].e, [&](const char* b, const char* e) {
                    const char* fp[2];
                    uint32_t fl[2];
                    if (!split<2>(b, e, fp, fl) || !fl[0] || !fl[1]) return;
                    out.push_back({hash_bytes(fp[0], fl[0]), fp[0], fl[0]});
                    out.push_back({hash_bytes(fp[1], fl[1]), fp[1], fl[1]});
                });
            }
        });
    }
    StrMap linkmap;
    vector<uint32_t> parent;
    vector<const char*> nodep;
    vector<uint32_t> nodel;
    vector<uint64_t> personh;  // per node: hash of its person's name
    {
        size_t nsides = 0;
        for (auto& v : lparts) nsides += v.size();
        linkmap.init(nsides / 2 + 16);
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
            uint32_t id = linkmap.insert(s.h, s.p, s.n, (uint32_t)parent.size(), ins);
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
        personh.resize(parent.size());
        for (uint32_t i = 0; i < parent.size(); i++) {
            uint32_t r = find(i);
            personh[i] = (r == i) ? hash_bytes(nodep[i], nodel[i]) : 0;
        }
        for (uint32_t i = 0; i < parent.size(); i++) personh[i] = personh[parent[i]];
        vector<vector<LinkSide>>().swap(lparts);
    }
    // user -> (person hash, name)
    auto person_of = [&](uint64_t uh, const char* p, uint32_t n, uint64_t& ph, const char*& name, uint32_t& nl) {
        int64_t id = linkmap.find(uh, p, n);
        if (id < 0) {
            ph = uh, name = p, nl = n;
        } else {
            uint32_t r = parent[id];
            ph = personh[id], name = nodep[r], nl = nodel[r];
        }
    };
    phase("links");

    // ---- events + clicks: parse into hash partitions
    // events by event_id hash, clicks by person hash
    vector<vector<vector<EvRec>>> evp(T, vector<vector<EvRec>>(NPART));
    vector<vector<vector<ClickRec>>> ckp(T, vector<vector<ClickRec>>(NPART));
    {
        size_t est_ev = ev_bytes / 60 / T / NPART + 16, est_ck = ck_bytes / 40 / T / NPART + 16;
        std::atomic<size_t> next{0};
        parallel(T, [&](int t) {
            for (auto& v : evp[t]) v.reserve(est_ev);
            for (auto& v : ckp[t]) v.reserve(est_ck);
            auto& ev = evp[t];
            auto& ck = ckp[t];
            for (size_t k; (k = next++) < main_chunks.size();) {
                const Chunk& c = main_chunks[k];
                if (c.kind == 0) {
                    for_lines(c.b, c.e, [&](const char* b, const char* e) {
                        const char* fp[7];
                        uint32_t fl[7];
                        if (!split<7>(b, e, fp, fl) || !fl[0]) return;
                        EvRec r;
                        r.h = hash_bytes(fp[0], fl[0]);
                        r.eid = fp[0];
                        r.eidlen = fl[0];
                        if (!parse_ts(fp[3], fl[3], r.ingest)) r.ingest = INT64_MAX;
                        r.flags = 0;
                        r.uid = fp[1];
                        r.uidlen = fl[1];
                        r.price = 0;
                        r.cat = 0;
                        r.uh = 0;
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
                            int64_t pi = prodmap.find(hash_bytes(fp[5], fl[5]), fp[5], fl[5]);
                            r.cat = pi < 0 ? unknown_cat : prodcat[pi];
                            r.uh = hash_bytes(fp[1], fl[1]);
                            r.flags |= 1;
                        }
                        ev[r.h >> (64 - PBITS)].push_back(r);
                    });
                } else {
                    for_lines(c.b, c.e, [&](const char* b, const char* e) {
                        const char* fp[4];
                        uint32_t fl[4];
                        if (!split<4>(b, e, fp, fl) || !fl[0] || !fl[1]) return;
                        int64_t camp = campmap.find(hash_bytes(fp[2], fl[2]), fp[2], fl[2]);
                        if (camp < 0) return;
                        int64_t ts;
                        if (!parse_ts(fp[3], fl[3], ts) || ts < ws - WEEK_MS || ts >= we) return;
                        ClickRec r;
                        const char* nm;
                        uint32_t nl;
                        person_of(hash_bytes(fp[1], fl[1]), fp[1], fl[1], r.ph, nm, nl);
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

    // ---- dedup events per partition, emit survivors by person partition
    vector<vector<vector<SessEv>>> sp(T, vector<vector<SessEv>>(NPART));
    {
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
                    person_of(x->uh, x->uid, x->uidlen, e.ph, e.name, e.namelen);
                    e.ts = x->ts;
                    e.price = x->price;
                    e.cat = x->cat;
                    e.purchase = (x->flags & 2) ? 1 : 0;
                    sp[t][e.ph >> (64 - PBITS)].push_back(e);
                }
                for (int s = 0; s < T; s++) vector<EvRec>().swap(evp[s][q]);
            }
        });
    }
    phase("dedup");

    // ---- sessions, credit, aggregate
    vector<vector<URow>> urows(T);
    const size_t CD = (size_t)(NCAMP + 1) * ndays;
    vector<vector<int64_t>> c_sess(T, vector<int64_t>(CD)), c_purch(T, vector<int64_t>(CD)),
        c_rev(T, vector<int64_t>(CD)), c_assist(T, vector<int64_t>(CD));
    {
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
            auto& rows = urows[t];
            int64_t* CS = c_sess[t].data();
            int64_t* CP = c_purch[t].data();
            int64_t* CR = c_rev[t].data();
            int64_t* CA = c_assist[t].data();
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
                    return a.ph != b.ph ? a.ph < b.ph : a.ts < b.ts;
                });
                std::sort(cks.begin(), cks.end(), [](const ClickRec& a, const ClickRec& b) {
                    if (a.ph != b.ph) return a.ph < b.ph;
                    if (a.ts != b.ts) return a.ts < b.ts;
                    return bcmp3(a.cid, a.cidlen, b.cid, b.cidlen) < 0;
                });
                size_t ci = 0;
                size_t i = 0;
                while (i < evs.size()) {
                    uint64_t ph = evs[i].ph;
                    size_t j = i;
                    while (j < evs.size() && evs[j].ph == ph) j++;
                    while (ci < cks.size() && cks[ci].ph < ph) ci++;
                    size_t cb = ci;
                    while (ci < cks.size() && cks[ci].ph == ph) ci++;
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
                            for (int c = 0; c < NC; c++)
                                if (catrev[c] > best && catrev[c] > 0) best = catrev[c], top = c;
                            std::fill(catrev.begin(), catrev.end(), 0);
                        }
                        rows.push_back({evs[i].name, evs[i].namelen, (int32_t)cur_day, us, ue, ur, top});
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
                            uint32_t c = active[z];
                            if (ccount[c]) {
                                CA[(size_t)c * ndays + di]++;
                                active[w++] = c;
                            }
                        }
                        active.resize(w);
                        a = b;
                    }
                    flush();
                    for (uint32_t c : active) ccount[c] = 0;
                    active.clear();
                    i = j;
                }
            }
        });
    }
    phase("sessions");

    // ---- write
    fs::create_directories(outdir + "/user_daily");
    fs::create_directories(outdir + "/campaign_daily");
    parallel(T, [&](int t) {
        auto& rows = urows[t];
        if (rows.empty() && t != 0) return;
        vector<Col> cols = {Col{"user_id", P_BYTES, false, 0, 1, {}}, Col{"day", P_INT32, false, 6, 6, {}},
                            col_i64("sessions"), col_i64("events"), col_i64("revenue_cents"),
                            Col{"top_category", P_BYTES, true, 0, 1, {}}};
        size_t n = rows.size();
        size_t namebytes = 0;
        for (auto& r : rows) namebytes += r.namelen + 4;
        cols[0].data.reserve(namebytes);
        cols[1].data.reserve(n * 4);
        for (int c = 2; c < 5; c++) cols[c].data.reserve(n * 8);
        string defs((n + 7) / 8, '\0');
        string vals;
        for (size_t k = 0; k < n; k++) {
            auto& r = rows[k];
            bytes_val(cols[0].data, r.name, r.namelen);
            put32(cols[1].data, (uint32_t)r.day);
            put64(cols[2].data, r.sessions);
            put64(cols[3].data, r.events);
            put64(cols[4].data, r.revenue);
            if (r.topcat >= 0) {
                defs[k >> 3] |= (char)(1 << (k & 7));
                bytes_val(vals, catnames[r.topcat].data(), (uint32_t)catnames[r.topcat].size());
            }
        }
        string rle;
        {
            uint64_t hdr = ((uint64_t)((n + 7) / 8) << 1) | 1;
            while (hdr >= 0x80) rle.push_back((char)((hdr & 0x7f) | 0x80)), hdr >>= 7;
            rle.push_back((char)hdr);
            rle += defs;
        }
        if (n) {
            put32(cols[5].data, (uint32_t)rle.size());
            cols[5].data += rle;
            cols[5].data += vals;
        }
        write_parquet(outdir + "/user_daily/part-" + std::to_string(t) + ".parquet", cols, (int64_t)n);
    });
    {
        for (int t = 1; t < T; t++)
            for (size_t k = 0; k < CD; k++) {
                c_sess[0][k] += c_sess[t][k];
                c_purch[0][k] += c_purch[t][k];
                c_rev[0][k] += c_rev[t][k];
                c_assist[0][k] += c_assist[t][k];
            }
        vector<Col> cols = {Col{"campaign_id", P_BYTES, false, 0, 1, {}}, Col{"channel", P_BYTES, false, 0, 1, {}},
                            Col{"day", P_INT32, false, 6, 6, {}}, col_i64("sessions"),
                            col_i64("purchasing_sessions"), col_i64("revenue_cents"), col_i64("assisted_sessions")};
        int64_t n = 0;
        for (uint32_t c = 0; c <= NCAMP; c++)
            for (int d = 0; d < ndays; d++) {
                size_t k = (size_t)c * ndays + d;
                if (!c_sess[0][k] && !c_assist[0][k]) continue;
                const string& id = c == NCAMP ? string("organic") : campid[c];
                const string& ch = c == NCAMP ? string("organic") : campch[c];
                bytes_val(cols[0].data, id.data(), (uint32_t)id.size());
                bytes_val(cols[1].data, ch.data(), (uint32_t)ch.size());
                put32(cols[2].data, (uint32_t)(day0 + d));
                put64(cols[3].data, c_sess[0][k]);
                put64(cols[4].data, c_purch[0][k]);
                put64(cols[5].data, c_rev[0][k]);
                put64(cols[6].data, c_assist[0][k]);
                n++;
            }
        write_parquet(outdir + "/campaign_daily/part-0.parquet", cols, n);
    }
    phase("write");
    return 0;
}
