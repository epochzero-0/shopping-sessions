// split(), for_lines(), parse_ts() vs ref/reference.py on the cases from tests/csv_cases.py;
// scan_lines() vs the reference on every split case and vs for_lines + split on every blob.
// Each input sits in an exact-size heap buffer, so ASan flags any read past its end.
// Usage: csv_test CASES   Prints "unit PASSED/TOTAL"; exit 1 on any mismatch.
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "../src/csv.hpp"
#include "../src/hash.hpp"

static std::string unhex(const std::string& s) {
    if (s == "." || s == "-") return {};
    std::string r(s.size() / 2, '\0');
    for (size_t i = 0; i < r.size(); i++) r[i] = (char)std::stoi(s.substr(2 * i, 2), nullptr, 16);
    return r;
}

static std::string hex(const char* p, size_t n) {
    if (!n) return ".";
    static const char* d = "0123456789abcdef";
    std::string r;
    for (size_t i = 0; i < n; i++) r += d[(unsigned char)p[i] >> 4], r += d[(unsigned char)p[i] & 15];
    return r;
}

struct Buf {  // exact-size copy; malloc(0) for an empty input, so even p[0] is caught
    char* p;
    size_t n;
    explicit Buf(const std::string& s) : p((char*)malloc(s.size())), n(s.size()) {
        if (n) memcpy(p, s.data(), n);
    }
    ~Buf() { free(p); }
};

template <int N>
static std::string run_split(const Buf& b) {
    const char* fp[N];
    uint32_t fl[N];
    if (!split<N>(b.p, b.p + b.n, fp, fl)) return "bad";
    std::string r = "ok";
    for (int i = 0; i < N; i++) r += " " + (fl[i] ? hex(fp[i], fl[i]) : std::string("-"));
    return r;
}

template <int N>
static std::string fields(const char* const* fp, const uint32_t* fl) {
    std::string r = "ok";
    for (int i = 0; i < N; i++) r += " " + (fl[i] ? hex(fp[i], fl[i]) : std::string("-"));
    return r;
}

// scan_lines<N> on a single line: same answer format as run_split
template <int N>
static std::string run_scan(const Buf& b) {
    std::string r = "bad";
    int calls = 0;
    scan_lines<N>(b.p, b.p + b.n, [&](const char* const* fp, const uint32_t* fl) {
        r = fields<N>(fp, fl);
        calls++;
    });
    return calls > 1 ? "multi" : r;
}

// scan_lines<N> over a blob vs for_lines + split<N>
template <int N>
static std::string scan_blob(const Buf& b, bool use_scan) {
    std::string r;
    if (use_scan) {
        scan_lines<N>(b.p, b.p + b.n, [&](const char* const* fp, const uint32_t* fl) { r += fields<N>(fp, fl) + "|"; });
    } else {
        for_lines(b.p, b.p + b.n, [&](const char* s, const char* e) {
            const char* fp[N];
            uint32_t fl[N];
            if (split<N>(s, e, fp, fl)) r += fields<N>(fp, fl) + "|";
        });
    }
    return r;
}

// pack_id vs pack_id_ref on random strings over the class boundaries; packed keys unpack to the input.
static long key_tests(long& total) {
    static const char alpha[] = "0189az/:`{AZ@[\x80\xff u-_\"";
    long bad = 0;
    uint64_t s = 88172645463325252ull;
    auto rnd = [&]() { s ^= s << 13, s ^= s >> 7, s ^= s << 17; return s; };
    for (int it = 0; it < 400000; it++) {
        uint32_t n = (uint32_t)(rnd() % 14);
        std::string x(n, '\0');
        bool lower = rnd() % 4 != 0;  // most strings stay in [0-9a-z]
        for (auto& ch : x) ch = lower ? "0123456789abcdefghijklmnopqrstuvwxyz"[rnd() % 36] : alpha[rnd() % (sizeof alpha - 1)];
        Buf b(x);
        uint64_t k = pack_id(b.p, n), r = pack_id_ref(b.p, n);
        total++;
        bool ok = k == r;
        if (ok && k) {
            char out[16];
            uint32_t m = unpack_id(k, out);
            ok = m == n && memcmp(out, x.data(), n) == 0 && k < (1ull << 60);
        }
        if (ok && !k) {  // 0 only outside the form
            bool form = n >= 1 && n <= 10;
            for (char ch : x) form &= (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z');
            ok = !form;
        }
        if (!ok && ++bad <= 10) fprintf(stderr, "KEY MISMATCH %s: %llx vs %llx\n", hex(x.data(), n).c_str(),
                                        (unsigned long long)k, (unsigned long long)r);
    }
    return bad;
}

int main(int argc, char** argv) {
    std::ifstream in(argc > 1 ? argv[1] : "build/csv_cases.txt");
    if (!in) { fprintf(stderr, "no cases file\n"); return 2; }
    std::string line;
    long total = 0, bad = 0, scan_total = 0, scan_bad = 0;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string kind, got, want;
        ss >> kind;
        if (kind == "S") {
            int n;
            std::string x;
            ss >> n >> x;
            std::getline(ss, want);
            want = want.substr(1);
            Buf b(unhex(x));
            got = n == 2 ? run_split<2>(b) : n == 4 ? run_split<4>(b) : run_split<7>(b);
            std::string sc = n == 2 ? run_scan<2>(b) : n == 4 ? run_scan<4>(b) : run_scan<7>(b);
            scan_total++;
            if (sc != want && ++scan_bad <= 10) fprintf(stderr, "SCAN MISMATCH %s\n  got  %s\n", line.c_str(), sc.c_str());
        } else if (kind == "T") {
            std::string x;
            ss >> x;
            std::getline(ss, want);
            want = want.substr(1);
            Buf b(unhex(x));
            int64_t v;
            got = parse_ts(b.p, (uint32_t)b.n, v) ? "ok " + std::to_string(v) : "bad";
        } else if (kind == "L") {
            std::string x;
            ss >> x;
            std::getline(ss, want);
            want = want.substr(1);
            Buf b(unhex(x));
            std::vector<std::string> ls;
            for_lines(b.p, b.p + b.n, [&](const char* s, const char* e) { ls.push_back(hex(s, e - s)); });
            got = std::to_string(ls.size()) + " ";
            for (size_t i = 0; i < ls.size(); i++) got += (i ? " " : "") + ls[i];
            bool ok = scan_blob<2>(b, true) == scan_blob<2>(b, false) && scan_blob<4>(b, true) == scan_blob<4>(b, false) &&
                      scan_blob<7>(b, true) == scan_blob<7>(b, false);
            scan_total++;
            if (!ok && ++scan_bad <= 10) fprintf(stderr, "SCAN BLOB MISMATCH %s\n", x.substr(0, 200).c_str());
        } else {
            continue;
        }
        total++;
        if (got != want) {
            if (++bad <= 10) fprintf(stderr, "MISMATCH %s\n  got  %s\n", line.c_str(), got.c_str());
        }
    }
    long key_total = 0, key_bad = key_tests(key_total);
    printf("unit %ld/%ld scan %ld/%ld keys %ld/%ld\n", total - bad, total, scan_total - scan_bad, scan_total,
           key_total - key_bad, key_total);
    return bad || scan_bad || key_bad ? 1 : 0;
}
