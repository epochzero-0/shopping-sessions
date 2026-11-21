// split(), for_lines(), parse_ts() vs ref/reference.py on the cases from tests/csv_cases.py.
// Each input sits in an exact-size heap buffer, so ASan flags any read past its end.
// Usage: csv_test CASES   Prints "unit PASSED/TOTAL"; exit 1 on any mismatch.
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "../src/csv.hpp"

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

int main(int argc, char** argv) {
    std::ifstream in(argc > 1 ? argv[1] : "build/csv_cases.txt");
    if (!in) { fprintf(stderr, "no cases file\n"); return 2; }
    std::string line;
    long total = 0, bad = 0;
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
        } else {
            continue;
        }
        total++;
        if (got != want) {
            if (++bad <= 10) fprintf(stderr, "MISMATCH %s\n  got  %s\n", line.c_str(), got.c_str());
        }
    }
    printf("unit %ld/%ld\n", total - bad, total);
    return bad ? 1 : 0;
}
