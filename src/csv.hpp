// CSV dialect: line iteration, field split, timestamps.
#pragma once
#include <cstdint>
#include <cstring>

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

static inline bool parse_ts(const char* p, uint32_t n, int64_t& out) {
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
