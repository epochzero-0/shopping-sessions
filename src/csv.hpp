// CSV dialect: line iteration, field split, timestamps.
#pragma once
#include <cstdint>
#include <cstring>
#ifdef __AVX512BW__
#include <immintrin.h>
#endif

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

// for_lines + split<N> in one pass: f(fp, fl) for every line that splits into exactly N fields.
// AVX-512BW: a line is read as a 128-byte window (two zmm); ',', '"', '\n' become bitmasks, fields are walked
// with tzcnt. Quote rule on masks: every quote sits at a field's first or last byte, quotes pair up with no ','
// between a pair (prefix XOR), even count. Lines over 127 bytes take memchr + split.
template <int N, class F>
static inline void scan_lines(const char* p, const char* e, F f) {
    const char* fp[N];
    uint32_t fl[N];
#ifdef __AVX512BW__
    const __m512i vc = _mm512_set1_epi8(','), vq = _mm512_set1_epi8('"'), vn = _mm512_set1_epi8('\n');
    while (p < e) {
        const size_t rem = (size_t)(e - p);
        __m512i a, b;
        if (rem >= 128) {
            a = _mm512_loadu_si512(p);
            b = _mm512_loadu_si512(p + 64);
        } else {  // masked loads: bytes past e are zero and never touched
            a = _mm512_maskz_loadu_epi8(_bzhi_u64(~0ull, (unsigned)rem), p);
            b = _mm512_maskz_loadu_epi8(rem > 64 ? _bzhi_u64(~0ull, (unsigned)(rem - 64)) : 0, p + 64);
        }
        const uint64_t n0 = _mm512_cmpeq_epi8_mask(a, vn), n1 = _mm512_cmpeq_epi8_mask(b, vn);
        uint32_t L = (uint32_t)_tzcnt_u64(n0) + (n0 ? 0 : (uint32_t)_tzcnt_u64(n1));  // tzcnt(0) = 64
        if (__builtin_expect((n0 | n1) == 0, 0)) {
            if (rem <= 128) {
                L = (uint32_t)rem;
            } else {  // long line
                const char* nl = (const char*)memchr(p, '\n', rem);
                const char* le = nl ? nl : e;
                if (split<N>(p, le, fp, fl)) f(fp, fl);
                p = le + 1;
                continue;
            }
        }
        const uint64_t lm0 = _bzhi_u64(~0ull, L), lm1 = L > 64 ? _bzhi_u64(~0ull, L - 64) : 0;
        const uint64_t c0 = _mm512_cmpeq_epi8_mask(a, vc) & lm0, c1 = _mm512_cmpeq_epi8_mask(b, vc) & lm1;
        const char* line = p;
        p += L + 1;
        if (_mm_popcnt_u64(c0) + _mm_popcnt_u64(c1) != N - 1) continue;
        const uint64_t q0 = _mm512_cmpeq_epi8_mask(a, vq) & lm0, q1 = _mm512_cmpeq_epi8_mask(b, vq) & lm1;
        // Branch-free: most lines have a quoted field, but which ones is random.
        // Field first bytes (after ',' or line start) and last bytes (before ',' or line end); L >= 1 here.
        const uint64_t s0 = c0 << 1 | 1, s1 = c1 << 1 | c0 >> 63;
        const uint64_t e0 = c0 >> 1 | c1 << 63 | _bzhi_u64(1ull << ((L - 1) & 63), L <= 64 ? 64 : 0);
        const uint64_t e1 = c1 >> 1 | _bzhi_u64(1ull << ((L - 1) & 63), L > 64 ? 64 : 0);
        const __m128i ones = _mm_set1_epi64x(-1);
        const uint64_t x0 = (uint64_t)_mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_cvtsi64_si128((long long)q0), ones, 0));
        const uint64_t x1 = (uint64_t)_mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_cvtsi64_si128((long long)q1), ones, 0)) ^
                            (0 - (uint64_t)(_mm_popcnt_u64(q0) & 1));
        const uint64_t bad = (q0 & ~(s0 | e0)) | (q1 & ~(s1 | e1)) | (x0 & c0) | (x1 & c1) |
                             x1 >> 63;  // last bit of the prefix XOR = odd quote count
        if (bad) continue;
        // Exactly N-1 commas: take the next from c0, else c1 (cmov, fixed trip count).
        uint64_t m0 = c0, m1 = c1;
        uint32_t s = 0;
        for (int k = 0; k < N; k++) {
            uint32_t end;
            if (k == N - 1) {
                end = L;
            } else {
                const bool lo = m0 != 0;
                end = lo ? (uint32_t)_tzcnt_u64(m0) : 64 + (uint32_t)_tzcnt_u64(m1);
                m1 = lo ? m1 : _blsr_u64(m1);
                m0 = _blsr_u64(m0);
            }
            // a quote at a field's first byte: quoted field (from the mask: line[s] may be past e)
            const uint32_t qt = (uint32_t)((s < 64 ? q0 : q1) >> (s & 63) & 1) & (end > s);
            fp[k] = line + s + qt;
            fl[k] = end - s - 2 * qt;
            s = end + 1;
        }
        f(fp, fl);
    }
#else
    for_lines(p, e, [&](const char* b, const char* le) {
        if (split<N>(b, le, fp, fl)) f(fp, fl);
    });
#endif
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
