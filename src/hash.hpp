// Hashing, byte comparison and a small open-addressing string map.
#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#ifdef __AVX512BW__
#include <immintrin.h>
#endif

// Exact 64-bit key of an ID of 1..10 bytes from [0-9a-z]: byte i becomes a 6-bit code (digit 1..10, letter 11..36) at
// bit 6i. Codes are nonzero, so the length is implicit: key != 0, key < 2^60, one key per ID. 0 = not in that form.
static inline uint64_t pack_id_ref(const char* p, uint32_t n) {
    if (n - 1 >= 10u) return 0;
    uint64_t k = 0;
    for (uint32_t i = 0; i < n; i++) {
        unsigned c = (unsigned char)p[i], d;
        if (c - '0' < 10u)
            d = c - '0' + 1;
        else if (c - 'a' < 26u)
            d = c - 'a' + 11;
        else
            return 0;
        k |= (uint64_t)d << (6 * i);
    }
    return k;
}

static inline uint64_t pack_id(const char* p, uint32_t n) {
#ifdef __AVX512BW__
    if (n - 1 >= 10u) return 0;
    const __mmask16 m = (__mmask16)_bzhi_u32(0xFFFF, n);
    const __m128i v = _mm_maskz_loadu_epi8(m, p);  // masked: never reads past p + n
    const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
    const __m128i a = _mm_sub_epi8(v, _mm_set1_epi8('a'));
    const __mmask16 isd = _mm_mask_cmplt_epu8_mask(m, d, _mm_set1_epi8(10));
    const __mmask16 isa = _mm_mask_cmplt_epu8_mask(m, a, _mm_set1_epi8(26));
    if ((__mmask16)(isd | isa) != m) return 0;
    __m128i c = _mm_maskz_add_epi8(isd, d, _mm_set1_epi8(1));
    c = _mm_mask_add_epi8(c, isa, a, _mm_set1_epi8(11));
    const uint64_t lo = (uint64_t)_mm_cvtsi128_si64(c);
    const uint32_t hi = (uint32_t)_mm_extract_epi16(c, 4);
    return _pext_u64(lo, 0x3F3F3F3F3F3F3F3Full) | (uint64_t)_pext_u32(hi, 0x3F3F) << 48;
#else
    return pack_id_ref(p, n);
#endif
}

// Inverse of pack_id; out needs 10 bytes. Returns the length.
static inline uint32_t unpack_id(uint64_t k, char* out) {
    uint32_t n = 0;
    for (; k; k >>= 6) {
        unsigned d = k & 63;
        out[n++] = (char)(d <= 10 ? '0' + d - 1 : 'a' + d - 11);
    }
    return n;
}

// Keys with this bit are not packed IDs: fallback tables number them.
static const uint64_t FBIT = 1ull << 63;

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

// Independent second hash: with hash_bytes it identifies a string by 96 bits, no byte compare needed.
static inline uint32_t hash2(const char* p, size_t n) {
    const uint64_t k0 = 0x9fb21c651e98df25ull, k1 = 0xc2b2ae3d27d4eb4full, k2 = 0x165667b19e3779f9ull;
    uint64_t h = 0x452821e638d01377ull ^ (n * k0);
    while (n > 16) {
        uint64_t a, b;
        memcpy(&a, p, 8);
        memcpy(&b, p + 8, 8);
        h = mix(a ^ k1 ^ h, b ^ k2);
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
    return (uint32_t)(mix(mix(a ^ k1 ^ h, b ^ k2), n ^ k0) >> 17);
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
    std::vector<uint64_t> h;
    std::vector<uint32_t> v;  // value + 1, 0 = empty
    std::vector<const char*> sp;
    std::vector<uint32_t> sl;
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
