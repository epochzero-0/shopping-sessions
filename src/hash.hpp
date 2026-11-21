// Hashing, byte comparison and a small open-addressing string map.
#pragma once
#include <cstdint>
#include <cstring>
#include <vector>

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
