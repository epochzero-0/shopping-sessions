// Shared types and the interface between pipeline stages.
#pragma once
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "hash.hpp"

using std::string;
using std::vector;

static const int64_t DAY_MS = 86400000LL;
static const int64_t GAP_MS = 1800000LL;
static const int64_t WEEK_MS = 7 * DAY_MS;
static const int PBITS = 8;  // 256 hash partitions
static const int NPART = 1 << PBITS;

void phase(const char* name);

template <class F>
static void parallel(int n, F f) {
    vector<std::thread> ts;
    for (int t = 0; t < n; t++) ts.emplace_back(f, t);
    for (auto& t : ts) t.join();
}

// ---- memory (main.cpp)
// Zeroed, lazily committed, transparent-huge-page backed.
void* big_alloc(size_t bytes);
void big_free(void* p, size_t bytes);

// Shared bump allocator handing out fixed-size blocks to BLists. Thread-safe; spills to malloc when full.
struct Arena {
    char* base = nullptr;
    size_t cap = 0;
    std::atomic<size_t> used{0};
    std::atomic<size_t> spilled{0};
    void init(size_t bytes) {
        cap = bytes;
        base = (char*)big_alloc(bytes);
    }
    void* take(size_t bytes) {
        size_t o = used.fetch_add(bytes, std::memory_order_relaxed);
        if (o + bytes <= cap) return base + o;
        spilled.fetch_add(1, std::memory_order_relaxed);
        return malloc(bytes);  // rare; leaked until exit
    }
    ~Arena() {
        if (base) big_free(base, cap);
    }
};

// Append-only list of T in 64 KB blocks from an Arena: no reallocation, no copying on growth.
template <class T>
struct BList {
    static constexpr uint32_t B = (1u << 16) / sizeof(T);
    vector<T*> blocks;
    T* cur = nullptr;
    uint32_t last = B;  // count in the last block
    void push(Arena& A, const T& x) {
        if (__builtin_expect(last == B, 0)) {
            cur = (T*)A.take(B * sizeof(T));
            blocks.push_back(cur);
            last = 0;
        }
        cur[last++] = x;
    }
    size_t size() const { return blocks.empty() ? 0 : (blocks.size() - 1) * B + last; }
    template <class F>
    void each(F f) const {
        for (size_t k = 0; k < blocks.size(); k++) {
            T* p = blocks[k];
            uint32_t n = k + 1 == blocks.size() ? last : B;
            for (uint32_t i = 0; i < n; i++) f(p[i]);
        }
    }
    template <class F>
    void each_block(F f) const {
        for (size_t k = 0; k < blocks.size(); k++) f(blocks[k], k + 1 == blocks.size() ? last : B);
    }
};

struct Chunk {
    const char* b;
    const char* e;
    int kind;  // 0 events, 1 clicks, 2 links
};

// Everything the stages read: window, small tables, input chunks.
struct Ctx {
    int T = 8;
    int64_t ws = 0, we = 0, day0 = 0;
    int ndays = 0;
    StrMap prodmap;
    vector<uint8_t> prodcat;
    vector<string> catnames;
    uint8_t unknown_cat = 0;
    int NC = 0;
    StrMap campmap;
    vector<string> campid, campch;
    uint32_t NCAMP = 0;  // index NCAMP = organic
    vector<Chunk> link_chunks, main_chunks;
    size_t ev_bytes = 0, ck_bytes = 0;
};

// ---- links.cpp
// User keys: pack_id(ID) when the ID is in that form, else FBIT | its number in FTab (exact compare). Rare in practice
// (upper-cased IDs), so FTab is a lazily touched reservation.
struct FTab {
    struct Slot {
        std::atomic<uint64_t> h;
        std::atomic<uint32_t> id1;  // id + 1, 0 while the inserting thread is still writing
        uint32_t pad;
    };
    Slot* s = nullptr;
    size_t mask = 0;
    const char** name_p = nullptr;
    uint32_t* name_l = nullptr;
    std::atomic<uint32_t> next{0};
    uint32_t cap = 0;
    void init(size_t n);
    ~FTab();
    uint32_t get(const char* p, uint32_t n) {
        uint64_t h = hash_bytes(p, n);
        if (!h) h = 1;
        for (size_t i = h & mask;; i = (i + 1) & mask) {
            uint64_t cur = s[i].h.load(std::memory_order_relaxed);
            if (cur == 0 && s[i].h.compare_exchange_strong(cur, h, std::memory_order_relaxed)) {
                uint32_t id = next.fetch_add(1, std::memory_order_relaxed);
                if (id >= cap) { fprintf(stderr, "fallback table full\n"); abort(); }
                name_p[id] = p;
                name_l[id] = n;
                s[i].id1.store(id + 1, std::memory_order_release);
                return id;
            }
            if (cur == h) {
                uint32_t id1;
                while (!(id1 = s[i].id1.load(std::memory_order_acquire))) {
                }
                if (same(name_p[id1 - 1], name_l[id1 - 1], p, n)) return id1 - 1;
            }
        }
    }
    uint64_t key(const char* p, uint32_t n) {
        uint64_t k = pack_id(p, n);
        return k ? k : FBIT | get(p, n);
    }
};

// Link IDs only: user key -> the key of its person's name (the byte-wise smallest ID). A key not in the table is a
// person of its own. While building, val holds node id + 1.
struct Links {
    struct Slot {
        std::atomic<uint64_t> key;
        std::atomic<uint64_t> val;
    };
    Slot* s = nullptr;
    size_t mask = 0;
    int shift = 64;
    FTab F;
    void init(size_t n);
    ~Links();
    size_t idx(uint64_t k) const { return (size_t)((k * 0xD6E8FEB86659FD93ull) >> shift); }
    const void* at(uint64_t k) const { return &s[idx(k)]; }
    uint64_t person(uint64_t k) const {
        for (size_t i = idx(k);; i = (i + 1) & mask) {
            uint64_t x = s[i].key.load(std::memory_order_relaxed);
            if (x == k) return s[i].val.load(std::memory_order_relaxed);
            if (!x) return k;
        }
    }
    // the person's name: unpacked into buf, or the fallback table's bytes
    const char* name(uint64_t k, char* buf, uint32_t& n) const {
        if (k & FBIT) {
            n = F.name_l[(uint32_t)k];
            return F.name_p[(uint32_t)k];
        }
        n = unpack_id(k, buf);
        return buf;
    }
};
static inline uint32_t kpart(uint64_t k) { return (uint32_t)((k * 0x9E3779B97F4A7C15ull) >> (64 - PBITS)); }
// est_users: an upper bound on distinct user IDs outside links
void build_links(const Ctx& c, Links& L, size_t est_users);

// ---- events.cpp
struct SessEv {
    uint32_t ts;   // ms since window_start
    uint32_t pc;   // price | cat << 24; price 0 for view and cart, >= 1 for a purchase
    uint64_t key;  // person
};

struct ClickRec {
    uint32_t ts;   // ms since window_start - 7 days
    uint32_t camp;
    uint32_t cidlen;
    uint32_t pad;
    uint64_t key;  // person
    const char* cid;
};
static const uint32_t PRICE_MASK = 0xFFFFFF;

// [thread][person partition], blocks in `arena`
struct EvParts {
    Arena arena;
    vector<vector<BList<SessEv>>> p;
};
struct CkParts {
    Arena arena;
    vector<vector<BList<ClickRec>>> p;
};
void parse_and_dedup(const Ctx& c, Links& L, EvParts& sp, CkParts& ckp);

// ---- sessions.cpp
struct URow {
    uint64_t key;  // person
    int32_t day;
    int64_t sessions, events, revenue;
    int32_t topcat;  // -1 null
};

struct Agg {
    vector<vector<URow>> urows;                          // [thread]
    vector<vector<int64_t>> c_sess, c_purch, c_rev, c_assist;  // [thread][campaign * ndays + day]
};
void sessions(const Ctx& c, EvParts& sp, CkParts& ckp, Agg& out);
