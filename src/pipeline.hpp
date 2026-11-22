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
#ifdef __AVX512F__
#include <immintrin.h>
#endif

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
        return aligned_alloc(64, bytes);  // rare; leaked until exit. 64 B aligned for WCLists
    }
    ~Arena() {
        if (base) big_free(base, cap);
    }
};

// One thread's 64 KB blocks freed by an earlier stage, handed out before fresh (kernel-zeroed) arena pages.
struct Recycler {
    Arena* A;
    vector<void*> free;
    void* take(size_t bytes) {
        if (free.empty()) return A->take(bytes);
        void* p = free.back();
        free.pop_back();
        return p;
    }
};

// Append-only list of T in 64 KB blocks from an Arena: no reallocation, no copying on growth.
template <class T>
struct BList {
    static constexpr uint32_t B = (1u << 16) / sizeof(T);
    vector<T*> blocks;
    T* cur = nullptr;
    uint32_t last = B;  // count in the last block
    template <class Alloc>  // Arena or Recycler
    void push(Alloc& A, const T& x) {
        if (__builtin_expect(last == B, 0)) {
            cur = (T*)A.take(B * sizeof(T));
            blocks.push_back(cur);
            last = 0;
        }
        cur[last++] = x;
    }
    // room for k records in one go (B % k == 0, so a 64 B group never straddles blocks)
    T* reserve(Arena& A, uint32_t k) {
        if (__builtin_expect(last == B, 0)) {
            cur = (T*)A.take(B * sizeof(T));
            blocks.push_back(cur);
            last = 0;
        }
        T* p = cur + last;
        last += k;
        return p;
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

// One thread's NPART BLists behind write-combining buffers: records collect in a 64 B line per partition (L1) and
// go out as one streaming store, so the destination line is never read. Record order per partition is unchanged.
// Call flush() once when the thread is done.
template <class T>
struct WCLists {
    static constexpr uint32_t K = 64 / sizeof(T);
    static_assert(64 % sizeof(T) == 0 && BList<T>::B % K == 0, "WCLists");
    struct alignas(64) Line {
        T r[K];
    };
    vector<Line> line;
    vector<uint8_t> n;
    vector<BList<T>>* out;
    Arena* A;
    WCLists(vector<BList<T>>& o, Arena& a) : line(o.size()), n(o.size(), 0), out(&o), A(&a) {}
    void push(uint32_t q, const T& x) {
        line[q].r[n[q]] = x;
        if (++n[q] == K) {
            n[q] = 0;
            T* d = (*out)[q].reserve(*A, K);
#ifdef __AVX512F__
            _mm512_stream_si512((__m512i*)d, _mm512_load_si512(&line[q]));
#else
            memcpy(d, &line[q], 64);
#endif
        }
    }
    void flush() {
        for (size_t q = 0; q < n.size(); q++)
            for (uint32_t i = 0; i < n[q]; i++) (*out)[q].push(*A, line[q].r[i]);
#ifdef __AVX512F__
        _mm_sfence();
#endif
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
// Key kinds (packed keys are < 2^60, so FBIT is free):
//   pack_id(ID)                    an ID in that form, exact
//   FBIT | HBIT | hash_bytes >> 2  any other user ID, identified by 62 bits of hash; no table
//   FBIT | node id                 a linked person named by an ID that does not pack; Links keeps its bytes
static const uint64_t HBIT = 1ull << 62;
static inline uint64_t user_key(const char* p, uint32_t n) {
    const uint64_t k = pack_id(p, n);
    return k ? k : FBIT | HBIT | hash_bytes(p, n) >> 2;
}
static inline bool hashed_key(uint64_t k) { return (k & (FBIT | HBIT)) == (FBIT | HBIT); }

// The bytes of a user ID that starts at p inside a parsed line: it ends at the next ',' or '"', which no value holds.
static inline uint32_t id_len(const char* p) {
    uint32_t n = 0;
    while (p[n] != ',' && p[n] != '"' && p[n] != '\n') n++;
    return n;
}

// Names of persons keyed by a hashed user key: filled from the parse's (key, ID pointer) records after sessions.
struct HNames {
    struct Slot {
        uint64_t key;
        std::atomic<const char*> p;
    };
    vector<Slot> s;
    size_t mask = 0;
    void init(size_t n) {
        size_t c = 16;
        while (c < n * 2) c <<= 1;
        s = vector<Slot>(c);
        mask = c - 1;
    }
    size_t at(uint64_t k) const { return (size_t)((k * 0x9FB21C651E98DF25ull) >> 20) & mask; }
    void add(uint64_t k) {
        for (size_t i = at(k);; i = (i + 1) & mask) {
            if (s[i].key == k) return;
            if (!s[i].key) {
                s[i].key = k;
                return;
            }
        }
    }
    Slot* find(uint64_t k) {
        for (size_t i = at(k);; i = (i + 1) & mask) {
            if (s[i].key == k) return &s[i];
            if (!s[i].key) return nullptr;
        }
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
    const char** np = nullptr;  // node id -> ID bytes
    uint32_t* nl = nullptr;
    size_t ncap = 0;
    HNames H;
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
    // the person's name: unpacked into buf, a link node's bytes, or a hashed key's ID in the input
    const char* name(uint64_t k, char* buf, uint32_t& n) {
        if (hashed_key(k)) {
            const char* p = H.find(k)->p.load(std::memory_order_relaxed);
            n = id_len(p);
            return p;
        }
        if (k & FBIT) {
            n = nl[(uint32_t)k];
            return np[(uint32_t)k];
        }
        n = unpack_id(k, buf);
        return buf;
    }
};
static inline uint32_t kpart(uint64_t k) { return (uint32_t)((k * 0x9E3779B97F4A7C15ull) >> (64 - PBITS)); }
void build_links(const Ctx& c, Links& L);

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

// A valid event's hashed user key and where its ID starts in the input
struct NameRec {
    uint64_t key;
    const char* p;
};

// [thread][person partition], blocks in `arena`
struct EvParts {
    Arena arena;
    Arena evarena;  // the dedup stage's EvRec blocks; dead ones hold SessEv blocks, so it lives as long as p
    vector<vector<BList<SessEv>>> p;
    Arena narena;
    vector<BList<NameRec>> names;  // [thread]
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
