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
// user_id -> dense person id, concurrent. Link IDs are inserted first (ids 0..nodes-1) and point at their
// union-find root; any other user is interned on first sight as a person of its own. names[pid] is the
// person's name for every pid that is a root or an unlinked user.
struct Persons {
    struct Slot {
        std::atomic<uint64_t> h;
        std::atomic<uint32_t> h2;
        std::atomic<uint32_t> id1;  // pid + 1, 0 while the inserting thread is still writing
    };
    Slot* s = nullptr;
    size_t mask = 0;
    const char** name_p = nullptr;
    uint32_t* name_l = nullptr;
    std::atomic<uint32_t> next{0};
    uint32_t cap = 0;

    void init(size_t n);
    ~Persons();
    // pid of a user, inserting it with a fresh pid if missing; `slot` receives the slot index
    uint32_t get(const char* p, uint32_t n, size_t* slot = nullptr) {
        uint64_t h = hash_bytes(p, n);
        uint32_t h2v = hash2(p, n);
        if (!h) h = 1;
        size_t i = h & mask;
        for (;;) {
            uint64_t cur = s[i].h.load(std::memory_order_relaxed);
            if (cur == 0) {
                if (s[i].h.compare_exchange_strong(cur, h, std::memory_order_relaxed)) {
                    uint32_t id = next.fetch_add(1, std::memory_order_relaxed);
                    if (id >= cap) { fprintf(stderr, "person table full\n"); abort(); }
                    name_p[id] = p;
                    name_l[id] = n;
                    s[i].h2.store(h2v, std::memory_order_relaxed);
                    s[i].id1.store(id + 1, std::memory_order_release);
                    if (slot) *slot = i;
                    return id;
                }
            }
            if (cur == h) {
                uint32_t id1;
                while (!(id1 = s[i].id1.load(std::memory_order_acquire))) {
                }
                if (s[i].h2.load(std::memory_order_relaxed) == h2v) {
                    if (slot) *slot = i;
                    return id1 - 1;
                }
            }
            i = (i + 1) & mask;
        }
    }
};
static inline uint32_t ppart(uint32_t pid) { return (pid * 0x9E3779B1u) >> (32 - PBITS); }
// est_users: an upper bound on distinct user IDs outside links
void build_links(const Ctx& c, Persons& P, size_t est_users);

// ---- events.cpp
struct SessEv {
    uint32_t ts;   // ms since window_start
    uint32_t pid;
    uint32_t pc;   // price | cat << 24; price 0 for view and cart, >= 1 for a purchase
};

struct ClickRec {
    uint32_t ts;   // ms since window_start - 7 days
    uint32_t pid;
    uint32_t camp;
    uint32_t cidlen;
    const char* cid;
};
static const uint32_t PRICE_MASK = 0xFFFFFF;

// [thread][person partition]
using EvParts = vector<vector<vector<SessEv>>>;
using CkParts = vector<vector<vector<ClickRec>>>;
void parse_and_dedup(const Ctx& c, Persons& P, EvParts& sp, CkParts& ckp);

// ---- sessions.cpp
struct URow {
    uint32_t pid;
    int32_t day;
    int64_t sessions, events, revenue;
    int32_t topcat;  // -1 null
};

struct Agg {
    vector<vector<URow>> urows;                          // [thread]
    vector<vector<int64_t>> c_sess, c_purch, c_rev, c_assist;  // [thread][campaign * ndays + day]
};
void sessions(const Ctx& c, EvParts& sp, CkParts& ckp, Agg& out);
