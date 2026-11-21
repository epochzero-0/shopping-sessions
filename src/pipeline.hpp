// Shared types and the interface between pipeline stages.
#pragma once
#include <cstdint>
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
struct Links {
    StrMap map;
    vector<uint32_t> parent;  // after build: the root of every node
    vector<const char*> nodep;
    vector<uint32_t> nodel;
    vector<uint64_t> personh;  // per node: hash of its person's name
    // user -> (person hash, name)
    void person_of(uint64_t uh, const char* p, uint32_t n, uint64_t& ph, const char*& name, uint32_t& nl) const {
        int64_t id = map.find(uh, p, n);
        if (id < 0) {
            ph = uh, name = p, nl = n;
        } else {
            uint32_t r = parent[id];
            ph = personh[id], name = nodep[r], nl = nodel[r];
        }
    }
};
void build_links(const Ctx& c, Links& L);

// ---- events.cpp
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

// [thread][person partition]
using EvParts = vector<vector<vector<SessEv>>>;
using CkParts = vector<vector<vector<ClickRec>>>;
void parse_and_dedup(const Ctx& c, const Links& L, EvParts& sp, CkParts& ckp);

// ---- sessions.cpp
struct URow {
    const char* name;
    uint32_t namelen;
    int32_t day;
    int64_t sessions, events, revenue;
    int32_t topcat;  // -1 null
};

struct Agg {
    vector<vector<URow>> urows;                          // [thread]
    vector<vector<int64_t>> c_sess, c_purch, c_rev, c_assist;  // [thread][campaign * ndays + day]
};
void sessions(const Ctx& c, EvParts& sp, CkParts& ckp, Agg& out);
