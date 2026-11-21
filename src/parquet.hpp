// Minimal Parquet writer: uncompressed, one row group, one data page per column, PLAIN, DELTA_BYTE_ARRAY or
// dictionary (PLAIN dictionary page + RLE_DICTIONARY data page).
#pragma once
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

struct Thrift {
    std::string b;
    std::vector<int16_t> st;
    int16_t last = 0;
    void byte(uint8_t x) { b.push_back((char)x); }
    void varint(uint64_t v) {
        while (v >= 0x80) {
            byte((uint8_t)((v & 0x7f) | 0x80));
            v >>= 7;
        }
        byte((uint8_t)v);
    }
    void zz(int64_t v) { varint(((uint64_t)v << 1) ^ (uint64_t)(v >> 63)); }
    void fh(int16_t id, uint8_t type) {
        int d = id - last;
        if (d > 0 && d <= 15)
            byte((uint8_t)((d << 4) | type));
        else {
            byte(type);
            zz(id);
        }
        last = id;
    }
    void i32(int16_t id, int32_t v) { fh(id, 5), zz(v); }
    void i64(int16_t id, int64_t v) { fh(id, 6), zz(v); }
    void str(int16_t id, const std::string& s) { fh(id, 8), varint(s.size()), b += s; }
    void lst(int16_t id, uint8_t et, int n) {
        fh(id, 9);
        if (n < 15)
            byte((uint8_t)((n << 4) | et));
        else {
            byte(0xF0 | et);
            varint(n);
        }
    }
    void open(int16_t id) { fh(id, 12), push(); }
    void push() { st.push_back(last), last = 0; }  // struct begins (also list elements)
    void close() { byte(0), last = st.back(), st.pop_back(); }
};

enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTES = 6 };

struct Col {
    std::string name;
    int ptype;
    bool optional;
    int conv;     // -1 none, 0 UTF8, 6 DATE
    int logical;  // 0 none, 1 STRING, 6 DATE
    std::string data;  // page body
    std::string dict;  // PLAIN dictionary page body; used when ndict > 0, and then `data` holds the indices
    int32_t ndict = 0;
    int enc = 0;  // data page encoding without a dictionary: 0 PLAIN, 7 DELTA_BYTE_ARRAY
};

static inline void put32(std::string& s, uint32_t v) { s.append((const char*)&v, 4); }
static inline void put64(std::string& s, uint64_t v) { s.append((const char*)&v, 8); }

static inline void put_varint(std::string& s, uint64_t v) {
    while (v >= 0x80) s.push_back((char)((v & 0x7f) | 0x80)), v >>= 7;
    s.push_back((char)v);
}

// Bits for dictionary indices 0..n-1 (at least 1).
static inline int index_width(uint32_t n) {
    int b = 1;
    while (b < 32 && (1u << b) < n) b++;
    return b;
}

// RLE / bit-packed hybrid of v[0..n) at bw bits: runs of >= 8 equal values as RLE, the rest in bit-packed groups
// of 8 (at most 63 per run, zero-padded at the end).
static void rle_hybrid(std::string& s, const uint32_t* v, size_t n, int bw) {
    const int vb = (bw + 7) / 8;
    size_t i = 0;
    while (i < n) {
        size_t r = i + 1;
        while (r < n && v[r] == v[i]) r++;
        if (r - i >= 8) {
            put_varint(s, (uint64_t)(r - i) << 1);
            for (int k = 0; k < vb; k++) s.push_back((char)(v[i] >> (8 * k)));
            i = r;
            continue;
        }
        const size_t hdr = s.size();
        s.push_back(0);
        uint32_t g = 0;
        for (;;) {
            uint64_t acc = 0;
            int nb = 0;
            for (int k = 0; k < 8; k++) {
                acc |= (uint64_t)(i + k < n ? v[i + k] : 0) << nb;
                for (nb += bw; nb >= 8; nb -= 8) s.push_back((char)acc), acc >>= 8;
            }
            i += 8, g++;
            if (i >= n || g == 63) break;
            size_t r2 = i + 1;
            while (r2 < n && r2 - i < 8 && v[r2] == v[i]) r2++;
            if (r2 - i >= 8) break;
        }
        s[hdr] = (char)((g << 1) | 1);
    }
}

// DELTA_BINARY_PACKED of v[0..n): blocks of 128 values in 4 miniblocks of 32, each miniblock bit-packed at its own
// width over (delta - the block's min delta); the last miniblock is zero-padded, empty ones have width 0.
static void delta_packed(std::string& s, const int32_t* v, size_t n) {
    auto zz = [](int64_t x) { return ((uint64_t)x << 1) ^ (uint64_t)(x >> 63); };
    put_varint(s, 128);
    put_varint(s, 4);
    put_varint(s, n);
    put_varint(s, zz(n ? v[0] : 0));
    for (size_t i = 1; i < n; i += 128) {
        const size_t m = n - i < 128 ? n - i : 128;
        int64_t d[128], mn = INT64_MAX;
        for (size_t k = 0; k < m; k++) {
            d[k] = (int64_t)v[i + k] - v[i + k - 1];
            if (d[k] < mn) mn = d[k];
        }
        put_varint(s, zz(mn));
        uint64_t u[128] = {};
        uint8_t w[4] = {};
        for (size_t k = 0; k < m; k++) {
            u[k] = (uint64_t)(d[k] - mn);
            while (w[k / 32] < 64 && (u[k] >> w[k / 32])) w[k / 32]++;
        }
        s.append((const char*)w, 4);
        for (size_t j = 0; j * 32 < m; j++) {
            unsigned __int128 acc = 0;
            int nb = 0;
            for (size_t k = 32 * j; k < 32 * j + 32; k++) {
                acc |= (unsigned __int128)u[k] << nb;
                for (nb += w[j]; nb >= 8; nb -= 8) s.push_back((char)acc), acc >>= 8;
            }
        }
    }
}

// DELTA_BYTE_ARRAY page body from per-value prefix lengths, suffix lengths and the concatenated suffixes.
static inline void delta_bytes(std::string& s, const std::vector<int32_t>& pre, const std::vector<int32_t>& suf,
                               const std::string& bytes) {
    delta_packed(s, pre.data(), pre.size());
    delta_packed(s, suf.data(), suf.size());
    s += bytes;
}

// Integer value -> dictionary index, dictionary page body built in first-seen order (vb bytes per value).
struct IntDict {
    std::vector<int64_t> k;
    std::vector<uint32_t> v;  // index + 1, 0 empty
    std::string dict;
    uint32_t n = 0;
    size_t mask = 0;
    int vb;
    explicit IntDict(int bytes) : vb(bytes) { init(1024); }
    void init(size_t cap) {
        k.assign(cap, 0);
        v.assign(cap, 0);
        mask = cap - 1;
    }
    static size_t slot(int64_t x, size_t mask) {
        const uint64_t h = (uint64_t)x * 0x9E3779B97F4A7C15ull;
        return (size_t)(h ^ (h >> 32)) & mask;
    }
    uint32_t get(int64_t x) {
        for (size_t i = slot(x, mask);; i = (i + 1) & mask) {
            if (!v[i]) {
                if ((size_t)(n + 1) * 2 > mask + 1) {
                    grow();
                    return get(x);
                }
                k[i] = x, v[i] = ++n;
                dict.append((const char*)&x, vb);
                return n - 1;
            }
            if (k[i] == x) return v[i] - 1;
        }
    }
    void grow() {
        std::vector<int64_t> ok;
        std::vector<uint32_t> ov;
        ok.swap(k), ov.swap(v);
        init((mask + 1) * 2);
        for (size_t i = 0; i < ok.size(); i++)
            if (ov[i])
                for (size_t j = slot(ok[i], mask);; j = (j + 1) & mask)
                    if (!v[j]) {
                        k[j] = ok[i], v[j] = ov[i];
                        break;
                    }
    }
};

// Fill a dictionary column from its indices; `data` may already hold the definition levels.
static inline void set_dict(Col& c,std::string&& dict, uint32_t ndict, const std::vector<uint32_t>& idx) {
    c.dict = std::move(dict);
    c.ndict = (int32_t)ndict;
    const int bw = index_width(ndict);
    c.data.push_back((char)bw);
    rle_hybrid(c.data, idx.data(), idx.size(), bw);
}

static inline void bytes_val(std::string& s, const char* p, uint32_t n) {
    put32(s, n);
    s.append(p, n);
}

static inline Col col_i64(const std::string& name) { return Col{name, P_INT64, false, -1, 0, {}}; }

static void write_parquet(const std::string& path, std::vector<Col>& cols, int64_t nrows) {
    std::string out = "PAR1";
    std::vector<int64_t> offs, dict_offs, sizes;
    for (auto& c : cols) {
        const size_t begin = out.size();
        dict_offs.push_back((int64_t)begin);
        if (c.ndict) {
            Thrift dh;
            dh.i32(1, 2);  // DICTIONARY_PAGE
            dh.i32(2, (int32_t)c.dict.size());
            dh.i32(3, (int32_t)c.dict.size());
            dh.open(7);
            dh.i32(1, c.ndict);
            dh.i32(2, 0);  // PLAIN
            dh.close();
            dh.byte(0);
            out += dh.b;
            out += c.dict;
            std::string().swap(c.dict);
        }
        Thrift ph;
        ph.i32(1, 0);
        ph.i32(2, (int32_t)c.data.size());
        ph.i32(3, (int32_t)c.data.size());
        ph.open(5);
        ph.i32(1, (int32_t)nrows);
        ph.i32(2, c.ndict ? 8 : c.enc);  // RLE_DICTIONARY, else PLAIN or DELTA_BYTE_ARRAY
        ph.i32(3, 3);
        ph.i32(4, 3);
        ph.close();
        ph.byte(0);
        offs.push_back(out.size());
        out += ph.b;
        out += c.data;
        sizes.push_back((int64_t)(out.size() - begin));
        std::string().swap(c.data);
    }
    Thrift m;
    m.i32(1, 1);
    m.lst(2, 12, (int)cols.size() + 1);
    m.push();
    m.str(4, "schema");
    m.i32(5, (int32_t)cols.size());
    m.close();
    for (auto& c : cols) {
        m.push();
        m.i32(1, c.ptype);
        m.i32(3, c.optional ? 1 : 0);
        m.str(4, c.name);
        if (c.conv >= 0) m.i32(6, c.conv);
        if (c.logical) {
            m.open(10);
            m.open((int16_t)c.logical);
            m.close();
            m.close();
        }
        m.close();
    }
    m.i64(3, nrows);
    if (nrows > 0) {
        int64_t total = 0;
        for (auto s : sizes) total += s;
        m.lst(4, 12, 1);
        m.push();
        m.lst(1, 12, (int)cols.size());
        for (size_t i = 0; i < cols.size(); i++) {
            m.push();
            m.i64(2, dict_offs[i]);
            m.open(3);
            m.i32(1, cols[i].ptype);
            if (cols[i].ndict) {
                m.lst(2, 5, 3);
                m.zz(0);
                m.zz(3);
                m.zz(8);
            } else {
                m.lst(2, 5, 2);
                m.zz(cols[i].enc);
                m.zz(3);
            }
            m.lst(3, 8, 1);
            m.varint(cols[i].name.size());
            m.b += cols[i].name;
            m.i32(4, 0);
            m.i64(5, nrows);
            m.i64(6, sizes[i]);
            m.i64(7, sizes[i]);
            m.i64(9, offs[i]);
            if (cols[i].ndict) m.i64(11, dict_offs[i]);
            m.close();
            m.close();
        }
        m.i64(2, total);
        m.i64(3, nrows);
        m.close();
    } else {
        m.lst(4, 12, 0);
    }
    m.str(6, "infinity");
    m.byte(0);
    out += m.b;
    put32(out, (uint32_t)m.b.size());
    out += "PAR1";
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(1); }
    fwrite(out.data(), 1, out.size(), fp);
    fclose(fp);
}
