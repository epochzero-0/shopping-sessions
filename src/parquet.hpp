// Minimal Parquet writer: PLAIN, uncompressed, one row group, one page per column.
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
};

static inline void put32(std::string& s, uint32_t v) { s.append((const char*)&v, 4); }
static inline void put64(std::string& s, uint64_t v) { s.append((const char*)&v, 8); }

static inline void bytes_val(std::string& s, const char* p, uint32_t n) {
    put32(s, n);
    s.append(p, n);
}

static inline Col col_i64(const std::string& name) { return Col{name, P_INT64, false, -1, 0, {}}; }

static void write_parquet(const std::string& path, std::vector<Col>& cols, int64_t nrows) {
    std::string out = "PAR1";
    std::vector<int64_t> offs, sizes;
    for (auto& c : cols) {
        Thrift ph;
        ph.i32(1, 0);
        ph.i32(2, (int32_t)c.data.size());
        ph.i32(3, (int32_t)c.data.size());
        ph.open(5);
        ph.i32(1, (int32_t)nrows);
        ph.i32(2, 0);
        ph.i32(3, 3);
        ph.i32(4, 3);
        ph.close();
        ph.byte(0);
        offs.push_back(out.size());
        out += ph.b;
        out += c.data;
        sizes.push_back(ph.b.size() + c.data.size());
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
            m.i64(2, offs[i]);
            m.open(3);
            m.i32(1, cols[i].ptype);
            m.lst(2, 5, 2);
            m.zz(0);
            m.zz(3);
            m.lst(3, 8, 1);
            m.varint(cols[i].name.size());
            m.b += cols[i].name;
            m.i32(4, 0);
            m.i64(5, nrows);
            m.i64(6, sizes[i]);
            m.i64(7, sizes[i]);
            m.i64(9, offs[i]);
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
