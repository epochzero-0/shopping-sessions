#!/usr/bin/env python3
"""Expected results for tests/csv_test.cpp, computed by ref/reference.py itself.

Usage: csv_cases.py OUT_FILE.  Case lines (all byte strings hex-encoded, '-' = null, '.' = empty):
  S N LINE ok|bad F1 .. FN    parse_line(LINE, N)
  T FIELD ok|bad MS           parse_ts(FIELD)
  L BLOB K L1 .. LK           lines of BLOB (split on \\n, a trailing \\n ends the last line)
Sources: every line of every fixture CSV, hand-written hostile lines, seeded random fuzz.
"""
import glob
import os
import random
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "ref"))
import reference as R  # noqa: E402

rng = random.Random(20261121)


def h(b):
    return b.hex() if b else "."


def blob_lines(data):
    if not data:
        return []
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()
    return lines


# ---------------------------------------------------------------- lines

lines = []  # (nfields, line)
blobs = []
for path in sorted(glob.glob(os.path.join(ROOT, "fixtures", "*", "input", "**", "*.csv"), recursive=True)):
    with open(path, "rb") as f:
        data = f.read()
    blobs.append(data)
    for ln in blob_lines(data):
        for k in (2, 4, 7):  # each line under every arity, not only its own
            lines.append((k, ln))

HOSTILE = [
    b"", b",", b'"', b'""', b'"""', b'""""', b'"a"b', b'a"b', b'a"', b'"a', b'"a,b"', b'"a""b"',
    b"a\r", b'"a"\r', b'"a\r"', b"\r", b"\r\r", b" ", b'" "', b'"",""', b'a,"",b', b"\x00", b"a\x00b",
    "é,ü".encode(), b"\t,\t", b",,", b",,,", b",,,,,,", b",,,,,,,", b"a,b,c,d,e,f,g", b"a,b,c,d,e,f,g,",
    b",a,b,c,d,e,f", b'"e1","u1","2026-10-03T10:00:00Z","2026-10-03T10:01:00Z","view","p1",""',
    b'e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,"1000"\r',
    b'e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,"1,000"',
    b'e1,"u1,u2",2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1', b'"' + b"x" * 300 + b'"',
    b"x" * 300 + b",y", b'k1,u1,c_1,"2026-10-03T09:00:00Z"', b'k1,u1,c_1,2026-10-03T09:00:00Z"',
    b'"k1,u1,c_1,2026-10-03T09:00:00Z', b'u1,"u2', b'"u1",u2"', b'"u1""u2"', b'""u1', b'u1""',
]
for ln in HOSTILE:
    for k in (2, 4, 7):
        lines.append((k, ln))

ALPHA = [b"a", b"b", b",", b",", b",", b'"', b'"', b"\r", b" ", b"\t", b"\x00", b"\xc3\xa9", b"1"]
for _ in range(30000):
    k = rng.choice((2, 4, 7))
    if rng.random() < 0.5:  # mostly well-formed shape, then mutate
        fields = []
        for _ in range(k):
            v = bytes(rng.choice(b"ab1 \r\t") for _ in range(rng.randrange(0, 4)))
            fields.append(b'"' + v + b'"' if rng.random() < 0.3 else v)
        ln = bytearray(b",".join(fields))
        for _ in range(rng.randrange(0, 3)):
            if ln:
                ln[rng.randrange(len(ln))] = rng.choice(b'",\r a')
        ln = bytes(ln)
    else:
        ln = b"".join(rng.choice(ALPHA) for _ in range(rng.randrange(0, 25)))
    lines.append((k, ln))

# ---------------------------------------------------------------- timestamps

ts = set()
for _, ln in lines:
    ts.update(ln.split(b","))
    ts.update(f[1:-1] for f in ln.split(b",") if len(f) >= 2)
BASE = [
    b"2026-10-03T10:00:00Z", b"2026-10-04T01:30:00+02:00", b"2026-10-03T10:00:00-00:00",
    b"2024-02-29T00:00:00Z", b"2100-02-29T00:00:00Z", b"2000-02-29T00:00:00Z", b"2026-02-29T00:00:00Z",
    b"2026-00-10T00:00:00Z", b"2026-13-10T00:00:00Z", b"2026-04-31T00:00:00Z", b"2026-10-00T00:00:00Z",
    b"2026-10-32T00:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T23:60:00Z", b"2026-10-03T23:59:60Z",
    b"2026-10-03T10:00:00+24:00", b"2026-10-03T10:00:00+23:59", b"2026-10-03T10:00:00+05:60",
    b"2026-10-03T10:00:00+0200", b"2026-10-03T10:00:00+02:00Z", b"2026-10-03t10:00:00z",
    b"2026-10-03 10:00:00Z", b"0000-01-01T00:00:00Z", b"9999-12-31T23:59:59+23:59",
    b"9999-12-31T23:59:59-23:59", b"2026-10-03T10:00:00.000Z", b"2026-10-03T10:00:00Z\r",
    b"+026-10-03T10:00:00Z", b"-026-10-03T10:00:00Z", b"2026-1-03T10:00:00Z", "２026-10-03T10:00:00Z".encode(), "٢٥٠".encode(), "1791071890000".encode(), b"1791071890000.0",
    b"0", b"00000000000000000000000000001", b"9223372036854775807", b"9223372036854775808",
    b"18446744073709551615", b"18446744073709551616", b"99999999999999999999", b"1" * 40,
    b"12345678901234567890", b"12345678901234567890123", b"-1", b"+1", b" 1", b"1 ", b"1\r", b"",
]
ts.update(BASE)
for _ in range(20000):
    b = bytearray(rng.choice(BASE[:3] + BASE[22:25]))
    for _ in range(rng.randrange(1, 3)):
        b[rng.randrange(len(b))] = rng.choice(b"0123456789-T:Z+ \r9")
    ts.add(bytes(b))
for _ in range(3000):
    ts.add(bytes(rng.choice(b"0123456789") for _ in range(rng.randrange(1, 26))))

# ---------------------------------------------------------------- blobs

for b in (b"", b"\n", b"\n\n", b"a", b"a\n", b"a\nb", b"a\n\nb\n", b"a\r\nb\r\n", b"\r\n", b"a\n\r"):
    blobs.append(b)
for _ in range(3000):
    blobs.append(bytes(rng.choice(b"ab\n\n\r,") for _ in range(rng.randrange(0, 30))))

with open(sys.argv[1], "w", newline="\n") as out:
    for k, ln in lines:
        r = R.parse_line(ln, k)
        if r is None:
            out.write(f"S {k} {h(ln)} bad\n")
        else:
            out.write(f"S {k} {h(ln)} ok " + " ".join("-" if f is None else h(f) for f in r) + "\n")
    for b in sorted(ts):
        v = R.parse_ts(b if b else None)
        out.write(f"T {h(b)} " + ("bad" if v is None else f"ok {v}") + "\n")
    for b in blobs:
        ls = blob_lines(b)
        out.write(f"L {h(b)} {len(ls)} " + " ".join(h(x) for x in ls) + "\n")
