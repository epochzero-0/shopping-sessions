#!/usr/bin/env python3
"""Correctness oracle for RULES.md. Plain Python, clarity over speed.

Usage: reference.py [DATA_DIR OUTPUT_DIR]   (falls back to $DATA_DIR / $OUTPUT_DIR)
"""
import glob
import json
import os
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
I64_MAX = (1 << 63) - 1
DIGITS = b"0123456789"


# ---------------------------------------------------------------- CSV dialect

def read_lines(path):
    """Data lines of a file: split on b'\\n' only, header (first line) skipped."""
    with open(path, "rb") as f:
        data = f.read()
    if not data:
        return []
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()  # the terminator of the last line, not an empty line
    return lines[1:]


def parse_line(line, nfields):
    """Fields as bytes (None for null), or None if the line is malformed."""
    if not line:
        return None
    raw = line.split(b",")
    if len(raw) != nfields:
        return None
    out = []
    for f in raw:
        if b'"' in f:
            if len(f) < 2 or f[0:1] != b'"' or f[-1:] != b'"' or b'"' in f[1:-1]:
                return None
            f = f[1:-1]
        out.append(f if f else None)
    return out


def read_table(folder, nfields):
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        for line in read_lines(path):
            row = parse_line(line, nfields)
            if row is not None:
                yield row


# ---------------------------------------------------------------- timestamps

def is_digits(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def days_from_civil(y, m, d):
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def days_in_month(y, m):
    if m == 2:
        leap = (y % 4 == 0 and y % 100 != 0) or y % 400 == 0
        return 29 if leap else 28
    return 30 if m in (4, 6, 9, 11) else 31


def parse_ts(b):
    """Epoch ms (clamped to int64 for long F3), or None if malformed."""
    if b is None:
        return None
    if is_digits(b):
        v = int(b)
        return min(v, I64_MAX)
    n = len(b)
    if n not in (20, 25):
        return None
    if not (b[4:5] == b"-" and b[7:8] == b"-" and b[10:11] == b"T" and b[13:14] == b":" and b[16:17] == b":"):
        return None
    parts = (b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19])
    if not all(is_digits(p) for p in parts):
        return None
    y, mo, d, h, mi, s = (int(p) for p in parts)
    if not (1 <= mo <= 12 and 1 <= d <= days_in_month(y, mo) and h <= 23 and mi <= 59 and s <= 59):
        return None
    off = 0
    if n == 20:
        if b[19:20] != b"Z":
            return None
    else:
        sign = b[19:20]
        if sign not in (b"+", b"-") or b[22:23] != b":" or not is_digits(b[20:22]) or not is_digits(b[23:25]):
            return None
        oh, om = int(b[20:22]), int(b[23:25])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        if sign == b"-":
            off = -off
    ms = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000
    return ms - off


# ---------------------------------------------------------------- union-find

class DSU:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        p = self.parent.setdefault(x, x)
        root = x
        while p != root:
            root = p
            p = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            if rb < ra:
                ra, rb = rb, ra
            self.parent[rb] = ra  # root is always the smallest ID of its set


# ---------------------------------------------------------------- pipeline

def main():
    if len(sys.argv) >= 3:
        data, out = sys.argv[-2], sys.argv[-1]
    else:
        data = os.environ.get("DATA_DIR", "/data")
        out = os.environ.get("OUTPUT_DIR", "/output")

    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    win_start = parse_ts(manifest["window_start"].encode())
    win_end = parse_ts(manifest["window_end"].encode())

    categories = {}
    for path in [os.path.join(data, "products.csv")]:
        for line in read_lines(path):
            row = parse_line(line, 2)
            if row is not None and row[0] is not None:
                categories[row[0]] = row[1]
    channels = {}
    for line in read_lines(os.path.join(data, "campaigns.csv")):
        row = parse_line(line, 2)
        if row is not None and row[0] is not None:
            channels[row[0]] = row[1]

    # Links
    dsu = DSU()
    for a, b in read_table(os.path.join(data, "links"), 2):
        if a is None or b is None:
            continue
        dsu.union(a, b)

    def person(uid):
        return dsu.find(uid) if uid in dsu.parent else uid

    # Events: parse, dedup on smallest ingest_ts
    kept = {}
    for row in read_table(os.path.join(data, "events"), 7):
        eid = row[0]
        if eid is None:
            continue
        ing = parse_ts(row[3])
        cur = kept.get(eid)
        if cur is None or ing < cur[0]:
            kept[eid] = (ing, row)

    # Validate, drop late
    by_person = defaultdict(list)  # person -> [(ts, price or 0, is_purchase, category)]
    for ing, (eid, uid, ts_b, _ing_b, etype, pid, price_b) in kept.values():
        if uid is None or pid is None:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (win_start <= ts < win_end):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        price = 0
        purchase = etype == b"purchase"
        if purchase:
            if price_b is None or not is_digits(price_b):
                continue
            price = int(price_b)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > DAY_MS:
            continue
        by_person[person(uid)].append((ts, price, purchase, categories.get(pid, b"unknown")))

    # Clicks: parse, dedup, validate
    clicks = {}
    for cid, uid, camp, ts_b in read_table(os.path.join(data, "clicks"), 4):
        if cid is None or cid in clicks:
            continue
        clicks[cid] = (uid, camp, ts_b)
    clicks_by_person = defaultdict(list)  # person -> [(ts, click_id, campaign)]
    for cid, (uid, camp, ts_b) in clicks.items():
        if uid is None or camp not in channels:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (win_start - WEEK_MS <= ts < win_end):
            continue
        clicks_by_person[person(uid)].append((ts, cid, camp))

    # Sessionize, credit, aggregate
    user_daily = {}  # (person, day) -> [sessions, events, revenue, {cat: rev}]
    camp_daily = defaultdict(lambda: [0, 0, 0, 0])  # (campaign, day) -> [sessions, purch, revenue, assisted]
    for p, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= GAP_MS:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        pclicks = clicks_by_person.get(p, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY_MS
            rev = sum(e[1] for e in s)
            purchased = any(e[2] for e in s)
            u = user_daily.setdefault((p, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for e in s:
                if e[2]:
                    u[3][e[3]] += e[1]
            qual = [c for c in pclicks if start - WEEK_MS <= c[0] <= start]
            if qual:
                latest = max(c[0] for c in qual)
                credit = min(c for c in qual if c[0] == latest)[2]  # tie: smallest click_id
            else:
                credit = b"organic"
            c = camp_daily[(credit, day)]
            c[0] += 1
            c[1] += purchased
            c[2] += rev
            for camp in {c[2] for c in qual}:
                camp_daily[(camp, day)][3] += 1

    # Write
    ud = {"user_id": [], "day": [], "sessions": [], "events": [], "revenue_cents": [], "top_category": []}
    for (p, day), (ns, ne, rev, cats) in user_daily.items():
        top = None
        if cats:
            top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
        ud["user_id"].append(p.decode())
        ud["day"].append(day)
        ud["sessions"].append(ns)
        ud["events"].append(ne)
        ud["revenue_cents"].append(rev)
        ud["top_category"].append(top)
    cd = {"campaign_id": [], "channel": [], "day": [], "sessions": [], "purchasing_sessions": [],
          "revenue_cents": [], "assisted_sessions": []}
    for (camp, day), (ns, npur, rev, nas) in camp_daily.items():
        cd["campaign_id"].append(camp.decode())
        cd["channel"].append("organic" if camp == b"organic" else channels[camp].decode())
        cd["day"].append(day)
        cd["sessions"].append(ns)
        cd["purchasing_sessions"].append(npur)
        cd["revenue_cents"].append(rev)
        cd["assisted_sessions"].append(nas)

    def table(cols):
        arrays, names = [], []
        for k, v in cols.items():
            if k == "day":
                arrays.append(pa.array(v, type=pa.int32()).cast(pa.date32()))
            elif k in ("sessions", "events", "revenue_cents", "purchasing_sessions", "assisted_sessions"):
                arrays.append(pa.array(v, type=pa.int64()))
            else:
                arrays.append(pa.array(v, type=pa.string()))
            names.append(k)
        return pa.Table.from_arrays(arrays, names=names)

    for name, cols in (("user_daily", ud), ("campaign_daily", cd)):
        os.makedirs(os.path.join(out, name), exist_ok=True)
        pq.write_table(table(cols), os.path.join(out, name, "part-0.parquet"))


if __name__ == "__main__":
    main()
