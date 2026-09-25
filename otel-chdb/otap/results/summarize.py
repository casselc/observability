#!/usr/bin/env python3
"""Summarise otapbench JSON lines: median across processes, [min-max]."""
import json
import statistics
import sys
from collections import defaultdict

rows = defaultdict(list)
for line in open(sys.argv[1]):
    line = line.strip()
    if not line.startswith("{"):
        continue
    r = json.loads(line)
    rows[(r["Dest"], r["Signal"], r["Variant"])].append(r)


def fmt(xs, nd=0):
    m = statistics.median(xs)
    f = f"{{:.{nd}f}}"
    if len(xs) == 1:
        return f.format(m)
    return f"{f.format(m)} [{f.format(min(xs))}–{f.format(max(xs))}]"


order = ["encode", "decode", "flatten", "startables", "ref", "ref-nobloom", "via-pdata", "star", "star-nobloom",
         "flat-parquet", "flat-parquet-nobloom", "flat-arrow", "raw", "bar"]
print("| dest | signal | variant | n | ms/batch | CPU ms/batch | Go allocs/batch | Go MB alloc/batch | peak RSS MB | objects | object KB | S3 req/batch |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|")
for dest in ["none", "local", "s3"]:
    for sig in ["traces", "logs"]:
        for v in order:
            rs = rows.get((dest, sig, v))
            if not rs:
                continue
            s3 = ""
            if rs[0].get("S3PerBatch"):
                s3 = ", ".join(f"{k} {statistics.median([r['S3PerBatch'].get(k, 0) for r in rs]):.1f}"
                               for k in sorted(rs[0]["S3PerBatch"]))
            kb = fmt([r.get("ObjectBytes", 0) / 1024 for r in rs]) if rs[0].get("ObjectBytes") else ""
            print(f"| {dest} | {sig} | {v} | {len(rs)} | {fmt([r['MedianMS'] for r in rs], 1)} | "
                  f"{fmt([r['CPUMSPerBatch'] for r in rs], 1)} | {fmt([r['GoAllocsPerBatch'] for r in rs])} | "
                  f"{fmt([r['GoBytesPerBatch'] / 1e6 for r in rs], 1)} | {fmt([r['MaxRSSMB'] for r in rs])} | "
                  f"{rs[0].get('Objects', '') or ''} | {kb} | {s3} |")
bar = {sig: rows.get(("none", sig, "encode")) for sig in ["traces", "logs"]}
for sig, rs in bar.items():
    if rs:
        print(f"\nOTAP BatchArrowRecords payload bytes, {sig}: {rs[0]['BARBytes'] / 1024:.0f} KB per 10k rows")
