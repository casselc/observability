#!/usr/bin/env python3
"""bench.jsonl (encbench, pubbench and pipeline lines) -> markdown table:
median across processes, [min-max]."""
import json, sys, statistics
from collections import defaultdict

rows = defaultdict(list)
loads = []
for line in open(sys.argv[1]):
    r = json.loads(line)
    if "rep" in r:
        loads.append(float(r["load"]))
        continue
    rows[(r["Dest"], r["Signal"], r["Impl"])].append(r)


def cell(rs, k, f="{:.0f}"):
    v = sorted(x[k] for x in rs if k in x and x[k] is not None)
    if not v:
        return "–"
    m = statistics.median(v)
    return f"{f.format(m)} [{f.format(v[0])}–{f.format(v[-1])}]" if len(v) > 1 else f.format(m)


print(f"load average during the runs: {min(loads):.1f}–{max(loads):.1f} (4 vCPUs)\n")
print("| dest | signal | impl | n | CPU ms/batch | ms/batch | k rows/s | peak RSS MB | object KB | flatten / encode / commit ms | S3 req/batch |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
order = lambda k: (k[0], k[1], 0 if "pipeline" in k[2] else 1 if k[2].startswith("rust") else 2, k[2])
for key in sorted(rows, key=order):
    rs = rows[key]
    d, s, i = key
    obj = cell(rs, "ObjectBytes", "{:.0f}")
    if obj != "–":
        v = sorted(x["ObjectBytes"] for x in rs if x.get("ObjectBytes"))
        obj = f"{statistics.median(v)/1024:.0f}" if v else "–"
    phases = "–"
    if "FlattenMSPerBatch" in rs[0]:
        f = lambda k: statistics.median(x[k] for x in rs)
        phases = f"{f('FlattenMSPerBatch'):.1f} / {f('EncodeMSPerBatch'):.1f} / {f('CommitMSPerBatch'):.1f}"
    s3 = "–"
    if rs[0].get("S3PerBatch"):
        s3 = ", ".join(f"{k} {statistics.median(x['S3PerBatch'].get(k, 0) for x in rs):.2f}" for k in sorted(rs[0]["S3PerBatch"]))
    print(f"| {d} | {s} | {i} | {len(rs)} | {cell(rs, 'CPUMSPerBatch')} | {cell(rs, 'MedianMS', '{:.1f}')} | "
          f"{cell([dict(x, K=x['RowsPerSec']/1000) for x in rs], 'K')} | {cell(rs, 'MaxRSSMB')} | {obj} | {phases} | {s3} |")
