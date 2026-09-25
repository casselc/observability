#!/usr/bin/env python3
"""Summarizes scripts/series_bench.sh's JSON lines: median [min-max] per
configuration of CPU per 10k-point request, bytes, objects and series objects
per request, phase times and peak RSS."""
import json, sys

rows = [json.loads(l) for l in open(sys.argv[1])]
loads = [float(r["load"]) for r in rows if "load" in r]
g = {}
for r in rows:
    if "Impl" in r:
        g.setdefault(r["Impl"], []).append(r)


def m(v, f, fmt="{:.1f}"):
    xs = sorted(x[f] for x in v if f in x)
    if not xs:
        return "–"
    return f"{fmt.format(xs[len(xs) // 2])} [{fmt.format(xs[0])}–{fmt.format(xs[-1])}]"


print(f"load average {min(loads):.2f}–{max(loads):.2f}, {len(next(iter(g.values())))} processes each\n")
print("| config | CPU ms / 10k points | µs / point | KB / request | B / point | objects (PUTs) / request | series objects / request | flatten / encode / commit ms | peak RSS MB |")
print("|---|---|---|---|---|---|---|---|---|")
for k, v in g.items():
    cpu = sorted(x["CPUMSPerBatch"] for x in v)[len(v) // 2]
    b = [x for x in v if "BytesPerBatch" in x]
    bpp = f"{sorted(x['BytesPerBatch'] for x in b)[len(b) // 2] / 10000:.1f}" if b else "–"
    ph = " / ".join(f"{sorted(x[f] for x in v)[len(v) // 2]:.1f}" for f in ("FlattenMSPerBatch", "EncodeMSPerBatch", "CommitMSPerBatch")) if b else "–"
    print(f"| {k} | {m(v, 'CPUMSPerBatch')} | {cpu / 10:.2f} | {(sorted(x['BytesPerBatch'] for x in b)[len(b) // 2] / 1000):.1f} | {bpp} | "
          f"{m(b, 'ObjectsPerBatch', '{:.3f}') if b else '–'} | {m(b, 'SeriesObjectsPerBatch', '{:.3f}') if b else '–'} | {ph} | {m(v, 'MaxRSSMB', '{:.0f}')} |"
          if b else f"| {k} | {m(v, 'CPUMSPerBatch')} | {cpu / 10:.2f} | – | – | – | – | – | {m(v, 'MaxRSSMB', '{:.0f}')} |")
