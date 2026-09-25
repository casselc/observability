#!/usr/bin/env python3
"""Fits server insert CPU per object = fixed + marginal * rows, per layout
and type, over the three object sizes loaded (load-meas-5k, load-meas-20k,
load-bulk), and prints the blended numbers the README uses."""
import json, os
here = os.path.dirname(os.path.abspath(__file__))
pts = {}
for f in ["load-meas-5k.jsonl", "load-meas-20k.jsonl", "load-bulk.jsonl"]:
    for l in open(os.path.join(here, f)):
        d = json.loads(l)
        if "Layout" in d and d["Type"] != "series":
            pts.setdefault((d["Layout"], d["Type"]), []).append((d["RowsPerObject"], d["CPUmsPerObject"]))
fit = {}
print("layout type                   fixed_ms  marginal_us/pt   (rows, ms/object)")
for k, v in sorted(pts.items()):
    n = len(v); sx = sum(x for x, _ in v); sy = sum(y for _, y in v)
    sxx = sum(x * x for x, _ in v); sxy = sum(x * y for x, y in v)
    m = (n * sxy - sx * sy) / (n * sxx - sx * sx)
    b = (sy - m * sx) / n
    fit[k] = (b, m * 1000)
    print(f"{k[0]:3s} {k[1]:24s} {b:7.1f}  {m*1000:7.2f}   {[(int(x), round(y,1)) for x, y in v]}")
w = {"sum": .40, "gauge": .30, "histogram": .20, "exponential_histogram": .05, "summary": .05}
wd = {"sum": .52, "gauge": .28, "histogram": .16, "exponential_histogram": .02, "summary": .02}
for name, ws in (("calculator weights 40/30/20/5/5", w), ("dataset mix 52/28/16/2/2", wd)):
    for lay in ("A", "B"):
        marg = sum(ws[t] * fit[(lay, t)][1] for t in ws)
        fixed = sum(fit[(lay, t)][0] for t in ws) / 5
        print(f"{name}: {lay} marginal {marg:.2f} us/pt, mean fixed {fixed:.1f} ms/object")
json.dump({f"{k[0]}/{k[1]}": v for k, v in fit.items()}, open(os.path.join(here, "fit.json"), "w"), indent=1)
