#!/usr/bin/env python3
"""Step 0 summary: CPU ms per 10k points (per 10k spans for the control), per
variant, median [min–max] over repetitions, beside the earlier otap-rs
results and bench/clean block 1; steal/load per run from env.jsonl.
  summarize.py > results.md"""
import json, os, statistics as st, subprocess, sys

H = os.path.dirname(os.path.abspath(__file__))
rows = [json.loads(l) for l in open(f"{H}/series.jsonl")]
EARLIER = {"series-fleet": 17.6, "series-testgen": 24.5, "clickstack-fleet": 50.4, "clickstack-testgen": 48.7,
           "rust-pipeline-series_table": 16.7, "rust-pipeline-clickstack_tables": 46.5, "control-traces": 44.7}
CLEAN = {"series-fleet": 20.8, "series-testgen": 29.9, "clickstack-fleet": 64.3, "clickstack-testgen": 59.9,
         "rust-pipeline-series_table": 20.6, "rust-pipeline-clickstack_tables": 62.0, "control-traces": 40.0}
VARS = ["pre", "head", "head-nodur"]


def fmt(v):
    return f"{st.median(v):.1f} [{min(v):.1f}–{max(v):.1f}]" if v else "–"


impls = list(dict.fromkeys(r["Impl"] for r in rows))
print("CPU ms per request of 10k points (control: 10k spans), median [min–max] over repetitions (n per cell in brackets after).\n")
print("| config | pre (8cf80ad) | head (d4bb951 + later) | head, no durable-buffer feature | head vs pre | earlier otap-rs (loaded box) | bench/clean block 1 |")
print("|---|---|---|---|---|---|---|")
for im in impls:
    cells, med = [], {}
    for v in VARS:
        xs = [r["CPUMSPerBatch"] for r in rows if r["Impl"] == im and r["variant"] == v]
        med[v] = st.median(xs) if xs else None
        cells.append(f"{fmt(xs)} (n={len(xs)})")
    d = f"{(med['head'] / med['pre'] - 1) * 100:+.1f}%" if med["pre"] and med["head"] else "–"
    print(f"| {im} | " + " | ".join(cells) + f" | {d} | {EARLIER.get(im, '–')} | {CLEAN.get(im, '–')} |")
print("\nPhases (encbench), ms per request, median: flatten / encode / commit; bytes per request.\n")
print("| config | variant | flatten | encode | commit | KB/request |\n|---|---|---|---|---|---|")
for im in impls:
    for v in VARS:
        xs = [r for r in rows if r["Impl"] == im and r["variant"] == v and "FlattenMSPerBatch" in r]
        if xs:
            m = lambda k: st.median(r[k] for r in xs)
            print(f"| {im} | {v} | {m('FlattenMSPerBatch'):.1f} | {m('EncodeMSPerBatch'):.1f} | {m('CommitMSPerBatch'):.1f} | {m('BytesPerBatch') / 1000:.1f} |")
env = subprocess.run([sys.executable, f"{H}/../../clean/lib/envlog.py", f"{H}/env.jsonl", "--json"], capture_output=True, text=True).stdout
reps = [json.loads(l) for l in env.splitlines() if '"finish"' in l]
if reps:
    print(f"\nEnvironment: {len(reps)} gated runs; load at start max {max(r['load_start'] for r in reps):.2f}; "
          f"steal per run median {st.median(r['steal_pct'] for r in reps):.2f}% / max {max(r['steal_pct'] for r in reps):.2f}%; "
          f"flagged {sum(r['bad'] for r in reps)}.")
