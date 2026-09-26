#!/usr/bin/env python3
"""Block 1 tables: CPU ms per 10k rows/points (= µs per row/point x 10) per
(dest, signal, impl), median [min-max] over the clean repetitions, next to
the earlier loaded-box medians from the committed results of the same
scripts (otap-rs/results/{bench,metrics/bench,series/bench}.jsonl) and
parquetgo's own pubbench runs (parquetgo/compare/results/*pubbench.jsonl).

  summarize.py > results.md
"""
import json, os, statistics as st
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
SPIKE = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


# Discarded (lib/envlog.py: steal > 2% during the run) and replaced by rerun.sh:
EXCLUDE = {(1, "local", "metrics_histogram", "rust-direct"), (3, "s3", "logs", "rust-pipeline-via_otap")}


def recs(path):
    out, rep = [], None
    for l in open(path):
        l = l.strip()
        if l:
            r = json.loads(l)
            if "clean_rep" in r:
                rep = r["clean_rep"]
            if "Impl" in r and (rep, r.get("Dest"), r.get("Signal"), r["Impl"]) not in EXCLUDE:
                out.append(r)
    return out


def key(r):
    impl = r["Impl"]
    if r.get("Signal", "").startswith("metrics") and impl == "parquet-go" and r.get("Par", 1) not in (None, 1):
        impl += f" x{r['Par']}"
    return (r.get("Dest"), r.get("Signal"), impl)


def group(rs, f="CPUMSPerBatch"):
    g = defaultdict(list)
    for r in rs:
        if r.get(f) is not None:
            g[key(r)].append(r[f])
    return g


def fmt(v):
    v = sorted(v)
    return f"{st.median(v):.1f} [{v[0]:.1f}–{v[-1]:.1f}]" if v else "–"


def main():
    new = recs(f"{HERE}/bench.jsonl") + recs(f"{HERE}/metrics.jsonl") + recs(f"{HERE}/series.jsonl")
    old = {"otap-rs": recs(f"{SPIKE}/otap-rs/results/bench.jsonl") + recs(f"{SPIKE}/otap-rs/results/metrics/bench.jsonl")
           + recs(f"{SPIKE}/otap-rs/results/series/bench.jsonl"),
           "parquetgo": recs(f"{SPIKE}/parquetgo/compare/results/pubbench.jsonl") + recs(f"{SPIKE}/parquetgo/compare/results/metrics-pubbench.jsonl")}
    gn = group(new)
    go = {k: group(v) for k, v in old.items()}
    size = group(new, "ObjectBytes")
    size2 = group(new, "BytesPerBatch")
    print("| dest | signal | impl | n | clean CPU ms/10k, median [min–max] | earlier, otap-rs results | Δ | earlier, parquetgo results | Δ | object KB (clean) |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for k in sorted(gn, key=lambda k: (k[0] or "", k[1] or "", k[2])):
        v = gn[k]
        m = st.median(v)
        cells = []
        for src in ("otap-rs", "parquetgo"):
            o = go[src].get(k)
            if src == "parquetgo" and k[1] == "metrics":
                o = None  # parquetgo's "metrics" batch is 10k points of each type (50k), not 2k of each
            if o:
                om = st.median(o)
                cells += [f"{om:.1f} (n={len(o)})", f"{(m - om) / om * 100:+.0f}%"]
            else:
                cells += ["–", ""]
        sz = size.get(k) or size2.get(k)
        print(f"| {k[0]} | {k[1]} | {k[2]} | {len(v)} | {fmt(v)} | {' | '.join(cells)} | {st.median(sz) / 1e3:.0f} |" if sz else
              f"| {k[0]} | {k[1]} | {k[2]} | {len(v)} | {fmt(v)} | {' | '.join(cells)} | – |")


if __name__ == "__main__":
    main()
