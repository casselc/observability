#!/usr/bin/env python3
"""Summarise sntp_probe.py samples (kubectl logs of the DaemonSet, or files
from hosts) into what the design's clock assumptions need:

  per node: offset against each reference, p50 / p99 / max |offset|, drift (ms/h, a
            least-squares slope), samples lost
  fleet:    the widest spread between two nodes' offsets in the same minute (the
            edges' skew against each other), and each node against ClickHouse
  verdicts: against the constants the design uses (AMBIGUITY.md E6, DECISIONS risk 5):
              --wm-skew 5 s          complete_through's allowance for an edge clock (FORMAT.md §3)
              lease margin 20 s      the server-side fence: worker vs ClickHouse (D9)
              zombie bound 10 min    an epoch named below the floor (D12, risk 5b)
              SigV4 15 min           403 RequestTimeTooSkewed (D18)
            and the expected share of rows whose received_at lands in the wrong day's
            partition: E|offset| / 86,400 s (a row within |offset| of midnight).

  skew_report.py samples.jsonl [more.jsonl ...] [--ref 169.254.169.123] [--json out.json]

Lines that are not JSON (kubectl --prefix adds "[pod/...] ") are handled.
"""
import argparse, collections, json, re, statistics as st, sys

ap = argparse.ArgumentParser()
ap.add_argument("files", nargs="+")
ap.add_argument("--ref", default=None, help="the reference to judge by (default: the most sampled one)")
ap.add_argument("--json", default=None)
a = ap.parse_args()

rows = []
for f in a.files:
    for line in open(f):
        i = line.find("{")
        if i < 0:
            continue
        try:
            rows.append(json.loads(line[i:]))
        except ValueError:
            pass
by_ref = collections.Counter(r["ref"] for r in rows if "offset_ms" in r and r["ref"] != "clickhouse")
ref = a.ref or (by_ref.most_common(1)[0][0] if by_ref else None)
out = {"ref": ref, "nodes": {}, "fleet": {}, "verdicts": {}}

def q(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))] if v else None

per_node = collections.defaultdict(list)
lost = collections.Counter()
ch = collections.defaultdict(list)
for r in rows:
    if r.get("ref") == ref:
        if "offset_ms" in r:
            per_node[r["node"]].append((r["t"], r["offset_ms"], r["delay_ms"]))
        else:
            lost[r["node"]] += 1
    elif r.get("ref") == "clickhouse" and "offset_ms" in r:
        ch[r["node"]].append(r["offset_ms"])

print(f"reference {ref}; {len(per_node)} nodes; {sum(len(v) for v in per_node.values())} samples")
print(f"{'node':40s} {'n':>6s} {'p50 ms':>9s} {'p99 |ms|':>9s} {'max |ms|':>9s} {'drift ms/h':>10s} {'lost':>5s} {'vs CH p50':>9s}")
for n, v in sorted(per_node.items()):
    offs = [o for _, o, _ in v]
    ab = [abs(o) for o in offs]
    ts = [t for t, _, _ in v]
    drift = None
    if len(v) > 2 and max(ts) > min(ts):
        mt, mo = st.mean(ts), st.mean(offs)
        drift = sum((t - mt) * (o - mo) for t, o in zip(ts, offs)) / sum((t - mt) ** 2 for t in ts) * 3600
    c = st.median(ch[n]) if ch.get(n) else None
    out["nodes"][n] = {"n": len(v), "p50_ms": st.median(offs), "p99_abs_ms": q(ab, 0.99), "max_abs_ms": max(ab),
                       "drift_ms_per_h": drift, "lost": lost[n], "vs_clickhouse_p50_ms": c}
    print(f"{n[:40]:40s} {len(v):6d} {st.median(offs):9.2f} {q(ab, 0.99):9.2f} {max(ab):9.2f} "
          f"{(drift if drift is not None else float('nan')):10.3f} {lost[n]:5d} {(c if c is not None else float('nan')):9.2f}")

# The spread between nodes, minute by minute.
minute = collections.defaultdict(dict)
for n, v in per_node.items():
    for t, o, _ in v:
        minute[int(t // 60)][n] = o
spreads = [max(m.values()) - min(m.values()) for m in minute.values() if len(m) > 1]
allabs = [abs(o) for v in per_node.values() for _, o, _ in v]
worst_node_abs = max(allabs) if allabs else 0.0
out["fleet"] = {"minutes": len(spreads), "spread_p50_ms": st.median(spreads) if spreads else None,
                "spread_p99_ms": q(spreads, 0.99), "spread_max_ms": max(spreads) if spreads else None,
                "max_abs_offset_ms": worst_node_abs, "mean_abs_offset_ms": st.mean(allabs) if allabs else None}
fmt = lambda x: "n/a" if x is None else f"{x:.2f}"
print(f"\nnode-to-node spread per minute: p50 {fmt(out['fleet']['spread_p50_ms'])} ms, p99 {fmt(out['fleet']['spread_p99_ms'])} ms, "
      f"max {fmt(out['fleet']['spread_max_ms'])} ms over {len(spreads)} minutes")

limits = {"wm-skew 5 s (complete_through)": 5_000, "lease margin 20 s (fence: worker vs ClickHouse)": 20_000,
          "zombie bound 10 min (epoch below the floor)": 600_000, "SigV4 15 min (403)": 900_000}
worst = max(worst_node_abs, out["fleet"]["spread_max_ms"] or 0)
chw = max((abs(x) for v in ch.values() for x in v), default=None)
for k, lim in limits.items():
    basis = chw if ("fence" in k and chw is not None) else worst
    ok = basis is not None and basis < lim
    out["verdicts"][k] = {"worst_ms": basis, "limit_ms": lim, "headroom_x": (lim / basis) if basis else None, "pass": ok}
    print(f"{'PASS' if ok else 'FAIL'} {k}: worst {fmt(basis)} ms{', %.0fx headroom' % (lim / basis) if basis else ''}")
m = out["fleet"]["mean_abs_offset_ms"]
if m is not None:
    out["verdicts"]["wrong-day rows"] = {"share": m / 86_400_000}
    print(f"expected share of rows in the wrong day's partition: {m / 86_400_000:.2e} (mean |offset| {m:.2f} ms / 86,400 s)")
if a.json:
    json.dump(out, open(a.json, "w"), indent=1)
