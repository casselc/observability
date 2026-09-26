#!/usr/bin/env python3
"""Block 3 projection: merge CPU per inserted row at a daily partition of
N = 1e3, 1e4, 1e5 parts, by ../../merges/proj.py's method, next to that
script's earlier (loaded-box) projection for the same table at ~100k rows.

  M(N) = M(N_end) + b · ln(N / N_end) · c           (proj.py, "step")
where M(N_end) is the run's measured merge µs per inserted row at its last
part, b its own slope of rewrites per row against ln N (fit.py: the earlier
100k runs reached 59–588 parts and borrowed the 10k runs' slope; these reach
1,000+), c its µs per row read in merges whose output is >= 16x the insert.
fit.py's direct fit M = am + bm · ln N is shown beside it ("direct"); the
two bracket the value.

Runs that the disk budget stopped below MIN_OWN (300) parts (random-id traces at 39
B/row: ~160 parts; the top-level merge's output needs as much free space as
the data) take the slope of the named run that went further (proj.py did the
same with its 10k runs); their own short-range slope is the other end of the
range.

  proj_clean.py [--json]      (after analyze.py and fit.py on results/*)
"""
import json, math, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")

MIN_OWN = 300
# slope donor for a run stopped below MIN_OWN parts
DONOR = {"traces-100k": "traces-100k-tg", "exphist-100k": "histogram-100k", "logs-100k": "traces-100k-tg", "clickstack-sum-100k": "clickstack-histogram-100k"}
# (label, table, clean run, earlier 100k-row projection at N=1e4 [µs/row] and ×insert from merges/results/projection.md)
ROWS = [
    ("traces, random ids", "traces", "traces-100k", 11.7, 2.2),
    ("traces, testgen ids", "traces", "traces-100k-tg", 12.2, 2.6),
    ("logs", "logs", "logs-100k", 9.7, 2.6),
    ("series layout: number points (gauge+sum)", "number", "number-100k", 5.9, 5.0),
    ("series layout: histogram points", "histogram", "histogram-100k", 7.4, 3.1),
    ("series layout: exp. histogram points", "exponential_histogram", "exphist-100k", 13.5, 2.6),
    ("series layout: summary points", "summary", "summary-100k", 8.1, 3.2),
    ("series layout: series table", "series", "series-100k", None, None),
    ("ClickStack otel_metrics_sum", "A_sum", "clickstack-sum-100k", None, None),
    ("ClickStack otel_metrics_histogram", "A_histogram", "clickstack-histogram-100k", 29.3, 2.7),
]
# the calculator's metrics blend (40% sum, 30% gauge, 20% histogram, 5% exp, 5% summary)
BLEND_B = {"number": 0.70, "histogram": 0.20, "exponential_histogram": 0.05, "summary": 0.05}
BLEND_A = {"A_sum": 0.70, "A_histogram": 0.30}  # gauge merges like sum; exp/summary like histogram [E]
EARLIER = {"mergeRow": 11.0, "mergePointB": 6.5, "mergePointA": 30.0}


def upper_cost(t):
    rows = cpu = 0
    for k, v in t["size_classes"].items():
        if int(k.split("-")[0]) >= 16 and v["us_per_row_in"]:
            rows += v["rows_in"]
            cpu += v["rows_in"] * v["us_per_row_in"]
    return cpu / rows if rows else t["with_drain"]["us_per_row_in"]


def project():
    out = []
    for label, tbl, run, e4, ex4 in ROWS:
        p = os.path.join(RES, run, "summary.json")
        if not os.path.exists(p):
            continue
        t = json.load(open(p))["tables"][tbl]
        fit = t.get("fit") or {}
        n_end = fit.get("parts_reached") or t["statements"]
        m_end, ins = t["merge_us_per_inserted_row"], t["insert_us_per_row"]
        r = {"label": label, "table": tbl, "run": run, "rows": t["rows_per_statement"], "N_end": n_end, "insert_us": ins,
             "merge_us_end": m_end, "rewrites_end": t["rows_rewritten_per_inserted_row"], "stored_B": t["stored_bytes_per_row"],
             "earlier_e4": e4, "earlier_x_e4": ex4, "proj": {}, "direct": {}}
        c = upper_cost(t)
        donor = None
        if n_end < MIN_OWN and run in DONOR:
            dp = os.path.join(RES, DONOR[run], "summary.json")
            if os.path.exists(dp):
                donor = next((v["fit"] for v in json.load(open(dp))["tables"].values() if v.get("fit")), None)
        slopes = ([(donor["b_per_ln"], f"slope of {DONOR[run]}")] if donor else []) + ([(fit["b_per_ln"], "own")] if fit else [])
        if slopes:
            b = slopes[0][0]
            r["b_per_decade"], r["c_upper"], r["slope_from"] = b * math.log(10), c, slopes[0][1]
            for N in (1e3, 1e4, 1e5):
                step = lambda bb: m_end + bb * max(0.0, math.log(N / n_end)) * c
                r["proj"][int(N)] = step(b)
                # the other estimate: the run's own slope (donor rows), or fit.py's direct fit M = am + bm ln N
                r["direct"][int(N)] = (step(slopes[1][0]) if donor and len(slopes) > 1 else
                                       fit["cpu_a_us"] + fit["cpu_b_us_per_ln"] * math.log(N) if fit and not donor else step(b))
        out.append(r)
    return out


def blend(rs, w, N):
    sel = {r["table"]: r for r in rs if r["table"] in w and N in r["proj"]}
    if set(sel) != set(w):
        return None
    lo = sum(w[k] * min(sel[k]["proj"][N], sel[k]["direct"][N]) for k in w)
    hi = sum(w[k] * max(sel[k]["proj"][N], sel[k]["direct"][N]) for k in w)
    mid = sum(w[k] * sel[k]["proj"][N] for k in w)
    return mid, lo, hi


def constants(rs):
    """the calculator's merge constants at N = 1e4 (value, lo, hi)."""
    by = {r["run"]: r for r in rs}
    out = {}
    tl = [by.get("traces-100k"), by.get("logs-100k")]
    if all(x and 10000 in x["proj"] for x in tl):
        w = (0.75, 0.25)  # mid scenario: 600k spans/s, 200k logs/s
        out["mergeRow"] = tuple(sum(wi * f(x) for wi, x in zip(w, tl)) for f in (
            lambda x: x["proj"][10000], lambda x: min(x["proj"][10000], x["direct"][10000]),
            lambda x: max(x["proj"][10000], x["direct"][10000])))
    b = blend(rs, BLEND_B, 10000)
    if b:
        out["mergePointB"] = b
    a = blend(rs, BLEND_A, 10000)
    if a:
        out["mergePointA"] = a
    return out


def main():
    rs = project()
    if "--json" in sys.argv:
        json.dump({"rows": rs, "constants": constants(rs)}, open(os.path.join(HERE, "proj_clean.json"), "w"), indent=1)
    print("| table | rows/stmt | parts reached | insert µs/row | merge µs/ins. row at end | rewrites/row at end | slope, rewrites per decade (from) "
          "| µs per rewritten row, upper levels | merge µs/row at N=1e3 | **N=1e4** [other estimate] | N=1e5 | ×insert at 1e4 "
          "| earlier (loaded box) µs/row at 1e4, ×insert | Δ at 1e4 |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rs:
        p, d = r["proj"], r["direct"]
        if p:
            e = f"{r['earlier_e4']} ({r['earlier_x_e4']}×)" if r["earlier_e4"] else "– (not projected)"
            dl = f"{(p[10000] - r['earlier_e4']) / r['earlier_e4'] * 100:+.0f}%" if r["earlier_e4"] else ""
            print(f"| {r['label']} | {r['rows']:,} | {r['N_end']:,} | {r['insert_us']:.2f} | {r['merge_us_end']:.2f} | {r['rewrites_end']:.2f} "
                  f"| {r['b_per_decade']:.2f} ({r['slope_from']}) | {r['c_upper']:.2f} | {p[1000]:.1f} | **{p[10000]:.1f}** [{d[10000]:.1f}] | {p[100000]:.1f} "
                  f"| {p[10000] / r['insert_us']:.1f}× | {e} | {dl} |")
        else:
            print(f"| {r['label']} | {r['rows']:,} | {r['N_end']:,} | {r['insert_us']:.2f} | {r['merge_us_end']:.2f} | {r['rewrites_end']:.2f} "
                  f"| – | – | – | – | – | – | – | |")
    c = constants(rs)
    if c:
        print("\n| calculator constant | earlier | clean, N=1e4 [range: the two estimates] | Δ |\n|---|---|---|---|")
        for k, (v, lo, hi) in c.items():
            print(f"| {k} | {EARLIER[k]} | {v:.1f} [{lo:.1f}–{hi:.1f}] | {(v - EARLIER[k]) / EARLIER[k] * 100:+.0f}% |")


if __name__ == "__main__":
    main()
