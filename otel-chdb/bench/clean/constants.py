#!/usr/bin/env python3
"""The sizing calculator's constants from the clean blocks: current value
(central-sizing.html), the earlier like-for-like measurement on the loaded
box, and the clean median [min–max].

  constants.py            writes clean.json (calc.py's overrides) and prints constants.md
Sources:
  block1/results.md       edge CPU per row (ms per 10k, s3 destination unless noted)
  block2/fit.json + raw   central INSERT…SELECT: per-rep fits, cpu = F_stmt + n·F_obj + rows·u
  block3/proj_clean.json  merge µs per inserted row at a 1e4-part daily partition
  block4/results.jsonl    stored bytes per row after OPTIMIZE FINAL
Blends use the calculator's metrics mix: 40% sum, 30% gauge, 20% histogram,
5% exp. histogram, 5% summary. Spans and logs share usRow and mergeRow; the
mid scenario weights them 600k spans : 200k logs per second (0.75 : 0.25).
"""
import gzip, json, os, re, statistics as st
from collections import defaultdict

C = os.path.dirname(os.path.abspath(__file__))
MIX = {"sum": 0.40, "gauge": 0.30, "histogram": 0.20, "exponential_histogram": 0.05, "summary": 0.05}
TL = (0.75, 0.25)
SERIES_PER_POINT = 1 / 120  # metrics-layout README: hourly re-announce at a 30 s interval


def spread(xs):
    xs = sorted(xs)
    return st.median(xs), xs[0], xs[-1], len(xs)


def block1():
    """{(dest, signal, impl): (median, lo, hi)} in µs per row."""
    out = {}
    for l in open(os.path.join(C, "block1", "results.md")):
        c = [x.strip() for x in l.strip().strip("|").split("|")]
        if len(c) < 6 or c[0] not in ("local", "s3"):
            continue
        m = re.match(r"([\d.]+) \[([\d.]+)–([\d.]+)\]", c[4])
        e = re.match(r"([\d.]+)", c[5])
        g = re.match(r"([\d.]+)", c[7]) if len(c) > 7 else None  # the earlier parquetgo results column
        out[(c[0], c[1], c[2])] = tuple(float(x) / 10 for x in m.groups()) + (float(e.group(1)) / 10 if e else None,
                                                                              float(g.group(1)) / 10 if g else None)
    return out


def edge(b1):
    r = {}
    for k, key, old in [("edgeGoSpan", ("s3", "traces", "parquet-go"), 7.3), ("edgeGoLog", ("s3", "logs", "parquet-go"), 5.2),
                        ("edgeRsSpan", ("s3", "traces", "rust-direct"), 4.5), ("edgeRsLog", ("s3", "logs", "rust-direct"), 3.3),
                        ("edgePointB", ("s3", "metrics fleet", "rust-pipeline-series_table"), 2.0),
                        ("edgeRsPointA", ("s3", "metrics fleet", "rust-pipeline-clickstack_tables"), 5.5)]:
        m, lo, hi, e, _ = b1[key]
        r[k] = {"clean": m, "lo": lo, "hi": hi, "n": 5, "earlier": e, "src": f"block 1: {key[2]}, {key[1]}, {key[0]}"}
    # Go ClickStack-table points: the per-type parquet-go blend (local), as the calculator's 7.8 was built
    vals = {t: b1[("local", f"metrics_{t}", "parquet-go")] for t in MIX}
    r["edgeGoPointA"] = {"clean": sum(MIX[t] * vals[t][0] for t in MIX), "lo": sum(MIX[t] * vals[t][1] for t in MIX),
                         "hi": sum(MIX[t] * vals[t][2] for t in MIX), "n": 5, "earlier": sum(MIX[t] * vals[t][4] for t in MIX),
                         "src": "block 1: parquet-go per type, local, mix blend (earlier: the parquetgo results it was built from)"}
    r["edgePointB"]["src"] += " (the calculator's 2.0 is the Go prototype)"
    return r


def block2():
    fit = json.load(open(os.path.join(C, "block2", "fit.json")))
    reps = range(len(fit["otel_traces"]["per_rep"]))
    u = lambda t, i: fit[t]["per_rep"][i]["u_row_us"]
    B = {"sum": "otel_metrics_number_points", "gauge": "otel_metrics_number_points", "histogram": "otel_metrics_histogram_points",
         "exponential_histogram": "otel_metrics_exponential_histogram_points", "summary": "otel_metrics_summary_points"}
    A = {t: f"otel_metrics_{t}" for t in MIX}
    usRow_marg = [TL[0] * u("otel_traces", i) + TL[1] * u("otel_logs", i) for i in reps]
    usB = [sum(MIX[t] * u(B[t], i) for t in MIX) + SERIES_PER_POINT * u("otel_metrics_series", i) for i in reps]
    usA = [sum(MIX[t] * u(A[t], i) for t in MIX) for i in reps]
    # fixed cost per object at one object per statement, measured directly: the small root's m1 statements
    # (20 points or 200 rows each), CPU minus rows x the table's per-row cost, median over metrics tables per rep
    per, allin = defaultdict(list), defaultdict(lambda: defaultdict(list))
    for l in gzip.open(os.path.join(C, "block2", "raw", "statements.jsonl.gz"), "rt"):
        r = json.loads(l)
        if r["root"] == "large" and int(r["max_batch"]) == 1 and not int(r["exception_code"]):
            t = [x for x in r["tables"] if not x.startswith("_table_function")][0].split(".", 1)[1]
            if t in ("otel_traces", "otel_logs"):
                allin[int(r["rep"]) - 1][t].append(float(r["cpu_us"]) / int(r["written_rows"]))
        if r["root"] != "small" or int(r["max_batch"]) != 1 or int(r["exception_code"]):
            continue
        t = [x for x in r["tables"] if not x.startswith("_table_function")][0].split(".", 1)[1]
        if "metrics" not in t:
            continue
        rep = int(r["rep"]) - 1
        per[rep].append(float(r["cpu_us"]) / 1e3 - int(r["written_rows"]) * u(t, rep) / 1e3)
    fixed = [st.median(per[i]) for i in sorted(per)]
    # usRow is all-in: the calculator charges spans and logs no fixed cost, at its 10k rows per object
    usRow = [TL[0] * st.median(allin[i]["otel_traces"]) + TL[1] * st.median(allin[i]["otel_logs"]) for i in sorted(allin)]
    fit_fixed = [st.median(fit[t]["per_rep"][i]["F_stmt_ms"] + fit[t]["per_rep"][i]["F_obj_ms"] for t in fit if "metrics" in t) for i in reps]
    at32 = [st.median(fit[t]["per_rep"][i]["F_stmt_ms"] / 32 + fit[t]["per_rep"][i]["F_obj_ms"] for t in fit) for i in reps]
    mk = lambda xs, e, src: dict(zip(("clean", "lo", "hi", "n"), spread(xs)), earlier=e, src=src)
    return {"usRow": mk(usRow, 5.0, "block 2: all-in CPU/row of 1-object 10k-row statements, traces·0.75 + logs·0.25"),
            "_usRow_marginal": mk(usRow_marg, None, "block 2 fit: marginal µs/row, traces·0.75 + logs·0.25 (no fixed cost)"),
            "usPointB": mk(usB, 1.15, "block 2: u_row, B points mix + series/120"),
            "usPointA": mk(usA, 7.85, "block 2: u_row, ClickStack tables mix"),
            "fixedMs": mk(fixed, 17.0, "block 2: 1-object statements of 20 points, CPU − rows·u_row"),
            "_fixed_fit": mk(fit_fixed, 17.0, "block 2 fit: F_stmt + F_obj, median over metrics tables"),
            "_fixed_at32": mk(at32, None, "block 2 fit: F_stmt/32 + F_obj per object at 32 objects/statement")}


def block3():
    p = os.path.join(C, "block3", "proj_clean.json")
    if not os.path.exists(p):
        return {}
    c = json.load(open(p))["constants"]
    src = {"mergeRow": "block 3: traces·0.75 + logs·0.25 at 1e4 parts", "mergePointB": "block 3: B points mix at 1e4 parts",
           "mergePointA": "block 3: sum·0.7 + histogram·0.3 at 1e4 parts"}
    old = {"mergeRow": 11.0, "mergePointB": 6.5, "mergePointA": 30.0}
    return {k: {"clean": v[0], "lo": v[1], "hi": v[2], "n": 1, "earlier": old[k], "src": src[k] + " (range: step … direct fit)"} for k, v in c.items()}


def block4():
    p = os.path.join(C, "block4", "results.jsonl")
    if not os.path.exists(p):
        return {}
    r = {x["label"]: x["data_compressed_B_per_row"] for x in map(json.loads, open(p)) if x.get("rows")}
    out = {}
    try:
        b = {"sum": r["B-sum"], "gauge": r["B-gauge"], "histogram": r["B-histogram"],
             "exponential_histogram": r["B-exponential_histogram"], "summary": r["B-summary"]}
        out["bPointB"] = {"clean": sum(MIX[t] * b[t] for t in MIX), "n": 1, "earlier": 6.3, "src": "block 4: B mix, OPTIMIZE FINAL"}
    except KeyError:
        pass
    try:
        a = {t: r[f"A-{t}"] for t in MIX}
        out["bPointA"] = {"clean": sum(MIX[t] * a[t] for t in MIX), "n": 1, "earlier": 26.0, "src": "block 4: ClickStack mix, OPTIMIZE FINAL"}
    except KeyError:
        pass
    if "B-series (5 cycles)" in r:
        out["bSeries"] = {"clean": r["B-series (5 cycles)"], "n": 1, "earlier": 39.6, "src": "block 4: series table, 5 cycles"}
    return out


CURRENT = {"usRow": 5, "usPointB": 1.15, "usPointA": 7.85, "fixedMs": 17, "mergeRow": 11, "mergePointB": 6.5, "mergePointA": 30,
           "bPointB": 6.3, "bPointA": 26, "bSeries": 40, "edgeGoSpan": 7.3, "edgeGoLog": 5.2, "edgeRsSpan": 4.5, "edgeRsLog": 3.3,
           "edgePointB": 2, "edgeGoPointA": 7.8, "edgeRsPointA": 5.5}
LABEL = {"usRow": "Insert µs per span or log", "usPointB": "Insert µs per point, series layout", "usPointA": "Insert µs per point, ClickStack tables",
         "fixedMs": "Fixed ms per inserted object", "mergeRow": "Merge µs per span or log", "mergePointB": "Merge µs per point, series layout",
         "mergePointA": "Merge µs per point, ClickStack tables", "bPointB": "Stored B per point, series layout",
         "bPointA": "Stored B per point, ClickStack tables", "bSeries": "Stored B per series row", "edgeGoSpan": "Edge µs/span, Go",
         "edgeGoLog": "Edge µs/log, Go", "edgeRsSpan": "Edge µs/span, Rust", "edgeRsLog": "Edge µs/log, Rust",
         "edgePointB": "Edge µs/point, series layout", "edgeGoPointA": "Edge µs/point, ClickStack, Go", "edgeRsPointA": "Edge µs/point, ClickStack, Rust"}


def main():
    allc = {}
    allc.update(block2())
    allc.update(block3())
    allc.update(block4())
    allc.update(edge(block1()))
    json.dump({k: round(v["clean"], 3) for k, v in allc.items() if not k.startswith("_")}, open(os.path.join(C, "clean.json"), "w"), indent=1)
    json.dump(allc, open(os.path.join(C, "constants.json"), "w"), indent=1)
    print("| constant | current (calculator) | earlier like-for-like (loaded box) | clean, median [min–max] | n | Δ vs current | >15%? | source |")
    print("|---|---|---|---|---|---|---|---|")
    for k in CURRENT:
        if k not in allc:
            print(f"| {LABEL[k]} (`{k}`) | {CURRENT[k]} | – | not measured yet | | | | |")
            continue
        v = allc[k]
        rng = f" [{v['lo']:.2f}–{v['hi']:.2f}]" if "lo" in v else ""
        d = (v["clean"] - CURRENT[k]) / CURRENT[k] * 100
        e = f"{v['earlier']:.2f}" if v.get("earlier") is not None else "–"
        print(f"| {LABEL[k]} (`{k}`) | {CURRENT[k]} | {e} | **{v['clean']:.2f}**{rng} | {v['n']} | {d:+.0f}% | {'**yes**' if abs(d) > 15 else 'no'} | {v['src']} |")
    for k in ("_usRow_marginal", "_fixed_fit", "_fixed_at32"):
        if k in allc:
            v = allc[k]
            print(f"| ({v['src']}) | | | {v['clean']:.2f} [{v['lo']:.2f}–{v['hi']:.2f}] | {v['n']} | | | |")


if __name__ == "__main__":
    main()
