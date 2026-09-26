#!/usr/bin/env python3
"""The calculator table: merge CPU per inserted row, and as a multiple of the
insert CPU, measured at the end of each run and projected to a partition of
N = 1e3, 1e4, 1e5 inserted parts.

Model (checked by validate_fit.py): each extra e-fold of parts in the
partition adds b rewrites per inserted row, and an upper-level rewrite costs
c µs per row, so
    M(N) = M(N_end) + b · ln(N / N_end) · c        for N > N_end
b is the slope of rewrites per row against ln N from the table's ~10k-row run
(2,000–3,600 parts; the ~100k runs have too few parts to fit, and their slopes
agree where they can be fitted); c is the run's own µs per row read in merges
whose output is >= 16x the insert size (the levels further merges will be).

  proj.py > results/projection.md
"""
import json, math, os

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")

# (label, table, run, run whose slope is used)
ROWS = [
    ("traces, random ids", "traces", "traces-10k", "traces-10k"),
    ("traces, random ids", "traces", "traces-100k", "traces-10k"),
    ("traces, testgen ids", "traces", "traces-10k-tg", "traces-10k-tg"),
    ("traces, testgen ids", "traces", "traces-100k-tg", "traces-10k-tg"),
    ("logs", "logs", "logs-10k", "logs-10k"),
    ("logs", "logs", "logs-100k", "logs-10k"),
    ("metrics number points (gauge+sum)", "number", "metrics-10k", "metrics-10k"),
    ("metrics number points (gauge+sum)", "number", "metrics-100k", "metrics-10k"),
    ("metrics histogram points", "histogram", "metrics-10k", "metrics-10k"),
    ("metrics histogram points", "histogram", "metrics-100k", "metrics-10k"),
    ("metrics exp. histogram points", "exponential_histogram", "metrics-10k", "metrics-10k"),
    ("metrics exp. histogram points", "exponential_histogram", "metrics-100k", "metrics-10k"),
    ("metrics summary points", "summary", "metrics-10k", "metrics-10k"),
    ("metrics summary points", "summary", "metrics-100k", "metrics-10k"),
    ("metrics series table", "series", "metrics-10k", None),
    ("metrics series table", "series", "metrics-100k", None),
    ("ClickStack otel_metrics_sum", "A_sum", "clickstack-100k", None),
    ("ClickStack otel_metrics_histogram", "A_histogram", "clickstack-10k", "clickstack-10k"),
    ("ClickStack otel_metrics_histogram", "A_histogram", "clickstack-100k", "clickstack-10k"),
]

# calculator weights for the metrics blend (40% sum, 30% gauge, 20% histogram, 5% exp, 5% summary)
BLEND = {"number": 0.70, "histogram": 0.20, "exponential_histogram": 0.05, "summary": 0.05}


def summ(run):
    p = os.path.join(RES, run, "summary.json")
    return json.load(open(p)) if os.path.exists(p) else None


def upper_cost(t):
    rows = cpu = 0
    for k, v in t["size_classes"].items():
        lo = int(k.split("-")[0])
        if lo >= 16 and v["us_per_row_in"]:
            rows += v["rows_in"]
            cpu += v["rows_in"] * v["us_per_row_in"]
    return cpu / rows if rows else t["with_drain"]["us_per_row_in"]


def project():
    out = []
    for label, tbl, run, slope_run in ROWS:
        s = summ(run)
        if not s or tbl not in s["tables"]:
            continue
        t = s["tables"][tbl]
        fit = t.get("fit") or {}
        n_end = fit.get("parts_reached") or t["statements"]
        m_end = t["merge_us_per_inserted_row"]
        ins = t["insert_us_per_row"]
        r = {"label": label, "table": tbl, "run": run, "rows": t["rows_per_statement"], "N_end": n_end, "insert_us": ins,
             "merge_us_end": m_end, "x_end": m_end / ins, "rewrites_end": t["rows_rewritten_per_inserted_row"],
             "bytes_amp": t["bytes_written_per_inserted_byte"], "stored_B": t["stored_bytes_per_row"], "proj": {}}
        if slope_run:
            b = summ(slope_run)["tables"][tbl]["fit"]["b_per_ln"]
            c = upper_cost(t)
            r["b"], r["c_upper"] = b, c
            for N in (1e3, 1e4, 1e5):
                m = m_end + b * max(0.0, math.log(N / n_end)) * c
                r["proj"][int(N)] = (m, m / ins)
        out.append(r)
    return out


def main():
    rs = project()
    print("| table | rows/insert | parts reached | insert µs/row [M] | merge µs/ins. row at end [M] | merge ÷ insert at end [M] "
          "| merge ÷ insert, N=1e3 | N=1e4 | N=1e5 [E] | merge µs/row at N=1e4 [E] |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in rs:
        p = r["proj"]
        f = lambda N: f"{p[N][1]:.1f}×" if N in p else "–"
        m4 = f"{p[10000][0]:.1f}" if 10000 in p else "–"
        print(f"| {r['label']} | {r['rows']:,} | {r['N_end']:,} | {r['insert_us']:.2f} | {r['merge_us_end']:.2f} | "
              f"{r['x_end']:.2f}× | {f(1000)} | {f(10000)} | {f(100000)} | {m4} |")
    # metrics blend
    for size, run in (("10k", "metrics-10k"), ("100k", "metrics-100k")):
        sel = [r for r in rs if r["run"] == run and r["table"] in BLEND]
        if len(sel) < 4:
            continue
        ins = sum(BLEND[r["table"]] * r["insert_us"] for r in sel)
        me = sum(BLEND[r["table"]] * r["merge_us_end"] for r in sel)
        line = f"| metrics blend (calculator weights) | {size} | – | {ins:.2f} | {me:.2f} | {me / ins:.2f}× |"
        for N in (1000, 10000, 100000):
            m = sum(BLEND[r["table"]] * r["proj"][N][0] for r in sel)
            line += f" {m / ins:.1f}× |"
        m4 = sum(BLEND[r["table"]] * r["proj"][10000][0] for r in sel)
        print(line + f" {m4:.1f} |")
    print()
    print("| table | run | rewrites per inserted row at end [M] | compressed bytes written by merges per inserted byte [M] | "
          "stored B/row [M] | slope: rewrites per decade of parts [M] | µs per rewritten row, upper levels [M] |")
    print("|---|---|---|---|---|---|---|")
    for r in rs:
        b = f"{r['b'] * math.log(10):.2f}" if "b" in r else "–"
        c = f"{r['c_upper']:.2f}" if "c_upper" in r else "–"
        print(f"| {r['label']} | {r['run']} | {r['rewrites_end']:.2f} | {r['bytes_amp']:.2f} | {r['stored_B']} | {b} | {c} |")


if __name__ == "__main__":
    main()
