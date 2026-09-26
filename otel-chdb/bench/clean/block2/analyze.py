#!/usr/bin/env python3
"""Block 2 analysis: per table, the consumer's INSERT statements from
query_log (raw/statements.jsonl.gz), with objects per statement
(= S3HeadObject: s3() HEADs each named key once; checked against the
'.parquet' mentions in the statement, one per key in the URL) and rows (written_rows).

Fit, per table, server CPU of a statement (OSCPUVirtualTimeMicroseconds) as
    cpu = F_stmt + n_objects x F_obj + rows x u_row
by least squares over group means (one point per root x max-batch x rep x
objects-per-statement), so the thousands of small statements don't swamp the
large ones. Also prints each configuration's measured medians directly.

  analyze.py > results.md ; analyze.py --json > fit.json
"""
import gzip, json, os, statistics as st, sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))


def solve(A, y):
    """least squares via normal equations (3 unknowns)."""
    k = len(A[0])
    M = [[sum(r[i] * r[j] for r in A) for j in range(k)] for i in range(k)]
    v = [sum(r[i] * yy for r, yy in zip(A, y)) for i in range(k)]
    for i in range(k):  # Gauss-Jordan
        p = max(range(i, k), key=lambda r: abs(M[r][i]))
        M[i], M[p], v[i], v[p] = M[p], M[i], v[p], v[i]
        for r in range(k):
            if r != i and M[i][i]:
                f = M[r][i] / M[i][i]
                M[r] = [a - f * b for a, b in zip(M[r], M[i])]
                v[r] -= f * v[i]
    return [v[i] / M[i][i] for i in range(k)]


def main():
    rows = [json.loads(l) for l in gzip.open(os.path.join(HERE, "raw", "statements.jsonl.gz"), "rt") if l.strip()]
    rows = [r for r in rows if not int(r["exception_code"])]
    by = defaultdict(list)
    mism = 0
    for r in rows:
        t = [x for x in r["tables"] if not x.startswith("_table_function")][0].split(".", 1)[1]
        n = int(r["heads"])
        if n != int(r["parquet_mentions"]):
            mism += 1
        r["n"], r["rows"], r["cpu"] = n, int(r["written_rows"]), float(r["cpu_us"])
        by[t].append(r)
    out = {}
    print(f"statements: {len(rows)}; objects-per-statement vs S3HeadObject mismatches: {mism}\n")
    print("| table | root | max-batch | statements | objects/stmt | rows/stmt | CPU ms/stmt, median [min–max] | CPU ms per object | µs per row |")
    print("|---|---|---|---|---|---|---|---|---|")
    for t in sorted(by):
        rs = by[t]
        cfg = defaultdict(list)
        for r in rs:
            cfg[(r["root"], int(r["max_batch"]))].append(r)
        for (root, mb), g in sorted(cfg.items()):
            c = sorted(x["cpu"] / 1e3 for x in g)
            n = st.median(x["n"] for x in g)
            ro = st.median(x["rows"] for x in g)
            per_obj = st.median(x["cpu"] / 1e3 / x["n"] for x in g if x["n"])
            per_row = st.median(x["cpu"] / x["rows"] for x in g if x["rows"])
            print(f"| {t} | {root} | {mb} | {len(g)} | {n:g} | {ro:,.0f} | {st.median(c):.2f} [{c[0]:.2f}–{c[-1]:.2f}] | {per_obj:.2f} | {per_row:.2f} |")
        # group means for the fit
        grp = defaultdict(list)
        for r in rs:
            grp[(r["root"], int(r["max_batch"]), int(r["rep"]), r["n"])].append(r)
        A, y, w = [], [], []
        for g in grp.values():
            A.append([1.0, st.mean(x["n"] for x in g), st.mean(x["rows"] for x in g)])
            y.append(st.mean(x["cpu"] for x in g))
        if len({a[1] for a in A}) < 2 or len({a[2] for a in A}) < 3:
            continue
        a, b, c = solve(A, y)
        # per-rep fits for the spread of the marginal cost
        reps = []
        for rep in sorted({int(r["rep"]) for r in rs}):
            Ar = [row for row, key in zip(A, grp) if key[2] == rep]
            yr = [yy for yy, key in zip(y, grp) if key[2] == rep]
            if len(Ar) >= 3 and len({x[2] for x in Ar}) >= 3:
                reps.append(solve(Ar, yr))
        out[t] = {"F_stmt_ms": a / 1e3, "F_obj_ms": b / 1e3, "u_row_us": c, "points": len(A),
                  "per_rep": [{"F_stmt_ms": x[0] / 1e3, "F_obj_ms": x[1] / 1e3, "u_row_us": x[2]} for x in reps]}
    print("\n| table | per statement, ms | per object, ms | per row, µs | per-rep µs/row [min–max] | fit points |")
    print("|---|---|---|---|---|---|")
    for t, f in sorted(out.items()):
        pr = sorted(x["u_row_us"] for x in f["per_rep"])
        rng = f"{pr[0]:.2f}–{pr[-1]:.2f}" if pr else "–"
        print(f"| {t} | {f['F_stmt_ms']:.2f} | {f['F_obj_ms']:.2f} | {f['u_row_us']:.3f} | {rng} | {f['points']} |")
    if "--json" in sys.argv:
        json.dump(out, open(os.path.join(HERE, "fit.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
