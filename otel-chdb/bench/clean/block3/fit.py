#!/usr/bin/env python3
"""Extrapolate merge cost to a production-sized partition.

Every row is rewritten once per merge level it passes through, so rows
rewritten per inserted row grows with the log of how many inserted parts the
partition has taken. From part_log (NewPart and MergeParts, in time order) this
takes the cumulative curve R(N) = rows read by merges / rows inserted, after N
inserted parts, fits R = a + b·ln N over N >= 50, and projects it, with the
run's measured merge µs per rewritten row, to partitions of 1e3 … 1e5 parts.

  fit.py results/<run> [...]      (after analyze.py; adds "fit" to summary.json)
"""
import json, math, os, sys
from analyze import TABLE_OF, load, ts, num


def curve(pl, tbl):
    ev = sorted((p for p in pl if p["table"] == tbl and p["event_type"] in ("NewPart", "MergeParts") and not p["error"]
                 and p.get("merge_reason", "RegularMerge") in ("RegularMerge", "NotAMerge")),
                key=lambda p: p["event_time_microseconds"])
    n = ins = rew = cpu = 0
    pts = []
    for p in ev:
        if p["event_type"] == "NewPart":
            n += 1
            ins += int(p["rows"])
        else:
            rew += int(p["read_rows"])
            cpu += num(p["cpu_us"])
        if n and (not pts or pts[-1][0] != n):
            pts.append((n, rew / ins, cpu / ins))
        elif n:
            pts[-1] = (n, rew / ins, cpu / ins)
    return pts


def lsq(xs, ys):
    k = len(xs)
    mx, my = sum(xs) / k, sum(ys) / k
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    return my - b * mx, b


def main():
    for d in sys.argv[1:]:
        args, run, pl, ql, ins, samples = load(d)
        sm = json.load(open(os.path.join(d, "summary.json")))
        for name, t in sm["tables"].items():
            pts = curve(pl, TABLE_OF[name])
            use = [(math.log(n), r, m) for n, r, m in pts if n >= 50]
            if len(use) < 10:
                continue
            # thin to one point per 1% of ln N so the dense tail doesn't dominate
            thin, last = [], None
            for x, y, m in use:
                if last is None or x - last >= 0.01:
                    thin.append((x, y, m))
                    last = x
            a, b = lsq([x for x, _, _ in thin], [y for _, y, _ in thin])
            am, bm = lsq([x for x, _, _ in thin], [m for _, _, m in thin])
            c = t["with_drain"]["us_per_row_in"]
            ins_us = t["insert_us_per_row"]
            proj = {}
            for N in (1e3, 1e4, 1e5):
                R = a + b * math.log(N)
                M = am + bm * math.log(N)
                proj[f"{int(N)}"] = {"rewrites_per_row": round(R, 2), "merge_us_per_row": round(M, 2),
                                     "merge_over_insert": round(M / ins_us, 2)}
            t["fit"] = {"a": round(a, 3), "b_per_ln": round(b, 3), "per_decade": round(b * math.log(10), 3),
                        "fan_in_equiv": round(math.exp(1 / b), 2) if b > 0 else None,
                        "cpu_a_us": round(am, 3), "cpu_b_us_per_ln": round(bm, 3), "cpu_per_decade_us": round(bm * math.log(10), 3),
                        "M_at_end_us": round(pts[-1][2], 3),
                        "parts_reached": pts[-1][0], "R_at_end": round(pts[-1][1], 3), "projection": proj}
            print(f"{sm['run']:>18} {name:>22}: R = {a:.2f} + {b:.3f} ln N  ({b * math.log(10):.2f} per decade), "
                  f"N_end={pts[-1][0]} R_end={pts[-1][1]:.2f}; M = {am:.2f} + {bm:.2f} ln N µs (end {pts[-1][2]:.2f}); "
                  + ", ".join(f"N={k}: R={v['rewrites_per_row']} ×{v['merge_over_insert']}" for k, v in proj.items()))
        json.dump(sm, open(os.path.join(d, "summary.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
