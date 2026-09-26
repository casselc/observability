#!/usr/bin/env python3
"""Runs of one table side by side at the same number of inserted parts N:
rows rewritten per inserted row R(N), cumulative merge CPU per inserted row
M(N), insert CPU per row, and (insert + merge) per row.

  compare.py N table results/<run> [...]
"""
import json, os, sys
from analyze import TABLE_OF, load, num
from fit import curve


def main():
    N, name = int(sys.argv[1]), sys.argv[2]
    print(f"| run | N | rewrites/row R(N) | merge µs per inserted row M(N) | insert µs/row | merge÷insert at N | insert+merge µs/row |")
    print("|---|---|---|---|---|---|---|")
    for d in sys.argv[3:]:
        args, run, pl, ql, ins, samples = load(d)
        pts = curve(pl, TABLE_OF[name])
        at = [p for p in pts if p[0] <= N]
        if not at:
            continue
        n, R, M = at[-1]
        sm = json.load(open(os.path.join(d, "summary.json")))
        iu = sm["tables"][name]["insert_us_per_row"]
        print(f"| {os.path.basename(d)} | {n} | {R:.2f} | {M:.2f} | {iu:.2f} | {M / iu:.2f} | {iu + M:.2f} |")


if __name__ == "__main__":
    main()
