#!/usr/bin/env python3
"""Block 4 table: stored bytes per row (data_compressed_bytes after OPTIMIZE
FINAL), replayed pool vs no-replay pool, next to the earlier values
(metrics-layout/README.md; bench/merges/results/projection.md for traces/logs).

  summary.py > results.md
"""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))
EARLIER = {"traces-random-ids": "40.9", "logs-random-ids": "18.9", "traces-testgen-ids": "9.4", "B-gauge": "1.45", "B-sum": "1.36",
           "B-histogram": "13.8", "B-exponential_histogram": "38.1", "B-summary": "11.7", "B-series (5 cycles)": "39.6 per series row",
           "A-sum": "20.4", "A-gauge": "19.1", "A-histogram": "38.1", "A-exponential_histogram": "57.3", "A-summary": "33.8",
           "B-number (gauge+sum)": "–"}


def main():
    r = {x["label"]: x for x in map(json.loads, open(os.path.join(HERE, "results.jsonl")))}
    print("| table | rows | B/row, replayed pool | B/row, no replay | earlier (loaded box) | top columns, B/row (no replay where measured) |")
    print("|---|---|---|---|---|---|")
    for lab in [l for l in r if not l.endswith("no replay")]:
        a, b = r[lab], r.get(lab + " no replay")
        top = ", ".join(f"{k} {v}" for k, v in list((b or a)["columns_B_per_row"].items())[:3])
        print(f"| {lab} | {(b or a)['rows']:,} | {a['data_compressed_B_per_row']:.2f} | {b['data_compressed_B_per_row']:.2f} |" if b else
              f"| {lab} | {a['rows']:,} | {a['data_compressed_B_per_row']:.2f} | – |", end="")
        print(f" {EARLIER.get(lab, '–')} | {top} |")


if __name__ == "__main__":
    main()
