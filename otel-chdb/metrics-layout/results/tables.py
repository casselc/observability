#!/usr/bin/env python3
"""Prints the README's query table from queries-ab.jsonl, queries-c.jsonl
and promtsdb-*.json: server CPU ms (median of 5) / MB read, per window."""
import json, os
h = os.path.dirname(os.path.abspath(__file__))
r = {}
for f in ("queries-ab.jsonl", "queries-c.jsonl"):
    for l in open(os.path.join(h, f)):
        d = json.loads(l)
        r[(d["Query"], d["Layout"], d["Window"])] = (d["CPUms"], d["WallMs"], d["ReadRows"], d["ReadBytes"] / 1e6)
for nat, f in (("D", "promtsdb-classic.json"), ("Dn", "promtsdb-native.json")):
    for q in json.load(open(os.path.join(h, f)))["queries"]:
        r[(q["query"], nat, q["window"])] = (q["cpuMs"], q["wallMs"], None, None)
def cell(k):
    if k not in r: return "–"
    c, w, rows, mb = r[k]
    s = f"{c:.0f}" if c >= 10 else f"{c:.1f}"
    if mb is not None: s += f" / {mb:.1f} MB"
    return s
print("| query | window | A | B | B via view | C (3 services) | D classic (4 services) | D native (4 services) |")
print("| --- | --- | --- | --- | --- | --- | --- | --- |")
for q in ("Q1", "Q2", "Q3", "Q4", "H1", "H2"):
    for w in ("1h", "6h"):
        print(f"| {q} | {w} | " + " | ".join(cell((q, l, w)) for l in ("A", "B", "BV", "C", "D", "Dn")) + " |")
print()
print("wall ms:")
for q in ("Q1", "Q2", "Q3", "Q4", "H1", "H2"):
    for w in ("1h", "6h"):
        print(q, w, {l: round(r[(q, l, w)][1]) for l in ("A", "B", "BV", "C", "D", "Dn") if (q, l, w) in r})
