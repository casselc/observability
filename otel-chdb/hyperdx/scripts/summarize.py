#!/usr/bin/env python3
"""Per-scenario latency, as a user of each source sees it, from replay.py's
JSON: the statements HyperDX sent for scenario X on the views (tag X-b) timed
on the views, against the statements it sent for X on the stock tables (tag
X-stock) timed on the stock tables. HyperDX renders different SQL for the two
(it adds primary-key time filters on the stock tables; on the views the picker
takes its exhaustive path), so this is the like-for-like user comparison;
replay.py's own table is the same-SQL comparison.

  summarize.py replay.json [--md out.md] [--exclude REGEX]
"""
import argparse, collections, json, re

ap = argparse.ArgumentParser()
ap.add_argument("json")
ap.add_argument("--md", default="")
ap.add_argument("--exclude", default="", help="regex: statements to leave out (e.g. the alert task's, which run under whatever tag is current)")
a = ap.parse_args()
items = json.load(open(a.json))


def kind(q):
    if "__hdx_time_bucket" in q:
        return "chart"
    if re.search(r"SELECT MetricName\s+FROM", q) or "mergeTreeIndex" in q:
        return "names"
    return "metadata"


by = collections.OrderedDict()
for it in items:
    if a.exclude and re.search(a.exclude, it["query"]):
        continue
    m = re.match(r"(.*)-(b|stock)(-search|-catalog)?$", it["tag"])
    if not m:
        continue
    base, side = m.group(1) + (m.group(3) or ""), m.group(2)
    g = by.setdefault(base, {"b": collections.Counter(), "stock": collections.Counter(), "same": True})
    ms = it["b_ms"] if side == "b" else it["stock_ms"]
    g[side][kind(it["query"])] += ms
    g[side]["all"] += ms
    if side == "b" and kind(it["query"]) == "chart":
        g["same"] = g["same"] and it["same"]

lines = ["| scenario | chart query views / stock ms | ratio | metric-name queries views / stock ms | metadata queries views / stock ms | all statements views / stock ms | ratio |",
         "|---|---:|---:|---:|---:|---:|---:|"]
tot = collections.Counter()
for base, g in by.items():
    b, s = g["b"], g["stock"]
    r1 = f"{b['chart'] / s['chart']:.2f}" if s["chart"] else "–"
    r2 = f"{b['all'] / s['all']:.2f}" if s["all"] else "–"
    lines.append(f"| {base} | {b['chart']:.0f} / {s['chart']:.0f} | {r1} | {b['names']:.0f} / {s['names']:.0f} | "
                 f"{b['metadata']:.0f} / {s['metadata']:.0f} | {b['all']:.0f} / {s['all']:.0f} | {r2} |")
    for k in ("chart", "names", "metadata", "all"):
        tot["b" + k] += b[k]
        tot["s" + k] += s[k]
lines.append(f"| **total** | {tot['bchart']:.0f} / {tot['schart']:.0f} | {tot['bchart'] / max(tot['schart'], 1e-9):.2f} | "
             f"{tot['bnames']:.0f} / {tot['snames']:.0f} | {tot['bmetadata']:.0f} / {tot['smetadata']:.0f} | "
             f"{tot['ball']:.0f} / {tot['sall']:.0f} | {tot['ball'] / max(tot['sall'], 1e-9):.2f} |")
out = "\n".join(lines)
print(out)
if a.md:
    open(a.md, "w").write(out + "\n")
