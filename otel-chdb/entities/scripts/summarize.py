#!/usr/bin/env python3
"""Markdown tables from the results/*.jsonl files (README §Results).

  summarize.py bench|queries|conformance [file]
"""
import collections, json, statistics, sys
import chlib as c

R = f"{c.HERE}/../results"


def med(xs):
    return statistics.median(xs) if xs else float("nan")


def rng(xs):
    return f"[{min(xs):.2f}–{max(xs):.2f}]" if xs else ""


def bench(path=f"{R}/bench.jsonl", mpath=f"{R}/merge.jsonl"):
    mby = collections.defaultdict(list)
    try:
        for l in open(mpath):
            r = json.loads(l)
            mby[r["variant"]].append(r)
    except FileNotFoundError:
        pass
    by = collections.defaultdict(list)
    for l in open(path):
        r = json.loads(l)
        by[r["variant"]].append(r)
    print("| variant | reps | insert µs/span | insert µs/log | merge µs/span | merge µs/log | B/span | B/log | of which indexes span / log | load (1-min) |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for v, rs in by.items():
        rs = [r for r in rs if r["rep"] > 0] or rs
        def per(sig, k):
            return [r[k][sig]["cpu_us"] / r["insert"][sig]["rows"] for r in rs]
        ti, li = per("traces", "insert"), per("logs", "insert")
        mr = [r for r in mby.get(v, []) if r["rep"] > 0] or mby.get(v, [])
        tm = [r["merge"]["traces"]["cpu_us"] / r["merge"]["traces"]["rows"] for r in mr]
        lm = [r["merge"]["logs"]["cpu_us"] / r["merge"]["logs"]["rows"] for r in mr]
        last = rs[-1]["after"]
        tt = next(t for t in last if t.startswith("otel_traces") and "rollup" not in t)
        lt = next(t for t in last if t.startswith("otel_logs") and "rollup" not in t)
        tb, lb = last[tt]["bytes"] / last[tt]["rows"], last[lt]["bytes"] / last[lt]["rows"]
        tix, lix = last[tt]["idx"] / last[tt]["rows"], last[lt]["idx"] / last[lt]["rows"]
        loads = [r["load_before"][0] for r in rs]
        print(f"| {v} | {len(rs)} | {med(ti):.2f} {rng(ti)} | {med(li):.2f} {rng(li)} | {med(tm):.2f} {rng(tm)} | {med(lm):.2f} {rng(lm)} | "
              f"{tb:.1f} | {lb:.1f} | {tix:.1f} / {lix:.1f} | {min(loads):.1f}–{max(loads):.1f} |")


def queries(path=f"{R}/queries.jsonl"):
    rec = [json.loads(l) for l in open(path)][-1]
    modes = ["a", "view", "c", "rw"]
    print(f"values: {rec['values']}; load {rec['load_before']} -> {rec['load_after']}\n")
    print("| scenario | statements | " + " | ".join(f"{m} ms" for m in modes) + " | " + " | ".join(f"{m} rows read" for m in modes) + " |")
    print("|---|---:|" + "---:|" * (2 * len(modes)))
    tot = collections.Counter()
    for n, d in rec["results"].items():
        ms, rows = [], []
        for m in modes:
            st = d.get(m)
            if not st:
                ms.append("–"); rows.append("–"); continue
            if any(x.get("error") for x in st):
                ms.append("error"); rows.append("–"); continue
            t = sum(x["ms"] for x in st)
            tot[m] += t
            ms.append(f"{t:,.1f}")
            rows.append(f"{sum(x['rows'] for x in st):,}")
        k = len(next(iter(d.values())))
        print(f"| {n} | {k} | " + " | ".join(ms) + " | " + " | ".join(rows) + " |")
    print("| **sum** | | " + " | ".join(f"**{tot[m]:,.0f}**" for m in modes) + " | | | | |")


def conformance(path=f"{R}/conformance.jsonl"):
    print("| variant | when | signal | rows (a) | rows (view) | sum(cityHash64) equal | a EXCEPT ALL b | b EXCEPT ALL a | rows the catalog could not rebuild |")
    print("|---|---|---|---:|---:|---|---:|---:|---:|")
    for l in open(path):
        r = json.loads(l)
        print(f"| {r['variant']} | {r['label']} | {r['signal']} | {r['count_a']:,} | {r['count_b']:,} | {'yes' if r['hash_equal'] else 'no'} | "
              f"{r['a_except_b']:,} | {r['b_except_a']:,} | {r['rows_unknown_to_catalog'] if r['rows_unknown_to_catalog'] is not None else '–'} |")


if __name__ == "__main__":
    {"bench": bench, "queries": queries, "conformance": conformance}[sys.argv[1]](*sys.argv[2:])
