#!/usr/bin/env python3
"""Block 5 analysis, next to the earlier (loaded-box) consumer results in
../../../otap-rs/results/consumer/{latency,bench}.jsonl.

a. steady state (latency.jsonl): per poll, median [min–max] over repetitions
   of visibility, objects per statement, consumer CPU per object and per row,
   server CPU per object (system.events, server-wide: merges included).
b. throughput (drain.jsonl + raw/stats-*.json): one worker draining block 2's
   roots. rows/s per worker = rows / (elapsed - 2 s), the consumer exiting
   after 2 s idle (--exit-after-idle 2s), so the denominator is the time to
   the last statement to within one poll. Consumer CPU per object / per row
   from its own stats (getrusage), server CPU per object from system.events.

  analyze.py > results.md
"""
import json, os, statistics as st

HERE = os.path.dirname(os.path.abspath(__file__))
OLD = os.path.join(HERE, "..", "..", "..", "otap-rs", "results", "consumer")


def lines(p):
    return [json.loads(l) for l in open(p) if l.strip()] if os.path.exists(p) else []


def mm(xs, f="{:.2f}"):
    xs = sorted(xs)
    return f"{f.format(st.median(xs))} [{f.format(xs[0])}–{f.format(xs[-1])}]" if xs else "–"


def main():
    lat = lines(os.path.join(HERE, "latency.jsonl"))
    old = {r["poll"]: r for r in lines(os.path.join(OLD, "latency.jsonl"))}
    print("### a. Steady state: one edge, one worker, 3.7 requests/s per signal, 90 s per run\n")
    print("| poll | n | visible p50 ms | visible p99 ms | objects/stmt | consumer CPU ms/object | consumer CPU µs/row | server CPU ms/object |")
    print("|---|---|---|---|---|---|---|---|")
    for poll in ("200ms", "1s"):
        g = [r for r in lat if r["poll"] == poll]
        o = old.get(poll)
        print(f"| {poll} (clean) | {len(g)} | {mm([r['visible_ms_p50'] for r in g], '{:.0f}')} | {mm([r['visible_ms_p99'] for r in g], '{:.0f}')} "
              f"| {mm([r['objects_per_statement'] for r in g])} | {mm([r['consumer_cpu_ms_per_object'] for r in g], '{:.3f}')} "
              f"| {mm([r['consumer_cpu_us_per_row'] for r in g])} | {mm([r['server_cpu_ms_per_object'] for r in g], '{:.1f}')} |")
        if o:
            print(f"| {poll} (earlier, loaded box, n=1) | 1 | {o['visible_ms_p50']} | {o['visible_ms_p99']} | {o['objects_per_statement']} "
                  f"| {o['consumer_cpu_ms_per_object']} | {o['consumer_cpu_us_per_row']} | {o['server_cpu_ms_per_object']} |")
    dr = lines(os.path.join(HERE, "drain.jsonl"))
    rows = []
    for d in dr:
        p = os.path.join(HERE, "raw", f"stats-{d['root']}-{d['poll']}-r{d['rep']}.json")
        if not os.path.exists(p):
            continue
        s = json.load(open(p))
        objs = s["objects_inserted"] + s["series_objects_inserted"]
        secs = s["elapsed_ms"] / 1000 - 2.0
        rows.append({"root": d["root"], "poll": d["poll"], "rep": d["rep"], "rows": s["rows_inserted"], "objects": objs,
                     "statements": s["statements"], "rows_s": s["rows_inserted"] / secs, "objs_s": objs / secs,
                     "c_obj": s["cpu_ms"] / objs, "c_row": s["cpu_ms"] * 1000 / s["rows_inserted"],
                     "srv_obj": d["server_cpu_us"] / 1000 / objs, "errors": s.get("errors", 0) + s.get("insert_errors", 0)})
    print("\n### b. Throughput: one worker draining a backlog (block 2's roots), --max-batch 32 (default)\n")
    print("| root | poll | n | objects | rows | statements | rows/s per worker | objects/s | consumer CPU ms/object | consumer CPU µs/row "
          "| server CPU ms/object | errors |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for root in ("small", "large"):
        for poll in ("200ms", "1s"):
            g = [r for r in rows if r["root"] == root and r["poll"] == poll]
            if not g:
                continue
            print(f"| {root} | {poll} | {len(g)} | {g[0]['objects']} | {g[0]['rows']:,} | {g[0]['statements']} | {mm([r['rows_s'] for r in g], '{:,.0f}')} "
                  f"| {mm([r['objs_s'] for r in g], '{:.0f}')} | {mm([r['c_obj'] for r in g], '{:.3f}')} | {mm([r['c_row'] for r in g], '{:.3f}')} "
                  f"| {mm([r['srv_obj'] for r in g], '{:.1f}')} | {sum(r['errors'] for r in g)} |")
    ob = [r for r in lines(os.path.join(OLD, "bench.jsonl")) if r["batch"] == 32]
    if ob:
        print(f"\nEarlier (loaded box, `consumer_bench.sh`, 2,800 small objects, 32 per statement, n={len(ob)}): "
              f"rows/s per worker {mm([r['rows'] / (r['wall_ms'] / 1000) for r in ob], '{:,.0f}')}, consumer CPU "
              f"{mm([r['consumer_cpu_ms'] / r['objects'] for r in ob], '{:.3f}')} ms/object "
              f"({mm([r['consumer_cpu_ms'] * 1000 / r['rows'] for r in ob], '{:.2f}')} µs/row), server CPU "
              f"{mm([r['server_cpu_ms_per_object'] for r in ob], '{:.1f}')} ms/object.")


if __name__ == "__main__":
    main()
