#!/usr/bin/env python3
"""Summarizes scripts/input_bench.sh (or durable.sh's cost.jsonl with
--durable): median [min-max] per configuration."""
import json, sys

rows = [json.loads(l) for l in open(sys.argv[-1]) if l.strip()]
durable = "--durable" in sys.argv


def m(v, f, fmt="{:.1f}"):
    xs = sorted(x[f] for x in v if x.get(f) is not None)
    if not xs:
        return "–"
    return f"{fmt.format(xs[len(xs) // 2])} [{fmt.format(xs[0])}–{fmt.format(xs[-1])}]"


loads = sorted(float(r["load"]) for r in rows)
print(f"load average {loads[0]:.2f}–{loads[-1]:.2f}, {len(rows)} runs\n")
g = {}
if durable:
    for r in rows:
        g.setdefault(r["config"], []).append(r)
    print("| config | edge CPU ms / 10k-span request | disk written KB / request | buffer dir after 30 requests, KB | client ack median ms | send start to all 30 committed, s |")
    print("|---|---|---|---|---|---|")
    for k, v in g.items():
        print(f"| {k} | {m(v, 'cpu_ms_per_request')} | {m(v, 'disk_write_kb_per_request', '{:.0f}')} | {m(v, 'buffer_dir_kb_after', '{:.0f}')} | "
              f"{m(v, 'median_ack_ms')} | {m(v, 'send_to_all_committed_s', '{:.2f}')} |")
else:
    for r in rows:
        g.setdefault((r["signal"], r["transport"]), []).append(r)
    print("| signal (10k items / request) | transport | edge process CPU ms / request | peak RSS MB | client ack median ms | sender encode ms (OTAP producer) |")
    print("|---|---|---|---|---|---|")
    for (s, t), v in g.items():
        print(f"| {s} | {t} | {m(v, 'cpu_ms_per_request')} | {m(v, 'peak_rss_mb', '{:.0f}')} | {m(v, 'median_ack_ms')} | {m(v, 'sender_encode_ms')} |")
