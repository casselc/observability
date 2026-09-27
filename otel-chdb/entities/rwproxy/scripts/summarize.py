#!/usr/bin/env python3
"""Markdown tables from ../results/{correctness,bench}.jsonl (the last run of
each kind and label).

  summarize.py correctness|scenarios|overhead|hop
"""
import json, os, statistics, sys

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "..", "results")


def lines(name):
    return [json.loads(l) for l in open(os.path.join(RES, name)) if l.strip()]


def correctness():
    last = {}
    for r in lines("correctness.jsonl"):
        last[r["label"]] = r
    print("| state | set | statements | mode | rewritten | equal | equal-rows | both-error | estimate | nondet | MISMATCH |")
    print("|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|")
    for label, r in last.items():
        for s, per in r["sets"].items():
            for m, d in per.items():
                x = d["results"]
                print(f"| {label} | {s} | {d['statements']} | {m} | {d['rewritten']} | {x.get('equal', 0)} | {x.get('equal-rows', 0)} | "
                      f"{x.get('both-error', 0)} | {x.get('estimate', 0)} | {x.get('nondet', 0)} | **{x.get('MISMATCH', 0)}** |")


def scenarios():
    r = [x for x in lines("bench.jsonl") if x["what"] == "scenarios"][-1]
    ways = ["a", "c", "c+rw direct", "c+proxy", "c+proxy cat"]
    print(f"load {r['load_before']} -> {r['load_after']}, {r['rounds']} rounds, {r['ts']}\n")
    # the proxy's share: per statement, median(wall - server) through the proxy
    # minus the same sent directly (server time varies more than the hop)
    def hop(d):
        tot = 0.0
        for i, (ws, ss) in enumerate(zip(d["c+proxy"]["wall_all"], d["c+proxy"]["server_all"])):
            pw = statistics.median(x - y for x, y in zip(ws, ss))
            dw = statistics.median(x - y for x, y in zip(d["c+rw direct"]["wall_all"][i], d["c+rw direct"]["server_all"][i]))
            tot += pw - dw
        return tot
    print("| scenario | stmts | " + " | ".join(f"{w} server" for w in ways) + " | proxy hop (wall − server, vs direct) |")
    print("|---|---:|" + "---:|" * (len(ways) + 1))
    tot = {w: 0.0 for w in ways}
    for n, d in r["results"].items():
        cells = []
        for w in ways:
            if w in d:
                cells.append(f"{d[w]['server_ms']:.1f}")
                tot[w] += d[w]["server_ms"]
            else:
                cells.append("–")
        ov = hop(d)
        print(f"| {n} | {d['c']['statements']} | " + " | ".join(cells) + f" | {ov:+.2f} |")
    print("| **sum** | | " + " | ".join(f"**{tot[w]:,.0f}**" for w in ways) + " | |")
    print()
    for m, p in r["proxy"].items():
        print(f"- proxy {m}: {p}")


def overhead():
    r = [x for x in lines("bench.jsonl") if x["what"] == "overhead"][-1]
    print(json.dumps(r["results"], indent=1))
    for m, p in r["proxy"].items():
        print(f"- proxy {m}: {p}")
    print("load", r["load_before"], "->", r["load_after"])


def hop():
    r = [x for x in lines("bench.jsonl") if x["what"] == "hop"][-1]
    print(f"load {r['load_before']} -> {r['load_after']}\n")
    print("| request | n | bytes | direct p50 ms | through the proxy p50 ms | added p50 / p90 / p99 ms | proxy CPU µs per request |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for n, d in r["results"].items():
        x = d["diff_ms"]
        print(f"| {n} | {d['n']} | {d['response_bytes']:,} | {d['direct_ms_p50']} | {d['proxy_ms_p50']} | {x['p50']} / {x['p90']} / {x['p99']} | {d['proxy_cpu_us_per_request']:.0f} |")


if __name__ == "__main__":
    globals()[sys.argv[1]]()
