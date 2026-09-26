#!/usr/bin/env python3
"""Summarise an env log (lib/env.sh): per run (a 'start X' .. next 'end X'
pair, or an explicit 'begin L' .. 'finish L' pair), the load average at the
start, the run queue, and CPU steal = steal jiffies delta / total delta.
Flags any run with steal > 2%, and a repetition ('begin'/'finish') whose
start load was > 0.5. Single processes inside a repetition start at the load
the repetition's own earlier processes left (one process at a time), so for
them the load is recorded, not gated.

  envlog.py ENVLOG [--json]         per-run table + totals
  envlog.py ENVLOG --bad            only the flagged runs (exit 1 if any)
"""
import json, sys

def load(path):
    return [json.loads(l) for l in open(path) if l.strip()]

def steal(a, b):
    d = [y - x for x, y in zip(a["cpu"], b["cpu"])]
    tot = sum(d)
    return (d[7] / tot * 100 if tot else 0.0), tot

def runs(rows):
    out, open_ = [], {}
    for r in rows:
        if "cpu" not in r:
            continue
        ev = r["ev"]
        w = ev.split(" ", 2)
        if w[0] in ("start", "begin"):
            open_[w[1]] = r
        elif w[0] in ("end", "finish") and w[1] in open_:
            a = open_.pop(w[1])
            st, tot = steal(a, r)
            out.append({"what": a["ev"][:140], "t0": a["t"], "secs": round(r["t"] - a["t"], 2), "load_start": a["load1"],
                        "procs_start": a["procs"], "steal_pct": round(st, 3),
                        "kind": w[0] if w[0] == "finish" else "proc",
                        "bad": st > 2.0 or (w[0] == "finish" and a["load1"] > 0.5)})
    return out

def main():
    rows = load(sys.argv[1])
    rs = runs(rows)
    if "--json" in sys.argv:
        for r in rs:
            print(json.dumps(r))
        return
    bad = [r for r in rs if r["bad"]]
    if "--bad" in sys.argv:
        for r in bad:
            print(json.dumps(r))
        sys.exit(1 if bad else 0)
    hdr = [r for r in rows if r["ev"].startswith("header")]
    for h in hdr:
        print(json.dumps(h))
    cr = [r for r in rows if "cpu" in r]
    st, tot = steal(cr[0], cr[-1])
    print(f"runs {len(rs)}, flagged {len(bad)}; whole log: steal {st:.3f}% over {cr[-1]['t'] - cr[0]['t']:.0f} s")
    if rs:
        ls = sorted(r["load_start"] for r in rs)
        ss = sorted(r["steal_pct"] for r in rs)
        print(f"load at start: median {ls[len(ls)//2]} max {ls[-1]}; steal per run: median {ss[len(ss)//2]}% max {ss[-1]}%")

if __name__ == "__main__":
    main()
