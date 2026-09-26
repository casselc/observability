#!/usr/bin/env python3
"""Time to steady state: when the first merge of each output size class
(output rows / insert rows: 4-16x, 16-64x, …) finished, after how many
inserted parts; the active parts by level at the end of inserts; and the
per-minute merge÷insert ratio after the first minutes.

  steady.py results/<run> [...]
"""
import json, math, os, sys
from analyze import TABLE_OF, load, ts, num


def main():
    print("| run | table | first merge finished, by output size (× insert): s after start / parts inserted | "
          "active parts by level at end of inserts | merge÷insert per minute, min–median–max (minutes 3+) |")
    print("|---|---|---|---|---|")
    for d in sys.argv[1:]:
        args, run, pl, ql, ins, samples = load(d)
        t0 = run["t0"]
        sm = json.load(open(os.path.join(d, "summary.json")))
        for name in args["tables"].split(","):
            tbl = TABLE_OF[name]
            t = sm["tables"][name]
            per = t["rows_per_statement"]
            ev = sorted((p for p in pl if p["table"] == tbl and p["event_type"] in ("NewPart", "MergeParts") and not p["error"]),
                        key=lambda p: p["event_time_microseconds"])
            n, first = 0, {}
            for p in ev:
                if p["event_type"] == "NewPart":
                    n += 1
                    continue
                k = int(math.log(max(1.0, int(p["rows"]) / per), 4))
                if k not in first:
                    first[k] = (ts(p["event_time_microseconds"]) - t0, n)
            fs = ", ".join(f"{4 ** k}–{4 ** (k + 1)}×: {first[k][0]:.0f} s / {first[k][1]}" for k in sorted(first))
            end = [s for s in samples if s["wall"] <= run["t_ins_end"]]
            lv = end[-1]["tables"].get(tbl, {}).get("levels", {}) if end else {}
            rs = sorted(c["ratio"] for c in t["curve"][2:] if c["ratio"] is not None and c["ins_rows"] > 0.8 * max(x["ins_rows"] for x in t["curve"]))
            rr = f"{rs[0]:.2f} – {rs[len(rs) // 2]:.2f} – {rs[-1]:.2f}" if rs else "–"
            print(f"| {os.path.basename(d)} | {name} | {fs} | {json.dumps(lv)} | {rr} |")


if __name__ == "__main__":
    main()
