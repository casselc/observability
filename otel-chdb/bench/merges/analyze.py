#!/usr/bin/env python3
"""Per-table merge and insert CPU from one run's raw files (merges.py).

  analyze.py results/<run> [results/<run> …]    writes results/<run>/summary.json and prints tables

Insert CPU: system.query_log, OSCPUVirtualTimeMicroseconds of each INSERT
(QueryFinish). Merge CPU: system.part_log MergeParts rows, the merge's own
ProfileEvents OSCPUVirtualTimeMicroseconds (User+System is reported beside it).
Windows: "ingest" = merges that finished while statements were being sent;
"+drain" also counts those that finished in the drain after the last insert.
"""
import gzip, json, math, os, sys
from collections import defaultdict
from datetime import datetime, timezone


def ts(s):
    return datetime.strptime(s[:26], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()


def load(d):
    def rd(f):
        p = os.path.join(d, f)
        fh = open(p) if os.path.exists(p) else gzip.open(p + ".gz", "rt")
        return [json.loads(l) for l in fh if l.strip()]
    return (json.load(open(os.path.join(d, "args.json"))), json.load(open(os.path.join(d, "run.json"))),
            rd("part_log.jsonl"), rd("query_log.jsonl"), rd("inserts.jsonl"), rd("samples.jsonl"))


TABLE_OF = {"traces": "otel_traces", "logs": "otel_logs", "number": "otel_metrics_number_points",
            "histogram": "otel_metrics_histogram_points", "exponential_histogram": "otel_metrics_exponential_histogram_points",
            "summary": "otel_metrics_summary_points", "series": "otel_metrics_series",
            "A_sum": "otel_metrics_sum", "A_histogram": "otel_metrics_histogram"}


def num(x):
    return float(x or 0)


def analyze(d):
    args, run, pl, ql, ins, samples = load(d)
    t0, t_end_ins = run["t0"], run["t_ins_end"]
    t_after = run.get("t_after", run["t_end"])
    names = args["tables"].split(",")
    q_by_id = {q["query_id"]: q for q in ql if q["type"] == "QueryFinish"}
    out = {"run": args["run"], "rows_per_insert": args["rows"], "tables": {}, "duration_s": round(t_end_ins - t0, 1),
           "stop_reason": run["reason"], "errors": run["n_errors"]}
    for n in names:
        tbl = TABLE_OF[n]
        mine = [i for i in ins if i["table"] == n and i["ok"]]
        qs = [q_by_id[i["qid"]] for i in mine if i["qid"] in q_by_id]
        ins_rows = sum(int(q["written_rows"]) for q in qs)
        ins_cpu = sum(num(q["cpu_us"]) for q in qs)
        ins_us = sum(num(q["user_us"]) + num(q["sys_us"]) for q in qs)
        new = [p for p in pl if p["table"] == tbl and p["event_type"] == "NewPart"]
        new_bytes = sum(int(p["size_in_bytes"]) for p in new)
        new_rows = sum(int(p["rows"]) for p in new)
        new_unc = sum(int(p["bytes_uncompressed"]) for p in new)
        merges = [p for p in pl if p["table"] == tbl and p["event_type"] == "MergeParts" and not p["error"]]
        for m in merges:
            m["t"] = ts(m["event_time_microseconds"])
        ttl_merges = [m for m in merges if m["merge_reason"] != "RegularMerge"]

        def agg(ms):
            r = {"merges": len(ms), "rows_in": sum(int(m["read_rows"]) for m in ms), "rows_out": sum(int(m["rows"]) for m in ms),
                 "bytes_read": sum(int(m["read_bytes"]) for m in ms), "bytes_written": sum(int(m["size_in_bytes"]) for m in ms),
                 "cpu_us": sum(num(m["cpu_us"]) for m in ms), "user_sys_us": sum(num(m["user_us"]) + num(m["sys_us"]) for m in ms),
                 "real_us": sum(num(m["real_us"]) for m in ms)}
            r["us_per_row_in"] = r["cpu_us"] / r["rows_in"] if r["rows_in"] else None
            return r

        w_ing = agg([m for m in merges if m["t"] <= t_end_ins])
        w_all = agg([m for m in merges if m["t"] <= t_after])
        rows_per_ins = ins_rows / len(qs) if qs else 0
        # merge size classes: output rows in multiples of the insert size, by powers of 4
        classes = defaultdict(list)
        for m in merges:
            if m["t"] > t_after:
                continue
            k = int(math.log(max(1.0, int(m["rows"]) / rows_per_ins), 4)) if rows_per_ins else 0
            classes[k].append(m)
        cls = {}
        for k in sorted(classes):
            a = agg(classes[k])
            cls[f"{4 ** k}-{4 ** (k + 1)}x"] = {"merges": a["merges"], "rows_in": a["rows_in"],
                                                 "us_per_row_in": round(a["us_per_row_in"], 3) if a["us_per_row_in"] else None,
                                                 "share_of_cpu": round(a["cpu_us"] / w_all["cpu_us"], 3) if w_all["cpu_us"] else None,
                                                 "algorithms": sorted({m["merge_algorithm"] for m in classes[k]}),
                                                 "avg_sources": round(sum(int(m["sources"]) for m in classes[k]) / len(classes[k]), 1)}
        # per-minute curve: inserted rows, merge CPU, cumulative rewrites per inserted row
        curve = []
        step = 60.0
        tmax = t_after
        k = 0
        while t0 + k * step < tmax:
            a_, b_ = t0 + k * step, t0 + (k + 1) * step
            ir = sum(int(q_by_id[i["qid"]]["written_rows"]) for i in mine if a_ <= i["start"] < b_ and i["qid"] in q_by_id)
            ic = sum(num(q_by_id[i["qid"]]["cpu_us"]) for i in mine if a_ <= i["start"] < b_ and i["qid"] in q_by_id)
            ms = [m for m in merges if a_ <= m["t"] < b_]
            cum_ins = sum(int(q_by_id[i["qid"]]["written_rows"]) for i in mine if i["start"] < b_ and i["qid"] in q_by_id)
            cum_mrg = sum(int(m["read_rows"]) for m in merges if m["t"] < b_)
            mc = sum(num(m["cpu_us"]) for m in ms)
            curve.append({"minute": k + 1, "ins_rows": ir, "ins_cpu_s": round(ic / 1e6, 2), "merge_cpu_s": round(mc / 1e6, 2),
                          "ratio": round(mc / ic, 2) if ic else None, "max_merge_rows": max([int(m["rows"]) for m in ms], default=0),
                          "cum_rewrites_per_row": round(cum_mrg / cum_ins, 2) if cum_ins else None})
            k += 1
        last = samples[-1]["tables"].get(tbl, {}) if samples else {}
        final = next((f for f in run["final_parts"] if f[0] == tbl), None)
        stored = {"rows": int(final[2]), "bytes": int(final[3]), "uncompressed": int(final[4]), "parts": final[1]} if final else {}
        t = {
            "statements": len(qs), "rows_per_statement": round(rows_per_ins), "inserted_rows": ins_rows,
            "achieved_rate": round(len(qs) / (t_end_ins - t0), 2),
            "insert_cpu_s": round(ins_cpu / 1e6, 2), "insert_us_per_row": round(ins_cpu / ins_rows, 3) if ins_rows else None,
            "insert_user_sys_us_per_row": round(ins_us / ins_rows, 3) if ins_rows else None,
            "insert_ms_per_statement": round(ins_cpu / 1e3 / len(qs), 1) if qs else None,
            "new_part_bytes_per_row": round(new_bytes / new_rows, 2) if new_rows else None,
            "new_part_uncompressed_per_row": round(new_unc / new_rows, 1) if new_rows else None,
            "stored_bytes_per_row": round(stored["bytes"] / stored["rows"], 2) if stored.get("rows") else None,
            "final_parts": stored.get("parts"), "final_levels": last.get("levels"),
            "ingest_window": w_ing, "with_drain": w_all,
            "merge_us_per_inserted_row": round(w_all["cpu_us"] / ins_rows, 3) if ins_rows else None,
            "merge_over_insert": round(w_all["cpu_us"] / ins_cpu, 3) if ins_cpu else None,
            "merge_over_insert_ingest_window": round(w_ing["cpu_us"] / ins_cpu, 3) if ins_cpu else None,
            "rows_rewritten_per_inserted_row": round(w_all["rows_in"] / ins_rows, 3) if ins_rows else None,
            "bytes_written_per_inserted_byte": round(w_all["bytes_written"] / new_bytes, 3) if new_bytes else None,
            "merge_user_sys_over_insert": round(w_all["user_sys_us"] / ins_us, 3) if ins_us else None,
            "ttl_merges": agg(ttl_merges) if ttl_merges else None,
            "merges_by_reason": {r: agg([m for m in merges if m["merge_reason"] == r]) for r in sorted({m["merge_reason"] for m in merges})},
            "moves": (lambda mv: {"parts": len(mv), "bytes": sum(int(m["size_in_bytes"]) for m in mv),
                                  "rows": sum(int(m["rows"]) for m in mv), "cpu_us": sum(num(m["cpu_us"]) for m in mv),
                                  "disks": sorted({m["disk_name"] for m in mv})})(
                [p for p in pl if p["table"] == tbl and p["event_type"] == "MovePart" and not p["error"]]),
            "removed_parts": sum(1 for p in pl if p["table"] == tbl and p["event_type"] == "RemovePart"),
            "size_classes": cls, "curve": curve,
        }
        out["tables"][n] = t
    # cross-check: all part_log merge CPU against the merge pool threads' /proc CPU
    if samples:
        b0 = run["base_threads"]
        s_end = [s for s in samples if s["wall"] <= t_after + 5][-1]
        thr = s_end["threads"].get("MergeMutate", 0) - b0.get("MergeMutate", 0)
        pl_cpu = sum(num(p["cpu_us"]) for p in pl if p["event_type"] in ("MergeParts", "MutatePart") and ts(p["event_time_microseconds"]) <= s_end["wall"]) / 1e6
        out["crosscheck"] = {"merge_threads_proc_cpu_s": round(thr, 2), "part_log_merge_cpu_s": round(pl_cpu, 2),
                             "part_log_over_proc": round(pl_cpu / thr, 3) if thr else None,
                             "move_threads_cpu_s": round(s_end["threads"].get("Move", 0) - b0.get("Move", 0), 2)}
    json.dump(out, open(os.path.join(d, "summary.json"), "w"), indent=1)
    return out


def main():
    for d in sys.argv[1:]:
        o = analyze(d)
        print(f"\n## {o['run']} ({o['rows_per_insert']} rows/insert, {o['duration_s']} s, stop: {o['stop_reason']}, errors {o['errors']})")
        print("| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for n, t in o["tables"].items():
            w = t["with_drain"]
            print(f"| {n} | {t['statements']} | {t['rows_per_statement']} | {t['achieved_rate']} | {t['insert_us_per_row']} | {w['merges']} | "
                  f"{w['us_per_row_in'] and round(w['us_per_row_in'], 3)} | {t['merge_us_per_inserted_row']} | {t['merge_over_insert']} "
                  f"({t['merge_over_insert_ingest_window']}) | {t['rows_rewritten_per_inserted_row']} | {t['bytes_written_per_inserted_byte']} | "
                  f"{t['stored_bytes_per_row']} | {t['final_parts']} |")
        if "crosscheck" in o:
            print("crosscheck:", o["crosscheck"])


if __name__ == "__main__":
    main()
