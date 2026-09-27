#!/usr/bin/env python3
"""Merge CPU of the schema comparison's tables, isolated from a shared server
(no part_log there, and OPTIMIZE has no ProfileEvents of its own): per variant
and rep, clickhouse-local loads the objects into a fresh on-disk table, one
object per INSERT with merges stopped (the parts a consumer writes), then two
further clickhouse-local processes on the same path: a baseline that only opens
the table, and one that runs OPTIMIZE TABLE ... FINAL. Merge CPU = the second's
user+system CPU minus the baseline's (getrusage of the child).

  CHC=.../clickhouse CONSUME=.../consume S3_PREFIX=http://127.0.0.1:18333/b/root WORK=/scratch/dir \
    VARIANTS=old,full_main,new_main OUT=merge.jsonl schema_merge.py REPS
"""
import json, os, resource, shutil, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema_bench as b  # noqa: E402

WORK = os.environ.get("WORK", "")
OUT = os.environ.get("OUT", "merge.jsonl")
MEM = os.environ.get("MAX_MEMORY", "2000000000")


def local(path, sql):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    r = subprocess.run([b.CHC, "local", "--path", path, "--max_memory_usage", MEM, "--max_threads", "2", "--multiquery", "--query", sql],
                       capture_output=True, text=True, timeout=1800)
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-800:])
    return (after.ru_utime - before.ru_utime + after.ru_stime - before.ru_stime) * 1e6, r.stdout.strip()


def main():
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    variants = os.environ.get("VARIANTS", "old,full_main,new_main").split(",")
    st = {s: subprocess.check_output([b.CONSUME, "--print-structure", s], text=True).strip() for s in ("traces", "logs")}
    cols = {s: subprocess.check_output([b.CONSUME, "--print-cols", s], text=True).strip() for s in ("traces", "logs")}
    objs = {s: b.keys(s) for s in ("traces", "logs")}
    for rep in range(reps):
        for v in variants[rep % len(variants):] + variants[:rep % len(variants)]:
            rec = {"variant": v, "rep": rep, "load": b.load(), "merge": {}}
            for sig in ("traces", "logs"):
                path = os.path.join(WORK, f"{v}-{sig}")
                shutil.rmtree(path, ignore_errors=True)
                ddl = b.ddl(v, f"d.otel_{sig}", sig)[0]
                ins = ";\n".join(
                    f"INSERT INTO d.otel_{sig} ({cols[sig]}, content_key) SELECT {cols[sig]}, 'ck{i}' FROM "
                    f"s3('http://127.0.0.1:18333/{k}', 'otel', 'otelsecret', 'Parquet', '{st[sig]}') SETTINGS max_threads = 1, "
                    "max_insert_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_block_size = 1048576, "
                    "max_insert_block_size = 1048576, input_format_parquet_max_block_size = 1048576"
                    for i, k in enumerate(objs[sig]))
                local(path, f"CREATE DATABASE d; {ddl}; SYSTEM STOP MERGES d.otel_{sig}; {ins}")
                base, parts = local(path, f"SELECT count() FROM system.parts WHERE database = 'd' AND active")
                cpu, _ = local(path, f"OPTIMIZE TABLE d.otel_{sig} FINAL")
                _, rows = local(path, f"SELECT count() FROM d.otel_{sig}")
                rec["merge"][sig] = {"parts": int(parts), "rows": int(rows), "cpu_us": cpu - base, "base_us": base, "opt_us": cpu}
                shutil.rmtree(path, ignore_errors=True)
            with open(OUT, "a") as f:
                f.write(json.dumps(rec) + "\n")
            print(v, rep, {s: round(m["cpu_us"] / m["rows"], 2) for s, m in rec["merge"].items()}, "load", rec["load"], flush=True)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--summarize"]:
        import collections, statistics
        by = collections.defaultdict(list)
        for l in open(sys.argv[2]):
            r = json.loads(l)
            by[r["variant"]].append(r)
        print("| variant | reps | merge µs/span | merge µs/log | parts merged | load (1-min) |\n|---|---:|---:|---:|---:|---|")
        for v, rs in by.items():
            t = [r["merge"]["traces"]["cpu_us"] / r["merge"]["traces"]["rows"] for r in rs]
            g = [r["merge"]["logs"]["cpu_us"] / r["merge"]["logs"]["rows"] for r in rs]
            lo = [float(r["load"][0]) for r in rs]
            print(f"| {v} | {len(rs)} | {statistics.median(t):.2f} [{min(t):.2f}–{max(t):.2f}] | {statistics.median(g):.2f} [{min(g):.2f}–{max(g):.2f}] | "
                  f"{rs[0]['merge']['traces']['parts']} / {rs[0]['merge']['logs']['parts']} | {min(lo):.1f}–{max(lo):.1f} |")
    else:
        main()
