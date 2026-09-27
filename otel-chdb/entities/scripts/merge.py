#!/usr/bin/env python3
"""Merge CPU per row, isolated from the shared server (which reports no
ProfileEvents for OPTIMIZE and has no part_log), as
../../hyperdx/scripts/schema_merge.py does it: per variant and rep,
clickhouse-local loads the same 30 Parquet objects (objects.py) into a fresh
on-disk table, one object per INSERT with merges stopped (30 parts); then two
more clickhouse-local processes on that path: a baseline that only counts the
parts, and one that runs OPTIMIZE TABLE ... FINAL. Merge CPU = the second's
user+system CPU minus the baseline's (getrusage of the child). Table only (no
rollup: its view does not run on merges).

  CH_CLIENT=.../clickhouse merge.py REPS [--variants a,full,b1,b2,ann] [--work DIR]
"""
import argparse, json, os, resource, shutil, subprocess, time
import chlib as c
import schemas

ap = argparse.ArgumentParser()
ap.add_argument("reps", type=int)
ap.add_argument("--variants", default="a,full,b1,b2,ann")
ap.add_argument("--work", default="/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad/ent-merge")
ap.add_argument("--out", default=f"{c.HERE}/../results/merge.jsonl")
a = ap.parse_args()
SHAPE = {"a": "a", "full": "a", "b1": "b", "b2": "b", "ann": "ann"}
S3 = "http://127.0.0.1:18333/ent-objects"


def local(path, sql):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    r = subprocess.run([c.CHC, "local", "--path", path, "--max_memory_usage", "2000000000", "--max_threads", "2", "--multiquery", "--query", sql],
                       capture_output=True, text=True, timeout=1800)
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-1500:])
    return (after.ru_utime - before.ru_utime + after.ru_stime - before.ru_stime) * 1e6, r.stdout.strip()


keys = {(s, g): c.one(f"SELECT DISTINCT _path FROM s3('{S3}/{s}/{g}/*.parquet', 'otel', 'otelsecret', 'One') ORDER BY _path").split()
        for s in ("a", "b", "ann") for g in schemas.SIGS}
vs = a.variants.split(",")
for rep in range(a.reps):
    for v in vs[rep % len(vs):] + vs[:rep % len(vs)]:
        rec = dict(variant=v, rep=rep, load=c.load(), merge={}, ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        for sig in schemas.SIGS:
            path = os.path.join(a.work, f"{v}-{sig}")
            shutil.rmtree(path, ignore_errors=True)
            base = f"d.otel_{sig}" if v in ("a", "full") else f"d.otel_{sig}_rid"
            ddl = schemas.base_ddl(v, sig, base)[0]
            cols = [x for x in schemas.a_columns(sig)]
            if v not in ("a", "full"):
                i = cols.index("ResourceAttributes")
                cols[i:i + 1] = ["resource_id"] + (["ResourceResidual"] if v != "ann" else [])
            cl = ", ".join(f"`{x}`" for x in cols)
            ins = ";\n".join(f"INSERT INTO {base} ({cl}) SELECT {cl} FROM s3('http://127.0.0.1:18333/{k}', 'otel', 'otelsecret', 'Parquet') "
                             f"SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, "
                             f"max_block_size = 1048576, max_insert_block_size = 1048576, input_format_parquet_max_block_size = 1048576"
                             for k in keys[SHAPE[v], sig])
            local(path, f"CREATE DATABASE d; {ddl}; SYSTEM STOP MERGES {base}; {ins}")
            base_us, parts = local(path, "SELECT count() FROM system.parts WHERE database = 'd' AND active")
            opt_us, _ = local(path, f"OPTIMIZE TABLE {base} FINAL")
            _, rows = local(path, f"SELECT count() FROM {base}")
            rec["merge"][sig] = dict(parts=int(parts), rows=int(rows), cpu_us=opt_us - base_us, base_us=base_us, opt_us=opt_us)
            shutil.rmtree(path, ignore_errors=True)
        with open(a.out, "a") as f:
            f.write(json.dumps(rec) + "\n")
        print(v, rep, {s: round(m["cpu_us"] / m["rows"], 2) for s, m in rec["merge"].items()}, "load", rec["load"], flush=True)
