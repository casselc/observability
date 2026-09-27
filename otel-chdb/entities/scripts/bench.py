#!/usr/bin/env python3
"""Insert CPU, merge CPU and stored bytes per row of each schema (schemas.py),
as ../../hyperdx/scripts/schema_bench.py measures them (CPU_SOURCE=client:
the shared server has no query_log / part_log): the same objects
(objects.py: 30 Parquet objects per signal in SeaweedFS, each variant's own
shape) inserted into a fresh database, one object per statement in the
consumer's shape (ONE_BLOCK, a dedup token), merges stopped; CPU = the
statement's own OSCPUVirtualTimeMicroseconds (clickhouse client
--print-profile-events). Then OPTIMIZE TABLE ... FINAL per table (30 parts -> 1) and bytes on disk
per row; merge CPU is merge.py's (the server reports no ProfileEvents for
OPTIMIZE). Variants rotate
order every rep; the first rep is a warm-up.

  bench.py REPS [--variants a,full,b1,b2,ann] [--out ../results/bench.jsonl]
"""
import argparse, json, time, uuid
import chlib as c
import schemas

S3 = "http://127.0.0.1:18333/ent-objects"
ONE_BLOCK = dict(max_threads=1, max_insert_threads=1, max_block_size=1048576, max_insert_block_size=1048576,
                 min_insert_block_size_rows=0, min_insert_block_size_bytes=0, input_format_parquet_max_block_size=1048576)
SHAPE = {"a": "a", "full": "a", "b1": "b", "b2": "b", "ann": "ann"}

ap = argparse.ArgumentParser()
ap.add_argument("reps", type=int)
ap.add_argument("--variants", default="a,full,b1,b2,ann")
ap.add_argument("--out", default=f"{c.HERE}/../results/bench.jsonl")
a = ap.parse_args()
variants = a.variants.split(",")
keys = {}
for shape in ("a", "b", "ann"):
    for sig in schemas.SIGS:
        keys[shape, sig] = c.one(f"SELECT DISTINCT _path FROM s3('{S3}/{shape}/{sig}/*.parquet', 'otel', 'otelsecret', 'One') ORDER BY _path").split()

for rep in range(a.reps):
    for v in variants[rep % len(variants):] + variants[:rep % len(variants)]:
        if c.disk_free_gb() < 4.2 or c.mem_available_gb() < 5:
            raise SystemExit(f"stop: disk {c.disk_free_gb():.1f} GB, mem {c.mem_available_gb():.1f} GB")
        db = f"ent_bench_{v}"
        c.q(f"DROP DATABASE IF EXISTS {db} SYNC")
        schemas.create(v, db)
        rec = dict(variant=v, rep=rep, load_before=c.load(), insert={}, merge={}, after={}, ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        for sig in schemas.SIGS:
            base = f"otel_{sig}" if v in ("a", "full", "c") else f"otel_{sig}_rid"
            c.q(f"SYSTEM STOP MERGES {db}.{base}")
            cols = schemas.ordinary_columns(db, base)
            cl = ", ".join(f"`{x}`" for x in cols)
            cpu = n = 0
            for i, k in enumerate(keys[SHAPE[v], sig]):
                ev = c.client_events(f"INSERT INTO {db}.{base} ({cl}) SELECT {cl} FROM s3('http://127.0.0.1:18333/{k}', 'otel', 'otelsecret', 'Parquet')",
                                     **ONE_BLOCK, insert_deduplication_token=uuid.uuid4().hex, insert_deduplicate=1)
                cpu += ev.get("OSCPUVirtualTimeMicroseconds", 0)
                n += 1
            rows = int(c.one(f"SELECT count() FROM {db}.{base}"))
            rec["insert"][sig] = dict(statements=n, rows=rows, cpu_us=cpu)
            parts = int(c.one(f"SELECT count() FROM system.parts WHERE database = '{db}' AND table = '{base}' AND active"))
            c.q(f"SYSTEM START MERGES {db}.{base}")
            c.q(f"OPTIMIZE TABLE {db}.{base} FINAL", max_threads=2)  # for the stored bytes; merge CPU: merge.py
            rec["merge"][sig] = dict(parts=parts)
        for r in c.rows(f"SELECT table, sum(rows) AS rows, count() AS parts, sum(bytes_on_disk) AS bytes, sum(secondary_indices_compressed_bytes) AS idx "
                        f"FROM system.parts WHERE database = '{db}' AND active GROUP BY table"):
            rec["after"][r["table"]] = {k: int(r[k]) for k in ("rows", "parts", "bytes", "idx")}
        rec["columns"] = {f"{r['table']}.{r['column']}": int(r["b"]) for r in c.rows(
            f"SELECT table, column, sum(column_data_compressed_bytes) AS b FROM system.parts_columns WHERE database = '{db}' AND active "
            f"AND table LIKE 'otel_%' AND table NOT LIKE '%rollup%' GROUP BY table, column")}
        rec["load_after"] = c.load()
        with open(a.out, "a") as f:
            f.write(json.dumps(rec) + "\n")
        ins = {s: round(x["cpu_us"] / x["rows"], 2) for s, x in rec["insert"].items()}
        byt = {t: round(x["bytes"] / x["rows"], 1) for t, x in rec["after"].items() if "rollup" not in t}
        print(v, rep, "insert us/row", ins, "B/row", byt, "load", rec["load_before"], flush=True)
        c.q(f"DROP DATABASE {db} SYNC")
