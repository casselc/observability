#!/usr/bin/env python3
"""The schema comparison's databases, loaded without consumers (a busy box):
every committed object under the ROOT prefixes is inserted into each database
with the consumer's statement shape (one object per statement, ONE_BLOCK, a
dedup token), then the haystack: the objects under HAYSTACK copied N times,
Timestamp and received_at shifted to one copy every 12 minutes back from
--end-ms, so HyperDX's fast paths have something to prune. OPTIMIZE FINAL at
the end, one table at a time.

  CH=http://127.0.0.1:18123 CONSUME=.../consume schema_load.py --dbs hdx_old,hdx_full,hdx_new \
      --root http://127.0.0.1:18333/b/hdxsmall --haystack http://127.0.0.1:18333/b/bench/edges/bench --copies 15 --end-ms MS
The databases must exist with their DDL (setup_db.py file / clickstack-full / consumer).
"""
import argparse, os, sys, uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema_bench as b  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--dbs", required=True)
ap.add_argument("--root", required=True)
ap.add_argument("--haystack", default="")
ap.add_argument("--copies", type=int, default=15)
ap.add_argument("--end-ms", type=int, default=0)
a = ap.parse_args()
dbs = a.dbs.split(",")
import subprocess  # noqa: E402
st = {s: subprocess.check_output([b.CONSUME, "--print-structure", s], text=True).strip() for s in ("traces", "logs")}
cols = {s: subprocess.check_output([b.CONSUME, "--print-cols", s], text=True).strip() for s in ("traces", "logs")}
S = dict(**b.ONE_BLOCK, **b.CAP)


def objects(prefix, sig):
    b.S3 = prefix
    return b.keys(sig)


for sig in ("traces", "logs"):
    for i, k in enumerate(objects(a.root, sig)):
        for db in dbs:
            b.q(f"INSERT INTO {db}.otel_{sig} ({cols[sig]}, content_key) SELECT {cols[sig]}, 'o{i}' "
                f"FROM s3('http://127.0.0.1:18333/{k}', 'otel', 'otelsecret', 'Parquet', '{st[sig]}')", **S, insert_deduplication_token=uuid.uuid4().hex)
    print(sig, "objects done", flush=True)
    if not a.haystack:
        continue
    keys = objects(a.haystack, sig)
    mx = int(b.q(f"SELECT toUnixTimestamp64Nano(max(Timestamp)) FROM s3('http://127.0.0.1:18333/{keys[-1]}', 'otel', 'otelsecret', 'Parquet', '{st[sig]}')"))
    for j in range(a.copies):
        sh = (a.end_ms - 60_000 - j * 12 * 60_000) * 10**6 - mx
        sel = cols[sig].replace("Timestamp,", f"Timestamp + toIntervalNanosecond({sh}),", 1).replace(
            "received_at", f"Timestamp + toIntervalNanosecond({sh}) + toIntervalSecond(5)")
        for i, k in enumerate(keys):
            for db in dbs:
                b.q(f"INSERT INTO {db}.otel_{sig} ({cols[sig]}, content_key) SELECT {sel}, 'hay{j}-{i}' "
                    f"FROM s3('http://127.0.0.1:18333/{k}', 'otel', 'otelsecret', 'Parquet', '{st[sig]}')", **S, insert_deduplication_token=uuid.uuid4().hex)
    print(sig, "haystack done", flush=True)
for db in dbs:
    for t in ("otel_traces", "otel_logs"):
        b.q(f"OPTIMIZE TABLE {db}.{t} FINAL", **b.CAP, max_threads=2)
print(b.q(f"SELECT database, table, sum(rows), count(), round(sum(bytes_on_disk)/sum(rows),1) FROM system.parts WHERE database IN ({', '.join(repr(d) for d in dbs)}) AND active GROUP BY database, table ORDER BY table, database FORMAT TSV"))
