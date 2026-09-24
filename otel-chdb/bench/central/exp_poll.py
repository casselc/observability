"""Standing cost of attached reader tables: S3 requests and server CPU per
second while idle, by number of tables, parts per table and
refresh_parts_interval. Tables are attached (main table only) into fresh
databases, settle for 3 s, then a 30 s window is measured."""
import json, sys, time
import os
from common import *

WINDOW = float(sys.argv[2]) if len(sys.argv) > 2 else 30
out = open(sys.argv[1] if len(sys.argv) > 1 else RESULTS + "/poll.jsonl", "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
pid = server_pid()
ddl = {}
for db, m in meta.items():
    ddl[(m["producer"], 1)] = attach("x", m["manifest"], refresh=1)[1]


def stmt(producer, refresh, db):
    s = ddl[(producer, 1)].replace("refresh_parts_interval = 1", f"refresh_parts_interval = {refresh}")
    return s.replace("EXISTS x.", f"EXISTS {db}.")


CONFIGS = [
    ("baseline (no refreshing tables)", [], 1),
    ("1 table x 5 parts", ["multi0"], 1),
    ("5 tables x 5 parts", [f"multi{i}" for i in range(5)], 1),
    ("10 tables x 5 parts", [f"multi{i}" for i in range(10)], 1),
    ("10 tables x 5 parts, refresh 5 s", [f"multi{i}" for i in range(10)], 5),
    ("1 table x 1 part (+~1.8k garbage objects)", ["optimized"], 1),
    ("1 table x 3 parts (+~1.7k garbage objects)", ["natural"], 1),
    ("1 table x 60 parts", ["unmerged"], 1),
]
for label, producers, refresh in CONFIGS:
    dbs = []
    a0 = snap(pid)
    for i, p in enumerate(producers):
        db = f"poll_{i}"
        q(f"CREATE DATABASE IF NOT EXISTS {db}")
        q(stmt(p, refresh, db))
        dbs.append(db)
    a1 = snap(pid)
    time.sleep(3)
    a = snap(pid)
    time.sleep(WINDOW)
    b = snap(pid)
    d = delta(a, b)
    secs = d["t"]
    rec = {"exp": "poll", "config": label, "tables": len(producers), "refresh_s": refresh,
           "attach_wall_s": a1["t"] - a0["t"], "attach_GET": a1["S3GetObject"] - a0["S3GetObject"], "attach_LIST": a1["S3ListObjects"] - a0["S3ListObjects"],
           "window_s": secs, "LIST_per_s": d["S3ListObjects"] / secs, "GET_per_s": d["S3GetObject"] / secs,
           "HEAD_per_s": d["S3HeadObject"] / secs, "bytes_per_s": d["ReadBufferFromS3Bytes"] / secs, "cpu_per_s": d["proc_cpu_s"] / secs}
    print(json.dumps(rec), flush=True)
    out.write(json.dumps(rec) + "\n")
    for db in dbs:
        q(f"DROP DATABASE {db} SYNC")
