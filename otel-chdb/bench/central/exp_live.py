"""A worker keeping up with one live producer whose active generation already
holds 240 batches (8k spans each): each cycle the writer publishes 3 new
batches, the worker reads exactly those 3 (native: attached table with
refresh_parts_interval = 1, with and without the manifests' event-time
range; Parquet: the 3 objects), to FORMAT Null. Then a catch-up read of 3
old batches that sit inside the big merged parts. Needs the 'big' generator
running with -control 127.0.0.1:17003."""
import json, sys, time
import requests
from common import *

out = open(sys.argv[1], "w")
CTL = "http://127.0.0.1:17003"
ctl = lambda path, **p: requests.get(CTL + path, params=p, timeout=600).text
pid = server_pid()
epoch, murl = sealed("big")
stmts = attach("r_big", murl, refresh=1)
q("DROP DATABASE IF EXISTS r_big SYNC")
for s in stmts[:2]:
    q(s)
NOC = {"use_query_condition_cache": 0}


def cases(ids):
    ms = {m["batch_id"]: m for m in manifests(f"{REGION}/traces/v1/big/{epoch}", GEN)}
    lo = min(ms[i]["min_event_time"] for i in ids).replace("T", " ").rstrip("Z")
    hi = max(ms[i]["max_event_time"] for i in ids).replace("T", " ").rstrip("Z")
    nat = native_select("r_big", epoch, ids)
    return [("native", nat),
            ("native+time-range", nat + f" AND Timestamp BETWEEN toDateTime64('{lo}', 9, 'UTC') AND toDateTime64('{hi}', 9, 'UTC')"),
            ("parquet", parquet_select(parquet_url("big", epoch, ids), epoch, ids))]


def run(kind, cycle, ids):
    parts = q(f"SELECT count() FROM system.parts WHERE active AND database = 'r_big' AND table = '{TABLE}'")
    total = q(f"SELECT count() FROM r_big.{TABLE}")
    for label, sel in cases(ids):
        d = run_measured(pid, sel + " FORMAT Null", NOC)
        rec = {"exp": "live", "kind": kind, "cycle": cycle, "source": label, "batches": ids, "reader_parts": int(parts), "gen_rows": int(total),
               "rows": int(d["summary"].get("result_rows", 0)),
               **{k: d[k] for k in ("wall_s", "S3GetObject", "S3ListObjects", "S3HeadObject", "ReadBufferFromS3Bytes", "SelectedParts", "SelectedMarks",
                                    "UserTimeMicroseconds", "SystemTimeMicroseconds")}}
        out.write(json.dumps(rec) + "\n"); out.flush()
        print(kind, cycle, label, ids, "parts", parts, "rows", rec["rows"], "GET", d["S3GetObject"], "MB %.2f" % (d["ReadBufferFromS3Bytes"] / 1e6),
              "%.3fs" % d["wall_s"], flush=True)


time.sleep(2)
for cycle in range(5):
    last = int(q(f"SELECT max(batch_id) FROM r_big.{TABLE}"))
    ctl("/push", n="3")
    want = last + 3
    for _ in range(100):  # wait for the reader's refresh to see them
        if int(q(f"SELECT max(batch_id) FROM r_big.{TABLE}")) >= want:
            break
        time.sleep(0.2)
    run("keep-up (newest 3)", cycle, [last + 1, last + 2, last + 3])
for cycle, start in enumerate((100, 150, 200)):
    run("catch-up (3 old batches)", cycle, [start, start + 1, start + 2])
q("DROP DATABASE r_big SYNC")
