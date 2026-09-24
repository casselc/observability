"""Cost of converting Parquet's plain String/Map(String,String) columns to the
target's LowCardinality types: read 50 batches to FORMAT Null with the plain
structure vs with the target's types in the structure (conversion in the
reader), 5 runs each, interleaved."""
import json, sys
import os
from common import *
out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
pid = server_pid()
ep = meta["r_nat"]["epoch"]
ids = list(range(1, 51))
url = parquet_url("natural", ep, ids)
lc = (STRUCTURE.replace("SpanName String", "SpanName LowCardinality(String)").replace("SpanKind String", "SpanKind LowCardinality(String)")
      .replace("ServiceName String", "ServiceName LowCardinality(String)").replace("StatusCode String", "StatusCode LowCardinality(String)")
      .replace("Map(String, String)", "Map(LowCardinality(String), String)").replace("`Events.Name` Array(String)", "`Events.Name` Array(LowCardinality(String))")
      .replace("producer_id String", "producer_id LowCardinality(String)").replace("producer_epoch String", "producer_epoch LowCardinality(String)"))
for run in range(5):
    for label, st in (("plain", STRUCTURE), ("target-types", lc)):
        d = run_measured(pid, f"SELECT {COLS} FROM s3('{url}', '{KEY}', '{SECRET}', 'Parquet', '{st}') FORMAT Null")
        rec = {"exp": "conv", "structure": label, "run": run, "wall_s": d["wall_s"],
               "qcpu_s": (d["UserTimeMicroseconds"] + d["SystemTimeMicroseconds"]) / 1e6}
        out.write(json.dumps(rec) + "\n"); print(rec, flush=True)
