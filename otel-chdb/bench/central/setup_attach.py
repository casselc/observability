"""Create the central target and attach every source generation read-only.
Measures the attach cost (wall, S3 requests) of each reader table: 3 runs per
source (attach, drop, attach again), keeping the last."""
import json, sys
import os
from common import *

out = open(sys.argv[1] if len(sys.argv) > 1 else RESULTS + "/attach.jsonl", "w")
pid = server_pid()
q("CREATE DATABASE IF NOT EXISTS central")
q(CENTRAL_DDL)

sources = {"r_nat": "natural", "r_unm": "unmerged", "r_opt": "optimized"}
sources.update({f"r_m{i}": f"multi{i}" for i in range(10)})
meta = {}
for db, producer in sources.items():
    epoch, murl = sealed(producer)
    stmts = attach(db, murl, refresh=0)
    runs = 3 if producer in ("natural", "unmerged", "optimized") else 1
    for r in range(runs):
        q(f"DROP DATABASE IF EXISTS {db} SYNC")
        q(stmts[0])  # CREATE DATABASE
        d = run_measured(pid, stmts[1])  # the main table only: that is what ingest reads
        parts = q(f"SELECT count() FROM system.parts WHERE active AND database = '{db}' AND table = '{TABLE}'")
        rec = {"exp": "attach", "source": producer, "run": r, "parts": int(parts),
               **{k: d[k] for k in ("wall_s", "S3GetObject", "S3ListObjects", "S3HeadObject", "ReadBufferFromS3Bytes", "proc_cpu_s")}}
        out.write(json.dumps(rec) + "\n")
        print(rec, flush=True)
    for s in stmts[2:]:
        q(s)
    meta[db] = {"producer": producer, "epoch": epoch, "manifest": murl}
json.dump(meta, open(os.path.join(RESULTS, "sources.json"), "w"), indent=1)
print(q(f"SELECT database, count(), sum(rows), groupArray(level) FROM system.parts WHERE active AND startsWith(database, 'r_') AND table = '{TABLE}' GROUP BY database ORDER BY database FORMAT TSV"))
