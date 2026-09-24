"""Native selection with the manifest's event-time range as an extra
predicate (partition min-max / primary-key pruning), vs without. Read to
FORMAT Null, query condition cache off, 3 runs each."""
import json, sys
import os
from common import *
out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
pid = server_pid()
NOC = {"use_query_condition_cache": 0}
for db in ("r_opt", "r_nat", "r_unm"):
    m = meta[db]
    ms = {x["batch_id"]: x for x in manifests(f"{REGION}/traces/v1/{m['producer']}/{m['epoch']}", GEN)}
    for name, ids in (("1", [30]), ("10", list(range(21, 31)))):
        lo = min(ms[i]["min_event_time"] for i in ids).replace("T", " ").rstrip("Z")
        hi = max(ms[i]["max_event_time"] for i in ids).replace("T", " ").rstrip("Z")
        for hint in (False, True):
            sel = native_select(db, m["epoch"], ids)
            if hint:
                sel += f" AND Timestamp BETWEEN toDateTime64('{lo}', 9, 'UTC') AND toDateTime64('{hi}', 9, 'UTC')"
            for run in range(3):
                d = run_measured(pid, sel + " FORMAT Null", NOC)
                rec = {"exp": "hint", "db": db, "set": name, "hint": hint, "run": run, "rows": d["summary"].get("result_rows"),
                       **{k: d[k] for k in ("wall_s", "S3GetObject", "ReadBufferFromS3Bytes", "SelectedParts", "SelectedMarks")}}
                out.write(json.dumps(rec) + "\n")
                print(rec, flush=True)
