"""Extra runs of the 50-batch INSERT ... SELECT cases (the insert side is
noisy), interleaved, query condition cache off."""
import json, sys, time, uuid
import os
from common import *
RUNS = int(sys.argv[2])
out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
pid = server_pid()
ep = meta["r_nat"]["epoch"]
ids = list(range(1, 51))
union = " UNION ALL ".join(native_select(f"r_m{i}", meta[f"r_m{i}"]["epoch"], [1, 2, 3, 4, 5]) for i in range(10))
pairs = [(f"multi{i}", meta[f"r_m{i}"]["epoch"], b) for i in range(10) for b in range(1, 6)]
CFG = [("native-1part", "50", native_select("r_opt", meta["r_opt"]["epoch"], ids)),
       ("native-3parts", "50", native_select("r_nat", ep, ids)),
       ("native-60parts", "50", native_select("r_unm", meta["r_unm"]["epoch"], ids)),
       ("parquet", "50", parquet_select(parquet_url("natural", ep, ids), ep, ids)),
       ("native-10tables", "50x10p", f"SELECT {COLS} FROM ({union})"),
       ("parquet", "50x10p", parquet_select(parquet_multi_url(pairs)))]
for run in range(RUNS):
    for label, name, sel in CFG:
        q("TRUNCATE TABLE central.otel_traces SYNC")
        time.sleep(0.5)
        d = run_measured(pid, f"INSERT INTO central.otel_traces ({COLS}) {sel}",
                         {"insert_deduplication_token": uuid.uuid4().hex, "use_query_condition_cache": 0})
        rec = {"exp": "insert50", "source": label, "set": name, "mode": "insert", "run": run, "rows": 400000,
               "result_rows": int(d["summary"].get("written_rows", 0)), "central_parts": 0,
               **{k: d[k] for k in ("wall_s", "S3GetObject", "S3ListObjects", "S3HeadObject", "ReadBufferFromS3Bytes", "proc_cpu_s",
                                    "UserTimeMicroseconds", "SystemTimeMicroseconds", "SelectedParts", "SelectedMarks")}}
        out.write(json.dumps(rec) + "\n"); out.flush()
        print(label, name, run, "%.3f" % d["wall_s"], flush=True)
