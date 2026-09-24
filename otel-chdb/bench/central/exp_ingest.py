"""Ingest matrix: batch sets of 1, 10 and 50 batches (8,000 spans each) read
from attached native tables (3 part layouts of the same 60-batch generation)
or from the same batches' Parquet objects named explicitly, either inserted
into central.otel_traces or read to FORMAT Null. Plus 50 batches spread over
10 producers (10 tables vs 50 objects under 10 prefixes)."""
import json, sys, time, uuid
import os
from common import *

RUNS = int(sys.argv[2]) if len(sys.argv) > 2 else 3
out = open(sys.argv[1] if len(sys.argv) > 1 else RESULTS + "/ingest.jsonl", "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
pid = server_pid()
nat_epoch = meta["r_nat"]["epoch"]

# A real cycle's IN-list is new every time, so the query condition cache
# (on by default) would only flatter repeated benchmark runs: off.
NOCACHE = {"use_query_condition_cache": 0}

SETS = {"1": [30], "10": list(range(21, 31)), "50": list(range(1, 51))}


def configs():
    for name, ids in SETS.items():
        for db, label in (("r_opt", "native-1part"), ("r_nat", "native-3parts"), ("r_unm", "native-60parts")):
            yield label, name, native_select(db, meta[db]["epoch"], ids), len(ids) * 8000
        yield "parquet", name, parquet_select(parquet_url("natural", nat_epoch, ids), nat_epoch, ids), len(ids) * 8000
    ids = SETS["10"]
    yield "parquet-inferred-schema", "10", parquet_select(parquet_url("natural", nat_epoch, ids), nat_epoch, ids, structure=False), 80000
    # 50 batches from 10 producers (5 each): 10 attached tables vs 50 objects.
    union = " UNION ALL ".join(native_select(f"r_m{i}", meta[f"r_m{i}"]["epoch"], [1, 2, 3, 4, 5]) for i in range(10))
    yield "native-10tables", "50x10p", f"SELECT {COLS} FROM ({union})", 400000
    pairs = [(f"multi{i}", meta[f"r_m{i}"]["epoch"], b) for i in range(10) for b in range(1, 6)]
    yield "parquet", "50x10p", parquet_select(parquet_multi_url(pairs)), 400000


FIELDS = ("wall_s", "S3GetObject", "S3ListObjects", "S3HeadObject", "ReadBufferFromS3Bytes", "proc_cpu_s",
          "UserTimeMicroseconds", "SystemTimeMicroseconds", "SelectedParts", "SelectedMarks", "SelectedRows", "SelectedBytes",
          "InsertedRows", "InsertedBytes")

# Warm-up: one untimed read of each config (schema cache, S3 client, disk metadata).
for label, name, sel, rows in configs():
    q(sel + " FORMAT Null", settings=NOCACHE)

for run in range(RUNS):
    for label, name, sel, rows in configs():
        for mode in ("null", "insert"):
            if mode == "insert":
                q("TRUNCATE TABLE central.otel_traces SYNC")
                time.sleep(0.3)
                sql = f"INSERT INTO central.otel_traces ({COLS}) {sel}"
                settings = {"insert_deduplication_token": "bench-" + uuid.uuid4().hex, **NOCACHE}
            else:
                sql, settings = sel + " FORMAT Null", NOCACHE
            d = run_measured(pid, sql, settings)
            got = d["summary"].get("written_rows") if mode == "insert" else d["summary"].get("result_rows")
            cparts = int(q("SELECT count() FROM system.parts WHERE active AND database = 'central' AND table = 'otel_traces'")) if mode == "insert" else 0
            rec = {"central_parts": cparts, "exp": "ingest", "source": label, "set": name, "mode": mode, "run": run, "rows": rows,
                   "result_rows": int(got or 0), "read_rows": int(d["summary"].get("read_rows", 0)),
                   "read_bytes": int(d["summary"].get("read_bytes", 0)), **{k: d[k] for k in FIELDS}}
            out.write(json.dumps(rec) + "\n")
            out.flush()
            print(label, name, mode, run, "%.3fs" % d["wall_s"], "rows", got, "GET", d["S3GetObject"], "LIST", d["S3ListObjects"],
                  "HEAD", d["S3HeadObject"], "bytes", d["ReadBufferFromS3Bytes"], "qcpu %.2f" % ((d["UserTimeMicroseconds"] + d["SystemTimeMicroseconds"]) / 1e6),
                  "pcpu %.2f" % d["proc_cpu_s"], flush=True)
