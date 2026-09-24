"""Objects and bytes each form leaves in the bucket, per source generation."""
import json, os, sys
from common import *
out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
for db in ("r_nat", "r_unm", "r_opt", "r_m0"):
    m = meta[db]
    ns = f"{m['producer']}/{m['epoch']}"
    rec = {"source": m["producer"]}
    for key, glob in (("native_main", f"{S3}/{REGION}/traces/v1/{ns}/{GEN}/otel_traces/**"),
                      ("native_trace_id_ts", f"{S3}/{REGION}/traces/v1/{ns}/{GEN}/otel_traces_trace_id_ts/**"),
                      ("parquet", f"{S3}/parquet/{REGION}/traces/v1/{ns}/{GEN}/*.parquet"),
                      ("manifests", f"{S3}/{REGION}/traces/v1/{ns}/manifests/{GEN}/*.json")):
        n, b = q(f"SELECT count(), sum(_size) FROM s3('{glob}', '{KEY}', '{SECRET}', 'One')").split("\t")
        rec[key + "_objects"], rec[key + "_bytes"] = int(n), int(b)
    n, b = q(f"SELECT count(), sum(bytes_on_disk) FROM system.parts WHERE active AND database = '{db}' AND table = '{TABLE}'").split("\t")
    rec["active_parts"], rec["active_part_bytes"] = int(n), int(b)
    print(rec); out.write(json.dumps(rec) + "\n")
