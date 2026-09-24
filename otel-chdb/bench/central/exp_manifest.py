"""Per-cycle discovery cost, common to both paths: list one producer
generation's manifest prefix (60 batch manifests + _sealed.json), then read
10 named manifests. Through the server's s3(); 3 runs each."""
import json, sys
import os
from common import *
out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
pid = server_pid()
m = meta["r_unm"]
pre = f"{S3}/{REGION}/traces/v1/{m['producer']}/{m['epoch']}/manifests/{GEN}"
ten = pre + "/{" + ",".join("%020d" % i for i in range(21, 31)) + "}.json"
for run in range(3):
    for label, sql in (("list prefix (_path only)", f"SELECT count() FROM (SELECT _path FROM s3('{pre}/*.json', '{KEY}', '{SECRET}', 'One'))"),
                       ("read 10 named manifests", f"SELECT sum(length(json)) FROM s3('{ten}', '{KEY}', '{SECRET}', 'JSONAsString')"),
                       ("list + read all 61", f"SELECT sum(length(json)) FROM s3('{pre}/*.json', '{KEY}', '{SECRET}', 'JSONAsString')")):
        d = run_measured(pid, sql)
        rec = {"exp": "manifest", "what": label, "run": run, **{k: d[k] for k in ("wall_s", "S3GetObject", "S3ListObjects", "S3HeadObject", "ReadBufferFromS3Bytes")}}
        out.write(json.dumps(rec) + "\n"); print(rec, flush=True)
