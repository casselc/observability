#!/usr/bin/env python3
"""consumer_ckpt_sample.py S3_URL BUCKET PREFIX OUT [EVERY_S]: samples the
consumer's control objects over time, for scripts/consumer_ckpt_soak.sh.

Every EVERY_S seconds it appends one JSON line to OUT:
  - per lane checkpoint (`{prefix}/_consumer/ckpt/{lane}.json`): its size in
    bytes, its explicit epoch entries, and its floor (if any);
  - `gc.json`'s size and its marks;
  - per lane, how many epoch "directories" a delimiter LIST returns: what a
    full epoch listing costs without a floor (SeaweedFS keeps a directory
    after its last object is deleted, so this counts every epoch ever made).

Requests are signed by curl (`--aws-sigv4`), credentials from S3_KEY /
S3_SECRET (default otel / otelsecret).
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse

S3, BUCKET, PREFIX, OUT = sys.argv[1:5]
EVERY = float(sys.argv[5]) if len(sys.argv) > 5 else 10.0
USER = os.environ.get("S3_KEY", "otel") + ":" + os.environ.get("S3_SECRET", "otelsecret")
CTL = PREFIX.rstrip("/") + "/_consumer"


def req(path, query=None):
    url = f"{S3.rstrip('/')}/{BUCKET}/{path}"
    if query:
        url += "?" + urllib.parse.urlencode(query)
    r = subprocess.run(["curl", "-sS", "--max-time", "20", "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", USER, url],
                       capture_output=True)
    return r.stdout


def list_all(prefix, delimiter=None):
    keys, dirs, token = [], [], None
    while True:
        q = {"list-type": "2", "prefix": prefix}
        if delimiter:
            q["delimiter"] = delimiter
        if token:
            q["continuation-token"] = token
        body = req("", q).decode(errors="replace")
        keys += re.findall(r"<Key>([^<]*)</Key>", body)
        dirs += re.findall(r"<Prefix>([^<]*)</Prefix>", body)[1:] if delimiter else []
        m = re.search(r"<NextContinuationToken>([^<]*)</NextContinuationToken>", body)
        if "<IsTruncated>true</IsTruncated>" not in body or not m:
            return keys, dirs
        token = m.group(1)


def sample():
    lanes = {}
    ckeys, _ = list_all(CTL + "/ckpt/")
    for k in ckeys:
        lane = k[len(CTL + "/ckpt/"):-len(".json")]
        body = req(k)
        try:
            d = json.loads(body)
        except Exception:
            continue
        lanes[lane] = {"bytes": len(body), "epochs": len(d.get("epochs", {})), "floor": d.get("floor", "")}
        _, dirs = list_all(f"{PREFIX.rstrip('/')}/{lane}/", "/")
        lanes[lane]["dirs"] = len(dirs)
    gc = req(CTL + "/gc.json")
    try:
        g = json.loads(gc)
        marks = len(g.get("marks", []))
        retired = sum(len(v) for v in g.get("retired", {}).values())
    except Exception:
        marks, retired = 0, 0
    b = [l["bytes"] for l in lanes.values()] or [0]
    e = [l["epochs"] for l in lanes.values()] or [0]
    return {
        "wall_ms": int(time.time() * 1000),
        "lanes": len(lanes),
        "ckpt_bytes_max": max(b), "ckpt_bytes_sum": sum(b),
        "ckpt_epochs_max": max(e), "ckpt_epochs_sum": sum(e),
        "dirs_sum": sum(l["dirs"] for l in lanes.values()),
        "gc_bytes": len(gc), "gc_marks": marks, "gc_retired": retired,
        "per_lane": lanes,
    }


while True:
    t = time.time()
    try:
        s = sample()
        with open(OUT, "a") as f:
            f.write(json.dumps(s) + "\n")
    except Exception as ex:  # keep sampling
        print("sample:", ex, file=sys.stderr)
    time.sleep(max(0.0, EVERY - (time.time() - t)))
