#!/bin/bash
# Request and byte counts, billed by S3 itself, for the run's prefix: an S3
# request-metrics filter on s3://$BUCKET/$VPREFIX/ (CloudWatch, 1-minute,
# about 15 min behind), priced at DECISIONS.md §3's list prices.
#
#   RUN=v1 BUCKET=... REGION=us-east-1 eks/cost.sh start
#   RUN=v1 BUCKET=... REGION=us-east-1 eks/cost.sh report [FROM TO]   # ISO times; default: since start, until now
#
# `start` adds a metrics configuration with the id otelval-$RUN (additive: a
# bucket's other metrics configurations are untouched; down.sh removes it).
# `report` prints per-operation counts and dollars and writes
# $STATE/cost-<from>-<to>.json. Request metrics cost $0.30 per metric per
# month while they exist (about 16 metrics).
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need aws python3
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
ID=otelval-$RUN
case ${1:-report} in
  start)
    if ! aws s3api get-bucket-metrics-configuration --bucket "$BUCKET" --id "$ID" > /dev/null 2>&1; then
      aws s3api put-bucket-metrics-configuration --bucket "$BUCKET" --id "$ID" \
        --metrics-configuration "{\"Id\":\"$ID\",\"Filter\":{\"Prefix\":\"$VPREFIX/\"}}" || die "put-bucket-metrics-configuration"
      created bucket-metrics "$BUCKET" "$ID"
    fi
    date -u +%FT%TZ > "$STATE/cost.start"
    log "request metrics $ID on s3://$BUCKET/$VPREFIX/ from $(cat "$STATE/cost.start") (first data after ~15 min)" ;;
  report)
    FROM=${2:-$(cat "$STATE/cost.start" 2> /dev/null)}; TO=${3:-$(date -u +%FT%TZ)}
    [ -n "$FROM" ] || die "no start time: run 'cost.sh start' first or pass FROM TO"
    out=$STATE/cost-${FROM//:/}-${TO//:/}.json
    python3 - "$BUCKET" "$ID" "$REGION" "$FROM" "$TO" > "$out" <<'PY' || die "report"
import json, subprocess, sys
bucket, fid, region, t0, t1 = sys.argv[1:]
ops = ["AllRequests", "PutRequests", "GetRequests", "HeadRequests", "ListRequests", "DeleteRequests", "PostRequests",
       "4xxErrors", "5xxErrors", "BytesUploaded", "BytesDownloaded"]
lat = ["FirstByteLatency", "TotalRequestLatency"]
def stat(m, s):
    r = subprocess.run(["aws", "cloudwatch", "get-metric-statistics", "--region", region, "--namespace", "AWS/S3",
        "--metric-name", m, "--dimensions", f"Name=BucketName,Value={bucket}", f"Name=FilterId,Value={fid}",
        "--start-time", t0, "--end-time", t1, "--period", "60", "--statistics", s, "--output", "json"],
        capture_output=True, text=True, check=True)
    return json.loads(r.stdout)["Datapoints"]
res = {"bucket": bucket, "filter": fid, "from": t0, "to": t1}
for m in ops:
    pts = stat(m, "Sum")
    res[m] = sum(p["Sum"] for p in pts)
    if m.endswith("Requests") and pts:
        res[m + "_peak_per_s"] = max(p["Sum"] for p in pts) / 60
for m in lat:
    pts = stat(m, "Average")
    res[m + "_avg_ms"] = sum(p["Average"] for p in pts) / len(pts) if pts else None
# List prices (DECISIONS.md §3): PUT, COPY, POST, LIST $0.005 per 1,000; GET, HEAD and others $0.0004 per 1,000.
res["usd_requests"] = round((res["PutRequests"] + res["ListRequests"] + res["PostRequests"]) * 0.005 / 1000
                            + (res["GetRequests"] + res["HeadRequests"]) * 0.0004 / 1000, 4)
json.dump(res, sys.stdout, indent=1)
PY
    cat "$out"
    result cost.requests "$(python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print(" ".join(f"{k}={int(v) if isinstance(v,float) else v}" for k,v in r.items() if k.endswith("Requests") or k.startswith("usd") or k.startswith("Bytes")))' "$out")"
    # What is stored under the run prefix now (GC deletes ingested slots, so this is the backlog plus control objects).
    result cost.stored "$(aws s3 ls --recursive --summarize "s3://$BUCKET/$VPREFIX/" | tail -2 | tr -s ' ' | tr '\n' ' ')" ;;
  *) die "start | report" ;;
esac
