#!/bin/bash
# bench/clean copy of otap-rs/scripts/consumer_latency.sh; the only change is
# the S3 prefix (otel/clean/b5/... instead of otel/otap-rs-consumer/...).
# consumer_latency.sh: steady state, one edge and one worker, no faults.
# Measures, per poll period: end-to-end latency to query visibility (the
# edge's receive time, in the object's metadata, to the worker's INSERT
# returning), consumer CPU and S3 requests per object, and server CPU per
# object (system.events deltas, server-wide).
#
#   B=bin OUT=results/consumer/latency.jsonl POLLS="200ms 1s" SECS=90 scripts/consumer_latency.sh
set -u
B=${B:?dir with otap-s3pq, consume, soaksend}
OUT=${OUT:-results/consumer/latency.jsonl}
SECS=${SECS:-90}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
ch() { curl -sS "$CH/" --data-binary "$1"; }
cpu() { ch "SELECT sum(value) FROM system.events WHERE event IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds')"; }
for poll in ${POLLS:-200ms 1s}; do
  RUN=lat$(date +%s)
  DB=otaprs_consumer_$RUN
  PREFIX=otel/clean/b5/lat/$RUN/edges
  env OTLP_HTTP=127.0.0.1:24418 OTLP_GRPC=127.0.0.1:24417 ADMIN_HTTP=127.0.0.1:28090 PUT_TIMEOUT=5s PRODUCER=edge-1 \
    S3_URL=$S3/$PREFIX/edge-1 "$B/otap-s3pq" -c "$here/scripts/consumer_soak_edge.yaml" > "$tmp/edge.log" 2>&1 &
  E=$!
  sleep 2
  "$B/consume" --s3 "$S3/$PREFIX" --db "$DB" --worker lat --poll "$poll" --ttl 30s --margin 2s --budget 10s \
    --stats "$tmp/stats.json" --stats-every 1s > "$tmp/w.log" 2>&1 &
  W=$!
  sleep 3
  c0=$(cpu)
  # 3.7 requests/s per signal (not a multiple of the poll), 200 spans / log records, 20 points per type
  "$B/soaksend" -url http://127.0.0.1:24418 -producer edge-1 -signals traces,logs,metrics -rate 3.7 -rows 200 -points 20 \
    -duration "${SECS}s" -out "$tmp/acked.jsonl" > "$tmp/send.log" 2>&1
  sleep 5
  c1=$(cpu)
  kill -TERM $W $E 2>/dev/null; wait $W $E 2>/dev/null
  python3 - "$poll" "$c0" "$c1" "$tmp/stats.json" "$tmp/acked.jsonl" <<'EOF' | tee -a "$OUT"
import json, statistics, sys
poll, c0, c1, sf, af = sys.argv[1:]
s = json.load(open(sf))
acks = [json.loads(l)["ack_ms"] for l in open(af)]
objs = s["objects_inserted"] + s["series_objects_inserted"]
s3 = s["s3"]
print(json.dumps({"poll": poll, "objects": objs, "rows": s["rows_inserted"], "statements": s["statements"],
    "objects_per_statement": round(s["statement_objects"] / max(s["statements"], 1), 2),
    "visible_ms_p50": round(s["visible_ms_p50"]), "visible_ms_p90": round(s["visible_ms_p90"]),
    "visible_ms_p99": round(s["visible_ms_p99"]), "visible_ms_max": round(s["visible_ms_max"]),
    "edge_ack_ms_p50": round(statistics.median(acks), 1),
    "consumer_cpu_ms_per_object": round(s["cpu_ms"] / objs, 3),
    "consumer_cpu_us_per_row": round(s["cpu_ms"] * 1000 / max(s["rows_inserted"], 1), 2),
    "server_cpu_ms_per_object": round((int(c1) - int(c0)) / 1000 / objs, 2),
    "s3_per_object": {k: round(v / objs, 3) for k, v in s3.items()},
    "ch_checks_per_object": round(s["ch_checks"] / objs, 3), "elapsed_s": round(s["elapsed_ms"] / 1000)}))
EOF
  ch "DROP DATABASE IF EXISTS $DB SYNC"
  "$B/consume" purge --s3 "$S3/otel/clean/b5/lat/$RUN" > /dev/null
done
rm -rf "$tmp"
