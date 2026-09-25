#!/usr/bin/env bash
# End-to-end: the ocb collector (stock awss3 + patched awss3inline, both with
# parquet_encoding and a persistent queue) behind faultproxy, which answers
# every 3rd PUT only after 8 s (past the 5 s exporter timeout; the object has
# landed by then). Midway the collector is killed with SIGKILL and restarted
# on the same queue directory. Then the objects are counted, and the inline
# prefix is consumed into a ClickHouse table with inlineconsume.
#
#   B=<dir with otelcol-awss3, faultproxy, inlineconsume> collector/run-demo.sh
set -euo pipefail
B=${B:?build dir}
HERE=$(cd "$(dirname "$0")" && pwd)
export RUN=${RUN:-$(date -u +%Y%m%dT%H%M%S)}
export DATA=$B/data-$RUN
export S3_ENDPOINT=http://127.0.0.1:18334
export AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret AWS_REGION=us-east-1
CH=${CH:-http://127.0.0.1:18123}
N=${N:-20}; SPANS=${SPANS:-50}
mkdir -p "$DATA"

"$B/faultproxy" -listen 127.0.0.1:18334 -target http://127.0.0.1:18333 -match "/s3inline/demo/$RUN/" -hold 8s -every 3 \
  > "$DATA/proxy.log" 2>&1 &
PROXY=$!
trap 'kill $PROXY 2>/dev/null || true' EXIT

start() {
  "$B/otelcol-awss3" --config "$HERE/config.yaml" >> "$DATA/collector.log" 2>&1 &
  COL=$!
  for _ in $(seq 50); do curl -s -o /dev/null http://127.0.0.1:14318/ && return; sleep 0.2; done
  echo "collector did not start"; exit 1
}

send() { # request i: SPANS spans with ids unique to it
  python3 - "$1" "$SPANS" <<'EOF' | curl -s -o /dev/null -w '%{http_code} ' -H 'Content-Type: application/json' --data-binary @- http://127.0.0.1:14318/v1/traces
import json, sys
i, n = int(sys.argv[1]), int(sys.argv[2])
spans = [{"traceId": "%032x" % (i * 1000 + j + 1), "spanId": "%016x" % (i * 1000 + j + 1), "name": "req%d-span%d" % (i, j),
          "kind": 2, "startTimeUnixNano": str(1758800000000000000 + i * 1000000 + j), "endTimeUnixNano": str(1758800000000000000 + i * 1000000 + j + 500)}
         for j in range(n)]
print(json.dumps({"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "demo"}}]},
                                      "scopeSpans": [{"spans": spans}]}]}))
EOF
}

start
echo "collector $COL up; sending $N requests x $SPANS spans"
for i in $(seq 1 "$N"); do send "$i"; done; echo
sleep 3
echo "SIGKILL collector $COL with answers still held back"
kill -9 "$COL"; wait "$COL" 2>/dev/null || true
sleep 9
echo "restart on the same queue directory"
start
sleep 30
kill -TERM "$COL"; wait "$COL" 2>/dev/null || true
count() { curl -s "$CH/" --data-binary "SELECT uniqExact(_path) FROM s3('http://127.0.0.1:18333/otel/s3inline/demo/$RUN/**/*.parquet', 'otel', 'otelsecret', 'One')"; }
echo "objects after the second run: $(count); faults off, third run to drain the queues"
kill "$PROXY"; wait "$PROXY" 2>/dev/null || true
"$B/faultproxy" -listen 127.0.0.1:18334 -target http://127.0.0.1:18333 -match "/s3inline/demo/$RUN/" -every 1000000000 \
  >> "$DATA/proxy.log" 2>&1 &
PROXY=$!
sleep 0.5
start
prev=-1; for _ in $(seq 60); do sleep 2; c=$(count); [ "$c" = "$prev" ] && break; prev=$c; done
kill -TERM "$COL"; wait "$COL" 2>/dev/null || true

q() { curl -s "$CH/" --data-binary "$1"; }
glob() { echo "s3('http://127.0.0.1:18333/otel/s3inline/demo/$RUN/$1', 'otel', 'otelsecret', 'Parquet')"; }
echo "--- proxy"; grep -c ' -> ' "$DATA/proxy.log" | sed 's/^/PUTs: /'
grep -c 'client gave up' "$DATA/proxy.log" | sed 's/^/answers the client never got: /' || true
echo "--- stock awss3exporter (random keys)"
echo "objects, rows, distinct spans: $(q "SELECT uniqExact(_path), count(), uniqExact(TraceId, SpanId) FROM $(glob 'stock/**/*.parquet') FORMAT TSV")"
echo "--- awss3inline (key_mode sequence)"
echo "epochs/objects: $(q "SELECT uniqExact(producer_epoch), uniqExact(_path), count(), uniqExact(TraceId, SpanId) FROM $(glob 'inline/traces/*/*.parquet') FORMAT TSV")"
q "SELECT _path, count() FROM $(glob 'inline/traces/*/*.parquet') GROUP BY _path ORDER BY _path FORMAT TSV" | sed "s#otel/s3inline/demo/$RUN/inline/traces/##"
echo "--- consumer"
"$B/inlineconsume" -prefix "s3inline/demo/$RUN/inline/traces" -table "default.s3inline_demo_$RUN" -once 2>&1 | tail -3
echo "central rows, distinct spans: $(q "SELECT count(), uniqExact(TraceId, SpanId) FROM default.s3inline_demo_$RUN FORMAT TSV") (sent $((N * SPANS)))"
echo "logs in $DATA"
