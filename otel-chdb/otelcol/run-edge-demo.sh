#!/usr/bin/env bash
# Edge-buffer mode end to end: run otelcol-chdb with config.edge.yaml, send it
# OTLP load, stop it (which seals the open generations), then act as the
# central consumer: read the seal manifests from S3, attach the published
# tables read-only in a separate chDB, and check them against the Parquet.
#
#   CHDB_LIB_PATH=/path/to/libchdb.so TELEMETRYGEN=/path/to/telemetrygen \
#   S3_ENDPOINT=http://127.0.0.1:8333/otel S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=... \
#   ./run-edge-demo.sh [seconds of load]
set -euo pipefail
: "${CHDB_LIB_PATH:?set CHDB_LIB_PATH to libchdb.so}"
: "${TELEMETRYGEN:?set TELEMETRYGEN to a telemetrygen binary}"
: "${S3_ENDPOINT:?set S3_ENDPOINT to an http(s) URL with the bucket}"
: "${S3_ACCESS_KEY_ID:?}" "${S3_SECRET_ACCESS_KEY:?}"
load=${1:-10}
export PRODUCER_ID=${PRODUCER_ID:-edge-demo} REGION=${REGION:-demo}
here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

[ -x _build/otelcol-chdb ] || go run go.opentelemetry.io/collector/cmd/builder@v0.161.0 --config builder-config.yaml
(cd ../chdbexporter && go build -o "$here/_build/chdbq" ./cmd/chdbq && go build -o "$here/_build/chdbattach" ./cmd/chdbattach)

rm -rf data
mkdir -p data
./_build/otelcol-chdb --config config.edge.yaml >data/collector.log 2>&1 &
col=$!
trap 'kill "$col" 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do
	curl -s -o /dev/null http://127.0.0.1:4318/ && break
	sleep 0.2
done
echo "telemetrygen: ${load}s of traces and logs over OTLP/gRPC"
"$TELEMETRYGEN" traces --otlp-insecure --otlp-endpoint 127.0.0.1:4317 --duration "${load}s" \
	--workers 4 --rate 2000 --child-spans 4 >data/tg-traces.log 2>&1 &
tg1=$!
"$TELEMETRYGEN" logs --otlp-insecure --otlp-endpoint 127.0.0.1:4317 --duration "${load}s" \
	--workers 2 --rate 5000 >data/tg-logs.log 2>&1 &
tg2=$!
wait "$tg1" "$tg2"
sent_spans=$(grep -o '"traces": [0-9]*' data/tg-traces.log | awk '{s+=$2} END {print s*5}')
sent_logs=$(grep -o '"logs": [0-9]*' data/tg-logs.log | awk '{s+=$2} END {print s}')

kill -TERM "$col"
wait "$col" || true
trap - EXIT
grep -iE '"level":"error"|\berror\b.*chdb|panic' data/collector.log | head -5 || true

creds="'$S3_ACCESS_KEY_ID', '$S3_SECRET_ACCESS_KEY'"
q() { ./_build/chdbq -format "${2:-PrettyCompactMonoBlock}" data/consumer-q "$1"; }
ns() { echo "$S3_ENDPOINT/$REGION/$1/v1/$PRODUCER_ID"; }
# The newest epoch: epochs start with their UTC start time, so they sort.
epoch=$(q "SELECT splitByChar('/', _path)[-4] FROM s3('$(ns traces)/*/manifests/*/_sealed.json', $creds, 'One') ORDER BY 1 DESC LIMIT 1" TSV)

echo
echo "== sent by telemetrygen: $sent_spans spans, $sent_logs logs; producer $PRODUCER_ID epoch $epoch"
echo "== seal manifests"
q "SELECT JSONExtractString(json, 'signal') AS signal, JSONExtractString(json, 'generation') AS generation,
          JSONExtractUInt(json, 'batches') AS batches, JSONExtractUInt(json, 'rows') AS rows
   FROM s3('$S3_ENDPOINT/$REGION/{traces,logs}/v1/$PRODUCER_ID/$epoch/manifests/*/_sealed.json', $creds, 'JSONAsString') ORDER BY signal"
echo "== batch manifests: every committed batch, and the Parquet beside it"
q "SELECT JSONExtractString(json, 'signal') AS signal, count() AS batches, sum(JSONExtractUInt(json, 'rows')) AS rows,
          min(JSONExtractUInt(json, 'batch_id')) AS first, max(JSONExtractUInt(json, 'batch_id')) AS last
   FROM s3('$S3_ENDPOINT/$REGION/{traces,logs}/v1/$PRODUCER_ID/$epoch/manifests/*/0*.json', $creds, 'JSONAsString') GROUP BY signal ORDER BY signal"
q "SELECT 'parquet traces' AS what, count() AS rows, uniqExact(batch_id) AS batches FROM s3('$S3_ENDPOINT/parquet/$REGION/traces/v1/$PRODUCER_ID/$epoch/*/*.parquet', $creds, 'Parquet')
   UNION ALL
   SELECT 'parquet logs', count(), uniqExact(batch_id) FROM s3('$S3_ENDPOINT/parquet/$REGION/logs/v1/$PRODUCER_ID/$epoch/*/*.parquet', $creds, 'Parquet')"

echo "== the published traces tables, attached read-only by a separate process from the seal manifest"
seal=$(q "SELECT _path FROM s3('$(ns traces)/$epoch/manifests/*/_sealed.json', $creds, 'One') LIMIT 1" TSV)
./_build/chdbattach -path data/consumer-attach -key "$S3_ACCESS_KEY_ID" -secret "$S3_SECRET_ACCESS_KEY" \
	-manifest "${S3_ENDPOINT%/*}/$seal" \
	"SELECT count() AS spans, uniqExact(batch_id) AS batches, uniqExact(TraceId) AS traces, (SELECT count() FROM {trace_id_ts}) AS trace_id_rows FROM {table}" \
	"SELECT ServiceName, SpanName, count() AS spans, round(quantile(0.95)(Duration) / 1e6, 3) AS p95_ms FROM {table} GROUP BY ALL ORDER BY spans DESC LIMIT 5" \
	"SELECT table, count() AS active_parts, sum(rows) AS rows FROM system.parts WHERE active AND database = 'r' GROUP BY table ORDER BY table"
