#!/usr/bin/env bash
# Build otelcol-chdb, run it, send it OTLP, stop it, and read what it stored.
#
#   CHDB_LIB_PATH=/path/to/libchdb.so ./run-demo.sh [seconds of load]
#
# Sends one hand-written OTLP/HTTP JSON trace and log (so there is a known
# row to look for), then load from telemetrygen over gRPC if TELEMETRYGEN
# points at it (go install .../cmd/telemetrygen@v0.161.0). The collector holds
# the chDB path while it runs, so the data is read back after it stops, with
# chdbq (../chdbexporter/cmd/chdbq).
set -euo pipefail
: "${CHDB_LIB_PATH:?set CHDB_LIB_PATH to libchdb.so}"
load=${1:-10}
here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

[ -x _build/otelcol-chdb ] || go run go.opentelemetry.io/collector/cmd/builder@v0.161.0 --config builder-config.yaml
cli=_build/chdbq
[ -x "$cli" ] || (cd ../chdbexporter && go build -o "$here/$cli" ./cmd/chdbq)

rm -rf data
mkdir -p data
./_build/otelcol-chdb --config config.yaml >data/collector.log 2>&1 &
col=$!
trap 'kill "$col" 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do
	curl -s -o /dev/null http://127.0.0.1:4318/ && break
	sleep 0.2
done

now=$(date +%s%N)
curl -sf -H 'Content-Type: application/json' http://127.0.0.1:4318/v1/traces -d @- <<JSON >/dev/null
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"demo-checkout"}}]},
 "scopeSpans":[{"scope":{"name":"demo"},"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",
 "name":"POST /checkout","kind":2,"startTimeUnixNano":"$now","endTimeUnixNano":"$((now + 42000000))",
 "attributes":[{"key":"http.response.status_code","value":{"intValue":"502"}}],
 "status":{"code":2,"message":"payment gateway timeout"}}]}]}]}
JSON
curl -sf -H 'Content-Type: application/json' http://127.0.0.1:4318/v1/logs -d @- <<JSON >/dev/null
{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"demo-checkout"}}]},
 "scopeLogs":[{"scope":{"name":"demo"},"logRecords":[{"timeUnixNano":"$now","severityNumber":17,"severityText":"ERROR",
 "body":{"stringValue":"gateway timed out after 40ms"},"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174"}]}]}]}
JSON
echo "sent one trace and one log over OTLP/HTTP JSON"

if [ -n "${TELEMETRYGEN:-}" ]; then
	echo "telemetrygen: ${load}s of traces and logs over OTLP/gRPC"
	"$TELEMETRYGEN" traces --otlp-insecure --otlp-endpoint 127.0.0.1:4317 --duration "${load}s" \
		--workers 4 --rate 2000 --child-spans 4 >data/tg-traces.log 2>&1 &
	tg1=$!
	"$TELEMETRYGEN" logs --otlp-insecure --otlp-endpoint 127.0.0.1:4317 --duration "${load}s" \
		--workers 2 --rate 5000 >data/tg-logs.log 2>&1 &
	tg2=$!
	wait "$tg1" "$tg2"
fi

# SIGTERM: the receiver stops, the queue drains into chDB, the sessions close.
kill -TERM "$col"
wait "$col" || true
trap - EXIT
grep -iE "error|panic" data/collector.log | grep -v '"level":"info"' | head -5 || true

q() { "$cli" data/chdb "$1"; }
echo
echo "== stored (read back from data/chdb after the collector exited)"
q "SELECT 'spans', count() FROM otel.otel_traces UNION ALL SELECT 'logs', count() FROM otel.otel_logs UNION ALL SELECT 'trace ids', count() FROM otel.otel_traces_trace_id_ts"
echo "== the hand-written trace, joined to its log"
q "SELECT t.ServiceName, t.SpanName, t.StatusCode, t.StatusMessage, t.SpanAttributes['http.response.status_code'] AS status, round(t.Duration / 1e6, 1) AS ms, l.Body
   FROM otel.otel_traces t JOIN otel.otel_logs l ON l.TraceId = t.TraceId AND l.SpanId = t.SpanId
   WHERE t.TraceId = '5b8efff798038103d269b633813fc60c'"
echo "== spans per service"
q "SELECT ServiceName, count() AS spans, countIf(StatusCode = 'Error') AS errors, round(quantile(0.95)(Duration) / 1e6, 2) AS p95_ms
   FROM otel.otel_traces GROUP BY ServiceName ORDER BY spans DESC"
