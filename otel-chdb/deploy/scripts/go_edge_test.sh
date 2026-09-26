#!/bin/bash
# The Go publisher (base/go/publisher-config.yaml on otelcol-awss3) end to
# end, locally: the sender streams 16 distinct 10k-span and 16 distinct
# 10k-log requests; the publisher is SIGKILLed 4 s in and restarted on the
# same persistent queue; then the Rust consumer ingests the lanes and the
# rows are counted.
#   GOCOL=otelcol-awss3 CONSUME=consume SEND=otlpsend MIX=... W=workdir RUN=name go_edge_test.sh
set -u
here=$(cd "$(dirname "$0")/.." && pwd)
: "${GOCOL:?}" "${CONSUME:?}" "${SEND:?}" "${MIX:?}" "${W:?}" "${RUN:?}"
S3=${S3:-http://127.0.0.1:18333}; CH=${CH:-http://127.0.0.1:18123}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
W=$W/$RUN; mkdir -p "$W/queue"; DB=deploy_$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)
q() { curl -s "$CH/" --data-binary "$1"; }
# Local only: plain http to SeaweedFS needs disable_ssl; loopback ports.
sed -e 's#0.0.0.0:4317#127.0.0.1:14617#; s#0.0.0.0:4318#127.0.0.1:14618#; s#0.0.0.0:13133#127.0.0.1:14633#' \
    -e 's#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 14688#' \
    -e 's#^      key_mode: sequence#      key_mode: sequence\n      disable_ssl: true#' "$here/base/go/publisher-config.yaml" > "$W/publisher.yaml"
export PRODUCER=goedge-$RUN S3_REGION=us-east-1 S3_BUCKET=deploy-edge S3_ENDPOINT=$S3 S3_FORCE_PATH_STYLE=true QUEUE_DIR=$W/queue
pub() { setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/pub.pid" "$GOCOL" --config "$W/publisher.yaml" >> "$W/pub.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14618/ && return; sleep 0.1; done; }
pub
senders=""
for sig in traces logs; do
  "$SEND" -url http://127.0.0.1:14618 -signal $sig -file "$(ls "$MIX"/$sig-b00*.pb | head -16 | paste -sd,)" -n 16 -backoff 300ms > "$W/send-$sig.jsonl" 2> "$W/send-$sig.err" &
  senders="$senders $!"
done
sleep 4; kill -KILL "$(cat "$W/pub.pid")"; echo "publisher SIGKILL 4 s in" > "$W/summary.txt"; sleep 1; pub
wait $senders
sleep 10
kill -INT "$(cat "$W/pub.pid")"; sleep 3
q "DROP DATABASE IF EXISTS $DB"
"$CONSUME" --s3 "$S3/deploy-edge/edge" --ch "$CH" --db "$DB" --signals traces,logs --exit-after-idle 5s \
  --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" --poll 500ms > "$W/consume.log" 2>&1
{
  echo "sender attempts: traces $(tail -1 "$W/send-traces.jsonl" | grep -o '"attempts":[0-9]*'), logs $(tail -1 "$W/send-logs.jsonl" | grep -o '"attempts":[0-9]*') (16 requests each)"
  echo "consumer: $(tail -1 "$W/consume.log" | grep -o '"objects_inserted":[0-9]*\|"rows_inserted":[0-9]*\|"dedup_skipped":[0-9]*\|"over_count":[0-9]*\|"errors":[0-9]*' | paste -sd' ')"
  echo "traces: rows, distinct (TraceId, SpanId), objects, epochs; expected 160000"
  q "SELECT count(), uniqExact(TraceId, SpanId), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.otel_traces FORMAT TSV"
  echo "logs: rows, distinct, objects, epochs; expected 160000"
  q "SELECT count(), uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.otel_logs FORMAT TSV"
} >> "$W/summary.txt"
rm -rf "$W/queue"
cat "$W/summary.txt"
