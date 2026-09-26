#!/bin/bash
# The Go publisher (base/go/publisher-config.yaml on otelcol-s3pq) end to
# end, locally: the sender streams 16 distinct 10k-span, 16 distinct 10k-log
# and 16 distinct 10k-point metrics requests (2,000 points of each type); the
# publisher is SIGKILLed 4 s in and restarted on the same persistent queue;
# then the Rust consumer ingests every lane (traces, logs and layout B's
# points and series lanes) and the rows are counted.
#   GOCOL=otelcol-s3pq CONSUME=consume SEND=otlpsend MIX=... W=workdir RUN=name go_edge_test.sh
# MIX holds {traces,logs,metrics}-b00NN.pb (bench/sorting/gen's mixgen for
# traces and logs; otap-rs/tools otlpgen -metrics -variants 16 for metrics).
# BUCKET (deploy-edge) and DBP (deploy_) name the bucket and the database.
set -u
here=$(cd "$(dirname "$0")/.." && pwd)
: "${GOCOL:?}" "${CONSUME:?}" "${SEND:?}" "${MIX:?}" "${W:?}" "${RUN:?}"
S3=${S3:-http://127.0.0.1:18333}; CH=${CH:-http://127.0.0.1:18123}; BUCKET=${BUCKET:-deploy-edge}; DBP=${DBP:-deploy_}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
W=$W/$RUN; mkdir -p "$W/queue"; DB=$DBP$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)
q() { curl -s "$CH/" --data-binary "$1"; }
# Local only: loopback ports.
sed -e 's#0.0.0.0:4317#127.0.0.1:14617#; s#0.0.0.0:4318#127.0.0.1:14618#; s#0.0.0.0:13133#127.0.0.1:14633#' \
    -e 's#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 14688#' "$here/base/go/publisher-config.yaml" > "$W/publisher.yaml"
export PRODUCER=goedge-$RUN S3_REGION=us-east-1 S3_BASE=$S3/$BUCKET/edge QUEUE_DIR=$W/queue
pub() { setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/pub.pid" "$GOCOL" --config "$W/publisher.yaml" >> "$W/pub.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14618/ && return; sleep 0.1; done; }
pub
senders=""
for sig in traces logs metrics; do
  "$SEND" -url http://127.0.0.1:14618 -signal $sig -file "$(ls "$MIX"/$sig-b00*.pb | head -16 | paste -sd,)" -n 16 -backoff 300ms > "$W/send-$sig.jsonl" 2> "$W/send-$sig.err" &
  senders="$senders $!"
done
sleep 4; kill -KILL "$(cat "$W/pub.pid")"; echo "publisher SIGKILL 4 s in" > "$W/summary.txt"; sleep 1; pub
wait $senders
sleep 10
kill -INT "$(cat "$W/pub.pid")"; sleep 3
q "DROP DATABASE IF EXISTS $DB"
"$CONSUME" --s3 "$S3/$BUCKET/edge" --ch "$CH" --db "$DB" --exit-after-idle 5s \
  --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" --poll 500ms > "$W/consume.log" 2>&1
{
  for sig in traces logs metrics; do
    echo "sender $sig: $(tail -1 "$W/send-$sig.jsonl" | grep -o '"attempts":[0-9]*\|"ok":[0-9]*\|"requests":[0-9]*' | paste -sd' ') (16 requests)"
  done
  echo "publisher: $(grep -o 's3pq stop.*' "$W/pub.log" | tail -1 | cut -c1-300)"
  echo "consumer: $(tail -1 "$W/consume.log" | grep -o '"objects_inserted":[0-9]*\|"rows_inserted":[0-9]*\|"dedup_skipped":[0-9]*\|"over_count":[0-9]*\|"errors":[0-9]*' | paste -sd' ')"
  echo "traces: rows, distinct (TraceId, SpanId), objects, epochs; expected 160000"
  q "SELECT count(), uniqExact(TraceId, SpanId), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.otel_traces FORMAT TSV"
  echo "logs: rows, distinct, objects, epochs; expected 160000"
  q "SELECT count(), uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.otel_logs FORMAT TSV"
  echo "metrics points per table: rows, distinct (series_id, TimeUnix, StartTimeUnix), objects, epochs; expected 64000 number (gauge + sum), 32000 each other type"
  for t in number_points histogram_points exponential_histogram_points summary_points; do
    printf '%s\t' "$t"; q "SELECT count(), uniqExact(series_id, TimeUnix, StartTimeUnix, MetricName), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.otel_metrics_$t FORMAT TSV"
  done
  echo "series: rows, distinct series; points without a series row"
  q "SELECT count(), uniqExact(series_id) FROM $DB.otel_metrics_series FORMAT TSV"
  q "SELECT count() FROM $DB.otel_metrics_number_points WHERE series_id NOT IN (SELECT series_id FROM $DB.otel_metrics_series) FORMAT TSV"
} >> "$W/summary.txt"
rm -rf "$W/queue"
cat "$W/summary.txt"
