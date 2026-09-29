#!/bin/bash
# The orderly close end to end (../../FORMAT.md §3.1, DECISIONS.md D35): an
# edge takes the datasets, is stopped in order (SIGINT), and must leave a
# close as the last slot of each of its lanes' epochs; the consumer then
# ingests the root and retires every lane at R = its close's low, and the
# cluster's complete_through passes them (close_check.py).
#
#   B=bins D=datasets EDGE=rust|rust-durable|go scripts/close_e2e.sh
#
# B holds otap-s3pq (EDGE=rust*), otelcol-s3pq (EDGE=go), consume, otlpsend;
# D the request files of tools/cmd/otlpgen -out. Env: S3
# (http://127.0.0.1:18333), BUCKET (goedge-conf), CH (http://127.0.0.1:18123),
# RUN, DATASETS (traces, logs and metrics testgen-3000), KEEP=1 keeps the database.
# RESTART=1 (rust-durable): then the edge starts again on the same buffer
# (a rolling restart: a new epoch, and a replay of whatever the buffer still
# held, all committed before the close), stops in order again, and the
# consumer must see the lanes reborn, pass the replay as copies (nothing
# quarantined) and retire them again at the second close.
set -u
here=$(cd "$(dirname "$0")" && pwd)
: "${B:?}" "${D:?}" "${EDGE:?}"
RUN=${RUN:-close$(date +%s)}
S3=${S3:-http://127.0.0.1:18333}; BUCKET=${BUCKET:-goedge-conf}; CH=${CH:-http://127.0.0.1:18123}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret} AWS_REGION=us-east-1
DATASETS=${DATASETS:-"traces-testgen-3000 logs-testgen-3000 metrics-testgen-3000"}
W=${W:-/tmp/close-e2e}/$RUN; mkdir -p "$W"
ROOT=$S3/$BUCKET/$RUN/$EDGE
PRODUCER=close-$EDGE
db=close_$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)

wait_up() { for _ in $(seq 150); do curl -s -o /dev/null "http://$1/" && return 0; sleep 0.2; done; echo "edge on $1 did not start"; return 1; }
start_edge() {
case $EDGE in
  rust)
    ADMIN_HTTP=127.0.0.1:14480 OTLP_HTTP=127.0.0.1:14418 OTLP_GRPC=127.0.0.1:14417 PRODUCER=$PRODUCER CLUSTER=closee2e \
      HEARTBEAT=1h S3_URL=$ROOT "$B/otap-s3pq" -c "$here/../configs/edge.yaml" > "$W/edge.log" 2>&1 &
    port=127.0.0.1:14418 ;;
  rust-durable)
    mkdir -p "$W/buffer"
    ADMIN_HTTP=127.0.0.1:14480 OTLP_HTTP=127.0.0.1:14418 OTLP_GRPC=127.0.0.1:14417 PRODUCER=$PRODUCER CLUSTER=closee2e \
      HEARTBEAT=1h BUFFER_DIR=$W/buffer S3_URL=$ROOT "$B/otap-s3pq" -c "$here/../configs/edge-durable.yaml" > "$W/edge.log" 2>&1 &
    port=127.0.0.1:14418 ;;
  go)
    OTLP_HTTP=127.0.0.1:14518 HEALTH=127.0.0.1:14533 PRODUCER=$PRODUCER CLUSTER=closee2e HEARTBEAT=1h S3_URL=$ROOT \
      "$B/otelcol-s3pq" --config "$here/../../conformance/go-edge.yaml" > "$W/edge.log" 2>&1 &
    port=127.0.0.1:14518 ;;
  *) echo "EDGE: rust, rust-durable or go"; exit 2 ;;
esac
}
start_edge; E=$!
wait_up $port || exit 1
for ds in $DATASETS; do
  "$B/otlpsend" -url "http://$port" -signal "${ds%%-*}" -file "$D/$ds.pb" -n 1 -timeout 120s -quiet >> "$W/send.log" 2>&1 \
    || echo "send $ds failed" >> "$W/send.log"
done
kill -INT $E; wait $E 2>/dev/null
grep -i "close" "$W/edge.log" | tail -5
python3 "$here/close_check.py" "$ROOT" closee2e "$PRODUCER" || rc=1
curl -s "$CH/" --data-binary "DROP DATABASE IF EXISTS $db" > /dev/null
"$B/consume" --s3 "$ROOT" --ch "$CH" --db "$db" --exit-after-idle 5s --poll 300ms \
  --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > "$W/consume.log" 2>&1 || echo "consume exited $?"
grep -E "retired|QUARANTINED" "$W/consume.log" | head -10
"$B/consume" watermark --s3 "$ROOT" --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > "$W/watermark.log" 2>&1 \
  || echo "consume watermark exited $?"
python3 "$here/close_check.py" "$ROOT" closee2e "$PRODUCER" --consumed || rc=1
if [ -n "${RESTART:-}" ]; then
  echo "--- restart on the same buffer"
  start_edge; E=$!
  wait_up $port || exit 1
  sleep 3; kill -INT $E; wait $E 2>/dev/null
  grep -i "close" "$W/edge.log" | tail -2
  "$B/consume" --s3 "$ROOT" --ch "$CH" --db "$db" --exit-after-idle 5s --poll 300ms --stats "$W/stats2.json" \
    --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > "$W/consume2.log" 2>&1 || echo "consume exited $?"
  grep -E "reborn|retired|QUARANTINED" "$W/consume2.log" | head -20
  "$B/consume" watermark --s3 "$ROOT" --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" >> "$W/watermark.log" 2>&1
  python3 "$here/close_check.py" "$ROOT" closee2e "$PRODUCER" --consumed || rc=1
fi
[ -z "${KEEP:-}" ] && curl -s "$CH/" --data-binary "DROP DATABASE IF EXISTS $db" > /dev/null
exit "${rc:-0}"
