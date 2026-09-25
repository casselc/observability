#!/bin/bash
# End-to-end latency to query visibility: otlpsend -> otap-s3pq (commit to
# S3, then the OTLP response) -> consume (polling every POLL) -> INSERT into
# central. "visible_after_ms" is the time from the edge receiving a request
# (received_at, stamped in the object) to the consumer's INSERT returning,
# on one box, so no clock skew. One request per second, N requests.
#
#   B=target/release T=tools-bin D=data OUT=results/latency POLL=200ms scripts/latency.sh
set -u
B=${B:?}; T=${T:?}; D=${D:?}; OUT=${OUT:?}
POLL=${POLL:-200ms}; N=${N:-30}; SIG=${SIG:-traces}
CH=${CH:-http://127.0.0.1:18123}
RUN=l$(date +%s)
here=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$OUT"
DB=otaprs_lat_$RUN
curl -sS "$CH/" --data-binary "CREATE DATABASE IF NOT EXISTS $DB"
env OTLP_HTTP=127.0.0.1:14318 OTLP_GRPC=127.0.0.1:14317 S3_URL=http://127.0.0.1:18333/otel/otap-rs/latency/$RUN \
  "$B/otap-s3pq" -c "$here/configs/edge.yaml" > "$OUT/edge-$POLL.log" 2>&1 &
E=$!
"$B/consume" --s3 http://127.0.0.1:18333/otel/otap-rs/latency/$RUN --signal $SIG --table $DB.t --poll $POLL \
  --exit-after-idle 6s > "$OUT/consume-$POLL.log" 2>&1 &
C=$!
sleep 1.5
files=$(ls "$D"/$SIG-bench-v*.pb | head -$N | paste -sd,)
"$T/otlpsend" -url http://127.0.0.1:14318 -signal $SIG -file "$files" -n $N -interval 1s > "$OUT/send-$POLL.jsonl"
wait $C
kill -INT $E; wait $E 2>/dev/null
curl -sS "$CH/" --data-binary "DROP DATABASE IF EXISTS $DB"
echo "poll $POLL: edge ack (commit) $(tail -1 "$OUT/send-$POLL.jsonl")"
echo "poll $POLL: consumer $(grep '"summary"' "$OUT/consume-$POLL.log")"
