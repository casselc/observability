#!/bin/bash
# A publisher whose durable buffer can't take writes must report not ready
# (README §Durable buffer, edgeprobe/): locally, one publisher of EDGE (rust:
# otap-rs/configs/edge-publisher.yaml; go: base/go/publisher-config.yaml)
# with its buffer on a tmpfs, S3 cut by kind/relay.py, and the manifests'
# readiness command (edgeprobe, thresholds scaled to the small volume) run
# every second into probe.log. Two cases:
#
#   CASE=volume  the tmpfs (VOL, 48m) is smaller than the buffer's cap: the
#                filesystem fills (the kind wedge). Then S3 comes back, then
#                the volume is grown (a remount standing in for PVC
#                expansion), then the publisher is restarted (the runbook).
#   CASE=cap     the tmpfs is larger than the cap (rust: BUFFER_CAP 192 MiB,
#                the engine's minimum; go: QUEUE_BYTES 150 MB of OTLP): the
#                buffer's own cap is reached. Then S3 comes back.
#
# A sender streams distinct 10k-span requests and resends each until 2xx;
# it is stopped once the probe has reported not ready for a while. At the
# end every request the publisher acknowledged must be on S3, once.
#
#   EDGE=rust|go CASE=volume|cap BIN=otap-s3pq GOCOL=otelcol-s3pq PROBE=edgeprobe SEND=otlpsend MIX=dir W=workdir RUN=name wedge_test.sh
#
# Needs root (mount). MNT (/mnt/ef-wedge) is mounted and unmounted here.
set -u
here=$(cd "$(dirname "$0")/.." && pwd)
: "${EDGE:?}" "${CASE:?}" "${PROBE:?}" "${SEND:?}" "${MIX:?}" "${W:?}" "${RUN:?}"
S3=${S3:-http://127.0.0.1:18333}; CH=${CH:-http://127.0.0.1:18123}; BUCKET=${BUCKET:-ef-wedge}
MNT=${MNT:-/mnt/ef-wedge}; VOL=${VOL:-48m}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
W=$W/$RUN; mkdir -p "$W"
q() { curl -s "$CH/" --data-binary "$1"; }
s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" "$@"; }
log() { echo "$(date +%T.%N | cut -c1-12) $*" | tee -a "$W/events.txt"; }
s3 -X PUT "$S3/$BUCKET" > /dev/null
[ "$CASE" = cap ] && VOL=${VOL_CAP:-400m}
mkdir -p "$MNT"; mountpoint -q "$MNT" && umount "$MNT"
mount -t tmpfs -o size="$VOL",uid=0 tmpfs "$MNT"
# S3 through a relay that can be cut (SIGUSR1 toggles down).
python3 "$here/kind/relay.py" 127.0.0.2 18333 > "$W/relay.log" 2>&1 &
relay=$!; sleep 0.5
ROOT=http://127.0.0.2:18333/$BUCKET/$RUN
if [ "$EDGE" = rust ]; then
  : "${BIN:?}"
  BUF=$MNT/buffer; OTLP=127.0.0.1:14918
  CAP=$([ "$CASE" = cap ] && echo "192 MiB" || echo "1 GiB")
  start() { setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/pub.pid" env ADMIN_HTTP=127.0.0.1:14980 OTLP_HTTP=$OTLP \
      OTLP_GRPC=127.0.0.1:14917 PRODUCER=wedge BUFFER_DIR="$BUF" BUFFER_CAP="$CAP" S3_URL="$ROOT/wedge" \
      "$BIN" -c "$here/../otap-rs/configs/edge-publisher.yaml" >> "$W/pub.log" 2>&1 < /dev/null & }
  probe() { "$PROBE" -ready http://127.0.0.1:14980/api/v1/readyz -dir "$BUF" -min-free "${MIN_FREE:-8Mi}" \
      -metrics http://127.0.0.1:14980/api/v1/metrics -used storage_bytes_used_bytes -cap storage_bytes_cap_bytes \
      -match otel_scope_name=processor.durable_buffer -max-fill 0.95; }
else
  : "${GOCOL:?}"
  BUF=$MNT/queue; OTLP=127.0.0.1:14818
  sed -e 's#0.0.0.0:4317#127.0.0.1:14817#; s#0.0.0.0:4318#127.0.0.1:14818#; s#0.0.0.0:13133#127.0.0.1:14833#' \
      -e 's#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 14888#' "$here/base/go/publisher-config.yaml" > "$W/publisher.yaml"
  QB=$([ "$CASE" = cap ] && echo 150000000 || echo 17179869184)
  start() { setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/pub.pid" env PRODUCER=wedge S3_REGION=us-east-1 S3_BASE="$ROOT" \
      QUEUE_DIR="$BUF" QUEUE_BYTES=$QB "$GOCOL" --config "$W/publisher.yaml" >> "$W/pub.log" 2>&1 < /dev/null & }
  probe() { "$PROBE" -ready http://127.0.0.1:14833/ -dir "$BUF" -min-free "${MIN_FREE:-8Mi}" \
      -metrics http://127.0.0.1:14888/metrics -used otelcol_exporter_queue_size -cap otelcol_exporter_queue_capacity \
      -match exporter=s3pq -max-fill 0.95; }
fi
waitup() { for _ in $(seq 150); do curl -s -o /dev/null "http://$OTLP/" && return; sleep 0.2; done; log "publisher did not start"; }
( while :; do printf '%s rc=%s %s\n' "$(date +%T)" "$(probe > "$W/p.out" 2>&1; echo $?)" "$(head -c 300 "$W/p.out")" >> "$W/probe.log"; sleep 1; done ) &
prober=$!
state() { tail -1 "$W/probe.log" | cut -d' ' -f2; }
waitstate() { # rc seconds
  for _ in $(seq "$2"); do [ "$(state)" = "rc=$1" ] && return 0; sleep 1; done; return 1; }

kill -USR1 $relay; log "S3 down"
start; waitup; sleep 2; log "publisher up, probe $(state)"
"$SEND" -url "http://$OTLP" -signal traces -file "$(ls "$MIX"/traces-b*.pb | head -${NREQ:-40} | paste -sd,)" -n "${NREQ:-40}" \
  -timeout 20s -backoff 500ms > "$W/send.jsonl" 2> "$W/send.err" &
sender=$!
if waitstate 1 180; then log "probe NOT READY: $(tail -1 "$W/probe.log" | cut -d' ' -f3-)"; else log "probe never went not ready"; fi
sleep "${HOLD:-15}"
kill "$sender" 2>/dev/null; wait "$sender" 2>/dev/null
acked=$(grep -c '"seq"' "$W/send.jsonl"); log "sender stopped: $acked requests acknowledged; probe $(state); buffer volume $(df -h "$MNT" | awk 'NR==2 {print $3 "/" $2}')"
kill -USR1 $relay; log "S3 back"
if waitstate 0 "${RECOVER:-90}"; then log "probe READY again after S3 came back"
else
  log "still not ready ${RECOVER:-90} s after S3 came back: $(tail -1 "$W/probe.log" | cut -d' ' -f3-)"
  mount -o remount,size="${GROW:-160m}" "$MNT"; log "volume grown to ${GROW:-160m}"
  if waitstate 0 60; then log "probe READY after the volume grew"; else log "still not ready after the volume grew"; fi
fi
count() { q "SELECT count(), uniqExact(TraceId, SpanId) FROM s3('$S3/$BUCKET/$RUN/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'TraceId String, SpanId String') FORMAT TSV" | tr '\t' ' '; }
want=$((acked * 10000))
for _ in $(seq 60); do set -- $(count); [ "${2:-0}" -ge "$want" ] && break; sleep 2; done
log "S3 rows / distinct: $(count) (acknowledged: $want)"
if [ "${2:-0}" -lt "$want" ]; then
  kill -INT "$(cat "$W/pub.pid")"; sleep 5; kill -KILL "$(cat "$W/pub.pid")" 2>/dev/null
  start; waitup; log "publisher restarted (the runbook's second step)"
  for _ in $(seq 60); do set -- $(count); [ "${2:-0}" -ge "$want" ] && break; sleep 2; done
  log "S3 rows / distinct after the restart: $(count) (acknowledged: $want); probe $(state)"
fi
kill "$prober"; kill -INT "$(cat "$W/pub.pid")"; sleep 3; kill -KILL "$(cat "$W/pub.pid")" 2>/dev/null
kill $relay; sleep 0.5; umount "$MNT"
{ echo "== $EDGE / $CASE: probe transitions"; awk '{print $2}' "$W/probe.log" | uniq -c | awk '{print "  " $2 " x" $1}'
  echo "== first not-ready reasons"; grep 'rc=1' "$W/probe.log" | cut -d' ' -f3- | cut -c1-160 | sort | uniq -c | sort -rn | head -5; } >> "$W/events.txt"
cat "$W/events.txt"
