#!/bin/bash
# Start or stop N local Rust publishers (otap-rs/configs/edge-durable.yaml),
# publisher i on OTLP/gRPC 127.0.0.1:151(i), OTLP/HTTP 152(i), admin 153(i)
# (two-digit i), each with its own buffer directory and producer lane:
#
#   BIN=.../otap-s3pq W=workdir RUN=r1 S3_ROOT=http://127.0.0.1:18333/deploy-edge/route \
#     publishers.sh start 8            # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the env
#   W=workdir publishers.sh stop       # SIGINT each (graceful), by recorded pid
#   W=workdir publishers.sh kill 3     # SIGKILL publisher 3 only
#   ... publishers.sh start1 3         # (re)start publisher 3 only, same buffer
set -u
here=$(cd "$(dirname "$0")/../.." && pwd)
CFG=${CFG:-$here/otap-rs/configs/edge-durable.yaml}
cmd=$1; n=${2:-8}
start1() {
  local i=$1 ii; ii=$(printf %02d "$i")
  mkdir -p "$W/buf-$i"
  setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/pub-$i.pid" env \
    MEM_SOURCE=rss OTLP_GRPC=127.0.0.1:151$ii OTLP_HTTP=127.0.0.1:152$ii ADMIN_HTTP=127.0.0.1:153$ii \
    PRODUCER=pub-$i BUFFER_DIR="$W/buf-$i" BUFFER_CAP="${BUFFER_CAP:-1 GiB}" VERBOSE=true \
    S3_URL="$S3_ROOT/$RUN" "$BIN" -c "$CFG" >> "$W/pub-$i.log" 2>&1 < /dev/null &
}
case $cmd in
  start) mkdir -p "$W"; for i in $(seq 0 $((n-1))); do start1 "$i"; done
         for i in $(seq 0 $((n-1))); do ii=$(printf %02d "$i")
           for _ in $(seq 100); do curl -s -o /dev/null "http://127.0.0.1:152$ii/" && break; sleep 0.1; done; done ;;
  start1) start1 "$n" ;;
  stop) for f in "$W"/pub-*.pid; do kill -INT "$(cat "$f")" 2>/dev/null; done; sleep 2 ;;
  kill) kill -KILL "$(cat "$W/pub-$n.pid")" ;;
esac
