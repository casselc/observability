#!/bin/bash
# Service-affine routing, locally: senders -> gateway (contrib load_balancing,
# routing_key: service, gateway-local.yaml) -> N Rust publishers
# (otap-rs/configs/edge-durable.yaml) -> SeaweedFS -> consume -> ClickHouse.
#
#   GW=.../otelcol-deploy BIN=.../otap-s3pq CONSUME=.../consume SEND=.../otlpsend \
#   MIX=dir-of-mixgen-batches W=workdir RUN=name SCEN=steady|gwkill|reroute|pubkill \
#   N=8 route_test.sh
#
# CFG picks the publisher configuration (default edge-durable.yaml; the
# deployment's is edge-publisher.yaml); CHECK=s3 skips the consumer
# (route_check.sh).
#
# Scenarios (the senders stream 32 distinct 10k-row traces requests and 32
# logs requests, 120 services each, resending on any transport error or 503,
# as an agent's persistent queue does):
#   steady   nothing happens
#   gwkill   SIGKILL the gateway 3 s in, restart it 2 s later; again at 9 s
#   gwterm   the same with SIGTERM (a rolling update): the gateway finishes
#            what is in flight before it exits
#   reroute  SIGKILL the gateway 3 s in, add publisher N (N+1 in the ring),
#            restart the gateway with the larger ring (a scale-up while
#            requests are in flight)
#   pubkill  SIGKILL publisher 0 3 s in, restart it 3 s later (same buffer)
#
# Writes $W/$RUN/summary.txt: per-publisher rows and services (affinity and
# skew), rows per object, and the central counts: rows, distinct rows,
# duplicate rows, and rows missing against the sent datasets.
set -u
here=$(cd "$(dirname "$0")" && pwd)
: "${GW:?}" "${BIN:?}" "${SEND:?}" "${MIX:?}" "${W:?}" "${RUN:?}"; [ "${CHECK:-consumer}" = s3 ] || : "${CONSUME:?}"
SCEN=${SCEN:-steady}; N=${N:-8}; export SCEN N GW CONSUME CHECK CFG
S3=${S3:-http://127.0.0.1:18333}; BUCKET=${BUCKET:-deploy-edge}; CH=${CH:-http://127.0.0.1:18123}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
export BIN RUN S3_ROOT=$S3/$BUCKET/route
export W=$W/$RUN
DB=deploy_$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)
mkdir -p "$W"
q() { curl -s "$CH/" --data-binary "$1"; }
ring() { local n=$1 i s=""; for i in $(seq 0 $((n-1))); do s="$s${s:+, }127.0.0.1:151$(printf %02d "$i")"; done; echo "[$s]"; }
gw_start() { GW_BACKENDS=$(ring "$1") setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/gw.pid" "$GW" --config "$here/gateway-local.yaml" >> "$W/gw.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14418/ && return; sleep 0.1; done; }
gw_kill() { kill -KILL "$(cat "$W/gw.pid")"; echo "$(date +%s.%N) gateway SIGKILL" >> "$W/events.log"; }

"$here/publishers.sh" start "$N"
gw_start "$N"
files() { ls "$MIX"/"$1"-b00*.pb | paste -sd,; }
t0=$(date +%s.%N)
# SENDERS concurrent senders per signal (as many agents), each with its own
# share of the 32 files; with one, a publisher never has two pieces to batch.
SENDERS=${SENDERS:-1}
senders=""
for sig in traces logs; do
  for k in $(seq 0 $((SENDERS-1))); do
    fl=$(ls "$MIX"/$sig-b00*.pb | awk -v k=$k -v n=$SENDERS 'NR % n == k' | paste -sd,)
    "$SEND" -url http://127.0.0.1:14418 -signal $sig -file "$fl" -n $(echo "$fl" | tr ',' '\n' | wc -l) -backoff 300ms -timeout 60s \
      > "$W/send-$sig-$k.jsonl" 2> "$W/send-$sig-$k.err" &
    senders="$senders $!"
  done
done
case $SCEN in
  gwkill) sleep 3; gw_kill; sleep 2; gw_start "$N"; echo "$(date +%s.%N) gateway up" >> "$W/events.log"
          sleep 4; gw_kill; sleep 2; gw_start "$N"; echo "$(date +%s.%N) gateway up" >> "$W/events.log" ;;
  gwterm) for _ in 1 2; do sleep 3; p=$(cat "$W/gw.pid"); kill -TERM "$p"; echo "$(date +%s.%N) gateway SIGTERM" >> "$W/events.log"
            while kill -0 "$p" 2>/dev/null; do sleep 0.1; done; echo "$(date +%s.%N) gateway exited" >> "$W/events.log"
            sleep 1; gw_start "$N"; echo "$(date +%s.%N) gateway up" >> "$W/events.log"; done ;;
  reroute) sleep 3; gw_kill; "$here/publishers.sh" start1 "$N"; sleep 1; gw_start $((N+1))
           echo "$(date +%s.%N) gateway up with $((N+1)) publishers" >> "$W/events.log" ;;
  pubkill) sleep 3; "$here/publishers.sh" kill 0; echo "$(date +%s.%N) publisher 0 SIGKILL" >> "$W/events.log"
           sleep 3; "$here/publishers.sh" start1 0; echo "$(date +%s.%N) publisher 0 up" >> "$W/events.log" ;;
esac
wait $senders
t1=$(date +%s.%N); echo "$t0" > "$W/t0"; echo "$t1" > "$W/t1"
sleep 4   # let the buffers' last segments (1 s open window) commit
kill -INT "$(cat "$W/gw.pid")" 2>/dev/null
"$here/publishers.sh" stop
exec "$here/route_check.sh"
