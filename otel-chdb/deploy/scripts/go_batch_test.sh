#!/bin/bash
# The Go edge with its batch step moved from the agents into the publisher,
# before its queue (2026-09-27), locally: K agents (base/go/agent-go.yaml,
# endpoints, ports and queue directories rewritten for this box) in front of
# one Go publisher (base/go/publisher-config.yaml on otelcol-s3pq), then the
# Rust consumer into ClickHouse, then the rows counted per signal.
#
#   SCEN=sizes      K agents (default 4), each fed by one sender per signal
#                   that sends NREQ SMALL-row requests every INTERVAL (a node's
#                   SDK traffic); rows and bytes per object are the result.
#   SCEN=bulk       1 agent, NREQ 10k-row requests per signal, back to back.
#   SCEN=killagent  1 agent, two senders per signal streaming NREQ requests;
#                   the agent is SIGKILLed KILLS times while they are in
#                   flight and restarted on the same queue; senders resend
#                   whatever was not answered 2xx.
#   SCEN=killpub    the same with the publisher SIGKILLed and restarted on
#                   its queue.
#   SCEN=killput    merged requests replayed after a crash mid-PUT: the
#                   publisher writes to S3 through faultproxy2 holding every
#                   PUT's answer 5 s (the object lands, the answer is late),
#                   and is SIGKILLed once objects have landed, then restarted
#                   straight to S3 on the same queue. Its queue items are the
#                   merged requests, so each replay must be byte-identical
#                   (same content key, same received_at): the consumer skips
#                   it and central has no duplicate row. Use requests well
#                   under 10k rows (MIX of 1,000-row requests) so that
#                   batches merge several. FAULTPROXY is faultproxy2.
#
# Every sender resends until it gets a 2xx, as an SDK with retries does, so
# anything missing at the end was acknowledged and lost.
#
#   GOCOL=otelcol-s3pq GW=otelcol-deploy CONSUME=consume SEND=otlpsend MIX=dir W=workdir RUN=name SCEN=... go_batch_test.sh
#
# MIX holds {traces,logs}-bNNNN.pb (bench/sorting/gen's mixgen; -rows 300 for
# sizes, 10000 or 1000 for the others). AGENT_CFG and PUB_CFG replace the
# configs: the "before" rows of results/go-batch.txt ran the configs of
# commit 30e5320 (the agents' batch processor at 10k / 2 s, a publisher that
# does not batch). BUCKET (ef-batch) and DBP (ef_batch_) name the bucket and
# the database; KEEP=1 keeps the database.
set -u
here=$(cd "$(dirname "$0")/.." && pwd)
: "${GOCOL:?}" "${GW:?}" "${CONSUME:?}" "${SEND:?}" "${MIX:?}" "${W:?}" "${RUN:?}" "${SCEN:?}"
S3=${S3:-http://127.0.0.1:18333}; CH=${CH:-http://127.0.0.1:18123}; BUCKET=${BUCKET:-ef-batch}; DBP=${DBP:-ef_batch_}
AGENT_CFG=${AGENT_CFG:-$here/base/go/agent-go.yaml}; PUB_CFG=${PUB_CFG:-$here/base/go/publisher-config.yaml}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
W=$W/$RUN; mkdir -p "$W"; DB=$DBP$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)
q() { curl -s "$CH/" --data-binary "$1"; }
s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" "$@"; }
s3 -X PUT "$S3/$BUCKET" > /dev/null
case $SCEN in
  sizes) K=${K:-4}; NREQ=${NREQ:-40}; INTERVAL=${INTERVAL:-1s}; PER=1 ;;
  bulk) K=1; NREQ=${NREQ:-16}; INTERVAL=0s; PER=1 ;;
  killagent|killpub) K=1; NREQ=${NREQ:-32}; INTERVAL=0s; PER=2; KILLS=${KILLS:-2} ;;
  killput) K=1; NREQ=${NREQ:-24}; INTERVAL=0s; PER=2; : "${FAULTPROXY:?}" ;;
  *) echo "SCEN?"; exit 2 ;;
esac

# The publisher: loopback ports, its queue in $W.
sed -e 's#0.0.0.0:4317#127.0.0.1:14617#; s#0.0.0.0:4318#127.0.0.1:14618#; s#0.0.0.0:13133#127.0.0.1:14633#' \
    -e 's#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 14688#' "$PUB_CFG" > "$W/publisher.yaml"
ROOT=$S3/$BUCKET/$RUN   # a root per run: the consumer keeps its checkpoints there
export PRODUCER=ef-$RUN S3_REGION=us-east-1 S3_BASE=$ROOT QUEUE_DIR=$W/pq
if [ "$SCEN" = killput ]; then
  "$FAULTPROXY" -listen 127.0.0.1:14699 -target "$S3" -match "/$RUN/" -mode answer-late -hold 5s > "$W/proxy.log" 2>&1 &
  proxy=$!; S3_BASE=http://127.0.0.1:14699/$BUCKET/$RUN; sleep 0.5
fi
pub() { setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/pub.pid" "$GOCOL" --config "$W/publisher.yaml" >> "$W/pub.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14618/ && return; sleep 0.1; done; }
# Agent i: OTLP/HTTP 1472i, its queue in $W/aq-i.
for i in $(seq 0 $((K-1))); do
  sed -e "s#/var/lib/otel-agent/queue#$W/aq-$i#g" -e "s#0.0.0.0:4317#127.0.0.1:1471$i#; s#0.0.0.0:4318#127.0.0.1:1472$i#" \
      -e "s#0.0.0.0:13133#127.0.0.1:1473$i#" -e "s#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 1474$i#" \
      -e 's#dns:///otelcol-publisher.otel-edge.svc.cluster.local:4317#127.0.0.1:14617#' "$AGENT_CFG" > "$W/agent-$i.yaml"
done
agent() { setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/agent-$1.pid" "$GW" --config "$W/agent-$1.yaml" >> "$W/agent-$1.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null "http://127.0.0.1:1472$1/" && return; sleep 0.1; done; echo "agent $1 did not start" >> "$W/summary.txt"; }
# SIGKILL and wait until the process is gone (its ports free) before a restart.
killwait() { local p; p=$(cat "$1"); kill -KILL "$p"; while kill -0 "$p" 2>/dev/null; do sleep 0.05; done; }

pub
for i in $(seq 0 $((K-1))); do agent "$i"; done
t0=$(date +%s.%N)
senders=""; nsend=0
for sig in traces logs; do
  files=($(ls "$MIX"/$sig-b*.pb))
  for i in $(seq 0 $((K-1))); do
    for j in $(seq 0 $((PER-1))); do
      s=$(( (i * PER + j) * NREQ ))
      f=$(printf '%s\n' "${files[@]:$s:$NREQ}" | paste -sd,)
      [ -z "$f" ] && { echo "MIX has too few $sig files"; exit 2; }
      "$SEND" -url "http://127.0.0.1:1472$i" -signal $sig -file "$f" -n "$NREQ" -interval "$INTERVAL" -timeout 90s -backoff 300ms \
        > "$W/send-$sig-$i-$j.jsonl" 2> "$W/send-$sig-$i-$j.err" &
      senders="$senders $!"; nsend=$((nsend + 1))
    done
  done
done
case $SCEN in
  killagent|killpub)
    for k in $(seq 1 "$KILLS"); do
      sleep "${KILL_EVERY:-3}"
      if [ $SCEN = killagent ]; then
        killwait "$W/agent-0.pid"; echo "agent SIGKILL at $(echo "$(date +%s.%N) - $t0" | bc | cut -c1-5) s" >> "$W/summary.txt"; agent 0
      else
        killwait "$W/pub.pid"; echo "publisher SIGKILL at $(echo "$(date +%s.%N) - $t0" | bc | cut -c1-5) s" >> "$W/summary.txt"; sleep 1; pub
      fi
    done ;;
  killput)
    # Once objects have landed (their answers still held), SIGKILL; restart
    # straight to S3.
    for _ in $(seq 200); do
      n=$(q "SELECT count() FROM s3('$ROOT/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'One') WHERE _size > 0" 2>/dev/null)
      [ "${n:-0}" -ge 4 ] 2>/dev/null && break; sleep 0.2
    done
    killwait "$W/pub.pid"; echo "publisher SIGKILL at $(echo "$(date +%s.%N) - $t0" | bc | cut -c1-5) s with $n objects landed, answers held" >> "$W/summary.txt"
    kill "$proxy"; S3_BASE=$ROOT; pub ;;
esac
wait $senders
# Drain: every agent queue empty, then the publisher's (graceful stop drains
# nothing we rely on: the consumer counts what is on S3).
for _ in $(seq 120); do
  busy=0
  for i in $(seq 0 $((K-1))); do
    n=$(curl -s "http://127.0.0.1:1474$i/metrics" | awk '/^otelcol_exporter_queue_size/ {s += $2} END {print s + 0}')
    [ "$n" != 0 ] && busy=1
  done
  n=$(curl -s http://127.0.0.1:14688/metrics | awk '/^otelcol_exporter_queue_size/ {s += $2} END {print s + 0}')
  [ "$n" != 0 ] && busy=1
  [ $busy = 0 ] && break; sleep 1
done
sleep 3
for i in $(seq 0 $((K-1))); do kill -INT "$(cat "$W/agent-$i.pid")"; done
kill -INT "$(cat "$W/pub.pid")"; sleep 3
q "DROP DATABASE IF EXISTS $DB"
"$CONSUME" --s3 "$ROOT" --ch "$CH" --db "$DB" --exit-after-idle 5s \
  --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" --poll 500ms > "$W/consume.log" 2>&1
U=$ROOT/$PRODUCER
{
  echo "scenario $SCEN: $K agent(s), $nsend senders, $NREQ requests each, interval $INTERVAL; agent $(basename "$AGENT_CFG"), publisher $(basename "$PUB_CFG")"
  echo "senders: $(grep -h '"summary"' "$W"/send-*.jsonl | grep -o '"attempts":[0-9]*' | cut -d: -f2 | paste -sd+ | bc) attempts for $((nsend * NREQ)) requests (more: resends)"
  echo "consumer: $(tail -1 "$W/consume.log" | grep -o '"objects_inserted":[0-9]*\|"rows_inserted":[0-9]*\|"dedup_skipped":[0-9]*\|"errors":[0-9]*' | paste -sd' ') (dedup_skipped: byte-identical copies it recognised)"
  for sig in traces logs; do
    want=$(( K * PER * NREQ * ${ROWS:-0} ))
    if [ $sig = traces ]; then id="TraceId, SpanId"; t=otel_traces; else id="Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes"; t=otel_logs; fi
    echo "$sig central: rows, distinct rows, duplicate rows, objects (content keys), epochs${ROWS:+; expected $want distinct}"
    q "SELECT count(), uniqExact($id), count() - uniqExact($id), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.$t FORMAT TSV"
    echo "$sig copies on S3 (the same rows in two objects: a replay or a resend): rows sets with copies, of which every copy has the same received_at"
    q "SELECT count(), countIf(nrecv = 1) FROM (SELECT fp, uniqExact(recv) nrecv, count() n FROM (SELECT _path, groupBitXor(cityHash64($id)) fp, any(received_at) recv FROM s3('$U/$sig/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet') GROUP BY _path) GROUP BY fp HAVING n > 1) FORMAT TSV"
    echo "$sig objects on S3: count, rows/object min / avg / max, bytes/object avg"
    q "SELECT count(), min(r), round(avg(r)), max(r), round(avg(sz)) FROM (SELECT _path, any(_size) sz, count() r FROM s3('$U/$sig/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'SpanId String') GROUP BY _path) FORMAT TSV"
  done
} >> "$W/summary.txt"
[ -z "${KEEP:-}" ] && q "DROP DATABASE IF EXISTS $DB"
rm -rf "$W/pq" "$W"/aq-*
cat "$W/summary.txt"
