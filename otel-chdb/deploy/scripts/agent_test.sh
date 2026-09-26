#!/bin/bash
# The node agent's delivery contract, locally: base/rust/agent-rust.yaml
# (endpoints and queue directory rewritten for this box) in front of one
# Rust publisher (otap-rs/configs/edge-durable.yaml).
#   1. publisher down; the sender pushes 16 distinct 10k-span and 16 distinct
#      10k-log requests into the agent, which acks them (batch processor,
#      then its persistent queue);
#   2. the agent is SIGKILLed 5 s later (its batches have been flushed into
#      the queue), the publisher is started, the agent restarted on the same
#      queue directory;
#   3. after the drain: count the objects' rows, distinct rows, rows per
#      object and objects with identical content, straight from S3.
#   GW=otelcol-deploy BIN=otap-s3pq SEND=otlpsend MIX=... W=workdir RUN=name agent_test.sh
set -u
here=$(cd "$(dirname "$0")/.." && pwd)
: "${GW:?}" "${BIN:?}" "${SEND:?}" "${MIX:?}" "${W:?}" "${RUN:?}"
S3=${S3:-http://127.0.0.1:18333}; CH=${CH:-http://127.0.0.1:18123}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
W=$W/$RUN; mkdir -p "$W/queue"
q() { curl -s "$CH/" --data-binary "$1"; }
sed -e "s#/var/lib/otel-agent/queue#$W/queue#g" -e 's#0.0.0.0:4317#127.0.0.1:14517#; s#0.0.0.0:4318#127.0.0.1:14518#' \
    -e 's#0.0.0.0:13133#127.0.0.1:14533#' -e 's#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 14588#' \
    -e 's#dns:///otap-publisher.otel-edge.svc.cluster.local:4317#127.0.0.1:15100#' "$here/base/rust/agent-rust.yaml" > "$W/agent.yaml"
agent() { setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/agent.pid" "$GW" --config "$W/agent.yaml" >> "$W/agent.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14518/ && return; sleep 0.1; done; }
agent
t0=$(date +%s)
for sig in traces logs; do
  "$SEND" -url http://127.0.0.1:14518 -signal $sig -file "$(ls "$MIX"/$sig-b00*.pb | head -16 | paste -sd,)" -n 16 > "$W/send-$sig.jsonl" 2> "$W/send-$sig.err"
done
sleep 5
kill -KILL "$(cat "$W/agent.pid")"; echo "agent SIGKILL after $(( $(date +%s) - t0 )) s; queue dir $(du -sm "$W/queue" | cut -f1) MB" > "$W/summary.txt"
W=$W RUN=$RUN S3_ROOT=$S3/deploy-edge/agent BIN=$BIN "$here/scripts/publishers.sh" start 1
agent
for _ in $(seq 120); do n=$(grep -c 'rows=' "$W/pub-0.log"); [ "$n" -ge 32 ] && break; sleep 1; done
sleep 3
kill -INT "$(cat "$W/agent.pid")"; W=$W "$here/scripts/publishers.sh" stop
U=$S3/deploy-edge/agent/$RUN/pub-0
{
  echo "sender: $(tail -1 "$W/send-traces.jsonl" | grep -o '"attempts":[0-9]*') traces, $(tail -1 "$W/send-logs.jsonl" | grep -o '"attempts":[0-9]*') logs, 16 requests each (acked by the agent while the publisher was down)"
  for sig in traces logs; do
    if [ $sig = traces ]; then id="TraceId, SpanId"; st="TraceId String, SpanId String"
    else id="Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes"; st="Timestamp DateTime64(9), TraceId String, SpanId String, ServiceName String, Body String, LogAttributes Map(String, String)"; fi
    src="s3('$U/$sig/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', '$st')"
    echo "$sig: rows, distinct rows, objects, max rows per object, extra copies of an object's content (expected 160000 rows, all distinct)"
    q "SELECT sum(r), (SELECT uniqExact($id) FROM $src), count(), max(r), count() - uniqExact(fp) FROM (SELECT _path, count() r, groupBitXor(cityHash64($id)) fp FROM $src GROUP BY _path) FORMAT TSV"
  done
} >> "$W/summary.txt"
rm -rf "$W/queue" "$W"/buf-*
cat "$W/summary.txt"
