#!/bin/bash
# max_connection_age end to end (patches/0004): does a scaled-up publisher get
# traffic from agents that are already running, and is delivery still exactly
# once while connections are recycled under load?
#
# Two publishers (configs/edge-publisher.yaml) on 127.0.0.11 and .12, behind
# one DNS name served by scripts/tinydns.py (the stand-in for a headless
# Service's endpoints); one stock collector agent running
# ../deploy/base/rust/agent-rust.yaml (otlp_grpc `dns://` + round_robin,
# persistent queue) in front of them; telemetrygen load into the agent. At
# T_ADD a third publisher starts on .13 and joins the DNS answer. The agent is
# never restarted. Per-publisher accepted requests and aged_out/force_closed
# (receiver.otlp.connections) are sampled every 5 s; at the end, the agent's
# accepted spans are compared with the rows and distinct (TraceId, SpanId) in
# S3 (loss and duplicates) and with each publisher's share.
#
#   BIN=otap-s3pq GW=otelcol TG=telemetrygen W=workdir S3_ROOT=http://127.0.0.1:18333/bucket/prefix \
#     RUN=age AGE=20s GRACE=15s TIMEOUT=10s T_ADD=40 T_TOTAL=150 scripts/goaway_e2e.sh
#   AGE= (empty) drops max_connection_age from the config: the control run.
#   GRACE=0s cuts every connection at its age, in-flight requests included.
# GW: an otelcol with the otlp receiver, otlp_grpc exporter and file_storage
# (../deploy/collector). Needs ClickHouse (CH) for the S3 counts.
set -u
here=$(cd "$(dirname "$0")/.." && pwd); REPO=$(cd "$here/.." && pwd)
: "${BIN:?}" "${GW:?}" "${TG:?}" "${W:?}" "${S3_ROOT:?}" "${RUN:?}"
: "${AGE?}" "${GRACE?}" "${TIMEOUT:=10s}" "${T_ADD:=40}" "${T_TOTAL:=150}" "${RATE:=100}"
CH=${CH:-http://127.0.0.1:18123}; DNS_PORT=${DNS_PORT:-15353}
E=$W; W=$W/$RUN; rm -rf "$W"; mkdir -p "$W/queue"; U=$S3_ROOT/$RUN
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
# a box with an HTTPS proxy: in a cluster the Service name is in NO_PROXY, so is ours here
export NO_PROXY="${NO_PROXY:-},ga-pubs.test" no_proxy="${no_proxy:-},ga-pubs.test"
q() { curl -s "$CH/" --data-binary "$1"; }
ts() { echo $(( $(date +%s) - t0 )); }

# publisher config: the repo's edge-publisher.yaml, minus the age for a control run
cfg=$W/edge-publisher.yaml
if [ -n "$AGE" ]; then cp -f "$here/configs/edge-publisher.yaml" "$cfg"
else grep -v 'max_connection_age' "$here/configs/edge-publisher.yaml" > "$cfg"; fi

pub() { # i -> 127.0.0.1(1+i)
  local i=$1 ip=127.0.0.1$((1+$1))
  mkdir -p "$W/buf-$i"
  setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/pub-$i.pid" env \
    MEM_SOURCE=rss OTLP_GRPC=$ip:14317 OTLP_HTTP=$ip:14318 ADMIN_HTTP=$ip:16080 \
    PRODUCER=ga-$RUN-$i BUFFER_DIR="$W/buf-$i" S3_URL="$U/pub-$i" OTLP_TIMEOUT=$TIMEOUT \
    OTLP_MAX_CONN_AGE="${AGE:-5m}" OTLP_MAX_CONN_AGE_GRACE="$GRACE" \
    "$BIN" -c "$cfg" >> "$W/pub-$i.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null "http://$ip:16080/api/v1/livez" && break; sleep 0.1; done
}
metric() { # ip name -> sum over the receiver's grpc buckets
  curl -s "http://$1:16080/api/v1/metrics" | awk -v n="$2" '$1 ~ "^"n"\\{" && /protocol="grpc"/ {s+=$2} END{print s+0}'
}

printf '127.0.0.11\n127.0.0.12\n' > "$E/addrs.txt"
if ! [ -e "$E/dns.pid" ] || ! kill -0 "$(cat "$E/dns.pid")" 2>/dev/null; then
  setsid bash -c 'echo $$ > "$0"; exec "$@"' "$E/dns.pid" python3 "$here/scripts/tinydns.py" \
    127.0.0.1 "$DNS_PORT" ga-pubs.test "$E/addrs.txt" > "$E/dns.out" 2>&1 < /dev/null &
  sleep 1
fi
pub 0; pub 1
sed -e "s#/var/lib/otel-agent/queue#$W/queue#g" -e 's#0.0.0.0:4317#127.0.0.1:14617#; s#0.0.0.0:4318#127.0.0.1:14618#' \
    -e 's#0.0.0.0:13133#127.0.0.1:14633#' -e 's#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 14688#' \
    -e "s#dns:///otap-publisher.otel-edge.svc.cluster.local:4317#dns://127.0.0.1:$DNS_PORT/ga-pubs.test:14317#" \
    "$REPO/deploy/base/rust/agent-rust.yaml" > "$W/agent.yaml"
setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/agent.pid" "$GW" --config "$W/agent.yaml" >> "$W/agent.log" 2>&1 < /dev/null &
for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14633/ && break; sleep 0.1; done
t0=$(date +%s); dns0=$(cat "$E/addrs.txt.log" 2>/dev/null | wc -l)
setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/tg.pid" "$TG" traces --otlp-endpoint 127.0.0.1:14617 --otlp-insecure \
  --workers 4 --rate "$RATE" --duration "${T_TOTAL}s" --child-spans 4 > "$W/tg.log" 2>&1 < /dev/null &

echo "t  pub0 pub1 pub2  (accepted requests; aged_out/force_closed per publisher)" > "$W/timeline.txt"
added=0
while [ "$(ts)" -lt $((T_TOTAL + 5)) ]; do
  if [ $added = 0 ] && [ "$(ts)" -ge "$T_ADD" ]; then
    pub 2; echo 127.0.0.13 >> "$E/addrs.txt"; added=1; echo "$(ts) pub2 started, DNS now 3 addresses" >> "$W/timeline.txt"
  fi
  line="$(ts)"; ages=""
  for i in 0 1 2; do ip=127.0.0.1$((1+i))
    line="$line $(metric $ip accepted_total)"; ages="$ages $(metric $ip aged_out_total)/$(metric $ip force_closed_total)"; done
  echo "$line  |$ages" >> "$W/timeline.txt"
  sleep 5
done

# drain: agent queue empty, then S3 rows stable
am() { curl -s http://127.0.0.1:14688/metrics | awk -v n="$1" '$1 ~ "^"n && /publishers/ {s+=$2} END{print s+0}'; }
for _ in $(seq 120); do [ "$(am otelcol_exporter_queue_size)" = 0 ] && break; sleep 1; done
acc=$(curl -s http://127.0.0.1:14688/metrics | awk '$1 ~ /^otelcol_receiver_accepted_spans/ {s+=$2} END{print s+0}')
sent=$(am otelcol_exporter_sent_spans); failed=$(am otelcol_exporter_send_failed_spans)
src="s3('$U/*/traces/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'TraceId String, SpanId String')"
prev=-1
for _ in $(seq 40); do n=$(q "SELECT count() FROM $src"); [ "$n" = "$acc" ] && break; [ "$n" = "$prev" ] && [ "$n" != 0 ] && sleep 10 && [ "$(q "SELECT count() FROM $src")" = "$n" ] && break; prev=$n; sleep 3; done
{
  echo "RUN=$RUN AGE=${AGE:-off} GRACE=$GRACE TIMEOUT=$TIMEOUT T_ADD=$T_ADD T_TOTAL=$T_TOTAL"
  echo "agent: accepted_spans=$acc sent_spans=$sent send_failed_spans(retried)=$failed queue=$(am otelcol_exporter_queue_size)"
  echo "S3 rows, distinct (TraceId,SpanId), duplicates, missing vs agent-accepted:"
  q "SELECT count(), uniqExact(TraceId, SpanId), count() - uniqExact(TraceId, SpanId), $acc - uniqExact(TraceId, SpanId) FROM $src FORMAT TSV"
  echo "S3 rows per publisher:"
  q "SELECT extract(_path, 'pub-[0-9]') p, count() FROM $src GROUP BY p ORDER BY p FORMAT TSV"
  echo "agent export failures (each retried): $(grep -c 'Exporting failed' "$W/agent.log")"
  echo "publisher force-close warnings: $(cat "$W"/pub-*.log | grep -c 'connection_age.force_close')"
  echo "DNS queries answered during the run: $(( $(wc -l < "$E/addrs.txt.log") - dns0 ))"
} > "$W/summary.txt"
kill -INT "$(cat "$W/agent.pid")"; sleep 2
for i in 0 1 2; do kill -INT "$(cat "$W/pub-$i.pid")" 2>/dev/null; done; sleep 3
cat "$W/timeline.txt" "$W/summary.txt"
