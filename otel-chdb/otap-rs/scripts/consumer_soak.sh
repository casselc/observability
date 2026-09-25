#!/bin/bash
# consumer_soak.sh: the consumer fleet under chaos, time-compressed.
#
#   senders (soaksend: distinct requests, resent until 2xx)
#     -> 3 edges (otap-s3pq, 7 lanes each: traces, logs, metrics layout B)
#     -> 3 fault proxies (faultproxy2: ambiguous, slow and dropped PUTs)
#     -> SeaweedFS
#     <- 3 consumer workers sharing the 21 lanes (leases + checkpoints on S3)
#     -> ClickHouse (a private database)
#   plus `consume gc` every few seconds and `consume audit` recording every
#   committed object (the ground truth, kept after GC deletes it).
#
# Chaos, one event every CHAOS_MIN..CHAOS_MAX s: SIGKILL a worker (restarted
# 1-3 s later as a new incarnation), SIGSTOP a worker for longer than its
# lease (a zombie: it resumes past its window), or SIGKILL an edge (restarted
# at once; its senders resend what they had no 2xx for, into new epochs).
# Lease timing is compressed: ttl 6 s, margin 1 s, budget 2 s, quiet 3 s.
#
# At the end the senders stop, every worker runs, and once central is
# stable scripts/consumer_soak_check.py compares central with the audit and
# with what the senders were acked.
#
#   B=bin OUT=results/consumer/soak DURATION=1800 scripts/consumer_soak.sh
set -u
B=${B:?dir with otap-s3pq, consume, soaksend, faultproxy2}
OUT=${OUT:-results/consumer/soak}
DURATION=${DURATION:-1800}
RUN=${RUN:-soak$(date +%s)}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
DB=otaprs_consumer_$RUN
RATE=${RATE:-3}          # traces and logs requests/s per edge (metrics: 1/s)
CHAOS_MIN=${CHAOS_MIN:-8}
CHAOS_MAX=${CHAOS_MAX:-20}
here=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$OUT"
echo "$RUN" > "$OUT/run.txt"
PREFIX=otel/otap-rs-consumer/$RUN/edges
ROOT=$S3/$PREFIX
log() { echo "$(date +%T.%3N) $*" | tee -a "$OUT/chaos.log"; }
ch() { curl -sS "$CH/" --data-binary "$1"; }

# ---- proxies and edges --------------------------------------------------------------
MODES=("-mode answer-late -hold 3s -every 7" "-mode apply-late -hold 2500ms -every 5" "-mode drop -hold 200ms -every 6")
declare -A PID
proxy() { # i
  "$B/faultproxy2" -listen 127.0.0.1:$((18340 + $1)) -target "$S3" -match "/otap-rs-consumer/$RUN/" ${MODES[$(($1 - 1))]} \
    >> "$OUT/proxy-$1.log" 2>&1 &
  PID[proxy$1]=$!
}
edge() { # i
  local lanes=1; [ "$1" = 2 ] && lanes=2
  env OTLP_HTTP=127.0.0.1:$((24308 + 10 * $1)) OTLP_GRPC=127.0.0.1:$((24307 + 10 * $1)) ADMIN_HTTP=127.0.0.1:$((28080 + $1)) \
    PUT_TIMEOUT=1s LANES=$lanes PRODUCER=edge-$1 S3_URL=http://127.0.0.1:$((18340 + $1))/$PREFIX/edge-$1 \
    "$B/otap-s3pq" -c "$here/scripts/consumer_soak_edge.yaml" >> "$OUT/edge-$1.log" 2>&1 &
  PID[edge$1]=$!
}
sender() { # i
  "$B/soaksend" -url http://127.0.0.1:$((24308 + 10 * $1)) -producer edge-$1 -signals traces,logs,metrics \
    -rate "$RATE" -rows 200 -points 20 -out "$OUT/acked-edge-$1.jsonl" -timeout 10s -backoff 300ms \
    >> "$OUT/send-$1.log" 2>&1 &
  PID[send$1]=$!
}
declare -A INC
worker() { # i
  INC[$1]=$((${INC[$1]:-0} + 1))
  "$B/consume" --s3 "$ROOT" --db "$DB" --worker "w$1" --poll 200ms --ttl 6s --margin 1s --budget 2s \
    --discover 1s --quiet 3s --full-list 10s --stats "$OUT/w$1-${INC[$1]}.stats.json" --stats-every 1s \
    >> "$OUT/w$1.log" 2>&1 &
  PID[w$1]=$!
}

log "run $RUN: db $DB, prefix $PREFIX, duration ${DURATION}s"
for i in 1 2 3; do proxy $i; done
sleep 0.5
for i in 1 2 3; do edge $i; done
sleep 2
for i in 1 2 3; do sender $i; done
for i in 1 2 3; do worker $i; done
"$B/consume" gc --s3 "$ROOT" --every 5s --delay 10s --zombie 60s --run-for 100000s >> "$OUT/gc.log" 2>&1 &
PID[gc]=$!
"$B/consume" audit --s3 "$ROOT" --out "$OUT/committed.jsonl" --every 1s --run-for 100000s >> "$OUT/audit.log" 2>&1 &
PID[audit]=$!

# The test box's disk: replaced parts of these small, frequent inserts stay
# for old_parts_lifetime (8 min by default), about 10x the live data here.
# Shorten it on this run's tables (a test setting, not the product's DDL).
( sleep 20; for t in $(ch "SELECT name FROM system.tables WHERE database = '$DB'"); do
    ch "ALTER TABLE $DB.$t MODIFY SETTING old_parts_lifetime = 20"; done ) &

# ---- chaos -------------------------------------------------------------------------------
end=$(( $(date +%s) + DURATION ))
n_kill=0 n_stop=0 n_edge=0
while [ "$(date +%s)" -lt "$end" ]; do
  sleep $((CHAOS_MIN + RANDOM % (CHAOS_MAX - CHAOS_MIN + 1)))
  avail=$(df --output=avail -BM / | tail -1 | tr -dc 0-9)
  if [ "$avail" -lt 3500 ]; then log "disk: ${avail} MB free, stopping early"; break; fi
  case $((RANDOM % 3)) in
    0) i=$((1 + RANDOM % 3)); log "SIGKILL worker w$i (pid ${PID[w$i]})"; kill -KILL "${PID[w$i]}" 2>/dev/null
       n_kill=$((n_kill + 1)); ( sleep $((1 + RANDOM % 3)) ) ; worker $i; log "restarted w$i (incarnation ${INC[$i]})" ;;
    1) i=$((1 + RANDOM % 3)); d=$((7 + RANDOM % 6)); log "SIGSTOP worker w$i for ${d}s (lease ttl 6s)"
       kill -STOP "${PID[w$i]}" 2>/dev/null; n_stop=$((n_stop + 1)); sleep "$d"; kill -CONT "${PID[w$i]}" 2>/dev/null; log "SIGCONT w$i" ;;
    2) i=$((1 + RANDOM % 3)); log "SIGKILL edge-$i (pid ${PID[edge$i]})"; kill -KILL "${PID[edge$i]}" 2>/dev/null
       wait "${PID[edge$i]}" 2>/dev/null; n_edge=$((n_edge + 1)); sleep 0.$((RANDOM % 9 + 1)); edge $i; log "restarted edge-$i" ;;
  esac
done
log "chaos over: $n_kill worker kills, $n_stop worker pauses, $n_edge edge kills"

# ---- drain -------------------------------------------------------------------------------
for i in 1 2 3; do kill -TERM "${PID[send$i]}" 2>/dev/null; done
for i in 1 2 3; do wait "${PID[send$i]}" 2>/dev/null; done
log "senders done: $(cat "$OUT"/acked-edge-*.jsonl | wc -l) requests acked"
for i in 1 2 3; do kill -CONT "${PID[w$i]}" 2>/dev/null; done
tables="otel_traces otel_logs otel_metrics_number_points otel_metrics_histogram_points otel_metrics_exponential_histogram_points otel_metrics_summary_points"
last="" stable=0
for _ in $(seq 1 120); do
  sleep 2
  cur=$(for t in $tables; do ch "SELECT uniqExact(content_key), count() FROM $DB.$t FORMAT TSV" 2>/dev/null; done | tr '\n\t' '  ')
  if [ "$cur" = "$last" ]; then stable=$((stable + 1)); else stable=0; fi
  last=$cur
  [ $stable -ge 10 ] && break
done
sleep 2
log "central stable: $cur"
kill -TERM "${PID[audit]}" "${PID[gc]}" 2>/dev/null
"$B/consume" audit --s3 "$ROOT" --out "$OUT/committed.jsonl" --run-for 0s >> "$OUT/audit.log" 2>&1
for i in 1 2 3; do kill -TERM "${PID[w$i]}" 2>/dev/null; done
sleep 1
for i in 1 2 3; do kill -TERM "${PID[edge$i]}" "${PID[proxy$i]}" 2>/dev/null; done
sleep 2
for i in 1 2 3; do kill -KILL "${PID[w$i]}" "${PID[edge$i]}" "${PID[proxy$i]}" 2>/dev/null; done
wait 2>/dev/null
python3 "$here/scripts/consumer_soak_check.py" "$OUT" "$DB" | tee "$OUT/summary.txt"
