#!/bin/bash
# consumer_ckpt_soak.sh: scripts/consumer_soak.sh with many more epochs, to
# show the checkpoint stays bounded (README "Consumer", "Checkpoint
# compaction").
#
# The same fleet (3 edges behind 3 fault proxies, 3 workers, GC, the audit),
# with two chaos loops instead of one:
#   - edges: SIGKILL a random edge every EDGE_MIN..EDGE_MAX s (restarted at
#     once; each restart opens a new epoch in each of its lanes);
#   - workers: every CHAOS_MIN..CHAOS_MAX s, SIGKILL a worker (restarted 1-3 s
#     later) or SIGSTOP one past its lease.
# scripts/consumer_ckpt_sample.py records every SAMPLE s each lane's
# checkpoint (bytes, explicit epochs, floor), gc.json's size, and how many
# epoch directories a delimiter LIST of each lane returns.
# At the end scripts/consumer_soak_check.py gives the exactly-once verdict,
# and the samples are summarized.
#
#   B=bin OUT=results/consumer/ckpt-soak DURATION=1200 scripts/consumer_ckpt_soak.sh
set -u
B=${B:?dir with otap-s3pq, consume, soaksend, faultproxy2}
OUT=${OUT:-results/consumer/ckpt-soak}
DURATION=${DURATION:-1200}
RUN=${RUN:-ck$(date +%s)}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
BUCKET=${BUCKET:-otel}
DB=otaprs_ckpt_$RUN
RATE=${RATE:-3}
CHAOS_MIN=${CHAOS_MIN:-8}
CHAOS_MAX=${CHAOS_MAX:-20}
EDGE_MIN=${EDGE_MIN:-2}
EDGE_MAX=${EDGE_MAX:-5}
SAMPLE=${SAMPLE:-10}
ZOMBIE=${ZOMBIE:-60s}
here=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$OUT"
echo "$RUN" > "$OUT/run.txt"
PREFIX=otap-rs-ckpt/$RUN/edges
ROOT=$S3/$BUCKET/$PREFIX
log() { echo "$(date +%T.%3N) $*" | tee -a "$OUT/chaos.log"; }
ch() { curl -sS "$CH/" --data-binary "$1"; }

MODES=("-mode answer-late -hold 3s -every 7" "-mode apply-late -hold 2500ms -every 5" "-mode drop -hold 200ms -every 6")
declare -A PID
proxy() {
  "$B/faultproxy2" -listen 127.0.0.1:$((18340 + $1)) -target "$S3" -match "/otap-rs-ckpt/$RUN/" ${MODES[$(($1 - 1))]} \
    >> "$OUT/proxy-$1.log" 2>&1 &
  PID[proxy$1]=$!
}
edge() {
  local lanes=1; [ "$1" = 2 ] && lanes=2
  env OTLP_HTTP=127.0.0.1:$((24308 + 10 * $1)) OTLP_GRPC=127.0.0.1:$((24307 + 10 * $1)) ADMIN_HTTP=127.0.0.1:$((28080 + $1)) \
    PUT_TIMEOUT=1s LANES=$lanes PRODUCER=edge-$1 S3_URL=http://127.0.0.1:$((18340 + $1))/$BUCKET/$PREFIX/edge-$1 \
    "$B/otap-s3pq" -c "$here/scripts/consumer_soak_edge.yaml" >> "$OUT/edge-$1.log" 2>&1 &
  PID[edge$1]=$!
}
sender() {
  "$B/soaksend" -url http://127.0.0.1:$((24308 + 10 * $1)) -producer edge-$1 -signals traces,logs,metrics \
    -rate "$RATE" -rows 200 -points 20 -out "$OUT/acked-edge-$1.jsonl" -timeout 10s -backoff 300ms \
    >> "$OUT/send-$1.log" 2>&1 &
  PID[send$1]=$!
}
declare -A INC
worker() {
  INC[$1]=$((${INC[$1]:-0} + 1))
  "$B/consume" --s3 "$ROOT" --db "$DB" --worker "w$1" --poll 200ms --ttl 6s --margin 1s --budget 2s \
    --discover 1s --quiet 3s --full-list 10s --stats "$OUT/w$1-${INC[$1]}.stats.json" --stats-every 1s \
    >> "$OUT/w$1.log" 2>&1 &
  PID[w$1]=$!
}

log "run $RUN: db $DB, prefix $PREFIX, duration ${DURATION}s, edge restarts every ${EDGE_MIN}-${EDGE_MAX}s"
for i in 1 2 3; do proxy $i; done
sleep 0.5
for i in 1 2 3; do edge $i; done
sleep 2
for i in 1 2 3; do sender $i; done
for i in 1 2 3; do worker $i; done
"$B/consume" gc --s3 "$ROOT" --every 5s --delay 10s --zombie "$ZOMBIE" --run-for 100000s >> "$OUT/gc.log" 2>&1 &
PID[gc]=$!
"$B/consume" audit --s3 "$ROOT" --out "$OUT/committed.jsonl" --every 1s --run-for 100000s >> "$OUT/audit.log" 2>&1 &
PID[audit]=$!
python3 "$here/scripts/consumer_ckpt_sample.py" "$S3" "$BUCKET" "$PREFIX" "$OUT/ckpt.jsonl" "$SAMPLE" 2>> "$OUT/sample.log" &
PID[sample]=$!
( sleep 20; for t in $(ch "SELECT name FROM system.tables WHERE database = '$DB'"); do
    ch "ALTER TABLE $DB.$t MODIFY SETTING old_parts_lifetime = 20"; done ) &

end=$(( $(date +%s) + DURATION ))
disk_ok() {
  local avail; avail=$(df --output=avail -BM / | tail -1 | tr -dc 0-9)
  if [ "$avail" -lt 3500 ]; then log "disk: ${avail} MB free, stopping early"; return 1; fi
}
# ---- chaos: one loop, two schedules (edges; workers), so PID[] stays current -------------
n_kill=0 n_stop=0 n_edge=0
next_w=$(( $(date +%s) + CHAOS_MIN + RANDOM % (CHAOS_MAX - CHAOS_MIN + 1) ))
next_e=$(( $(date +%s) + EDGE_MIN + RANDOM % (EDGE_MAX - EDGE_MIN + 1) ))
stop_until=0 stopped=""
while [ "$(date +%s)" -lt "$end" ]; do
  sleep 0.5
  now=$(date +%s)
  if [ -n "$stopped" ] && [ "$now" -ge "$stop_until" ]; then
    kill -CONT "${PID[w$stopped]}" 2>/dev/null; log "SIGCONT w$stopped"; stopped=""
  fi
  if [ "$now" -ge "$next_e" ]; then
    disk_ok || break
    i=$((1 + RANDOM % 3)); kill -KILL "${PID[edge$i]}" 2>/dev/null; wait "${PID[edge$i]}" 2>/dev/null
    n_edge=$((n_edge + 1)); edge $i; log "SIGKILL + restart edge-$i ($n_edge)"
    next_e=$((now + EDGE_MIN + RANDOM % (EDGE_MAX - EDGE_MIN + 1)))
  fi
  if [ "$now" -ge "$next_w" ] && [ -z "$stopped" ]; then
    case $((RANDOM % 2)) in
      0) i=$((1 + RANDOM % 3)); log "SIGKILL worker w$i (pid ${PID[w$i]})"; kill -KILL "${PID[w$i]}" 2>/dev/null
         n_kill=$((n_kill + 1)); sleep $((1 + RANDOM % 3)); worker $i; log "restarted w$i (incarnation ${INC[$i]})" ;;
      1) i=$((1 + RANDOM % 3)); d=$((7 + RANDOM % 6)); log "SIGSTOP worker w$i for ${d}s (lease ttl 6s)"
         kill -STOP "${PID[w$i]}" 2>/dev/null; n_stop=$((n_stop + 1)); stopped=$i; stop_until=$((now + d)) ;;
    esac
    next_w=$((now + CHAOS_MIN + RANDOM % (CHAOS_MAX - CHAOS_MIN + 1)))
  fi
done
[ -n "$stopped" ] && kill -CONT "${PID[w$stopped]}" 2>/dev/null
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
kill -TERM "${PID[audit]}" "${PID[gc]}" "${PID[sample]}" 2>/dev/null
"$B/consume" audit --s3 "$ROOT" --out "$OUT/committed.jsonl" --run-for 0s >> "$OUT/audit.log" 2>&1
for i in 1 2 3; do kill -TERM "${PID[w$i]}" 2>/dev/null; done
sleep 1
for i in 1 2 3; do kill -TERM "${PID[edge$i]}" "${PID[proxy$i]}" 2>/dev/null; done
sleep 2
for i in 1 2 3; do kill -KILL "${PID[w$i]}" "${PID[edge$i]}" "${PID[proxy$i]}" 2>/dev/null; done
wait 2>/dev/null
python3 "$here/scripts/consumer_soak_check.py" "$OUT" "$DB" | tee "$OUT/summary.txt"
python3 - "$OUT" <<'EOF' | tee -a "$OUT/summary.txt"
import glob, json, os, sys
out = sys.argv[1]
s = [json.loads(l) for l in open(os.path.join(out, "ckpt.jsonl"))]
if s:
    t0 = s[0]["wall_ms"]
    print("checkpoint over time (t s: max bytes / max explicit epochs per lane; sum bytes; epoch dirs listed; gc.json bytes):")
    step = max(1, len(s) // 12)
    for x in s[::step] + ([s[-1]] if (len(s) - 1) % step else []):
        print(f"  t {(x['wall_ms'] - t0) / 1000:6.0f}: {x['ckpt_bytes_max']:7d} B / {x['ckpt_epochs_max']:4d}; "
              f"sum {x['ckpt_bytes_sum']:8d} B; dirs {x['dirs_sum']:6d}; gc.json {x['gc_bytes']:8d} B")
    print(f"peak: ckpt {max(x['ckpt_bytes_max'] for x in s)} B, {max(x['ckpt_epochs_max'] for x in s)} explicit epochs per lane; "
          f"gc.json {max(x['gc_bytes'] for x in s)} B")
lists = steps = full = 0
for f in glob.glob(os.path.join(out, "w*.stats.json")):
    try:
        w = json.load(open(f))
    except Exception:
        continue
    lists += w.get("s3", {}).get("list", 0)
    steps += w.get("steps", 0)
print(f"worker LISTs {lists} over {steps} polls: {lists / max(steps, 1):.2f} per poll")
EOF
