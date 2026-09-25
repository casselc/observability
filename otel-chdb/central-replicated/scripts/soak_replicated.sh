#!/bin/bash
# soak_replicated.sh: otap-rs/scripts/consumer_soak.sh against a replicated,
# zero-copy, S3-tiered central (two replicas, three Keeper nodes).
#
#   senders -> 3 edges -> 3 fault proxies -> SeaweedFS
#     <- 3 consumer workers, split across the replicas on purpose
#        (w1, w3: --ch r1,r2; w2: --ch r2,r1), so lane takeovers cross replicas
#     -> ReplicatedMergeTree tables (scripts/ddl.py: hot local volume, TTL move
#        to the zero-copy S3 volume after MOVE, allow_remote_fs_zero_copy_replication = 1)
#
# Chaos, one event every CHAOS_MIN..CHAOS_MAX s, one of:
#   worker SIGKILL | worker SIGSTOP past its lease | edge SIGKILL      (as consumer_soak.sh)
#   replica SIGKILL, restarted after 5-20 s (start-replicas.sh)
#   Keeper node SIGKILL, restarted after 3-8 s (one of three: quorum kept)
#   Keeper quorum loss: two nodes SIGKILLed for 8-15 s (tables go read-only)
# At the end: drain, SYNC REPLICA on both replicas, then consumer_soak_check.py
# against EACH replica.
#
#   OUT=results/soak DURATION=900 SYNC=1 scripts/soak_replicated.sh
set -u
. "$(dirname "$0")/common.sh"
OTAP=/home/user/oscope/spike/otel-chdb/otap-rs
B=${B:-$SP/consumer/bin}                      # otap-s3pq, soaksend, faultproxy2
CONSUME=${CONSUME:-$SP/otap-rs-target/release/consume}
OUT=${OUT:-$HERE/results/soak}
DURATION=${DURATION:-900}
RUN=${RUN:-rsoak$(date +%s)}
S3=http://127.0.0.1:18333
DB=${DB:-rsoak_$RUN}
RATE=${RATE:-2}
CHAOS_MIN=${CHAOS_MIN:-8}
CHAOS_MAX=${CHAOS_MAX:-20}
MOVE=${MOVE:-3 MINUTE}
SYNC=${SYNC:-1}                               # 0: the negative control, no --sync-replica
KINDS=${KINDS:-0 1 2 3 3 4 4 5}               # chaos kinds drawn from (3 = replica, 4 = keeper node, 5 = keeper quorum)
mkdir -p "$OUT"
echo "$RUN" > "$OUT/run.txt"
PREFIX=otel/otap-rs-consumer/$RUN/edges
ROOT=$S3/$PREFIX
log() { echo "$(date +%T.%3N) $*" | tee -a "$OUT/chaos.log"; }

# ---- central: the replicated tables on both replicas -------------------------------------
python3 "$HERE/scripts/ddl.py" --db "$DB" --policy tiered_zc --zero-copy 1 --move "$MOVE" --delete '1 DAY' \
  --extra 'old_parts_lifetime = 30' > "$OUT/ddl.sql"
apply_sql 29000 "$OUT/ddl.sql" && apply_sql 39000 "$OUT/ddl.sql" || { log "DDL failed"; exit 1; }
for r in $R1 $R2; do curl -sS "$r/" --data-binary "SYSTEM FLUSH LOGS" >/dev/null; done
snap() { # tag: server-wide counters of both replicas, and the Keeper leader's mntr
  for r in 1 2; do
    curl -sS "$([ $r = 1 ] && echo $R1 || echo $R2)/" --data-binary \
      "SELECT '$1', 'r$r', name, value FROM system.events WHERE name LIKE 'S3%' OR name LIKE 'DiskS3%' OR name LIKE 'ZooKeeper%' OR name IN ('OSCPUVirtualTimeMicroseconds','UserTimeMicroseconds','SystemTimeMicroseconds','InsertedRows','MergedRows','ReplicatedPartFetches','ReplicatedPartMerges') FORMAT TSV" >> "$OUT/events.tsv"
  done
  for k in 1 2 3; do (exec 3<>/dev/tcp/127.0.0.1/2918$k && echo mntr >&3 && timeout 1 cat <&3 | sed "s/^/$1\tk$k\t/") >> "$OUT/keeper-mntr.tsv" 2>/dev/null; done
}
snap start
date +%s > "$OUT/t_start"

# ---- proxies, edges, senders, workers (as consumer_soak.sh) --------------------------------
MODES=("-mode answer-late -hold 3s -every 7" "-mode apply-late -hold 2500ms -every 5" "-mode drop -hold 200ms -every 6")
declare -A PID
proxy() {
  "$B/faultproxy2" -listen 127.0.0.1:$((18340 + $1)) -target "$S3" -match "/otap-rs-consumer/$RUN/" ${MODES[$(($1 - 1))]} \
    >> "$OUT/proxy-$1.log" 2>&1 &
  PID[proxy$1]=$!
}
edge() {
  local lanes=1; [ "$1" = 2 ] && lanes=2
  env OTLP_HTTP=127.0.0.1:$((24308 + 10 * $1)) OTLP_GRPC=127.0.0.1:$((24307 + 10 * $1)) ADMIN_HTTP=127.0.0.1:$((28080 + $1)) \
    PUT_TIMEOUT=1s LANES=$lanes PRODUCER=edge-$1 S3_URL=http://127.0.0.1:$((18340 + $1))/$PREFIX/edge-$1 \
    "$B/otap-s3pq" -c "$OTAP/scripts/consumer_soak_edge.yaml" >> "$OUT/edge-$1.log" 2>&1 &
  PID[edge$1]=$!
}
sender() {
  "$B/soaksend" -url http://127.0.0.1:$((24308 + 10 * $1)) -producer edge-$1 -signals traces,logs,metrics \
    -rate "$RATE" -rows 200 -points 20 -out "$OUT/acked-edge-$1.jsonl" -timeout 10s -backoff 300ms \
    >> "$OUT/send-$1.log" 2>&1 &
  PID[send$1]=$!
}
declare -A INC
CHS=([1]="$R1,$R2" [2]="$R2,$R1" [3]="$R1,$R2")
worker() {
  INC[$1]=$((${INC[$1]:-0} + 1))
  local extra=(--no-ddl)
  [ "$SYNC" = 1 ] && extra+=(--sync-replica --sync-timeout 3s)
  "$CONSUME" --s3 "$ROOT" --db "$DB" --ch "${CHS[$1]}" "${extra[@]}" --worker "w$1" --poll 200ms --ttl 6s --margin 1s --budget 2s \
    --discover 1s --quiet 3s --full-list 10s --stats "$OUT/w$1-${INC[$1]}.stats.json" --stats-every 1s \
    >> "$OUT/w$1.log" 2>&1 &
  PID[w$1]=$!
}

log "run $RUN: db $DB, prefix $PREFIX, duration ${DURATION}s, sync $SYNC, workers w1,w3 -> r1 first, w2 -> r2 first"
for i in 1 2 3; do proxy $i; done
sleep 0.5
for i in 1 2 3; do edge $i; done
sleep 2
for i in 1 2 3; do sender $i; done
for i in 1 2 3; do worker $i; done
"$CONSUME" gc --s3 "$ROOT" --every 5s --delay 10s --zombie 60s --run-for 100000s >> "$OUT/gc.log" 2>&1 &
PID[gc]=$!
"$CONSUME" audit --s3 "$ROOT" --out "$OUT/committed.jsonl" --every 1s --run-for 100000s >> "$OUT/audit.log" 2>&1 &
PID[audit]=$!

# ---- chaos ---------------------------------------------------------------------------------
kpid() { cat "$SP/crep/$1/pid"; }
end=$(( $(date +%s) + DURATION ))
declare -A N
KA=($KINDS)
while [ "$(date +%s)" -lt "$end" ]; do
  sleep $((CHAOS_MIN + RANDOM % (CHAOS_MAX - CHAOS_MIN + 1)))
  avail=$(df --output=avail -BM / | tail -1 | tr -dc 0-9)
  if [ "$avail" -lt 2800 ]; then log "disk: ${avail} MB free, stopping early"; break; fi
  kind=${KA[$((RANDOM % ${#KA[@]}))]}
  N[$kind]=$((${N[$kind]:-0} + 1))
  case $kind in
    0) i=$((1 + RANDOM % 3)); log "SIGKILL worker w$i"; kill -KILL "${PID[w$i]}" 2>/dev/null
       sleep $((1 + RANDOM % 3)); worker $i; log "restarted w$i (incarnation ${INC[$i]})" ;;
    1) i=$((1 + RANDOM % 3)); d=$((7 + RANDOM % 6)); log "SIGSTOP worker w$i for ${d}s (lease ttl 6s)"
       kill -STOP "${PID[w$i]}" 2>/dev/null; sleep "$d"; kill -CONT "${PID[w$i]}" 2>/dev/null; log "SIGCONT w$i" ;;
    2) i=$((1 + RANDOM % 3)); log "SIGKILL edge-$i"; kill -KILL "${PID[edge$i]}" 2>/dev/null
       wait "${PID[edge$i]}" 2>/dev/null; sleep 0.$((RANDOM % 9 + 1)); edge $i; log "restarted edge-$i" ;;
    3) r=r$((1 + RANDOM % 2)); d=$((5 + RANDOM % 16)); log "SIGKILL replica $r (pid $(kpid $r)) for ${d}s"
       kill -KILL "$(kpid $r)" 2>/dev/null; sleep "$d"; "$SP/start-replicas.sh"; log "restarted replica $r" ;;
    4) k=k$((1 + RANDOM % 3)); d=$((3 + RANDOM % 6)); log "SIGKILL keeper $k for ${d}s"
       kill -KILL "$(kpid $k)" 2>/dev/null; sleep "$d"; "$SP/start-replicas.sh"; log "restarted keeper $k" ;;
    5) a=$((1 + RANDOM % 3)); b=$((a % 3 + 1)); d=$((8 + RANDOM % 8)); log "SIGKILL keepers k$a and k$b (quorum lost) for ${d}s"
       kill -KILL "$(kpid k$a)" "$(kpid k$b)" 2>/dev/null; sleep "$d"; "$SP/start-replicas.sh"; log "restarted keepers k$a k$b" ;;
  esac
done
log "chaos over: worker kills ${N[0]:-0}, worker pauses ${N[1]:-0}, edge kills ${N[2]:-0}, replica kills ${N[3]:-0}, keeper node kills ${N[4]:-0}, keeper quorum losses ${N[5]:-0}"
"$SP/start-replicas.sh"

# ---- drain -----------------------------------------------------------------------------------
for i in 1 2 3; do kill -TERM "${PID[send$i]}" 2>/dev/null; done
for i in 1 2 3; do wait "${PID[send$i]}" 2>/dev/null; done
log "senders done: $(cat "$OUT"/acked-edge-*.jsonl | wc -l) requests acked"
for i in 1 2 3; do kill -CONT "${PID[w$i]}" 2>/dev/null; done
tables="otel_traces otel_logs otel_metrics_number_points otel_metrics_histogram_points otel_metrics_exponential_histogram_points otel_metrics_summary_points"
last="" stable=0
for _ in $(seq 1 150); do
  sleep 2
  cur=$(for t in $tables; do q1 "SELECT uniqExact(content_key), count() FROM $DB.$t FORMAT TSV" 2>/dev/null; done | tr '\n\t' '  ')
  if [ "$cur" = "$last" ]; then stable=$((stable + 1)); else stable=0; fi
  last=$cur
  [ $stable -ge 10 ] && break
done
log "central (r1) stable: $cur"
kill -TERM "${PID[audit]}" "${PID[gc]}" 2>/dev/null
"$CONSUME" audit --s3 "$ROOT" --out "$OUT/committed.jsonl" --run-for 0s >> "$OUT/audit.log" 2>&1
for i in 1 2 3; do kill -TERM "${PID[w$i]}" 2>/dev/null; done
sleep 1
for i in 1 2 3; do kill -TERM "${PID[edge$i]}" "${PID[proxy$i]}" 2>/dev/null; done
sleep 2
for i in 1 2 3; do kill -KILL "${PID[w$i]}" "${PID[edge$i]}" "${PID[proxy$i]}" 2>/dev/null; done
wait 2>/dev/null
date +%s > "$OUT/t_end"
for t in $tables otel_metrics_series; do
  for r in $R1 $R2; do curl -sS "$r/?receive_timeout=300" --data-binary "SYSTEM SYNC REPLICA $DB.$t" || log "sync $t on $r failed"; done
done
for r in $R1 $R2; do curl -sS "$r/" --data-binary "SYSTEM FLUSH LOGS" >/dev/null; done
snap end
for r in 1 2; do
  echo "=== replica r$r" | tee -a "$OUT/summary.txt"
  CH=$([ $r = 1 ] && echo $R1 || echo $R2) python3 "$OTAP/scripts/consumer_soak_check.py" "$OUT" "$DB" | tee -a "$OUT/summary.txt"
done
