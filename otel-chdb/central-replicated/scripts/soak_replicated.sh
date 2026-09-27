#!/bin/bash
# soak_replicated.sh: otap-rs/scripts/consumer_soak.sh against a replicated,
# S3-tiered central (two replicas, three Keeper nodes), with the consumer's
# fleet-scale features on.
#
#   senders -> 3 edges -> 3 fault proxies -> SeaweedFS
#     <- 3 consumer workers, split across the replicas on purpose
#        (w1, w3: --ch r1,r2; w2: --ch r2,r1), so lane takeovers cross replicas;
#        --sync-replica, --no-ddl, the check's partition range (3-day horizon),
#        load balancing, idle backoff, linger, --metrics-addr
#     -> ReplicatedMergeTree tables (scripts/ddl.py: hot local volume, TTL move
#        to S3 after MOVE; POLICY tiered_own = a copy per replica, the chosen
#        design (DECISIONS D13); tiered_zc + ZC=1 = the rejected zero-copy)
#   + `consume gc --ch r1,r2 --db --sync-replica` with the horizon audit every
#     AUDIT_EVERY, and `consume audit` (the ledger of committed objects);
#     every process's /metrics scraped every SCRAPE s.
#
# TIMING=prod (default): the worker's defaults, ttl 75 s, margin 20 s, budget
# 10 s, keeper slack 20 s (GC --delay 115 s); pauses 80-92 s (past the lease).
# TIMING=short: ttl 6 s, margin 1 s, budget 2 s, --allow-short-margin (the
# first replicated soak's timing; GC --delay 10 s); pauses 7-12 s.
#
# Chaos, one event every CHAOS_MIN..CHAOS_MAX s, drawn from KINDS:
#   0 worker SIGKILL (restarted 1-3 s later)   1 worker SIGSTOP past its lease
#   2 edge SIGKILL                             3 replica SIGKILL, restarted after 5-20 s
#   4 Keeper node SIGKILL for 3-8 s            5 Keeper quorum loss: two nodes SIGKILLed for 8-15 s
#   6 Keeper leader SIGSTOP for 5-15 s (its sessions' requests hang: the overrun case)
#   7 network partition of a replica for 10-40 s: its HTTP and interserver ports
#     DROPped both ways (iptables), so in-flight statements lose their answer
#     and the other replica can't fetch from it; it keeps its Keeper session
#   8 network partition of a Keeper node (client and raft ports) for 5-15 s
# At the end: drain, SYNC REPLICA on both replicas, consumer_soak_check.py
# against EACH replica, a final horizon audit on each replica, the metrics,
# and the Keeper overrun of the workers' inserts (scripts/overrun.py).
#
#   OUT=results/soak-fleet DURATION=1200 scripts/soak_replicated.sh
set -u
. "$(dirname "$0")/common.sh"
OTAP=/home/user/observability/otel-chdb/otap-rs
B=${B:-$SP/consumer/bin}                      # otap-s3pq, soaksend, faultproxy2
CONSUME=${CONSUME:-$SP/otap-rs-target/release/consume}
OUT=${OUT:-$HERE/results/soak-fleet}
DURATION=${DURATION:-900}
RUN=${RUN:-rsoak$(date +%s)}
S3=http://127.0.0.1:18333
BUCKET=${BUCKET:-repl-soak}
DB=${DB:-repl_$RUN}
RATE=${RATE:-2}
CHAOS_MIN=${CHAOS_MIN:-8}
CHAOS_MAX=${CHAOS_MAX:-20}
MOVE=${MOVE:-5 MINUTE}
POLICY=${POLICY:-tiered_own}
ZC=${ZC:-0}
SYNC=${SYNC:-1}                               # 0: the negative control, no --sync-replica
TIMING=${TIMING:-prod}
KINDS=${KINDS:-0 1 2 3 3 4 5 6 6 7 7 8}
AUDIT_EVERY=${AUDIT_EVERY:-60s}
SCRAPE=${SCRAPE:-15}
MPORT=${MPORT:-19480}                         # gc on MPORT, worker i on MPORT + i
MIN_FREE_MB=${MIN_FREE_MB:-2800}
case $TIMING in
  prod) TFLAGS=(); BUDGET_S=10; PAUSE_MIN=80; PAUSE_SPAN=13; GCFLAGS=(--delay 115s --zombie 240s) ;;
  short) TFLAGS=(--ttl 6s --margin 1s --budget 2s --allow-short-margin); BUDGET_S=2; PAUSE_MIN=7; PAUSE_SPAN=6; GCFLAGS=(--delay 10s --zombie 60s) ;;
  *) echo "TIMING prod|short"; exit 1 ;;
esac
mkdir -p "$OUT"
echo "$RUN" > "$OUT/run.txt"
PREFIX=$BUCKET/$RUN/edges
ROOT=$S3/$PREFIX
log() { echo "$(date +%T.%3N) $*" | tee -a "$OUT/chaos.txt"; }
curl -s -o /dev/null --aws-sigv4 "aws:amz:us-east-1:s3" --user otel:otelsecret -X PUT "$S3/$BUCKET" || true
IPT=()
heal() { local x; for x in "${IPT[@]}"; do iptables -D INPUT $x 2>/dev/null; done; IPT=(); }
partition() { # port...
  for p in "$@"; do for dir in --dport --sport; do IPT+=("-i lo -p tcp $dir $p -j DROP"); iptables -I INPUT -i lo -p tcp $dir $p -j DROP; done; done
}
trap heal EXIT

# ---- central: the replicated tables on both replicas -------------------------------------
python3 "$HERE/scripts/ddl.py" --db "$DB" --policy "$POLICY" --zero-copy "$ZC" --move "$MOVE" --delete '1 DAY' \
  --extra 'old_parts_lifetime = 30' > "$OUT/ddl.sql"
apply_sql 29000 "$OUT/ddl.sql" && apply_sql 39000 "$OUT/ddl.sql" || { log "DDL failed"; exit 1; }
for r in $R1 $R2; do curl -sS "$r/" --data-binary "SYSTEM FLUSH LOGS" >/dev/null; done
snap() { # tag: server-wide counters of both replicas, and each Keeper node's mntr
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
  "$B/faultproxy2" -listen 127.0.0.1:$((18340 + $1)) -target "$S3" -match "/$RUN/" ${MODES[$(($1 - 1))]} \
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
  "$CONSUME" --s3 "$ROOT" --db "$DB" --ch "${CHS[$1]}" "${extra[@]}" --worker "w$1" --poll 200ms "${TFLAGS[@]}" \
    --linger 300ms --idle-backoff 500ms..5s --idle-after 2s --min-hold 5s --loads-every 2s \
    --discover 1s --quiet 3s --full-list 10s --stats "$OUT/w$1-${INC[$1]}.stats.json" --stats-every 1s \
    --metrics-addr 127.0.0.1:$((MPORT + $1)) >> "$OUT/w$1.log" 2>&1 &
  PID[w$1]=$!
}
scrape() { # every SCRAPE s: each process's consumer_* series, with the time and the process
  while :; do
    local t=$(date +%s)
    for p in 0 1 2 3; do
      curl -s -m 2 "127.0.0.1:$((MPORT + p))/metrics" | grep '^consumer_' | sed "s/^/$t\t$([ $p = 0 ] && echo gc || echo w$p)\t/"
    done >> "$OUT/metrics-scrape.tsv"
    sleep "$SCRAPE"
  done
}

log "run $RUN: db $DB, prefix $PREFIX, duration ${DURATION}s, timing $TIMING, policy $POLICY, sync $SYNC, workers w1,w3 -> r1 first, w2 -> r2 first"
for i in 1 2 3; do proxy $i; done
sleep 0.5
for i in 1 2 3; do edge $i; done
sleep 2
for i in 1 2 3; do sender $i; done
for i in 1 2 3; do worker $i; done
gsync=(); [ "$SYNC" = 1 ] && gsync=(--sync-replica)
"$CONSUME" gc --s3 "$ROOT" --every 5s "${GCFLAGS[@]}" --run-for 100000s --ch "$R1,$R2" --db "$DB" "${gsync[@]}" \
  --audit-every "$AUDIT_EVERY" --metrics-addr 127.0.0.1:$MPORT > "$OUT/gc.log" 2> "$OUT/gc.err" &
PID[gc]=$!
"$CONSUME" audit --s3 "$ROOT" --out "$OUT/committed.jsonl" --every 1s --run-for 100000s >> "$OUT/audit.log" 2>&1 &
PID[audit]=$!
scrape &
PID[scrape]=$!

# ---- chaos ---------------------------------------------------------------------------------
kpid() { cat "$SP/crep/$1/pid"; }
mntr() { (exec 3<>/dev/tcp/127.0.0.1/2918$1 && echo mntr >&3 && timeout 1 cat <&3) 2>/dev/null; }
leader() { for k in 1 2 3; do mntr $k | grep -q 'zk_server_state.leader' && { echo $k; return; }; done; echo 1; }
end=$(( $(date +%s) + DURATION ))
declare -A N
KA=($KINDS)
while [ "$(date +%s)" -lt "$end" ]; do
  sleep $((CHAOS_MIN + RANDOM % (CHAOS_MAX - CHAOS_MIN + 1)))
  avail=$(df --output=avail -BM / | tail -1 | tr -dc 0-9)
  if [ "$avail" -lt "$MIN_FREE_MB" ]; then log "disk: ${avail} MB free, stopping early"; break; fi
  kind=${KA[$((RANDOM % ${#KA[@]}))]}
  N[$kind]=$((${N[$kind]:-0} + 1))
  case $kind in
    0) i=$((1 + RANDOM % 3)); log "SIGKILL worker w$i"; kill -KILL "${PID[w$i]}" 2>/dev/null
       sleep $((1 + RANDOM % 3)); worker $i; log "restarted w$i (incarnation ${INC[$i]})" ;;
    1) i=$((1 + RANDOM % 3)); d=$((PAUSE_MIN + RANDOM % PAUSE_SPAN)); log "SIGSTOP worker w$i for ${d}s (past its lease)"
       kill -STOP "${PID[w$i]}" 2>/dev/null; sleep "$d"; kill -CONT "${PID[w$i]}" 2>/dev/null; log "SIGCONT w$i" ;;
    2) i=$((1 + RANDOM % 3)); log "SIGKILL edge-$i"; kill -KILL "${PID[edge$i]}" 2>/dev/null
       wait "${PID[edge$i]}" 2>/dev/null; sleep 0.$((RANDOM % 9 + 1)); edge $i; log "restarted edge-$i" ;;
    3) r=r$((1 + RANDOM % 2)); d=$((5 + RANDOM % 16)); log "SIGKILL replica $r (pid $(kpid $r)) for ${d}s"
       kill -KILL "$(kpid $r)" 2>/dev/null; sleep "$d"; "$SP/start-replicas.sh"; log "restarted replica $r" ;;
    4) k=k$((1 + RANDOM % 3)); d=$((3 + RANDOM % 6)); log "SIGKILL keeper $k for ${d}s"
       kill -KILL "$(kpid $k)" 2>/dev/null; sleep "$d"; "$SP/start-replicas.sh"; log "restarted keeper $k" ;;
    5) a=$((1 + RANDOM % 3)); b=$((a % 3 + 1)); d=$((8 + RANDOM % 8)); log "SIGKILL keepers k$a and k$b (quorum lost) for ${d}s"
       kill -KILL "$(kpid k$a)" "$(kpid k$b)" 2>/dev/null; sleep "$d"; "$SP/start-replicas.sh"; log "restarted keepers k$a k$b" ;;
    6) L=$(leader); d=$((5 + RANDOM % 11)); log "SIGSTOP keeper leader k$L for ${d}s"
       kill -STOP "$(kpid k$L)"; sleep "$d"; kill -CONT "$(kpid k$L)"; log "SIGCONT keeper k$L" ;;
    7) r=$((1 + RANDOM % 2)); d=$((10 + RANDOM % 31)); log "partition replica r$r (HTTP and interserver ports) for ${d}s"
       if [ $r = 1 ]; then partition 28123 29009; else partition 38123 39009; fi
       sleep "$d"; heal; log "healed replica r$r" ;;
    8) k=$((1 + RANDOM % 3)); d=$((5 + RANDOM % 11)); log "partition keeper k$k (leader k$(leader)) for ${d}s"
       partition 2918$k 2923$k; sleep "$d"; heal; log "healed keeper k$k" ;;
  esac
done
log "chaos over: worker kills ${N[0]:-0}, worker pauses ${N[1]:-0}, edge kills ${N[2]:-0}, replica kills ${N[3]:-0}, keeper node kills ${N[4]:-0}, keeper quorum losses ${N[5]:-0}, keeper leader stops ${N[6]:-0}, replica partitions ${N[7]:-0}, keeper partitions ${N[8]:-0}"
heal
"$SP/start-replicas.sh"

# ---- drain -----------------------------------------------------------------------------------
for i in 1 2 3; do kill -TERM "${PID[send$i]}" 2>/dev/null; done
for i in 1 2 3; do wait "${PID[send$i]}" 2>/dev/null; done
log "senders done: $(cat "$OUT"/acked-edge-*.jsonl | wc -l) requests acked"
for i in 1 2 3; do kill -CONT "${PID[w$i]}" 2>/dev/null; done
tables="otel_traces otel_logs otel_metrics_number_points otel_metrics_histogram_points otel_metrics_exponential_histogram_points otel_metrics_summary_points"
last="" stable=0
for _ in $(seq 1 300); do
  sleep 3
  cur=$(for t in $tables; do q1 "SELECT uniqExact(content_key), count() FROM $DB.$t FORMAT TSV" 2>/dev/null; done | tr '\n\t' '  ')
  if [ "$cur" = "$last" ]; then stable=$((stable + 1)); else stable=0; fi
  last=$cur
  [ $stable -ge 40 ] && break      # 120 s unchanged: longer than a lane's takeover at the prod timing (95 s)
done
log "central (r1) stable: $cur"
curl -s "127.0.0.1:$MPORT/metrics" > "$OUT/metrics-gc.txt"
for i in 1 2 3; do curl -s "127.0.0.1:$((MPORT + i))/metrics" > "$OUT/metrics-w$i.txt"; done
kill -TERM "${PID[audit]}" "${PID[gc]}" "${PID[scrape]}" 2>/dev/null
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
# The final horizon audit, on each replica (no saved state), and the audits beside GC.
for r in 1 2; do
  "$CONSUME" horizon-audit --s3 "$ROOT" --ch "$([ $r = 1 ] && echo $R1 || echo $R2)" --db "$DB" --sync-replica --audit-lookback 30d --audit-no-state \
    > "$OUT/horizon-audit-r$r.json" 2> "$OUT/horizon-audit-r$r.err"
done
python3 - "$OUT" <<'PY' | tee -a "$OUT/summary.txt"
import collections, json, sys
out = sys.argv[1]
runs = [json.loads(l)["horizon_audit"] for l in open(f"{out}/gc.log") if l.startswith('{"horizon_audit"')]
fin = [json.loads(open(f"{out}/horizon-audit-r{r}.json").read())["horizon_audit"] for r in (1, 2)]
late = sum(len(r["late"]) for r in runs + fin)
unexp = sum(len(r["unexplained"]) for r in runs + fin)
errs = [e for r in runs for e in r["errors"]]
print(f"horizon audit: {len(runs)} runs beside GC ({len(runs) - sum(1 for r in runs if r['errors'])} clean, "
      f"{sum(1 for r in runs if r['errors'])} with errors), replicas read {dict(collections.Counter(r.get('replica', '?') for r in runs))}")
print(f"  errors beside GC (first 3): {errs[:3]}")
print(f"  final on r1: tables {fin[0]['tables']}, candidates {fin[0]['candidates']}, late {len(fin[0]['late'])}, unexplained {len(fin[0]['unexplained'])}, errors {fin[0]['errors']}")
print(f"  final on r2: tables {fin[1]['tables']}, candidates {fin[1]['candidates']}, late {len(fin[1]['late'])}, unexplained {len(fin[1]['unexplained'])}, errors {fin[1]['errors']}")
ok = late == 0 and unexp == 0 and all(f["tables"] > 0 and not f["errors"] for f in fin) and fin[0]["candidates"] == fin[1]["candidates"]
print(f"  late {late}, unexplained {unexp} -> " + ("PASS" if ok else "FAIL"))
# The metrics as the fleet ended, and over time.
m = collections.defaultdict(float)
for f in ("metrics-w1.txt", "metrics-w2.txt", "metrics-w3.txt", "metrics-gc.txt"):
    for l in open(f"{out}/{f}"):
        if l.startswith("consumer_") and " " in l and not l.startswith("consumer_visible"):
            k, v = l.rsplit(" ", 1)
            try:
                m[k] += float(v)
            except ValueError:
                pass
keys = [k for k in sorted(m) if any(s in k for s in ("late_copies", "unexplained", "audit_runs", "gc_runs", "repairs", "over_count", "unsettled",
        "checks_total", "recounts", "range_guard", "lane_changes", "insert_errors", "statements_total", "copies_skipped", "objects_ingested"))]
print("metrics at the end (workers' last incarnations + gc): " + "; ".join(f"{k} {m[k]:g}" for k in keys))
PY
# The Keeper overrun of the workers' inserts.
echo "=== workers' inserts: commit past max_execution_time ($BUDGET_S s; the margin must cover it)" | tee -a "$OUT/summary.txt"
python3 "$HERE/scripts/overrun.py" "$OUT" "$BUDGET_S" 'otaprs-consumer-%' "$DB" "$(cat "$OUT/t_start")" "$(cat "$OUT/t_end")" | tee -a "$OUT/summary.txt"
gzip -f "$OUT/metrics-scrape.tsv" "$OUT/inserts.tsv" 2>/dev/null
