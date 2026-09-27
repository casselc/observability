#!/bin/bash
# keeper_overrun.sh: how long after max_execution_time a replicated INSERT
# can still commit when Keeper misbehaves (DECISIONS risk 5c: the lease
# margin must cover it; the consumer's --keeper-slack, 10 s).
#
# LOOPS insert loops per replica, each a stream of INSERT … SELECT FROM
# numbers(ROWS) into one ReplicatedMergeTree table with the consumer's
# statement settings (max_execution_time = BUDGET, timeout_overflow_mode =
# throw, a dedup token, a query_id), and a client that waits up to 90 s for
# the answer. Meanwhile, one fault every GAP_MIN..GAP_MAX s:
#   0  SIGSTOP the Keeper leader for 5-15 s (its sessions' requests hang)
#   1  network partition of one Keeper node (client and raft ports DROPped
#      by iptables, both directions) for 5-15 s
#   2  SIGSTOP two Keeper nodes (quorum lost, requests hang) for 8-20 s
#   3  SIGKILL the leader, restarted 3-8 s later (connections reset)
# Afterwards, per insert (query_log + part_log on both replicas): when it
# started, when its part was committed (part_log NewPart, same query_id),
# how it ended; overrun = commit - start - BUDGET.
#
#   OUT=results/keeper-overrun DURATION=300 scripts/keeper_overrun.sh
set -u
. "$(dirname "$0")/common.sh"
OUT=${OUT:-$HERE/results/keeper-overrun}
DURATION=${DURATION:-300}
BUDGET=${BUDGET:-2}
LOOPS=${LOOPS:-2}
ROWS=${ROWS:-2000}
GAP_MIN=${GAP_MIN:-4}
GAP_MAX=${GAP_MAX:-10}
KINDS=${KINDS:-0 1 2 3}
DB=${DB:-repl_ovr_$(date +%s)}
T=$DB.t
mkdir -p "$OUT"
log() { echo "$(date +%T.%3N) $*" | tee -a "$OUT/faults.log"; }
: > "$OUT/faults.log"
IPT=()
unpartition() { local x; for x in "${IPT[@]}"; do iptables -D INPUT $x 2>/dev/null; done; IPT=(); }
trap 'unpartition; for p in $(jobs -p); do kill $p 2>/dev/null; done' EXIT

ddl="CREATE TABLE $T (d Date, id UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$DB/t', '{replica}') PARTITION BY d ORDER BY id"
for r in $R1 $R2; do curl -sS "$r/" --data-binary "CREATE DATABASE $DB" && curl -sS "$r/" --data-binary "$ddl" || exit 1; done

loop() { # replica-url tag
  local n=0
  while :; do
    n=$((n + 1))
    local qid="ovr-$2-$n-$RANDOM"
    local t0=$(date +%s%3N)
    local res
    res=$(curl -sS -m 90 "$1/?max_execution_time=$BUDGET&timeout_overflow_mode=throw&insert_deduplication_token=$qid&query_id=$qid" \
      --data-binary "INSERT INTO $T SELECT today(), number, repeat('x', 50) FROM numbers($ROWS)" 2>&1 | head -c 160 | tr '\n\t' '  ')
    echo -e "$qid\t$t0\t$(date +%s%3N)\t${res:-ok}" >> "$OUT/client-$2.tsv"
    sleep 0.$((RANDOM % 5))
  done
}
for i in $(seq 1 "$LOOPS"); do loop "$R1" "r1l$i" & loop "$R2" "r2l$i" & done

mntr() { (exec 3<>/dev/tcp/127.0.0.1/2918$1 && echo mntr >&3 && timeout 1 cat <&3) 2>/dev/null; }
leader() { for k in 1 2 3; do mntr $k | grep -q 'zk_server_state.leader' && { echo $k; return; }; done; echo 1; }
kpid() { cat "$SP/crep/k$1/pid"; }
end=$(( $(date +%s) + DURATION ))
KA=($KINDS)
declare -A N
while [ "$(date +%s)" -lt "$end" ]; do
  sleep $((GAP_MIN + RANDOM % (GAP_MAX - GAP_MIN + 1)))
  kind=${KA[$((RANDOM % ${#KA[@]}))]}
  N[$kind]=$((${N[$kind]:-0} + 1))
  L=$(leader)
  case $kind in
    0) d=$((5 + RANDOM % 11)); log "SIGSTOP keeper leader k$L for ${d}s"; kill -STOP "$(kpid $L)"; sleep "$d"; kill -CONT "$(kpid $L)"; log "SIGCONT k$L" ;;
    1) k=$((1 + RANDOM % 3)); d=$((5 + RANDOM % 11)); log "partition keeper k$k (leader k$L) for ${d}s"
       for p in 2918$k 2923$k; do for dir in --dport --sport; do IPT+=("-i lo -p tcp $dir $p -j DROP"); iptables -I INPUT -i lo -p tcp $dir $p -j DROP; done; done
       sleep "$d"; unpartition; log "healed k$k" ;;
    2) a=$((1 + RANDOM % 3)); b=$((a % 3 + 1)); d=$((8 + RANDOM % 13)); log "SIGSTOP keepers k$a k$b (quorum lost) for ${d}s"
       kill -STOP "$(kpid $a)" "$(kpid $b)"; sleep "$d"; kill -CONT "$(kpid $a)" "$(kpid $b)"; log "SIGCONT k$a k$b" ;;
    3) d=$((3 + RANDOM % 6)); log "SIGKILL keeper leader k$L for ${d}s"; kill -KILL "$(kpid $L)"; sleep "$d"; "$SP/start-replicas.sh"; log "restarted k$L" ;;
  esac
done
log "faults over: leader stops ${N[0]:-0}, keeper partitions ${N[1]:-0}, quorum stops ${N[2]:-0}, leader kills ${N[3]:-0}"
sleep 5
for p in $(jobs -p); do kill $p 2>/dev/null; done
wait 2>/dev/null
sleep 95   # every insert has had its answer or the client gave up
cat "$OUT"/client-*.tsv > "$OUT/client.tsv"; rm -f "$OUT"/client-*.tsv
python3 "$HERE/scripts/overrun.py" "$OUT" "$BUDGET" 'ovr-%' "$DB" | tee "$OUT/summary.txt"
[ "${KEEP:-0}" = 1 ] || for p in 29000 39000; do $CHBIN client --port $p --query "DROP DATABASE $DB SYNC"; done
