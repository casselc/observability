#!/bin/bash
# The Keeper overrun bound on a production-like Keeper (DECISIONS.md risk 5c,
# AMBIGUITY.md C1/C5): central-replicated/scripts/keeper_overrun.sh and
# keeper_long_outage.sh, with the faults sent over SSH to three real Keeper
# hosts instead of to local processes. Insert loops with the consumer's
# statement settings run against both replicas; afterwards
# central-replicated/scripts/overrun.py reads query_log and part_log on both
# and reports, per statement, how far past max_execution_time its part
# committed. The design assumes it is below the session timeout (29.0 s
# measured locally under a 30 s session; margin and slack are 20 s).
#
#   KEEPERS="k1 k2 k3" R1=http://r1:8123 R2=http://r2:8123 OUT=dir \
#   [SSH_USER=ec2-user KEEPER_UNIT=clickhouse-keeper CLIENT_PORT=9181 RAFT_PORT=9234 \
#    DURATION=1800 BUDGET=10 LOOPS=2 ROWS=2000 GAP_MIN=20 GAP_MAX=60 KINDS="0 1 2 3 4" YES=1] central/keeper_faults.sh
#
# Faults (one at a time, GAP_MIN..GAP_MAX s apart):
#   0  SIGSTOP the leader for 5-25 s              (its sessions' requests hang)
#   1  partition one Keeper (client + raft ports, iptables, both directions) for 5-25 s
#   2  SIGSTOP two Keepers (quorum lost) for 8-40 s
#   3  SIGKILL the leader; start it again 3-10 s later
#   4  quorum lost for 15-40 s starting about BUDGET-3 s after an insert began (keeper_long_outage's
#      "during the commit" case, the one that measured 19.0 s past a 10 s budget)
# Every change is undone on exit (SIGCONT, iptables rules carrying the comment
# otelval-$RUN deleted, the unit started). The hosts need passwordless sudo
# for systemctl, kill and iptables over SSH (BatchMode). Run it against a
# Keeper and replicas that carry nothing else: it breaks them on purpose.
set -u -o pipefail
: "${KEEPERS:?}" "${R1:?}" "${R2:?}" "${OUT:?}"
here=$(cd "$(dirname "$0")" && pwd); spike=$(cd "$here/../../.." && pwd)
RUN=${RUN:-kf$(date +%s)}
SSH_USER=${SSH_USER:-$USER}; KEEPER_UNIT=${KEEPER_UNIT:-clickhouse-keeper}
CLIENT_PORT=${CLIENT_PORT:-9181}; RAFT_PORT=${RAFT_PORT:-9234}
DURATION=${DURATION:-1800}; BUDGET=${BUDGET:-10}; LOOPS=${LOOPS:-2}; ROWS=${ROWS:-2000}
GAP_MIN=${GAP_MIN:-20}; GAP_MAX=${GAP_MAX:-60}; KINDS=${KINDS:-0 1 2 3 4}
DB=${DB:-kf_$RUN}; T=$DB.t
read -r -a K <<< "$KEEPERS"; [ ${#K[@]} -eq 3 ] || { echo "KEEPERS: three hosts" >&2; exit 2; }
mkdir -p "$OUT"
if [ "${YES:-0}" != 1 ]; then read -r -p "inject Keeper faults into ${K[*]} for ${DURATION}s? [y/N] " a; [ "$a" = y ] || exit 1; fi
log() { echo "$(date -u +%T.%3N) $*" | tee -a "$OUT/faults.log"; }
rsh() { ssh -o BatchMode=yes -o ConnectTimeout=5 "$SSH_USER@$1" "$2"; }
mntr() { rsh "$1" "echo mntr | nc -w 2 127.0.0.1 $CLIENT_PORT" 2> /dev/null; }
leader() { local h; for h in "${K[@]}"; do mntr "$h" | grep -q 'zk_server_state.leader' && { echo "$h"; return; }; done; echo "${K[0]}"; }
mainpid() { rsh "$1" "systemctl show -p MainPID --value $KEEPER_UNIT"; }
sig() { rsh "$1" "sudo kill -$2 $(mainpid "$1")"; }
PART=()
partition() { # host: drop client and raft traffic on that host, both directions, tagged
  local p d
  for p in "$CLIENT_PORT" "$RAFT_PORT"; do for d in --dport --sport; do
    rsh "$1" "sudo iptables -I INPUT -p tcp $d $p -m comment --comment otelval-$RUN -j DROP && sudo iptables -I OUTPUT -p tcp $d $p -m comment --comment otelval-$RUN -j DROP"
  done; done
  PART+=("$1")
}
heal() { # remove every rule this run tagged, on every host
  local h
  for h in "${K[@]}"; do
    rsh "$h" "for c in INPUT OUTPUT; do while n=\$(sudo iptables -L \$c --line-numbers | awk '/otelval-$RUN/ {print \$1; exit}'); [ -n \"\$n\" ]; do sudo iptables -D \$c \$n; done; done" || true
  done
  PART=()
}
restore() {
  local h
  heal
  for h in "${K[@]}"; do sig "$h" CONT 2> /dev/null; rsh "$h" "sudo systemctl start $KEEPER_UNIT" 2> /dev/null; done
}
trap 'restore; for p in $(jobs -p); do kill $p 2>/dev/null; done' EXIT
: > "$OUT/faults.log"

# The session timeout the replicas actually have: the bound under test.
for r in "$R1" "$R2"; do
  log "$r: $(curl -sS "$r/" --data-binary "SELECT version(), any(session_timeout_ms) FROM system.zookeeper_connection FORMAT TSV" 2>&1 | tr '\t' ' ')"
done
ddl="CREATE TABLE IF NOT EXISTS $T (d Date, id UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$DB/t', '{replica}') PARTITION BY d ORDER BY id"
for r in "$R1" "$R2"; do curl -sS "$r/" --data-binary "CREATE DATABASE IF NOT EXISTS $DB" && curl -sS "$r/" --data-binary "$ddl" || exit 1; done

loop() { # replica tag
  local n=0 qid t0 res
  while :; do
    n=$((n + 1)); qid="kf-$2-$n-$RANDOM"; t0=$(date +%s%3N)
    res=$(curl -sS -m 90 "$1/?max_execution_time=$BUDGET&timeout_overflow_mode=throw&insert_deduplication_token=$qid&query_id=$qid" \
      --data-binary "INSERT INTO $T SELECT today(), number, repeat('x', 50) FROM numbers($ROWS)" 2>&1 | head -c 160 | tr '\n\t' '  ')
    printf '%s\t%s\t%s\t%s\n' "$qid" "$t0" "$(date +%s%3N)" "${res:-ok}" >> "$OUT/client-$2.tsv"
    sleep 0.$((RANDOM % 5))
  done
}
# Kind 4 needs inserts that are still running BUDGET-3 s in: one loop reads for READ_S s before writing.
slowloop() {
  local n=0 qid t0 res
  while :; do
    n=$((n + 1)); qid="kf-slow-$n-$RANDOM"; t0=$(date +%s%3N)
    res=$(curl -sS -m 90 "$R1/?max_execution_time=$BUDGET&timeout_overflow_mode=throw&insert_deduplication_token=$qid&query_id=$qid" \
      --data-binary "INSERT INTO $T SELECT today(), number + sleepEachRow(0.001) * 0, repeat('y', 50) FROM numbers(${READ_ROWS:-7000}) SETTINGS max_block_size = 1000" 2>&1 | head -c 160 | tr '\n\t' '  ')
    printf '%s\t%s\t%s\t%s\n' "$qid" "$t0" "$(date +%s%3N)" "${res:-ok}" >> "$OUT/client-slow.tsv"
  done
}
for i in $(seq 1 "$LOOPS"); do loop "$R1" "r1l$i" & loop "$R2" "r2l$i" & done
[[ " $KINDS " == *" 4 "* ]] && slowloop &

end=$(( $(date +%s) + DURATION ))
read -r -a KA <<< "$KINDS"
declare -A N
while [ "$(date +%s)" -lt "$end" ]; do
  sleep $((GAP_MIN + RANDOM % (GAP_MAX - GAP_MIN + 1)))
  kind=${KA[$((RANDOM % ${#KA[@]}))]}; N[$kind]=$((${N[$kind]:-0} + 1)); L=$(leader)
  case $kind in
    0) d=$((5 + RANDOM % 21)); log "SIGSTOP leader $L for ${d}s"; sig "$L" STOP; sleep "$d"; sig "$L" CONT; log "SIGCONT $L" ;;
    1) h=${K[$((RANDOM % 3))]}; d=$((5 + RANDOM % 21)); log "partition $h (leader $L) for ${d}s"; partition "$h"; sleep "$d"; heal; log "healed $h" ;;
    2) a=$((RANDOM % 3)); b=$(((a + 1) % 3)); d=$((8 + RANDOM % 33)); log "SIGSTOP ${K[$a]} ${K[$b]} (quorum lost) for ${d}s"
       sig "${K[$a]}" STOP; sig "${K[$b]}" STOP; sleep "$d"; sig "${K[$a]}" CONT; sig "${K[$b]}" CONT; log "SIGCONT ${K[$a]} ${K[$b]}" ;;
    3) d=$((3 + RANDOM % 8)); log "SIGKILL leader $L for ${d}s"; rsh "$L" "sudo systemctl kill -s SIGKILL $KEEPER_UNIT"; sleep "$d"
       rsh "$L" "sudo systemctl start $KEEPER_UNIT"; log "started $L" ;;
    4) # quorum lost while a slow insert is committing: wait for a slow statement to be BUDGET-3 s old
       sleep $((BUDGET > 3 ? BUDGET - 3 : 1)); d=$((15 + RANDOM % 26))
       others=(); for h in "${K[@]}"; do [ "$h" != "$L" ] && others+=("$h"); done
       log "quorum lost (followers ${others[*]} stopped; leader $L keeps its sessions) for ${d}s, during the commit"
       sig "${others[0]}" STOP; sig "${others[1]}" STOP; sleep "$d"; sig "${others[0]}" CONT; sig "${others[1]}" CONT; log "SIGCONT ${others[*]}" ;;
  esac
done
log "faults over: leader stops ${N[0]:-0}, partitions ${N[1]:-0}, quorum stops ${N[2]:-0}, leader kills ${N[3]:-0}, during-commit ${N[4]:-0}"
sleep 5
for p in $(jobs -p); do kill "$p" 2> /dev/null; done
wait 2> /dev/null
sleep 95   # every insert has its answer or its client gave up
cat "$OUT"/client-*.tsv > "$OUT/client.tsv"; rm -f "$OUT"/client-*.tsv
R1=$R1 R2=$R2 python3 "$spike/central-replicated/scripts/overrun.py" "$OUT" "$BUDGET" 'kf-%' "$DB" | tee "$OUT/summary.txt"
[ "${KEEP:-0}" = 1 ] || for r in "$R1" "$R2"; do curl -sS "$r/" --data-binary "DROP DATABASE $DB SYNC"; done
