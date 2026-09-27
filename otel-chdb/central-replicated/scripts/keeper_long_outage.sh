#!/bin/bash
# keeper_long_outage.sh: does anything bound how late a replicated INSERT
# commits when Keeper loses quorum for a long time? (keeper_overrun.sh found
# commits up to 11.6 s past max_execution_time, landing the moment quorum
# came back; the Keeper operation timeout, 10 s, did not end them.)
#
# For each outage length D in DURS: find the Keeper node r1's session is on,
# lose quorum around it (the two OTHER nodes SIGSTOPped, or with MODE=partition
# DROPped by iptables, client and raft ports), send INSERTS inserts on r1
# 1 s later (max_execution_time BUDGET, a client that waits 200 s), end the
# outage after D s, and report per insert: its answer, when it ended, and
# when its part was committed (part_log NewPart) or first appeared on either
# replica (part_log, by the part holding its marker rows).
#
# TARGET=self stops (or partitions) only the node r1's session is on: the
# quorum stays, the client times out and reconnects elsewhere, and the
# request it sent sits in the stopped node's socket until it returns.
# ON_LEADER=1 first moves r1's session onto the Keeper leader (SYSTEM
# RECONNECT ZOOKEEPER until it lands there), so the insert's commit is
# proposed by the leader and waits there for the followers to return.
#
#   OUT=results/keeper-long DURS="20 40 70" [BUDGET=10 READ_S=8 OFFSET=7] [ON_LEADER=1] scripts/keeper_long_outage.sh
set -u
. "$(dirname "$0")/common.sh"
OUT=${OUT:-$HERE/results/keeper-long}
DURS=${DURS:-20 40 70}
BUDGET=${BUDGET:-2}
INSERTS=${INSERTS:-3}
MODE=${MODE:-stop}
READ_S=${READ_S:-0}      # each insert reads for about this long before it commits (sleepEachRow, 1 s blocks)
OFFSET=${OFFSET:--1}     # the outage starts this long after the inserts (-1: 1 s before them)
DB=${DB:-repl_long_$(date +%s)}
T=$DB.t
mkdir -p "$OUT"
say() { echo "$(date +%T.%3N) $*" | tee -a "$OUT/log.txt"; }
IPT=()
heal() { local x; for x in "${IPT[@]}"; do iptables -D INPUT $x 2>/dev/null; done; IPT=(); }
trap heal EXIT
kpid() { cat "$SP/crep/k$1/pid"; }
for r in $R1 $R2; do
  curl -sS "$r/" --data-binary "CREATE DATABASE IF NOT EXISTS $DB" &&
  curl -sS "$r/" --data-binary "CREATE TABLE $T (d Date, id UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$DB/t', '{replica}') PARTITION BY d ORDER BY id" || exit 1
  curl -sS "$r/" --data-binary "SYSTEM STOP MERGES $T"
done
for D in $DURS; do
  if [ "${ON_LEADER:-0}" = 1 ]; then
    # Put r1's session on the Keeper leader (so the request is proposed, and waits for a quorum there).
    L=$(for i in 1 2 3; do (exec 3<>/dev/tcp/127.0.0.1/2918$i && echo mntr >&3 && timeout 1 cat <&3) 2>/dev/null | grep -q 'zk_server_state.leader' && echo $i; done)
    for _ in $(seq 1 20); do
      [ "$(q1 "SELECT port FROM system.zookeeper_connection WHERE name = 'default'")" = "2918$L" ] && break
      q1 "SYSTEM RECONNECT ZOOKEEPER"; sleep 1
    done
  fi
  port=$(q1 "SELECT port FROM system.zookeeper_connection WHERE name = 'default'")
  k=${port: -1}
  others=$(for i in 1 2 3; do [ "$i" != "$k" ] && echo $i; done | tr '\n' ' ')
  # TARGET=self: the node r1's session is on, alone (Keeper keeps its quorum; the
  # session's requests sit unanswered in that node's socket until it returns).
  [ "${TARGET:-others}" = self ] && others=$k
  say "outage $D s ($MODE): r1's session is on k$k (leader k${L:-?}); stopping $others"
  stop_nodes() {
    if [ "$MODE" = stop ]; then for i in $others; do kill -STOP "$(kpid $i)"; done
    else for i in $others; do for p in 2918$i 2923$i; do for dir in --dport --sport; do IPT+=("-i lo -p tcp $dir $p -j DROP"); iptables -I INPUT -i lo -p tcp $dir $p -j DROP; done; done; done; fi
  }
  if [ "$OFFSET" = -1 ]; then stop_nodes; t_out=$(date +%s%3N); sleep 1; fi
  for n in $(seq 1 "$INSERTS"); do
    qid="long-$D-$n-$RANDOM"
    if [ "$READ_S" -gt 0 ]; then sel="SELECT today(), number, '$qid' FROM numbers($((READ_S * 10))) WHERE sleepEachRow(0.1) = 0 SETTINGS max_block_size = 10"
    else sel="SELECT today(), number, '$qid' FROM numbers(100)"; fi
    ( t0=$(date +%s%3N)
      res=$(curl -sS -m 200 "$R1/?max_execution_time=$BUDGET&timeout_overflow_mode=throw&insert_deduplication_token=$qid&query_id=$qid" \
        --data-binary "INSERT INTO $T $sel" 2>&1 | head -c 200 | tr '\n\t' '  ')
      echo -e "$qid\t$D\t$t0\t$(date +%s%3N)\t${res:-ok}" >> "$OUT/client.tsv" ) &
    sleep 0.3
  done
  if [ "$OFFSET" != -1 ]; then sleep "$OFFSET"; stop_nodes; t_out=$(date +%s%3N); sleep 1; fi
  sleep $((D - 1))
  if [ "$MODE" = stop ]; then for i in $others; do kill -CONT "$(kpid $i)"; done; else heal; fi
  t_back=$(date +%s%3N)
  echo -e "$D\t$t_out\t$t_back" >> "$OUT/outages.tsv"
  say "quorum back after $(( (t_back - t_out) / 1000 )) s"
  wait
  sleep 20
done
for r in $R1 $R2; do curl -sS "$r/?receive_timeout=120" --data-binary "SYSTEM SYNC REPLICA $T LIGHTWEIGHT"; curl -sS "$r/" --data-binary "SYSTEM FLUSH LOGS"; done
python3 - "$OUT" "$DB" "$BUDGET" <<'PY' | tee "$OUT/summary.txt"
import sys, urllib.request
out, db, budget = sys.argv[1], sys.argv[2], float(sys.argv[3]) * 1000
R = {"r1": "http://127.0.0.1:28123", "r2": "http://127.0.0.1:38123"}
q = lambda u, s: [l.split("\t") for l in urllib.request.urlopen(urllib.request.Request(u + "/", data=s.encode()), timeout=60).read().decode().splitlines() if l]
parts, first = {}, {}
for name, u in R.items():
    for s, part, n in q(u, f"SELECT s, _part, count() FROM {db}.t GROUP BY s, _part FORMAT TSV"):
        parts.setdefault(s, {})[name] = (part, int(n))
    for part, ev, t in q(u, f"SELECT part_name, toString(event_type), toUnixTimestamp64Milli(event_time_microseconds) FROM system.part_log WHERE database = '{db}' FORMAT TSV"):
        first[(name, part)] = min(first.get((name, part), 1 << 62), int(t))
outages = {l.split("\t")[0]: l.rstrip().split("\t") for l in open(f"{out}/outages.tsv")}
print("D s | query | answer after | rows r1/r2 | first on a replica, s after start (past max_execution_time) | quorum back, s after start")
for l in open(f"{out}/client.tsv"):
    qid, d, t0, t1, res = l.rstrip("\n").split("\t")
    p = parts.get(qid, {})
    ts = [first.get((r, p[r][0])) for r in p if first.get((r, p[r][0]))]
    t_first = min(ts) if ts else None
    back = int(outages[d][2]) - int(t0)
    fa = f"{(t_first - int(t0)) / 1000:.1f} ({(t_first - int(t0) - budget) / 1000:+.1f})" if t_first else "never"
    print(f"{d} | {qid} | {(int(t1) - int(t0)) / 1000:.1f} s: {res[:70]} | {p.get('r1', ('', 0))[1]}/{p.get('r2', ('', 0))[1]} | {fa} | {back / 1000:.1f}")
PY
[ "${KEEP:-0}" = 1 ] || for p in 29000 39000; do $CHBIN client --port $p --query "DROP DATABASE $DB SYNC"; done
