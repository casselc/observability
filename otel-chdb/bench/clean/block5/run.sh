#!/bin/bash
# Block 5: the consumer end to end, on the shared ClickHouse (:18123, pinned
# to CPUs 2-3), SeaweedFS and the edge + load generator on CPU 1, the
# consumer alone on CPU 0.
#  a. steady state (latency.sh = otap-rs consumer_latency.sh): one edge, one
#     worker, 3.7 requests/s per signal of 200 rows / 20 points per type, 90 s
#     per poll (200 ms, 1 s); consumer CPU per object and row from its own
#     stats, server CPU per object from system.events (server-wide, so merges
#     are in it, as before). 5 repetitions.
#  b. throughput: one worker draining a backlog (block 2's roots: small
#     objects, and 10k-row / 100k-point objects), --poll 200ms and 1s,
#     --exit-after-idle 2s, fresh database and checkpoint each; rows/s is
#     rows over the time to the last statement. 5 repetitions.
# Restart-safe: a (rep, poll) already in latency.jsonl / drain.jsonl is skipped.
set -u
here=$(cd "$(dirname "$0")" && pwd); clean=$(dirname "$here")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
REL=$S/otap-rs-target/release
export ENVLOG=$here/env.jsonl; . "$clean/lib/env.sh"; . "$clean/lib/mkwrap.sh"
W=$S/clean/wrap5
mkwrap $W otap-s3pq $REL/otap-s3pq 1 exec
mkwrap $W consume $REL/consume 0 exec
mkwrap $W soaksend $S/clean/bin/soaksend 1
CH=http://127.0.0.1:18123
ch() { curl -sS "$CH/" --data-binary "$1"; }
header "block5 before"
for rep in $(seq 1 "${NREP:-5}"); do
  for poll in 200ms 1s; do
    grep -q "\"poll\": \"$poll\", \"rep\": $rep," $here/latency.jsonl 2>/dev/null && continue  # restart-safe
    gate; snap "begin b5a-$poll-r$rep"
    REP=$rep B=$W OUT=$here/latency.jsonl POLLS=$poll SECS=90 bash $here/latency.sh > /dev/null 2>> $here/run.log
    snap "finish b5a-$poll-r$rep"
  done
done
mkdir -p $here/raw
for rep in $(seq 1 "${NREP:-5}"); do
  for root in large small; do
    for poll in 200ms 1s; do
      db=clean_b5_${root}_$rep
      grep -q "\"root\":\"$root\",\"poll\":\"$poll\",\"rep\":$rep," $here/drain.jsonl 2>/dev/null && continue  # restart-safe
      ch "DROP DATABASE IF EXISTS $db SYNC"
      gate; snap "begin b5b-$root-$poll-r$rep"
      c0=$(ch "SELECT sum(value) FROM system.events WHERE event IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds')")
      t0=$(date +%s.%N)
      taskset -c 0 $REL/consume --s3 http://127.0.0.1:18333/otel/clean/b2/$root/edges --ctl clean/b5/ctl/$root-$poll-r$rep-$RANDOM \
        --ch $CH --db $db --worker w --poll $poll --exit-after-idle 2s --ttl 60s --margin 2s --budget 30s \
        --stats $here/raw/stats-$root-$poll-r$rep.json --stats-every 1s > $here/raw/drain-$root-$poll-r$rep.out 2>&1
      t1=$(date +%s.%N)
      c1=$(ch "SELECT sum(value) FROM system.events WHERE event IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds')")
      snap "finish b5b-$root-$poll-r$rep"
      echo "{\"root\":\"$root\",\"poll\":\"$poll\",\"rep\":$rep,\"t0\":$t0,\"t1\":$t1,\"server_cpu_us\":$((c1 - c0))}" >> $here/drain.jsonl
      ch "DROP DATABASE IF EXISTS $db SYNC"
    done
  done
done
header "block5 after"
