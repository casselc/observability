#!/bin/bash
# Step 1, central: INSERT … SELECT CPU per row, presorted objects against
# unsorted, in the consumer's own statements (bench/clean block 2's method).
# The real consumer (consume --once, one worker) ingests one configuration's
# object set (edge.sh "set": 32 traces + 32 logs objects of 10k rows) into a
# fresh database, at --max-batch 1 and 32. Each INSERT's CPU
# (OSCPUVirtualTimeMicroseconds), rows and the MergeTree writer's sort time
# and "block already sorted" count come from query_log. The shared
# ClickHouse on CPUs 1-3, consume and SeaweedFS on CPU 0. NREP repetitions,
# configurations rotated per repetition, each run gated on load <= 0.45.
# Restart-safe: a run with its tables line in raw/tables.jsonl is skipped.
set -u
here=$(cd "$(dirname "$0")" && pwd); spike=$(dirname "$(dirname "$here")")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
export ENVLOG=$here/central-env.jsonl; . "$spike/bench/clean/lib/env.sh"
CH=http://127.0.0.1:18123
ch() { curl -sS "$CH/" --data-binary "$1"; }
CONSUME=$S/sorting/bin/sort/consume
mkdir -p $here/raw
touch $here/raw/tables.jsonl
chpid=$(awk '/^PID/{print $2}' $S/chsrv/data/status)
taskset -a -cp 1-3 $chpid > /dev/null
for p in $(pgrep -x weed); do taskset -a -cp 0 "$p" > /dev/null; done
CFGS=(a-unsorted b-sorted-1rg d-hash-16rg f-range-16rg)
header "central before"
for rep in $(seq 1 "${NREP:-5}"); do
  for i in 0 1 2 3; do
    cfg=${CFGS[$(( (i + rep - 1) % 4 ))]}
    for mb in 1 32; do
      db=sort_c_${cfg//-/_}_m${mb}_r$rep
      grep -q "\"db\":\"$db\"" $here/raw/tables.jsonl && continue
      ch "DROP DATABASE IF EXISTS $db SYNC"
      gate || continue
      snap "begin central-$cfg-m$mb-r$rep"
      t0=$(date +%s)
      taskset -c 0 $CONSUME --s3 http://127.0.0.1:18333/otel/sorting/set/$cfg/edges \
        --ctl sorting/ctl/$cfg-m$mb-r$rep-$RANDOM --ch $CH --db $db --once --max-batch $mb \
        --ttl 60s --margin 2s --budget 30s > $here/raw/consume-$cfg-m$mb-r$rep.json 2> $here/raw/consume-$cfg-m$mb-r$rep.err
      rc=$?
      snap "finish central-$cfg-m$mb-r$rep rc=$rc"
      ch "SYSTEM FLUSH LOGS"
      ch "SELECT '$cfg' AS config, $mb AS max_batch, $rep AS rep, $t0 AS attempt, query_id, tables, event_time_microseconds, query_duration_ms,
            written_rows, read_rows, read_bytes,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us, ProfileEvents['UserTimeMicroseconds'] AS user_us,
            ProfileEvents['SystemTimeMicroseconds'] AS sys_us, ProfileEvents['S3HeadObject'] AS heads, ProfileEvents['S3GetObject'] AS gets,
            ProfileEvents['InsertedRows'] AS inserted_rows,
            ProfileEvents['MergeTreeDataWriterBlocks'] AS blocks, ProfileEvents['MergeTreeDataWriterBlocksAlreadySorted'] AS blocks_sorted,
            ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds'] AS sort_us,
            ProfileEvents['MergeTreeDataWriterCompressedBytes'] AS part_bytes,
            exception_code
          FROM system.query_log WHERE type != 'QueryStart' AND query_kind = 'Insert' AND has(databases, '$db')
            AND event_time >= toDateTime($t0 - 5) ORDER BY event_time_microseconds FORMAT JSONEachRow" | gzip >> $here/raw/statements.jsonl.gz
      ch "SELECT '$db' AS db, '$cfg' AS config, $mb AS max_batch, $rep AS rep, $rc AS rc, table, sum(rows) AS rows, count() AS parts,
            sum(data_compressed_bytes) AS bytes FROM system.parts WHERE database = '$db' AND active GROUP BY table FORMAT JSONEachRow" >> $here/raw/tables.jsonl
      ch "DROP DATABASE IF EXISTS $db SYNC"
    done
  done
done
header "central after"
taskset -a -cp 0-3 $chpid > /dev/null
for p in $(pgrep -x weed); do taskset -a -cp 0-3 "$p" > /dev/null; done
