#!/bin/bash
# Block 2: central insert CPU in the consumer's own statements. The real
# consumer (consume --once, one worker) ingests every committed object of a
# root into a fresh database on the private ClickHouse (query_log on), at
# --max-batch 1 and 32; each INSERT's own CPU (ProfileEvents
# OSCPUVirtualTimeMicroseconds, User+System beside it), rows and objects come
# from query_log. analyze.py fits CPU = per statement + per object + per row.
# ClickHouse on CPUs 1-3; consume and SeaweedFS on CPU 0. 5 repetitions,
# each consumer run gated on load <= 0.45.
set -u
here=$(cd "$(dirname "$0")" && pwd); clean=$(dirname "$here")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
export ENVLOG=$here/env.jsonl; . "$clean/lib/env.sh"
CH=http://127.0.0.1:18623
ch() { curl -sS "$CH/" --data-binary "$1"; }
mkdir -p $here/raw
header "block2 before"
for rep in $(seq 1 "${NREP:-5}"); do
  for root in small large; do
    for mb in 1 32; do
      db=clean_b2_${root}_m${mb}_r$rep
      ch "DROP DATABASE IF EXISTS $db SYNC"
      gate
      snap "begin b2-$root-m$mb-r$rep"
      t0=$(date +%s)
      taskset -c 0 $S/otap-rs-target/release/consume --s3 http://127.0.0.1:18333/otel/clean/b2/$root/edges \
        --ctl clean/b2/ctl/$root-m$mb-r$rep-$RANDOM --ch $CH --db $db --once --max-batch $mb \
        --ttl 60s --margin 2s --budget 30s > $here/raw/consume-$root-m$mb-r$rep.json 2> $here/raw/consume-$root-m$mb-r$rep.err
      snap "finish b2-$root-m$mb-r$rep rc=$?"
      ch "SYSTEM FLUSH LOGS"
      ch "SELECT '$root' AS root, $mb AS max_batch, $rep AS rep, query_id, tables, event_time_microseconds, query_duration_ms,
            written_rows, written_bytes, read_rows, read_bytes,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us, ProfileEvents['UserTimeMicroseconds'] AS user_us,
            ProfileEvents['SystemTimeMicroseconds'] AS sys_us, ProfileEvents['S3HeadObject'] AS heads, ProfileEvents['S3GetObject'] AS gets,
            ProfileEvents['ReadBufferFromS3Bytes'] AS s3_bytes, ProfileEvents['InsertedRows'] AS inserted_rows,
            length(extractAll(query, '\\.parquet')) AS parquet_mentions, memory_usage, exception_code
          FROM system.query_log WHERE type != 'QueryStart' AND query_kind = 'Insert' AND has(databases, '$db')
            AND event_time >= toDateTime($t0 - 5) ORDER BY event_time_microseconds FORMAT JSONEachRow" | gzip >> $here/raw/statements.jsonl.gz
      ch "SELECT '$db' AS db, table, sum(rows) AS rows, count() AS parts FROM system.parts WHERE database = '$db' AND active GROUP BY table FORMAT JSONEachRow" >> $here/raw/tables.jsonl
      ch "DROP DATABASE IF EXISTS $db SYNC"
    done
  done
done
header "block2 after"
