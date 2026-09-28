#!/bin/bash
# Central insert cost on an IDLE dedicated machine, in the consumer's own
# statements (bench/clean block 2, made portable): the edge writes production
# objects (10,000-row trace and log requests, the kind/EKS datasets' mixgen
# shape), then the real consumer ingests them into a fresh database at
# --max-batch 1 and 32, REPS times, each run gated on an idle box; per table,
# every INSERT's own CPU (query_log ProfileEvents) per row, and the block-2
# fit (bench/clean/block2/analyze.py) of CPU = per statement + per object +
# per row.
#
#   OUT=dir CH=http://127.0.0.1:8123 S3_ROOT=http://127.0.0.1:8333/bucket/validation/bench \
#   BIN=dir-with-otap-s3pq,consume,otlpsend,mixgen [S3_KEY=... S3_SECRET=... REPS=5 BATCHES="1 32" DATASETS=4 \
#   LOAD_MAX=0.45 REPLICAS=http://r2:8123] central/bench_insert.sh prep|run|analyze|all
#
# CH must have query_log (and part_log, for merges) on, and be private to this
# run: its load is the measurement. Pin it away from the consumer and the
# store (taskset; bench/clean used CH on CPUs 1-3, consume and the store on
# CPU 0), and record what you pinned. REPLICAS (optional): the second replica
# of a ReplicatedMergeTree pair whose tables central-replicated/scripts/ddl.py
# created in database $DB on both; the run then uses --ch CH,REPLICAS
# --sync-replica --no-ddl, as the replicated consumer does (DECISIONS risk 9).
set -u -o pipefail
: "${OUT:?}" "${CH:?}" "${S3_ROOT:?}" "${BIN:?}"
here=$(cd "$(dirname "$0")" && pwd); spike=$(cd "$here/../../.." && pwd)
S3_KEY=${S3_KEY:-otel}; S3_SECRET=${S3_SECRET:-otelsecret}
REPS=${REPS:-5}; BATCHES=${BATCHES:-1 32}; DATASETS=${DATASETS:-4}; LOAD_MAX=${LOAD_MAX:-0.45}
mkdir -p "$OUT/raw"
ch() { curl -sS "$CH/" --data-binary "$1"; }
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/run.log" >&2; }
envline() { # the box, for the record
  printf '{"t":"%s","ev":"%s","load":"%s","nproc":%s,"model":"%s","kernel":"%s","steal_jiffies":%s}\n' "$(date -u +%FT%TZ)" "$1" \
    "$(cut -d' ' -f1-3 /proc/loadavg)" "$(nproc)" "$(lscpu | sed -n 's/^Model name: *//p')" "$(uname -r)" \
    "$(awk '/^cpu /{print $9}' /proc/stat)" >> "$OUT/env.jsonl"
}
gate() { # wait until the 1-min load is <= LOAD_MAX (15 min at most)
  local t=0 l
  while read -r l _ < /proc/loadavg; ! awk -v l="$l" -v m="$LOAD_MAX" 'BEGIN{exit !(l <= m)}'; do
    [ $t -ge 900 ] && { log "gate: load $l after 900 s; running anyway (flagged)"; envline "gate-timeout"; return; }
    sleep 5; t=$((t + 5))
  done
  envline "gate-ok waited=${t}s"
}

prep() {
  # DATASETS x (32 traces + 32 logs requests of 10,000 rows) through one edge
  # (configs/edge.yaml: one object per request), so objects are 10k rows.
  local W=$OUT/prep; mkdir -p "$W/mix"
  AWS_ACCESS_KEY_ID=$S3_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET OTLP_HTTP=127.0.0.1:24318 OTLP_GRPC=127.0.0.1:24317 \
    ADMIN_HTTP=127.0.0.1:24380 PRODUCER=bench-0 CLUSTER=bench S3_URL="$S3_ROOT/edges" PUT_TIMEOUT=10s \
    "$BIN/otap-s3pq" -c "$spike/otap-rs/configs/edge.yaml" > "$W/edge.log" 2>&1 &
  local e=$!; sleep 3
  for d in $(seq 1 "$DATASETS"); do
    rm -f "$W"/mix/*.pb
    for s in traces logs; do
      "$BIN/mixgen" -signal $s -out "$W/mix" -batches 32 -publishers 1 -route none -seed $((200 + d)) \
        -start "$(date -u -d @$(( $(date -u -d 2026-09-26T10:00:00Z +%s) + 3600 * d )) +%FT%TZ)" > /dev/null || { kill $e; return 1; }
      "$BIN/otlpsend" -url http://127.0.0.1:24318 -signal $s -file "$(ls "$W"/mix/$s-b00*.pb | paste -sd,)" -n 32 -quiet || { kill $e; return 1; }
    done
    log "prep: dataset $d sent"
  done
  kill -INT $e; wait $e 2> /dev/null
  log "prep: objects under $S3_ROOT/edges"
}

run() {
  local rep mb db extra=()
  [ -n "${REPLICAS:-}" ] && extra=(--sync-replica --no-ddl)
  envline "run before"
  for rep in $(seq 1 "$REPS"); do
    for mb in $BATCHES; do
      db=${DB:-vbench}_m${mb}_r$rep
      [ -n "${REPLICAS:-}" ] && db=${DB:?REPLICAS needs DB: the database ddl.py created on both replicas}
      grep -q "\"db\":\"$db\",\"rep\":$rep,\"mb\":$mb" "$OUT/raw/tables.jsonl" 2> /dev/null && continue   # restart-safe
      if [ -z "${REPLICAS:-}" ]; then ch "DROP DATABASE IF EXISTS $db SYNC"; else
        for u in "$CH" "$REPLICAS"; do for t in $(curl -sS "$u/" --data-binary "SELECT name FROM system.tables WHERE database = '$db' AND engine LIKE 'Replicated%'"); do
          curl -sS "$u/" --data-binary "TRUNCATE TABLE $db.$t SYNC"; done; done
      fi
      gate
      local t0; t0=$(date +%s)
      "$BIN/consume" --s3 "$S3_ROOT/edges" --key "$S3_KEY" --secret "$S3_SECRET" --ctl "${S3_ROOT#*://*/*/}/ctl/m$mb-r$rep-$RANDOM" \
        --ch "$CH${REPLICAS:+,$REPLICAS}" --db "$db" --once --max-batch "$mb" --ttl 75s --margin 20s --budget 30s \
        ${extra[@]+"${extra[@]}"} > "$OUT/raw/consume-m$mb-r$rep.json" 2> "$OUT/raw/consume-m$mb-r$rep.err"
      envline "finish m$mb r$rep rc=$?"
      for u in "$CH" ${REPLICAS:+"$REPLICAS"}; do curl -sS "$u/" --data-binary "SYSTEM FLUSH LOGS"; done
      for u in "$CH" ${REPLICAS:+"$REPLICAS"}; do
        curl -sS "$u/" --data-binary "SELECT 'prod' AS root, $mb AS max_batch, $rep AS rep, '$u' AS server, query_id, tables, event_time_microseconds, query_duration_ms,
            written_rows, written_bytes, read_rows, read_bytes,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us, ProfileEvents['UserTimeMicroseconds'] AS user_us,
            ProfileEvents['SystemTimeMicroseconds'] AS sys_us, ProfileEvents['S3HeadObject'] AS heads, ProfileEvents['S3GetObject'] AS gets,
            ProfileEvents['ReadBufferFromS3Bytes'] AS s3_bytes, ProfileEvents['InsertedRows'] AS inserted_rows,
            length(extractAll(query, '\\.parquet')) AS parquet_mentions, memory_usage, exception_code
          FROM system.query_log WHERE type != 'QueryStart' AND query_kind = 'Insert' AND has(databases, '$db')
            AND event_time >= toDateTime($t0 - 5) ORDER BY event_time_microseconds FORMAT JSONEachRow"
      done | gzip >> "$OUT/raw/statements.jsonl.gz"
      ch "SELECT '$db' AS db, $rep AS rep, $mb AS mb, table, sum(rows) AS rows, count() AS parts FROM system.parts
          WHERE database = '$db' AND active GROUP BY table FORMAT JSONEachRow" >> "$OUT/raw/tables.jsonl"
      [ -z "${REPLICAS:-}" ] && ch "DROP DATABASE IF EXISTS $db SYNC"
      log "run m$mb r$rep done"
    done
  done
  envline "run after"
}

analyze() {
  # block 2's analysis, unchanged, reading $OUT/raw (it looks next to itself).
  cp -f "$spike/bench/clean/block2/analyze.py" "$OUT/analyze.py"
  python3 "$OUT/analyze.py" | tee "$OUT/results.md"
  python3 "$OUT/analyze.py" --json > "$OUT/fit.json"
  # usRow as the calculator defines it: all-in CPU per row, spans 0.75 + logs 0.25, at 32 objects per statement.
  zcat "$OUT/raw/statements.jsonl.gz" | python3 -c '
import json, sys, statistics as st, collections
g = collections.defaultdict(list)
for l in sys.stdin:
    r = json.loads(l)
    if int(r["exception_code"]) or not int(r["written_rows"]): continue
    t = [x for x in r["tables"] if not x.startswith("_table_function")][0].split(".", 1)[1]
    g[(t, int(r["max_batch"]), int(r["rep"]))].append((float(r["cpu_us"]), int(r["written_rows"])))
per = collections.defaultdict(list)
for (t, mb, rep), v in g.items():
    per[(t, mb)].append(sum(c for c, _ in v) / sum(n for _, n in v))
for (t, mb), v in sorted(per.items()):
    print(f"{t}\tmax-batch {mb}\tus/row median {st.median(v):.2f} [{min(v):.2f}-{max(v):.2f}] over {len(v)} reps")
tr = [st.median(per[(t, 32)]) for t in ("otel_traces",) if (t, 32) in per]
lg = [st.median(per[(t, 32)]) for t in ("otel_logs",) if (t, 32) in per]
if tr and lg: print(f"usRow (0.75 spans + 0.25 logs, 32 objects/statement): {0.75 * tr[0] + 0.25 * lg[0]:.2f}")
' | tee "$OUT/usrow.txt"
}

case ${1:?prep|run|analyze|all} in
  prep) prep ;;
  run) run ;;
  analyze) analyze ;;
  all) prep && run && analyze ;;
  *) echo "prep|run|analyze|all" >&2; exit 2 ;;
esac
