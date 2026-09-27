#!/bin/bash
# bench_insert.sh: the consumer's insert cost per row, replicated central
# (ReplicatedMergeTree, 2 replicas, tiered_own) against the single node, at
# 32 objects per statement (DECISIONS risk 9).
#
# "single" is a plain MergeTree database on r1 itself (the consumer's own
# DDL), so both targets run on the same server, config and disk; the shared
# single-node server on 18123 has no query_log.
#
# 1. One edge (otap-s3pq, no fault proxy) and one sender for GEN_S seconds at
#    RATE requests/s per signal, 200 spans / 200 logs / 20 points per request,
#    into s3://repl-bench/$RUN/edges.
# 2. REPS times, each target: a fresh database (replicated: scripts/ddl.py on
#    both replicas, the worker with --ch r1,r2 --sync-replica --no-ddl), a fresh
#    checkpoint prefix, `consume --once --max-batch 32`; then from query_log
#    of every server the consumer's INSERT statements (query_id
#    otaprs-consumer-*): statements, objects per statement, rows, and the
#    statements' own CPU (ProfileEvents UserTime + SystemTime) per row, per
#    table; and system.events CPU over the run on every server (inserts plus
#    the other replica's fetches and any merges).
# The load average is recorded with each run: the box may be shared.
#
#   OUT=results/bench-insert REPS=3 scripts/bench_insert.sh
set -u
. "$(dirname "$0")/common.sh"
OTAP=/home/user/observability/otel-chdb/otap-rs
B=${B:-$SP/consumer/bin}
CONSUME=${CONSUME:-$SP/otap-rs-target/release/consume}
OUT=${OUT:-$HERE/results/bench-insert}
REPS=${REPS:-3}
GEN_S=${GEN_S:-60}
RATE=${RATE:-4}
RUN=${RUN:-rbench$(date +%s)}
S3=http://127.0.0.1:18333
SINGLE=$R1
mkdir -p "$OUT"
curl -s -o /dev/null --aws-sigv4 "aws:amz:us-east-1:s3" --user otel:otelsecret -X PUT "$S3/repl-bench" || true
ROOT=$S3/repl-bench/$RUN/edges

if [ "${SKIP_GEN:-0}" != 1 ]; then
  env OTLP_HTTP=127.0.0.1:24398 OTLP_GRPC=127.0.0.1:24397 ADMIN_HTTP=127.0.0.1:28099 PUT_TIMEOUT=5s LANES=1 PRODUCER=bench \
    S3_URL=$ROOT "$B/otap-s3pq" -c "$OTAP/scripts/consumer_soak_edge.yaml" > "$OUT/edge.log" 2>&1 &
  edge=$!
  sleep 2
  "$B/soaksend" -url http://127.0.0.1:24398 -producer bench -signals traces,logs,metrics -rate "$RATE" -rows 200 -points 20 \
    -out "$OUT/acked.jsonl" -timeout 10s -backoff 300ms > "$OUT/send.log" 2>&1 &
  send=$!
  sleep "$GEN_S"
  kill -TERM $send; wait $send 2>/dev/null
  sleep 3
  kill -TERM $edge; wait $edge 2>/dev/null
fi
echo "$RUN: $(wc -l < "$OUT/acked.jsonl") requests acked" | tee "$OUT/gen.txt"

cpu() { curl -sS "$1/" --data-binary "SELECT sum(value) FROM system.events WHERE event IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds')"; }
for rep in $(seq 1 "$REPS"); do
  for target in single replicated; do
    db=repl_bench_${target}_$rep
    ctl=repl-bench/$RUN/ctl-$target-$rep-$RANDOM
    if [ $target = single ]; then
      urls=$SINGLE; flags=(); servers=("$SINGLE")
      curl -sS "$SINGLE/" --data-binary "DROP DATABASE IF EXISTS $db SYNC"
    else
      urls=$R1,$R2; flags=(--sync-replica --no-ddl); servers=("$R1" "$R2")
      python3 "$HERE/scripts/ddl.py" --db "$db" --policy tiered_own --zero-copy 0 --move '30 DAY' --delete '100 DAY' > "$OUT/ddl.sql"
      apply_sql 29000 "$OUT/ddl.sql" && apply_sql 39000 "$OUT/ddl.sql" || exit 1
    fi
    declare -A c0
    for s in "${servers[@]}"; do c0[$s]=$(cpu "$s"); done
    t0=$(date +%s); load0=$(cut -d' ' -f1-3 /proc/loadavg)
    "$CONSUME" --s3 "$ROOT" --ctl "$ctl" --db "$db" --ch "$urls" "${flags[@]}" --once --max-batch 32 \
      > "$OUT/consume-$target-$rep.json" 2> "$OUT/consume-$target-$rep.err"
    t1=$(date +%s); load1=$(cut -d' ' -f1-3 /proc/loadavg)
    sleep 3
    ev=""
    for s in "${servers[@]}"; do
      curl -sS "$s/" --data-binary "SYSTEM FLUSH LOGS"
      ev="$ev $s=$(( ($(cpu "$s") - ${c0[$s]}) / 1000 ))"
    done
    for s in "${servers[@]}"; do
      curl -sS "$s/" --data-binary "
        SELECT '$target', $rep, '$s', arrayFilter(x -> x LIKE '$db.%', tables)[1] AS t, count() AS statements, sum(written_rows) AS rows,
               round(sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / sum(written_rows), 2) AS us_per_row,
               round(sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1000) AS cpu_ms,
               round(avg(query_duration_ms)) AS avg_ms, sum(ProfileEvents['ZooKeeperTransactions']) AS keeper_tx
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND query_id LIKE 'otaprs-consumer-%' AND has(databases, '$db')
          AND event_time >= toDateTime($t0) - 5
        GROUP BY t ORDER BY t FORMAT TSV" >> "$OUT/inserts.tsv"
    done
    python3 - "$OUT/consume-$target-$rep.json" "$target" "$rep" "$load0" "$load1" "$ev" "$t0" "$t1" <<'PY' | tee -a "$OUT/runs.jsonl"
import json, sys
f, target, rep, load0, load1, ev, t0, t1 = sys.argv[1:]
s = json.loads(open(f).read().strip().splitlines()[-1])
print(json.dumps({"target": target, "rep": int(rep), "objects": s["objects_inserted"] + s.get("series_objects_inserted", 0),
                  "rows": s["rows_inserted"], "statements": s["statements"], "wall_s": int(t1) - int(t0),
                  "server_cpu_ms": {kv.split("=", 1)[0]: int(kv.split("=", 1)[1]) for kv in ev.split()},
                  "load_before": load0, "load_after": load1}))
PY
    if [ $target = single ]; then curl -sS "$SINGLE/" --data-binary "DROP DATABASE IF EXISTS $db SYNC"
    else for p in 29000 39000; do $CHBIN client --port $p --query "DROP DATABASE IF EXISTS $db SYNC"; done; fi
  done
done
python3 - "$OUT" <<'PY' | tee "$OUT/summary.txt"
import collections, json, sys
out = sys.argv[1]
rows = [l.rstrip("\n").split("\t") for l in open(f"{out}/inserts.tsv") if l.strip()]
agg = collections.defaultdict(lambda: [0, 0, 0.0])
for target, rep, server, t, st, n, usr, cpu, avg, ktx in rows:
    a = agg[(target, t.split(".")[-1])]
    a[0] += int(st); a[1] += int(n); a[2] += float(cpu)
print("insert statements' own CPU per row (query_log ProfileEvents, summed over reps; the server that ran them):")
for (target, t), (st, n, cpu) in sorted(agg.items()):
    print(f"  {target:10s} {t:45s} statements {st:5d} rows {n:9d} rows/statement {n / max(st, 1):8.0f}  {cpu * 1000 / max(n, 1):6.2f} us/row")
for target in ("single", "replicated"):
    st = sum(v[0] for k, v in agg.items() if k[0] == target); n = sum(v[1] for k, v in agg.items() if k[0] == target)
    cpu = sum(v[2] for k, v in agg.items() if k[0] == target)
    print(f"  {target:10s} all tables: {cpu * 1000 / max(n, 1):.2f} us/row over {n} rows, {st} statements")
runs = [json.loads(l) for l in open(f"{out}/runs.jsonl")]
for r in runs:
    tot = sum(r["server_cpu_ms"].values())
    print(f"  run {r['target']:10s} rep {r['rep']}: {r['objects']} objects in {r['statements']} statements ({r['objects'] / max(r['statements'], 1):.1f}/statement), "
          f"server CPU (system.events, all servers) {tot} ms = {tot * 1000 / max(r['rows'], 1):.1f} us/row; load {r['load_before']} -> {r['load_after']}")
PY
