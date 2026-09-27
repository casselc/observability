#!/bin/bash
# range_audit_replicated.sh: the consumer's partition-range count check and
# the horizon audit on a replicated table (plain ReplicatedMergeTree, a copy
# per replica: policy tiered_own), both replicas compared.
#
#  1. range: DAYS daily partitions inserted on r1 (old ones moved to each
#     replica's own S3 prefix by TTL), read on r2 after SYNC REPLICA
#     LIGHTWEIGHT; the consumer's ranged check (`_partition_value.1 BETWEEN`,
#     the SQL of sql.rs `range_sql`) against the unranged one, on both
#     replicas: same counts, answered from `by_content` (EXPLAIN), parts read.
#  2. sync coverage: fetches stopped on r2, a batch inserted on r1 into
#     today's partition and one 2 days back; r2's ranged check is stale (0),
#     SYNC … LIGHTWEIGHT times out rather than answer, and after START
#     FETCHES the sync returns and the check sees both partitions.
#  3. audit: a copy 5 days after its original (late at a 3-day horizon) and
#     one 2 days after (within the horizon: "unexplained"); `consume
#     horizon-audit` against r1 and against r2 must report the same copies;
#     then r2 lagging (fetches stopped) with a new late copy on r1: what each
#     replica's audit says.
#
#   OUT=results/range-audit scripts/range_audit_replicated.sh
set -u
. "$(dirname "$0")/common.sh"
CONSUME=${CONSUME:-$SP/otap-rs-target/release/consume}
OUT=${OUT:-$HERE/results/range-audit}
DAYS=${DAYS:-10}
DB=${DB:-repl_range_$(date +%s)}
S3=http://127.0.0.1:18333
BUCKET=repl-range
mkdir -p "$OUT"
L="$OUT/log.txt"
: > "$L"
say() { echo "$*" | tee -a "$L"; }
curl -s -o /dev/null --aws-sigv4 "aws:amz:us-east-1:s3" --user otel:otelsecret -X PUT "$S3/$BUCKET" || true

python3 "$HERE/scripts/ddl.py" --db "$DB" --policy tiered_own --zero-copy 0 --signals logs --move '1 DAY' --delete '100 DAY' > "$OUT/ddl.sql"
apply_sql 29000 "$OUT/ddl.sql" && apply_sql 39000 "$OUT/ddl.sql" || { say "DDL failed"; exit 1; }
T=$DB.otel_logs
say "db $DB, table $T, $DAYS daily partitions, policy tiered_own (move after 1 day)"

# Rows of one ingestion: key, day offset, epoch, rows (row_ordinal 0..rows-1), all received at one time.
ins() { # replica-url key day epoch rows
  curl -sS "$1/" --data-binary "INSERT INTO $T (Timestamp, ServiceName, Body, producer_id, producer_epoch, batch_id, row_ordinal, received_at, content_key)
    SELECT toStartOfDay(now64(9)) - INTERVAL $3 DAY, 'svc', 'b', 'p1', '$4', 0, number, toStartOfDay(now64(9)) + INTERVAL 1 HOUR - INTERVAL $3 DAY, '$2'
    FROM numbers($5)"
}
# 10 keys a day, one part per day, on r1.
for d in $(seq 0 $((DAYS - 1))); do
  curl -sS "$R1/" --data-binary "INSERT INTO $T (Timestamp, ServiceName, Body, producer_id, producer_epoch, batch_id, row_ordinal, received_at, content_key)
    SELECT toStartOfDay(now64(9)) - INTERVAL $d DAY, 'svc', 'b', 'p1', 'E$d', 0, number % 20, toStartOfDay(now64(9)) + INTERVAL 1 HOUR - INTERVAL $d DAY,
           concat('k', toString($d), '-', toString(intDiv(number, 20))) FROM numbers(200)"
done
# Planted copies: late (original 5 days back, copy today) and within the horizon (2 days back, copy today).
ins "$R1" late 5 Eorig-late 7; ins "$R1" late 0 Ecopy-late 7
ins "$R1" near 2 Eorig-near 5; ins "$R1" near 0 Ecopy-near 5
q2 "SYSTEM SYNC REPLICA $T LIGHTWEIGHT"
# Let the TTL moves run (per replica, not coordinated).
for _ in $(seq 1 30); do
  c=$(q1 "SELECT count() FROM system.parts WHERE database = '$DB' AND active AND disk_name = 's3_own'")
  c2=$(q2 "SELECT count() FROM system.parts WHERE database = '$DB' AND active AND disk_name = 's3_own'")
  [ "$c" -ge $((DAYS - 2)) ] && [ "$c2" -ge $((DAYS - 2)) ] && break
  sleep 2
done
say "parts per disk: $(qb "SELECT disk_name, count() FROM system.parts WHERE database = '$DB' AND active GROUP BY disk_name ORDER BY 1 FORMAT CSV" | tr '\n' ' ')"

# ---- 1. the ranged check on each replica -------------------------------------------------
now_ns=$(q1 "SELECT (toUnixTimestamp(toStartOfDay(now())) + 3600) * 1000000000")
H=$((3 * 86400 * 1000000000))
lo=$((now_ns - H)) hi=$((now_ns + H))
range=" AND _partition_value.1 BETWEEN toDate(fromUnixTimestamp64Nano(toInt64($lo))) AND toDate(fromUnixTimestamp64Nano(toInt64($hi)))"
keys="'k0-0','k0-9','k2-3','k3-1','k4-0','k5-5','k9-9','late','near','absent-1','absent-2'"
chk() { # url pred qid
  curl -sS "$1/?optimize_use_projections=1&query_id=$3" --data-binary "SELECT content_key, count() FROM $T WHERE content_key IN ($keys)$2 GROUP BY content_key ORDER BY content_key FORMAT TSV" | tr '\n\t' ' ='
}
run=$(date +%s)
for r in 1 2; do
  U=$([ $r = 1 ] && echo $R1 || echo $R2)
  say "r$r ranged (today +- 3 days): $(chk "$U" "$range" "ra-$run-$r")"
  say "r$r every partition:          $(chk "$U" "" "full-$run-$r")"
  say "r$r EXPLAIN ranged: $(curl -sS "$U/?optimize_use_projections=1" --data-binary "EXPLAIN projections = 1 SELECT content_key, count() FROM $T WHERE content_key IN ($keys)$range GROUP BY content_key" | grep -o 'ReadFromMergeTree ([a-z_]*)\|Name: by_content' | sort -u | tr '\n' ' ')"
done
for r in $R1 $R2; do curl -sS "$r/" --data-binary "SYSTEM FLUSH LOGS"; done
for r in 1 2; do
  U=$([ $r = 1 ] && echo $R1 || echo $R2)
  say "r$r query_log: $(curl -sS "$U/" --data-binary "SELECT query_id, ProfileEvents['SelectedParts'] AS parts, ProfileEvents['S3GetObject'] + ProfileEvents['DiskS3GetObject'] AS gets, round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000, 1) AS cpu_ms, projections FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ('ra-$run-$r', 'full-$run-$r') ORDER BY query_id FORMAT TSV" | tr '\n\t' '; ')"
done
exp_r=$(q1 "SELECT count() FROM $T WHERE content_key IN ($keys)$range" )
say "expected rows in range (r1, no projection): $exp_r"

# ---- 2. does the sync cover every partition? ---------------------------------------------
q2 "SYSTEM STOP FETCHES $T"
ins "$R1" new-today 0 Enew 3; ins "$R1" new-old 2 Enew2 4
keys="'new-today','new-old'"
say "r2, fetches stopped, ranged check: [$(chk "$R2" "$range" "stale-$run")] (r1: [$(chk "$R1" "$range" "fresh-$run")])"
t0=$(date +%s%N)
res=$(curl -sS "$R2/?receive_timeout=3" --data-binary "SYSTEM SYNC REPLICA $T LIGHTWEIGHT" 2>&1 | head -c 200)
say "r2 SYNC LIGHTWEIGHT with fetches stopped: $(( ($(date +%s%N) - t0) / 1000000 )) ms: ${res:-ok}"
q2 "SYSTEM START FETCHES $T"
t0=$(date +%s%N)
res=$(curl -sS "$R2/?receive_timeout=10" --data-binary "SYSTEM SYNC REPLICA $T LIGHTWEIGHT" 2>&1 | head -c 200)
say "r2 SYNC LIGHTWEIGHT after START FETCHES: $(( ($(date +%s%N) - t0) / 1000000 )) ms: ${res:-ok}"
say "r2 ranged check after the sync: [$(chk "$R2" "$range" "synced-$run")]"

# ---- 3. the horizon audit on each replica ------------------------------------------------
audit() { # url tag [flags]
  "$CONSUME" horizon-audit --s3 "$S3/$BUCKET/$DB" --ch "$1" --db "$DB" --audit-no-state --audit-lookback 2d ${3:-} > "$OUT/audit-$2.json" 2> "$OUT/audit-$2.err"
  python3 - "$OUT/audit-$2.json" <<'PY'
import json, sys
a = json.loads(open(sys.argv[1]).read())["horizon_audit"]
f = lambda xs: sorted((x["key"], x["copy"]["epoch"], x["original"]["epoch"], x["dup_rows"]) for x in xs)
print(json.dumps({"replica": a.get("replica", ""), "tables": a["tables"], "candidates": a["candidates"], "late": f(a["late"]), "unexplained": f(a["unexplained"]),
                  "errors": [e[:120] for e in a["errors"]]}))
PY
}
a1=$(audit "$R1" r1 --sync-replica); a2=$(audit "$R2" r2 --sync-replica)
say "audit r1: $a1"; say "audit r2: $a2"
[ "${a1#*\"tables}" = "${a2#*\"tables}" ] && say "audit r1 = r2 (but the replica read): yes" || say "audit r1 = r2: NO"
q2 "SYSTEM STOP FETCHES $T"
ins "$R1" late2 6 Eorig-late2 3; ins "$R1" late2 0 Ecopy-late2 3
say "r2 lagging (fetches stopped), a new late copy on r1:"
say "  audit r1: $(audit "$R1" r1-lag)"; say "  audit r2 (no sync): $(audit "$R2" r2-lag)"
say "  audit r2 --sync-replica: $(audit "$R2" r2-lag-sync "--sync-replica --audit-sync-timeout 3s")"
say "  audit --ch dead,r2 --sync-replica (fails over): $(audit "http://127.0.0.1:9,$R2" dead-r2-lag-sync "--sync-replica --audit-sync-timeout 3s")"
q2 "SYSTEM START FETCHES $T"; q2 "SYSTEM SYNC REPLICA $T LIGHTWEIGHT"
say "  after START FETCHES, audit r2 --sync-replica: $(audit "$R2" r2-after --sync-replica)"
[ "${KEEP:-0}" = 1 ] || { for p in 29000 39000; do $CHBIN client --port $p --query "DROP DATABASE $DB SYNC"; done; say "dropped $DB on both replicas"; }
