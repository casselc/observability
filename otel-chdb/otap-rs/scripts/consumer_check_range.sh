#!/bin/bash
# consumer_check_range.sh: what the consumer's count check costs on a table
# whose parts are on S3 (a cold tier), over every partition and restricted
# to the partitions of the batch's own received time +- the horizon
# (`_partition_value.1 BETWEEN ...`, README "The check's partition range"),
# at a 1-day and a HORIZON_DAYS horizon; and what the horizon audit's
# queries cost on the same table (`consume --print-audit-sql`, the
# statements `consume horizon-audit` sends): the candidates over every key
# and over a 1/16 sample, and the confirmation of two planted copies (one
# 10 days after its original: late; one 2 days after: within the horizon).
#
# A logs table (the consumer's DDL) on a dynamic S3 disk (SeaweedFS), wide
# parts, DAYS daily partitions of 2 parts each, KEYS content keys per part.
# Each check asks for 32 content keys (31 absent, 1 present today), cold
# (mark, index, uncompressed and filesystem caches dropped) and warm, REPS
# times; per query: S3 GETs, S3 read time, CPU, parts and rows read (the
# query's own ProfileEvents, via clickhouse client), and whether the
# projection answered (EXPLAIN).
#
#   CHC="clickhouse client --port 19000" B=bin DAYS=90 ROWS=4000 KEYS=40 REPS=5 HORIZON_DAYS=3 \
#     OUT=results/consumer/scale/check-range.jsonl scripts/consumer_check_range.sh
set -u
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
BUCKET=${BUCKET:-scale-consumer}
DAYS=${DAYS:-90}
ROWS=${ROWS:-4000}
KEYS=${KEYS:-40}
REPS=${REPS:-5}
HORIZON_DAYS=${HORIZON_DAYS:-3}
MODES=${MODES:-full ranged1 ranged audit audit16 confirm}
AUDIT_THREADS=${AUDIT_THREADS:-2}
OUT=${OUT:-/dev/stdout}
B=${B:?dir with consume}
CHC=${CHC:?clickhouse client command}
DB=${DBP:-scale_cold_}$(date +%s)
tmp_runs=$(mktemp); tmp_proj=$(mktemp)
trap 'rm -f "$tmp_runs" "$tmp_proj"' EXIT
ch() { curl -sS "$CH/" --data-binary "$1"; }
# The query's own ProfileEvents, as JSON.
prof() { # query [client settings]
  $CHC ${2:-} --print-profile-events --profile-events-delay-ms=-1 --query "$1" 2>&1 >/dev/null \
    | awk '/\[ 0 \] [A-Za-z0-9]+: [0-9]+ \(increment\)/ {n = $7; sub(":", "", n); v[n] += $8} END {printf "{"; s = ""; for (k in v) {printf "%s\"%s\": %s", s, k, v[k]; s = ", "}; print "}"}'
}

ddl=$("$B/consume" --print-ddl logs --s3 "$S3/$BUCKET/x" | sed "s/db\.otel_logs/$DB.otel_logs/")
disk="disk = disk(type = 's3', endpoint = '$S3/$BUCKET/cold-$DB/', access_key_id = 'otel', secret_access_key = 'otelsecret'), min_bytes_for_wide_part = 0"
ch "CREATE DATABASE $DB"
ch "${ddl}, $disk" || exit 1
T=$DB.otel_logs
for d in $(seq 0 $((DAYS - 1))); do
  for part in 0 1; do
    ch "INSERT INTO $T (Timestamp, ServiceName, Body, received_at, row_ordinal, producer_id, content_key)
        SELECT now64(9) - INTERVAL $d DAY, 'svc', concat('body ', toString(number)), toStartOfDay(now64(9)) - INTERVAL $d DAY + INTERVAL 12 HOUR,
               number % 100, 'p', lower(hex(MD5(concat(toString($d), '-', toString($part), '-', toString(intDiv(number, $((ROWS / KEYS))))))))
        FROM numbers($ROWS)" || exit 1
  done
done
parts=$(ch "SELECT count() FROM system.parts WHERE database = '$DB' AND active")
bytes=$(ch "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '$DB' AND active")
# Content keys are hashes (BLAKE3 in the edge; MD5 here): every part's keys
# span the whole key range, so the projection's index prunes no part by key.
present=$(ch "SELECT lower(hex(MD5('0-0-3')))")
keys="'$present'"
for i in $(seq 1 31); do keys="$keys, '$(ch "SELECT lower(hex(MD5('absent-$i-$RANDOM')))")'"; done
now_ns=$(ch "SELECT toUnixTimestamp64Nano(now64(9))")
day=86400000000000
# Two copies, ingested again today (their own part): a key of day 10's first
# part (a copy 10 days later: late) and one of day 2's (2 days: within).
late=$(ch "SELECT lower(hex(MD5('10-0-3')))"); near=$(ch "SELECT lower(hex(MD5('2-0-5')))")
for k in "$late" "$near"; do
  ch "INSERT INTO $T (Timestamp, ServiceName, Body, received_at, row_ordinal, producer_id, producer_epoch, content_key)
      SELECT now64(9), 'svc', 'copy', toStartOfDay(now64(9)) + INTERVAL 12 HOUR, number, 'p', 'E2', '$k' FROM numbers($((ROWS / KEYS)))" || exit 1
done
parts=$(ch "SELECT count() FROM system.parts WHERE database = '$DB' AND active")
range_q() { # horizon in days
  echo "SELECT content_key, count() FROM $T WHERE content_key IN ($keys) AND _partition_value.1 BETWEEN toDate(fromUnixTimestamp64Nano(toInt64($((now_ns - $1 * day))))) AND toDate(fromUnixTimestamp64Nano(toInt64($((now_ns + $1 * day))))) GROUP BY content_key FORMAT TSV"
}
full="SELECT content_key, count() FROM $T WHERE content_key IN ($keys) GROUP BY content_key FORMAT TSV"
since=$((now_ns - 2 * day))
audit=$("$B/consume" --print-audit-sql "$T" --since-ns "$since" --s3 "$S3/$BUCKET/x")
audit16=$("$B/consume" --print-audit-sql "$T" --since-ns "$since" --audit-sample-hex 1 --s3 "$S3/$BUCKET/x")
cand=$(curl -sS "$CH/?max_threads=$AUDIT_THREADS&optimize_use_projections=1" --data-binary "$audit")
[ "$(echo "$cand" | cut -f1 | sort | tr '\n' ' ')" = "$(printf '%s\n' "$late" "$near" | sort | tr '\n' ' ')" ] || { echo "audit candidates: $cand" >&2; exit 1; }
cdays=$(echo "$cand" | cut -f2 | tr -d '[]' | tr '\n' ',' | sed 's/,$//')
confirm=$("$B/consume" --print-audit-sql "$T" --keys "$late,$near" --days "$cdays" --s3 "$S3/$BUCKET/x" | tr '\n' ';')
query_of() {
  case $1 in
    full) echo "$full" ;; ranged1) range_q 1 ;; ranged) range_q "$HORIZON_DAYS" ;;
    audit) echo "$audit" ;; audit16) echo "$audit16" ;; confirm) echo "$confirm" ;;
  esac
}
settings_of() { case $1 in audit*|confirm) echo "--max_threads=$AUDIT_THREADS --optimize_use_projections=1" ;; esac; }
drop() {
  for c in "MARK CACHE" "UNCOMPRESSED CACHE" "FILESYSTEM CACHE" "PRIMARY INDEX CACHE" "INDEX MARK CACHE" "INDEX UNCOMPRESSED CACHE" "QUERY CACHE"; do
    ch "SYSTEM DROP $c" > /dev/null 2>&1
  done
}
for mode in full ranged1 ranged; do
  q=$(query_of $mode)
  res=$(ch "$q")
  [ "$(echo "$res" | wc -l)" = 1 ] && echo "$res" | grep -q "^$present	$((ROWS / KEYS))$" || { echo "wrong answer ($mode): $res" >&2; exit 1; }
done
for mode in $MODES; do
  q=$(query_of $mode)
  [ $mode = confirm ] && { echo "$mode: table" >> "$tmp_proj"; continue; }
  ch "EXPLAIN projections = 1 ${q% FORMAT TSV}" | grep -q "ReadFromMergeTree (by_content)" && echo "$mode: by_content" >> "$tmp_proj" || echo "$mode: table" >> "$tmp_proj"
done
# The query's own ProfileEvents, summed over its statements (confirm: two).
prof_all() { # mode
  local q; q=$(query_of "$1")
  IFS=';' read -ra qs <<< "$q"
  for x in "${qs[@]}"; do [ -n "$x" ] && prof "$x" "$(settings_of "$1")"; done \
    | python3 -c 'import json,sys; t={}
for l in sys.stdin:
    for k,v in json.loads(l).items(): t[k]=t.get(k,0)+v
print(json.dumps(t))'
}
for rep in $(seq 1 "$REPS"); do
  for mode in $MODES; do
    for cache in cold warm; do
      [ $cache = cold ] && drop
      echo "{\"mode\": \"$mode\", \"cache\": \"$cache\", \"ev\": $(prof_all $mode)}" >> "$tmp_runs"
    done
  done
done
python3 - "$tmp_runs" "$tmp_proj" "$DAYS" "$parts" "$bytes" "$ROWS" "$KEYS" "$HORIZON_DAYS" "$AUDIT_THREADS" <<'PY' | tee -a "$OUT"
import json, sys, statistics as st
runs, proj, days, parts, nbytes, rows, keys, hdays, threads = sys.argv[1:]
proj = dict(l.strip().split(": ") for l in open(proj))
by = {}
for l in open(runs):
    r = json.loads(l)
    by.setdefault((r["mode"], r["cache"]), []).append(r["ev"])
for (mode, cache), evs in sorted(by.items()):
    med = lambda f: round(st.median([f(e) for e in evs]), 2)
    print(json.dumps({"mode": mode, "cache": cache, "n": len(evs),
        "s3_gets": med(lambda e: e.get("S3GetObject", 0) + e.get("DiskS3GetObject", 0)),
        "s3_read_ms": med(lambda e: e.get("ReadBufferFromS3Microseconds", 0) / 1000),
        "cpu_ms": med(lambda e: e.get("OSCPUVirtualTimeMicroseconds", 0) / 1000),
        "user_sys_ms": med(lambda e: (e.get("UserTimeMicroseconds", 0) + e.get("SystemTimeMicroseconds", 0)) / 1000),
        "real_ms": med(lambda e: e.get("RealTimeMicroseconds", 0) / 1000),
        "parts_read": med(lambda e: e.get("SelectedParts", 0)), "marks_read": med(lambda e: e.get("SelectedMarks", 0)),
        "rows_read": med(lambda e: e.get("SelectedRows", 0)), "answered_by": proj[mode],
        "days": int(days), "parts": int(parts), "table_bytes": int(nbytes), "rows_per_part": int(rows), "keys_per_part": int(keys),
        "horizon_days": {"ranged1": 1, "ranged": int(hdays)}.get(mode), "max_threads": int(threads) if mode.startswith(("audit", "confirm")) else None,
        "peak_memory": med(lambda e: e.get("MemoryTrackerPeakUsage", 0) if "MemoryTrackerPeakUsage" in e else 0)}))
PY
ch "DROP DATABASE $DB SYNC"
"$B/consume" purge --s3 "$S3/$BUCKET/cold-$DB" > /dev/null 2>&1
