#!/bin/bash
# consumer_check_range.sh: what the consumer's count check costs on a table
# whose parts are on S3 (a cold tier), over every partition and restricted
# to the partitions of the batch's own received time +- the horizon
# (`_partition_value.1 BETWEEN ...`, README "The check's partition range").
#
# A logs table (the consumer's DDL) on a dynamic S3 disk (SeaweedFS), wide
# parts, DAYS daily partitions of 2 parts each, KEYS content keys per part.
# Each check asks for 32 content keys (31 absent, 1 present today), cold
# (mark, index, uncompressed and filesystem caches dropped) and warm, REPS
# times; per query: S3 GETs, S3 read time, CPU, parts and rows read (the
# query's own ProfileEvents, via clickhouse client), and whether the
# projection answered (EXPLAIN).
#
#   CHC="clickhouse client --port 19000" B=bin DAYS=90 ROWS=4000 KEYS=40 REPS=5 OUT=results/consumer/scale/check-range.jsonl scripts/consumer_check_range.sh
set -u
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
BUCKET=${BUCKET:-scale-consumer}
DAYS=${DAYS:-90}
ROWS=${ROWS:-4000}
KEYS=${KEYS:-40}
REPS=${REPS:-5}
OUT=${OUT:-/dev/stdout}
B=${B:?dir with consume}
CHC=${CHC:?clickhouse client command}
DB=scale_cold_$(date +%s)
tmp_runs=$(mktemp); tmp_proj=$(mktemp)
trap 'rm -f "$tmp_runs" "$tmp_proj"' EXIT
ch() { curl -sS "$CH/" --data-binary "$1"; }
# The query's own ProfileEvents, as JSON.
prof() {
  $CHC --print-profile-events --profile-events-delay-ms=-1 --query "$1" 2>&1 >/dev/null \
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
lo=$((now_ns - day)); hi=$((now_ns + day))
full="SELECT content_key, count() FROM $T WHERE content_key IN ($keys) GROUP BY content_key FORMAT TSV"
ranged="SELECT content_key, count() FROM $T WHERE content_key IN ($keys) AND _partition_value.1 BETWEEN toDate(fromUnixTimestamp64Nano(toInt64($lo))) AND toDate(fromUnixTimestamp64Nano(toInt64($hi))) GROUP BY content_key FORMAT TSV"
drop() {
  for c in "MARK CACHE" "UNCOMPRESSED CACHE" "FILESYSTEM CACHE" "PRIMARY INDEX CACHE" "INDEX MARK CACHE" "INDEX UNCOMPRESSED CACHE" "QUERY CACHE"; do
    ch "SYSTEM DROP $c" > /dev/null 2>&1
  done
}
for mode in full ranged; do
  q=$full; [ $mode = ranged ] && q=$ranged
  res=$(ch "$q")
  [ "$(echo "$res" | wc -l)" = 1 ] && echo "$res" | grep -q "^$present	$((ROWS / KEYS))$" || { echo "wrong answer ($mode): $res" >&2; exit 1; }
  ch "EXPLAIN projections = 1 ${q% FORMAT TSV}" | grep -q "ReadFromMergeTree (by_content)" && echo "$mode: by_content" >> "$tmp_proj" || echo "$mode: table" >> "$tmp_proj"
done
for rep in $(seq 1 "$REPS"); do
  for mode in full ranged; do
    for cache in cold warm; do
      [ $cache = cold ] && drop
      q=$full; [ $mode = ranged ] && q=$ranged
      echo "{\"mode\": \"$mode\", \"cache\": \"$cache\", \"ev\": $(prof "$q")}" >> "$tmp_runs"
    done
  done
done
python3 - "$tmp_runs" "$tmp_proj" "$DAYS" "$parts" "$bytes" "$ROWS" <<'PY' | tee -a "$OUT"
import json, sys, statistics as st
runs, proj, days, parts, nbytes, rows = sys.argv[1:]
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
        "days": int(days), "parts": int(parts), "table_bytes": int(nbytes), "rows_per_part": int(rows)}))
PY
ch "DROP DATABASE $DB SYNC"
"$B/consume" purge --s3 "$S3/$BUCKET/cold-$DB" > /dev/null 2>&1
