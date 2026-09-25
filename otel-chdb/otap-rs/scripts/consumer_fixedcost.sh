#!/bin/bash
# consumer_fixedcost.sh: per-statement server CPU (the query's own
# ProfileEvents, via clickhouse-client) of the consumer's statements on
# small edge objects: INSERT … SELECT FROM s3({k1,…,kN}) for N = 1, 8, 32,
# with the single-block settings (one part per object) and squashed into one
# part, and the projection count check for 1 and 32 content keys.
#
#   CHC="clickhouse client --port 19000" RUN=bench123 SIGNAL=traces REPS=3 scripts/consumer_fixedcost.sh
#
# Needs a table made by the consumer: $DB.otel_$SIGNAL (run consume once).
set -u
CHC=${CHC:?clickhouse client command}
RUN=${RUN:?}
SIGNAL=${SIGNAL:-traces}
DB=${DB:-otaprs_consumer_fc}
REPS=${REPS:-3}
S3=${S3:-http://127.0.0.1:18333}
BASE="max_threads=1,max_insert_threads=1,max_block_size=1048576,max_insert_block_size=1048576,input_format_parquet_max_block_size=1048576,input_format_parquet_prefer_block_bytes=4294967296"
NOSQ="$BASE,min_insert_block_size_rows=0,min_insert_block_size_bytes=0"
SQ="$BASE,min_insert_block_size_rows=1048576,min_insert_block_size_bytes=4294967296"
here=$(cd "$(dirname "$0")/.." && pwd)
prefix=otap-rs-consumer/$RUN/edges/edge-1/$SIGNAL
mapfile -t KEYS < <(curl -sS http://127.0.0.1:18123/ --data-binary "SELECT _path FROM s3('$S3/otel/$prefix/*/*.parquet','otel','otelsecret','One') ORDER BY _path" | sed 's|^otel/||')
struct=$(cd "$here" && CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target} cargo run -q --release --bin consume -- --print-structure "$SIGNAL" 2>/dev/null)
cols=$(cd "$here" && CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target} cargo run -q --release --bin consume -- --print-cols "$SIGNAL" 2>/dev/null)
tbl=$($CHC --query "SELECT name FROM system.tables WHERE database = '$DB' AND name LIKE 'otel_%' AND name LIKE '%$SIGNAL%' LIMIT 1")
$CHC --query "DROP TABLE IF EXISTS $DB.fc"
$CHC --query "CREATE TABLE $DB.fc AS $DB.$tbl"
cpu() { # query -> cpu ms of the query itself
  $CHC --print-profile-events --profile-events-delay-ms=-1 --query "$1" 2>&1 \
    | awk '/\[ 0 \] (UserTimeMicroseconds|SystemTimeMicroseconds):/ {c += $(NF-1)} END {printf "%.2f", c / 1000}'
}
i=0
take() { local n=$1 out=""; for _ in $(seq 1 "$n"); do out="$out,${KEYS[$i]}"; i=$((i + 1)); done; echo "${out#,}"; }
for rep in $(seq 1 "$REPS"); do
  for n in 1 8 32; do
    for mode in nosquash squash; do
      [ "$n" = 1 ] && [ "$mode" = squash ] && continue
      st=$NOSQ; [ $mode = squash ] && st=$SQ
      keys=$(take "$n"); url="$S3/otel/{$keys}"; [ "$n" = 1 ] && url="$S3/otel/$keys"
      ms=$(cpu "INSERT INTO $DB.fc ($cols, content_key) SETTINGS $st, insert_deduplication_token='fc-$RANDOM-$RANDOM' SELECT $cols, 'k' || toString(cityHash64(_path)) FROM s3('$url', 'otel', 'otelsecret', 'Parquet', '$struct')")
      echo "{\"signal\": \"$SIGNAL\", \"what\": \"insert\", \"objects\": $n, \"mode\": \"$mode\", \"cpu_ms\": $ms, \"cpu_ms_per_object\": $(echo "$ms / $n" | bc -l | xargs printf %.2f)}"
    done
  done
  for n in 1 32; do
    list=$(for j in $(seq 1 $n); do printf "'k%d'," $((RANDOM * j)); done); list=${list%,}
    ms=$(cpu "SELECT content_key, count() FROM $DB.fc WHERE content_key IN ($list) GROUP BY content_key FORMAT Null")
    echo "{\"signal\": \"$SIGNAL\", \"what\": \"check\", \"keys\": $n, \"cpu_ms\": $ms}"
  done
done
$CHC --query "DROP TABLE IF EXISTS $DB.fc"
