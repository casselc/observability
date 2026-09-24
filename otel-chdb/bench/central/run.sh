#!/usr/bin/env bash
# Central ingest benchmark: a ClickHouse server bulk-ingesting published edge
# batches either from the edge's native s3_plain_rewritable tables (attached
# read-only) or from the per-batch Parquet objects via s3().
#
# Needs: an S3 endpoint with a bucket that the server can reach, a ClickHouse
# server's HTTP port, libchdb (CHDB_LIB_PATH), Go, python3 with requests.
#
#   CHDB_LIB_PATH=/path/libchdb.so S3_ROOT=http://127.0.0.1:18333/otel \
#   S3_KEY=otel S3_SECRET=otelsecret CH_URL=http://127.0.0.1:18123 ./run.sh
#
# Writes results/*.jsonl and results/summary.md. It creates databases
# central, r_* (and drops temporary ones) on the server, and publishes under
# $REGION/ in the bucket (default: a fresh name per run). Runs about 20 min.
# STALE_CHECK=1 adds a final check, 11 min after the writer's merge, that a
# refresh_parts_interval = 0 reader of a live generation breaks.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
: "${CHDB_LIB_PATH:?set CHDB_LIB_PATH to libchdb.so (chDB 26.7.x)}"
export S3_ROOT=${S3_ROOT:-http://127.0.0.1:18333/otel} S3_KEY=${S3_KEY:-otel} S3_SECRET=${S3_SECRET:-otelsecret}
export CH_URL=${CH_URL:-http://127.0.0.1:18123}
export REGION=${REGION:-ce$(date -u +%m%d%H%M)}
WORK=${WORK:-$(mktemp -d)}
export SCRATCH=$WORK RESULTS=${RESULTS:-$HERE/results}
mkdir -p "$RESULTS"
echo "work dir $WORK, region $REGION, results $RESULTS"

(cd "$HERE/gen" && go build -o "$WORK/gen" .)
(cd "$HERE/../../chdbexporter" && go build -o "$WORK/chdbattach" ./cmd/chdbattach)
export CHDBATTACH=$WORK/chdbattach
G=$WORK/gen
C=(-s3 "$S3_ROOT" -key "$S3_KEY" -secret "$S3_SECRET" -region "$REGION" -spans 8000)

wait_log() { for _ in $(seq 600); do grep -q "$2" "$1" && return 0; sleep 0.5; done; echo "timeout: $1" >&2; exit 1; }
ctl() { curl -sf "http://127.0.0.1:$1$2"; }
PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# 1. Data. "natural": 60 batches, merges as they happen, writer stays up.
"$G" "${C[@]}" -path "$WORK/natural" -producer natural -batches 60 -control 127.0.0.1:17001 > "$WORK/natural.log" 2>&1 &
PIDS+=($!)
wait_log "$WORK/natural.log" "control on"
# "unmerged": the same 60 batches with merges stopped after the first.
"$G" "${C[@]}" -path "$WORK/unmerged" -producer unmerged -batches 1 -control 127.0.0.1:17002 > "$WORK/unmerged.log" 2>&1 &
UP=$!
wait_log "$WORK/unmerged.log" "control on"
curl -sf --get "http://127.0.0.1:17002/sql" --data-urlencode "q=SYSTEM STOP MERGES" > /dev/null
ctl 17002 "/push?n=59" > /dev/null
ctl 17002 /quit > /dev/null
wait "$UP"
# "optimized": 60 batches, OPTIMIZE FINAL at seal, then the process exits.
"$G" "${C[@]}" -path "$WORK/opt" -producer optimized -batches 60 -seal-optimize > "$WORK/opt.log" 2>&1
# 10 producers x 5 batches.
for i in $(seq 0 9); do "$G" "${C[@]}" -path "$WORK/m$i" -producer "multi$i" -batches 5 > "$WORK/m$i.log" 2>&1; done
sleep 5

cd "$HERE"
# 2. Measurements.
python3 setup_attach.py "$RESULTS/attach.jsonl"
python3 exp_ingest.py "$RESULTS/ingest.jsonl" 3
python3 exp_insert50.py "$RESULTS/insert50.jsonl" 6
python3 exp_hint.py "$RESULTS/hint.jsonl"
python3 exp_conv.py "$RESULTS/conv.jsonl"
python3 exp_manifest.py "$RESULTS/manifest.jsonl"
python3 exp_poll.py "$RESULTS/poll.jsonl" 30
python3 exp_schema.py "$RESULTS/schema.txt"
python3 exp_storage.py "$RESULTS/storage.jsonl"
python3 exp_dedup.py "$RESULTS/dedup.jsonl"     # pushes to and merges "natural"
python3 exp_dedup2.py "$RESULTS/dedup2.jsonl"
MERGED_AT=$(date +%s)

# 3. A live 240-batch generation for keep-up / catch-up reads.
"$G" "${C[@]}" -path "$WORK/big" -producer big -batches 240 -control 127.0.0.1:17003 > "$WORK/big.log" 2>&1 &
PIDS+=($!)
wait_log "$WORK/big.log" "control on"
python3 exp_live.py "$RESULTS/live.jsonl"
ctl 17003 /quit > /dev/null || true

if [ "${STALE_CHECK:-0}" = 1 ]; then
  sleep $(( MERGED_AT + 660 - $(date +%s) > 0 ? MERGED_AT + 660 - $(date +%s) : 0 ))
  python3 - > "$RESULTS/stale.txt" <<'EOF'
from common import *
txt, _, _, code = ch(f"SELECT count() FROM r_nat.{TABLE} SETTINGS use_query_condition_cache = 0", check=False)
print(f"refresh_parts_interval = 0 reader of the live 'natural' generation, >10 min after the writer merged: HTTP {code}: {txt.strip()[:300]}")
EOF
  cat "$RESULTS/stale.txt"
fi
ctl 17001 /quit > /dev/null || true

python3 summarize.py "$RESULTS/ingest.jsonl" > "$RESULTS/summary.md"
python3 summarize.py "$RESULTS/insert50.jsonl" >> "$RESULTS/summary.md"
echo "done: $RESULTS"
