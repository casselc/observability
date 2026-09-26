#!/bin/bash
# Block 3 preparation (not measured): the merge benchmark's replay pool, as
# ../../merges/README.md "Reproduce" builds it, under otel/clean/merges/pool.
#  traces, logs: the 10 testgen objects the Rust exporter writes (encbench,
#                1 warm-up + 9 batches of the 10k-row testgen request, TraceId
#                bloom), renamed to pool/{traces,logs}/NN.parquet;
#  metrics:      mgen big (24 rounds, 200 pods per batch, layouts B and A) and
#                big50 (50 rounds of exp. histogram and summary). The 10k-row
#                "small" pool is not needed: every clean run is 100k rows.
set -eu
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
F=http://127.0.0.1:18888/buckets/otel/clean/merges
for sig in traces logs; do
  $S/otap-rs-target/release/encbench --file $S/otaprs/data/$sig-testgen-10000.pb --signal $sig --warmup 1 --batches 9 \
    --s3 http://127.0.0.1:18333/otel/clean/merges/tmp --key otel --secret otelsecret > /dev/null
  i=0
  for k in $(curl -s -H 'Accept: application/json' "$F/tmp/$sig/?limit=10" | python3 -c 'import json,sys; print(" ".join(e["FullPath"] for e in json.load(sys.stdin)["Entries"]))'); do
    for o in $(curl -s -H 'Accept: application/json' "http://127.0.0.1:18888$k/?limit=100" | python3 -c 'import json,sys; print(" ".join(sorted(e["FullPath"] for e in json.load(sys.stdin)["Entries"])))'); do
      curl -sf -X POST "$F/pool/$sig/$(printf %02d $i).parquet?mv.from=$o" > /dev/null
      i=$((i + 1))
    done
  done
  echo "$sig: $i objects"
done
curl -s -X DELETE "$F/tmp?recursive=true&ignoreRecursiveError=true" > /dev/null
cd $S/clean/bin
./mgen -prefix clean/merges/pool/big -rounds 24 -pods-per-batch 200 -a
./mgen -prefix clean/merges/pool/big50 -rounds 50 -pods-per-batch 200 -only exponential_histogram,summary
