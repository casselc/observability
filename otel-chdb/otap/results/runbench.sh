#!/bin/sh
# Runs otapbench: every variant x signal, 3 separate processes each, locally
# and (publishing variants) against S3 through the counting proxy.
# S=scratch dir holding the otapbench binary; S3=http://host/bucket/prefix.
set -eu
rm -rf "$S/out"
: "${S:?scratch dir}" "${S3:=http://127.0.0.1:18333/otel/otap/bench}"
OUT=${OUT:-$S/bench.jsonl}
REPS=${REPS:-3}
run() { "$S/otapbench" "$@" >> "$OUT"; }
for rep in $(seq 1 "$REPS"); do
  for sig in traces logs; do
    for v in encode decode flatten startables; do
      run -variant $v -signal $sig
    done
    for v in ref star flat-parquet flat-arrow via-pdata bar raw; do
      run -variant $v -signal $sig -url "file://$S/out"
    done
    for v in ref star flat-parquet; do
      run -variant $v -signal $sig -url "file://$S/out" -bloom=false
    done
    for v in ref star flat-parquet flat-arrow via-pdata raw; do
      run -variant $v -signal $sig -url "$S3/r$rep" -count-s3
    done
  done
done
