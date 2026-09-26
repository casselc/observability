#!/bin/bash
# The Go edge's cost per batch, old path against new, to S3 (SeaweedFS on
# localhost), through a request-counting proxy: parquet-go with manifests
# (parquetgo.Publisher: object + manifest PUT) against the manifest-less
# edge (parquetgo/edge, the s3pq exporter's core: one create-only PUT per
# object), 10k spans / logs / metric points per batch (metrics: 2,000 of
# each type; the edge in layout B and in the ClickStack tables), REPS
# processes each, interleaved.
#   S=scratch-with-pubbench REPS=3 results/runedge.sh && python3 results/summarize.py $S/edge.jsonl
S=${S:?set S to a scratch dir holding a pubbench binary}
export CHDB_TEST_S3=${CHDB_TEST_S3:-http://127.0.0.1:18333/goedge-bench} CHDB_TEST_S3_KEY=otel CHDB_TEST_S3_SECRET=otelsecret
REPS=${REPS:-3}; OUT=${OUT:-$S/edge.jsonl}; BATCHES=${BATCHES:-30}
for rep in $(seq 1 $REPS); do
  for sig in traces logs metrics; do
    n=10000; [ $sig = metrics ] && n=2000
    impls="parquet-go edge"; [ $sig = metrics ] && impls="parquet-go edge edge:clickstack_tables"
    for impl in $impls; do
      extra="-bloom=true"; [ "${impl#edge}" != "$impl" ] && extra=""
      layout=series_table; [ "$impl" = edge:clickstack_tables ] && layout=clickstack_tables
      echo "{\"rep\":$rep,\"load\":\"$(cut -d' ' -f1 /proc/loadavg)\"}" >> $OUT
      $S/pubbench -impl ${impl%%:*} -layout $layout -url $CHDB_TEST_S3/r$rep -signal $sig -n $n -batches $BATCHES -warmup 3 -count-s3 $extra >> $OUT 2>>$S/edge.err
    done
  done
done
