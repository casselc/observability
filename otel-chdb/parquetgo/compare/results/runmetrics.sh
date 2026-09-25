#!/bin/bash
# Runs pubbench for metrics: every metric type (and all five in one request)
# x dest, REPS processes each, interleaved. parquet-go engine only.
S=${S:?set S to a scratch dir holding a pubbench binary}
export CHDB_TEST_S3=${CHDB_TEST_S3:-http://127.0.0.1:18333/otel} CHDB_TEST_S3_KEY=${CHDB_TEST_S3_KEY:-otel} CHDB_TEST_S3_SECRET=${CHDB_TEST_S3_SECRET:-otelsecret}
REPS=${REPS:-3}; OUT=${OUT:-$S/metrics.jsonl}; BATCHES=${BATCHES:-30}
for rep in $(seq 1 $REPS); do
  for dest in local s3; do
    for sig in metrics_gauge metrics_sum metrics_histogram metrics_exponential_histogram metrics_summary metrics; do
      for par in 1 4; do
        if [ $dest = local ]; then rm -rf $S/benchout; mkdir -p $S/benchout; url=file://$S/benchout; else url=$CHDB_TEST_S3/metrics-go/bench; fi
        echo "{\"rep\":$rep,\"load\":\"$(cut -d' ' -f1 /proc/loadavg)\"}" >> $OUT
        $S/pubbench -impl parquet-go -par $par -url $url -signal $sig -batches $BATCHES -warmup 3 $EXTRA >> $OUT 2>>$S/bench.err
      done
    done
  done
done
rm -rf $S/benchout
