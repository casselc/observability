#!/bin/bash
# Runs pubbench: every impl x signal x dest, REPS processes each, interleaved.
S=${S:?set S to a scratch dir holding a pubbench binary}
export CHDB_LIB_PATH=${CHDB_LIB_PATH:?}  CHDB_TEST_S3=http://127.0.0.1:18333/otel CHDB_TEST_S3_KEY=otel CHDB_TEST_S3_SECRET=otelsecret
REPS=${REPS:-3}; OUT=${OUT:-$S/bench.jsonl}; BATCHES=${BATCHES:-30}
for rep in $(seq 1 $REPS); do
  for dest in local s3; do
    for sig in traces logs; do
      for impl in "chdb" "arrow" "parquet-go" "parquet-go -par 4"; do
        if [ $dest = local ]; then rm -rf $S/benchout; mkdir -p $S/benchout; url=file://$S/benchout; else url=$CHDB_TEST_S3/pqbench; fi
        echo "{\"rep\":$rep,\"load\":\"$(cut -d' ' -f1 /proc/loadavg)\"}" >> $OUT
        $S/pubbench -impl $impl -url $url -signal $sig -batches $BATCHES -warmup 3 $EXTRA >> $OUT 2>>$S/bench.err
      done
    done
  done
done
rm -rf $S/benchout
