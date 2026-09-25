#!/bin/bash
# Edge benchmarks, interleaved, REPS processes per configuration, each 3
# warm-up + 30 timed 10k-row batches (the accounting of parquetgo's pubbench):
#  1. in-process (encbench vs pubbench): encode + commit, local file or S3;
#  2. the whole pipeline process (otap-s3pq: OTLP/HTTP receiver + exporter),
#     fed 33 distinct 10k-span requests by otlpsend; CPU and peak RSS read
#     from /proc/<pid> around the 30 timed requests.
#
#   B=target/release T=tools-bin D=data OUT=results/bench.jsonl scripts/bench.sh
set -u
B=${B:?}; T=${T:?}; D=${D:?}; OUT=${OUT:?}
REPS=${REPS:-3}; BATCHES=${BATCHES:-30}
S3=${S3:-http://127.0.0.1:18333/otel}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
export CHDB_TEST_S3=$S3 CHDB_TEST_S3_KEY=otel CHDB_TEST_S3_SECRET=otelsecret
load() { echo "{\"rep\":$1,\"load\":\"$(cut -d' ' -f1 /proc/loadavg)\"}" >> "$OUT"; }

cpu_ms() { # utime+stime of a pid, ms
  awk '{print ($14+$15)*1000/'"$(getconf CLK_TCK)"'}' /proc/$1/stat
}
pipeline() { # signal path
  local sig=$1 path=$2 port=14318
  env OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port-1)) OTLP_PATH=$path PRODUCER=bench-pipe \
    S3_URL=$S3/otap-rs/bench/pipeline "$B/otap-s3pq" -c "$here/configs/edge.yaml" > "$tmp/pipe.log" 2>&1 &
  local pid=$!
  sleep 1.5
  local rss0; rss0=$(awk '/VmRSS/{print $2/1024}' /proc/$pid/status)
  files=$(ls "$D"/$sig-bench-v*.pb | paste -sd,)
  "$T/otlpsend" -url http://127.0.0.1:$port -signal $sig -file "$files" -n 3 -quiet > /dev/null
  local c0 t0; c0=$(cpu_ms $pid); t0=$(date +%s.%N)
  # the next 30 distinct requests (files 3..32)
  files=$(ls "$D"/$sig-bench-v*.pb | sed -n "4,$((3+BATCHES))p" | paste -sd,)
  local sum; sum=$("$T/otlpsend" -url http://127.0.0.1:$port -signal $sig -file "$files" -n $BATCHES -quiet | tail -1)
  local c1 t1; c1=$(cpu_ms $pid); t1=$(date +%s.%N)
  local hwm; hwm=$(awk '/VmHWM/{print $2/1024}' /proc/$pid/status)
  kill -INT $pid; wait $pid 2>/dev/null
  python3 -c "
import json,sys
s=json.loads('''$sum''')
print(json.dumps({'Impl':'rust-pipeline-$path','Signal':'$sig','Dest':'s3','Rows':10000,'Batches':$BATCHES,
 'CPUMSPerBatch':($c1-$c0)/$BATCHES,'MaxRSSMB':$hwm,'RSSAfterStartMB':$rss0,'MedianMS':s['median_ack_ms'],
 'MinMS':s['min_ack_ms'],'MaxMS':s['max_ack_ms'],'RowsPerSec':10000*$BATCHES/($t1-$t0)}))" >> "$OUT"
}

for rep in $(seq 1 "$REPS"); do
  for dest in local s3; do
    for sig in traces logs; do
      if [ $dest = local ]; then dst=(); purl=file://$tmp/pq; rm -rf "$tmp/pq"; mkdir -p "$tmp/pq";
      else dst=(--s3 $S3/otap-rs/bench/encbench --key otel --secret otelsecret); purl=$S3/otap-rs/bench/pubbench; fi
      f=$D/$sig-testgen-10000.pb
      load $rep
      "$B/encbench" --file $f --signal $sig --batches $BATCHES --warmup 3 "${dst[@]}" >> "$OUT"
      "$B/encbench" --file $f --signal $sig --batches $BATCHES --warmup 3 --bloom none "${dst[@]}" >> "$OUT"
      "$B/encbench" --file $f --signal $sig --batches $BATCHES --warmup 3 --bloom all "${dst[@]}" >> "$OUT"
      "$B/encbench" --file $f --signal $sig --batches $BATCHES --warmup 3 --path via_otap "${dst[@]}" >> "$OUT"
      if [ $dest = local ]; then
        "$B/encbench" --file $f --signal $sig --batches $BATCHES --warmup 3 --zstd 1 --label rust-direct-zstd1 >> "$OUT"
        "$B/encbench" --file $f --signal $sig --batches $BATCHES --warmup 3 --format arrow "${dst[@]}" >> "$OUT"
      fi
      "$T/pubbench" -impl parquet-go -url $purl -signal $sig -batches $BATCHES -warmup 3 >> "$OUT" 2>>"$tmp/pubbench.err"
      "$T/pubbench" -impl parquet-go -url $purl -signal $sig -batches $BATCHES -warmup 3 -bloom=false >> "$OUT" 2>>"$tmp/pubbench.err"
    done
  done
  for sig in traces logs; do
    load $rep
    pipeline $sig direct
    pipeline $sig via_otap
  done
done
cat "$tmp/pubbench.err" >&2
rm -rf "$tmp"
