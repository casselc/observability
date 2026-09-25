#!/bin/bash
# Metrics edge benchmarks, interleaved, REPS processes per configuration,
# each 3 warm-up + 30 timed batches of 10,000 data points (pubbench's
# accounting), per metric type and for a mixed request (2,000 points of each
# type, five objects):
#  1. in-process: encbench (Rust: OTLP direct, OTAP input, via OTAP) against
#     parquetgo's pubbench, to a local file and to S3;
#  2. the whole otap-s3pq process, fed 30 distinct 10k-point requests (six
#     batches of each type) by otlpsend; CPU and peak RSS from /proc.
#
#   B=target/release T=tools-bin D=metrics-data OUT=results/metrics/bench.jsonl scripts/metrics_bench.sh
set -u
B=${B:?}; T=${T:?}; D=${D:?}; OUT=${OUT:?}
REPS=${REPS:-3}; BATCHES=${BATCHES:-30}
S3=${S3:-http://127.0.0.1:18333/otel}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
export CHDB_TEST_S3=$S3 CHDB_TEST_S3_KEY=otel CHDB_TEST_S3_SECRET=otelsecret
load() { echo "{\"rep\":$1,\"load\":\"$(cut -d' ' -f1 /proc/loadavg)\"}" >> "$OUT"; }
cpu_ms() { awk '{print ($14+$15)*1000/'"$(getconf CLK_TCK)"'}' /proc/$1/stat; }
TYPES="gauge sum histogram exponential_histogram summary"

pipeline() {
  local port=14618
  env OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port-1)) PRODUCER=bench-pipe \
    S3_URL=$S3/metrics-rs/bench/pipeline METRICS_LAYOUT=${METRICS_LAYOUT:-clickstack_tables} "$B/otap-s3pq" -c "$here/configs/edge.yaml" > "$tmp/pipe.log" 2>&1 &
  local pid=$!
  sleep 1.5
  local rss0; rss0=$(awk '/VmRSS/{print $2/1024}' /proc/$pid/status)
  "$T/otlpsend" -url http://127.0.0.1:$port -signal metrics -file "$(ls "$D"/metrics-mixed-10000-b0[0-2].pb | paste -sd,)" -n 3 -quiet > /dev/null
  local files; files=$(for b in 00 01 02 03 04 05; do for t in $TYPES; do echo "$D/metrics-$t-10000-b$b.pb"; done; done | paste -sd,)
  local c0 t0; c0=$(cpu_ms $pid); t0=$(date +%s.%N)
  local sum; sum=$("$T/otlpsend" -url http://127.0.0.1:$port -signal metrics -file "$files" -n 30 -quiet | tail -1)
  local c1 t1; c1=$(cpu_ms $pid); t1=$(date +%s.%N)
  local hwm; hwm=$(awk '/VmHWM/{print $2/1024}' /proc/$pid/status)
  kill -INT $pid; wait $pid 2>/dev/null
  python3 -c "
import json
s=json.loads('''$sum''')
print(json.dumps({'Impl':'rust-pipeline-direct','Signal':'metrics (30 single-type requests)','Dest':'s3','Rows':10000,'Batches':30,
 'CPUMSPerBatch':($c1-$c0)/30,'MaxRSSMB':$hwm,'RSSAfterStartMB':$rss0,'MedianMS':s['median_ack_ms'],
 'MinMS':s['min_ack_ms'],'MaxMS':s['max_ack_ms'],'RowsPerSec':10000*30/($t1-$t0)}))" >> "$OUT"
}

for rep in $(seq 1 "$REPS"); do
  for dest in local s3; do
    for t in $TYPES mixed; do
      if [ $dest = local ]; then dst=(); purl=file://$tmp/pq; rm -rf "$tmp/pq"; mkdir -p "$tmp/pq";
      else dst=(--s3 $S3/metrics-rs/bench/encbench --key otel --secret otelsecret); purl=$S3/metrics-rs/bench/pubbench; fi
      if [ $t = mixed ]; then f=$D/metrics-mixed-10000.pb; psig=metrics; pn=2000; else f=$D/metrics-$t-10000-b00.pb; psig=metrics_$t; pn=10000; fi
      load $rep
      "$B/encbench" --file $f --signal metrics --batches $BATCHES --warmup 3 "${dst[@]}" >> "$OUT"
      "$B/encbench" --file $f --signal metrics --batches $BATCHES --warmup 3 --path otap "${dst[@]}" >> "$OUT"
      "$B/encbench" --file $f --signal metrics --batches $BATCHES --warmup 3 --path via_otap "${dst[@]}" >> "$OUT"
      "$T/pubbench" -impl parquet-go -url $purl -signal $psig -n $pn -batches $BATCHES -warmup 3 >> "$OUT" 2>>"$tmp/pubbench.err"
      "$T/pubbench" -impl parquet-go -url $purl -signal $psig -n $pn -batches $BATCHES -warmup 3 -bloom=false >> "$OUT" 2>>"$tmp/pubbench.err"
    done
  done
  load $rep
  pipeline
done
cat "$tmp/pubbench.err" >&2
rm -rf "$tmp"
