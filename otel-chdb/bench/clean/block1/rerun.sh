#!/bin/bash
# Replacement runs for the two processes lib/envlog.py flagged (steal > 2%):
#  rep 1, local metrics_histogram rust-direct (encbench), and
#  rep 3, s3 logs rust-pipeline-via_otap (its warm-up otlpsend window).
# Same wrappers, pinning and commands as run.sh; each gated on load <= 0.45.
set -u
here=$(cd "$(dirname "$0")" && pwd); clean=$(dirname "$here"); spike=$(dirname "$(dirname "$clean")")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
export ENVLOG=$here/env.jsonl; . "$clean/lib/env.sh"
W=$S/clean/wrap1; S3=http://127.0.0.1:18333/otel/clean/b1
gate; snap "begin rerun-1"; echo '{"clean_rep":"1r"}' >> $here/metrics.jsonl
$W/b/encbench --file $S/otaprs-mdata/metrics-histogram-10000-b00.pb --signal metrics --batches 30 --warmup 3 >> $here/metrics.jsonl
snap "finish rerun-1"
# bench.sh's pipeline(), verbatim but for the variables
B=$W/b; T=$W/t; D=$S/otaprs/data; OUT=$here/bench.jsonl; BATCHES=30; tmp=$(mktemp -d)
cpu_ms() { awk '{print ($14+$15)*1000/'"$(getconf CLK_TCK)"'}' /proc/$1/stat; }
pipeline() { # signal path
  local sig=$1 path=$2 port=14318
  env OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port-1)) OTLP_PATH=$path PRODUCER=bench-pipe \
    S3_URL=$S3/otap-rs/bench/pipeline "$B/otap-s3pq" -c "$spike/otap-rs/configs/edge.yaml" > "$tmp/pipe.log" 2>&1 &
  local pid=$!
  sleep 1.5
  local rss0; rss0=$(awk '/VmRSS/{print $2/1024}' /proc/$pid/status)
  files=$(ls "$D"/$sig-bench-v*.pb | paste -sd,)
  "$T/otlpsend" -url http://127.0.0.1:$port -signal $sig -file "$files" -n 3 -quiet > /dev/null
  local c0 t0; c0=$(cpu_ms $pid); t0=$(date +%s.%N)
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
gate; snap "begin rerun-3"; echo '{"clean_rep":"3r"}' >> $here/bench.jsonl
pipeline logs via_otap
snap "finish rerun-3"
rm -rf $tmp
curl -s -X DELETE 'http://127.0.0.1:18888/buckets/otel/clean/b1?recursive=true&ignoreRecursiveError=true' > /dev/null
