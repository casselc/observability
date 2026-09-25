#!/bin/bash
# Metrics layout B (series_table) against the ClickStack tables, at the edge:
# CPU, bytes and objects (S3 PUTs) per request, REPS processes each,
# interleaved.
#  1. in-process (encbench, to S3 through the real lanes; a committed series
#     object marks its series announced, as in the exporter):
#       fleet:   the spike's fleet (../metrics-layout/fleet, seriesref -fleet):
#                130 consecutive 10k-point batches of 20 pods, 3 warm-up (the
#                first announces every series) + 127 timed, crossing one hourly
#                cache window, so the timed part holds one re-announce;
#       testgen: otlpgen's mixed 10k-point batches (2,000 points of each type,
#                mostly unique series: layout B's worst case), 12 distinct
#                requests cycled, 3 + 30;
#  2. the whole otap-s3pq process fed the fleet batches over OTLP/HTTP:
#     CPU from /proc, around 127 requests after 3 warm-up requests.
#
#   B=target/release T=tools-bin FLEET=fleet-dir D=metrics-data OUT=results/series/bench.jsonl scripts/series_bench.sh
set -u
B=${B:?}; T=${T:?}; FLEET=${FLEET:?}; D=${D:?}; OUT=${OUT:?}
REPS=${REPS:-3}
S3=${S3:-http://127.0.0.1:18333/otel}
PREFIX=${PREFIX:-otap-rs-edge}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
cpu_ms() { awk '{print ($14+$15)*1000/'"$(getconf CLK_TCK)"'}' /proc/$1/stat; }
load() { echo "{\"rep\":$1,\"load\":\"$(cut -d' ' -f1 /proc/loadavg)\"}" >> "$OUT"; }
tg=$(ls "$D"/metrics-mixed-10000-b*.pb | paste -sd,)

pipeline() { # layout
  local port=14718 layout=$1
  METRICS_LAYOUT=$layout OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port-1)) PRODUCER=bench-pipe \
    S3_URL=$S3/$PREFIX/bench/pipeline-$layout "$B/otap-s3pq" -c "$here/configs/edge.yaml" > "$tmp/pipe.log" 2>&1 &
  local pid=$!
  sleep 1.5
  local all; all=$(ls "$FLEET"/*.pb)
  "$T/otlpsend" -url http://127.0.0.1:$port -signal metrics -file "$(echo "$all" | head -3 | paste -sd,)" -n 3 -quiet > /dev/null
  local c0 t0; c0=$(cpu_ms $pid); t0=$(date +%s.%N)
  local sum; sum=$("$T/otlpsend" -url http://127.0.0.1:$port -signal metrics -file "$(echo "$all" | tail -n +4 | paste -sd,)" -n 127 -quiet | tail -1)
  local c1 t1; c1=$(cpu_ms $pid); t1=$(date +%s.%N)
  local hwm; hwm=$(awk '/VmHWM/{print $2/1024}' /proc/$pid/status)
  kill -INT $pid; wait $pid 2>/dev/null
  python3 -c "
import json
s=json.loads('''$sum''')
print(json.dumps({'Impl':'rust-pipeline-$layout','Signal':'metrics fleet','Dest':'s3','Rows':10000,'Batches':127,
 'CPUMSPerBatch':($c1-$c0)/127,'MaxRSSMB':$hwm,'MedianMS':s['median_ack_ms'],'RowsPerSec':10000*127/($t1-$t0)}))" >> "$OUT"
}

for rep in $(seq 1 "$REPS"); do
  for layout in series clickstack; do
    load $rep
    "$B/encbench" --files "$FLEET" --signal metrics --layout $layout --batches 127 --warmup 3 --label "$layout-fleet" \
      --s3 $S3/$PREFIX/bench/enc --key otel --secret otelsecret >> "$OUT"
    "$B/encbench" --files "$tg" --signal metrics --layout $layout --batches 30 --warmup 3 --label "$layout-testgen" \
      --s3 $S3/$PREFIX/bench/enc --key otel --secret otelsecret >> "$OUT"
  done
  for layout in series_table clickstack_tables; do
    load $rep
    pipeline $layout
  done
done
rm -rf "$tmp"
