#!/bin/bash
# Block 2 preparation (not measured): edge objects for the central insert
# benchmark, written by the Rust edge (otap-s3pq) as in production.
#  small root  otel/clean/b2/small/edges: soaksend, 400 requests per signal of
#              200 spans / 200 log records / 20 points per metric type
#              (consumer_bench.sh's objects), into edge smallB (series_table:
#              traces, logs, layout B) and edge smallA (clickstack_tables:
#              metrics only).
#  large root  otel/clean/b2/large/edges: 20 distinct 10k-span and 10k-log
#              requests (otlpgen bench-v*), and 12 whole-fleet metrics
#              requests (metrics-layout fleet via seriesref -fleet: 20
#              services, 200 pods, 100k points per request: 52k sum, 28k
#              gauge, 16k histogram, 2k exp. histogram, 2k summary), into
#              bigB (series_table) and bigA (clickstack_tables, metrics only).
set -eu
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
REL=$S/otap-rs-target/release; BIN=$S/clean/bin
spike=$(cd "$(dirname "$0")/../../.." && pwd)
S3=http://127.0.0.1:18333/otel/clean/b2
F=$S/clean/fleet200
[ -d $F ] || $S/otaprs/bin/seriesref -fleet $F -services 20 -rounds 12 -pods-per-batch 200
edge() { # root producer layout port
  METRICS_LAYOUT=$3 OTLP_HTTP=127.0.0.1:$4 OTLP_GRPC=127.0.0.1:$(($4 - 1)) PRODUCER=$2 PUT_TIMEOUT=10s \
    S3_URL=$S3/$1/edges/$2 OTLP_MAX_BODY=256MiB $REL/otap-s3pq -c $spike/otap-rs/configs/edge.yaml > $S/clean/edge-$2.log 2>&1 &
  echo $!
}
p1=$(edge small smallB series_table 24518); p2=$(edge small smallA clickstack_tables 24528)
p3=$(edge large bigB series_table 24538); p4=$(edge large bigA clickstack_tables 24548)
sleep 2
$BIN/soaksend -url http://127.0.0.1:24518 -producer smallB -signals traces,logs,metrics -rate 40 -n 400 -rows 200 -points 20 -out /dev/null
$BIN/soaksend -url http://127.0.0.1:24528 -producer smallA -signals metrics -rate 40 -n 400 -points 20 -out /dev/null
D=$S/otaprs/data
for sig in traces logs; do
  $BIN/otlpsend -url http://127.0.0.1:24538 -signal $sig -file "$(ls $D/$sig-bench-v*.pb | head -20 | paste -sd,)" -n 20 -quiet
done
for port in 24538 24548; do
  $BIN/otlpsend -url http://127.0.0.1:$port -signal metrics -file "$(ls $F/*.pb | paste -sd,)" -n 12 -quiet
done
kill -INT $p1 $p2 $p3 $p4; wait
