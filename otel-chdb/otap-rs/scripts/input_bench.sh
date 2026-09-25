#!/bin/bash
# Input transports into the same exporter (configs/edge-otap.yaml, one
# process): OTLP/HTTP (otlpsend), OTLP/gRPC (otlpsend -grpc) and OTAP/gRPC
# (otapsend: the Go otelarrow producer, one Arrow stream, dictionary deltas
# across batches). Per signal, 30 distinct 10k-item requests after 3 warm-up
# ones; the whole edge process's CPU from /proc per request, peak RSS, and the
# client's median ack. REPS processes per configuration, interleaved.
#
#   B=target/release T=tools-bin D=data MD=metrics-data OUT=results/inputs/bench.jsonl scripts/input_bench.sh
set -u
B=${B:?}; T=${T:?}; D=${D:?}; MD=${MD:?}; OUT=${OUT:?}
REPS=${REPS:-3}
S3=${S3:-http://127.0.0.1:18333/otel}
PREFIX=${PREFIX:-otap-rs-edge}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
cpu_ms() { awk '{print ($14+$15)*1000/'"$(getconf CLK_TCK)"'}' /proc/$1/stat; }
files() { # signal -> 33 distinct 10k-item requests (3 warm-up + 30)
  case $1 in
    traces|logs) ls "$D"/$1-bench-v*.pb | head -33 ;;
    metrics) ls "$MD"/metrics-mixed-10000-b*.pb "$MD"/metrics-gauge-10000-b*.pb "$MD"/metrics-sum-10000-b*.pb | head -33 ;;
  esac
}
for rep in $(seq 1 "$REPS"); do
  for sig in traces logs metrics; do
    for tr in otlp-http otlp-grpc otap-grpc; do
      METRICS_LAYOUT=${METRICS_LAYOUT:-series_table} OTLP_HTTP=127.0.0.1:14818 OTLP_GRPC=127.0.0.1:14817 OTAP_GRPC=127.0.0.1:14819 \
        PRODUCER=inbench S3_URL=$S3/$PREFIX/inbench/$tr "$B/otap-s3pq" -c "$here/configs/edge-otap.yaml" > "$tmp/edge.log" 2>&1 &
      p=$!
      sleep 1.5
      all=$(files $sig)
      warm=$(echo "$all" | head -3 | paste -sd,); timed=$(echo "$all" | tail -n +4 | paste -sd,)
      send() { # files n
        case $tr in
          otlp-http) "$T/otlpsend" -url http://127.0.0.1:14818 -signal $sig -file "$1" -n $2 -quiet ;;
          otlp-grpc) "$T/otlpsend" -grpc 127.0.0.1:14817 -signal $sig -file "$1" -n $2 -quiet ;;
          otap-grpc) "$T/otapsend" -addr 127.0.0.1:14819 -signal $sig -file "$1" -n $2 -quiet ;;
        esac
      }
      if [ $tr = otap-grpc ]; then
        # one stream for the warm-up and timed batches, as an exporter keeps it
        "$T/otapsend" -addr 127.0.0.1:14819 -signal $sig -file "$warm,$timed" -n 33 -warmup 3 -pause 1s -quiet > "$tmp/send.out" &
        s=$!
        while ! grep -q warm "$tmp/send.out" 2>/dev/null; do sleep 0.02; done
        sleep 0.3
        c0=$(cpu_ms $p); t0=$(date +%s.%N); wait $s
        sum=$(tail -1 "$tmp/send.out"); n=30
      else
        send "$warm" 3 > /dev/null
        c0=$(cpu_ms $p); t0=$(date +%s.%N)
        sum=$(send "$timed" 30 | tail -1); n=30
      fi
      c1=$(cpu_ms $p); t1=$(date +%s.%N)
      hwm=$(awk '/VmHWM/{print $2/1024}' /proc/$p/status)
      kill -INT $p; wait $p 2>/dev/null
      python3 -c "
import json
s=json.loads('''$sum''')
print(json.dumps({'rep':$rep,'signal':'$sig','transport':'$tr','cpu_ms_per_request':($c1-$c0)/$n,'peak_rss_mb':$hwm,
 'median_ack_ms':s['median_ack_ms'],'sender_encode_ms':s.get('encode_ms_per_batch'),'load':open('/proc/loadavg').read().split()[0]}))" >> "$OUT"
    done
  done
done
rm -rf "$tmp"
