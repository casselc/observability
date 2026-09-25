#!/bin/bash
# OTAP input end to end: the Go otelarrow producer (tools/cmd/otapsend) sends
# the traces and logs datasets over OTAP gRPC streams into the upstream OTAP
# receiver (configs/edge-otap.yaml) -> exporter:s3pq; then correctness.py
# compares the objects with parquetgo's reference exactly as for the OTLP
# paths (path name "otapgrpc"), optionally next to an OTLP/HTTP run
# ("direct"). Metrics over OTAP: PATHS="otapgrpc series:otapgrpc" in
# scripts/metrics_e2e.sh.
#
#   B=target/release T=tools-bin D=data RUN=o1 PREFIX=otap-rs-edge scripts/otap_e2e.sh
set -u
B=${B:?}; T=${T:?}; D=${D:?}
RUN=${RUN:-o$(date +%s)}
PREFIX=${PREFIX:-otap-rs-edge}
S3=${S3:-http://127.0.0.1:18333}
here=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-$here/results/otap}
mkdir -p "$OUT"
tmp=$(mktemp -d)
for path in otapgrpc direct; do
  cfg=edge-otap.yaml; [ $path = direct ] && cfg=edge.yaml
  VERBOSE=true PRODUCER=corr-$path OTLP_HTTP=127.0.0.1:14418 OTLP_GRPC=127.0.0.1:14417 OTAP_GRPC=127.0.0.1:14419 \
    S3_URL=$S3/otel/$PREFIX/corr/$RUN/$path "$B/otap-s3pq" -c "$here/configs/$cfg" >> "$OUT/$RUN-$path.edge.log" 2>&1 &
  E=$!
  sleep 1.5
  for sig in traces logs; do
    for ds in testgen-3000 nasty-700; do  # slot 0, slot 1
      if [ $path = otapgrpc ]; then
        "$T/otapsend" -addr 127.0.0.1:14419 -signal $sig -file "$D/$sig-$ds.pb" -n 1 >> "$OUT/$RUN-$path.send.log" 2>&1
      else
        "$T/otlpsend" -url http://127.0.0.1:14418 -signal $sig -file "$D/$sig-$ds.pb" -n 1 -quiet >> "$OUT/$RUN-$path.send.log" 2>&1
      fi
    done
  done
  kill -INT $E; wait $E 2>/dev/null
done
"$T/otlpgen" -out "$tmp" -ref $S3/otel/$PREFIX/corr/$RUN/ref -epoch $RUN > "$OUT/$RUN-ref.log" 2>&1
S3_PREFIX=$PREFIX python3 "$here/scripts/correctness.py" "$RUN" otapgrpc direct | tee "$OUT/correctness-$RUN.txt"
rm -rf "$tmp"
