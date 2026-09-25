#!/bin/bash
# Metrics correctness, end to end: otlpsend (OTLP/HTTP, /v1/metrics) ->
# otap-s3pq -> SeaweedFS, then scripts/metrics_correctness.py against the
# contrib exporter's rows and parquetgo's objects for the same requests.
#
#   B=target/release T=tools-bin D=metrics-data RUN=m1 [PATHS="direct via_otap"] scripts/metrics_e2e.sh
set -u
B=${B:?dir with otap-s3pq}
T=${T:?dir with otlpsend, otlpgen, metricsref}
D=${D:?dir with the otlpgen -metrics files}
RUN=${RUN:-m$(date +%s)}
PATHS=${PATHS:-direct}
S3=${S3:-http://127.0.0.1:18333}
here=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-$here/results/metrics}
mkdir -p "$OUT"
export T

# One exporter run per (path, dataset), each under its own prefix, so a
# request the path rejects doesn't shift the others' slots.
for path in $PATHS; do
  for f in testgen-3000 nasty-700 extra; do
    OTLP_HTTP=127.0.0.1:14418 OTLP_GRPC=127.0.0.1:14417 OTLP_PATH=$path VERBOSE=true PRODUCER=corr-$path \
      S3_URL=$S3/otel/metrics-rs/corr/$RUN/$path/$f "$B/otap-s3pq" -c "$here/configs/edge.yaml" >> "$OUT/corr-$RUN-$path.edge.log" 2>&1 &
    E=$!
    sleep 1.5
    "$T/otlpsend" -url http://127.0.0.1:14418 -signal metrics -file "$D/metrics-$f.pb" -n 1 -timeout 120s -quiet \
      >> "$OUT/corr-$RUN-$path.send.log" 2>&1
    kill -INT $E; wait $E 2>/dev/null
  done
done
"$T/otlpgen" -metrics -ref $S3/otel/metrics-rs/corr/$RUN/ref -epoch $RUN > "$OUT/corr-$RUN-ref.log" 2>&1
python3 "$here/scripts/metrics_correctness.py" "$RUN" "$D" $PATHS | tee "$OUT/correctness-$RUN.txt"
