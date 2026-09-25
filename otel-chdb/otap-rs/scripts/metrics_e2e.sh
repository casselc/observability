#!/bin/bash
# Metrics correctness, end to end: otlpsend (OTLP/HTTP, /v1/metrics) ->
# otap-s3pq -> SeaweedFS, then scripts/metrics_correctness.py against the
# contrib exporter's rows and parquetgo's objects for the same requests.
#
#   B=target/release T=tools-bin D=metrics-data RUN=m1 [PATHS="direct via_otap series:direct series:via_otap"] scripts/metrics_e2e.sh
#
# A path "otapgrpc" (or "series:otapgrpc") sends each dataset as OTAP instead,
# with tools/cmd/otapsend (the Go otelarrow producer) into the upstream OTAP
# receiver (configs/edge-otap.yaml).
# A path "series:X" runs the edge with metrics_layout series_table (layout B,
# src/series.rs) on OTLP path X, under the prefix series_X; the others with
# clickstack_tables. metrics_correctness.py checks layout B through the
# compatibility views (sql/series_views.sql) against contrib's rows.
set -u
B=${B:?dir with otap-s3pq}
T=${T:?dir with otlpsend, otlpgen, metricsref}
D=${D:?dir with the otlpgen -metrics files}
RUN=${RUN:-m$(date +%s)}
PATHS=${PATHS:-direct}
S3=${S3:-http://127.0.0.1:18333}
PREFIX=${PREFIX:-metrics-rs}   # under the bucket otel
export S3_PREFIX=$PREFIX
here=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-$here/results/metrics}
mkdir -p "$OUT"
export T

# One exporter run per (path, dataset), each under its own prefix, so a
# request the path rejects doesn't shift the others' slots.
names=""
for spec in $PATHS; do
  layout=clickstack_tables path=$spec name=$spec
  case $spec in series:*) layout=series_table path=${spec#series:} name=series_${spec#series:};; esac
  names="$names $name"
  cfg=edge.yaml; [ "$path" = otapgrpc ] && { cfg=edge-otap.yaml; path=direct; }
  for f in testgen-3000 nasty-700 extra; do
    METRICS_LAYOUT=$layout OTLP_HTTP=127.0.0.1:14418 OTLP_GRPC=127.0.0.1:14417 OTAP_GRPC=127.0.0.1:14419 OTLP_PATH=$path VERBOSE=true PRODUCER=corr-$name \
      S3_URL=$S3/otel/$PREFIX/corr/$RUN/$name/$f "$B/otap-s3pq" -c "$here/configs/$cfg" >> "$OUT/corr-$RUN-$name.edge.log" 2>&1 &
    E=$!
    sleep 1.5
    if [ $cfg = edge-otap.yaml ]; then
      "$T/otapsend" -addr 127.0.0.1:14419 -signal metrics -file "$D/metrics-$f.pb" -n 1 >> "$OUT/corr-$RUN-$name.send.log" 2>&1
    else
      "$T/otlpsend" -url http://127.0.0.1:14418 -signal metrics -file "$D/metrics-$f.pb" -n 1 -timeout 120s -quiet \
        >> "$OUT/corr-$RUN-$name.send.log" 2>&1
    fi
    kill -INT $E; wait $E 2>/dev/null
  done
done
"$T/otlpgen" -metrics -ref $S3/otel/$PREFIX/corr/$RUN/ref -epoch $RUN > "$OUT/corr-$RUN-ref.log" 2>&1
python3 "$here/scripts/metrics_correctness.py" "$RUN" "$D" $names | tee "$OUT/correctness-$RUN.txt"
