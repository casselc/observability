#!/usr/bin/env bash
# Builds the Go test tools and writes the datasets the otap-rs integration
# tests and the end-to-end scripts read. Offline: nothing is published to S3.
#
#   ci/gen-data.sh DATA [BIN]
#
#   DATA     otlpgen's traces/logs and metrics datasets (OTAPRS_DATA)
#   BIN      where the tools go (default DATA/bin): otlpgen otlpsend
#            faultproxy2 soaksend seriesref (otap-rs/tools) and credstubs
#            (parquetgo/compare)
# Env:
#   VARIANTS=N    also N distinct 10k-row requests per signal
#                 (traces-bench-v{i}.pb; the fault scripts read 12: 6 per
#                 scenario, and the 1st-12th in the zombie scenario)
#   CONFORMANCE=1 also conformance/gen's hostile and mixed datasets
#   SERIES=1      the Go prototype's layout-B objects for tests/series.rs
#                 (DATA/series/go/corr: OTAPRS_SERIES_GO=DATA/series/go)
#   FLEET=1       with SERIES, also the fleet batches (DATA/series/fleet:
#                 OTAPRS_SERIES_FLEET) and their Go objects
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
DATA=${1:?usage: gen-data.sh DATA [BIN]}
mkdir -p "$DATA"; DATA=$(cd "$DATA" && pwd)
BIN=${2:-$DATA/bin}; mkdir -p "$BIN"; BIN=$(cd "$BIN" && pwd)

t0=$(date +%s)
(cd "$root/otel-chdb/otap-rs/tools" &&
  go build -o "$BIN/" ./cmd/otlpgen ./cmd/otlpsend ./cmd/faultproxy2 ./cmd/soaksend ./cmd/seriesref)
(cd "$root/otel-chdb/parquetgo/compare" && go build -o "$BIN/" ./cmd/credstubs)
echo "tools built in $(( $(date +%s) - t0 ))s: $(ls "$BIN" | tr '\n' ' ')"

v=()
[ -n "${VARIANTS:-}" ] && v=(-variants "$VARIANTS")
"$BIN/otlpgen" -out "$DATA" "${v[@]}"
"$BIN/otlpgen" -metrics -out "$DATA"
if [ -n "${CONFORMANCE:-}" ]; then
  (cd "$root/otel-chdb/conformance" && go run ./gen -out "$DATA")
fi
if [ -n "${SERIES:-}" ]; then
  "$BIN/seriesref" -out "$DATA/series/go/corr" \
    "$DATA/metrics-testgen-3000.pb" "$DATA/metrics-nasty-700.pb" "$DATA/metrics-extra.pb"
  if [ -n "${FLEET:-}" ]; then
    "$BIN/seriesref" -fleet "$DATA/series/fleet" -services 2 -rounds 130 -pods-per-batch 20
    "$BIN/seriesref" -out "$DATA/series/go/fleet" "$DATA"/series/fleet/*.pb
  fi
fi
echo "datasets in $(( $(date +%s) - t0 ))s: $(du -sh "$DATA" | cut -f1) in $DATA"
