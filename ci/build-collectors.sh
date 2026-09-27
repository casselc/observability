#!/usr/bin/env bash
# Builds the two collector distributions with ocb (the OpenTelemetry
# Collector Builder, the version their builder-config.yaml files name):
#   otelcol-s3pq  otel-chdb/awss3/collector: the Go edge (s3pq exporter),
#                 which conformance/run.sh and go_faults.sh run
#   otelcol-chdb  otel-chdb/otelcol: the chdb exporter distribution
#                 (a build check; running it needs libchdb)
#
#   ci/build-collectors.sh BIN [s3pq|chdb|all]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
BIN=${1:?usage: build-collectors.sh BIN [s3pq|chdb|all]}; which=${2:-all}
mkdir -p "$BIN"; BIN=$(cd "$BIN" && pwd)
OCB=go.opentelemetry.io/collector/cmd/builder@v0.161.0
export CGO_ENABLED=0

if [ "$which" = s3pq ] || [ "$which" = all ]; then
  t0=$(date +%s)
  work=$(mktemp -d)
  awss3="$root/otel-chdb/awss3"
  sed "s#@AWSS3@#$awss3#; s#@OUT@#$work/out#" "$awss3/collector/builder-config.yaml" > "$work/builder.yaml"
  go run "$OCB" --config "$work/builder.yaml"
  cp -f "$work/out/otelcol-s3pq" "$BIN/"
  rm -rf "$work"
  echo "otelcol-s3pq built in $(( $(date +%s) - t0 ))s"
fi
if [ "$which" = chdb ] || [ "$which" = all ]; then
  t0=$(date +%s)
  # Its output_path and replaces are relative to otel-chdb/otelcol (_build is gitignored).
  (cd "$root/otel-chdb/otelcol" && go run "$OCB" --config builder-config.yaml)
  cp -f "$root/otel-chdb/otelcol/_build/otelcol-chdb" "$BIN/"
  echo "otelcol-chdb built in $(( $(date +%s) - t0 ))s"
fi
