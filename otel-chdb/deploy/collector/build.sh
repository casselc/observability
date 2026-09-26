#!/bin/bash
# Build otelcol-deploy (the agent and the routing gateway) with ocb v0.161.0.
#
#   collector/build.sh OUT_DIR            # stock contrib loadbalancing exporter
#   PATCHED=1 collector/build.sh OUT_DIR  # with patches/0001 (deterministic
#                                         # per-backend order for logs and metrics)
#
# The patched build copies loadbalancingexporter v0.161.0 out of the Go module
# cache into OUT_DIR/loadbalancingexporter, applies the patch and points ocb at
# it with a `replaces` line. See ../README.md §Routing for why.
set -euo pipefail
out=$(mkdir -p "$1" && cd "$1" && pwd)
here=$(cd "$(dirname "$0")" && pwd)
sed "s#@OUT@#$out#" "$here/builder-config.yaml" > "$out/builder.yaml"
if [ -n "${PATCHED:-}" ]; then
  mod=github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter@v0.161.0
  src=$(go env GOMODCACHE)/$mod
  [ -d "$src" ] || (cd "$out" && GOFLAGS=-mod=mod go mod download "$mod")
  rm -rf "$out/loadbalancingexporter"; cp -r "$src" "$out/loadbalancingexporter"; chmod -R u+w "$out/loadbalancingexporter"
  patch -p3 -d "$out/loadbalancingexporter" < "$here/patches/0001-loadbalancing-deterministic-order.patch"
  printf '\nreplaces:\n  - github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter => %s/loadbalancingexporter\n' "$out" >> "$out/builder.yaml"
fi
cd "$out" && go run go.opentelemetry.io/collector/cmd/builder@v0.161.0 --config "$out/builder.yaml"
