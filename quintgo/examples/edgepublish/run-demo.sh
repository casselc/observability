#!/usr/bin/env bash
# Build the woven demo and the quintgo CLI, lint the binding, then run every
# scenario and validate its telemetry against edgePublish.qnt.
#   OUT=dir            where binaries, spans and generated modules go (default: a temp dir)
#   QUINTGO_BACKEND    quint test backend: rust (default) or typescript
set -uo pipefail
cd "$(dirname "$0")"
OUT=${OUT:-$(mktemp -d)}
mkdir -p "$OUT"
export QUINTGO_BACKEND=${QUINTGO_BACKEND:-rust}
echo "# output in $OUT"
(cd ../.. && go build -o "$OUT/quintgo" ./cmd/quintgo) || exit 2
go run github.com/DataDog/orchestrion go build -o "$OUT/demo" ./cmd/demo || exit 2
echo; echo "\$ quintgo lint -binding binding.yaml -src ."
"$OUT/quintgo" lint -binding binding.yaml -src . || exit 2
for sc in ${SCENARIOS:-happy rotation-race ambiguous-seal manifest-first no-lock}; do
  echo; echo "=== scenario $sc"
  "$OUT/demo" -scenario "$sc" -out "$OUT/$sc.jsonl" | sed -n '1p;$p'
  "$OUT/quintgo" validate -binding binding.yaml -dir "$OUT/$sc" "$OUT/$sc.jsonl"
  echo "(exit $?)"
done
