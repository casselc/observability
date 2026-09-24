#!/usr/bin/env bash
# Build the woven demo and the quintgo CLI, lint the binding, then run every
# scenario. Each run emits its model steps three ways, and each is validated
# against edgePublish.qnt:
#   spans.jsonl   OTLP/JSON through the OTel SDK      -> reconstructed, replayed
#   steps.jsonl   native step log (no OTel)           -> reconstructed, replayed
#   itf/*.json    model-level ITF, binding applied in-process -> replayed directly
#   OUT=dir            where binaries, traces and generated modules go (default: a temp dir)
#   QUINTGO_BACKEND    quint test backend: rust (default) or typescript
#   VERBOSE=1          print the full reports, not just the verdicts
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

verdict() { # file -> one line: conformance, failing step, invariants, exit status
  local name=$1; shift
  local out; out=$("$OUT/quintgo" validate "$@" 2>&1); local rc=$?
  [ -n "${VERBOSE:-}" ] && echo "$out"
  printf '  %-9s %-28s exit %d  %s\n' "$name" "$(grep -m1 -o 'input .*: [a-zA-Z/ -]*$' <<<"$out" | sed 's/^input [^:]*: //')" "$rc" \
    "$(grep -E 'conformance:|invariant .*: (FAIL|PASS)|step \[[0-9]+\]|violated after' <<<"$out" | sed -E 's/^ +//; s/ is not a transition.*/ disabled/; s/the model.s state does not match.*/state mismatch/' | paste -sd ';' -)"
}

for sc in ${SCENARIOS:-happy rotation-race ambiguous-seal manifest-first no-lock}; do
  echo; echo "=== scenario $sc"
  d="$OUT/$sc"; mkdir -p "$d"
  "$OUT/demo" -scenario "$sc" -out "$d/spans.jsonl" -steps "$d/steps.jsonl" -itf "$d/itf" -binding binding.yaml | grep -E '^scenario|^wrote'
  verdict "otel"  -binding binding.yaml -dir "$d/v-otel"  "$d/spans.jsonl"
  verdict "steps" -binding binding.yaml -dir "$d/v-steps" "$d/steps.jsonl"
  verdict "itf"                         -dir "$d/v-itf"   "$d"/itf/*.itf.json
done
