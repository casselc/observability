#!/bin/bash
# wmhistory_model.sh: the checks of wmHistory.qnt (the watermark's history,
# DECISIONS.md D29 amendment 2026-10-01, FORMAT.md §4.1), quint 0.32 with the
# Rust evaluator, in retirement_model.sh's form:
#  - `sim` rows: the design by simulation (expect ok), witnesses (a
#    violation of `not(w)` means w was reached: expect VIOLATED), each mutant
#    against the property it breaks (expect VIOLATED);
#  - `runs` rows: the scripted runs (`*DesignTest` on the design,
#    `*BreaksTest` on the mutant): expect all-pass.
# One quint process at a time.
#
#   OUT=results.txt model/wmhistory_model.sh
set -u
OUT=${OUT:-/dev/stdout}
SEED=${SEED:-0x5eed}
N=${N:-20000}
cd "$(dirname "$0")" || exit 1
echo "quint $(quint --version), seed $SEED" | tee "$OUT"
fails=0
sim() { # main invariant samples steps expect [note]
  local t0 out v; t0=$(date +%s.%N)
  out=$(quint run wmHistory.qnt --main "$1" --invariant "$2" --max-samples "$3" --max-steps "$4" --seed "$SEED" 2>&1)
  v=ok; echo "$out" | grep -q "\[violation\]" && v=VIOLATED
  echo "$out" | grep -q -E "\[ok\]|\[violation\]" || v=ERROR
  [ "$v" = "$5" ] || fails=$((fails + 1))
  printf "sim   %-18s %-28s %6s x %-3s %-8s (expect %s) %5.1fs %s\n" "$1" "$2" "$3" "$4" "$v" "$5" \
    "$(echo "$(date +%s.%N) - $t0" | bc)" "${6:-}" | tee -a "$OUT"
}
runs() { # main match
  local out n f; out=$(quint test wmHistory.qnt --main "$1" --match "$2" --max-samples 1 2>&1)
  n=$(echo "$out" | grep -c " ok "); f=$(echo "$out" | grep -c "failed after")
  [ "$n" -gt 0 ] && [ "$f" -eq 0 ] || fails=$((fails + 1))
  printf "runs  %-18s %-44s passed %d failed %d (expect all-pass)\n" "$1" "$2" "$n" "$f" | tee -a "$OUT"
}

sim historyDesign safety "$N" 40 ok
for w in wSealed wFinalAnswer wProvisionalRose wConcurrent; do
  sim historyDesign "not($w)" "$N" 40 VIOLATED
done
sim stampEarly sound "$N" 40 VIOLATED
sim noSkew sound "$N" 40 VIOLATED
sim noClamp answersHold "$N" 40 VIOLATED
sim sealFromRead answersHold "$N" 40 VIOLATED
runs historyDesign "DesignTest"
for m in stampEarly noSkew noClamp sealFromRead; do runs "$m" "${m}BreaksTest"; done
echo "failures: $fails" | tee -a "$OUT"
[ "$fails" -eq 0 ]
