#!/bin/bash
# retirement_model.sh: the checks of retirement.qnt (dead-lane retirement,
# DECISIONS.md D35, FORMAT.md §3.1), quint 0.32 with the Rust evaluator, in
# open_models.sh's form:
#  - `sim` rows: the design by simulation (expect ok), witnesses (a
#    violation of `not(w)` means w was reached: expect VIOLATED), each mutant
#    against the property it breaks (expect VIOLATED), and the operator's
#    mistake (safe: expect ok, and its quarantine reached);
#  - `runs` rows: the scripted runs (`*DesignTest` on the design,
#    `*BreaksTest` on the mutant): expect all-pass.
# One quint process at a time (each takes up to a few GB).
#
#   OUT=results.txt model/retirement_model.sh
set -u
OUT=${OUT:-/dev/stdout}
SEED=${SEED:-0x5eed}
N=${N:-20000}
cd "$(dirname "$0")" || exit 1
echo "quint $(quint --version), seed $SEED" | tee "$OUT"
fails=0
sim() { # main invariant samples steps expect [note]
  local t0 out v; t0=$(date +%s.%N)
  out=$(quint run retirement.qnt --main "$1" --invariant "$2" --max-samples "$3" --max-steps "$4" --seed "$SEED" 2>&1)
  v=ok; echo "$out" | grep -q "\[violation\]" && v=VIOLATED
  echo "$out" | grep -q -E "\[ok\]|\[violation\]" || v=ERROR
  [ "$v" = "$5" ] || fails=$((fails + 1))
  printf "sim   %-18s %-28s %6s x %-3s %-8s (expect %s) %5.1fs %s\n" "$1" "$2" "$3" "$4" "$v" "$5" \
    "$(echo "$(date +%s.%N) - $t0" | bc)" "${6:-}" | tee -a "$OUT"
}
runs() { # main match
  local out n f; out=$(quint test retirement.qnt --main "$1" --match "$2" --max-samples 1 2>&1)
  n=$(echo "$out" | grep -c " ok "); f=$(echo "$out" | grep -c "failed after")
  [ "$n" -gt 0 ] && [ "$f" -eq 0 ] || fails=$((fails + 1))
  printf "runs  %-18s %-44s passed %d failed %d (expect all-pass)\n" "$1" "$2" "$n" "$f" | tee -a "$OUT"
}

sim retirementDesign safety "$N" 60 ok
for w in wRetiredClosed wRetiredOperator wReborn wLost wZombie wAdoptedReplay; do
  sim retirementDesign "not($w)" "$N" 60 VIOLATED
done
sim retirementDesign "not(wQuarantined)" "$N" 60 ok "(the design never quarantines)"
sim opMistake stable "$N" 60 ok "(an operator retiring a kept volume: nothing lands below the published value)"
sim opMistake completeSound "$N" 60 VIOLATED "(... but its unread custody is below it until quarantined: the evidence matters)"
sim opMistake "not(wQuarantined)" "$N" 60 VIOLATED "(... its replay is quarantined)"
sim retireStale completeSound "$N" 60 VIOLATED
sim closeUndrained completeSound "$N" 60 VIOLATED
sim ingestBelow noLateBelow "$N" 60 VIOLATED
sim staysRetired completeSound "$N" 60 VIOLATED
sim retireInFlight quarantineOnlyOnMistake "$N" 60 VIOLATED
# closeUnsealed (found building it, 2026-09-29): random simulation does not
# reach it (20,000 x 60: ok, which is why the design's first check missed
# it); its scripted run below does.
runs retirementDesign "DesignTest"
runs opMistake opMistakeQuarantinesTest
runs ingestBelow ingestBelowBreaksTest
runs retireStale retireStaleBreaksTest
runs closeUndrained closeUndrainedBreaksTest
runs closeUnsealed closeZombieBreaksTest
echo "failures: $fails" | tee -a "$OUT"
[ "$fails" -eq 0 ]
