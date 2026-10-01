#!/usr/bin/env bash
# Checks bitemporalCatalog.qnt (the entity catalog as bitemporal events, D32
# proposed; ../entities/bitemp/; pseudonymise on departure, D38 O-G9): the
# design keeps its invariants (`safety`: the four of D32 and O-G9's six) over
# random traces, each witness is reached, each mutant breaks the invariant it
# is named for, and the scripted runs pass (the design's *DesignTest on
# `design`, each mutant's *BreaksTest on the mutant). Exits non-zero otherwise.
# One quint process at a time.
#
#   model/bitemp_model.sh                         # SAMPLES=10000 STEPS=12
#   QUINT_BACKEND=typescript model/bitemp_model.sh
#   TRACES=dir model/bitemp_model.sh              # also write ITF traces of `designTrace` for
#                                                 # entities/bitemp (BITEMP_TRACES=dir go test ./...)
set -uo pipefail
cd "$(dirname "$0")" || exit 2
SAMPLES=${SAMPLES:-10000}
STEPS=${STEPS:-12}
SEED=${SEED:-0x5eed}
F=bitemporalCatalog.qnt
backend=()
[ -n "${QUINT_BACKEND:-}" ] && backend=(--backend "$QUINT_BACKEND")
rc=0
echo "quint $(quint --version), seed $SEED, $SAMPLES samples x $STEPS steps"
check() { # main invariant expect(ok|VIOLATED) [samples]
  local out got t0
  t0=$(date +%s)
  out=$(quint run $F --main "$1" --invariant "$2" --max-steps "$STEPS" --max-samples "${4:-$SAMPLES}" --seed "$SEED" "${backend[@]}" 2>&1)
  if echo "$out" | grep -q "No violation found"; then got=ok
  elif echo "$out" | grep -q -E "Invariant violated|\[violation\]"; then got=VIOLATED
  else got=error; fi
  printf 'sim  %-16s %-28s %-8s (expect %s) %4ss\n' "$1" "$2" "$got" "$3" "$(( $(date +%s) - t0 ))"
  if [ "$got" != "$3" ]; then rc=1; echo "$out" | tail -20; fi
}
runs() { # main match
  local out n f
  out=$(quint test $F --main "$1" --match "$2" --max-samples 1 "${backend[@]}" 2>&1)
  n=$(echo "$out" | grep -c " ok "); f=$(echo "$out" | grep -c "failed after")
  printf 'runs %-16s %-40s passed %d failed %d (expect all-pass)\n' "$1" "$2" "$n" "$f"
  if [ "$n" -eq 0 ] || [ "$f" -ne 0 ]; then rc=1; echo "$out" | tail -20; fi
}

check design safety ok
for w in wAnnounceFills wOverseerTakesOver wControllerHolds wUnknownHidesAssert wAnnounceOverridden wCorrected wPruned \
         wPseudonymised wOldBasisRedacted wLateNameHidden wDuplicate; do
  check design "not($w)" VIOLATED 5000
done
check strict d32Safety ok 5000                     # O-G9's properties do not depend on W: checked on design
check strict "not(wOverseerTakesOver)" ok 5000     # strict precedence: the overseer never overrides the controller
check noCeiling resolvesAsSpec VIOLATED 5000
check forward resolvesAsSpec VIOLATED 5000
check sourceBlind resolvesAsSpec VIOLATED 5000
check unknownAsRetract unknownNeverAsserts VIOLATED 5000
check ignoreST monotoneHistory VIOLATED 5000
check pruneAny currentIsResolved VIOLATED 5000
check pruneBySt currentIsResolved VIOLATED 5000
# O-G9 (pseudonymise on departure): the weaker readings, each breaking the property it is named for
check basisScoped pseudonymHidesName VIOLATED 5000
check basisScoped noNameAtOrAfter ok 5000          # pure bitemporal keeps the weaker property only
check lastWins pseudonymStable VIOLATED 5000
check asAssert noNameAtOrAfter VIOLATED 5000
check asAssert stateUnchanged VIOLATED 5000
runs design DesignTest
for m in noCeiling forward sourceBlind unknownAsRetract ignoreST pruneAny pruneBySt basisScoped lastWins asAssert; do runs $m "${m}BreaksTest"; done

if [ -n "${TRACES:-}" ]; then
  mkdir -p "$TRACES"
  quint run $F --main designTrace --invariant safety --max-steps "$STEPS" --max-samples "${TRACE_SAMPLES:-50}" --n-traces "${TRACE_SAMPLES:-50}" \
    --seed "$SEED" --mbt --out-itf "$TRACES/trace_{seq}.itf.json" "${backend[@]}" >/dev/null 2>&1 || rc=1
  echo "traces: $(ls "$TRACES" | wc -l) in $TRACES"
fi
exit $rc
