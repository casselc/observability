#!/usr/bin/env bash
# Checks alertEvaluator.qnt (../alerts/, AMBIGUITY.md X5/X6): the design
# keeps every invariant over random traces, each witness is reached, and each
# mutant breaks the invariant it is named for. Exits non-zero otherwise.
#
#   model/alert_model.sh                       # SAMPLES=20000 STEPS=40
#   QUINT_BACKEND=typescript model/alert_model.sh   # where the Rust evaluator can't be fetched
set -uo pipefail
cd "$(dirname "$0")" || exit 2
SAMPLES=${SAMPLES:-20000}
STEPS=${STEPS:-40}
backend=()
[ -n "${QUINT_BACKEND:-}" ] && backend=(--backend "$QUINT_BACKEND")
rc=0
check() { # main invariant expect(ok|VIOLATED)
  local out got
  out=$(quint run alertEvaluator.qnt --main "$1" --invariant "$2" --max-steps "$STEPS" --max-samples "$SAMPLES" "${backend[@]}" 2>&1)
  if echo "$out" | grep -q "No violation found"; then got=ok
  elif echo "$out" | grep -q "Invariant violated"; then got=VIOLATED
  else got=error; fi
  printf 'sim %-14s %-24s %s (expect %s)\n' "$1" "$2" "$got" "$3"
  if [ "$got" != "$3" ]; then rc=1; echo "$out" | tail -20; fi
}
check design        safe                   ok
check design        wResolvedAcked         VIOLATED
check design        wTakenNoAnswer         VIOLATED
check evalPastCt    evalOnlyComplete       VIOLATED
check evalPastCt    noLostEpisode          VIOLATED
check ackOnNoAnswer resolvedAfterFiringAck VIOLATED
check ackOnNoAnswer noLostEpisode          VIOLATED
check blindWrite    nextMonotone           VIOLATED
exit $rc
