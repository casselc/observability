#!/usr/bin/env bash
# Runs otap-rs/scripts/consumer_model.sh (the consumer models: design
# simulations, witnesses, mutants, scripted counterexamples) and turns its
# report into an exit status. consumer_model.sh itself always exits 0.
#
#   OUT=report.txt ci/model-check.sh
#
# Fails when
#   - a simulation expected `ok` (a design instance) reports VIOLATED,
#     i.e. the model found a counterexample to a design's invariant;
#   - a witness or mutant expected VIOLATED is reported with anything but
#     ok/VIOLATED (quint errored), or a `quint test` group has failures or
#     ran nothing.
# Warns (does not fail) when a witness `not(w)` is not reached (a coverage
# gap: the scenario did not occur within the samples), or when a mutant
# expected VIOLATED comes out `ok`:
# random simulation does not always reach those counterexamples within the
# sample budget (the recorded report, otap-rs/results/consumer/scale/model.txt,
# has four), and each such mutant is pinned instead by its scripted
# `*BreaksTest` run, which is checked above.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-${RUNNER_TEMP:-/tmp}/consumer-model.txt}
OUT="$OUT" "$root/otel-chdb/otap-rs/scripts/consumer_model.sh"
rc=0
while IFS= read -r l; do
  case "$l" in
    sim*)
      got=$(echo "$l" | sed -nE 's/.* (ok|VIOLATED) +\(expect (ok|VIOLATED)\).*/\1/p')
      want=$(echo "$l" | sed -nE 's/.*\(expect (ok|VIOLATED)\).*/\1/p')
      if [ -z "$got" ]; then echo "::error::unparsed: $l"; rc=1
      elif [ "$got" = "$want" ]; then :
      elif [ "$want" = ok ]; then echo "::error::design invariant violated: $l"; rc=1
      elif echo "$l" | grep -q ' not('; then echo "::warning::witness not reached by simulation (a coverage gap, not a violation): $l"
      else echo "::warning::mutant not caught by simulation (its scripted *BreaksTest pins it): $l"
      fi ;;
    runs*)
      p=$(echo "$l" | sed -nE 's/.*passed ([0-9]+) failed ([0-9]+).*/\1/p')
      f=$(echo "$l" | sed -nE 's/.*passed ([0-9]+) failed ([0-9]+).*/\2/p')
      if [ -z "$p" ] || [ "$p" -eq 0 ] || [ "$f" -ne 0 ]; then echo "::error::quint test: $l"; rc=1; fi ;;
  esac
done < "$OUT"
n=$(grep -cE '^(sim|runs) ' "$OUT")
[ "$n" -gt 0 ] || { echo "::error::no results in $OUT"; rc=1; }
echo "$n checks in $OUT, exit $rc"
exit $rc
