#!/usr/bin/env bash
# The fail and warn rules for a Quint model report (ci/model-check.sh, the
# nightly `model` job; ci/model-open.sh, the nightly `model-open` job).
# A report is lines of
#   sim    <file> <main> <invariant> <samples> x <steps> <result> (expect ok|VIOLATED) ...
#   verify <file> <main> <invariant> <steps> <result> (expect ok|VIOLATED) ...   (Apalache, bounded)
#   runs   <main> <match> passed N failed M (expect all-pass)                    (quint test)
# where <result> is ok, VIOLATED, ERROR or UNKNOWN (a time limit or the job's
# budget ended it before an answer).
#
#   ci/model-verdict.sh report.txt
#
# Fails when
#   - a row expected `ok` (a design instance) reports VIOLATED: the model
#     found a counterexample to a design's invariant;
#   - a row reports ERROR or cannot be parsed (quint or Apalache failed);
#   - a `quint test` group has failures or ran nothing;
#   - the report has no rows.
# Warns (does not fail) when a witness `not(w)` is not reached (a coverage
# gap, not a violation), when a mutant expected VIOLATED comes out `ok`
# (random simulation does not always reach a counterexample within its
# sample budget; the mutant is pinned by its scripted `*BreaksTest`), and
# when a row is UNKNOWN (no answer within its time: an unknown, which the
# traceability records mark skipped).
set -uo pipefail
report=${1:?report}
rc=0
while IFS= read -r l; do
  case "$l" in
    sim*|verify*)
      got=$(echo "$l" | sed -nE 's/.* (ok|VIOLATED|ERROR|UNKNOWN) +\(expect (ok|VIOLATED)\).*/\1/p')
      want=$(echo "$l" | sed -nE 's/.*\(expect (ok|VIOLATED)\).*/\1/p')
      if [ -z "$got" ]; then echo "::error::unparsed: $l"; rc=1
      elif [ "$got" = "$want" ]; then :
      elif [ "$got" = ERROR ]; then echo "::error::the tool failed: $l"; rc=1
      elif [ "$got" = UNKNOWN ]; then echo "::warning::no answer within the time (an unknown, not a pass): $l"
      elif [ "$want" = ok ]; then echo "::error::design invariant violated: $l"; rc=1
      elif echo "$l" | grep -q ' not('; then echo "::warning::witness not reached by simulation (a coverage gap, not a violation): $l"
      else echo "::warning::mutant not caught by simulation (its scripted *BreaksTest pins it): $l"
      fi ;;
    runs*UNKNOWN*) echo "::warning::no answer within the time (an unknown, not a pass): $l" ;;
    runs*)
      p=$(echo "$l" | sed -nE 's/.*passed ([0-9]+) failed ([0-9]+).*/\1/p')
      f=$(echo "$l" | sed -nE 's/.*passed ([0-9]+) failed ([0-9]+).*/\2/p')
      if [ -z "$p" ] || [ "$p" -eq 0 ] || [ "$f" -ne 0 ]; then echo "::error::quint test: $l"; rc=1; fi ;;
  esac
done < "$report"
n=$(grep -cE '^(sim|verify|runs) ' "$report")
[ "$n" -gt 0 ] || { echo "::error::no results in $report"; rc=1; }
echo "$n checks in $report, exit $rc"
exit $rc
