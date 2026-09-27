#!/usr/bin/env bash
# The end-to-end scripts (faults.sh, go_faults.sh, consumer_soak.sh) report
# PASS/FAIL lines in a summary file and exit 0 either way. This turns the
# summary into an exit status: non-zero if any line says FAIL, or if fewer
# than MIN lines start with PASS (a run that died early reports nothing).
#
#   ci/verdict.sh SUMMARY [MIN]
set -uo pipefail
f=${1:?usage: verdict.sh SUMMARY [MIN]}; min=${2:-1}
[ -s "$f" ] || { echo "::error::$f is missing or empty"; exit 1; }
pass=$(grep -c '^PASS' "$f")
if grep -qw FAIL "$f"; then
  grep -w FAIL "$f" | while read -r l; do echo "::error title=$(basename "$(dirname "$f")")::$l"; done
  exit 1
fi
if [ "$pass" -lt "$min" ]; then
  echo "::error::$f: $pass PASS lines, expected at least $min"; exit 1
fi
echo "$f: $pass PASS, no FAIL"
