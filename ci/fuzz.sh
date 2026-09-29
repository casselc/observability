#!/usr/bin/env bash
# Runs every native Go fuzz target (func FuzzXxx(*testing.F)) in the
# repository's Go modules for FUZZTIME each, one at a time (ci/README.md,
# nightly job `fuzz`). Per push the targets run only as ordinary tests, over
# their f.Add seeds and checked-in testdata/fuzz corpus (go-test).
#
#   ci/fuzz.sh [GROUP]      GROUP as for ci/go-modules.sh (default fast)
#
# Env: FUZZTIME (default 60s), OUT (default: none): a failing target's new
# corpus entries (testdata/fuzz/FuzzXxx/*, the crasher) are copied there, so
# the workflow can upload them. Exit status non-zero if any target failed.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root" || exit 1
fuzztime=${FUZZTIME:-60s}
failed=()
n=0
for m in $(ci/go-modules.sh list "${1:-fast}"); do
  # "FuzzXxx" lines, then "ok  <package> ..." closing each package's list
  pending=()
  while read -r line; do
    case "$line" in
      Fuzz*) pending+=("$line") ;;
      ok*)
        pkg=$(awk '{print $2}' <<<"$line")
        for f in "${pending[@]}"; do
          n=$((n + 1))
          echo "::group::$pkg $f ($fuzztime)"
          if ! (cd "$m" && go test -run '^$' -fuzz "^${f}\$" -fuzztime "$fuzztime" "$pkg"); then
            failed+=("$pkg.$f")
            if [ -n "${OUT:-}" ]; then
              dir=$(cd "$m" && go list -f '{{.Dir}}' "$pkg")/testdata/fuzz/$f
              mkdir -p "$OUT/$f" && cp -f "$dir"/* "$OUT/$f/" 2>/dev/null
            fi
            echo "::error::fuzz target $pkg.$f failed (crasher under testdata/fuzz/$f)"
          fi
          echo "::endgroup::"
        done
        pending=() ;;
    esac
  done < <(cd "$m" && go test -list '^Fuzz' ./... 2>/dev/null)
done
echo "$n fuzz targets, ${#failed[@]} failed ${failed[*]}"
[ ${#failed[@]} -eq 0 ]
