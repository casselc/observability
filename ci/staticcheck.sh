#!/usr/bin/env bash
# staticcheck (honnef.co/go/tools, default checks) over every Go module the
# repository has (ci/go-modules.sh list all), one module at a time. A finding
# is fixed, or suppressed where it stands with a reason:
#   //lint:ignore CHECK reason        (one line)
#   //lint:file-ignore CHECK reason   (a file: generated-like mirrors, build-tagged helpers)
# ci.yml's job `staticcheck` runs it on every push (ci/README.md).
#
#   ci/staticcheck.sh [GROUP]    GROUP as for ci/go-modules.sh (default all)
#
# Env: STATICCHECK (default: staticcheck on PATH). Exit status non-zero if any
# module has a finding or does not type-check.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root" || exit 1
bin=${STATICCHECK:-staticcheck}
"$bin" -version
failed=()
n=0
for m in $(ci/go-modules.sh list "${1:-all}"); do
  n=$((n + 1))
  echo "::group::staticcheck $m"
  if ! (cd "$m" && "$bin" ./...); then
    failed+=("$m")
    echo "::error::staticcheck: findings in $m"
  fi
  echo "::endgroup::"
done
echo "$n modules, ${#failed[@]} with findings ${failed[*]}"
[ ${#failed[@]} -eq 0 ]
