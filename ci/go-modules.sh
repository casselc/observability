#!/usr/bin/env bash
# Discovers the repository's Go modules and runs go vet / go test in each.
#
#   ci/go-modules.sh list  GROUP        # print the module directories
#   ci/go-modules.sh vet   GROUP        # go vet ./... in each
#   ci/go-modules.sh test  GROUP        # go test ./... in each
#
# GROUP:
#   all      every module with our code (otel-chdb/chdb-go/lib/* are excluded:
#            platform stubs that only embed a libchdb release blob)
#   fast     all minus chdb and slow: runs on every push and PR
#   chdb     modules whose tests load libchdb (need CHDB_LIB_PATH), plus
#            parquetgo/compare, whose correctness test is gated on it
#   slow     long model-checking suites (nightly)
#   DIR      one module, by its path from the repository root
#
# Env: GO_TEST_FLAGS (default "-count=1 -timeout 20m"), RACE=1 adds -race
# except in the modules listed in NORACE (space-separated, relative paths),
# LOG_DIR (default: none) writes one log per module there as well.
# Each module's result is printed; the exit status is non-zero if any failed.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root" || exit 1

CHDB="otel-chdb/chdb-go otel-chdb/chdbexporter otel-chdb/parquetgo/compare"
SLOW="otel-chdb/parquetgo/modelcheck"

# Tests skipped in one module, with the reason; each skip is announced as a
# warning in the job's log. Remove an entry when its reason goes away.
declare -A SKIP=(
  # s3cas probes a store's conditional operations. Against SeaweedFS (4.47,
  # the CI store) concurrent conditional CompleteMultipartUpload is not
  # atomic: UPSTREAM_ISSUES.md U4, model/S3NATIVE.md section 2.
  [otel-chdb/s3cas]='^TestMultipart(Create|CAS)Race$'
)

all() {
  find otel-chdb quintgo -name go.mod \
    -not -path 'otel-chdb/chdb-go/lib/*' -not -path '*/.upstream/*' -not -path '*/_build/*' \
    -printf '%h\n' | sort
}
in_list() { case " $2 " in *" $1 "*) return 0 ;; esac; return 1; }

modules() {
  case "$1" in
    all) all ;;
    chdb) for m in $CHDB; do echo "$m"; done ;;
    slow) for m in $SLOW; do echo "$m"; done ;;
    fast) all | while read -r m; do
            if [ "$m" = otel-chdb/parquetgo/compare ]; then echo "$m"; continue; fi  # also fast: skips its libchdb test
            in_list "$m" "$CHDB $SLOW" || echo "$m"
          done ;;
    *) if [ -f "$1/go.mod" ]; then echo "$1"; else echo "unknown group or module: $1" >&2; exit 2; fi ;;
  esac
}

cmd=${1:?list|vet|test}; group=${2:-fast}
[ "$cmd" = list ] && { modules "$group"; exit; }

read -r -a flags <<< "${GO_TEST_FLAGS:--count=1 -timeout 20m}"
failed=()
[ -n "${LOG_DIR:-}" ] && mkdir -p "$LOG_DIR"
for m in $(modules "$group"); do
  args=()
  case "$cmd" in
    vet) args=(vet ./...) ;;
    test)
      args=(test "${flags[@]}")
      if [ -n "${RACE:-}" ] && ! in_list "$m" "${NORACE:-}"; then args+=(-race); fi
      if [ -n "${SKIP[$m]:-}" ]; then
        args+=(-skip "${SKIP[$m]}")
        echo "::warning::$m: skipping ${SKIP[$m]} (known store limitation, see ci/go-modules.sh)"
      fi
      args+=(./...) ;;
    *) echo "unknown command: $cmd" >&2; exit 2 ;;
  esac
  echo "::group::go ${args[*]} ($m)"
  start=$(date +%s)
  if [ -n "${LOG_DIR:-}" ]; then
    (cd "$m" && go "${args[@]}") 2>&1 | tee "$LOG_DIR/$(echo "$m" | tr / _).$cmd.log"
    rc=${PIPESTATUS[0]}
  else
    (cd "$m" && go "${args[@]}"); rc=$?
  fi
  echo "::endgroup::"
  echo "$m: go $cmd exit $rc in $(( $(date +%s) - start ))s"
  [ "$rc" -eq 0 ] || failed+=("$m")
done
if [ ${#failed[@]} -gt 0 ]; then
  for m in "${failed[@]}"; do echo "::error::go $cmd failed in $m"; done
  exit 1
fi
