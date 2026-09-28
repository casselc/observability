# shellcheck shell=bash
# Shared helpers for the validation scripts. Sourced, not run.
#
# Conventions every script here follows:
# - parameters come from the environment (each script's header lists them);
#   nothing is hard-coded to one account, cluster, bucket or site;
# - idempotent: a second run skips what exists, and says so;
# - a ledger ($STATE/ledger.tsv) records every cloud or cluster resource a
#   script CREATED; the down scripts delete only ledger entries, newest
#   first, so nothing that existed before is ever removed;
# - objects are deleted only under this run's prefix ($VPREFIX, which always
#   contains "/validation/"); a bucket is never deleted by anything here.
#
#   RUN      run id (default: vYYYYMMDD; set it once and reuse it for every step)
#   STATE    state directory (default: $HOME/.otel-validation/$RUN)

set -u -o pipefail
VALIDATION_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd "$VALIDATION_DIR/.." && pwd)
# shellcheck disable=SC2034 # used by the scripts that source this
OTEL_CHDB=$(cd "$DEPLOY_DIR/.." && pwd)
RUN=${RUN:-v$(date -u +%Y%m%d)}
STATE=${STATE:-$HOME/.otel-validation/$RUN}
mkdir -p "$STATE"
LEDGER=$STATE/ledger.tsv
touch "$LEDGER"

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$STATE/log.txt" >&2; }
die() { log "FATAL: $*"; exit 1; }
need() { local c; for c in "$@"; do command -v "$c" > /dev/null 2>&1 || die "missing tool: $c"; done; }

# created TYPE ID [EXTRA]: record a resource this run created (once).
created() { grep -qxF "$1	$2	${3:-}" "$LEDGER" || printf '%s\t%s\t%s\n' "$1" "$2" "${3:-}" >> "$LEDGER"; }
# was_created TYPE ID: did this run create it?
was_created() { awk -F'\t' -v t="$1" -v i="$2" '$1 == t && $2 == i { f = 1 } END { exit !f }' "$LEDGER"; }
# ledger_rev: the ledger, newest first.
ledger_rev() { tac "$LEDGER"; }

# confirm WHAT: ask unless YES=1 (for anything that costs money or changes a shared resource).
confirm() {
  [ "${YES:-0}" = 1 ] && return 0
  local a; read -r -p "$* [y/N] " a < /dev/tty || return 1
  [ "$a" = y ] || [ "$a" = Y ]
}

# render SRC DST KEY=VALUE...: copy a template, replacing each {{KEY}}.
render() {
  local src=$1 dst=$2; shift 2
  python3 - "$src" "$dst" "$@" <<'PY'
import sys
src, dst, *kv = sys.argv[1:]
s = open(src).read()
for x in kv:
    k, v = x.split("=", 1)
    s = s.replace("{{" + k + "}}", v)
if "{{" in s:
    left = sorted({s[i:s.index("}}", i) + 2] for i in range(len(s)) if s.startswith("{{", i)})
    sys.exit(f"render {src}: unreplaced {left}")
open(dst, "w").write(s)
PY
}

# awss3 ...: the aws CLI for the store under test (S3_ENDPOINT and CA_BUNDLE
# for Nutanix Objects; nothing for AWS).
awss3() {
  local extra=()
  [ -n "${S3_ENDPOINT:-}" ] && extra+=(--endpoint-url "$S3_ENDPOINT")
  [ -n "${CA_BUNDLE:-}" ] && extra+=(--ca-bundle "$CA_BUNDLE")
  aws "${extra[@]}" "$@"
}

# The run's object prefix inside the bucket. Every object a script writes is
# below it; cleanup refuses anything that is not.
VPREFIX=${VPREFIX:-validation/$RUN}
case "$VPREFIX" in validation/*) ;; *) die "VPREFIX must start with validation/ (got $VPREFIX)";; esac

# delete_run_objects BUCKET SUBPREFIX: delete objects under $VPREFIX/SUBPREFIX
# only. Never the bucket, never outside the run's prefix.
delete_run_objects() {
  local bucket=$1 sub=${2:-}
  local p="$VPREFIX/${sub}"
  case "$p" in validation/*/*|validation/*) ;; *) die "refusing to delete under $p";; esac
  [ -n "$bucket" ] || die "delete_run_objects: no bucket"
  log "deleting objects under s3://$bucket/$p (the bucket stays)"
  awss3 s3 rm --recursive --only-show-errors "s3://$bucket/$p"
}

# result KEY VALUE: append a measured value to the run's results file, which
# results/TEMPLATE.md's rows are filled from.
result() { printf '%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$1" "$2" | tee -a "$STATE/results.tsv"; }
