#!/bin/bash
# The write-side ABAC matrix for ONE identity (the ambient credentials), as
# deploy/iam/seaweedfs_abac.sh proves it on SeaweedFS: an edge of cluster
# SELF creates and reads slots under its own cluster only, never deletes,
# never writes a control object, and a free slot of its own is 404 (not 403).
# Runs wherever the aws CLI (v2.22+, for --if-none-match) has credentials: in
# a pod under IRSA or Pod Identity (abac.sh pods), in the operator's shell
# under an assumed role with session tags (abac.sh sts), or with a Nutanix
# per-cluster key (nutanix/abac.sh).
#
#   BUCKET=... ROOT=validation/v1/edge [S3_ENDPOINT=https://... CA_BUNDLE=ca.pem] abac_matrix.sh SELF OTHER [TAG]
#
# EXPECT_DENY_ALL=1 for credentials without a cluster tag: every row wants 403.
# SELF: the cluster these credentials belong to; OTHER: another cluster's
# prefix (it need not exist). TAG names the rows in the output. Exit status:
# the number of failed rows. `HEAD free slot` is reported as 404 or 403
# separately (it is the question behind AMBIGUITY.md S5), and does not count
# as a failure of the boundary.
set -u
: "${BUCKET:?}" "${ROOT:?}"
SELF=${1:?SELF}; OTHER=${2:?OTHER}; TAG=${3:-$SELF}
EP=(); [ -n "${S3_ENDPOINT:-}" ] && EP+=(--endpoint-url "$S3_ENDPOINT"); [ -n "${CA_BUNDLE:-}" ] && EP+=(--ca-bundle "$CA_BUNDLE")
R=$(date -u +%Y%m%dT%H%M%S.000Z)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
body=$(mktemp); echo "abac $TAG" > "$body"; trap 'rm -f "$body"' EXIT
slot() { printf '%s/%s/abac-%s/traces/%s/%020d.parquet' "$ROOT" "$1" "$TAG" "$R" "$2"; }

# rc CMD...: ok | 403 | 404 | 409 | 412 | err:<code>
rc() {
  local out
  if out=$("$@" 2>&1 > /dev/null); then echo ok; return; fi
  case "$out" in
    *"Unknown options"*) echo "err:aws-cli-too-old"; echo "aws CLI does not know an option: need v2.22+ ($out)" >&2;;
    *"(AccessDenied)"*|*"(403)"*|*Forbidden*) echo 403;;
    *"(PreconditionFailed)"*|*"(412)"*) echo 412;;
    *"(404)"*|*NoSuchKey*|*"Not Found"*|*NoSuchBucket*) echo 404;;
    *ConditionalRequestConflict*|*"(409)"*) echo 409;;
    *) echo "err:$(printf '%s' "$out" | grep -o '([A-Za-z0-9]*)' | head -1 | tr -d '()')";;
  esac
}
put() { # key [if-none-match]
  local c=(); [ -n "${2:-}" ] && c=(--if-none-match "$2")
  rc aws ${EP[@]+"${EP[@]}"} s3api put-object --bucket "$BUCKET" --key "$1" --body "$body" ${c[@]+"${c[@]}"}
}
head_() { rc aws ${EP[@]+"${EP[@]}"} s3api head-object --bucket "$BUCKET" --key "$1"; }
del() { rc aws ${EP[@]+"${EP[@]}"} s3api delete-object --bucket "$BUCKET" --key "$1"; }
lst() { rc aws ${EP[@]+"${EP[@]}"} s3api list-objects-v2 --bucket "$BUCKET" --prefix "$1" --max-keys 1; }

fails=0
# EXPECT_DENY_ALL=1: an identity without a cluster tag; every row wants 403.
check() { # name want got
  local want=$2; [ "${EXPECT_DENY_ALL:-0}" = 1 ] && want=403
  if [ "$want" = "$3" ]; then echo "PASS [$TAG] $1: $3"; else echo "FAIL [$TAG] $1: $3 (want $want)"; fails=$((fails + 1)); fi
}
info() { echo "INFO [$TAG] $1: $2"; }

echo "identity: $(aws ${EP[@]+"${EP[@]}"} sts get-caller-identity --query Arn --output text 2>/dev/null || echo 'no STS (static key)')"
check "creates its own slot (If-None-Match: *)"      ok  "$(put "$(slot "$SELF" 0)" '*')"
check "again on the same slot: create-only"           412 "$(put "$(slot "$SELF" 0)" '*')"
check "HEADs its own slot"                            ok  "$(head_ "$(slot "$SELF" 0)")"
free=$(head_ "$(slot "$SELF" 1)")
info  "HEAD of its own next, free slot (404 = free; 403 = the ListBucket grant does not cover a HEAD)" "$free"
check "lists its own prefix"                          ok  "$(lst "$ROOT/$SELF/")"
check "creates a slot under cluster $OTHER"           403 "$(put "$(slot "$OTHER" 0)" '*')"
check "HEADs a key under cluster $OTHER"              403 "$(head_ "$(slot "$OTHER" 0)")"
check "lists cluster $OTHER's prefix"                 403 "$(lst "$ROOT/$OTHER/")"
check "writes a consumer lease"                       403 "$(put "$ROOT/_consumer/lease/$SELF/abac-$TAG/traces.json" '*')"
check "writes the watermark"                          403 "$(put "$ROOT/_consumer/watermark.json" '*')"
check "writes a key under _x (an underscore cluster)" 403 "$(put "$ROOT/_x/abac-$TAG/traces/$R/$(printf %020d 0).parquet" '*')"
check "deletes its own slot"                          403 "$(del "$(slot "$SELF" 0)")"
check "deletes a key under cluster $OTHER"            403 "$(del "$(slot "$OTHER" 0)")"
bp=$(put "$(slot "$SELF" 2)" '')
case "${BUCKET_POLICY:-unknown}" in
  on) check "plain PUT of a slot (bucket policy: create-only)" 403 "$bp";;
  *)  info  "plain PUT of a slot (no bucket policy applied: allowed)" "$bp";;
esac
echo "RESULT [$TAG] free-slot-head=$free fails=$fails"
exit "$fails"
