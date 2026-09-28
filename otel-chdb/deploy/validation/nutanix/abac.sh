#!/bin/bash
# Write-side ABAC on Nutanix Objects (nutanix.md §4). Objects has no STS and no
# session tags, so the design's fallback is one Objects user (key) per cluster
# and a bucket policy scoping each to its prefix (deploy/README.md §Write-side
# ABAC; the SeaweedFS stand-in: deploy/iam/seaweedfs_abac.sh). This finds out
# whether Objects can express and enforce that.
#
#   RUN=v1 ENDPOINT=https://objects.example BUCKET=... CA_BUNDLE=ca.pem \
#   C1_KEY= C1_SECRET= C2_KEY= C2_SECRET= CONS_KEY= CONS_SECRET= \
#   [P1=... P2=... PCONS=... ALLOW_BUCKET_POLICY=1] nutanix/abac.sh
#
# C1/C2: two Objects users standing for two clusters (clusters nx-c1, nx-c2);
# CONS: the consumer's user. P1/P2/PCONS: how Objects names those users in a
# bucket policy's Principal (ask the Objects admin; the AWS form is
# arn:aws:iam::<account>:user/<name>). Without ALLOW_BUCKET_POLICY=1, or if the
# bucket already has a policy, no policy is applied and the matrix records
# what the bucket's existing access model allows (with plain bucket shares:
# every user writes everywhere, and the rows say so). An applied policy is
# removed at the end, and the previous state (none) restored.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need aws python3
: "${ENDPOINT:?}" "${BUCKET:?}" "${CA_BUNDLE:?}" "${C1_KEY:?}" "${C1_SECRET:?}" "${C2_KEY:?}" "${C2_SECRET:?}"
export S3_ENDPOINT=$ENDPOINT CA_BUNDLE AWS_REGION=${REGION:-us-east-1}
export BUCKET ROOT=$VPREFIX/edge AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
OUT=$STATE/nutanix; mkdir -p "$OUT"
M=$VALIDATION_DIR/eks/abac_matrix.sh
as() { local k=$1 s=$2; shift 2; env -u AWS_PROFILE -u AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID="$k" AWS_SECRET_ACCESS_KEY="$s" "$@"; }

BUCKET_POLICY=off
applied=0
if [ "${ALLOW_BUCKET_POLICY:-0}" = 1 ]; then
  : "${P1:?}" "${P2:?}" "${PCONS:?}" "${CONS_KEY:?}" "${CONS_SECRET:?}"
  if as "$CONS_KEY" "$CONS_SECRET" aws --endpoint-url "$ENDPOINT" --ca-bundle "$CA_BUNDLE" s3api get-bucket-policy --bucket "$BUCKET" > "$OUT/policy-before.json" 2> "$OUT/policy-before.err"; then
    log "bucket $BUCKET already has a policy ($OUT/policy-before.json): not replaced; policy rows are INFO"
  else
    render "$VALIDATION_DIR/nutanix/bucket-policy.tmpl.json" "$OUT/bucket-policy.json" BUCKET="$BUCKET" ROOT="$ROOT" \
      C1=nx-c1 C2=nx-c2 P1="$P1" P2="$P2" PCONS="$PCONS"
    if as "$CONS_KEY" "$CONS_SECRET" aws --endpoint-url "$ENDPOINT" --ca-bundle "$CA_BUNDLE" s3api put-bucket-policy \
         --bucket "$BUCKET" --policy "file://$OUT/bucket-policy.json" 2> "$OUT/put-policy.err"; then
      applied=1; BUCKET_POLICY=on; sleep 5
      trap 'as "$CONS_KEY" "$CONS_SECRET" aws --endpoint-url "$ENDPOINT" --ca-bundle "$CA_BUNDLE" s3api delete-bucket-policy --bucket "$BUCKET" && log "bucket policy removed"' EXIT
    else
      log "put-bucket-policy refused: $(cat "$OUT/put-policy.err")"
      result nutanix.bucket_policy "refused: $(head -c 300 "$OUT/put-policy.err")"
    fi
  fi
fi
export BUCKET_POLICY
result nutanix.bucket_policy_applied "$applied"
{
  as "$C1_KEY" "$C1_SECRET" bash "$M" nx-c1 nx-c2 nx-c1
  as "$C2_KEY" "$C2_SECRET" bash "$M" nx-c2 nx-c1 nx-c2
} 2>&1 | tee "$OUT/abac.txt"
result nutanix.abac "$(grep -c '^FAIL' "$OUT/abac.txt") failed rows; $(grep -o 'free-slot-head=[0-9a-z]*' "$OUT/abac.txt" | tr '\n' ' ')"
# The consumer removes what the matrix wrote (edges may not delete).
if [ -n "${CONS_KEY:-}" ]; then
  as "$CONS_KEY" "$CONS_SECRET" aws --endpoint-url "$ENDPOINT" --ca-bundle "$CA_BUNDLE" s3 rm --recursive --only-show-errors "s3://$BUCKET/$ROOT/"
fi
