#!/bin/bash
# Write-side ABAC on real AWS (DECISIONS.md D18, STPA R-S7; the SeaweedFS
# demonstration is deploy/iam/seaweedfs_abac.sh). Three phases:
#
#   sts   from the operator's shell: the edge policy assumed with session tags
#         cluster=<CLUSTER>-sts1 / -sts2 / none (two clusters and a session
#         without a tag, from one role), then the consumer/GC role; checks the
#         cross-cluster denials, no delete, no control write, create-only via
#         the bucket policy, and that the consumer and GC still work.
#   pods  the same matrix from pods under the real credential paths: IRSA with
#         a role tagged cluster=<CLUSTER> (abac-self), one tagged
#         <CLUSTER>-other (abac-other), one untagged (abac-untagged), and Pod
#         Identity with the automatic eks-cluster-name session tag (abac-podid).
#   all   both (default).
#
#   RUN=v1 BUCKET=... REGION=us-east-1 [CLUSTER=otel-val-v1 NAME=otelval-v1 AWSCLI_IMAGE=amazon/aws-cli:2.31.0] eks/abac.sh [sts|pods|all]
#
# The bucket policy (deploy/iam/bucket-policy.json: slots create-only,
# control objects CAS) is applied only to a bucket this tooling created and
# that has no policy yet (a bucket policy replaces the whole policy); down.sh
# removes it. Otherwise its rows are reported as INFO, not checked.
# Output: $STATE/abac-*.txt; results.tsv lines abac.*.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need aws kubectl python3
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
CLUSTER=${CLUSTER:-otel-val-$RUN}
AWSCLI_IMAGE=${AWSCLI_IMAGE:-amazon/aws-cli:latest}
PHASE=${1:-all}
export KUBECONFIG=$STATE/kubeconfig
# shellcheck source=/dev/null
. "$STATE/iam.env" || die "run eks/iam.sh first"
export BUCKET ROOT=$VPREFIX/edge AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION
M=$VALIDATION_DIR/eks/abac_matrix.sh

# The bucket policy.
BUCKET_POLICY=off
cur=$(aws s3api get-bucket-policy --bucket "$BUCKET" --query Policy --output text 2> /dev/null || true)
ours=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])), sort_keys=True))' "$STATE/iam/bucket-policy.json")
norm() { python3 -c 'import json,sys; print(json.dumps(json.loads(sys.stdin.read()), sort_keys=True))' 2> /dev/null; }
if [ -n "$cur" ] && [ "$(printf '%s' "$cur" | norm)" = "$ours" ]; then
  BUCKET_POLICY=on
elif [ -z "$cur" ] && aws s3api get-bucket-tagging --bucket "$BUCKET" --query "TagSet[?Key=='created-by'].Value" --output text 2> /dev/null | grep -q otel-chdb-validation; then
  aws s3api put-bucket-policy --bucket "$BUCKET" --policy "file://$STATE/iam/bucket-policy.json" || die "put-bucket-policy"
  created bucket-policy "$BUCKET"
  BUCKET_POLICY=on
  sleep 10
else
  log "bucket $BUCKET has another policy or is not this tooling's: the bucket-policy rows are INFO only"
fi
export BUCKET_POLICY
result abac.bucket_policy "$BUCKET_POLICY"

# assume ROLE [cluster-tag]: prints env assignments for a session.
assume() {
  local tags=(); [ -n "${2:-}" ] && tags=(--tags "Key=cluster,Value=$2")
  env -u AWS_PROFILE aws sts assume-role --role-arn "$1" --role-session-name "abac-${2:-untagged}" ${tags[@]+"${tags[@]}"} \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text \
  | awk '{ printf "AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_SESSION_TOKEN=%s\n", $1, $2, $3 }'
}
# shellcheck disable=SC2086 # the assignments are NAME=VALUE words
as() { local c=$1; shift; env -u AWS_PROFILE $c "$@"; }   # as "<assignments>" cmd...

if [ "$PHASE" = sts ] || [ "$PHASE" = all ]; then
  A=$CLUSTER-sts1; B=$CLUSTER-sts2
  CA=$(assume "$R_STS" "$A") && CB=$(assume "$R_STS" "$B") && CU=$(assume "$R_STS") && CC=$(assume "$R_CONS") || die "assume-role failed (the caller needs sts:AssumeRole and sts:TagSession)"
  {
    # shellcheck disable=SC2086 # the assignments are words
    as "$CA" bash "$M" "$A" "$B" sts1
    as "$CB" bash "$M" "$B" "$A" sts2
    as "$CU" env EXPECT_DENY_ALL=1 bash "$M" "$A" "$B" sts-untagged
    # The consumer and GC against sts1's slots (abac_matrix.sh left slot 0; slot 1 is free).
    R=$(as "$CA" aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "$ROOT/$A/abac-sts1/traces/" --query 'Contents[0].Key' --output text)
    lane=${R%/*}
    code() { # creds cmd...: ok | 403 | 412 | err
      local creds=$1 out; shift
      # shellcheck disable=SC2086 # creds are NAME=VALUE words
      if out=$(env -u AWS_PROFILE $creds "$@" 2>&1 > /dev/null); then echo ok; return; fi
      case "$out" in *AccessDenied*|*"(403)"*) echo 403;; *PreconditionFailed*|*"(412)"*) echo 412;; *) echo "err";; esac
    }
    row() { if [ "$2" = "$3" ]; then echo "PASS [consumer] $1: $3"; else echo "FAIL [consumer] $1: $3 (want $2)"; fi; }
    tmp=$(mktemp); echo '{}' > "$tmp"
    row "HEADs a cluster's slot"                 ok  "$(code "$CC" aws s3api head-object --bucket "$BUCKET" --key "$R")"
    row "tombstones the next slot (create-only)" ok  "$(code "$CC" aws s3api put-object --bucket "$BUCKET" --key "$lane/$(printf %020d 1).parquet" --body "$tmp" --if-none-match '*')"
    row "creates a lease (create-only)"          ok  "$(code "$CC" aws s3api put-object --bucket "$BUCKET" --key "$ROOT/_consumer/lease/$A/abac-sts1/traces.json" --body "$tmp" --if-none-match '*')"
    if [ "$BUCKET_POLICY" = on ]; then
      row "plain PUT of a lease (bucket policy: CAS)" 403 "$(code "$CC" aws s3api put-object --bucket "$BUCKET" --key "$ROOT/_consumer/lease/$A/abac-sts1/traces.json" --body "$tmp")"
      row "plain PUT of a data slot"                  403 "$(code "$CC" aws s3api put-object --bucket "$BUCKET" --key "$lane/$(printf %020d 7).parquet" --body "$tmp")"
    fi
    row "writes gc.json"                         ok  "$(code "$CC" aws s3api put-object --bucket "$BUCKET" --key "$ROOT/_consumer/gc.json" --body "$tmp" --if-none-match '*')"
    row "GC deletes an ingested slot"            ok  "$(code "$CC" aws s3api delete-object --bucket "$BUCKET" --key "$R")"
    rm -f "$tmp"
  } 2>&1 | tee "$STATE/abac-sts.txt"
fi

if [ "$PHASE" = pods ] || [ "$PHASE" = all ]; then
  kubectl -n otel-validate create configmap abac-matrix --from-file=abac_matrix.sh="$M" --dry-run=client -o yaml | kubectl apply -f - > /dev/null
  # Pod Identity association for abac-podid (the ledger remembers it).
  if [ -z "$(aws eks list-pod-identity-associations --region "$REGION" --cluster-name "$CLUSTER" --namespace otel-validate \
        --service-account abac-podid --query 'associations[0].associationId' --output text | grep -v None)" ]; then
    id=$(aws eks create-pod-identity-association --region "$REGION" --cluster-name "$CLUSTER" --namespace otel-validate \
      --service-account abac-podid --role-arn "$R_PODID" --query association.associationId --output text) || die "pod identity association"
    created pod-identity-association "$id" "$CLUSTER"
  fi
  job() { # name serviceaccount role-annotation self other [deny-all]
    cat <<EOF
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $2
  namespace: otel-validate
  annotations: {$( [ -n "$3" ] && echo "eks.amazonaws.com/role-arn: \"$3\", eks.amazonaws.com/sts-regional-endpoints: \"true\"" )}
---
apiVersion: batch/v1
kind: Job
metadata: {name: $1, namespace: otel-validate, labels: {app: abac}}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      serviceAccountName: $2
      restartPolicy: Never
      volumes: [{name: m, configMap: {name: abac-matrix}}]
      containers:
        - name: m
          image: $AWSCLI_IMAGE
          command: ["bash", "-c", "aws --version; bash /m/abac_matrix.sh $4 $5 $1; echo exit=\$?"]
          env:
            - {name: BUCKET, value: "$BUCKET"}
            - {name: ROOT, value: "$ROOT"}
            - {name: AWS_REGION, value: "$REGION"}
            - {name: BUCKET_POLICY, value: "$BUCKET_POLICY"}
            - {name: EXPECT_DENY_ALL, value: "${6:-0}"}
          volumeMounts: [{name: m, mountPath: /m}]
EOF
  }
  {
    job abac-self abac-self "$R_EDGE" "$CLUSTER" "$CLUSTER-other"
    job abac-other abac-other "$R_OTHER" "$CLUSTER-other" "$CLUSTER"
    job abac-untagged abac-untagged "$R_UNTAGGED" "$CLUSTER" "$CLUSTER-other" 1
    job abac-podid abac-podid "" "$CLUSTER" "$CLUSTER-other"
  } > "$STATE/abac-jobs.yaml"
  kubectl -n otel-validate delete job -l app=abac --ignore-not-found > /dev/null
  kubectl apply -f "$STATE/abac-jobs.yaml" > /dev/null
  created k8s-manifest "$STATE/abac-jobs.yaml"
  for j in abac-self abac-other abac-untagged abac-podid; do
    kubectl -n otel-validate wait --for=condition=complete "job/$j" --timeout=300s > /dev/null 2>&1 \
      || kubectl -n otel-validate wait --for=condition=failed "job/$j" --timeout=5s > /dev/null 2>&1
    kubectl -n otel-validate logs "job/$j"
  done 2>&1 | tee "$STATE/abac-pods.txt"
fi
cat "$STATE"/abac-*.txt 2> /dev/null | grep -E '^(FAIL|RESULT)' || true
result abac.fails "$(cat "$STATE"/abac-*.txt 2> /dev/null | grep -c '^FAIL')"
result abac.free_slot_head "$(cat "$STATE"/abac-*.txt 2> /dev/null | grep -o 'free-slot-head=[0-9a-z]*' | sort | uniq -c | tr '\n' ' ')"
