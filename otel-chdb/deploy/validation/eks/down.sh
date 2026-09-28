#!/bin/bash
# Remove what this run created, newest first, from its ledger
# ($STATE/ledger.tsv): Kubernetes objects, Pod Identity associations, the
# S3 endpoint, IAM roles and users, ECR repositories, bucket metrics and the
# bucket policy it applied, the objects under the run's prefix, and the EKS
# cluster. A bucket is never deleted (not even one this run created: it is
# reported, with its lifecycle rule expiring validation/); anything that
# existed before the run is not in the ledger and is not touched.
#
#   RUN=v1 [BUCKET=... REGION=us-east-1 KEEP_OBJECTS=1 YES=1] eks/down.sh
#
# Copy $STATE (results, reports, rendered manifests) somewhere safe first:
# down.sh leaves it in place, but the cluster's logs go with the cluster.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need aws kubectl
REGION=${REGION:-us-east-1}
export KUBECONFIG=$STATE/kubeconfig
[ -s "$LEDGER" ] || { log "ledger empty: nothing to remove"; exit 0; }
log "ledger of run $RUN:"; ledger_rev | sed 's/^/  /' >&2
confirm "remove everything above (the bucket stays)?" || die "not confirmed"
# del_or_gone DELETE EXISTS: true if DELETE succeeded or the thing no longer exists.
# (Both are single commands of words without spaces inside: ids and names.)
del_or_gone() {
  # shellcheck disable=SC2086
  $1 > /dev/null 2>&1 && return 0
  # shellcheck disable=SC2086
  ! $2 > /dev/null 2>&1
}
done_=$STATE/ledger.removed
touch "$done_"
ledger_rev | while IFS=$'\t' read -r type id extra; do
  grep -qxF "$type	$id	$extra" "$done_" && continue
  ok=1
  case $type in
    k8s-manifest)   kubectl delete -f "$id" --ignore-not-found --wait=false > /dev/null 2>&1 || ok=0 ;;
    k8s-namespace)  kubectl delete ns "$id" --ignore-not-found --timeout=10m > /dev/null 2>&1 || ok=0 ;;
    k8s-storageclass) kubectl delete sc "$id" --ignore-not-found > /dev/null 2>&1 || ok=0 ;;
    pod-identity-association)
                    del_or_gone "aws eks delete-pod-identity-association --region $REGION --cluster-name $extra --association-id $id" \
                      "aws eks describe-pod-identity-association --region $REGION --cluster-name $extra --association-id $id" || ok=0 ;;
    vpc-endpoint)   aws ec2 delete-vpc-endpoints --region "$extra" --vpc-endpoint-ids "$id" > /dev/null || ok=0 ;;
    iam-role)
      for p in $(aws iam list-role-policies --role-name "$id" --query 'PolicyNames[]' --output text 2> /dev/null); do
        aws iam delete-role-policy --role-name "$id" --policy-name "$p"
      done
      del_or_gone "aws iam delete-role --role-name $id" "aws iam get-role --role-name $id" || ok=0 ;;
    iam-user)
      for k in $(aws iam list-access-keys --user-name "$id" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2> /dev/null); do
        aws iam delete-access-key --user-name "$id" --access-key-id "$k"
      done
      for p in $(aws iam list-user-policies --user-name "$id" --query 'PolicyNames[]' --output text 2> /dev/null); do
        aws iam delete-user-policy --user-name "$id" --policy-name "$p"
      done
      del_or_gone "aws iam delete-user --user-name $id" "aws iam get-user --user-name $id" || ok=0
      rm -f "$STATE/consumer-key.json" ;;
    ecr-repo)       del_or_gone "aws ecr delete-repository --region $extra --repository-name $id --force" \
                      "aws ecr describe-repositories --region $extra --repository-names $id" || ok=0 ;;
    bucket-metrics) aws s3api delete-bucket-metrics-configuration --bucket "$id" --id "$extra" || ok=0 ;;
    bucket-policy)  # only if it is still the policy this run applied
      cur=$(aws s3api get-bucket-policy --bucket "$id" --query Policy --output text 2> /dev/null || true)
      if [ -n "$cur" ] && python3 -c 'import json,sys; a=json.loads(sys.argv[1]); b=json.load(open(sys.argv[2])); sys.exit(a != b)' "$cur" "$STATE/iam/bucket-policy.json"; then
        aws s3api delete-bucket-policy --bucket "$id" || ok=0
      else
        log "bucket policy on $id changed since this run applied it: left alone"
      fi ;;
    bucket)
      [ "${KEEP_OBJECTS:-0}" = 1 ] || BUCKET=$id delete_run_objects "$id" "" || ok=0
      log "bucket $id was created by this run and is KEPT (never deleted here): its lifecycle rule expires validation/; delete it yourself if wanted" ;;
    eks-cluster)    eksctl delete cluster --region "$extra" --name "$id" --wait > /dev/null || ok=0 ;;
    *)              log "unknown ledger type $type ($id): left alone"; ok=0 ;;
  esac
  if [ "$ok" = 1 ]; then printf '%s\t%s\t%s\n' "$type" "$id" "$extra" >> "$done_"; log "removed $type $id"; else log "NOT removed: $type $id (rerun down.sh)"; fi
done
# Objects under the run prefix in a bucket this run did not create.
if [ -n "${BUCKET:-}" ] && [ "${KEEP_OBJECTS:-0}" != 1 ] && ! was_created bucket "$BUCKET"; then
  delete_run_objects "$BUCKET" ""
fi
log "done; state kept in $STATE"
