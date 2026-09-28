#!/bin/bash
# IAM for the EKS validation: roles for IRSA and Pod Identity with the ABAC
# policies of deploy/iam/, the session-tag roles the ABAC matrix assumes from
# the operator's shell, the acceptance kit's roles (with, without and with a
# prefix-scoped s3:ListBucket), and one IAM user for the consumer.
# Idempotent: an existing role or user is reused only if this tooling tagged
# it; its inline policies are rewritten from the templates on every run.
#
#   RUN=v1 BUCKET=... REGION=us-east-1 [CLUSTER=otel-val-v1 NAME=otelval-v1] eks/iam.sh
#
# Why a user: `consume` takes static keys only (--key/--secret, which it also
# passes into ClickHouse's s3()); there is no chain, IRSA or session token in
# it yet (eks-aws.md §0). The key goes into the Secret otel-validate/consumer-s3
# and $STATE/consumer-key.json (mode 600); down.sh deletes both.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need aws kubectl python3
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
CLUSTER=${CLUSTER:-otel-val-$RUN}
NAME=${NAME:-otelval-$RUN}
export KUBECONFIG=$STATE/kubeconfig
ACCOUNT=$(aws sts get-caller-identity --query Account --output text) || die "no AWS credentials"
ISSUER=$(aws eks describe-cluster --region "$REGION" --name "$CLUSTER" --query cluster.identity.oidc.issuer --output text | sed 's#^https://##')
ROOT=$VPREFIX/edge
ENTITIES=$VPREFIX/entities
P=$STATE/iam
mkdir -p "$P"
TAGS="Key=created-by,Value=otel-chdb-validation Key=run,Value=$RUN"

# policy SRC OUT [TAGKEY]: a deploy/iam/ template with this run's bucket and prefixes.
policy() {
  python3 - "$1" "$2" "$BUCKET" "$ROOT" "$ENTITIES" "${3:-cluster}" <<'PY' || die "policy $2"
import json, sys
src, dst, bucket, root, ents, tag = sys.argv[1:]
s = open(src).read().replace("arn:aws:s3:::BUCKET", "arn:aws:s3:::" + bucket)
for a, b in ((":::%s/ROOT" % bucket, ":::%s/%s" % (bucket, root)), (":::%s/ENTITIES" % bucket, ":::%s/%s" % (bucket, ents)),
             ('"ROOT/', '"%s/' % root), ('"ROOT"', '"%s"' % root), ('"ENTITIES/', '"%s/' % ents), ('"ENTITIES"', '"%s"' % ents),
             ("aws:PrincipalTag/cluster", "aws:PrincipalTag/" + tag)):
    s = s.replace(a, b)
json.loads(s)
for w in ("BUCKET", '"ROOT', "/ROOT", "ENTITIES"):
    if w in s:
        sys.exit(f"{src}: placeholder {w} left")
open(dst, "w").write(s)
PY
}
irsa_trust() { # namespace:serviceaccount[,namespace:serviceaccount...]
  python3 - "$ACCOUNT" "$ISSUER" "$1" <<'PY'
import json, sys
acct, iss, sas = sys.argv[1:]
subs = ["system:serviceaccount:" + s for s in sas.split(",")]
print(json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
  "Principal": {"Federated": f"arn:aws:iam::{acct}:oidc-provider/{iss}"},
  "Action": "sts:AssumeRoleWithWebIdentity",
  "Condition": {"StringEquals": {f"{iss}:aud": "sts.amazonaws.com"}, "StringLike": {f"{iss}:sub": subs}}}]}))
PY
}
podid_trust() { echo '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"pods.eks.amazonaws.com"},"Action":["sts:AssumeRole","sts:TagSession"]}]}'; }
account_trust() { echo "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::$ACCOUNT:root\"},\"Action\":[\"sts:AssumeRole\",\"sts:TagSession\"]}]}"; }

# role NAME TRUST [TAGS...] then policies as NAME=FILE: create or update.
role() {
  local r=$1 trust=$2; shift 2
  local tags=() pols=()
  for x in "$@"; do case "$x" in Key=*) tags+=("$x");; *) pols+=("$x");; esac; done
  if aws iam get-role --role-name "$r" > /dev/null 2>&1; then
    aws iam list-role-tags --role-name "$r" --query "Tags[?Key=='created-by'].Value" --output text | grep -q otel-chdb-validation \
      || die "role $r exists and is not this tooling's: pick another NAME"
    aws iam update-assume-role-policy --role-name "$r" --policy-document "$trust"
  else
    # shellcheck disable=SC2086 # TAGS is a list
    aws iam create-role --role-name "$r" --assume-role-policy-document "$trust" --max-session-duration 3600 \
      --tags $TAGS "${tags[@]}" > /dev/null || die "create-role $r"
    created iam-role "$r"
  fi
  [ ${#tags[@]} -gt 0 ] && aws iam tag-role --role-name "$r" --tags "${tags[@]}"
  for pf in "${pols[@]}"; do aws iam put-role-policy --role-name "$r" --policy-name "${pf%%=*}" --policy-document "file://${pf#*=}"; done
  echo "arn:aws:iam::$ACCOUNT:role/$r"
}

policy "$DEPLOY_DIR/iam/edge-publisher.json" "$P/edge-publisher.json"
policy "$DEPLOY_DIR/iam/edge-publisher.json" "$P/edge-publisher-podid.json" eks-cluster-name
policy "$DEPLOY_DIR/iam/entity-controller.json" "$P/entity-controller.json"
policy "$DEPLOY_DIR/iam/consumer.json" "$P/consumer.json"
policy "$DEPLOY_DIR/iam/gc.json" "$P/gc.json"
policy "$DEPLOY_DIR/iam/bucket-policy.json" "$P/bucket-policy.json"
# The acceptance kit's role: everything under the run's prefix, and s3:ListBucket
# three ways (eks-aws.md §3: head-missing).
python3 - "$BUCKET" "$VPREFIX" "$P" <<'PY'
import json, sys
b, pre, out = sys.argv[1:]
objs = {"Sid": "RunPrefix", "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        "Resource": f"arn:aws:s3:::{b}/{pre}/*"}
cfg = {"Sid": "BucketConfigReadOptional", "Effect": "Allow", "Action": ["s3:GetBucketVersioning", "s3:GetLifecycleConfiguration", "s3:GetBucketPolicy"],
       "Resource": f"arn:aws:s3:::{b}"}
lst = {"Sid": "List", "Effect": "Allow", "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads"], "Resource": f"arn:aws:s3:::{b}"}
plst = dict(lst, Condition={"StringLike": {"s3:prefix": f"{pre}/*"}})
for name, st in [("accept", [objs, lst, cfg]), ("accept-nolist", [objs, cfg]), ("accept-prefixlist", [objs, plst, cfg])]:
    json.dump({"Version": "2012-10-17", "Statement": st}, open(f"{out}/{name}.json", "w"), indent=1)
PY

EDGE_SAS=otel-edge:otap-publisher,otel-edge:otelcol-publisher
R_EDGE=$(role "$NAME-edge" "$(irsa_trust "$EDGE_SAS,otel-validate:abac-self")" "Key=cluster,Value=$CLUSTER" "edge-publisher=$P/edge-publisher.json")
R_OTHER=$(role "$NAME-edge-other" "$(irsa_trust otel-validate:abac-other)" "Key=cluster,Value=$CLUSTER-other" "edge-publisher=$P/edge-publisher.json")
R_UNTAGGED=$(role "$NAME-edge-untagged" "$(irsa_trust otel-validate:abac-untagged)" "edge-publisher=$P/edge-publisher.json")
R_PODID=$(role "$NAME-edge-podid" "$(podid_trust)" "edge-publisher=$P/edge-publisher-podid.json")
R_ENT=$(role "$NAME-entityctl" "$(irsa_trust otel-validate:entityctl)" "Key=cluster,Value=$CLUSTER" "entity-controller=$P/entity-controller.json")
R_STS=$(role "$NAME-abac-sts" "$(account_trust)" "edge-publisher=$P/edge-publisher.json")
R_CONS=$(role "$NAME-consumer-sts" "$(account_trust)" "consumer=$P/consumer.json" "gc=$P/gc.json")
R_ACC=$(role "$NAME-accept" "$(irsa_trust otel-validate:s3accept)" "accept=$P/accept.json")
R_ACCN=$(role "$NAME-accept-nolist" "$(irsa_trust otel-validate:s3accept-nolist)" "accept=$P/accept-nolist.json")
R_ACCP=$(role "$NAME-accept-prefixlist" "$(irsa_trust otel-validate:s3accept-prefixlist)" "accept=$P/accept-prefixlist.json")

# The consumer's user and key.
U=$NAME-consumer
if ! aws iam get-user --user-name "$U" > /dev/null 2>&1; then
  # shellcheck disable=SC2086
  aws iam create-user --user-name "$U" --tags $TAGS > /dev/null || die "create-user $U"
  created iam-user "$U"
fi
aws iam put-user-policy --user-name "$U" --policy-name consumer --policy-document "file://$P/consumer.json"
aws iam put-user-policy --user-name "$U" --policy-name gc --policy-document "file://$P/gc.json"
# The toolbox also counts and cleans up under the run prefix with this key.
aws iam put-user-policy --user-name "$U" --policy-name run-prefix --policy-document "file://$P/accept.json"
if [ ! -s "$STATE/consumer-key.json" ]; then
  (umask 077; aws iam create-access-key --user-name "$U" --output json > "$STATE/consumer-key.json") || die "create-access-key"
fi
KEY=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["AccessKey"]["AccessKeyId"])' "$STATE/consumer-key.json")
SECRET=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["AccessKey"]["SecretAccessKey"])' "$STATE/consumer-key.json")
if ! kubectl get ns otel-validate > /dev/null 2>&1; then
  kubectl create ns otel-validate > /dev/null && kubectl label ns otel-validate created-by=otel-chdb-validation > /dev/null
  created k8s-namespace otel-validate
fi
kubectl -n otel-validate create secret generic consumer-s3 --from-literal=AWS_ACCESS_KEY_ID="$KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$SECRET" --dry-run=client -o yaml | kubectl apply -f - > /dev/null

cat > "$STATE/iam.env" <<EOF
ACCOUNT=$ACCOUNT
R_EDGE=$R_EDGE
R_OTHER=$R_OTHER
R_UNTAGGED=$R_UNTAGGED
R_PODID=$R_PODID
R_ENT=$R_ENT
R_STS=$R_STS
R_CONS=$R_CONS
R_ACC=$R_ACC
R_ACCN=$R_ACCN
R_ACCP=$R_ACCP
EOF
log "roles and user ready: $STATE/iam.env (IAM is eventually consistent: wait ~10 s before the first assume)"
