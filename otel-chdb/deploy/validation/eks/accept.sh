#!/bin/bash
# The acceptance kit (acceptance/RUNBOOK.md) against real S3, from pods under
# IRSA, on the cluster's nodes and network path. Three pods, one per
# s3:ListBucket grant (eks/iam.sh): full (the kit's policy), none, and
# prefix-scoped (as deploy/iam/edge-publisher.json scopes it).
#
#   RUN=v1 BUCKET=... REGION=us-east-1 eks/accept.sh [STEP...]
#
# Steps (default: all but hold): dry creds run1 run2 race conflicts list perf headmissing hold
# Every JSON report lands in $STATE/accept/; results.tsv gets the headline
# numbers. Objects go under s3://$BUCKET/$VPREFIX/accept/<s3accept run id>/ and
# each s3accept run deletes its own (the kit's cleanup check).
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need kubectl python3
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
export KUBECONFIG=$STATE/kubeconfig
# shellcheck source=/dev/null
. "$STATE/iam.env" || die "run eks/iam.sh first"
# shellcheck source=/dev/null
. "$STATE/images.env" || die "run eks/images.sh first"
STEPS=${*:-dry creds run1 run2 race conflicts list perf headmissing}
URL=s3://$BUCKET/$VPREFIX/accept
OUT=$STATE/accept
mkdir -p "$OUT"

pod() { # name serviceaccount role
  kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $2
  namespace: otel-validate
  annotations: {eks.amazonaws.com/role-arn: "$3", eks.amazonaws.com/sts-regional-endpoints: "true"}
---
apiVersion: v1
kind: Pod
metadata: {name: $1, namespace: otel-validate, labels: {app: s3accept}}
spec:
  serviceAccountName: $2
  nodeSelector: {otel-validate/pool: main}
  containers:
    - name: t
      image: $IMG_TOOLBOX
      env: [{name: AWS_REGION, value: "$REGION"}, {name: AWS_EC2_METADATA_DISABLED, value: "true"}]
      resources: {requests: {cpu: "1", memory: 1Gi}, limits: {memory: 4Gi}}
EOF
  kubectl -n otel-validate wait --for=condition=Ready "pod/$1" --timeout=5m > /dev/null || die "pod $1"
}
x() { local p=$1; shift; kubectl -n otel-validate exec "$p" -- "$@"; }
# acc POD NAME ARGS...: one s3accept run, its JSON copied out.
acc() {
  local p=$1 n=$2; shift 2
  log "s3accept $n on $p: $*"
  x "$p" s3accept "$@" --out "/tmp/$n.json" 2>&1 | tee "$OUT/$n.txt"
  kubectl -n otel-validate exec "$p" -- cat "/tmp/$n.json" > "$OUT/$n.json" 2> /dev/null || true
}
jq_py() { python3 -c "import json,sys; r=json.load(open(sys.argv[1])); $2" "$1" 2> /dev/null; }
status() { jq_py "$1" "print(' '.join(f\"{x['id']}={x['status']}\" for x in r['results']))"; }

pod accept s3accept "$R_ACC"
for s in $STEPS; do
  case $s in
    dry)   x accept s3accept --url "$URL" --region "$REGION" --dry-run | tee "$OUT/dry.txt" ;;
    creds) acc accept creds creds --url "$URL" --region "$REGION" --modes auto -v
           result accept.creds "$(grep -E '^\s*(irsa|chain|pod-identity)' "$OUT/creds.txt" | tr -s ' ' | tr '\n' ';')" ;;
    hold)  # across a real credential refresh (IRSA sessions last 1 h): 65 min
           acc accept hold creds --url "$URL" --region "$REGION" --modes chain --hold 65m --every 60s ;;
    run1|run2)
           acc accept "$s" --url "$URL" --region "$REGION" -v
           result "accept.$s.verdicts" "$(jq_py "$OUT/$s.json" "print(' '.join(c['id']+'='+c['verdict'] for c in r['capabilities']))")"
           result "accept.$s.checks" "$(status "$OUT/$s.json")" ;;
    race)  acc accept race --url "$URL" --region "$REGION" --only create-race,cas-race,ambiguous-create,ambiguous-cas \
             --race-writers 64 --race-rounds 50 --cas-writers 32 --ambiguous 50 -v
           result accept.race "$(status "$OUT/race.json")" ;;
    conflicts)
           # 409 ConditionalRequestConflict rate: many writers on one key, many rounds.
           acc accept conflicts --url "$URL" --region "$REGION" --only create-race --race-writers 128 --race-rounds 200 -v
           result accept.conflicts_409 "$(jq_py "$OUT/conflicts.json" "x=[c for c in r['results'] if c['id']=='create-race'][0]; print((x.get('data') or {}).get('conflicts_409','see txt'), 'in', 128*200, 'create attempts')")" ;;
    list)  acc accept list --url "$URL" --region "$REGION" --only list,list-race,read-after-write --consistency 500 --race-writers 32 -v
           result accept.list "$(status "$OUT/list.json")" ;;
    perf)  # one lane (concurrency 1) and a publisher's lanes (8) at the objects' real sizes
           acc accept perf1 --url "$URL" --region "$REGION" --only perf --perf-sizes 200B,1MB,3MiB,8MiB --perf-concurrency 1 --perf-ops 200
           acc accept perf8 --url "$URL" --region "$REGION" --only perf --perf-sizes 200B,1MB,3MiB,8MiB --perf-concurrency 8 --perf-ops 400 ;;
    headmissing)
           pod accept-nolist s3accept-nolist "$R_ACCN"
           pod accept-prefixlist s3accept-prefixlist "$R_ACCP"
           acc accept hm-full --url "$URL" --region "$REGION" --only head-missing -v
           acc accept-nolist hm-nolist --url "$URL" --region "$REGION" --only head-missing -v
           acc accept-prefixlist hm-prefixlist --url "$URL" --region "$REGION" --only head-missing -v
           result accept.head_missing "full=$(status "$OUT/hm-full.json") nolist=$(status "$OUT/hm-nolist.json") prefixlist=$(status "$OUT/hm-prefixlist.json")" ;;
    *) die "unknown step $s" ;;
  esac
done
log "reports in $OUT"
