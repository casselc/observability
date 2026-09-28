#!/bin/bash
# Per-prefix request limits (DECISIONS.md risk 3): create-only PUT load in the
# format-v2 layout from PODS pods that share one run id, so every key shares
# the prefix a fleet's edges share ({root}/, then the cluster). AWS documents
# 3,500 PUT/s per partitioned prefix and 503 SlowDown until S3 has split it.
#
#   RUN=v1 BUCKET=... REGION=us-east-1 [PLAN="cluster-first:2000:300 cluster-first:6000:600 single-lane:6000:300" PODS=4] eks/load.sh
#
# PLAN is layout:total-rate:seconds per phase, run in order with a 2-minute
# pause between. Cost: every PUT is $0.005 per 1,000 (the default plan sends
# ~6.3 M PUTs: about $32, plus the DELETEs, which are free). Each pod deletes
# the keys it wrote at the end; nothing else is touched.
# Output: $STATE/load/<phase>-<pod>.json, and $STATE/load/<phase>.tsv, the
# per-second sum over the pods by wall clock (python below).
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need kubectl python3
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
PLAN=${PLAN:-cluster-first:2000:300 cluster-first:6000:600 single-lane:6000:300}
PODS=${PODS:-4}
export KUBECONFIG=$STATE/kubeconfig
# shellcheck source=/dev/null
. "$STATE/images.env" || die "run eks/images.sh first"
OUT=$STATE/load
mkdir -p "$OUT"
total=0; for ph in $PLAN; do IFS=: read -r _ r s <<< "$ph"; total=$((total + r * s)); done
confirm "send about $total PUTs (~\$$(( total * 5 / 1000000 ))) to s3://$BUCKET/$VPREFIX/load/?" || die "not confirmed"
kubectl -n otel-validate get sa s3accept > /dev/null 2>&1 || die "run eks/accept.sh first (it creates the s3accept ServiceAccount)"
n=0
for ph in $PLAN; do
  IFS=: read -r layout rate secs <<< "$ph"
  n=$((n + 1)); name=p$n-$layout-$rate
  id=$RUN-$name
  per=$(( (rate + PODS - 1) / PODS ))
  log "phase $name: $layout, $rate PUT/s total ($PODS pods x $per) for ${secs}s, run id $id"
  for i in $(seq 1 "$PODS"); do
    kubectl apply -f - > /dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: load-$name-$i, namespace: otel-validate, labels: {app: s3load, phase: "$name"}}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 7200
  template:
    spec:
      serviceAccountName: s3accept
      restartPolicy: Never
      nodeSelector: {otel-validate/pool: main}
      containers:
        - name: load
          image: $IMG_TOOLBOX
          command: ["s3accept", "load", "--url", "s3://$BUCKET/$VPREFIX/load", "--region", "$REGION", "--run-id", "$id",
                    "--load-layout", "$layout", "--load-rate", "$per", "--load-duration", "${secs}s", "--load-ramp", "60s",
                    "--load-workers", "512", "--load-size", "1KB", "--out", "/dev/stdout"]
          env: [{name: AWS_REGION, value: "$REGION"}, {name: AWS_EC2_METADATA_DISABLED, value: "true"}]
          resources: {requests: {cpu: 700m, memory: 1Gi}, limits: {memory: 3Gi}}
EOF
  done
  for i in $(seq 1 "$PODS"); do
    kubectl -n otel-validate wait --for=condition=complete "job/load-$name-$i" --timeout="$((secs + 1800))s" > /dev/null \
      || log "job load-$name-$i did not complete"
    # The log is the progress lines, then the JSON report (stdout), then the summary.
    kubectl -n otel-validate logs "job/load-$name-$i" > "$OUT/$name-$i.log"
    python3 - "$OUT/$name-$i.log" "$OUT/$name-$i.json" <<'PY'
import json, sys
s = open(sys.argv[1]).read()
i = s.find("{\n")
obj, _ = json.JSONDecoder().raw_decode(s[i:])
json.dump(obj, open(sys.argv[2], "w"))
PY
  done
  python3 - "$OUT" "$name" <<'PY' | tee "$OUT/$name.tsv"
import collections, datetime, glob, json, sys
out, name = sys.argv[1:]
agg = collections.defaultdict(lambda: collections.Counter())
for f in sorted(glob.glob(f"{out}/{name}-*.json")):
    r = json.load(open(f))
    t0 = datetime.datetime.fromisoformat(r["started"].replace("Z", "+00:00")).timestamp()
    for s in r["seconds"]:
        k = int(t0) + s["t"]
        for f2 in ("target", "ok", "slowdown_503", "other_5xx", "conflict_409", "transport", "missed"):
            agg[k][f2] += s[f2]
print("t\ttarget\tok\tslowdown_503\tother_5xx\tconflict_409\ttransport\tmissed")
ks = sorted(agg)
for k in ks:
    a = agg[k]
    print(f"{k - ks[0]}\t{a['target']}\t{a['ok']}\t{a['slowdown_503']}\t{a['other_5xx']}\t{a['conflict_409']}\t{a['transport']}\t{a['missed']}")
tot = sum((agg[k] for k in ks), collections.Counter())
sent = tot["ok"] + tot["slowdown_503"] + tot["other_5xx"] + tot["conflict_409"] + tot["transport"]
tail = ks[-120:]
print(f"# {name}: sent {sent}, ok {tot['ok']}, 503 {tot['slowdown_503']} ({100 * tot['slowdown_503'] / max(sent, 1):.2f}%), "
      f"missed {tot['missed']}; last 2 min: {sum(agg[k]['ok'] for k in tail) / max(len(tail), 1):.0f} ok/s, "
      f"{sum(agg[k]['slowdown_503'] for k in tail) / max(len(tail), 1):.1f} 503/s", file=sys.stderr)
PY
  result "load.$name" "$(grep -h '^  sent' "$OUT/$name"-*.log | tr -s ' ' | tr '\n' '|' | cut -c1-800)"
  sleep 120
done
