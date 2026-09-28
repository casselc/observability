#!/bin/bash
# Step one node's clock on purpose and watch what the edge on it does
# (clocks-and-skew.md §4; AMBIGUITY.md E6's fault injection, which only an
# LD_PRELOAD shim did before: otap-rs/scripts/replay_received.sh). EKS only:
# it uses the validation cluster's ng-clock node group, which has no nodes
# until `node-up` and whose taint keeps every other pod off it; `node-down`
# terminates the node, so no stepped clock outlives the experiment.
#
#   RUN=v1 BUCKET=... REGION=us-east-1 CONFIRM_CLOCK_STEP=yes clocks/clock_step.sh <cmd>
#
#   node-up          scale ng-clock to 1 and wait for the node
#   edge-up          a publisher Pod pinned to it (the StatefulSet's own pod template, producer otap-clock-0,
#                    buffer on an emptyDir) and a sender Job beside it (soaksend-like: otlpsend of one
#                    mixgen dataset every 60 s)
#   step SECONDS     stop chronyd on the node and move its clock by SECONDS (negative: back)
#   set TIME         stop chronyd and set the clock to TIME (e.g. "23:59:00" today, to cross midnight)
#   restore          start chronyd again and step to the reference (chronyc makestep)
#   observe          the publisher's commit outcomes and the rows' received_at days in ClickHouse
#   edge-down        remove the Pod and the Job
#   node-down        scale ng-clock back to 0 (the node, and its clock, are gone)
#
# Suggested sequence (the runbook's table): +30 s, -30 s (beyond the 20 s lease margin, but the edge
# holds no lease), set 23:59:00 (rows either side of midnight), +16 min (beyond SigV4's 15 min: 403
# RequestTimeTooSkewed, the edge must keep the slot unresolved, not guess), -15 min (an epoch named
# below one already written: the zombie bound is 10 min), restore between each.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need kubectl aws eksctl python3
[ "${CONFIRM_CLOCK_STEP:-}" = yes ] || die "this steps a node's clock: set CONFIRM_CLOCK_STEP=yes (the ng-clock node only)"
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
CLUSTER=${CLUSTER:-otel-val-$RUN}
DB=${DB:-eks_edge}
export KUBECONFIG=$STATE/kubeconfig
# shellcheck source=/dev/null
. "$STATE/images.env" || die "run eks/images.sh first"
BUSYBOX=${BUSYBOX:-public.ecr.aws/docker/library/busybox:1.37}
C=$STATE/clocks; mkdir -p "$C"
node() { kubectl get nodes -l otel-validate/pool=clock -o jsonpath='{.items[0].metadata.name}'; }
q() { kubectl -n otel-validate exec toolbox -- curl -sS http://clickhouse.otel-validate:8123/ --data-binary "$1"; }
onnode() { # run a shell command in the host's namespaces on the clock node (privileged, hostPID)
  local n k; n=$(node); [ -n "$n" ] || die "no clock node: node-up first"; k=clk-$(date +%s)
  kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $k, namespace: otel-validate, labels: {app: clock-step}}
spec:
  nodeName: $n
  hostPID: true
  restartPolicy: Never
  tolerations: [{operator: Exists}]
  containers:
    - name: s
      image: $BUSYBOX
      securityContext: {privileged: true}
      command: ["nsenter", "-t", "1", "-m", "-u", "-n", "-i", "--", "sh", "-c", "$1"]
EOF
  kubectl -n otel-validate wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$k" --timeout=120s > /dev/null || true
  kubectl -n otel-validate logs "$k"; kubectl -n otel-validate delete pod "$k" --wait=false > /dev/null
}
ev() { echo "$(date -u +%FT%T.%3NZ) $*" | tee -a "$C/steps.log"; }

case ${1:?command} in
  node-up)
    eksctl scale nodegroup --region "$REGION" --cluster "$CLUSTER" --name ng-clock --nodes 1 --nodes-max 1 || die "scale ng-clock"
    kubectl wait --for=condition=Ready node -l otel-validate/pool=clock --timeout=15m; ev "clock node $(node) up" ;;
  node-down)
    kubectl -n otel-validate delete pod otap-clock-0 --ignore-not-found > /dev/null
    eksctl scale nodegroup --region "$REGION" --cluster "$CLUSTER" --name ng-clock --nodes 0 --nodes-min 0 || die "scale ng-clock"
    ev "clock node group scaled to 0" ;;
  edge-up)
    n=$(node); [ -n "$n" ] || die "node-up first"
    # The publisher: the StatefulSet's pod template as a bare Pod on the clock node.
    kubectl -n otel-edge get sts otap-publisher -o json | python3 -c '
import json, sys
s = json.load(sys.stdin); t = s["spec"]["template"]
t["metadata"] = {"name": "otap-clock-0", "namespace": "otel-edge", "labels": {"app": "otap-clock"}}
spec = t["spec"]
spec["nodeSelector"] = {"otel-validate/pool": "clock"}
spec["tolerations"] = spec.get("tolerations", []) + [{"key": "otel-validate/clock", "operator": "Exists", "effect": "NoSchedule"}]
spec["hostname"] = "otap-clock-0"
for c in spec["containers"]:
    for e in c.get("env", []):
        if e.get("name") == "POD_NAME": e.pop("valueFrom", None); e["value"] = "otap-clock-0"
names = {m["name"] for c in spec["containers"] for m in c.get("volumeMounts", [])}
vols = {v["name"] for v in spec.get("volumes", [])}
spec["volumes"] = spec.get("volumes", []) + [{"name": v, "emptyDir": {}} for v in sorted(names - vols) if v != "kube-api-access"]
spec.pop("subdomain", None)
json.dump({"apiVersion": "v1", "kind": "Pod", "metadata": t["metadata"], "spec": spec}, sys.stdout)
' > "$C/otap-clock-0.json"
    kubectl apply -f "$C/otap-clock-0.json" > /dev/null && created k8s-manifest "$C/otap-clock-0.json"
    kubectl -n otel-edge wait --for=condition=Ready pod/otap-clock-0 --timeout=5m
    ip=$(kubectl -n otel-edge get pod otap-clock-0 -o jsonpath='{.status.podIP}')
    kubectl apply -f - > /dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: clock-sender, namespace: otel-edge, labels: {app: clock-sender}}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      nodeSelector: {otel-validate/pool: clock}
      tolerations: [{key: otel-validate/clock, operator: Exists, effect: NoSchedule}]
      volumes: [{name: mix, emptyDir: {}}]
      containers:
        - name: s
          image: $IMG_TOOLBOX
          command: ["sh", "-c", "i=0; while :; do i=\$((i+1)); rm -f /mix/*.pb; mixgen -signal traces -out /mix -batches 4 -publishers 1 -route none -seed \$((500+i)) -start \$(date -u +%Y-%m-%dT%H:%M:%SZ) >/dev/null; otlpsend -url http://$ip:4318 -signal traces -file \$(ls /mix/traces-b00*.pb | paste -sd,) -n 4 -backoff 1s -timeout 60s -quiet; sleep 60; done"]
          volumeMounts: [{name: mix, mountPath: /mix}]
EOF
    ev "edge on $n: otap-clock-0 ($ip) and clock-sender" ;;
  edge-down)
    kubectl -n otel-edge delete job clock-sender --ignore-not-found > /dev/null
    kubectl -n otel-edge delete pod otap-clock-0 --ignore-not-found > /dev/null; ev "edge on the clock node removed" ;;
  step)
    s=${2:?seconds}
    ev "step ${s}s: $(onnode "systemctl stop chronyd; date -u; date -u -s @\$((\$(date +%s) + $s)); date -u")" ;;
  set)
    ev "set clock to ${2:?time}: $(onnode "systemctl stop chronyd; date -u -s '$2'; date -u")" ;;
  restore)
    ev "restore: $(onnode "systemctl start chronyd; sleep 2; chronyc -a makestep; sleep 3; chronyc tracking | head -5; date -u")" ;;
  observe)
    ip=$(kubectl -n otel-edge get pod otap-clock-0 -o jsonpath='{.status.podIP}')
    ev "commit outcomes: $(kubectl -n otel-validate exec toolbox -- curl -s "http://$ip:8080/api/v1/metrics" | grep '^s3pq_commit_outcomes_total' | tr '\n' ' ')"
    q "SELECT toDate(received_at) AS day, min(received_at), max(received_at), count(), uniqExact(content_key)
       FROM $DB.otel_traces WHERE producer_id = 'otap-clock-0' GROUP BY day ORDER BY day FORMAT PrettyCompactMonoBlock" | tee -a "$C/steps.log"
    q "SELECT producer_epoch, min(received_at), max(received_at), count() FROM $DB.otel_traces WHERE producer_id = 'otap-clock-0'
       GROUP BY producer_epoch ORDER BY min(received_at) FORMAT PrettyCompactMonoBlock" | tee -a "$C/steps.log"
    kubectl -n otel-validate exec deploy/consume-gc -- sh -c 'curl -s http://127.0.0.1:9465/metrics' 2> /dev/null \
      | grep -E '^consumer_(complete_through|watermark|lane_watermark)' | tee -a "$C/steps.log" ;;
  *) die "node-up|edge-up|step S|set T|restore|observe|edge-down|node-down" ;;
esac
