#!/bin/bash
# The kind test (deploy/kind/kind_test.sh, results/k8s-sim.md §8) on EKS:
# datasets through agents → publishers → real S3 → the consumer → ClickHouse,
# the exactly-once check per dataset, and the faults, now on real nodes,
# EBS volumes, webhooks and S3 latency. Every command appends to
# $STATE/fleet/events.log.
#
#   RUN=v1 BUCKET=... REGION=us-east-1 [EDGE=rust] eks/fleet_test.sh <cmd> [args]
#
#   send D            Job otlpsend-D (namespace otel-edge): mixgen dataset D (seed 100+D, its own hour
#                     of timestamps: 32 traces + 32 logs requests of 10,000 rows, 120 services) into
#                     an emptyDir, then otlpsend -> otel-agent:4318, resending on any error
#   waitsend D [T]    wait for the Job (default 900 s); print the senders' summaries
#   check D [T]       poll ClickHouse until dataset D has 320,000 rows per signal or T s pass
#                     (default 600); rows, distinct, duplicates, missing, content keys
#   total             every dataset: rows, distinct, duplicates per signal
#   sigkill POD [C]   SIGKILL container C's processes from a hostPID pod on its node (restart in place)
#   forcedelete POD   kubectl delete --grace-period=0 --force
#   restart WHAT      rollout restart: publishers | agents | gateways
#   scale N           the publishers' StatefulSet to N replicas
#   smallpvc ORD SIZE pre-create ordinal ORD's buffer PVC at SIZE (e.g. 2Gi) before a scale-up takes it
#   growpvc ORD SIZE  expand ordinal ORD's buffer PVC (real EBS expansion), then wait for the resize
#   s3cut on|off      (EKS) deny s3:* to the publishers' roles (an IAM Deny; S3 answers 403, as an outage
#                     the edge cannot resolve), and remove it
#   drain             cordon and drain the node running publisher 0 (multi-node rescheduling), uncordon after
#   gc on|off         scale consume-gc to 1 / 0 (off keeps ingested objects, for `lanes`)
#   lanes [SINCE]     (EKS) per lane: objects, bytes and objects/s from S3 LastModified since SINCE (a
#                     DateTime, default 1 hour ago): lane throughput with real PUT latency (gc off first)
#   audit             the consumer's horizon audit over everything (late copies, re-cut duplicates)
#   metrics NAME      snapshot the publishers' and the consumer's metrics into $STATE/fleet/metrics-NAME.txt
#   ev TEXT           append to events.log
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need kubectl python3
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
EDGE=${EDGE:-rust}
DB=${DB:-eks_edge}
export KUBECONFIG=${KUBECONFIG:-$STATE/kubeconfig}
# shellcheck source=/dev/null
. "$STATE/images.env" || die "run eks/images.sh first"
F=$STATE/fleet
mkdir -p "$F"
case "$EDGE" in rust) STS=otap-publisher; PVC=buffer; C0=otap-s3pq;; go) STS=otelcol-publisher; PVC=queue; C0=otelcol;; esac
BUSYBOX=${BUSYBOX:-public.ecr.aws/docker/library/busybox:1.37}
# CH_URL: a ClickHouse reachable from here (a Nutanix site's VM); default: the in-cluster one via the toolbox.
q() {
  if [ -n "${CH_URL:-}" ]; then curl -sS "$CH_URL/" --data-binary "$1"
  else kubectl -n otel-validate exec toolbox -- curl -sS http://clickhouse.otel-validate:8123/ --data-binary "$1"; fi
}
ev() { echo "$(date -u +%FT%T.%3NZ) $*" | tee -a "$F/events.log"; }
start() { date -u -d @$(( $(date -u -d 2026-09-26T10:00:00Z +%s) + 3600 * $1 )) +%Y-%m-%dT%H:%M:%SZ; }
win() { local s; s=$(( $(date -u -d 2026-09-26T10:00:00Z +%s) + 3600 * $1 )); echo "${s}000000000 $((s + 11))000000000"; }
ROLES() { # the publishers' roles (IRSA and Pod Identity)
  # shellcheck source=/dev/null
  . "$STATE/iam.env"; echo "${R_EDGE##*/} ${R_PODID##*/}"
}

cmd=${1:?command}; shift
case $cmd in
  send)
    d=${1:?dataset}
    kubectl -n otel-edge delete job "otlpsend-$d" --ignore-not-found > /dev/null
    kubectl apply -f - > /dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: otlpsend-$d, namespace: otel-edge, labels: {app: otlpsend}}
spec:
  backoffLimit: 0
  template:
    metadata: {labels: {app: otlpsend}}
    spec:
      restartPolicy: Never
      securityContext: {runAsUser: 10001}
      volumes: [{name: mix, emptyDir: {}}]
      initContainers:
        - name: gen
          image: $IMG_TOOLBOX
          command: ["sh", "-c", "for s in traces logs; do mixgen -signal \$s -out /mix -batches 32 -publishers 1 -route none -seed $((100 + d)) -start $(start "$d") > /dev/null || exit 1; done; ls /mix | wc -l"]
          volumeMounts: [{name: mix, mountPath: /mix}]
      containers:
$(for s in traces logs; do cat <<C
        - name: $s
          image: $IMG_TOOLBOX
          command: ["sh", "-c", "exec otlpsend -url \${URL:-http://otel-agent:4318} -signal $s -file \$(ls /mix/$s-b00*.pb | paste -sd,) -n 32 -backoff 300ms -timeout 60s -quiet"]
          volumeMounts: [{name: mix, mountPath: /mix}]
C
done)
EOF
    ev "send dataset $d started" ;;
  waitsend)
    kubectl -n otel-edge wait --for=condition=complete "job/otlpsend-$1" --timeout="${2:-900}s" > /dev/null \
      && ev "send dataset $1 complete" || ev "send dataset $1 NOT complete"
    for s in traces logs; do echo "$s: $(kubectl -n otel-edge logs "job/otlpsend-$1" -c "$s" | tail -1)"; done ;;
  check)
    read -r t0 t1 <<< "$(win "$1")"; end=$(( $(date +%s) + ${2:-600} ))
    W="Timestamp >= fromUnixTimestamp64Nano($t0) AND Timestamp < fromUnixTimestamp64Nano($t1)"
    while :; do
      tr=$(q "SELECT count() FROM $DB.otel_traces WHERE $W FORMAT TSV" 2> /dev/null)
      lg=$(q "SELECT count() FROM $DB.otel_logs WHERE $W FORMAT TSV" 2> /dev/null)
      { [ "${tr:-0}" -ge 320000 ] && [ "${lg:-0}" -ge 320000 ]; } || [ "$(date +%s)" -ge "$end" ] && break
      sleep 10
    done
    sleep 30   # late copies, if any
    echo "dataset $1: signal rows distinct duplicates missing(of 320000)" | tee -a "$F/checks.txt"
    { q "SELECT 'traces', count(), uniqExact(TraceId, SpanId), count() - uniqExact(TraceId, SpanId), 320000 - uniqExact(TraceId, SpanId) FROM $DB.otel_traces WHERE $W FORMAT TSV"
      q "SELECT 'logs', count(), uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), count() - uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), 320000 - uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes) FROM $DB.otel_logs WHERE $W FORMAT TSV"
      q "SELECT 'content keys / producers', uniqExact(content_key), groupUniqArray(producer_id) FROM $DB.otel_traces WHERE $W FORMAT TSV"
    } | tee -a "$F/checks.txt"
    result "fleet.dataset.$1" "$(tail -3 "$F/checks.txt" | head -2 | tr '\t\n' ' ;')" ;;
  total)
    q "SELECT 'traces', count(), uniqExact(TraceId, SpanId), count() - uniqExact(TraceId, SpanId) FROM $DB.otel_traces FORMAT TSV"
    q "SELECT 'logs', count(), uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), count() - uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes) FROM $DB.otel_logs FORMAT TSV" ;;
  sigkill)
    pod=${1:?pod}; c=${2:-$C0}
    node=$(kubectl -n otel-edge get pod "$pod" -o jsonpath='{.spec.nodeName}')
    cid=$(kubectl -n otel-edge get pod "$pod" -o jsonpath="{.status.containerStatuses[?(@.name==\"$c\")].containerID}" | sed 's#^.*://##')
    [ -n "$cid" ] || die "no container $c in $pod"
    k=kill-$(date +%s)
    kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $k, namespace: otel-validate, labels: {app: sigkill}}
spec:
  nodeName: $node
  hostPID: true
  restartPolicy: Never
  tolerations: [{operator: Exists}]
  containers:
    - name: k
      image: $BUSYBOX
      securityContext: {privileged: true}
      command: ["sh", "-c", "n=0; for p in /proc/[0-9]*; do grep -q $cid \$p/cgroup 2>/dev/null && kill -9 \${p#/proc/} && n=\$((n+1)); done; echo killed \$n process(es) of $cid"]
EOF
    kubectl -n otel-validate wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$k" --timeout=120s > /dev/null
    ev "SIGKILL $pod/$c on $node: $(kubectl -n otel-validate logs "$k")"
    kubectl -n otel-validate delete pod "$k" --wait=false > /dev/null ;;
  forcedelete)
    kubectl -n otel-edge delete pod "${1:?pod}" --grace-period=0 --force > /dev/null 2>&1; ev "force-deleted $1" ;;
  restart)
    case ${1:?what} in publishers) o=sts/$STS;; agents) o=ds/otel-agent;; gateways) o=deploy/otel-gateway;; *) die "publishers|agents|gateways";; esac
    kubectl -n otel-edge rollout restart "$o" > /dev/null; ev "rollout restart $o"
    kubectl -n otel-edge rollout status "$o" --timeout=15m > /dev/null; ev "rollout $o done" ;;
  scale)
    kubectl -n otel-edge scale "sts/$STS" --replicas="${1:?N}" > /dev/null; ev "scale $STS to $1" ;;
  smallpvc)
    ord=${1:?ordinal}; size=${2:?size}
    kubectl apply -f - > /dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $PVC-$STS-$ord, namespace: otel-edge, labels: {created-by: otel-chdb-validation}}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: $size}}}
EOF
    ev "pre-created PVC $PVC-$STS-$ord at $size" ;;
  growpvc)
    ord=${1:?ordinal}; size=${2:?size}
    kubectl -n otel-edge patch pvc "$PVC-$STS-$ord" -p "{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"$size\"}}}}" > /dev/null
    ev "PVC $PVC-$STS-$ord expansion to $size requested"
    for _ in $(seq 120); do
      [ "$(kubectl -n otel-edge get pvc "$PVC-$STS-$ord" -o jsonpath='{.status.capacity.storage}')" = "$size" ] && break; sleep 5
    done
    ev "PVC $PVC-$STS-$ord capacity $(kubectl -n otel-edge get pvc "$PVC-$STS-$ord" -o jsonpath='{.status.capacity.storage}') (conditions: $(kubectl -n otel-edge get pvc "$PVC-$STS-$ord" -o jsonpath='{.status.conditions[*].type}'))" ;;
  s3cut)
    for r in $(ROLES); do
      if [ "${1:?on|off}" = on ]; then
        aws iam put-role-policy --role-name "$r" --policy-name validation-s3cut \
          --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}'
      else
        aws iam delete-role-policy --role-name "$r" --policy-name validation-s3cut 2> /dev/null || true
      fi
    done
    ev "S3 cut $1 (IAM Deny on $(ROLES); takes a few seconds to apply)" ;;
  drain)
    node=$(kubectl -n otel-edge get pod "$STS-0" -o jsonpath='{.spec.nodeName}')
    ev "drain $node"
    kubectl drain "$node" --ignore-daemonsets --delete-emptydir-data --timeout=10m > /dev/null || ev "drain $node incomplete"
    kubectl -n otel-edge rollout status "sts/$STS" --timeout=15m > /dev/null
    ev "drained $node; $STS-0 now on $(kubectl -n otel-edge get pod "$STS-0" -o jsonpath='{.spec.nodeName}')"
    kubectl uncordon "$node" > /dev/null; ev "uncordon $node" ;;
  metrics)
    o=$F/metrics-${1:?name}.txt; : > "$o"
    for ip in $(kubectl -n otel-edge get pod -l app.kubernetes.io/component=publisher -o jsonpath='{.items[*].status.podIP}'); do
      port=8080; path=/api/v1/metrics; [ "$EDGE" = go ] && port=8888 && path=/metrics
      echo "# publisher $ip" >> "$o"
      kubectl -n otel-validate exec toolbox -- curl -s "http://$ip:$port$path" | grep -E '^(s3pq_commit_outcomes_total|storage_bytes|otelcol_exporter_queue)' >> "$o"
    done
    for ip in $(kubectl -n otel-validate get pod -l 'app in (consume, consume-gc)' -o jsonpath='{.items[*].status.podIP}'); do
      echo "# consumer $ip" >> "$o"
      kubectl -n otel-validate exec toolbox -- sh -c "curl -s http://$ip:9464/metrics; curl -s http://$ip:9465/metrics" | grep -E '^consumer_' >> "$o"
    done
    ev "metrics snapshot $o ($(wc -l < "$o") lines)" ;;
  gc)
    kubectl -n otel-validate scale deploy/consume-gc --replicas="$([ "${1:?on|off}" = on ] && echo 1 || echo 0)" > /dev/null; ev "consume-gc $1" ;;
  lanes)
    since=${1:-$(date -u -d '1 hour ago' '+%F %T')}
    kubectl -n otel-validate exec toolbox -- sh -c "curl -sS http://clickhouse.otel-validate:8123/ --data-binary \"
      SELECT arrayStringConcat(arraySlice(splitByChar('/', _path), -5, 3), '/') AS lane, count() AS objects,
             round(sum(_size) / count() / 1e6, 2) AS mb_per_object, min(_time) AS first, max(_time) AS last,
             round(count() / greatest(dateDiff('second', min(_time), max(_time)), 1), 2) AS objects_per_s
      FROM s3('https://s3.$REGION.amazonaws.com/$BUCKET/$VPREFIX/edge/*/*/*/*/*.parquet', '\$AWS_ACCESS_KEY_ID', '\$AWS_SECRET_ACCESS_KEY', 'One')
      WHERE _time >= '$since' AND _size > 0 GROUP BY lane ORDER BY objects_per_s DESC FORMAT PrettyCompactMonoBlock\"" | tee -a "$F/lanes.txt" ;;
  audit)
    kubectl -n otel-validate exec toolbox -- sh -c "consume horizon-audit --s3 s3://$BUCKET/$VPREFIX/edge --key \$AWS_ACCESS_KEY_ID \
      --secret \$AWS_SECRET_ACCESS_KEY --ch \$CH --db $DB --check-horizon all" 2>&1 | tee -a "$F/audit.txt" ;;
  ev) ev "$*" ;;
  *) die "unknown command $cmd (see the header)" ;;
esac
