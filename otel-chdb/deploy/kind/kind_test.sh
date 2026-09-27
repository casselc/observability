#!/bin/bash
# The kind test of deploy/ (../results/k8s-sim.md §kind): datasets, a sender
# Job, the consumer on the host, and the exactly-once check per dataset.
#   W=workdir KUBECTL=kubectl KUBECONFIG=... MIXGEN=.../mixgen CONSUME=.../consume kind_test.sh <cmd> [args]
# W/mix must be kind-config.yaml's /mix mount. SeaweedFS (bucket k8s-edge,
# reached by the pods through relay.py) and ClickHouse on the host.
#   gen D         dataset D (seed 100+D, start 2026-09-26T10:00Z + D h) into W/mix (/mix in the node)
#   send D [NAME] Job otlpsend-D: 32 traces + 32 logs requests (10k rows each) -> otel-agent:4318, resend on error
#   waitsend D    wait for the Job to finish; print sender summaries
#   consumer      start the consumer in the background (db k8s_edge, run-for 4h)
#   check D [T]   poll ClickHouse until dataset D has 320k rows per signal or T s pass; print counts
#   total         all datasets: rows, distinct, duplicates per signal
#   ev TEXT       append to events.log
#   sigkill POD [CONTAINER]  SIGKILL the container's process from the node (it restarts in place)
K=${W:?}
KC="${KUBECTL:-kubectl}"
CH=http://127.0.0.1:18123; DB=k8s_edge
q() { curl -s "$CH/" --data-binary "$1"; }
ev() { echo "$(date -u +%FT%T.%3NZ) $*" | tee -a $K/events.log; }
win() { # D -> [t0, t1) ns of dataset D (32 batches x 333.33 ms)
  local s; s=$(( $(date -u -d 2026-09-26T10:00:00Z +%s) + 3600 * $1 )); echo "${s}000000000 $((s+11))000000000"; }
start() { date -u -d @$(( $(date -u -d 2026-09-26T10:00:00Z +%s) + 3600 * $1 )) +%Y-%m-%dT%H:%M:%SZ; }
case $1 in
  gen) rm -f $K/mix/*.pb; for s in traces logs; do ${MIXGEN:?} -signal $s -out $K/mix -batches 32 -publishers 1 -route none \
         -seed $((100+$2)) -start "$(start $2)" > /dev/null || exit 1; done; ls $K/mix | wc -l ;;
  send)
    d=$2; files() { ls $K/mix/$1-b00*.pb | sed "s#$K/mix#/mix#" | paste -sd,; }
    $KC -n otel-edge delete job otlpsend-$d --ignore-not-found > /dev/null
    cat <<EOF | $KC apply -f - > /dev/null
apiVersion: batch/v1
kind: Job
metadata: {name: otlpsend-$d, namespace: otel-edge}
spec:
  backoffLimit: 0
  template:
    metadata: {labels: {app: otlpsend}}
    spec:
      restartPolicy: Never
      securityContext: {runAsUser: 10001}
      volumes: [{name: mix, hostPath: {path: /mix}}]
      containers:
$(for s in traces logs; do cat <<C
        - name: $s
          image: localhost/otlpsend:kind
          args: ["-url", "${URL:-http://otel-agent:4318}", "-signal", "$s", "-file", "$(files $s)", "-n", "32", "-backoff", "300ms", "-timeout", "60s", "-quiet"]
          volumeMounts: [{name: mix, mountPath: /mix}]
C
done)
EOF
    ev "send dataset $d started" ;;
  waitsend)
    $KC -n otel-edge wait --for=condition=complete job/otlpsend-$2 --timeout=${3:-900s} > /dev/null && ev "send dataset $2 complete" || ev "send dataset $2 NOT complete"
    for s in traces logs; do echo "$s: $($KC -n otel-edge logs job/otlpsend-$2 -c $s | tail -1)"; done ;;
  consumer)
    (cd $K && exec setsid nohup ${CONSUME:?} --s3 http://127.0.0.1:18333/k8s-edge/edge --ch $CH --db $DB \
      --signals traces,logs --key otel --secret otelsecret --poll 1s --run-for 4h --stats $K/consume-stats.jsonl --stats-every 10s \
      >> $K/consume.log 2>&1 < /dev/null) & echo $! > $K/consume.pid; echo "consumer pid $(cat $K/consume.pid)" ;;
  check)
    read -r t0 t1 <<< "$(win $2)"; end=$(( $(date +%s) + ${3:-300} ))
    while :; do
      tr=$(q "SELECT count() FROM $DB.otel_traces WHERE Timestamp >= fromUnixTimestamp64Nano($t0) AND Timestamp < fromUnixTimestamp64Nano($t1) FORMAT TSV")
      lg=$(q "SELECT count() FROM $DB.otel_logs WHERE Timestamp >= fromUnixTimestamp64Nano($t0) AND Timestamp < fromUnixTimestamp64Nano($t1) FORMAT TSV")
      { [ "${tr:-0}" -ge 320000 ] && [ "${lg:-0}" -ge 320000 ]; } || [ $(date +%s) -ge $end ] && break; sleep 5
    done
    sleep 15   # late copies, if any
    echo "dataset $2: signal rows distinct duplicates missing(of 320000)"
    q "SELECT 'traces', count(), uniqExact(TraceId, SpanId), count() - uniqExact(TraceId, SpanId), 320000 - uniqExact(TraceId, SpanId) FROM $DB.otel_traces WHERE Timestamp >= fromUnixTimestamp64Nano($t0) AND Timestamp < fromUnixTimestamp64Nano($t1) FORMAT TSV"
    q "SELECT 'logs', count(), uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), count() - uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), 320000 - uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes) FROM $DB.otel_logs WHERE Timestamp >= fromUnixTimestamp64Nano($t0) AND Timestamp < fromUnixTimestamp64Nano($t1) FORMAT TSV"
    q "SELECT 'objects/content keys/producers', uniqExact(content_key), groupUniqArray(replaceRegexpOne(producer_id, '^kind-edge-', '')) FROM $DB.otel_traces WHERE Timestamp >= fromUnixTimestamp64Nano($t0) AND Timestamp < fromUnixTimestamp64Nano($t1) FORMAT TSV" ;;
  total)
    q "SELECT 'traces', count(), uniqExact(TraceId, SpanId), count() - uniqExact(TraceId, SpanId) FROM $DB.otel_traces FORMAT TSV"
    q "SELECT 'logs', count(), uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes), count() - uniqExact(Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes) FROM $DB.otel_logs FORMAT TSV" ;;
  ev) shift; ev "$*" ;;
esac
# sigkill POD: SIGKILL the pod's main container process from the node (the container restarts in place)
if [ "$1" = sigkill ]; then
  cid=$(docker exec ${NODE:-k8s-edge-control-plane} crictl ps -q --label io.kubernetes.pod.name=$2 --label io.kubernetes.container.name=${3:-otap-s3pq} | head -1)
  pid=$(docker exec ${NODE:-k8s-edge-control-plane} crictl inspect -o go-template --template '{{.info.pid}}' $cid)
  docker exec ${NODE:-k8s-edge-control-plane} kill -9 $pid && ev "SIGKILL $2 (container $cid pid-in-node $pid)"
fi
