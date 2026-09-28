#!/bin/bash
# Measure clock skew across a cluster's nodes (AMBIGUITY.md E6).
#
#   RUN=v1 [KUBECONFIG=... REFS=169.254.169.123,time.aws.com EVERY=10 CH_URL=http://clickhouse.otel-validate:8123 \
#    PY_IMAGE=public.ecr.aws/docker/library/python:3.12-slim] clocks/run.sh up|collect|report|down
#
#   up       the ConfigMap and DaemonSet (namespace otel-validate, created if missing)
#   collect  every probe's log so far into $STATE/clocks/samples-<time>.log (run it at the end, or daily:
#            a pod's log rotates, so collect at least once per day of a long run)
#   report   skew_report.py over every collected file -> $STATE/clocks/report.txt, .json
#   down     remove the DaemonSet and ConfigMap
# Off-cluster hosts (the consumer's, ClickHouse's): run sntp_probe.py there with the same REFS
# and append its output to $STATE/clocks/ as another samples-*.log.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need kubectl python3
export KUBECONFIG=${KUBECONFIG:-$STATE/kubeconfig}
REFS=${REFS:-169.254.169.123,time.aws.com}
EVERY=${EVERY:-10}
CH_URL=${CH_URL:-http://clickhouse.otel-validate:8123}
PY_IMAGE=${PY_IMAGE:-public.ecr.aws/docker/library/python:3.12-slim}
C=$STATE/clocks; mkdir -p "$C"
case ${1:?up|collect|report|down} in
  up)
    kubectl get ns otel-validate > /dev/null 2>&1 || { kubectl create ns otel-validate > /dev/null; created k8s-namespace otel-validate; }
    kubectl -n otel-validate create configmap clock-probe --from-file=sntp_probe.py="$VALIDATION_DIR/clocks/sntp_probe.py" \
      --dry-run=client -o yaml | kubectl apply -f - > /dev/null
    render "$VALIDATION_DIR/clocks/skew-daemonset.yaml.tmpl" "$C/daemonset.yaml" PY_IMAGE="$PY_IMAGE" REFS="$REFS" EVERY="$EVERY" CH_URL="$CH_URL"
    kubectl apply -f "$C/daemonset.yaml" && created k8s-manifest "$C/daemonset.yaml"
    kubectl -n otel-validate rollout status ds/clock-probe --timeout=5m
    kubectl -n otel-validate logs -l app=clock-probe --tail=2 --prefix | head -20 ;;
  collect)
    f=$C/samples-$(date -u +%Y%m%dT%H%M%S).log; now=$(date -u +%FT%TZ)
    since=(); [ -s "$C/last-collect" ] && since=(--since-time="$(cat "$C/last-collect")")   # no sample twice
    kubectl -n otel-validate logs -l app=clock-probe --prefix --tail=-1 --max-log-requests=100 ${since[@]+"${since[@]}"} > "$f"
    echo "$now" > "$C/last-collect"
    log "$(wc -l < "$f") samples -> $f" ;;
  report)
    python3 "$VALIDATION_DIR/clocks/skew_report.py" "$C"/samples-*.log ${REF:+--ref "$REF"} --json "$C/report.json" | tee "$C/report.txt"
    result clocks.fleet "$(python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print(json.dumps(r["fleet"]))' "$C/report.json")" ;;
  down)
    kubectl delete -f "$C/daemonset.yaml" --ignore-not-found > /dev/null
    kubectl -n otel-validate delete configmap clock-probe --ignore-not-found > /dev/null ;;
  *) die "up|collect|report|down" ;;
esac
