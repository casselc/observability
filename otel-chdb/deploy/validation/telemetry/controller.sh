#!/bin/bash
# The entity controller at real churn (real-cluster-telemetry.md §5):
# deploy entityctl, run the aggregator into the validation ClickHouse, sample
# the controller's cost, and measure freshness, record volume and resource_id
# agreement with the agents (entities/controller/README.md, k8s-sim.md §2-5
# for the KWOK numbers these replace).
#
#   RUN=v1 BUCKET=... CLUSTER=... [REGION=us-east-1 STATIC=cloud.provider=aws,... S3_ENDPOINT= \
#    ENTITY_KEY= ENTITY_SECRET= CAT_DB=k8s_cat EDGE_DB=eks_edge] telemetry/controller.sh up|aggregator|sample SECS|freshness|volume|joinrate|agree|probe
#
# KUBECONFIG defaults to $STATE/kubeconfig (the EKS runbook's); for another
# cluster export KUBECONFIG and build the toolbox image there (eks/images.sh
# or deploy/kind/prebuilt.Dockerfile). agree and probe run ridcheck from this
# machine through a port-forward to ClickHouse; `probe` CREATES, relabels and
# deletes one test pod per iteration in namespace freshness-probe, which
# ridcheck creates (the ledger records it if it did not exist; down.sh removes it).
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need kubectl python3
: "${BUCKET:?}" "${CLUSTER:?}"
REGION=${REGION:-us-east-1}
CAT_DB=${CAT_DB:-k8s_cat}; EDGE_DB=${EDGE_DB:-eks_edge}
ENTITIES=$VPREFIX/entities
export KUBECONFIG=${KUBECONFIG:-$STATE/kubeconfig}
# shellcheck source=/dev/null
. "$STATE/images.env" 2> /dev/null || : "${IMG_TOOLBOX:?set IMG_TOOLBOX or run eks/images.sh}"
OUT=$STATE/telemetry; mkdir -p "$OUT"
STATIC=${STATIC:-cloud.provider=aws,cloud.platform=aws_eks,cloud.region=$REGION}
q() { kubectl -n otel-validate exec toolbox -- curl -sS http://clickhouse.otel-validate:8123/ --data-binary "$1"; }
RIDCHECK=${RIDCHECK:-$STATE/build/bin/ridcheck}

case ${1:?command} in
  up)
    ann="{}"
    if [ -z "${S3_ENDPOINT:-}" ] && [ -f "$STATE/iam.env" ]; then
      # shellcheck source=/dev/null
      . "$STATE/iam.env"; ann="{eks.amazonaws.com/role-arn: \"$R_ENT\", eks.amazonaws.com/sts-regional-endpoints: \"true\"}"
    fi
    if [ -n "${ENTITY_KEY:-}" ]; then
      kubectl -n otel-validate create secret generic entityctl-s3 --from-literal=AWS_ACCESS_KEY_ID="$ENTITY_KEY" \
        --from-literal=AWS_SECRET_ACCESS_KEY="$ENTITY_SECRET" --dry-run=client -o yaml | kubectl apply -f - > /dev/null
    fi
    render "$VALIDATION_DIR/telemetry/entityctl.yaml.tmpl" "$STATE/entityctl.yaml" SA_ANNOTATIONS="$ann" BUCKET="$BUCKET" \
      ENTITIES="$ENTITIES" CLUSTER="$CLUSTER" REGION="$REGION" STATIC="$STATIC" S3_ENDPOINT="${S3_ENDPOINT:-}" IMG_TOOLBOX="$IMG_TOOLBOX"
    kubectl apply -f "$STATE/entityctl.yaml" && created k8s-manifest "$STATE/entityctl.yaml"
    kubectl -n otel-validate rollout status deploy/entityctl --timeout=5m ;;
  aggregator)
    kubectl -n otel-validate exec toolbox -- mkdir -p sql
    kubectl cp "$OTEL_CHDB/entities/controller/sql/." "otel-validate/toolbox:sql"
    kubectl -n otel-validate exec toolbox -- sh -c "nohup aggregator --create sql/aggregator.sql --db $CAT_DB --bucket $BUCKET \
      --prefix $ENTITIES --ch \$CH --announced $EDGE_DB.otel_resources ${S3_ENDPOINT:+--s3-endpoint $S3_ENDPOINT} > aggregator.log 2>&1 &"
    sleep 20; kubectl -n otel-validate exec toolbox -- tail -5 aggregator.log ;;
  sample)
    # every 60 s: /stats and the container's cgroup CPU (cpu.stat usage_usec), for SECS seconds
    end=$(( $(date +%s) + ${2:-86400} ))
    pod=$(kubectl -n otel-validate get pod -l app=entityctl -o jsonpath='{.items[0].metadata.name}')
    while [ "$(date +%s)" -lt "$end" ]; do
      st=$(kubectl -n otel-validate exec "$pod" -- curl -s http://127.0.0.1:19900/stats 2> /dev/null)
      cpu=$(kubectl -n otel-validate exec "$pod" -- sh -c 'awk "/usage_usec/{print \$2}" /sys/fs/cgroup/cpu.stat' 2> /dev/null)
      rss=$(kubectl -n otel-validate exec "$pod" -- sh -c 'cat /sys/fs/cgroup/memory.current' 2> /dev/null)
      echo "{\"t\":$(date +%s),\"cpu_usec\":${cpu:-null},\"mem_bytes\":${rss:-null},\"stats\":${st:-null}}" >> "$OUT/controller-samples.jsonl"
      sleep 60
    done
    python3 - "$OUT/controller-samples.jsonl" <<'PY' | tee "$OUT/controller-cost.txt"
import json, sys
s = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
s = [x for x in s if x["cpu_usec"] is not None]
if len(s) > 1:
    cores = (s[-1]["cpu_usec"] - s[0]["cpu_usec"]) / 1e6 / (s[-1]["t"] - s[0]["t"])
    mem = [x["mem_bytes"] for x in s if x["mem_bytes"]]
    st0, st1 = s[0]["stats"] or {}, s[-1]["stats"] or {}
    days = (s[-1]["t"] - s[0]["t"]) / 86400
    print(f"cores avg {cores:.4f}; memory avg {sum(mem) / len(mem) / 2**20:.0f} MiB max {max(mem) / 2**20:.0f} MiB")
    for k in ("records", "bytes", "objects", "relists", "gaps", "deleted_unknown", "retries"):
        if k in st1 and k in st0:
            print(f"{k}: {st1[k] - st0[k]} in {days:.2f} d = {(st1[k] - st0[k]) / max(days, 1e-9):.0f} per day")
PY
    ;;
  freshness)
    # lag = first_ingested - first_event per version opened in the window (k8s-sim.md §2), by level
    q "SELECT level, count() AS versions, quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', first_event, first_ingested) / 1000) AS lag_s_p50_p95_p99,
         max(dateDiff('millisecond', first_event, first_ingested)) / 1000 AS max_s
       FROM $CAT_DB.versions_final WHERE first_event > now() - INTERVAL ${WINDOW_H:-24} HOUR AND level != 'gap' GROUP BY level FORMAT PrettyCompactMonoBlock" \
      | tee "$OUT/freshness.txt" ;;
  volume)
    kubectl -n otel-validate exec toolbox -- sh -c "curl -sS http://clickhouse.otel-validate:8123/ --data-binary \"SELECT count() AS objects, sum(size) AS bytes, countIf(p LIKE '%.delta.%') AS deltas, countIf(p LIKE '%.sync.%') AS syncs FROM (SELECT _path AS p, any(_size) AS size FROM s3('${S3_ENDPOINT:-https://s3.$REGION.amazonaws.com}/$BUCKET/$ENTITIES/$CLUSTER/*/*.gz', '\$AWS_ACCESS_KEY_ID', '\$AWS_SECRET_ACCESS_KEY', 'One') GROUP BY p) FORMAT PrettyCompactMonoBlock\"" \
      | tee "$OUT/volume.txt" ;;
  joinrate)
    # the share of rows whose edge-computed resource_id the catalog knows (controller or announcement), and
    # of those, the share only an announcement explains (the controller missed or disagreed): the edge vs
    # controller agreement on REAL rows, as ridcheck measures it on the API's view
    q "SELECT signal, count() AS rows, round(countIf(known) / count(), 5) AS known_share,
              round(countIf(known AND only_ann) / count(), 5) AS announce_only_share, countIf(resource_id = 0) AS no_id
       FROM (
         SELECT 'traces' AS signal, resource_id, resource_id IN (SELECT resource_id FROM $CAT_DB.resources) AS known,
                resource_id NOT IN (SELECT resource_id FROM $CAT_DB.resources WHERE source != 'announce') AS only_ann
         FROM $EDGE_DB.otel_traces WHERE received_at > now() - INTERVAL ${WINDOW_H:-1} HOUR
         UNION ALL
         SELECT 'logs', resource_id, resource_id IN (SELECT resource_id FROM $CAT_DB.resources),
                resource_id NOT IN (SELECT resource_id FROM $CAT_DB.resources WHERE source != 'announce')
         FROM $EDGE_DB.otel_logs WHERE received_at > now() - INTERVAL ${WINDOW_H:-1} HOUR
       ) GROUP BY signal FORMAT PrettyCompactMonoBlock" | tee "$OUT/joinrate.txt" ;;
  agree|probe)
    [ -x "$RIDCHECK" ] || die "ridcheck not built: (cd entities/controller && go build -o $RIDCHECK ./cmd/ridcheck)"
    kubectl -n otel-validate port-forward svc/clickhouse 18124:8123 > /dev/null 2>&1 & pf=$!; trap 'kill $pf 2>/dev/null' EXIT; sleep 3
    if [ "$1" = agree ]; then
      "$RIDCHECK" --kubeconfig "$KUBECONFIG" --cluster "$CLUSTER" --static "$STATIC" --ch http://127.0.0.1:18124 --db "$CAT_DB" | tee "$OUT/agree.txt"
    else
      kubectl get ns freshness-probe > /dev/null 2>&1 || created k8s-namespace freshness-probe
      "$RIDCHECK" --kubeconfig "$KUBECONFIG" --cluster "$CLUSTER" --static "$STATIC" --ch http://127.0.0.1:18124 --db "$CAT_DB" --mode probe --n "${N:-10}" | tee "$OUT/probe.txt"
    fi ;;
  *) die "up | aggregator | sample SECS | freshness | volume | joinrate | agree | probe" ;;
esac
