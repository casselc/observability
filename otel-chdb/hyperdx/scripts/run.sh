#!/bin/sh
# The HyperDX evaluation stack, step by step (../README.md §Reproduce).
#
#   W=/path/to/workdir B=/path/with/otap-s3pq+consume+hdxgen scripts/run.sh STEP...
#
# Steps, in order after a container restart:
#   dockerd    Docker daemon inside the dev container: data root under $W, host
#              networking only (no bridge, no iptables), overlay2
#   hyperdx    Mongo 5.0.32 (:27117) and HyperDX 2.39.1 (app :18880, api :18800)
#   chproxy    the logging proxy HyperDX's connection points at (:18124 -> :18123)
#   sink       the alert webhook sink (:18890)
#   edges      the bucket hdx-otel, and the Rust edge twice: layout B on :14518 (all signals), ClickStack
#              tables on :14528 (metrics, for the stock comparison)
#   consumers  one consumer per root (hdx_b, hdx_stock), plus GC of consumed objects
#   gen        hdxgen: 3 h of backfill, then live for 6 h, into both edges
#   big        hdxgen: 6 h of backfill for 200 more pods (cluster "big"), metrics only
#   setup      the views over B, the picker helper, HyperDX user/connection/sources,
#              dashboards and alerts (after `consumers` has created B's tables)
#   down       stop everything started here and delete the containers and images
# The schema comparison (../README.md §Schema), instead of edges..setup:
#   schema-dbs        hdx_old with the pre-alignment DDL (../sql/pre_alignment_traces_logs.sql),
#                     hdx_full with ClickStack 2.39.1's full DDL (../sql/clickstack_full_*.sql),
#                     hdx_new with the consumer's (../../otap-rs/sql/otel_*.sql, option 2); with rollups
#   schema-edge       one edge (:14518), bucket hdx-otel, root schema/
#   schema-consumers  three consumers over that one root (own --ctl each): hdx_old, hdx_full, hdx_new
#                     get the same objects through the consumer's own INSERT
#   schema-gen        hdxgen traces and logs: 3 h of backfill
#   schema-setup      HyperDX sources for each (hdx_setup.py --schema old=hdx_old,full=hdx_full,new=hdx_new)
# Needs: AWS-style keys otel/otelsecret on SeaweedFS :18333; ClickHouse on CH (default
# 127.0.0.1:18123; the schema comparison used a private server with query_log).
set -eu
W=${W:?work dir}
B=${B:-$W/bin}
CH=${CH:-127.0.0.1:18123}
H=$(cd "$(dirname "$0")" && pwd)
OTAP=$H/../../otap-rs
mkdir -p "$W"
cd "$W"
bg() { log=$1; shift; setsid nohup "$@" >> "$W/$log" 2>&1 < /dev/null & }
for step in "$@"; do
  case $step in
  dockerd)
    # The exec root must be short: containerd's socket path has a 104-byte limit.
    bg dockerd.log dockerd --data-root "$W/docker-root" --exec-root /run/hdxd --pidfile "$W/dockerd.pid" \
      --iptables=false --ip6tables=false --bridge=none --storage-driver=overlay2
    until docker info > /dev/null 2>&1; do sleep 1; done ;;
  hyperdx)
    mkdir -p "$W/mongo-data"
    docker start hdx-mongo 2> /dev/null || docker run -d --name hdx-mongo --network host -v "$W/mongo-data:/data/db" \
      mongo:5.0.32-focal --port 27117 --bind_ip 127.0.0.1 --wiredTigerCacheSizeGB 0.25
    docker start hdx-app 2> /dev/null || docker run -d --name hdx-app --network host \
      -e MONGO_URI=mongodb://127.0.0.1:27117/hyperdx \
      -e HYPERDX_API_PORT=18800 -e HYPERDX_APP_PORT=18880 -e HYPERDX_OPAMP_PORT=18820 \
      -e HYPERDX_APP_URL=http://localhost -e FRONTEND_URL=http://localhost:18880 -e SERVER_URL=http://127.0.0.1:18800 \
      -e USAGE_STATS_ENABLED=false -e OTEL_SDK_DISABLED=true -e HYPERDX_LOG_LEVEL=info \
      -e EXPRESS_SESSION_SECRET=hdx-eval-secret -e WEBHOOK_HOSTNAME_ALLOWLIST=127.0.0.1 \
      hyperdx/hyperdx:2.39.1
    until curl -sf http://localhost:18800/health > /dev/null; do sleep 2; done ;;
  chproxy) bg chproxy.log python3 "$H/chproxy.py" --listen 127.0.0.1:18124 --ch "$CH" --log "$W/chproxy.jsonl" ;;
  sink) bg sink.log python3 "$H/webhook_sink.py" ;;   # appends to $W/hooks.jsonl
  edges)
    curl -sf -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" -u otel:otelsecret http://127.0.0.1:18333/hdx-otel > /dev/null || true
    export AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret
    bg edge.log env METRICS_LAYOUT=series_table OTLP_HTTP=127.0.0.1:14518 OTLP_GRPC=127.0.0.1:14517 ADMIN_HTTP=127.0.0.1:14580 \
      PRODUCER=hdx-edge-1 S3_URL=http://127.0.0.1:18333/hdx-otel/edges/hdx-edge-1 "$B/otap-s3pq" -c "$OTAP/configs/edge.yaml"
    bg edge-stock.log env METRICS_LAYOUT=clickstack_tables OTLP_HTTP=127.0.0.1:14528 OTLP_GRPC=127.0.0.1:14527 ADMIN_HTTP=127.0.0.1:14581 \
      PRODUCER=hdx-edge-stock S3_URL=http://127.0.0.1:18333/hdx-otel/stock/hdx-edge-stock "$B/otap-s3pq" -c "$OTAP/configs/edge.yaml" ;;
  consumers)
    python3 "$H/setup_db.py" stock hdx_stock > /dev/null   # stock tables first: the consumer keeps them
    for r in edges:hdx_b:w1 stock:hdx_stock:w2; do
      root=${r%%:*} rest=${r#*:} db=${rest%%:*}
      bg "consumer-$root.log" "$B/consume" --s3 "http://127.0.0.1:18333/hdx-otel/$root" --ch http://127.0.0.1:18123 --db "$db" \
        --key otel --secret otelsecret --worker "hdx-${rest#*:}"
      bg "gc-$root.log" "$B/consume" gc --s3 "http://127.0.0.1:18333/hdx-otel/$root" --key otel --secret otelsecret \
        --delay 15s --zombie 60s --every 30s --run-for 20h
    done ;;
  gen) bg gen.log "$B/hdxgen" -url http://127.0.0.1:14518 -also-metrics http://127.0.0.1:14528 -backfill 3h -step 30s \
         -pods 2 -traces 8 -live 6h ;;
  big) bg gen-big.log "$B/hdxgen" -url http://127.0.0.1:14518 -also-metrics http://127.0.0.1:14528 -cluster big -pods 40 \
         -extra-metrics 30 -traces 0 -signals metrics -backfill 6h -step 30s -seed 7 ;;
  setup)
    python3 "$H/setup_db.py" views hdx_b
    python3 "$H/setup_db.py" picker hdx_b
    python3 "$H/hdx_setup.py"
    python3 "$H/hdx_dash.py" ;;
  down)
    docker rm -f hdx-app hdx-mongo || true
    docker rmi hyperdx/hyperdx:2.39.1 mongo:5.0.32-focal || true
    [ -f "$W/dockerd.pid" ] && kill "$(cat "$W/dockerd.pid")" || true ;;
  schema-dbs)
    CH_URL=http://$CH python3 "$H/setup_db.py" file hdx_old "$H/../sql/pre_alignment_traces_logs.sql"
    CH_URL=http://$CH python3 "$H/setup_db.py" clickstack-full hdx_full
    CH_URL=http://$CH python3 "$H/setup_db.py" consumer hdx_new ;;
  schema-edge)
    curl -sf -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" -u otel:otelsecret http://127.0.0.1:18333/hdx-otel > /dev/null || true
    AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret bg edge.log env OTLP_HTTP=127.0.0.1:14518 OTLP_GRPC=127.0.0.1:14517 \
      ADMIN_HTTP=127.0.0.1:14580 PRODUCER=hdx-schema S3_URL=http://127.0.0.1:18333/hdx-otel/schema/hdx-schema "$B/otap-s3pq" -c "$OTAP/configs/edge.yaml" ;;
  schema-consumers)
    for db in hdx_old hdx_full hdx_new; do
      bg "consumer-$db.log" "$B/consume" --s3 http://127.0.0.1:18333/hdx-otel/schema --ctl "ctl-$db" --ch "http://$CH" --db "$db" \
        --key otel --secret otelsecret --worker "w-$db" --signals traces,logs
    done ;;
  schema-gen) bg gen.log "$B/hdxgen" -url http://127.0.0.1:14518 -signals traces,logs -backfill 3h -step 30s -pods 2 -traces 8 ;;
  schema-setup) python3 "$H/hdx_setup.py" --schema old=hdx_old,full=hdx_full,new=hdx_new ;;
  *) echo "unknown step $step" >&2; exit 2 ;;
  esac
done
