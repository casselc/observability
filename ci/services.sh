#!/usr/bin/env bash
# The test services, in Docker, on the host network (ClickHouse reads
# SeaweedFS through s3(), so both must see 127.0.0.1:18333):
#   SeaweedFS  S3 :18333, keys otel/otelsecret, 64 volume slots of 1 GB
#   ClickHouse HTTP :18123, TCP :19000, user default without a password
#
#   ci/services.sh start     # pull, start, wait until both answer, create buckets
#   ci/services.sh stop      # remove the containers
#   ci/services.sh logs      # print both containers' logs
#
# Env: CH_IMAGE, SW_IMAGE; the ports (S3_PORT, CH_HTTP_PORT, CH_TCP_PORT,
# SW_MASTER_PORT, SW_VOLUME_PORT, SW_FILER_PORT) to run beside a local stack;
# BUCKETS to create.
set -euo pipefail
CH_IMAGE=${CH_IMAGE:-clickhouse/clickhouse-server:26.9}
SW_IMAGE=${SW_IMAGE:-chrislusf/seaweedfs:4.47}
S3_PORT=${S3_PORT:-18333}
CH_HTTP_PORT=${CH_HTTP_PORT:-18123}
CH_TCP_PORT=${CH_TCP_PORT:-19000}
SW_MASTER_PORT=${SW_MASTER_PORT:-19333}
SW_VOLUME_PORT=${SW_VOLUME_PORT:-18080}
SW_FILER_PORT=${SW_FILER_PORT:-18888}
BUCKETS=${BUCKETS:-otel goedge-test goedge-conf audit-consumer pqcmp}
KEY=otel SECRET=otelsecret
SW=${SW_NAME:-ci-seaweedfs} CH=${CH_NAME:-ci-clickhouse}
here=$(cd "$(dirname "$0")" && pwd)

s3() { curl -sS --aws-sigv4 aws:amz:us-east-1:s3 --user "$KEY:$SECRET" "$@"; }

wait_for() { # what command...
  local what=$1; shift
  for _ in $(seq 120); do "$@" >/dev/null 2>&1 && { echo "$what is up"; return 0; }; sleep 1; done
  echo "::error::$what did not come up within 120 s"; docker logs --tail 100 "$SW" || true; docker logs --tail 100 "$CH" || true
  return 1
}

case "${1:-start}" in
start)
  docker pull -q "$SW_IMAGE" & docker pull -q "$CH_IMAGE" & wait
  docker run -d --name "$SW" --network host \
    -v "$here/seaweedfs-s3.json:/etc/seaweedfs/s3.json:ro" \
    "$SW_IMAGE" server -dir=/data -ip=127.0.0.1 -ip.bind=127.0.0.1 \
    -master.port="$SW_MASTER_PORT" -master.volumeSizeLimitMB=1024 \
    -volume.port="$SW_VOLUME_PORT" -volume.max=64 -filer.port="$SW_FILER_PORT" \
    -s3 -s3.port="$S3_PORT" -s3.config=/etc/seaweedfs/s3.json
  # The image's entrypoint passes arguments after `--` to clickhouse-server as
  # config overrides. CLICKHOUSE_SKIP_USER_SETUP keeps `default` passwordless
  # with network access, as the tests expect.
  docker run -d --name "$CH" --network host --ulimit nofile=262144:262144 \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    "$CH_IMAGE" -- --http_port="$CH_HTTP_PORT" --tcp_port="$CH_TCP_PORT" \
    --mysql_port=$((CH_TCP_PORT + 4)) --postgresql_port=$((CH_TCP_PORT + 5)) \
    --interserver_http_port=$((CH_TCP_PORT + 9))
  wait_for "ClickHouse on :$CH_HTTP_PORT" curl -sf "http://127.0.0.1:$CH_HTTP_PORT/ping"
  wait_for "SeaweedFS S3 on :$S3_PORT" s3 -f "http://127.0.0.1:$S3_PORT/"
  for b in $BUCKETS; do
    # 200 created, 409 already there; SeaweedFS may need a moment after the
    # gateway answers before the filer takes writes.
    for _ in $(seq 30); do
      code=$(s3 -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$S3_PORT/$b" || true)
      case "$code" in 200|409) break ;; esac; sleep 1
    done
    echo "bucket $b: $code"
  done
  echo "ClickHouse $(curl -sS "http://127.0.0.1:$CH_HTTP_PORT/" --data-binary 'SELECT version()')"
  ;;
stop) docker rm -f "$SW" "$CH" >/dev/null 2>&1 || true ;;
logs) for c in "$SW" "$CH"; do echo "== $c"; docker logs "$c" 2>&1 | tail -n "${TAIL:-500}" || true; done ;;
*) echo "usage: $0 start|stop|logs" >&2; exit 2 ;;
esac
