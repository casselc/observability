#!/usr/bin/env bash
# A replicated central for CI, in Docker on the host network: one ClickHouse
# Keeper and two replicas of shard 01 of cluster "central" (the layout of
# otel-chdb/central-replicated/, minus its S3 disks and its three Keepers),
# so the tests that need a lagging replica can run in a workflow (STPA CAST
# row 6: the audit read a replica that had not fetched a part).
#
#   Keeper   :29181 (raft :29231)
#   r1       HTTP :28123, TCP :29000, interserver :29009
#   r2       HTTP :38123, TCP :39000, interserver :39009
#
# The replicas' macros are {shard}=01 and {replica}=r1 / r2, their `default`
# user has no password (as ci/services.sh's ClickHouse). Under Actions,
# `start` adds clickhouse-replicated to OSCOPE_REQUIRE_SERVICES, so a test
# that finds the replicas missing fails instead of skipping (CAST row 43).
#
#   ci/replicated.sh start | stop | logs
#
# Env: CH_IMAGE (as ci/services.sh).
set -euo pipefail
CH_IMAGE=${CH_IMAGE:-clickhouse/clickhouse-server:26.9}
cfg=${RUNNER_TEMP:-/tmp}/ci-replicated
names=(ci-keeper ci-r1 ci-r2)

keeper_xml() {
  cat <<'EOF'
<clickhouse>
  <listen_host>127.0.0.1</listen_host>
  <logger><console>1</console><level>information</level></logger>
  <keeper_server>
    <tcp_port>29181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse-keeper/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse-keeper/snapshots</snapshot_storage_path>
    <coordination_settings><operation_timeout_ms>10000</operation_timeout_ms><session_timeout_ms>30000</session_timeout_ms></coordination_settings>
    <raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname><port>29231</port></server></raft_configuration>
  </keeper_server>
</clickhouse>
EOF
}

replica_xml() { # name interserver-port
  cat <<EOF
<clickhouse>
  <interserver_http_host>127.0.0.1</interserver_http_host>
  <macros><shard>01</shard><replica>$1</replica></macros>
  <zookeeper><node><host>127.0.0.1</host><port>29181</port></node></zookeeper>
  <remote_servers>
    <central>
      <shard>
        <internal_replication>true</internal_replication>
        <replica><host>127.0.0.1</host><port>29000</port></replica>
        <replica><host>127.0.0.1</host><port>39000</port></replica>
      </shard>
    </central>
  </remote_servers>
  <distributed_ddl><path>/clickhouse/task_queue/ddl</path></distributed_ddl>
</clickhouse>
EOF
}

up() { # port
  for _ in $(seq 120); do curl -sf "http://127.0.0.1:$1/ping" > /dev/null 2>&1 && return 0; sleep 1; done
  echo "::error::ClickHouse on :$1 did not come up within 120 s"; return 1
}

case "${1:-start}" in
start)
  mkdir -p "$cfg"
  keeper_xml > "$cfg/keeper.xml"
  replica_xml r1 > "$cfg/r1.xml"
  replica_xml r2 > "$cfg/r2.xml"
  docker pull -q "$CH_IMAGE"
  docker run -d --name ci-keeper --network host -v "$cfg/keeper.xml:/etc/ci-keeper.xml:ro" \
    --entrypoint /usr/bin/clickhouse "$CH_IMAGE" keeper --config-file=/etc/ci-keeper.xml
  for r in 1 2; do
    base=$((r == 1 ? 29000 : 39000)); http=$((r == 1 ? 28123 : 38123))
    docker run -d --name "ci-r$r" --network host --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
      -v "$cfg/r$r.xml:/etc/clickhouse-server/config.d/zz-replica.xml:ro" \
      "$CH_IMAGE" -- --http_port="$http" --tcp_port="$base" --interserver_http_port=$((base + 9)) \
      --mysql_port=$((base + 4)) --postgresql_port=$((base + 5))
  done
  up 28123 && up 38123
  # both replicas see Keeper (a replicated table would otherwise be read-only)
  for p in 28123 38123; do
    for _ in $(seq 60); do
      curl -sf "http://127.0.0.1:$p/" --data-binary "SELECT count() FROM system.zookeeper WHERE path = '/'" > /dev/null && break
      sleep 1
    done
    echo "replica :$p: $(curl -sS "http://127.0.0.1:$p/" --data-binary "SELECT version() || ' ' || getMacro('replica')")"
  done
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "OSCOPE_REQUIRE_SERVICES=${OSCOPE_REQUIRE_SERVICES:+$OSCOPE_REQUIRE_SERVICES,}clickhouse-replicated" >> "$GITHUB_ENV"
  fi
  ;;
stop) docker rm -f "${names[@]}" > /dev/null 2>&1 || true ;;
logs) for n in "${names[@]}"; do echo "=== $n"; docker logs --tail 200 "$n" 2>&1 || true; done ;;
*) echo "usage: $0 start|stop|logs" >&2; exit 2 ;;
esac
