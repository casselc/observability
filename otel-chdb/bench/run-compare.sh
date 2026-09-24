#!/usr/bin/env bash
# Compare the chdb exporter (in-process chDB) with the contrib clickhouse
# exporter writing to a real ClickHouse server on this machine, through the
# same collector API and the same data.
#
#   CHDB_LIB_PATH=/path/to/libchdb.so CLICKHOUSE_BIN=/path/to/clickhouse ./run-compare.sh [benchtime]
#
# Two phases, so neither side runs next to the other's background work: the
# chdb cases with no server up, then the clickhouse cases against a server
# started from a scratch directory with its default embedded config (native
# protocol on 127.0.0.1:19000). For those, cpu-ns/row counts this process and
# the server together.
set -euo pipefail
: "${CHDB_LIB_PATH:?set CHDB_LIB_PATH to libchdb.so}"
: "${CLICKHOUSE_BIN:?set CLICKHOUSE_BIN to a clickhouse binary}"
benchtime=${1:-3s}
here=$(cd "$(dirname "$0")" && pwd)
out=${OUT:-/dev/null}
cd "$here"

go test -run '^$' -bench 'Exporters/.*/.*/chdb-' -benchtime="$benchtime" -count=1 -timeout 60m . | tee "$out"

work=$(mktemp -d)
ch_pid=
trap '[ -n "$ch_pid" ] && kill "$ch_pid" 2>/dev/null; wait 2>/dev/null; rm -rf "$work"' EXIT
mkdir -p "$work/data"
# With its watchdog off the server does not fork, so $ch_pid is the process
# doing the work rather than an idle parent.
(cd "$work" && CLICKHOUSE_WATCHDOG_ENABLE=0 exec "$CLICKHOUSE_BIN" server -- \
	--path="$work/data/" --tcp_port=19000 --http_port=18123 --listen_host=127.0.0.1 \
	--mysql_port=19004 --postgresql_port=19005 --interserver_http_port=19009 \
	--logger.log="$work/server.log" --logger.errorlog="$work/server.err.log" --logger.level=warning \
	>"$work/stdout.log" 2>&1) &
ch_pid=$!
for _ in $(seq 1 100); do
	curl -sf http://127.0.0.1:18123/ping >/dev/null 2>&1 && break
	sleep 0.2
done
curl -sf http://127.0.0.1:18123/ping >/dev/null || { cat "$work/stdout.log" "$work/server.err.log" 2>/dev/null; exit 1; }
echo "clickhouse $(curl -s 'http://127.0.0.1:18123/?query=SELECT%20version()') pid $ch_pid" | tee -a "$out"

CLICKHOUSE_ENDPOINT=tcp://127.0.0.1:19000 CLICKHOUSE_PID=$ch_pid \
	go test -run '^$' -bench 'Exporters/.*/.*/clickhouse-' -benchtime="$benchtime" -count=1 -timeout 60m . |
	tee -a "$out"
