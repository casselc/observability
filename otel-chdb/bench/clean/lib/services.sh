# Sourced by driver.sh. Brings up exactly the services a block needs after a
# container restart (which kills every process) and checks they answer.
#   weed_up CPUS        SeaweedFS (the weed half of start-services.sh), all its
#                       threads pinned to CPUS; waits for S3 on :18333
#   shared_up [CPUS]    the shared ClickHouse (:18123) via start-services.sh;
#                       optionally pinned to CPUS
#   shared_down         stops the shared ClickHouse (SIGTERM, by its status pid)
#   priv_up / priv_down the private ClickHouse (:18623, lib/chpriv.sh)
# start-services.sh decides with `pgrep -x clickhouse`, which the private
# server also matches, so shared_up only calls it when the private one is down.
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
LIBDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
up() { curl -s -m 3 -o /dev/null "$1"; }
wait_up() { local i; for i in $(seq 1 ${2:-90}); do up "$1" && return 0; sleep 1; done; echo "not up: $1" >&2; return 1; }
pin_all() { local p; for p in "$@"; do taskset -a -cp "$CPUS_PIN" "$p" > /dev/null 2>&1; done; }
weed_up() {
  sh $S/clean/start-weed.sh
  wait_up http://127.0.0.1:18333/ 120 || return 1
  # S3 answers before the volume server has registered; wait for a real listing
  local i; for i in $(seq 1 60); do curl -sf -m 5 'http://127.0.0.1:18888/buckets/otel/?limit=1' -H 'Accept: application/json' > /dev/null && break; sleep 1; done
  CPUS_PIN=$1 pin_all $(pgrep -x weed)
}
shared_pid() { [ -f $S/chsrv/data/status ] && awk '/^PID/{print $2}' $S/chsrv/data/status; }
shared_up() {
  if ! up http://127.0.0.1:18123/ping; then
    priv_down
    sh $S/start-services.sh
    wait_up http://127.0.0.1:18123/ping 120 || return 1
  fi
  [ -n "${1:-}" ] && CPUS_PIN=$1 pin_all $(shared_pid)
  return 0
}
shared_down() {
  local p; p=$(shared_pid)
  up http://127.0.0.1:18123/ping || return 0
  [ -n "$p" ] && kill -TERM "$p" 2>/dev/null
  local i; for i in $(seq 1 90); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
  ! up http://127.0.0.1:18123/ping
}
priv_up() { up http://127.0.0.1:18623/ping || CPUS=${1:-1-3} bash $LIBDIR/chpriv.sh start > /dev/null; wait_up http://127.0.0.1:18623/ping 90; }
priv_down() { up http://127.0.0.1:18623/ping && bash $LIBDIR/chpriv.sh stop; return 0; }
