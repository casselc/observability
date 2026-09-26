#!/bin/bash
# chpriv.sh start|stop|pid  -- the private ClickHouse (HTTP :18623, part_log +
# query_log) for blocks 2-4, pinned to CPUS (default 1-3). Same binary as the
# shared server. Its data lives in the scratchpad and is removed on "wipe".
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
SRV=$S/clean/chpriv
here=$(cd "$(dirname "$0")" && pwd)
pid() { [ -f $SRV/data/status ] && awk '/^PID/{print $2}' $SRV/data/status; }
case $1 in
start)
  mkdir -p $SRV/data
  sed "s|SRV|$SRV|g" $here/server-config.xml > $SRV/config.xml
  cd $SRV && CLICKHOUSE_WATCHDOG_ENABLE=0 setsid nohup taskset -c ${CPUS:-1-3} $S/ch/clickhouse server --config-file=$SRV/config.xml \
    >> $SRV/stdout.log 2>&1 < /dev/null &
  for i in $(seq 1 60); do curl -s http://127.0.0.1:18623/ping > /dev/null && break; sleep 1; done
  curl -s http://127.0.0.1:18623/ --data-binary 'SELECT version()';;
stop)
  p=$(pid); [ -n "$p" ] && kill -TERM "$p" 2>/dev/null
  for i in $(seq 1 60); do [ -n "$p" ] && kill -0 "$p" 2>/dev/null || break; sleep 1; done;;
wipe) rm -rf $SRV;;
pid) pid;;
esac
