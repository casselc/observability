#!/bin/bash
# chpriv.sh start|stop|pid|wipe -- this experiment's private ClickHouse (HTTP
# :18723, TCP :19700) with query_log, for central.sh and reads.py ch: the
# shared server (:18123) runs without a config file and so has no query_log.
# Same binary as the shared server; bench/clean/lib/server-config.xml with
# its own directory and ports. Data in the scratchpad, removed on "wipe".
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
SRV=$S/sorting/chpriv
here=$(cd "$(dirname "$0")" && pwd)
pid() { [ -f $SRV/data/status ] && awk '/^PID/{print $2}' $SRV/data/status; }
case $1 in
start)
  curl -s -m 2 http://127.0.0.1:18723/ping > /dev/null && exit 0
  mkdir -p $SRV/data
  sed -e "s|SRV|$SRV|g" -e 's|18623|18723|; s|19600|19700|' $here/../clean/lib/server-config.xml > $SRV/config.xml
  cd $SRV && CLICKHOUSE_WATCHDOG_ENABLE=0 setsid nohup $S/ch/clickhouse server --config-file=$SRV/config.xml \
    >> $SRV/stdout.log 2>&1 < /dev/null &
  for i in $(seq 1 60); do curl -s http://127.0.0.1:18723/ping > /dev/null && break; sleep 1; done;;
stop)
  p=$(pid); [ -n "$p" ] && kill -TERM "$p" 2>/dev/null
  for i in $(seq 1 60); do [ -n "$p" ] && kill -0 "$p" 2>/dev/null || break; sleep 1; done;;
wipe) rm -rf $SRV;;
pid) pid;;
esac
