#!/bin/bash
# The Go edge's commit protocol under faults, end to end, as
# ../otap-rs/scripts/faults.sh does for the Rust edge: sender (retries until
# 2xx, like a collector exporter with retry_on_failure) -> otelcol-s3pq
# (go-edge.yaml: OTLP/HTTP, no queue, the response follows the commit) ->
# faultproxy2 -> SeaweedFS; then the Rust consumer into a private ClickHouse
# database. Each scenario checks that central holds every request exactly
# once and counts the objects and tombstones on S3.
#
#   B=dir-with-otelcol-s3pq-consume-otlpsend-faultproxy2 D=dir-with-traces-bench-vNN.pb OUT=dir go_faults.sh
set -u
B=${B:?dir with otelcol-s3pq, consume, otlpsend and faultproxy2}
D=${D:?dir with otlpgen -variants traces-bench-vNN.pb}
OUT=${OUT:-/tmp/goedge-faults}
RUN=${RUN:-f$(date +%s)}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
PROXY=127.0.0.1:18336
BP=${BP:-goedge-conf}
DB=${DBP:-goedge_faults_}$RUN
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"
ch() { curl -sS "$CH/" --data-binary "$1"; }
ch "CREATE DATABASE IF NOT EXISTS $DB"
files=$(ls "$D"/traces-bench-v0[0-9].pb | head -${NREQ:-6} | paste -sd,)
nreq=$(echo "$files" | tr ',' '\n' | wc -l)

edge() { # name port: the Go collector, one producer (edge-go), its S3 root through the proxy
  local name=$1 port=$2
  env OTLP_HTTP=127.0.0.1:$port HEALTH=127.0.0.1:$((port + 15)) PUT_TIMEOUT=${PUT_TIMEOUT:-1s} PRODUCER=edge-go \
    S3_URL=http://$PROXY/$BP/faults/$RUN/$SCEN "$B/otelcol-s3pq" --config "$here/go-edge.yaml" >> "$OUT/$SCEN.edge.log" 2>&1 &
  local pid=$!
  for _ in $(seq 100); do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.1; done
  echo $pid
}
proxy() { "$B/faultproxy2" -listen $PROXY -target $S3 -match "/faults/$RUN/$SCEN/" "$@" >> "$OUT/$SCEN.proxy.log" 2>&1 & echo $!; }
send() { # port [n [files]]
  "$B/otlpsend" -url http://127.0.0.1:$1 -signal traces -file "${3:-$files}" -n "${2:-$nreq}" -timeout 30s -backoff 300ms -quiet >> "$OUT/$SCEN.send.log" 2>&1
}
fl() { ls "$D"/traces-bench-v*.pb | sed -n "$1p" | paste -sd,; }
consume() { # extra args
  "$B/consume" --s3 $S3/$BP/faults/$RUN/$SCEN --signal traces --ch $CH --table $DB.$SCEN \
    --state "$OUT/$SCEN.ckpt.json" "$@" >> "$OUT/$SCEN.consume.log" 2>&1
}
verdict() { # expected requests
  local objs rows want
  objs=$(ch "SELECT count() FROM s3('$S3/$BP/faults/$RUN/$SCEN/*/*/traces/**/*.parquet', 'otel', 'otelsecret', 'One') WHERE _size > 0")
  tombs=$(ch "SELECT count() FROM s3('$S3/$BP/faults/$RUN/$SCEN/*/*/traces/**/*.parquet', 'otel', 'otelsecret', 'One') WHERE _size = 0 SETTINGS s3_skip_empty_files = 0")
  rows=$(ch "SELECT count(), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.$SCEN FORMAT TSV")
  want="$(( $1 * 10000 ))	$1"
  local ok=FAIL
  [ "$(echo "$rows" | cut -f1,2)" = "$want" ] && ok=PASS
  echo "$ok $SCEN: requests=$1 objects=$objs zero-byte(tombstones+heartbeats)=$tombs central(rows, distinct content, epochs)=$(echo $rows)" | tee -a "$OUT/summary.txt"
  grep -ho '"committed".*"heads": [0-9]*' "$OUT/$SCEN.edge.log" | sed 's/^/  edge: /' | tee -a "$OUT/summary.txt"
  grep -h '"summary"' "$OUT/$SCEN.consume.log" | tail -1 | cut -c1-400 | sed 's/^/  consumer: /' | tee -a "$OUT/summary.txt"
  grep -hc "PUT" "$OUT/$SCEN.proxy.log" | sed 's/^/  proxy PUT lines: /' | tee -a "$OUT/summary.txt"
}
stop() { kill -INT "$@" 2>/dev/null; sleep 2; kill -KILL "$@" 2>/dev/null; wait "$@" 2>/dev/null; }

echo "run $RUN, $nreq requests of 10k spans per scenario, Go edge (otelcol-s3pq)" | tee "$OUT/summary.txt"

# 1. Ambiguous PUT: applied, the answer held past put_timeout. HEAD finds our batch.
SCEN=ambiguous
P=$(proxy -mode answer-late -hold 3s -every 2); sleep 0.5; E=$(edge $SCEN 14518)
send 14518; stop $E; stop $P; consume --once; verdict $nreq

# 2. Slow PUT then retry: the request is held 2.5 s before it reaches S3; the
#    PUT times out at 1 s, HEAD (free), resend; the late copy gets 412.
SCEN=applylate
P=$(proxy -mode apply-late -hold 2500ms -every 2); sleep 0.5; E=$(edge $SCEN 14518)
send 14518; sleep 3; stop $E; stop $P; consume --once; verdict $nreq

# 3. Dropped PUT (never lands, 503): the SDK's retry, or HEAD finds the slot free and resends.
SCEN=dropped
P=$(proxy -mode drop -hold 200ms -every 3); sleep 0.5; E=$(edge $SCEN 14518)
send 14518; stop $E; stop $P; consume --once; verdict $nreq

# 4. Unresolved: PUT answers held past the timeout and the HEADs too, so the
#    request is answered 503 and resent; the retry resolves the slot first.
SCEN=unresolved
P=$(proxy -mode answer-late -hold 3s -every 2 -head-hold 3s -head-limit 2); sleep 0.5; E=$(edge $SCEN 14518)
send 14518; stop $E; stop $P; consume --once; verdict $nreq

# 5. Crash and restart: every 2nd PUT lands but its answer is held 8 s; the
#    edge is SIGKILLed after 3 s, restarted (new epoch), and the sender resends.
SCEN=crash
P=$(proxy -mode answer-late -hold 8s -every 2); sleep 0.5; E=$(edge $SCEN 14518)
( send 14518 ) & S=$!
sleep 3; kill -KILL $E; echo "SIGKILL edge at $(date +%T.%N)" >> "$OUT/$SCEN.edge.log"; sleep 1
stop $P; P=$(proxy -mode none); E=$(edge $SCEN 14518)
wait $S; stop $E; stop $P
consume --once; verdict $nreq

# 6. Zombie: edge A keeps running after edge B (same producer) starts. The
#    consumer closes A's superseded epoch with a tombstone once it is quiet;
#    A's next PUT hits it (412, HEAD: tomb), A halts and continues in a new epoch.
SCEN=zombie
P=$(proxy -mode none); sleep 0.5
A=$(edge $SCEN 14518); send 14518 2 "$(fl 7,8)"
Bp=$(edge $SCEN 14528); send 14528 2 "$(fl 9,10)"
consume --poll 200ms --quiet 1s --exit-after-idle 3s
send 14518 2 "$(fl 11,12)"
consume --poll 200ms --quiet 1s --exit-after-idle 3s
stop $A $Bp; stop $P
verdict 6

[ -n "${KEEP:-}" ] || ch "DROP DATABASE IF EXISTS $DB"
