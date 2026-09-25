#!/bin/bash
# The commit protocol under faults, end to end: sender (retries until 2xx,
# like a collector exporter with retry_on_failure) -> otap-s3pq (OTLP/HTTP,
# wait_for_result) -> faultproxy2 -> SeaweedFS; then the Rust consumer into a
# private ClickHouse database. Each scenario checks that central holds every
# request exactly once and counts the objects on S3.
#
#   B=target/release T=tools-bin D=data OUT=results/faults scripts/faults.sh
set -u
B=${B:?dir with otap-s3pq and consume}
T=${T:?dir with otlpsend and faultproxy2}
D=${D:?dir with the otlpgen .pb files}
OUT=${OUT:-results/faults}
RUN=${RUN:-f$(date +%s)}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
PROXY=127.0.0.1:18335
DB=otaprs_faults_$RUN
here=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$OUT"
ch() { curl -sS "$CH/" --data-binary "$1"; }
ch "CREATE DATABASE IF NOT EXISTS $DB"
files=$(ls "$D"/traces-bench-v0[0-9].pb | head -${NREQ:-6} | paste -sd,)
nreq=$(echo "$files" | tr ',' '\n' | wc -l)

edge() { # name port env...
  local name=$1 port=$2; shift 2
  env OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port - 1)) PUT_TIMEOUT=${PUT_TIMEOUT:-1s} VERBOSE=true \
    S3_URL=http://$PROXY/otel/otap-rs/faults/$RUN/$SCEN "$@" \
    "$B/otap-s3pq" -c "$here/configs/edge.yaml" >> "$OUT/$SCEN.edge.log" 2>&1 &
  echo $!
}
proxy() { "$T/faultproxy2" -listen $PROXY -target $S3 -match "/faults/$RUN/$SCEN/" "$@" >> "$OUT/$SCEN.proxy.log" 2>&1 & echo $!; }
send() { # port [n [files]]
  "$T/otlpsend" -url http://127.0.0.1:$1 -signal traces -file "${3:-$files}" -n "${2:-$nreq}" -timeout 30s -backoff 300ms -quiet >> "$OUT/$SCEN.send.log" 2>&1
}
fl() { ls "$D"/traces-bench-v*.pb | sed -n "$1p" | paste -sd,; }
consume() { # extra args
  "$B/consume" --s3 $S3/otel/otap-rs/faults/$RUN/$SCEN --signal traces --ch $CH --table $DB.$SCEN \
    --state "$OUT/$SCEN.ckpt.json" "$@" >> "$OUT/$SCEN.consume.log" 2>&1
}
verdict() { # expected requests
  local objs rows want
  objs=$(ch "SELECT count() FROM s3('$S3/otel/otap-rs/faults/$RUN/$SCEN/traces/**/*.parquet', 'otel', 'otelsecret', 'One') WHERE _size > 0")
  tombs=$(ch "SELECT count() FROM s3('$S3/otel/otap-rs/faults/$RUN/$SCEN/traces/**/*.parquet', 'otel', 'otelsecret', 'One') WHERE _size = 0 SETTINGS s3_skip_empty_files = 0")
  rows=$(ch "SELECT count(), uniqExact(content_key), uniqExact(producer_epoch) FROM $DB.$SCEN FORMAT TSV")
  want="$(( $1 * 10000 ))	$1"
  local ok=FAIL
  [ "$(echo "$rows" | cut -f1,2)" = "$want" ] && ok=PASS
  echo "$ok $SCEN: requests=$1 objects=$objs tombstones=$tombs central(rows, distinct content, epochs)=$(echo $rows)" | tee -a "$OUT/summary.txt"
  grep -h "exporter stop" "$OUT/$SCEN.edge.log" | sed 's/^[0-9.]* otap-s3pq: /  edge: /' | tee -a "$OUT/summary.txt"
  grep -h '"summary"' "$OUT/$SCEN.consume.log" | tail -1 | sed 's/^/  consumer: /' | tee -a "$OUT/summary.txt"
  grep -hc "PUT #" "$OUT/$SCEN.proxy.log" | sed 's/^/  proxy PUT lines: /' | tee -a "$OUT/summary.txt"
}
stop() { kill -INT "$@" 2>/dev/null; sleep 1.5; kill -KILL "$@" 2>/dev/null; wait "$@" 2>/dev/null; }

echo "run $RUN, $nreq requests of 10k spans per scenario" | tee "$OUT/summary.txt"

# 1. Ambiguous PUT: applied, the answer held past put_timeout. HEAD finds our batch.
SCEN=ambiguous
P=$(proxy -mode answer-late -hold 3s -every 2); sleep 0.5; E=$(edge $SCEN 14318); sleep 2
send 14318; stop $E; stop $P; consume --once; verdict $nreq

# 2. Slow PUT then retry: the request is held 2.5s before it reaches S3; the
#    exporter times out at 1s, HEADs (free), resends; the late copy gets 412.
SCEN=applylate
P=$(proxy -mode apply-late -hold 2500ms -every 2); sleep 0.5; E=$(edge $SCEN 14318); sleep 2
send 14318; sleep 3; stop $E; stop $P; consume --once; verdict $nreq

# 3. Dropped PUT (never lands, 503): HEAD finds the slot free, resend.
SCEN=dropped
P=$(proxy -mode drop -hold 200ms -every 3); sleep 0.5; E=$(edge $SCEN 14318); sleep 2
send 14318; stop $E; stop $P; consume --once; verdict $nreq

# 4. Crash and restart: every 2nd PUT lands but its answer is held 8s; the
#    edge is SIGKILLed after 3 s, restarted (new epoch), and the sender, which
#    never got its 2xx, resends. The copy in the dead epoch and the one in the
#    new epoch have the same content key; the consumer ingests one, and closes
#    the dead epoch with a tombstone in its first free slot.
SCEN=crash
P=$(proxy -mode answer-late -hold 8s -every 2); sleep 0.5; E=$(edge $SCEN 14318); sleep 2
( send 14318 ) & S=$!
sleep 3; kill -KILL $E; echo "SIGKILL edge at $(date +%T.%N)" >> "$OUT/$SCEN.edge.log"; sleep 1
stop $P; P=$(proxy -mode none); E=$(edge $SCEN 14318)
wait $S; stop $E; stop $P
consume --once; verdict $nreq

# 5. Zombie: edge A keeps running after edge B (same producer) starts. The
#    consumer closes A's superseded epoch with a tombstone once it is quiet;
#    A's next PUT hits it (412, HEAD: tomb), A halts that log and continues in
#    a new epoch. Nothing is lost or duplicated.
SCEN=zombie
P=$(proxy -mode none); sleep 0.5
A=$(edge $SCEN 14318); sleep 1.5; send 14318 2 "$(fl 7,8)"
Bp=$(edge $SCEN 14328); sleep 2; send 14328 2 "$(fl 9,10)"
consume --poll 200ms --quiet 1s --exit-after-idle 3s
send 14318 2 "$(fl 11,12)"   # A again: its PUT hits the tombstone, it halts that log, new epoch
consume --poll 200ms --quiet 1s --exit-after-idle 3s
stop $A $Bp; stop $P
verdict 6
grep -h "halted\|closed" "$OUT/$SCEN.consume.log" "$OUT/$SCEN.edge.log" | head -5 | sed 's/^/  /' | tee -a "$OUT/summary.txt"

ch "DROP DATABASE IF EXISTS $DB"
