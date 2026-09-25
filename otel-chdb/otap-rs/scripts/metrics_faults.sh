#!/bin/bash
# Metrics under faults: a request is five objects (one per metric type, each
# in its own lane and log), and must never end up half-acked. The chain is
# faults.sh's: sender (resends until 2xx) -> otap-s3pq (OTLP/HTTP,
# wait_for_result) -> faultproxy2 -> SeaweedFS; then one Rust consumer per
# metric type into a private ClickHouse database. Each scenario checks that
# every request's every type is in central exactly once (rows and distinct
# content keys per table), and counts the objects and the NACKs that had some,
# but not all, of a request's objects committed.
#
#   B=target/release T=tools-bin D=metrics-data OUT=results/metrics/faults scripts/metrics_faults.sh
set -u
B=${B:?dir with otap-s3pq and consume}
T=${T:?dir with otlpsend and faultproxy2}
D=${D:?dir with the otlpgen -metrics files}
OUT=${OUT:-results/metrics/faults}
RUN=${RUN:-mf$(date +%s)}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
PROXY=127.0.0.1:18336
DB=otaprs_mfaults_$RUN
TYPES="metrics_gauge metrics_sum metrics_histogram metrics_exponential_histogram metrics_summary"
here=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$OUT"
ch() { curl -sS "$CH/" --data-binary "$1"; }
ch "CREATE DATABASE IF NOT EXISTS $DB"
NREQ=${NREQ:-6}
files=$(ls "$D"/metrics-mixed-10000-b[0-9][0-9].pb | head -$NREQ | paste -sd,)
PER_TYPE=2000 # points of each type per request

edge() { # port env...
  local port=$1; shift
  env OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port - 1)) PUT_TIMEOUT=${PUT_TIMEOUT:-1s} VERBOSE=true \
    S3_URL=http://$PROXY/otel/metrics-rs/faults/$RUN/$SCEN "$@" \
    METRICS_LAYOUT=${METRICS_LAYOUT:-clickstack_tables} "$B/otap-s3pq" -c "$here/configs/edge.yaml" >> "$OUT/$SCEN.edge.log" 2>&1 &
  echo $!
}
proxy() { "$T/faultproxy2" -listen $PROXY -target $S3 "$@" >> "$OUT/$SCEN.proxy.log" 2>&1 & echo $!; }
send() { # port
  "$T/otlpsend" -url http://127.0.0.1:$1 -signal metrics -file "$files" -n "$NREQ" -timeout 30s -backoff 300ms >> "$OUT/$SCEN.send.log" 2>&1
}
consume() {
  for t in $TYPES; do
    "$B/consume" --s3 $S3/otel/metrics-rs/faults/$RUN/$SCEN --signal $t --ch $CH --table $DB.${SCEN}_$t \
      --state "$OUT/$SCEN.$t.ckpt.json" --once >> "$OUT/$SCEN.consume.log" 2>&1
  done
}
verdict() {
  local ok=PASS line="" objs tombs r
  for t in $TYPES; do
    objs=$(ch "SELECT count() FROM s3('$S3/otel/metrics-rs/faults/$RUN/$SCEN/$t/**/*.parquet', 'otel', 'otelsecret', 'One') WHERE _size > 0")
    tombs=$(ch "SELECT count() FROM s3('$S3/otel/metrics-rs/faults/$RUN/$SCEN/$t/**/*.parquet', 'otel', 'otelsecret', 'One') WHERE _size = 0 SETTINGS s3_skip_empty_files = 0")
    r=$(ch "SELECT count(), uniqExact(content_key) FROM $DB.${SCEN}_$t FORMAT TSV")
    [ "$r" = "$((NREQ * PER_TYPE))	$NREQ" ] || ok=FAIL
    line="$line ${t#metrics_}=$(echo $r | tr ' ' '/')(objs $objs, tombs $tombs)"
  done
  local acked partial
  acked=$(grep -c '"seq"' "$OUT/$SCEN.send.log")
  partial=$(grep -c 'nack: [1-4] of 5 objects committed' "$OUT/$SCEN.edge.log")
  echo "$ok $SCEN: requests=$NREQ acked=$acked partial-commit NACKs=$partial central rows/contents per type:$line" | tee -a "$OUT/summary.txt"
  grep -h "exporter stop" "$OUT/$SCEN.edge.log" | sed 's/^[0-9.]* otap-s3pq: /  edge: /' | tee -a "$OUT/summary.txt"
  grep -h '"summary"' "$OUT/$SCEN.send.log" | tail -1 | sed 's/^/  sender: /' | tee -a "$OUT/summary.txt"
  grep -h 'dedup_skipped' "$OUT/$SCEN.consume.log" | sed 's/.*"dedup_skipped":\([0-9]*\).*"epochs_closed_by_tombstone":\([0-9]*\).*/\1 \2/' |
    awk '{d+=$1; c+=$2} END {print "  consumers: cross-epoch copies skipped " d ", epochs closed by tombstone " c}' | tee -a "$OUT/summary.txt"
}
stop() { kill -INT "$@" 2>/dev/null; sleep 1.5; kill -KILL "$@" 2>/dev/null; wait "$@" 2>/dev/null; }

echo "run $RUN: $NREQ requests, each $PER_TYPE points of each of 5 types (5 objects)" | tee "$OUT/summary.txt"

# 1. Ambiguous PUTs, any type: applied, answer held past put_timeout. HEAD resolves.
SCEN=ambiguous
P=$(proxy -match "/faults/$RUN/$SCEN/" -mode answer-late -hold 3s -every 2); sleep 0.5; E=$(edge 14518); sleep 2
send 14518; stop $E; stop $P; consume; verdict

# 2. Half-committed attempts: the histogram PUTs time out AND their HEADs
#    time out (head_timeout 2s), so that part stays unresolved while the
#    other four commit. The request is NACKed; the retry finds the four in
#    their lanes' known set (no request) and resolves the histogram slot.
SCEN=partial
P=$(proxy -match "/faults/$RUN/$SCEN/metrics_histogram/" -mode answer-late -hold 3s -every 2 -head-hold 3s -head-limit 3)
sleep 0.5; E=$(edge 14518); sleep 2
send 14518; stop $E; stop $P; consume; verdict

# 3. Dropped PUTs (503, never lands), any type.
SCEN=dropped
P=$(proxy -match "/faults/$RUN/$SCEN/" -mode drop -hold 200ms -every 3); sleep 0.5; E=$(edge 14518); sleep 2
send 14518; stop $E; stop $P; consume; verdict

# 4. Crash mid-request: every histogram PUT lands but its answer, and the
#    answers to the HEADs that would resolve it, are held 8 s, so the first
#    request has four types committed and the fifth in doubt (NACKed, and
#    NACKed again on each retry) when the edge is SIGKILLed. The restarted edge (new epochs) gets the resend
#    and commits all five again; the consumers skip the copies of the four
#    by content key and close the dead epochs with tombstones.
SCEN=crash
P=$(proxy -match "/faults/$RUN/$SCEN/metrics_histogram/" -mode answer-late -hold 8s -every 1 -head-hold 8s -head-limit 1000)
sleep 0.5; E=$(edge 14518); sleep 2
( send 14518 ) & S=$!
sleep 3; kill -KILL $E; echo "SIGKILL edge at $(date +%T.%N)" >> "$OUT/$SCEN.edge.log"; sleep 1
stop $P; P=$(proxy -mode none -match "/faults/$RUN/$SCEN/"); E=$(edge 14518)
wait $S; stop $E; stop $P
consume; verdict

# 5. Crash with every type ambiguous: every 2nd PUT of any type is held 8 s.
SCEN=crashall
P=$(proxy -match "/faults/$RUN/$SCEN/" -mode answer-late -hold 8s -every 2); sleep 0.5; E=$(edge 14518); sleep 2
( send 14518 ) & S=$!
sleep 3; kill -KILL $E; echo "SIGKILL edge at $(date +%T.%N)" >> "$OUT/$SCEN.edge.log"; sleep 1
stop $P; P=$(proxy -mode none -match "/faults/$RUN/$SCEN/"); E=$(edge 14518)
wait $S; stop $E; stop $P
consume; verdict

ch "DROP DATABASE IF EXISTS $DB"
