#!/bin/bash
# consumer_scale.sh: fleet-scale measurements of the consumer, against the
# local SeaweedFS and ClickHouse (README "Consumer at fleet scale").
#
#   MODE=lists: IDLE producers' lanes (7 each: traces, logs, layout B's
#     five), seeded with one request per signal and then quiet, plus BUSY
#     producers sending RATE requests/s per signal; one worker runs for SECS
#     with the flags in CFLAGS. Prints LISTs per lane per month (all of the
#     worker's LISTs, and the lane LISTs of busy and idle lanes apart when
#     the binary reports them).
#   MODE=linger: LANES producers at a low RATE, one worker per value in
#     LINGERS; prints objects per statement, server CPU per object
#     (system.events, server-wide) and visibility latency.
#
#   B=bin CONSUME=bin/consume MODE=lists IDLE=5 BUSY=1 SECS=180 CFLAGS="--poll 1s" OUT=f.jsonl scripts/consumer_scale.sh
#   B=bin MODE=linger LANES=4 RATE=0.5 SECS=90 LINGERS="0ms 1s 3s" OUT=f.jsonl scripts/consumer_scale.sh
set -u
B=${B:?dir with otap-s3pq, consume, soaksend}
CONSUME=${CONSUME:-$B/consume}
MODE=${MODE:-lists}
SECS=${SECS:-180}
OUT=${OUT:-/dev/stdout}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
BUCKET=${BUCKET:-scale-consumer}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
ch() { curl -sS "$CH/" --data-binary "$1"; }
cpu() { ch "SELECT sum(value) FROM system.events WHERE event IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds')"; }
PIDS=()
# (soaksend resends until 2xx, so it goes first, and by KILL: its edge may be gone)
cleanup() { for p in "${PIDS[@]}"; do kill -KILL "$p" 2>/dev/null; done; wait 2>/dev/null; rm -rf "$tmp"; }
trap cleanup EXIT

edge() { # producer port-offset
  env OTLP_HTTP=127.0.0.1:$((25400 + $2)) OTLP_GRPC=127.0.0.1:$((25500 + $2)) ADMIN_HTTP=127.0.0.1:$((25600 + $2)) PUT_TIMEOUT=5s \
    PRODUCER="$1" S3_URL="$S3/$PREFIX/$1" "$B/otap-s3pq" -c "$here/scripts/consumer_soak_edge.yaml" >> "$tmp/edge-$1.log" 2>&1 &
  echo $!
}

RUN=${RUN:-sc$(date +%s)}
PREFIX=$BUCKET/$RUN/edges

if [ "$MODE" = lists ]; then
  IDLE=${IDLE:-5}
  BUSY=${BUSY:-1}
  RATE=${RATE:-3}
  # Seed the idle lanes: one request per signal per idle producer, then the edge stops.
  for i in $(seq 1 "$IDLE"); do
    e=$(edge "idle-$i" "$i")
    sleep 2
    "$B/soaksend" -url "http://127.0.0.1:$((25400 + i))" -producer "idle-$i" -signals traces,logs,metrics -rows 50 -points 10 \
      -n 1 -rate 5 -out /dev/null > /dev/null 2>&1
    kill -TERM "$e"; wait "$e" 2>/dev/null
  done
  for i in $(seq 1 "$BUSY"); do
    p=$(edge "busy-$i" $((100 + i))); PIDS+=("$p")
  done
  sleep 2
  for i in $(seq 1 "$BUSY"); do
    "$B/soaksend" -url "http://127.0.0.1:$((25400 + 100 + i))" -producer "busy-$i" -signals traces,logs,metrics -rows 50 -points 10 \
      -rate "$RATE" -duration "$((SECS + 10))s" -out /dev/null > "$tmp/send-$i.log" 2>&1 &
    PIDS+=($!)
  done
  DB=scale_lists_$RUN
  # shellcheck disable=SC2086
  "$CONSUME" --s3 "$S3/$PREFIX" --db "$DB" --worker lists ${CFLAGS:-} --run-for "${SECS}s" --stats "$tmp/stats.json" --stats-every 5s \
    > "$tmp/w.log" 2>&1
  python3 - "$tmp/stats.json" "$IDLE" "$BUSY" "${CFLAGS:-}" "$CONSUME" <<'EOF' | tee -a "$OUT"
import json, sys
sf, idle, busy, flags, binary = sys.argv[1:]
s = json.load(open(sf))
secs = s["elapsed_ms"] / 1000
month = 30 * 86400
lanes = s["lanes_known"]
lists = s["s3"]["list"]
out = {"mode": "lists", "binary": binary.split("/")[-1], "flags": flags, "lanes": lanes, "idle_producers": int(idle),
       "busy_producers": int(busy), "secs": round(secs), "lists": lists,
       "lists_per_lane_month": round(lists / lanes / secs * month), "gets": s["s3"]["get"],
       "cas": s["s3"]["put_cas"], "objects": s["objects_inserted"] + s.get("series_objects_inserted", 0)}
by = s.get("lists_by_lane")
if by is not None:
    b = [v for k, v in by.items() if k.startswith("busy-")]
    i = [by.get(k, 0) for k in by if k.startswith("idle-")]
    nb, ni = 7 * int(busy), lanes - 7 * int(busy)
    out["lane_lists_busy_per_lane_month"] = round(sum(b) / max(nb, 1) / secs * month)
    out["lane_lists_idle_per_lane_month"] = round(sum(i) / max(ni, 1) / secs * month)
    out["discovery_lists_per_month"] = round((lists - s["lane_lists"]) / secs * month)
    out["lists_skipped"] = s["lists_skipped"]
    out["renewals"] = s["renewals"]
print(json.dumps(out))
EOF
  ch "DROP DATABASE IF EXISTS $DB SYNC"
elif [ "$MODE" = linger ]; then
  LANES=${LANES:-4}
  RATE=${RATE:-0.5}
  for linger in ${LINGERS:-0ms 1s}; do
    R2=$RUN-$linger
    PREFIX=$BUCKET/$R2/edges
    DB=scale_linger_${RUN}_${linger}
    for i in $(seq 1 "$LANES"); do p=$(edge "p-$i" "$i"); PIDS+=("$p"); done
    sleep 2
    # shellcheck disable=SC2086
    "$CONSUME" --s3 "$S3/$PREFIX" --db "$DB" --worker linger ${CFLAGS:-} --linger "$linger" \
      --stats "$tmp/stats.json" --stats-every 1s > "$tmp/w.log" 2>&1 &
    W=$!
    sleep 3
    c0=$(cpu)
    for i in $(seq 1 "$LANES"); do
      "$B/soaksend" -url "http://127.0.0.1:$((25400 + i))" -producer "p-$i" -signals traces -rows 200 -rate "$RATE" \
        -duration "${SECS}s" -out /dev/null > /dev/null 2>&1 &
      PIDS+=($!)
    done
    sleep "$((SECS + 8))"
    c1=$(cpu)
    kill -TERM $W; wait $W 2>/dev/null
    python3 - "$linger" "$c0" "$c1" "$tmp/stats.json" "$LANES" "$RATE" <<'EOF' | tee -a "$OUT"
import json, sys
linger, c0, c1, sf, lanes, rate = sys.argv[1:]
s = json.load(open(sf))
objs = s["objects_inserted"]
print(json.dumps({"mode": "linger", "linger": linger, "lanes": int(lanes), "rate_per_lane": float(rate), "objects": objs,
    "statements": s["statements"], "objects_per_statement": round(s["statement_objects"] / max(s["statements"], 1), 2),
    "server_cpu_ms_per_object": round((int(c1) - int(c0)) / 1000 / max(objs, 1), 2),
    "checks_per_object": round(s["ch_checks"] / max(objs, 1), 2),
    "visible_ms_p50": round(s["visible_ms_p50"]), "visible_ms_p90": round(s["visible_ms_p90"]), "visible_ms_max": round(s["visible_ms_max"]),
    "consumer_cpu_ms_per_object": round(s["cpu_ms"] / max(objs, 1), 3), "linger_deferred": s.get("linger_deferred")}))
EOF
    for p in "${PIDS[@]}"; do kill -KILL "$p" 2>/dev/null; done; wait 2>/dev/null; PIDS=()
    ch "DROP DATABASE IF EXISTS $DB SYNC"
    "$CONSUME" purge --s3 "$S3/$BUCKET/$R2" > /dev/null
  done
fi
"$CONSUME" purge --s3 "$S3/$BUCKET/$RUN" > /dev/null 2>&1
