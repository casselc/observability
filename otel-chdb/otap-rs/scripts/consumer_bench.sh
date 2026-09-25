#!/bin/bash
# consumer_bench.sh: the consumer on a fixed set of committed objects, with
# 1 object per INSERT statement against N per statement. Each run gets a
# fresh database and a fresh checkpoint prefix, so every run ingests every
# object. Reports, per run: server CPU (system.events UserTime + SystemTime
# deltas, server-wide), consumer CPU, wall time, statements, S3 requests.
#
#   B=bin RUN=bench123 BATCHES="1 8 32" REPS=3 OUT=results/consumer/bench.jsonl scripts/consumer_bench.sh
#
# RUN names objects already under s3://otel/otap-rs-consumer/$RUN/edges.
set -u
B=${B:?dir with consume}
RUN=${RUN:?}
CH=${CH:-http://127.0.0.1:18123}
S3=${S3:-http://127.0.0.1:18333}
OUT=${OUT:-results/consumer/bench.jsonl}
REPS=${REPS:-3}
mkdir -p "$(dirname "$OUT")"
ch() { curl -sS "$CH/" --data-binary "$1"; }
cpu() { ch "SELECT sum(value) FROM system.events WHERE event IN ('UserTimeMicroseconds', 'SystemTimeMicroseconds')"; }
for rep in $(seq 1 "$REPS"); do
  for b in ${BATCHES:-1 32}; do
    db=otaprs_consumer_bench_$b
    ch "DROP DATABASE IF EXISTS $db"
    ctl=otap-rs-consumer/$RUN/ctl-b$b-r$rep-$RANDOM
    c0=$(cpu); t0=$(date +%s.%N)
    "$B/consume" --s3 "$S3/otel/otap-rs-consumer/$RUN/edges" --ctl "$ctl" --db $db --once --max-batch "$b" \
      --ttl 60s --margin 2s --budget 20s ${EXTRA:-} > /tmp/consumer_bench.$$.json 2>/dev/null
    t1=$(date +%s.%N); c1=$(cpu)
    python3 - "$b" "$rep" "$c0" "$c1" "$t0" "$t1" /tmp/consumer_bench.$$.json <<'EOF' | tee -a "$OUT"
import json, sys
b, rep, c0, c1, t0, t1, f = sys.argv[1:]
s = json.loads(open(f).read().strip().splitlines()[-1])
objs = s["objects_inserted"] + s["series_objects_inserted"]
print(json.dumps({"batch": int(b), "rep": int(rep), "objects": objs, "rows": s["rows_inserted"],
    "statements": s["statements"], "server_cpu_ms": (int(c1) - int(c0)) / 1000, "wall_ms": (float(t1) - float(t0)) * 1000,
    "consumer_cpu_ms": s["cpu_ms"], "s3": s["s3"], "checks": s["checks"],
    "server_cpu_ms_per_object": (int(c1) - int(c0)) / 1000 / max(objs, 1)}))
EOF
    ch "DROP DATABASE IF EXISTS $db"
  done
done
rm -f /tmp/consumer_bench.$$.json
