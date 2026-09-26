#!/bin/bash
# Row correctness of every otap-rs edge configuration, as edited, against
# local SeaweedFS with credentials from the environment only (the default
# chain), reusing the crate's checks:
#   - traces and logs: scripts/correctness.py (the Rust objects against
#     parquetgo's reference objects for the same requests: count+hash, the
#     inferred schema, EXCEPT both ways, the envelope, a central insert);
#   - metrics, layout B: scripts/metrics_correctness.py (the points and
#     series objects through the compatibility views against the contrib
#     clickhouseexporter's own rows).
# Each configuration runs as it ships (sorting off, layout B, memory
# limiter, the durable buffer where it has one); edge-otap.yaml is fed over
# OTLP here (its OTAP receiver is scripts/otap_e2e.sh's subject).
#
#   BIN=.../otap-s3pq T=dir-with-otlpsend,otlpgen,metricsref W=workdir RUN=name configs_e2e.sh
set -u
here=$(cd "$(dirname "$0")/../.." && pwd)
: "${BIN:?}" "${T:?}" "${W:?}" "${RUN:?}"
S3=${S3:-http://127.0.0.1:18333}; BUCKET=${BUCKET:-deploy-edge}; PREFIX=${PREFIX:-configs}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
W=$W/$RUN; D=$W/data; MD=$W/mdata; mkdir -p "$D" "$MD"
"$T/otlpgen" -out "$D" -ref "$S3/$BUCKET/$PREFIX/corr/$RUN/ref" -epoch "$RUN" > "$W/otlpgen.log" 2>&1
"$T/otlpgen" -metrics -out "$MD" > "$W/otlpgen-metrics.log" 2>&1
edge() { # cfg name s3url log
  setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/edge.pid" env MEM_SOURCE=rss OTLP_HTTP=127.0.0.1:14818 OTLP_GRPC=127.0.0.1:14817 \
    OTAP_GRPC=127.0.0.1:14819 ADMIN_HTTP=127.0.0.1:14820 BUFFER_DIR="$W/buf-$2" VERBOSE=true PRODUCER="corr-$2" S3_URL="$3" \
    "$BIN" -c "$here/otap-rs/configs/$1" >> "$4" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14818/ && break; sleep 0.1; done
}
stop() { # log min-commits: wait for the commits to settle (the durable buffer acks first), then SIGINT
  local last=-1 c
  for _ in $(seq 80); do c=$(grep -c ' rows=' "$1"); [ "$c" -ge "$2" ] && [ "$c" = "$last" ] && break; last=$c; sleep 1.5; done
  kill -INT "$(cat "$W/edge.pid")"; for _ in $(seq 50); do kill -0 "$(cat "$W/edge.pid")" 2>/dev/null || break; sleep 0.2; done
}
names=""; mnames=""
for cfg in edge.yaml edge-durable.yaml edge-otap.yaml; do
  name=${cfg%.yaml}; name=${name//-/_}; names="$names $name"; mnames="$mnames series_$name"
  log=$W/$name.edge.log
  edge "$cfg" "$name" "$S3/$BUCKET/$PREFIX/corr/$RUN/$name" "$log"
  n=0
  for sig in traces logs; do
    for ds in testgen-3000 nasty-700; do
      "$T/otlpsend" -url http://127.0.0.1:14818 -signal $sig -file "$D/$sig-$ds.pb" -n 1 -quiet >> "$W/$name.send.log" 2>&1
      n=$((n+1)); for _ in $(seq 40); do [ "$(grep -c ' rows=' "$log")" -ge $n ] && break; sleep 0.25; done
    done
  done
  stop "$log" 4
  # layout B: one edge run per dataset, each under its own prefix (slot 0)
  for f in testgen-3000 nasty-700 extra; do
    mlog=$W/series_$name.edge.log; before=$(grep -c ' rows=' "$mlog" 2>/dev/null || echo 0)
    edge "$cfg" "series_$name-$f" "$S3/$BUCKET/$PREFIX/corr/$RUN/series_$name/$f" "$mlog"
    "$T/otlpsend" -url http://127.0.0.1:14818 -signal metrics -file "$MD/metrics-$f.pb" -n 1 -timeout 120s -quiet >> "$W/series_$name.send.log" 2>&1
    stop "$mlog" $((before+1))
  done
done
"$T/otlpgen" -metrics -ref "$S3/$BUCKET/$PREFIX/corr/$RUN/ref" -epoch "$RUN" > "$W/otlpgen-metrics-ref.log" 2>&1
export S3_ROOT=$S3/$BUCKET S3_PREFIX=$PREFIX T
{
  echo "# configs_e2e $RUN: otap-rs/configs/{edge,edge-durable,edge-otap}.yaml, credentials from AWS_* env only"
  python3 "$here/otap-rs/scripts/correctness.py" "$RUN" $names 2>&1
  python3 "$here/otap-rs/scripts/metrics_correctness.py" "$RUN" "$MD" $mnames 2>&1
} > "$W/summary.txt"
rm -rf "$W"/buf-* "$D" "$MD"
grep -E '^(FAIL|[0-9]+ failed)' "$W/summary.txt"; grep -c '^PASS' "$W/summary.txt"
