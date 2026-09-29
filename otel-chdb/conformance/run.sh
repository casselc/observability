#!/bin/bash
# Cross-edge conformance: the same OTLP requests through the Rust edge
# (otap-s3pq, configs/edge.yaml) and the Go edge (otelcol-s3pq, go-edge.yaml),
# each into its own S3 root; then the Rust consumer (`consume`) ingests each
# root into its own ClickHouse database, and compare.py checks that the two
# databases hold the same rows, table by table (and the same objects: Parquet
# schema, metadata, content keys).
#
#   B=dir-with-binaries D=datasets RUN=name conformance/run.sh
#
# B holds otap-s3pq, consume, otelcol-s3pq, otlpsend; D the request files
# of otap-rs/tools/cmd/otlpgen (-out, and -metrics -out) and conformance/gen.
# Env: S3 (http://127.0.0.1:18333), BUCKET (goedge-conf), CH
# (http://127.0.0.1:18123), DATASETS (below), LAYOUT (series_table), KEEP=1
# keeps the databases.
set -u
here=$(cd "$(dirname "$0")" && pwd)
: "${B:?}" "${D:?}"
RUN=${RUN:-c$(date +%s)}
S3=${S3:-http://127.0.0.1:18333}; BUCKET=${BUCKET:-goedge-conf}; CH=${CH:-http://127.0.0.1:18123}
LAYOUT=${LAYOUT:-series_table}
# The request cap (offload.max_request_bytes, D36) above the largest dataset
# (traces-nasty-700 is 85 MB): the datasets are compared, not refused.
MAXREQ=${MAXREQ:-268435456}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret} AWS_REGION=us-east-1
DATASETS=${DATASETS:-"traces-testgen-3000 traces-nasty-700 traces-hostile traces-genai logs-testgen-3000 logs-nasty-700 logs-hostile logs-genai
  metrics-testgen-3000 metrics-nasty-700 metrics-extra metrics-hostile metrics-mixed-10000"}
W=${W:-/tmp/goedge-conf}/$RUN; mkdir -p "$W"
tag=$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)

send() { # edge-port: every dataset, in order, each resent until 2xx
  for ds in $DATASETS; do
    "$B/otlpsend" -url "http://$1" -signal "${ds%%-*}" -file "$D/$ds.pb" -n 1 -timeout 120s -quiet >> "$W/send-$2.log" 2>&1 \
      || echo "send $ds to $2 failed" >> "$W/send-$2.log"
  done
}
wait_up() { for _ in $(seq 150); do curl -s -o /dev/null "http://$1/" && return 0; sleep 0.2; done; echo "edge on $1 did not start"; return 1; }

# Rust edge
ADMIN_HTTP=127.0.0.1:14480 OTLP_HTTP=127.0.0.1:14418 OTLP_GRPC=127.0.0.1:14417 PRODUCER=edge-rust METRICS_LAYOUT=$LAYOUT \
  CLUSTER=conf HEARTBEAT=1h OFFLOAD_MAX_REQUEST=$MAXREQ S3_URL=$S3/$BUCKET/$RUN/rust "$B/otap-s3pq" -c "$here/../otap-rs/configs/edge.yaml" > "$W/edge-rust.log" 2>&1 &
E=$!; wait_up 127.0.0.1:14418 && send 127.0.0.1:14418 rust; kill -INT $E; wait $E 2>/dev/null

# Go edge
OTLP_HTTP=127.0.0.1:14518 HEALTH=127.0.0.1:14533 PRODUCER=edge-go METRICS_LAYOUT=$LAYOUT \
  CLUSTER=conf HEARTBEAT=1h OFFLOAD_MAX_REQUEST=$MAXREQ S3_URL=$S3/$BUCKET/$RUN/go "$B/otelcol-s3pq" --config "$here/go-edge.yaml" > "$W/edge-go.log" 2>&1 &
E=$!; wait_up 127.0.0.1:14518 && send 127.0.0.1:14518 go; kill -INT $E; wait $E 2>/dev/null

# The consumer, one root and one database per edge (format v2: {root}/conf/edge-{rust,go}/{signal}/...).
for edge in rust go; do
  curl -s "$CH/" --data-binary "DROP DATABASE IF EXISTS goedge_${tag}_$edge" > /dev/null
  "$B/consume" --s3 "$S3/$BUCKET/$RUN/$edge" --ch "$CH" --db "goedge_${tag}_$edge" --exit-after-idle 5s --poll 300ms \
    --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > "$W/consume-$edge.log" 2>&1 \
    || echo "consume $edge exited $?" >> "$W/consume-$edge.log"
done
python3 "$here/compare.py" "$tag" "$S3/$BUCKET/$RUN" | tee "$W/compare.txt"
rc=${PIPESTATUS[0]}
# Every dataset must have gone in on both edges: a request an edge refused
# (permanent) or a send that failed is a dataset neither side compares, and
# the comparison would pass on less data (both edges refusing alike).
for edge in rust go; do
  if grep -Eq "permanent|failed" "$W/send-$edge.log"; then
    echo "FAIL send to $edge: $(grep -Ec 'permanent|failed' "$W/send-$edge.log") refused or failed requests (send-$edge.log)" | tee -a "$W/compare.txt"
    rc=1
  fi
done
if [ -z "${KEEP:-}" ]; then
  for edge in rust go; do curl -s "$CH/" --data-binary "DROP DATABASE IF EXISTS goedge_${tag}_$edge" > /dev/null; done
fi
exit "$rc"
