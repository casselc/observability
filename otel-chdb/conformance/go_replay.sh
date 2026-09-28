#!/bin/bash
# The Go publisher's received_at across a persistent-queue replay.
#
# A request whose commit lands but which is never removed from the
# collector's persistent queue (the publisher dies while the PUT's answer is
# in flight) is replayed by the next incarnation, in a new epoch. The s3pq
# exporter stamps received_at into the request's client metadata as it takes
# the request, before the queue; the file_storage queue keeps it with the
# request, so the replay carries the original's value (not the restart's
# clock), lands in the original's toDate(received_at) partition, and the
# consumer's count check skips it however long the publisher was down.
#
# The publisher is ../deploy/base/go/publisher-config.yaml (file_storage
# queue, 2 lanes) on otelcol-s3pq, behind faultproxy2 holding every PUT's
# answer 5 s; 4 traces requests; SIGKILL once objects have landed; GAP
# seconds later a restart on the same queue, straight to S3. The consumer
# (default check horizon) runs during the outage and after the restart,
# then its horizon audit. The check: every
# replayed copy has its original's received_at in metadata and rows (the
# restart's clock is >= GAP s later), central holds each request once, the
# audit is silent. (The Go runtime reads the clock through the vDSO, so the
# gap is real time here; ../otap-rs/scripts/replay_received.sh shifts the
# Rust edge's clock by 4 days, and parquetgo/s3pqexporter's
# TestReceivedSurvivesTheQueue does the same for Go with a fake clock.)
#
#   B=dir-with-otelcol-s3pq-consume-otlpsend-faultproxy2 D=dir-with-traces-bench-vNN.pb OUT=dir go_replay.sh
set -u
B=${B:?}; D=${D:?}; OUT=${OUT:?}
S3=${S3:-http://127.0.0.1:18333}
CH=${CH:-http://127.0.0.1:18123}
BUCKET=${BUCKET:-stamp-goreplay}
DBP=${DBP:-stamp_goreplay_}
GAP=${GAP:-10}
RUN=${RUN:-g$(date +%s)}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
mkdir -p "$OUT"
N=4
FILES=$(ls "$D"/traces-bench-v0[0-3].pb | paste -sd,)
proxy=127.0.0.1:18347
db=$DBP$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)
q() { curl -sS "$CH/" --data-binary "$1"; }
s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" "$@"; }
s3 -X PUT "$S3/$BUCKET" > /dev/null

sed -e 's#0.0.0.0:4317#127.0.0.1:14817#; s#0.0.0.0:4318#127.0.0.1:14818#; s#0.0.0.0:13133#127.0.0.1:14833#' \
    -e 's#host: 0.0.0.0#host: 127.0.0.1#; s#port: 8888#port: 14888#' "$here/deploy/base/go/publisher-config.yaml" > "$tmp/publisher.yaml"
export CLUSTER=replay PRODUCER=goreplay S3_REGION=us-east-1 QUEUE_DIR=$tmp/queue
pub() { # s3-base  (background; the pid in $tmp/pub.pid)
  S3_BASE=$1 setsid bash -c 'echo $$ > "$0"; exec "$@"' "$tmp/pub.pid" "$B/otelcol-s3pq" --config "$tmp/publisher.yaml" >> "$OUT/pub.log" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14818/ && return; sleep 0.1; done
}
count() { q "SELECT count() FROM s3('$S3/$BUCKET/$RUN/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'One') WHERE _size > 0" 2>/dev/null || echo 0; }

"$B/faultproxy2" -listen $proxy -target "$S3" -match "/$RUN/" -mode answer-late -hold 5s > "$OUT/proxy.log" 2>&1 &
fp=$!
sleep 0.5
pub "http://$proxy/$BUCKET/$RUN"
"$B/otlpsend" -url http://127.0.0.1:14818 -signal traces -file "$FILES" -n $N -backoff 300ms > "$OUT/send.jsonl" 2> "$OUT/send.err"
for _ in $(seq 40); do [ "$(count)" -ge 1 ] && break; sleep 0.1; done
sleep 0.5
kill -KILL "$(cat "$tmp/pub.pid")"; kill $fp; wait $fp 2>/dev/null
before=$(count)
echo "SIGKILL with $before of $N objects landed, none removed from the queue; restart in ${GAP}s" | tee "$OUT/summary.txt"
consume() {
  "$B/consume" --s3 "$S3/$BUCKET/$RUN" --ch "$CH" --db "$db" --signals traces --exit-after-idle 5s --poll 300ms \
    --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY"
}
q "DROP DATABASE IF EXISTS $db"
consume > "$OUT/consume.log" 2>&1   # the originals, during the outage
sleep "$GAP"
restart_ns=$(date +%s%N)
pub "$S3/$BUCKET/$RUN"
for _ in $(seq 150); do [ "$(count)" -ge $((N + before)) ] && break; sleep 0.2; done
sleep 1; kill -INT "$(cat "$tmp/pub.pid")"; sleep 3

q "SELECT _path, toUnixTimestamp64Nano(min(received_at)), toUnixTimestamp64Nano(max(received_at)), count()
   FROM s3('$S3/$BUCKET/$RUN/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'received_at DateTime64(9)')
   GROUP BY _path ORDER BY _path FORMAT TSV SETTINGS s3_skip_empty_files = 1" | while IFS=$'\t' read -r path lo hi rows; do
  h=$(s3 -I "$S3/$path" | tr -d '\r')
  m() { echo "$h" | awk -F': ' -v k="x-amz-meta-oscope-$1" 'tolower($1)==k{print $2}'; }
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "${path#"$BUCKET/"}" "$(m content)" "$(m received)" "$lo" "$hi" "$rows"
done > "$OUT/objects.tsv"

consume >> "$OUT/consume.log" 2>&1  # the replays
"$B/consume" horizon-audit --s3 "$S3/$BUCKET/$RUN" --ch "$CH" --db "$db" --audit-no-state \
  --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > "$OUT/audit.log" 2>&1
q "SELECT count(), uniqExact(content_key), uniqExact(producer_epoch), uniqExact(received_at) FROM $db.otel_traces FORMAT TSV" > "$tmp/central"
python3 - "$OUT/objects.tsv" "$tmp/central" "$OUT/audit.log" "$N" "$restart_ns" <<'EOF' | tee -a "$OUT/summary.txt"
import sys, collections
objs, central, audit, n, restart = sys.argv[1:]
n, restart = int(n), int(restart)
rows = [l.split("\t") for l in open(objs).read().splitlines() if l]
by = collections.defaultdict(list)
for key, content, meta, lo, hi, cnt in rows:
    by[content].append((key.split("/")[-2], int(meta or 0), int(lo), int(hi)))
agree = all(m == lo == hi for c in by.values() for _, m, lo, hi in c)
copies = {c: v for c, v in by.items() if len(v) > 1}
kept = [c for c, v in copies.items() if len({m for _, m, _, _ in v}) == 1]
before = all(m < restart for c in copies.values() for _, m, _, _ in c)
c_rows, c_keys, c_epochs, c_recv = open(central).read().split()
warns = sum(1 for l in open(audit) if "WARN horizon-audit" in l)
print(f"go: {len(rows)} objects, {len(by)} requests, {len(copies)} replayed in a second epoch; "
      f"metadata == rows' received_at in every object: {agree}; replays with the original's received_at: "
      f"{len(kept)}/{len(copies)} (all before the restart: {before})")
print(f"  central: {c_rows} rows (expected {n * 10000}), {c_keys} requests, {c_epochs} epochs; horizon-audit WARN lines: {warns}")
ok = agree and copies and len(kept) == len(copies) and before and int(c_rows) == n * 10000 and warns == 0
print(f"  {'PASS' if ok else 'FAIL'}")
EOF
grep -o 's3pq stop.*' "$OUT/pub.log" | tail -2 | cut -c1-300 | sed 's/^/  publisher: /' | tee -a "$OUT/summary.txt"
q "DROP DATABASE IF EXISTS $db"
# Only this run's objects: the bucket may be shared (SeaweedFS deletes a
# bucket that still holds objects, everyone's).
[ -n "${KEEP:-}" ] || "$B/consume" purge --s3 "$S3/$BUCKET/$RUN" --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > /dev/null 2>&1
rm -rf "$tmp"
