#!/bin/bash
# received_at across a durable-buffer replay after a long outage.
#
# A request whose commit lands but whose ACK is lost (the edge dies while
# the PUT's answer is in flight) is replayed from the WAL by the next
# incarnation, in a new epoch. The replay must carry the original's
# received_at (the WAL write, persisted by Quiver: patches/0003), so it lands
# in the original's toDate(received_at) partition and the consumer's count
# check skips it however long the edge was down.
#
# Per scenario: edge-durable.yaml behind faultproxy2 holding every PUT's
# answer 5 s (the object lands, the exporter doesn't learn it); 4 requests
# (LANES=4, so several commit at once); SIGKILL once objects have landed;
# restart on the same buffer with the wall clock 4 days ahead (GAP_S,
# LD_PRELOAD shim; S3 then goes to an unauthenticated gateway on the same
# filer, unsigned, since a 4-day skew fails SigV4). The consumer (default
# check horizon, 3 days) runs once during the outage (it ingests the
# originals) and once after the restart, then its horizon audit.
#
#   design   the edge under test ($B/otap-s3pq): every replayed copy has the
#            original's received_at, in metadata and rows; central holds each
#            request once; the audit is silent
#   control  OLD=path to an edge that stamps at receipt: the copies carry the
#            restart's clock, the check misses the originals, central holds
#            them twice and the audit reports them
#
#   B=target/release T=tools-bin D=data OUT=results/replay-received [OLD=...] scripts/replay_received.sh
# B holds otap-s3pq and consume; T otlpsend and faultproxy2; D traces-bench-vNN.pb.
set -u
B=${B:?}; T=${T:?}; D=${D:?}; OUT=${OUT:?}
S3=${S3:-http://127.0.0.1:18333}
NOAUTH_PORT=${NOAUTH_PORT:-18343}
FILER=${FILER:-127.0.0.1:18888}
MASTER=${MASTER:-127.0.0.1:19333}
WEED=${WEED:-weed}
CH=${CH:-http://127.0.0.1:18123}
BUCKET=${BUCKET:-stamp-replay}
DBP=${DBP:-stamp_replay_}
GAP_S=${GAP_S:-345600}          # 4 days: beyond the consumer's 3-day check horizon
RUN=${RUN:-r$(date +%s)}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
mkdir -p "$OUT"
port=14718; admin=18480; proxy=127.0.0.1:18346
FILES=$(ls "$D"/traces-bench-v0[0-3].pb | paste -sd,)
N=4

q() { curl -sS "$CH/" --data-binary "$1"; }
cat > "$tmp/clockshift.c" <<'EOF'
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdlib.h>
#include <time.h>
static int (*real_cg)(clockid_t, struct timespec *);
static long long shift = -1;
int clock_gettime(clockid_t c, struct timespec *ts) {
  if (!real_cg) real_cg = (int (*)(clockid_t, struct timespec *))dlsym(RTLD_NEXT, "clock_gettime");
  if (shift < 0) { const char *e = getenv("CLOCK_SHIFT_S"); shift = e ? atoll(e) : 0; }
  int r = real_cg(c, ts);
  if (r == 0 && (c == CLOCK_REALTIME || c == CLOCK_REALTIME_COARSE)) ts->tv_sec += shift;
  return r;
}
EOF
cc -O2 -shared -fPIC -o "$tmp/clockshift.so" "$tmp/clockshift.c" -ldl || exit 1
echo "s3.bucket.create -name $BUCKET" | "$WEED" shell -master=$MASTER > /dev/null 2>&1
# An anonymous gateway (a 4-day skew fails SigV4): the restarted edge sends unsigned requests.
echo '{"identities":[{"name":"anonymous","actions":["Admin","Read","Write","List","Tagging"]}]}' > "$tmp/anon.json"
"$WEED" s3 -config="$tmp/anon.json" -filer=$FILER -ip.bind=127.0.0.1 -port=$NOAUTH_PORT -port.grpc=$((NOAUTH_PORT + 10000)) -port.iceberg=0 -port.lance=0 > "$tmp/noauth.log" 2>&1 &
noauth=$!
for _ in $(seq 50); do curl -s -o /dev/null "http://127.0.0.1:$NOAUTH_PORT/" && break; sleep 0.2; done

edge() { # bin s3url buffer-dir log [shift_s]  (background; prints the pid)
  env ${5:+LD_PRELOAD=$tmp/clockshift.so CLOCK_SHIFT_S=$5 AWS_SKIP_SIGNATURE=true} BUFFER_DIR=$3 VERBOSE=true LANES=4 \
    OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port - 1)) ADMIN_HTTP=127.0.0.1:$admin \
    PRODUCER=edge PUT_TIMEOUT=2s S3_URL=$2 "$1" -c "$here/configs/edge-durable.yaml" >> "$4" 2>&1 &
  echo $!
}
ready() { for _ in $(seq 100); do curl -s -o /dev/null "http://127.0.0.1:$port/" && return; sleep 0.1; done; }
objs() { # prefix -> "path \t min(row received_at) \t max \t rows", one line per data object
  q "SELECT _path, toUnixTimestamp64Nano(min(received_at)), toUnixTimestamp64Nano(max(received_at)), count()
     FROM s3('$S3/$BUCKET/$1/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'received_at DateTime64(9)')
     GROUP BY _path ORDER BY _path FORMAT TSV SETTINGS s3_skip_empty_files = 1" 2>/dev/null
}
head_meta() { # path-in-bucket name -> x-amz-meta-oscope-<name> (HEAD through the unauthenticated gateway)
  curl -sSI "http://127.0.0.1:$NOAUTH_PORT/$BUCKET/$1" | tr -d '\r' | awk -F': ' -v k="x-amz-meta-oscope-$2" 'tolower($1)==k{print $2}'
}
count() { q "SELECT count() FROM s3('$S3/$BUCKET/$1/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'One') WHERE _size > 0 SETTINGS s3_skip_empty_files = 1" 2>/dev/null || echo 0; }

consume() { # the consumer, once over the scenario's root (uses $root, $db)
  "$B/consume" --s3 "$S3/$BUCKET/$root" --ch "$CH" --db "$db" --signals traces --exit-after-idle 5s --poll 300ms \
    --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY"
}
scenario() { # name edge-binary
  local name=$1 bin=$2 root=$RUN/$1 buf=$tmp/buf-$1 log=$OUT/$1.edge.log
  local db=$DBP$(echo "${RUN}_$1" | tr -c 'a-zA-Z0-9_\n' _)
  : > "$log"
  "$T/faultproxy2" -listen $proxy -target "$S3" -match "/$root/" -mode answer-late -hold 5s > "$OUT/$name.proxy.log" 2>&1 &
  local fp=$!
  sleep 0.5
  local p; p=$(edge "$bin" "http://$proxy/$BUCKET/$root" "$buf" "$log"); ready
  "$T/otlpsend" -url http://127.0.0.1:$port -signal traces -file "$FILES" -n $N -backoff 300ms > "$OUT/$name.send.jsonl" 2> "$OUT/$name.send.err"
  # every request is acked from the WAL; wait for objects to land, then kill
  # while their answers are held
  for _ in $(seq 40); do [ "$(count "$root")" -ge 1 ] && break; sleep 0.1; done
  sleep 0.5
  kill -KILL $p; wait $p 2>/dev/null
  kill $fp; wait $fp 2>/dev/null
  local before; before=$(count "$root")
  echo "$name: SIGKILL with $before of $N objects landed, none acked" | tee -a "$OUT/summary.txt"
  # the consumer ingests the originals during the outage (as it would: the
  # replay comes days later); one run per phase, so nothing but the check
  # can tell a later copy from its original
  q "DROP DATABASE IF EXISTS $db"
  consume > "$OUT/$name.consume.log" 2>&1
  sleep 2
  p=$(edge "$bin" "http://127.0.0.1:$NOAUTH_PORT/$BUCKET/$root" "$buf" "$log" "$GAP_S"); ready
  for _ in $(seq 100); do [ "$(count "$root")" -ge $((N + before)) ] && break; sleep 0.2; done
  sleep 1; kill -INT $p; wait $p 2>/dev/null
  # every object: its metadata's received_at and its rows'
  objs "$root" | while IFS=$'\t' read -r path lo hi rows; do
    key=${path#"$BUCKET/"}
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$key" "$(head_meta "$key" content)" "$(head_meta "$key" received)" "$lo" "$hi" "$rows"
  done > "$OUT/$name.objects.tsv"
  # central: the consumer (default check horizon), then the horizon audit
  consume >> "$OUT/$name.consume.log" 2>&1
  "$B/consume" horizon-audit --s3 "$S3/$BUCKET/$root" --ch "$CH" --db "$db" --audit-no-state \
    --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" > "$OUT/$name.audit.log" 2>&1
  q "SELECT count(), uniqExact(content_key), uniqExact(producer_epoch), uniqExact(received_at) FROM $db.otel_traces FORMAT TSV" > "$tmp/central"
  python3 - "$name" "$OUT/$name.objects.tsv" "$tmp/central" "$OUT/$name.audit.log" "$N" "$GAP_S" <<'EOF' | tee -a "$OUT/summary.txt"
import sys, collections
name, objs, central, audit, n, gap = sys.argv[1:]
n, gap = int(n), int(gap)
rows = [l.split("\t") for l in open(objs).read().splitlines() if l]
by = collections.defaultdict(list)
for key, content, meta, lo, hi, cnt in rows:
    by[content].append((key.split("/")[-2], int(meta or 0), int(lo), int(hi), int(cnt)))
agree = all(m == lo == hi for c in by.values() for _, m, lo, hi, _ in c)
copies = {c: v for c, v in by.items() if len(v) > 1}
kept = [c for c, v in copies.items() if len({m for _, m, _, _, _ in v}) == 1]
drift = [max(m for _, m, _, _, _ in v) - min(m for _, m, _, _, _ in v) for v in copies.values()]
c_rows, c_keys, c_epochs, c_recv = open(central).read().split()
warns = sum(1 for l in open(audit) if "WARN horizon-audit" in l)
print(f"{name}: {len(rows)} objects, {len(by)} requests, {len(copies)} replayed in a second epoch; "
      f"metadata == rows' received_at in every object: {agree}; "
      f"replays with the original's received_at: {len(kept)}/{len(copies)}"
      + (f" (others {max(drift)/86400e9:.1f} days later)" if len(kept) < len(copies) else ""))
print(f"  central: {c_rows} rows (expected {n * 10000}), {c_keys} requests, {c_epochs} epochs; "
      f"horizon-audit WARN lines: {warns}")
ok = agree and len(copies) > 0 and int(c_rows) == n * 10000 and len(kept) == len(copies) and warns == 0
verdict = ("PASS" if ok else "FAIL") if name == "design" else ("mutant caught" if not ok else "mutant NOT caught")
print(f"  {verdict} (the design's claim: every replay keeps received_at, central holds each request once, the audit is silent)")
EOF
  q "DROP DATABASE IF EXISTS $db"
}

scenario design "$B/otap-s3pq"
[ -n "${OLD:-}" ] && scenario control "$OLD"

kill $noauth; wait $noauth 2>/dev/null
[ -n "${KEEP:-}" ] || echo "s3.bucket.delete -name $BUCKET" | "$WEED" shell -master=$MASTER > /dev/null 2>&1
rm -rf "$tmp"
