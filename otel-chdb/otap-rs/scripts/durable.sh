#!/bin/bash
# Edge durability with upstream's durable buffer (configs/edge-durable.yaml:
# OTLP receiver -> processor:durable_buffer (Quiver WAL + segments) ->
# exporter:s3pq). A request is acknowledged to the client once it is in the
# WAL; the claim checked here is that every request acknowledged before a
# SIGKILL of the edge ends up committed in S3 after a restart.
#
#   baseline  12 distinct 10k-span requests through a plain edge: each
#             object's fingerprint (xor of cityHash64 over the rows' content)
#   s3down    the edge's S3 endpoint is a closed port: all 12 requests are
#             acked from the WAL, nothing can commit; SIGKILL; restart with S3
#             up: every fingerprint must be in S3
#   midflight S3 through faultproxy2 holding every PUT's answer 5 s (the
#             object lands, the exporter doesn't learn it); the sender streams
#             the 12 requests, the edge is SIGKILLed 0.8 s in, then restarted:
#             every request acked before the kill must be in S3 (a copy of one
#             committed by both incarnations is expected: the consumer's
#             content check drops it)
#   control   as s3down, but the restart gets an empty buffer directory:
#             the check must report every acked request missing
#   cost      30 distinct requests through each config: edge CPU and bytes
#             written to disk (/proc/<pid>/io) per request, ack latency, and
#             the delay to the commit
#
#   B=target/release T=tools-bin D=data OUT=results/durable scripts/durable.sh   (SKIP_COST=1 / ONLY_COST=1)
set -u
B=${B:?}; T=${T:?}; D=${D:?}; OUT=${OUT:?}
S3=${S3:-http://127.0.0.1:18333}
PREFIX=${PREFIX:-otap-rs-edge/durable}
CH=${CH:-http://127.0.0.1:18123}
RUN=${RUN:-d$(date +%s)}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
mkdir -p "$OUT"
FILES=$(for i in $(seq -w 0 11); do echo "$D/traces-bench-v$i.pb"; done | paste -sd,)
port=14618

q() { curl -s "$CH/" --data-binary "$1"; }
# The bench variants share their ids and differ in timestamps and attributes.
FPCOLS='Timestamp DateTime64(9), TraceId String, SpanId String, SpanName String, Duration UInt64, SpanAttributes Map(String, String)'
fps() { # prefix -> "count fp" lines, one per data object
  q "SELECT count(), groupBitXor(cityHash64(*)) FROM s3('$S3/otel/$1/**.parquet', 'otel', 'otelsecret', 'Parquet', '$FPCOLS') GROUP BY _path ORDER BY 2 FORMAT TSV"
}
edge() { # config s3url buffer-dir log  (starts in background, prints the pid)
  BUFFER_DIR=$3 VERBOSE=true OTLP_HTTP=127.0.0.1:$port OTLP_GRPC=127.0.0.1:$((port-1)) PRODUCER=durable-$RUN \
    PUT_TIMEOUT=2s S3_URL=$2 "$B/otap-s3pq" -c "$here/configs/$1" >> "$4" 2>&1 &
  echo $!
}
ready() { for _ in $(seq 50); do curl -s -o /dev/null "http://127.0.0.1:$port/" && return; sleep 0.1; done; }

if [ -z "${ONLY_COST:-}" ]; then
# ---- baseline ----
p=$(edge edge.yaml "$S3/otel/$PREFIX/$RUN/baseline" "" "$OUT/baseline.edge.log"); ready
"$T/otlpsend" -url http://127.0.0.1:$port -signal traces -file "$FILES" -n 12 > "$OUT/baseline.send.jsonl"
kill -INT $p; wait $p 2>/dev/null
fps "$PREFIX/$RUN/baseline" > "$tmp/base.fp"
echo "baseline: $(wc -l < "$tmp/base.fp") objects" | tee "$OUT/summary.txt"

# Map each file to its fingerprint: the baseline objects are slots 0..11 in request order.
q "SELECT _path, groupBitXor(cityHash64(*)) FROM s3('$S3/otel/$PREFIX/$RUN/baseline/**.parquet', 'otel', 'otelsecret', 'Parquet', '$FPCOLS') GROUP BY _path ORDER BY _path FORMAT TSV" \
  | awk -F'\t' '{print $2}' > "$tmp/base.byslot"
echo "$FILES" | tr ',' '\n' | paste - "$tmp/base.byslot" > "$OUT/baseline.file-fp.tsv"

verify() { # scenario prefix sendlog kill_ns
  fps "$2" | awk -F'\t' '{print $2}' | sort > "$tmp/got"
  python3 - "$1" "$OUT/baseline.file-fp.tsv" "$tmp/got" "$3" "$4" <<'EOF' | tee -a "$OUT/summary.txt"
import sys, json, collections
name, base, got, send, kill_ns = sys.argv[1:]
fp = dict(l.split("\t") for l in open(base).read().split("\n") if l)
got = collections.Counter(l for l in open(got).read().split("\n") if l)
rs = [json.loads(l) for l in open(send) if l.startswith('{"seq"')]
kill = int(kill_ns)
before = [r for r in rs if r["attempts"] == 1 and r["sent_ns"] + r["ack_ms"] * 1e6 < kill]
missing = [r["seq"] for r in before if got[fp[r["file"]]] == 0]
dups = sum(n - 1 for n in got.values() if n > 1)
allin = sum(1 for f in fp.values() if got[f] > 0)
print(f"{name}: {len(before)} of {len(rs)} requests acked before the SIGKILL; after restart, "
      f"{'ALL present' if not missing else 'MISSING ' + str(missing)}; {allin}/12 distinct requests in S3, "
      f"{sum(got.values())} objects ({dups} cross-incarnation copies)")
EOF
}

# ---- s3down ----
buf=$tmp/buf-s3down
p=$(edge edge-durable.yaml "http://127.0.0.1:18399/otel/$PREFIX/$RUN/s3down" "$buf" "$OUT/s3down.edge.log"); ready
"$T/otlpsend" -url http://127.0.0.1:$port -signal traces -file "$FILES" -n 12 > "$OUT/s3down.send.jsonl"
sleep 2
kill_ns=$(date +%s%N); kill -KILL $p; wait $p 2>/dev/null
echo "s3down: buffer after kill: $(du -sk "$buf" | cut -f1) KB" | tee -a "$OUT/summary.txt"
p=$(edge edge-durable.yaml "$S3/otel/$PREFIX/$RUN/s3down" "$buf" "$OUT/s3down.edge.log"); ready
for _ in $(seq 60); do [ "$(fps "$PREFIX/$RUN/s3down" | wc -l)" -ge 12 ] && break; sleep 1; done
sleep 2; kill -INT $p; wait $p 2>/dev/null
verify s3down "$PREFIX/$RUN/s3down" "$OUT/s3down.send.jsonl" "$kill_ns"

# ---- control: the same, but the restart gets an empty buffer directory
# (the WAL lost): the check must report the acked requests missing ----
buf=$tmp/buf-wiped
p=$(edge edge-durable.yaml "http://127.0.0.1:18399/otel/$PREFIX/$RUN/wiped" "$buf" "$OUT/wiped.edge.log"); ready
"$T/otlpsend" -url http://127.0.0.1:$port -signal traces -file "$FILES" -n 12 > "$OUT/wiped.send.jsonl"
sleep 2
kill_ns=$(date +%s%N); kill -KILL $p; wait $p 2>/dev/null
rm -rf "$buf"
p=$(edge edge-durable.yaml "$S3/otel/$PREFIX/$RUN/wiped" "$buf" "$OUT/wiped.edge.log"); ready
sleep 5; kill -INT $p; wait $p 2>/dev/null
verify "control (WAL wiped before the restart)" "$PREFIX/$RUN/wiped" "$OUT/wiped.send.jsonl" "$kill_ns"

# ---- midflight ----
buf=$tmp/buf-mid
"$T/faultproxy2" -listen 127.0.0.1:18336 -match "/$RUN/mid/" -mode answer-late -hold 5s > "$OUT/midflight.proxy.log" 2>&1 &
fp_pid=$!
sleep 0.5
p=$(edge edge-durable.yaml "http://127.0.0.1:18336/otel/$PREFIX/$RUN/mid" "$buf" "$OUT/midflight.edge.log"); ready
"$T/otlpsend" -url http://127.0.0.1:$port -signal traces -file "$FILES" -n 12 -interval 100ms -backoff 300ms > "$OUT/midflight.send.jsonl" 2>"$OUT/midflight.send.err" &
s_pid=$!
sleep 0.8
kill_ns=$(date +%s%N); kill -KILL $p; wait $p 2>/dev/null
kill $fp_pid; wait $fp_pid 2>/dev/null
p=$(edge edge-durable.yaml "$S3/otel/$PREFIX/$RUN/mid" "$buf" "$OUT/midflight.edge.log"); ready
wait $s_pid
for _ in $(seq 60); do [ "$(fps "$PREFIX/$RUN/mid" | wc -l)" -ge 12 ] && break; sleep 1; done
sleep 3; kill -INT $p; wait $p 2>/dev/null
verify midflight "$PREFIX/$RUN/mid" "$OUT/midflight.send.jsonl" "$kill_ns"

fi
# ---- cost ----
[ "${SKIP_COST:-}" ] && { rm -rf "$tmp"; exit 0; }
cpu_ms() { awk '{print ($14+$15)*1000/'"$(getconf CLK_TCK)"'}' /proc/$1/stat; }
wbytes() { awk '/^write_bytes/{print $2}' /proc/$1/io; }
ALL=$(ls "$D"/traces-bench-v*.pb | head -30 | paste -sd,)
for rep in 1 2 3; do
  for cfg in edge.yaml edge-durable.yaml; do
    buf=$tmp/buf-cost-$rep; rm -rf "$buf"
    p=$(edge $cfg "$S3/otel/$PREFIX/$RUN/cost-$cfg-$rep" "$buf" "$tmp/cost.log"); ready
    "$T/otlpsend" -url http://127.0.0.1:$port -signal traces -file "$D/traces-testgen-3000.pb" -n 2 -quiet > /dev/null
    c0=$(cpu_ms $p); w0=$(wbytes $p); t0=$(date +%s%N)
    sum=$("$T/otlpsend" -url http://127.0.0.1:$port -signal traces -file "$ALL" -n 30 -quiet | tail -1)
    # the buffered edge acks before the commit: wait for the 30 objects
    for _ in $(seq 100); do [ "$(fps "$PREFIX/$RUN/cost-$cfg-$rep" | wc -l)" -ge 31 ] && break; sleep 0.1; done
    t1=$(date +%s%N); c1=$(cpu_ms $p); w1=$(wbytes $p)
    peak=$(du -sk "$buf" 2>/dev/null | cut -f1)
    kill -INT $p; wait $p 2>/dev/null
    python3 -c "
import json
s=json.loads('''$sum''')
print(json.dumps({'config':'$cfg','rep':$rep,'cpu_ms_per_request':($c1-$c0)/30,'disk_write_kb_per_request':($w1-$w0)/30/1024,
 'buffer_dir_kb_after':${peak:-0},'median_ack_ms':s['median_ack_ms'],'max_ack_ms':s['max_ack_ms'],
 'send_to_all_committed_s':($t1-$t0)/1e9,'load':open('/proc/loadavg').read().split()[0]}))" | tee -a "$OUT/cost.jsonl"
  done
done
rm -rf "$tmp"
