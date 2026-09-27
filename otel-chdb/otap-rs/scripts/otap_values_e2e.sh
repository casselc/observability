#!/bin/bash
# The values the property tests found changing on the OTAP path (patch 0005,
# HEGEL.md, UPSTREAM_ISSUES.md U-entry), end to end: one OTLP/HTTP request
# per signal into the Rust edge (configs/edge.yaml) on the direct walk and on
# via_otap (upstream's OTLP -> OTAP conversion), both to S3; ClickHouse reads
# the objects with s3() (as the consumer's INSERT ... SELECT does) and the two
# paths must agree.
#
# The requests: a log whose body is the int 0 (alone in its batch), a log
# with the attributes a = [1.5, 7] and z = -0.0, and a span with no start
# time (proto3 leaves a 0 off the wire) ending at 1000 ns.
#
#   B=$CARGO_TARGET_DIR/release T=tools-bin scripts/otap_values_e2e.sh
# Before patch 0005 via_otap read Body "", a "[null,7]", z "0", Duration 0.
set -u
B=${B:?}; T=${T:?}
RUN=${RUN:-v$(date +%s)}
S3=${S3:-http://127.0.0.1:18333}
CH=${CH:-http://127.0.0.1:18123}
PREFIX=${PREFIX:-otap-rs-edge}
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
python3 - "$tmp" <<'EOF'
import struct, sys
def varint(n):
    out = b''
    while True:
        b, n = n & 0x7f, n >> 7
        if not n:
            return out + bytes([b])
        out += bytes([b | 0x80])
def ln(f, b): return varint(f << 3 | 2) + varint(len(b)) + b
def fx64(f, x): return varint(f << 3 | 1) + struct.pack('<Q', x)
def f64(f, x): return varint(f << 3 | 1) + struct.pack('<d', x)
def vi(f, x): return varint(f << 3) + varint(x)
def kv(k, v): return ln(1, k.encode()) + ln(2, v)
rec_a = fx64(1, 1790000000000000000) + ln(5, vi(3, 0))  # body: int 0, set explicitly
rec_b = (fx64(1, 1790000000000000001) + ln(5, ln(1, b"b"))
         + ln(6, kv("a", ln(5, ln(1, f64(4, 1.5)) + ln(1, vi(3, 7)))))
         + ln(6, kv("z", f64(4, -0.0))))
open(sys.argv[1] + '/logs.pb', 'wb').write(ln(1, ln(2, ln(2, rec_a) + ln(2, rec_b))))
span = ln(1, bytes([1] * 16)) + ln(2, bytes([2] * 8)) + ln(5, b"zero-start") + fx64(8, 1000)
open(sys.argv[1] + '/traces.pb', 'wb').write(ln(1, ln(2, ln(2, span))))
EOF
for path in direct via_otap; do
  ADMIN_HTTP=127.0.0.1:18581 PRODUCER=values-$path OTLP_PATH=$path OTLP_HTTP=127.0.0.1:14518 OTLP_GRPC=127.0.0.1:14517 \
    S3_URL=$S3/otel/$PREFIX/values/$RUN/$path "$B/otap-s3pq" -c "$here/configs/edge.yaml" > "$tmp/$path.log" 2>&1 &
  E=$!
  sleep 2
  for sig in logs traces; do "$T/otlpsend" -url http://127.0.0.1:14518 -signal $sig -file "$tmp/$sig.pb" -n 1 -quiet > /dev/null; done
  kill -INT $E; wait $E 2>/dev/null
  src="s3('$S3/otel/$PREFIX/values/$RUN/$path/%s/**.parquet', 'otel', 'otelsecret', 'Parquet')"
  q() { curl -sS "$CH/" --data-binary "$1"; }
  { q "SELECT Body, LogAttributes FROM $(printf "$src" logs) ORDER BY Timestamp FORMAT TSV"
    q "SELECT SpanName, Duration FROM $(printf "$src" traces) FORMAT TSV"; } > "$tmp/$path.out"
  echo "== $path"; cat "$tmp/$path.out"
done
if cmp -s "$tmp/direct.out" "$tmp/via_otap.out"; then echo "PASS: via_otap reads back as direct"; rc=0
else echo "FAIL: via_otap differs from direct"; rc=1; fi
rm -rf "$tmp"
exit $rc
