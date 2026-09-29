#!/bin/bash
# The query service's KMS basis signer against moto server's KMS emulator
# (CreateKey HMAC_256, DescribeKey, GenerateMac, VerifyMac; DECISIONS D30
# amendment of 2026-09-28): the startup check, two replicas verifying each
# other's tokens, tampering, and mint/verify latency (logged).
#
#   ci/kms-emulator.sh [PORT]     needs moto_server on PATH: pip install 'moto[server]'
set -euo pipefail
port=${1:-18555}
root=$(cd "$(dirname "$0")/.." && pwd)
log=${TMPDIR:-/tmp}/moto-kms-$port.log
moto_server -H 127.0.0.1 -p "$port" > "$log" 2>&1 &
pid=$!
trap 'kill "$pid" 2> /dev/null || true' EXIT
for _ in $(seq 1 120); do
  curl -s -o /dev/null "http://127.0.0.1:$port/" && break
  sleep 0.5
done
curl -s -o /dev/null "http://127.0.0.1:$port/" || { cat "$log"; echo "moto_server did not start" >&2; exit 1; }
cd "$root/otel-chdb/query"
# moto is up, so the test must run (testgate: kms required, CAST row 43).
OSCOPE_REQUIRE_SERVICES="${OSCOPE_REQUIRE_SERVICES:+$OSCOPE_REQUIRE_SERVICES,}kms" \
  QS_TEST_KMS_ENDPOINT="http://127.0.0.1:$port" go test -count=1 -run TestKMSEmulator -v ./internal/app/
