#!/bin/bash
# Run measure.sql's blocks against the mirror central and keep each result.
#
#   CH=http://host:8123 DB=mirror FROM='2026-10-01 00:00:00' TO='2026-10-02 00:00:00' OUT=dir \
#   [S3_GLOB="'https://s3.us-east-1.amazonaws.com/B/validation/v1/edge/*/*/traces/*/*.parquet', 'KEY', 'SECRET'"] \
#   telemetry/measure.sh [block...] | optimize
#
# `optimize` merges every partition of the day before FROM to one part
# (OPTIMIZE ... PARTITION ... FINAL) so bytes_per_row is the merged figure the
# calculator's bSpan/bLog mean; run it on a closed day, once. S3_GLOB (the
# s3() arguments before the format) enables the wire_bytes block.
set -u -o pipefail
: "${CH:?}" "${DB:?}" "${FROM:?}" "${TO:?}" "${OUT:?}"
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"
ch() { curl -sS --fail-with-body "$CH/?max_execution_time=1800" --data-binary "$1"; }
if [ "${1:-}" = optimize ]; then
  day=$(date -u -d "$FROM - 1 day" +%F)
  for t in $(ch "SELECT name FROM system.tables WHERE database = '$DB' AND engine LIKE '%MergeTree' AND name LIKE 'otel_%' FORMAT TSV"); do
    echo "OPTIMIZE $DB.$t PARTITION '$day' FINAL"
    ch "OPTIMIZE TABLE $DB.$t PARTITION '$day' FINAL SETTINGS optimize_throw_if_noop = 0" || true
  done
  exit 0
fi
python3 - "$here/measure.sql" "$OUT" "$DB" "$FROM" "$TO" "${S3_GLOB:-}" "$@" <<'PY'
import re, subprocess, sys, os
sql, out, db, t0, t1, s3, *only = sys.argv[1:]
blocks = re.split(r"^-- name: (\S+)\n", open(sql).read(), flags=re.M)[1:]
for name, body in zip(blocks[::2], blocks[1::2]):
    if only and name not in only:
        continue
    if "{s3}" in body and not s3:
        print(f"{name}: skipped (no S3_GLOB)"); continue
    q = "\n".join(l for l in body.splitlines() if not l.startswith("--")).strip().rstrip(";")
    q = q.replace("{db}", db).replace("{from}", t0).replace("{to}", t1).replace("{s3}", s3)
    r = subprocess.run(["curl", "-sS", "--fail-with-body", os.environ["CH"] + "/?max_execution_time=1800", "--data-binary", q],
                       capture_output=True, text=True)
    open(f"{out}/{name}.tsv", "w").write(r.stdout)
    print(f"{name}: {'ok' if r.returncode == 0 else 'ERROR ' + r.stdout.strip()[:300]} -> {out}/{name}.tsv")
PY
