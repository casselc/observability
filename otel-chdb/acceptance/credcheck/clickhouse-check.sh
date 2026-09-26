#!/bin/sh
# Runs clickhouse-central.sql against a ClickHouse server's HTTP interface,
# one statement at a time, printing each answer (or error) under its comment.
#
#   credcheck/clickhouse-check.sh SAMPLE_URL
#   CH_URL=http://central:8123 CH_USER=ingest CH_PASSWORD=... credcheck/clickhouse-check.sh \
#       https://otel-telemetry.s3.eu-west-1.amazonaws.com/otel-accept/creds/sample.tsv
#
# SAMPLE_URL is printed by `s3accept creds --leave-sample`.
set -u
SQL=$(dirname "$0")/clickhouse-central.sql
URL=${1:?usage: clickhouse-check.sh SAMPLE_URL}
GLOB=$(printf %s "$URL" | sed 's|/[^/]*$|/*.tsv|')
CH_URL=${CH_URL:-http://127.0.0.1:8123}
CH_USER=${CH_USER:-default}
CH_PASSWORD=${CH_PASSWORD:-}
# Strip comments, substitute, split on ';'.
sed -e 's/--.*$//' -e "s|__URL__|$URL|g" -e "s|__GLOB__|$GLOB|g" "$SQL" | tr '\n' ' ' | tr ';' '\n' |
  while IFS= read -r q; do
    q=$(printf %s "$q" | sed 's/^ *//; s/ *$//')
    [ -z "$q" ] && continue
    echo "--- $(printf %s "$q" | cut -c1-110)"
    curl -sS --max-time 60 -u "$CH_USER:$CH_PASSWORD" "$CH_URL/?default_format=TSVWithNames" --data-binary "$q" |
      sed 's/^/    /' | cut -c1-400
  done
