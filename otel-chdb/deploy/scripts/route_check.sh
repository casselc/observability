#!/bin/bash
# The second half of route_test.sh, rerunnable on its own: ingest a run's
# objects with the consumer into a fresh database and summarise.
#   CONSUME=... W=workdir/RUN RUN=name SCEN=... N=... [GW=...] route_check.sh
# CHECK=s3 reads the objects straight from S3 instead, and drops an object
# whose rows are identical to another's (what the consumer's content-key
# check skips) before counting duplicates.
set -u
: "${W:?}" "${RUN:?}"; CHECK=${CHECK:-consumer}; [ "$CHECK" = consumer ] && : "${CONSUME:?}"
SCEN=${SCEN:-steady}; N=${N:-8}; GW=${GW:-gateway}
S3=${S3:-http://127.0.0.1:18333}; BUCKET=${BUCKET:-deploy-edge}; CH=${CH:-http://127.0.0.1:18123}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
DB=deploy_$(echo "$RUN" | tr -c 'a-zA-Z0-9_\n' _)
q() { curl -s "$CH/" --data-binary "$1"; }
t0=$(cat "$W/t0" 2>/dev/null || echo 0); t1=$(cat "$W/t1" 2>/dev/null || echo 0)
if [ "$CHECK" = consumer ]; then
q "DROP DATABASE IF EXISTS $DB"
"$CONSUME" --s3 "$S3/$BUCKET/route/$RUN" --ch "$CH" --db "$DB" --signals traces,logs --exit-after-idle 5s \
  --key "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" --poll 500ms > "$W/consume.log" 2>&1
fi
{
echo "# route_test $RUN: scenario $SCEN, N=$N, gateway $(basename "$(dirname "$GW")")/$(basename "$GW")"
att() { cat "$W"/send-$1*.jsonl | grep '"summary"' | grep -o '"attempts":[0-9]*' | cut -d: -f2 | paste -sd+ | bc 2>/dev/null || python3 -c "import sys; print(sum(int(x) for x in sys.argv[1:]))" $(cat "$W"/send-$1*.jsonl | grep '"summary"' | grep -o '"attempts":[0-9]*' | cut -d: -f2); }
echo "send wall $(awk "BEGIN{printf \"%.1f\", $t1 - $t0}") s; sender attempts for 32 requests: traces $(att traces), logs $(att logs); senders per signal: $(ls "$W"/send-traces*.jsonl | wc -l)"
[ -f "$W/events.log" ] && sed 's/^/event /' "$W/events.log"
[ "$CHECK" = consumer ] && echo "consumer: $(tail -1 "$W/consume.log" | grep -o '"objects_inserted":[0-9]*\|"rows_inserted":[0-9]*\|"dedup_skipped":[0-9]*\|"over_count":[0-9]*\|"errors":[0-9]*' | paste -sd' ')"
for sig in traces logs; do
  if [ $sig = traces ]; then id="TraceId, SpanId"; st="TraceId String, SpanId String, ServiceName String, producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32"
  else id="Timestamp, TraceId, SpanId, ServiceName, Body, LogAttributes"; st="Timestamp DateTime64(9), TraceId String, SpanId String, ServiceName String, Body String, LogAttributes Map(String, String), producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32"; fi
  if [ "$CHECK" = consumer ]; then t=$DB.otel_$sig
  else
    raw="s3('$S3/$BUCKET/route/$RUN/*/$sig/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', '$st')"
    # keep one object per distinct content (by row fingerprint), as the consumer would
    # An object's content, in row order: the same rows in another order are
    # other bytes, so another content key, which the consumer does not skip.
    fp="cityHash64(arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((row_ordinal, cityHash64($id))))))"
    t="(SELECT *, _path obj FROM $raw WHERE _path IN (SELECT any(p) FROM (SELECT _path p, $fp f FROM $raw GROUP BY _path) GROUP BY f))"
    echo "objects in S3: $(q "SELECT uniqExact(_path) FROM $raw FORMAT TSV"); left after dropping byte-identical copies (the consumer's content check): $(q "SELECT uniqExact(f) FROM (SELECT _path, $fp f FROM $raw GROUP BY _path) FORMAT TSV")"
  fi
  echo; echo "## $sig"
  echo "per publisher: rows, share, rows/mean, services, objects, rows/object"
  if [ "$CHECK" = consumer ]; then objs="uniqExact(content_key)"; else objs="uniqExact(obj)"; fi
  q "SELECT producer_id, count() r, round(r / sum(r) OVER (), 3), round(r / avg(r) OVER (), 2), uniqExact(ServiceName), $objs o, round(r / o) FROM $t GROUP BY producer_id ORDER BY producer_id FORMAT TSV"
  echo "affinity: services, services on exactly one publisher, max publishers per service"
  q "SELECT count(), countIf(p = 1), max(p) FROM (SELECT ServiceName, uniqExact(producer_id) p FROM $t GROUP BY ServiceName) FORMAT TSV"
  echo "central: rows, distinct rows, duplicate rows (rows - distinct), expected distinct rows (320000)"
  q "SELECT count(), uniqExact($id), count() - uniqExact($id), 320000 FROM $t FORMAT TSV"
done
} > "$W/summary.txt" 2>&1
cat "$W/summary.txt"
