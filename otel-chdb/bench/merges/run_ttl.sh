#!/bin/sh
# TTL costs. received_at runs 360x faster (a synthetic day every 4 real minutes),
# and merge_with_ttl_timeout is scaled by the same factor (14400 s -> 40 s).
set -e
cd "$(dirname "$0")"
M="--tables number --rows 10k --rate 4 --workers 3 --duration 720 --drain 60 --time-scale 360 --max-db-gb 0.5"
# ClickStack's own form: a per-day TTL on a per-day partition, whole parts dropped
python3 merges.py --run ttl-drop $M --ttl "TTL toDate(received_at) + toIntervalDay(1)" \
  --setting "ttl_only_drop_parts = 1" --setting "merge_with_ttl_timeout = 40"
# row-level TTL: rows expire through the day, TTL merges rewrite partly expired parts
python3 merges.py --run ttl-rows $M --ttl "TTL toDateTime(received_at) + toIntervalDay(1)" \
  --setting "ttl_only_drop_parts = 0" --setting "merge_with_ttl_timeout = 40"
# TTL MOVE of each day's partition, once the day is 1 day old, to a second local disk and to S3 (SeaweedFS)
T="--tables traces --rows 10k --rate 2 --workers 3 --duration 600 --drain 90 --time-scale 360 --max-db-gb 0.5"
python3 merges.py --run ttl-move-local $T --ttl "TTL toDate(received_at) + toIntervalDay(1) TO VOLUME 'cold'" \
  --setting "storage_policy = 'tiered_local'" --setting "merge_with_ttl_timeout = 40"
python3 merges.py --run ttl-move-s3 $T --ttl "TTL toDate(received_at) + toIntervalDay(1) TO VOLUME 'cold'" \
  --setting "storage_policy = 'tiered_s3'" --setting "merge_with_ttl_timeout = 40"
