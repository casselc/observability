#!/bin/sh
# TTL costs, with a production day scaled 360x: received_at is the wall clock,
# a "day" partition is 4 minutes (toStartOfInterval(received_at, 4 min)), a
# 1-day TTL is 4 minutes, and merge_with_ttl_timeout 14400 s becomes 40 s.
set -e
cd "$(dirname "$0")"
DAY="toStartOfInterval(received_at,toIntervalMinute(4))"
M="--tables number --rows 10k --rate 4 --workers 3 --duration 720 --drain 60 --real-time --partition $DAY --max-db-gb 0.5"
# ClickStack's own form: TTL on the partition's day, whole parts dropped
python3 merges.py --run ttl-drop $M --ttl "TTL $DAY + toIntervalMinute(4)" \
  --setting "ttl_only_drop_parts = 1" --setting "merge_with_ttl_timeout = 40"
# row-level TTL: rows expire through the day, TTL merges rewrite partly expired parts
python3 merges.py --run ttl-rows $M --ttl "TTL toDateTime(received_at) + toIntervalMinute(4)" \
  --setting "ttl_only_drop_parts = 0" --setting "merge_with_ttl_timeout = 40"
# TTL MOVE of each day's partition once it is a day old, to a second local disk and to S3 (SeaweedFS)
T="--tables traces --rows 10k --rate 2 --workers 3 --duration 600 --drain 90 --real-time --partition $DAY --max-db-gb 0.5"
python3 merges.py --run ttl-move-local $T --ttl "TTL $DAY + toIntervalMinute(4) TO VOLUME 'cold'" \
  --setting "storage_policy = 'tiered_local'" --setting "merge_with_ttl_timeout = 40"
python3 merges.py --run ttl-move-s3 $T --ttl "TTL $DAY + toIntervalMinute(4) TO VOLUME 'cold'" \
  --setting "storage_policy = 'tiered_s3'" --setting "merge_with_ttl_timeout = 40"
