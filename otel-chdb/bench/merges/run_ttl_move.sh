#!/bin/sh
# TTL MOVE again, with move_factor = 0 on both policies (the first attempt,
# results/movefactor-*, was driven by the default move_factor 0.1 on the
# nearly full shared disk, not by the TTL).
set -e
cd "$(dirname "$0")"
DAY="toStartOfInterval(received_at,toIntervalMinute(4))"
T="--tables traces --rows 10k --rate 2 --workers 3 --duration 600 --drain 90 --real-time --partition $DAY --max-db-gb 0.5"
python3 merges.py --run ttl-move-local $T --ttl "TTL $DAY + toIntervalMinute(4) TO VOLUME 'cold'" \
  --setting "storage_policy = 'tiered_local'" --setting "merge_with_ttl_timeout = 40"
python3 merges.py --run ttl-move-s3 $T --ttl "TTL $DAY + toIntervalMinute(4) TO VOLUME 'cold'" \
  --setting "storage_policy = 'tiered_s3'" --setting "merge_with_ttl_timeout = 40"
