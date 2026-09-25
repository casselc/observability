#!/bin/sh
# Settings effects on traces at ~10k rows per statement, 4 statements/s, 5 min
# each: compare with the first 5 minutes of traces-10k (the per-minute curve).
set -e
cd "$(dirname "$0")"
R="--tables traces --rows 10k --rate 4 --workers 3 --duration 300 --drain 60 --max-db-gb 0.5"
python3 merges.py --run set-wide $R --setting "min_bytes_for_wide_part = 0"
python3 merges.py --run set-stochastic $R --setting "merge_selector_algorithm = 'StochasticSimple'"
python3 merges.py --run set-novertical $R --setting "enable_vertical_merge_algorithm = 0"
# an hourly partition, with received_at running 30x faster: an hour every 2 minutes
python3 merges.py --run set-hourly $R --partition "toStartOfHour(received_at)" --time-scale 30
