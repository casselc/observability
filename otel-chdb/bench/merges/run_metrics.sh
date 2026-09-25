#!/bin/sh
# Metrics, series layout (otap-rs/sql/series_tables.sql): the four points tables
# and the series table fed together, at ~10k and ~100k rows per statement.
# Rates per table keep the run inside the disk budget (bytes/row differ 5x).
set -e
cd "$(dirname "$0")"
python3 merges.py --run metrics-10k --tables number,histogram,exponential_histogram,summary,series --rows 10k \
  --rate number=4,histogram=3,exponential_histogram=1,summary=2,series=0.5 --workers 3 --duration 900 --drain 90 --max-db-gb 0.6
python3 merges.py --run metrics-100k --tables number,histogram,exponential_histogram,summary,series --rows 100k \
  --rate number=1,histogram=0.5,exponential_histogram=0.2,summary=0.3,series=0.1 --workers 3 --duration 600 --drain 120 --max-db-gb 0.6
