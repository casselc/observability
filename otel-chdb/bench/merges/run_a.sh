#!/bin/sh
# For comparison: the ClickStack otel_metrics_sum / otel_metrics_histogram tables
# (contrib DDL + envelope), fed from parquetgo's objects of the same fleet data.
# One whole-fleet round per object: sum 52k, histogram 16k rows (x6 = 96k).
set -e
cd "$(dirname "$0")"
python3 merges.py --run clickstack-100k --tables A_sum,A_histogram --rows 100k --rate A_sum=1,A_histogram=0.5 \
  --workers 3 --duration 420 --drain 90 --max-db-gb 0.5
python3 merges.py --run clickstack-10k --tables A_histogram --rows 10k --rate 2 --workers 3 --duration 420 --drain 90 --max-db-gb 0.5
