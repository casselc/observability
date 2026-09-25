#!/bin/sh
# Traces and logs, one table at a time, at ~10k and ~100k rows per statement.
set -e
cd "$(dirname "$0")"
python3 merges.py --run traces-10k --tables traces --rows 10k --rate 4 --workers 3 --duration 600 --drain 90 --max-db-gb 0.9
python3 merges.py --run traces-100k --tables traces --rows 100k --rate 1 --workers 3 --duration 420 --drain 120 --max-db-gb 0.9
python3 merges.py --run logs-10k --tables logs --rows 10k --rate 4 --workers 3 --duration 600 --drain 90 --max-db-gb 0.9
python3 merges.py --run logs-100k --tables logs --rows 100k --rate 1 --workers 3 --duration 420 --drain 120 --max-db-gb 0.9
