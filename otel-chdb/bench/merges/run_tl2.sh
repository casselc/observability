#!/bin/sh
# Second pass inside the tighter disk budget: traces with the pool's own
# (testgen) ids, ~3x fewer stored bytes per row, so more parts fit; and the
# random-id traces-100k again.
set -e
cd "$(dirname "$0")"
python3 merges.py --run traces-100k-tg --tables traces --rows 100k --rate 1 --workers 3 --ids testgen --duration 600 --drain 120 --max-db-gb 0.5
python3 merges.py --run traces-10k-tg --tables traces --rows 10k --rate 4 --workers 3 --ids testgen --duration 600 --drain 90 --max-db-gb 0.5
