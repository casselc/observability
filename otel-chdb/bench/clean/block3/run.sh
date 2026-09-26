#!/bin/bash
# Block 3: merge CPU per row, per table, at 100k-row statements, one table per
# run, on the private ClickHouse (part_log + query_log) pinned to CPUs 1-3,
# with the driver (merges.py) and SeaweedFS on CPU 0. Each run starts gated on
# load <= 0.45. Parts per partition are bounded by the disk budget:
# --max-db-gb 1.0 of active parts leaves room for a top-level merge within
# the 2 GB data budget (free disk never below 2.5 GB); --min-free-gb 2.7.
# Afterwards: analyze.py, fit.py, proj_clean.py.
set -u
here=$(cd "$(dirname "$0")" && pwd); clean=$(dirname "$here")
export ENVLOG=$here/env.jsonl; . "$clean/lib/env.sh"
header "block3 before"
run() { # name args...
  local name=$1; shift
  gate; snap "begin b3-$name free=$(freegb)"
  taskset -c 0 python3 $here/merges.py --run $name --workers 3 --drain 120 --max-db-gb ${MAXDB:-1.0} --min-free-gb 2.7 "$@" >> $here/run.log 2>&1
  snap "finish b3-$name free=$(freegb)"
}
for r in ${RUNS:-traces-tg traces logs number histogram exp summary series A_sum A_histogram}; do
  case $r in
    traces-tg) run traces-100k-tg --tables traces --rows 100k --ids testgen --rate 1.2 --duration 1500;;
    traces)    run traces-100k --tables traces --rows 100k --rate 1 --duration 1500;;
    logs)      run logs-100k --tables logs --rows 100k --rate 1.2 --duration 1500;;
    number)    run number-100k --tables number --rows 100k --rate 3 --duration 700;;
    histogram) run histogram-100k --tables histogram --rows 100k --rate 2 --duration 900;;
    exp)       run exphist-100k --tables exponential_histogram --rows 100k --rate 1.2 --duration 1200;;
    summary)   run summary-100k --tables summary --rows 100k --rate 2 --duration 900;;
    series)    run series-100k --tables series --rows 100k --rate 1 --duration 600;;
    A_sum)     run clickstack-sum-100k --tables A_sum --rows 100k --rate 0.6 --duration 1500;;
    A_histogram) run clickstack-histogram-100k --tables A_histogram --rows 100k --rate 0.6 --duration 1500;;
  esac
done
header "block3 after"
