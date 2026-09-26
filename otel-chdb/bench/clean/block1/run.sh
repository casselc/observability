#!/bin/bash
# Block 1: edge encode + publish CPU per 10k rows/points, on the idle box.
# Runs otap-rs's own bench scripts unchanged (bench.sh, metrics_bench.sh,
# series_bench.sh), REPS=1 each, 5 repetitions interleaved, each repetition
# of each script gated on load <= 0.45. $B/$T point at wrappers (lib/mkwrap.sh)
# that pin the measured process (encbench, pubbench, otap-s3pq) to CPUs 0-1
# and the driver (otlpsend) to CPUs 2-3, with SeaweedFS pinned to CPUs 2-3,
# and log /proc/stat + loadavg around every process.
set -u
here=$(cd "$(dirname "$0")" && pwd); clean=$(dirname "$here"); spike=$(dirname "$(dirname "$clean")")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
REL=$S/otap-rs-target/release
export ENVLOG=$here/env.jsonl
. "$clean/lib/env.sh"; . "$clean/lib/mkwrap.sh"
W=$S/clean/wrap1
mkwrap $W/b encbench $REL/encbench 0,1
mkwrap $W/b otap-s3pq $REL/otap-s3pq 0,1 exec
mkwrap $W/t pubbench $S/clean/bin/pubbench 0,1
mkwrap $W/t otlpsend $S/clean/bin/otlpsend 2,3
for p in $(pgrep -x weed); do taskset -a -p -c 2,3 "$p" > /dev/null; done
header "block1 before"
S3=http://127.0.0.1:18333/otel/clean/b1
for rep in $(seq 1 "${NREP:-5}"); do
  gate; snap "begin bench-rep$rep"; echo "{\"clean_rep\":$rep}" >> "$here/bench.jsonl"
  REPS=1 S3=$S3 B=$W/b T=$W/t D=$S/otaprs/data OUT=$here/bench.jsonl bash "$spike/otap-rs/scripts/bench.sh"
  snap "finish bench-rep$rep"
  gate; snap "begin metrics-rep$rep"; echo "{\"clean_rep\":$rep}" >> "$here/metrics.jsonl"
  REPS=1 S3=$S3 B=$W/b T=$W/t D=$S/otaprs-mdata OUT=$here/metrics.jsonl bash "$spike/otap-rs/scripts/metrics_bench.sh"
  snap "finish metrics-rep$rep"
  gate; snap "begin series-rep$rep"; echo "{\"clean_rep\":$rep}" >> "$here/series.jsonl"
  REPS=1 S3=$S3 B=$W/b T=$W/t FLEET=$S/series/fleet D=$S/otaprs-mdata OUT=$here/series.jsonl bash "$spike/otap-rs/scripts/series_bench.sh"
  snap "finish series-rep$rep"
  curl -s -X DELETE 'http://127.0.0.1:18888/buckets/otel/clean/b1?recursive=true&ignoreRecursiveError=true' > /dev/null
done
header "block1 after"
