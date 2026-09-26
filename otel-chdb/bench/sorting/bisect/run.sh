#!/bin/bash
# Step 0 bisect, measured: otap-rs's scripts/series_bench.sh (unchanged, as in
# bench/clean block 1) with each variant's binaries (build.sh), plus a
# control that no change touched (encbench, testgen traces, rust-direct to
# S3). 5 repetitions, variants interleaved within each repetition, each
# (variant, repetition) gated on load <= 0.45 with env snapshots around it.
# The measured process on CPUs 0-1, the driver and SeaweedFS on CPUs 2-3.
# Restart-safe: a (variant, rep) with a "done" line in done.txt is skipped.
set -u
here=$(cd "$(dirname "$0")" && pwd); sorting=$(dirname "$here"); spike=$(dirname "$(dirname "$sorting")")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
clean=$spike/bench/clean
export ENVLOG=$here/env.jsonl
. "$clean/lib/env.sh"; . "$clean/lib/mkwrap.sh"
for p in $(pgrep -x weed); do taskset -a -p -c 2,3 "$p" > /dev/null; done
header "bisect before"
S3=http://127.0.0.1:18333/otel/sorting/bisect
touch "$here/done.txt"
for rep in $(seq 1 "${NREP:-5}"); do
  for v in ${VARIANTS:-pre head head-nodur}; do
    grep -qx "$v $rep" "$here/done.txt" && continue
    W=$S/sorting/wrap/$v
    mkwrap $W/b encbench $S/sorting/bin/$v/encbench 0,1
    mkwrap $W/b otap-s3pq $S/sorting/bin/$v/otap-s3pq 0,1 exec
    mkwrap $W/t otlpsend $S/clean/bin/otlpsend 2,3
    gate || continue
    snap "begin bisect-$v-rep$rep"
    tmp=$(mktemp)
    REPS=1 S3=$S3 B=$W/b T=$W/t FLEET=$S/series/fleet D=$S/otaprs-mdata OUT=$tmp bash "$spike/otap-rs/scripts/series_bench.sh"
    $W/b/encbench --files "$(ls $S/otaprs/data/traces-bench-v*.pb | head -20 | paste -sd,)" --signal traces --batches 60 --warmup 3 \
      --label control-traces --s3 $S3/enc --key otel --secret otelsecret >> $tmp
    snap "finish bisect-$v-rep$rep"
    python3 - "$tmp" "$v" "$rep" >> "$here/series.jsonl" <<'P'
import json, sys
for l in open(sys.argv[1]):
    d = json.loads(l)
    if "Impl" in d:
        d["variant"], d["clean_rep"] = sys.argv[2], int(sys.argv[3])
        print(json.dumps(d))
P
    rm -f "$tmp"
    echo "$v $rep" >> "$here/done.txt"
    curl -s -X DELETE 'http://127.0.0.1:18888/buckets/otel/sorting/bisect?recursive=true&ignoreRecursiveError=true' > /dev/null
  done
done
header "bisect after"
