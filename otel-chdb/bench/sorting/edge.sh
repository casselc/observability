#!/bin/bash
# Step 1, edge: CPU per 10k rows and object size per sort configuration,
# traces and logs, encbench (in-process: flatten, sort, encode, create-only
# commit to S3 through the real lane), the mixgen batches of prep.sh.
#   pass "cpu":  NREP repetitions; in each, every (signal, config) once, in
#                a rotated order, 3 warm-up + 60 timed batches cycling the 32
#                distinct requests; each process gated on load <= 0.45 with
#                env snapshots around it; pinned to CPUs 0-1 (SeaweedFS 2-3).
#                Objects go to otel/sorting/cpu/, deleted after each rep.
#   pass "set":  once per (signal, config): the 32 distinct requests as 32
#                objects under otel/sorting/set/<config>/edges/<producer>, the
#                object set the read and central measurements use.
# Restart-safe: a (pass, rep, signal, config) already in edge.jsonl is skipped.
set -u
here=$(cd "$(dirname "$0")" && pwd); spike=$(dirname "$(dirname "$here")")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
EB=$S/sorting/bin/sort/encbench
export ENVLOG=$here/edge-env.jsonl
. "$spike/bench/clean/lib/env.sh"
S3=http://127.0.0.1:18333/otel/sorting
D=$S/sorting/data
OUT=$here/edge.jsonl
touch $OUT
# name: encbench flags
declare -A CFG=(
  [a-unsorted]=""
  [b-sorted-1rg]="--sort service_time --row-groups 1"
  [c-hash-4rg]="--sort service_time --row-groups 4 --split hash"
  [d-hash-16rg]="--sort service_time --row-groups 16 --split hash"
  [e-range-4rg]="--sort service_time --row-groups 4 --split range"
  [f-range-16rg]="--sort service_time --row-groups 16 --split range"
  [g-hash-4rg-bloom]="--sort service_time --row-groups 4 --split hash --bloom-add ServiceName"
  [h-hash-16rg-bloom]="--sort service_time --row-groups 16 --split hash --bloom-add ServiceName"
)
NAMES=(a-unsorted b-sorted-1rg c-hash-4rg d-hash-16rg e-range-4rg f-range-16rg g-hash-4rg-bloom h-hash-16rg-bloom)
for p in $(pgrep -x weed); do taskset -a -p -c 2,3 "$p" > /dev/null; done
done_() { grep -q "\"pass\": \"$1\", \"rep\": $2, \"signal\": \"$3\", \"config\": \"$4\"" $OUT; }
one() { # pass rep signal config
  local pass=$1 rep=$2 sig=$3 c=$4 files url extra
  done_ "$pass" "$rep" "$sig" "$c" && return 0
  files=$(ls $D/$sig/$sig-b*.pb | paste -sd,)
  if [ "$pass" = set ]; then
    url=$S3/set/$c/edges; extra="--warmup 1 --batches 31"
  else
    url=$S3/cpu/$c/edges; extra="--warmup 3 --batches 60"
  fi
  gate || return 1
  snap "start $pass-$rep-$sig-$c"
  local line
  line=$(taskset -c 0,1 $EB --files "$files" --signal $sig $extra ${CFG[$c]} --label $c \
    --s3 $url/sort-$c --key otel --secret otelsecret)
  local rc=$?
  snap "end $pass-$rep-$sig-$c rc=$rc"
  [ $rc -eq 0 ] || return 1
  python3 -c "
import json, sys
d = json.loads(sys.argv[1]); o = {'pass': sys.argv[2], 'rep': int(sys.argv[3]), 'signal': sys.argv[4], 'config': sys.argv[5]}
o.update(d); print(json.dumps(o))" "$line" "$pass" "$rep" "$sig" "$c" >> $OUT
}
header "edge before"
if [ "${PASS:-cpu}" = set ]; then
  for sig in traces logs; do for c in "${NAMES[@]}"; do one set 1 $sig $c; done; done
else
  for rep in $(seq 1 "${NREP:-5}"); do
    n=${#NAMES[@]}
    for sig in traces logs; do
      for i in $(seq 0 $((n - 1))); do
        c=${NAMES[$(( (i + rep - 1) % n ))]}
        one cpu $rep $sig $c
      done
    done
    curl -s -X DELETE "http://127.0.0.1:18888/buckets/otel/sorting/cpu?recursive=true&ignoreRecursiveError=true" > /dev/null
  done
fi
header "edge after"
