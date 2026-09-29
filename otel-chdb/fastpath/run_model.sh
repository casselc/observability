#!/usr/bin/env bash
# Sweep model/fastPath.qnt: every instance x every invariant, by simulation.
# Usage: SAMPLES=2000 STEPS=40 JOBS=3 ./run_model.sh [instance ...]
# Output: results/model/<instance>.<invariant>.txt (raw, not kept) and results/model/summary.tsv
# (appended to). OUT elsewhere leaves the recorded summary alone (the nightly
# model-open job compares its sweep with it: ci/model-open.sh); BACKEND=rust
# uses quint's Rust evaluator; CELL_MIN bounds each cell (a cell that runs
# out is recorded TIMEOUT).
set -u
cd "$(dirname "$0")"
SAMPLES=${SAMPLES:-2000}
STEPS=${STEPS:-40}
JOBS=${JOBS:-3}
SEED=${SEED:-0x5eed}
BACKEND=${BACKEND:-typescript}
CELL_MIN=${CELL_MIN:-0}
OUT=${OUT:-results/model}
mkdir -p "$OUT"
SPEC=../model/fastPath.qnt
if [ $# -gt 0 ]; then INSTANCES="$*"; else
  INSTANCES=$(grep -o '^module [A-Za-z]*' $SPEC | awk '{print $2}' | grep -v '^fastPath$')
fi
INVS="onlyCommittedIngested batchIngestedAtMostOnce noLostBehindCheckpoint importerNeverInserts"

one() {
  inst=$1 inv=$2
  f=$OUT/$inst.$inv.txt
  start=$(date +%s)
  timeout "${CELL_MIN}m" quint run $SPEC --main "$inst" --invariant "$inv" --max-steps "$STEPS" --max-samples "$SAMPLES" \
    --backend "$BACKEND" --seed "$SEED" > "$f" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  if grep -q '\[violation\]' "$f"; then
    traces=$(grep -o 'Found an issue ([0-9]*ms at [0-9.]* traces/second)' "$f" | head -1)
    res="VIOLATED"
  elif grep -q '\[ok\]' "$f"; then res="ok"; traces=""
  elif [ "$rc" -eq 124 ]; then res="TIMEOUT"; traces=""
  else res="ERROR rc=$rc"; traces=""; fi
  printf '%s\t%s\t%s\t%ss\t%s\n' "$inst" "$inv" "$res" "$secs" "$traces" >> $OUT/summary.tsv
}
export -f one
export OUT SPEC STEPS SAMPLES SEED BACKEND CELL_MIN
for i in $INSTANCES; do for v in $INVS; do echo "$i $v"; done; done |
  xargs -P "$JOBS" -n 2 bash -c 'one "$0" "$1"'
sort $OUT/summary.tsv -o $OUT/summary.tsv
