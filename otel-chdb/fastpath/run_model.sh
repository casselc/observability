#!/usr/bin/env bash
# Sweep model/fastPath.qnt: every instance x every invariant, by simulation.
# Usage: SAMPLES=2000 STEPS=40 JOBS=3 ./run_model.sh [instance ...]
# Output: results/model/<instance>.<invariant>.txt and results/model/summary.tsv
set -u
cd "$(dirname "$0")"
SAMPLES=${SAMPLES:-2000}
STEPS=${STEPS:-40}
JOBS=${JOBS:-3}
SEED=${SEED:-0x5eed}
OUT=results/model
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
  quint run $SPEC --main "$inst" --invariant "$inv" --max-steps "$STEPS" --max-samples "$SAMPLES" \
    --backend typescript --seed "$SEED" > "$f" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  if grep -q '\[violation\]' "$f"; then
    traces=$(grep -o 'Found an issue ([0-9]*ms at [0-9.]* traces/second)' "$f" | head -1)
    res="VIOLATED"
  elif grep -q '\[ok\]' "$f"; then res="ok"; traces=""
  else res="ERROR rc=$rc"; traces=""; fi
  printf '%s\t%s\t%s\t%ss\t%s\n' "$inst" "$inv" "$res" "$secs" "$traces" >> $OUT/summary.tsv
}
export -f one
export OUT SPEC STEPS SAMPLES SEED
for i in $INSTANCES; do for v in $INVS; do echo "$i $v"; done; done |
  xargs -P "$JOBS" -n 2 bash -c 'one "$0" "$1"'
sort $OUT/summary.tsv -o $OUT/summary.tsv
