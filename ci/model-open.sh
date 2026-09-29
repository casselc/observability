#!/usr/bin/env bash
# The open-ended model nightly (`model-open`; VERIFICATION.md §7 P1,
# ci/README.md "model-open"): the models no other job runs, longer random
# simulation than the per-push and `model` jobs can afford, and Apalache
# bounded runs. A new random seed every night (SEED to replay one).
#
#   OUT_DIR=out ci/model-open.sh
#
# Parts, in order (the most important first, since the budget can stop the rest):
#   1. open      model/open_models.sh: completeness, entityCatalog, retention,
#                sealer (82 rows: designs, witnesses, mutants, scripted runs)
#   2. s3native  model/s3Native.qnt: the design, the mutants of S3NATIVE.md §5,
#                and every scenario of s3Native_test.qnt
#   3. lifetime  model/partLifetime.qnt: the instances of model/README.md
#                "Model B", and partLifetime_test.qnt
#   4. fastpath  fastpath/run_model.sh: every fastPath.qnt instance x
#                invariant, against the recorded results/model/summary.tsv
#   5. long      the consumer designs at 4x the `model` job's samples and 100
#                steps, and the wider instances of model/openInstances.qnt
#   6. apalache  `quint verify` (bounded, exhaustive) at the depths the
#                model READMEs record, each under APALACHE_MIN minutes
# Every row goes to $OUT_DIR/model-open.txt in the format ci/model-verdict.sh
# judges (the same fail and warn rules as the `model` job), which also sets
# the exit status. Once BUDGET_MIN minutes have passed no new row starts:
# the rest are reported UNKNOWN (a warning). A design violation keeps its
# counterexample as an ITF trace in $OUT_DIR/cex/.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
M=$root/otel-chdb/model
OUT_DIR=${OUT_DIR:-${RUNNER_TEMP:-/tmp}/model-open}
SEED=${SEED:-$(printf '0x%x' $(( (RANDOM << 15 | RANDOM) + 1 )))}
BUDGET_MIN=${BUDGET_MIN:-170}
SIM_MIN=${SIM_MIN:-30}
APALACHE_MIN=${APALACHE_MIN:-40}
PARTS=${PARTS:-open s3native lifetime fastpath long apalache}
export JVM_ARGS=${JVM_ARGS:--Xmx5g}
mkdir -p "$OUT_DIR/cex" "$OUT_DIR/raw"
R=$OUT_DIR/model-open.txt
t_start=$(date +%s)
echo "model-open: quint $(quint --version), seed $SEED, budget ${BUDGET_MIN} min, parts: $PARTS" | tee "$R"

over() { [ $(( $(date +%s) - t_start )) -ge $(( BUDGET_MIN * 60 )) ]; }
secs() { echo $(( $(date +%s) - $1 )); }

# sim file main invariant samples steps expect: random simulation (Rust evaluator).
sim() {
  local t0 out v cex=""
  t0=$(date +%s)
  if over; then v=UNKNOWN; out="budget"
  else
    [ "$6" = ok ] && cex="$OUT_DIR/cex/$2.$(echo "$3" | tr -c 'A-Za-z0-9' _).itf.json"
    out=$(cd "$M" && timeout "${SIM_MIN}m" quint run "$1" --main "$2" --invariant "$3" --max-samples "$4" \
      --max-steps "$5" --seed "$SEED" ${cex:+--out-itf "$cex"} 2>&1)
    local rc=$?
    v=ERROR
    echo "$out" | grep -q "\[ok\]" && v=ok
    echo "$out" | grep -q "\[violation\]" && v=VIOLATED
    [ $rc -eq 124 ] && v=UNKNOWN
    [ "$v" = ok ] && [ -n "$cex" ] && rm -f "$cex"
  fi
  [ "$v" = ERROR ] && echo "$out" | tail -20 > "$OUT_DIR/raw/$2.$(echo "$3" | tr -c 'A-Za-z0-9' _).err"
  printf "sim   %-24s %-28s %-34s %6s x %-3s %-8s (expect %s) %ss\n" "$1" "$2" "$3" "$4" "$5" "$v" "$6" "$(secs "$t0")" | tee -a "$R"
}

# runs file main [match]: a group of scripted runs (quint test).
runs() {
  local out n f
  if over; then printf "runs  %-24s %-28s %-30s UNKNOWN (expect all-pass) budget\n" "$1" "$2" "${3:-all}" | tee -a "$R"; return; fi
  out=$(cd "$M" && timeout "${SIM_MIN}m" quint test "$1" --main "$2" ${3:+--match "$3"} 2>&1)
  n=$(echo "$out" | grep -c " ok "); f=$(echo "$out" | grep -c "failed after")
  printf "runs  %-24s %-28s %-30s passed %d failed %d (expect all-pass)\n" "$1" "$2" "${3:-all}" "$n" "$f" | tee -a "$R"
}

# verify file main invariant steps expect: Apalache, every execution up to `steps`.
verify() {
  local t0 out v rc
  t0=$(date +%s)
  if over; then v=UNKNOWN
  else
    out=$(cd "$M" && timeout "${APALACHE_MIN}m" quint verify "$1" --main "$2" --invariant "$3" --max-steps "$4" 2>&1)
    rc=$?
    v=ERROR
    echo "$out" | grep -q "\[ok\]" && v=ok
    echo "$out" | grep -q "\[violation\]" && v=VIOLATED
    [ $rc -eq 124 ] && v=UNKNOWN
    echo "$out" | tail -40 > "$OUT_DIR/raw/verify.$2.$3.$4.txt"
  fi
  printf "verify %-23s %-28s %-34s <= %-3s steps %-8s (expect %s) %ss\n" "$1" "$2" "$3" "$4" "$v" "$5" "$(secs "$t0")" | tee -a "$R"
}

part() { echo "== $1 ($(( ($(date +%s) - t_start) / 60 )) min in)" | tee -a "$R"; }

for p in $PARTS; do case $p in
open)
  part "open: model/open_models.sh"
  if over; then echo "sim   open_models.sh (not started)  UNKNOWN (expect ok) budget" | tee -a "$R"
  else
    # (it exits non-zero on any row off expectation, mutant misses included;
    # model-verdict.sh applies the `model` job's rules to its rows instead)
    SEED=$SEED OUT="$OUT_DIR/open-models.txt" timeout 120m "$M/open_models.sh" > /dev/null
    grep -E '^(sim|runs) ' "$OUT_DIR/open-models.txt" | tee -a "$R"
  fi ;;
s3native)
  part "s3native: model/s3Native.qnt (S3NATIVE.md §5)"
  S=s3Native.qnt
  sim $S s3NativeDesign safety 3000 120 ok
  sim $S noFenceEntry safety 3000 120 ok
  sim $S nonConditionalLog noWriteFromFencedWriter 3000 120 VIOLATED
  sim $S leaseWithoutFencing noWriteFromFencedWriter 3000 120 VIOLATED
  sim $S restartReusesTable nsSingleWriter 3000 120 VIOLATED
  sim $S genFromClock generationsSealedOrPending 3000 120 VIOLATED
  sim $S noSealRetry generationsSealedOrPending 3000 120 VIOLATED
  sim $S gcBlindWrite gcKeepsLiveData 3000 120 VIOLATED
  sim $S gcIgnoresLeases gcKeepsLiveData 3000 120 VIOLATED
  sim $S noCheckCentral payloadIngestedAtMostOnce 3000 120 VIOLATED
  sim $S unboundedZombieInsert payloadIngestedAtMostOnce 3000 120 VIOLATED
  for t in $(grep -o '^module [A-Za-z0-9]*' "$M/s3Native_test.qnt" | awk '{print $2}'); do runs s3Native_test.qnt "$t"; done ;;
lifetime)
  part "lifetime: model/partLifetime.qnt (README Model B)"
  P=partLifetime.qnt
  sim $P lifetime0 noReadOfDeleted 3000 40 VIOLATED
  sim $P lifetimeEqQmax noReadOfDeleted 3000 40 VIOLATED
  sim $P lifetimeSafe noReadOfDeleted 20000 40 ok
  sim $P stoppedRefresh noReadOfDeleted 3000 40 VIOLATED
  sim $P writerExitNoGc noLeakAfterExit 3000 40 VIOLATED
  sim $P gcWithoutLeases noReadOfDeleted 3000 40 VIOLATED
  for i in noReadOfDeleted noLeakAfterExit noDoubleCount; do sim $P proposedLifecycle $i 20000 40 ok; done
  for t in $(grep -o '^module [A-Za-z0-9]*' "$M/partLifetime_test.qnt" | awk '{print $2}'); do runs partLifetime_test.qnt "$t"; done ;;
fastpath)
  part "fastpath: fastpath/run_model.sh against results/model/summary.tsv"
  want=$root/otel-chdb/fastpath/results/model/summary.tsv
  fp=$OUT_DIR/fastpath
  rm -rf "$fp"; mkdir -p "$fp"
  if over; then echo "sim   fastPath.qnt (not started)  UNKNOWN (expect ok) budget" | tee -a "$R"
  else
    # (the Rust evaluator; two cells at a time, each bounded)
    OUT=$fp BACKEND=rust SEED=$SEED SAMPLES=2000 STEPS=40 JOBS=2 CELL_MIN=$SIM_MIN \
      timeout 90m "$root/otel-chdb/fastpath/run_model.sh" > /dev/null
    # The recorded cells: every cell of the designs FASTPATH.md §6 names
    # (a violation of a recorded ok fails), and every recorded violation of
    # the other instances (a miss warns). A recorded ok of a mutant instance
    # claims nothing; a new violation there is in fastpath/summary.tsv only.
    designs=" recommended recommendedNoFastPath recommendedNoWait happyPath "
    while IFS=$'\t' read -r inst inv exp _; do
      [ "$exp" = ok ] && [ "${designs/ $inst /}" = "$designs" ] && continue
      got=$(awk -F'\t' -v i="$inst" -v v="$inv" '$1 == i && $2 == v { print $3; exit }' "$fp/summary.tsv" 2>/dev/null)
      case "$got" in ok|VIOLATED) ;; "") got=UNKNOWN ;; *TIMEOUT*) got=UNKNOWN ;; *) got=ERROR ;; esac
      printf "sim   %-24s %-28s %-34s %6s x %-3s %-8s (expect %s)\n" fastPath.qnt "$inst" "$inv" 2000 40 "$got" "$exp" | tee -a "$R"
    done < "$want"
  fi ;;
long)
  part "long: the consumer designs at 4x samples, 100 steps; wider instances"
  C=s3InlineConsumer.qnt
  for m in s3InlineConsumerDesign designCopies designDays; do sim $C $m safety 20000 100 ok; done
  for m in designSlow designLate; do sim $C $m slowSafety 20000 100 ok; done
  sim s3InlineConsumerCompact.qnt compactDesign compactSafety 20000 100 ok
  sim openInstances.qnt wideDesign "and { safety, auditSilent }" 10000 120 ok
  sim openInstances.qnt wideSlow slowSafety 10000 120 ok ;;
apalache)
  part "apalache: quint verify, bounded"
  # (depths from model/README.md, FASTPATH.md §6 and S3NATIVE.md §5)
  verify partLifetime.qnt lifetime0 noReadOfDeleted 10 VIOLATED
  verify partLifetime.qnt lifetimeSafe noReadOfDeleted 12 ok
  verify partLifetime.qnt proposedLifecycle noReadOfDeleted 10 ok
  verify partLifetime.qnt proposedLifecycle noLeakAfterExit 10 ok
  verify edgePublish.qnt noRotationLock sealMatchesManifests 6 VIOLATED
  verify edgePublish.qnt fixedDesign sealMatchesManifests 10 ok
  verify edgePublish.qnt recommended commitImpliesData 8 ok
  verify fastPath.qnt recommended safety 10 ok
  verify s3Native.qnt s3NativeDesign safety 6 ok ;;
*) echo "::error::model-open: unknown part $p"; exit 2 ;;
esac; done

echo "model-open: $(( ($(date +%s) - t_start) / 60 )) min" | tee -a "$R"
exec "$root/ci/model-verdict.sh" "$R"
