#!/bin/bash
# consumer_model.sh: the consumer models' checks (quint 0.32, rust evaluator):
# the design instances by simulation, witnesses (a violation of `not(w)` means
# w was reached), each mutant by simulation, and each fleet-scale mutant's
# scripted counterexample (`*BreaksTest` on the mutant; `*DesignTest` on the
# designs: the step the counterexample needs is disabled there).
#
#   OUT=results/consumer/scale/model.txt scripts/consumer_model.sh
set -u
OUT=${OUT:-/dev/stdout}
SEED=${SEED:-0x5eed}
cd "$(dirname "$0")/../../model" || exit 1
echo "quint $(quint --version), seed $SEED" | tee "$OUT"
sim() { # file main invariant samples steps expect
  local t0 out v; t0=$(date +%s.%N)
  out=$(quint run "$1" --main "$2" --invariant "$3" --max-samples "$4" --max-steps "$5" --seed "$SEED" 2>&1)
  v=ok; echo "$out" | grep -q "\[violation\]" && v=VIOLATED
  printf "sim   %-32s %-26s %-28s %6s x %-3s %-8s (expect %s) %.1fs\n" "$1" "$2" "$3" "$4" "$5" "$v" "$6" "$(echo "$(date +%s.%N) - $t0" | bc)" | tee -a "$OUT"
}
runs() { # main match expect
  local out n f; out=$(quint test s3InlineConsumer.qnt --main "$1" --match "$2" 2>&1)
  n=$(echo "$out" | grep -c " ok "); f=$(echo "$out" | grep -c "failed after")
  printf "runs  %-26s %-26s passed %d failed %d (expect %s)\n" "$1" "$2" "$n" "$f" "$3" | tee -a "$OUT"
}
C=s3InlineConsumer.qnt K=s3InlineConsumerCompact.qnt
for m in s3InlineConsumerDesign designQuiet designShortLease designCopies designDays noHorizonReplays; do sim $C $m safety 5000 60 ok; done
# The horizon audit: silent on the designs; under noHorizon every duplicate is one it reports.
# Edge replays keep their custody day: with no sender resends, even HORIZON 0 raises nothing.
for m in designCopies designDays noHorizonReplays; do sim $C $m auditSilent 5000 60 ok; done
sim $C noHorizon dupAudited 20000 60 ok
sim $C restamp dupAudited 20000 60 ok
for w in wReleasedIngest wCopyAcrossDays wReplayKeepsDay wLateLanding wMidnight wTakeoverIngest wPartial wFenced wCasLost; do
  sim $C designCopies "not($w)" 20000 60 VIOLATED
done
sim $C noTimeBound atMostOnce 20000 60 VIOLATED
sim $C noVerify neverSkipsCommitted 20000 60 VIOLATED
sim $C gcTombs safety 20000 60 VIOLATED
sim $C announceEarly announcedOnlyAfterCommit 20000 60 VIOLATED
sim $C wallRange atMostOnce 20000 60 VIOLATED
sim $C releaseInFlight atMostOnce 20000 60 VIOLATED
sim $C keeperOverrun atMostOnce 20000 60 VIOLATED
sim $C errorSettles atMostOnce 20000 60 VIOLATED
sim $C noHorizon atMostOnce 20000 60 VIOLATED
sim $C restamp atMostOnce 20000 60 VIOLATED
sim $C noHorizonReplays "not(wReplayKeepsDay)" 20000 60 VIOLATED
sim $C gcReopens neverSkipsCommitted 20000 60 VIOLATED
# (gcReopensDesignTest needs writer faults: a lost answer)
for m in s3InlineConsumerDesign designCopies designDays; do runs $m DesignTest all-pass; done
for m in designQuiet designShortLease; do runs $m "(releaseInFlight|releaseSettled|keeperOverrun|noHorizon|restamp|wallRange|errorSettles)DesignTest" all-pass; done
# HORIZON 0: an edge replay is skipped (restampDesignTest); a sender's resend on a later day is not.
runs noHorizonReplays "(restamp|releaseInFlight|keeperOverrun|errorSettles|gcReopens)DesignTest" all-pass
runs noHorizon restampDesignTest all-pass
for m in releaseInFlight keeperOverrun noHorizon restamp wallRange gcReopens errorSettles; do runs $m "${m}BreaksTest" all-pass; done
sim $K compactDesign compactSafety 5000 60 ok
sim $K compactQuiet compactSafety 5000 60 ok
sim $K compactQuiet "not(wReleased)" 20000 60 VIOLATED
sim $K earlyCompact neverSkipsCommittedCompact 20000 60 VIOLATED
sim $K floorOnly bounded 20000 150 VIOLATED
