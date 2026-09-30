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
# (the scripted runs before the durations; the durations' own runs are below)
for m in s3InlineConsumerDesign designCopies designDays; do
  runs $m "(releaseInFlight|releaseSettled|keeperOverrun|noHorizon|restamp|wallRange|errorSettles|gcReopens)DesignTest" all-pass
done
for m in designQuiet designShortLease; do runs $m "(releaseInFlight|releaseSettled|keeperOverrun|noHorizon|restamp|wallRange|errorSettles)DesignTest" all-pass; done
# HORIZON 0: an edge replay is skipped (restampDesignTest); a sender's resend on a later day is not.
runs noHorizonReplays "(restamp|releaseInFlight|keeperOverrun|errorSettles|gcReopens)DesignTest" all-pass
runs noHorizon restampDesignTest all-pass
for m in releaseInFlight keeperOverrun noHorizon restamp wallRange gcReopens errorSettles; do runs $m "${m}BreaksTest" all-pass; done
# Durations (2026-09-27, the DST CAST: STPA.md issues 13 and 14; AMBIGUITY.md S3):
# slow LIST answers, HEADs that cost time, ambiguous lease / checkpoint writes.
# slowSafety = safety (with noLiveTakeover) + workFitsWindow + noOwnDrop + noGapStall
# + noLateLeaseStall + oneHolder.
for m in designSlow designSlowQuiet designSlowShort; do sim $C $m slowSafety 5000 60 ok; done
for w in wListRace wRenewMidScan wOwn412Kept wOrphanWrite wCasLost wTakeover wPrevKept; do
  sim $C designSlow "not($w)" 20000 60 VIOLATED
done
sim $C designSlowQuiet "not(wSlowIngest)" 20000 60 VIOLATED
sim $C observeAtRequest noLiveTakeover 20000 60 VIOLATED
# (the duplicate needs a slow LIST, a takeover of a live lease and the old
# holder's statement landing: random runs miss it; observeAtRequestBreaksTest has it)
sim $C observeAtRequest atMostOnce 20000 60 ok
# The two liveness mutants: their progress proxy breaks, safety holds.
sim $C renewOnlyAtInsert workFitsWindow 20000 60 VIOLATED
sim $C renewOnlyAtInsert safety 5000 60 ok
sim $C own412IsTakeover noOwnDrop 20000 60 VIOLATED
sim $C own412IsTakeover safety 5000 60 ok
for m in designSlow designSlowQuiet; do runs $m observeDesignTest all-pass; done
runs designSlowShort "(scan|own412)DesignTest" all-pass
# (an MBT finding, fixed: a lost renewal request keeps the lane, on its old window)
runs designSlowShort lostRenewalKeepsTest all-pass
for m in observeAtRequest renewOnlyAtInsert own412IsTakeover; do runs $m "${m}BreaksTest" all-pass; done
# A write applied after the reader's check (2026-09-29, STPA.md CAST-50; LATE_CAS):
# a checkpoint write's answer times out in flight, the read-back finds it
# unchanged, it lands later and GC deletes by it. slowSafety includes
# noGapStall. designSlow* have the step too (above, and the MBT replays them).
sim $C designLate slowSafety 5000 60 ok
for w in wLateLanded wLateTaken; do sim $C designLate "not($w)" 20000 60 VIOLATED; done
sim $C designSlow "not(wLateLanded)" 20000 60 VIOLATED
# (random runs rarely reach the stall once a worker awaiting its own answer is excluded:
# 0 in 20,000 at 0x5eed, 465 s; lateCkptLostBreaksTest pins it)
sim $C lateCkptLost noGapStall 5000 60 VIOLATED
sim $C lateCkptLost safety 5000 60 ok
for m in designLate designSlow designSlowQuiet; do runs $m lateCkptDesignTest all-pass; done
runs lateCkptLost lateCkptLostBreaksTest all-pass
# A lease renewal applied after the reader's check (2026-09-30, STPA.md CAST-74; LEASE_REFRESH):
# the holder adopts its own late renewal (owner, epoch, the renewal's own send time / beat)
# instead of lapsing on, or dropping the lane from, the older version. slowSafety (above,
# designLate and designSlow*) includes noLateLeaseStall and oneHolder.
sim $C designLate "not(wLeaseLateTaken)" 20000 60 VIOLATED
sim $C lateLeaseLost noLateLeaseStall 5000 60 VIOLATED
sim $C lateLeaseLost safety 5000 60 ok
for m in designLate designSlow designSlowQuiet; do runs $m "lateLease(Drop)?DesignTest" all-pass; done
runs lateLeaseLost "lateLease(Lost|Drop)BreaksTest" all-pass
sim $K compactDesign compactSafety 5000 60 ok
sim $K compactQuiet compactSafety 5000 60 ok
sim $K compactQuiet "not(wReleased)" 20000 60 VIOLATED
sim $K earlyCompact neverSkipsCommittedCompact 20000 60 VIOLATED
sim $K floorOnly bounded 20000 150 VIOLATED
