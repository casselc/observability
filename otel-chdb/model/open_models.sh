#!/bin/bash
# open_models.sh: the checks of the models for the open loss scenarios
# (completeness.qnt, entityCatalog.qnt, retention.qnt, sealer.qnt), quint
# 0.32 with the Rust evaluator. As in otap-rs/scripts/consumer_model.sh:
#  - `sim` rows: the design instances by simulation (expect ok), witnesses
#    (a violation of `not(w)` means w was reached: expect VIOLATED), each
#    mutant against the invariant it breaks (expect VIOLATED), and the
#    findings the design does not claim (expect VIOLATED, marked "finding");
#  - `runs` rows: the scripted runs (`*BreaksTest` on the mutant ends with
#    the invariant broken; `*DesignTest` on the design, where the fatal step
#    is disabled): expect all-pass.
# One quint process at a time (each takes up to a few GB).
#
#   OUT=results.txt model/open_models.sh
set -u
OUT=${OUT:-/dev/stdout}
SEED=${SEED:-0x5eed}
cd "$(dirname "$0")" || exit 1
echo "quint $(quint --version), seed $SEED" | tee "$OUT"
fails=0
sim() { # file main invariant samples steps expect [note]
  local t0 out v; t0=$(date +%s.%N)
  out=$(quint run "$1" --main "$2" --invariant "$3" --max-samples "$4" --max-steps "$5" --seed "$SEED" 2>&1)
  v=ok; echo "$out" | grep -q "\[violation\]" && v=VIOLATED
  echo "$out" | grep -q -E "\[ok\]|\[violation\]" || v=ERROR
  [ "$v" = "$6" ] || fails=$((fails + 1))
  printf "sim   %-18s %-18s %-34s %6s x %-3s %-8s (expect %s) %5.1fs %s\n" "$1" "$2" "$3" "$4" "$5" "$v" "$6" \
    "$(echo "$(date +%s.%N) - $t0" | bc)" "${7:-}" | tee -a "$OUT"
}
runs() { # file main match
  local out n f; out=$(quint test "$1" --main "$2" --match "$3" --max-samples 1 2>&1)
  n=$(echo "$out" | grep -c " ok "); f=$(echo "$out" | grep -c "failed after")
  [ "$n" -gt 0 ] && [ "$f" -eq 0 ] || fails=$((fails + 1))
  printf "runs  %-18s %-18s %-44s passed %d failed %d (expect all-pass)\n" "$1" "$2" "$3" "$n" "$f" | tee -a "$OUT"
}

# ---- completeness: complete_through and the alert evaluator (LS-6, LS-7, LS-8) ----
C=completeness.qnt
sim $C completenessDesign safety 20000 80 ok
for w in wFire wOk wOkIdleLane wLakeServed wPageNoSource wPageStale wReplayEvaluated wZombieLanded wRegress wNotOldestFirst wAllWindows; do
  sim $C completenessDesign "not($w)" 20000 80 VIOLATED
done
sim $C noHeartbeat safety 5000 80 ok "(safe; stalls: idleLaneStallsTest)"
sim $C listTimeIdle completeSound 20000 80 VIOLATED
sim $C lastReceived completeSound 20000 80 VIOLATED
sim $C maxNotPrefix completeSound 20000 80 VIOLATED
sim $C noBirth completeSound 20000 80 VIOLATED
sim $C evalPastComplete evalWithinComplete 20000 80 VIOLATED
sim $C noDataOk noSilentOk 20000 80 VIOLATED
sim $C unlabeledFallback resultLabeled 20000 80 VIOLATED
runs $C completenessDesign "DesignTest|idleLaneStallsTest"
for m in listTimeIdle lastReceived maxNotPrefix noBirth evalPastComplete noDataOk unlabeledFallback; do runs $C $m "${m}BreaksTest"; done
runs $C noHeartbeat idleLaneStallsTest

# ---- entityCatalog: grace window, controller outages, the announcement lane (LS-5) ----
E=entityCatalog.qnt
sim $E catalogDesign safety 20000 60 ok
sim $E sameLane "and { safety, exactAfterLag }" 20000 60 ok
sim $E catalogHealthy "and { safety, exactAtQuery }" 20000 60 ok
sim $E catalogDesign exactAfterLag 20000 60 VIOLATED "finding: a separate announcement lane lags its rows"
sim $E catalogDesign exactAtQuery 20000 60 VIOLATED "finding: outages are transiently inexact"
for w in wOutageLife wHealed wGraceRow wReannounced wAnnLost; do sim $E catalogDesign "not($w)" 20000 60 VIOLATED; done
sim $E noAnnounce noPermanentOrphan 20000 60 VIOLATED
sim $E announceEarly noPermanentOrphan 20000 60 VIOLATED
sim $E shortGrace exactAtQuery 20000 60 VIOLATED
runs $E catalogDesign "outageAnnouncedDesignTest|separateLane"
runs $E sameLane sameLaneDesignTest
runs $E catalogHealthy graceDesignTest
for m in noAnnounce announceEarly shortGrace; do runs $E $m "${m}BreaksTest"; done

# ---- retention: TTL against edge custody age (LS-10) ----
R=retention.qnt
sim $R retentionDesign safety 20000 50 ok
sim $R retentionDesign custodyAgeBounded 20000 50 ok
for w in wOldReplay wReplayToCold wTtlDrop wBackpressure wOlderThanCap wCutOffline; do sim $R retentionDesign "not($w)" 20000 50 VIOLATED; done
sim $R retentionShort acceptedVisible 20000 50 VIOLATED
sim $R cutDuringOutage acceptedVisible 20000 50 VIOLATED
sim $R capSized acceptedVisible 20000 50 VIOLATED
sim $R capSized custodyAgeBounded 20000 50 VIOLATED
sim $R dropOldest acceptedVisible 20000 50 ok
sim $R dropOldest noEdgeDrop 20000 50 VIOLATED "(the price of drop_oldest)"
runs $R retentionDesign DesignTest
runs $R retentionShort retentionShortBreaksTest
runs $R cutDuringOutage cutOfflineBreaksTest
runs $R capSized capSizedBreaksTest
runs $R dropOldest dropOldestTest

# ---- sealer: the lake's snapshot log ----
S=sealer.qnt
sim $S sealerDesign safety 20000 40 ok
for w in wLost412 wSkippedCopy wWmAdvanced wBeatAdvances wBudgetLeft; do sim $S sealerDesign "not($w)" 20000 40 VIOLATED; done
sim $S blindCommit repeatableAsOf 20000 40 VIOLATED
sim $S noRebase monotone 20000 40 VIOLATED
sim $S wmFromList wmSound 20000 40 VIOLATED
sim $S noDedup atMostOncePerSnapshot 20000 40 VIOLATED
runs $S sealerDesign DesignTest
for m in blindCommit noRebase wmFromList noDedup; do runs $S $m "${m}BreaksTest"; done

echo "rows not as expected: $fails" | tee -a "$OUT"
[ "$fails" -eq 0 ]
