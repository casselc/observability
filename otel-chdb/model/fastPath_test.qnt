// -*- mode: Bluespec; -*-
// Deterministic scenarios for fastPath: the minimal story behind each
// counterexample the simulator found, and the same story against the
// recommended design. Run one module with
//   quint test fastPath_test.qnt --main <module> --backend typescript

module fpScenarios {
  import fastPath.* from "./fastPath"
  pure def rq(b: int, t: int): Req = { b: b, sentAt: t }
  pure def en(b: int, t: int, w: bool): Entry = { b: b, at: t, waiter: w }
}

// The recommended design, fault-free: the fast path does every insert.
module happyPathTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = false, CENTRAL_CRASH = false, IMPORTER_CRASH = false, OTHER_TRAFFIC = true,
    MV_FAIL = false, REQ_BOUNDED = true, COMMIT_FAILS = false,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run bothBatchesFastImporterOnlyChecksTest =
    init
      .then(write(1)).then(write(2)).then(commit(1)).then(commit(2))
      .then(fpSend(1)).then(fpSend(2))
      .then(fpArrive(rq(1, 0))).then(fpArrive(rq(2, 0)))
      .then(flush(en(1, 0, true), true)).then(flush(en(2, 0, true), true))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .then(impStep(true)).then(impAdvance)
      .expect(and { allIngested, impInserts == 0, impChecks == 2, safety })
}

// Fault-free, but the importer does not wait: it inserts what the fast path
// has queued, and the fast path's copy is dropped by the dedup window.
module happyPathNoGraceTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = false, CENTRAL_CRASH = false, IMPORTER_CRASH = false, OTHER_TRAFFIC = true,
    MV_FAIL = false, REQ_BOUNDED = true, COMMIT_FAILS = false,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = false, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run importerDuplicatesTheWorkTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(impStep(true)).then(impAdvance)
      .then(flush(en(1, 0, true), true))
      .expect(and { impInserts == 1, not(importerNeverInserts), batchIngestedAtMostOnce,
                    events.contains("dedupHit") })
}

module recommendedTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  // Central crashes with the entry still buffered; the exporter (wait=1) gets
  // an error and does not retry. After the grace, the importer finds nothing
  // and inserts.
  run lostBufferImporterRecoversTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(centralCrash(en(1, 0, true), Set()))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { fullyIn(1), impInserts == 1, safety, exps.get(1).phase == "idle" })

  // Central crashes mid-flush: partition 1 committed, partition 2 not.
  // Unrelated inserts evict the token. The importer's full check sees the
  // batch incomplete and inserts only partition 2.
  run partialFlushRepairedTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(centralCrash(en(1, 0, true), Set(1)))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { target.get((1, 1)) == 1, target.get((1, 2)) == 1, safety })

  // The MV fails after the fast path's rows are committed: the target is
  // what the importer consults, so the missing ledger row costs nothing.
  run mvFailureHarmlessTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(flush(en(1, 0, true), false))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { not(ledger.contains(1)), impInserts == 0, safety })

  // An importer crash after inserting (F4's shape): the retry checks first.
  run importerCrashRetryChecksTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpDrop(rq(1, 0))).then(fpGiveUp(1))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impCrash)
      .then(otherInsert).then(otherInsert)
      .then(impStep(true)).then(impAdvance)
      .expect(and { impInserts == 1, fullyIn(1), safety })
}

// The same design with the fast path off: the importer alone is correct.
module recommendedNoFastPathTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = false, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run importerAloneTest =
    init
      .then(write(1)).then(commit(1)).then(write(2)).then(commit(2))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impCrash).then(otherInsert).then(otherInsert)
      .then(impStep(true)).then(impAdvance)
      .then(impStep(true)).then(impAdvance)
      .expect(and { allIngested, impInserts == 2, safety })
}

// Grace = D + REQ_BOUND + BUSY exactly: the importer may look in the same
// tick as the last possible flush, before it.
module graceNoMarginTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 3, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run checkRacesLastFlushTest =
    init
      .then(write(1)).then(commit(1)).then(tick)
      .then(fpSend(1)).then(tick)             // sent at 1 = commit + D
      .then(fpArrive(rq(1, 1))).then(tick)    // arrives at 2; clock 3 = commit + GRACE
      .then(impStep(true)).then(impAdvance)   // entry still buffered: absent, insert
      .then(otherInsert).then(otherInsert)
      .then(flush(en(1, 2, true), true))      // flushed at 3, within BUSY
      .expect(not(batchIngestedAtMostOnce))
}

module noGraceTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = false, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run bufferedEntryMissedByCheckTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(impStep(true)).then(impAdvance)
      .then(otherInsert).then(otherInsert)
      .then(flush(en(1, 0, true), true))
      .expect(not(batchIngestedAtMostOnce))
}

module checkAnyRowTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "any", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run partialBatchSkippedTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(centralCrash(en(1, 0, true), Set(1)))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { events.contains("impSkipped"), not(noLostBehindCheckpoint) })
}

module fullNoRepairTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  run partialBatchReinsertedWholeTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(centralCrash(en(1, 0, true), Set(1)))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { target.get((1, 1)) == 2, not(batchIngestedAtMostOnce) })
}

module fireBeforeCommitTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = false, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run uncommittedBatchIngestedTest =
    init
      .then(write(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(flush(en(1, 0, true), true)).then(abandon(1))
      .expect(not(onlyCommittedIngested))
}

// F4 with no fast path at all: tokens alone do not cover an importer crash.
module tokenOnlyNoFastPathTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = false, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "none", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  run importerCrashEvictedTest =
    init
      .then(write(1)).then(commit(1))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impCrash)
      .then(otherInsert).then(otherInsert)
      .then(impStep(true))
      .expect(not(batchIngestedAtMostOnce))
}

module tokenOnlyTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "none", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  // Even on the fast path's happy path, the importer re-inserts every batch
  // and only the window stops the copy.
  run happyPathReinsertEvictedTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(flush(en(1, 0, true), true))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true))
      .expect(not(batchIngestedAtMostOnce))
}

module markerNoWaitTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = false, CHECK = "marker", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  // wait=0: "ok" means queued. The marker is written, central crashes, the
  // entry is lost, and the importer trusts the marker.
  run markerForLostEntryTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(writeMarker(1))
      .then(centralCrash(en(1, 0, false), Set()))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { markers.contains(1), not(noLostBehindCheckpoint) })
}

module markerWaitTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "marker", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  // wait=1 markers are sound, but a marker check alone leaves the importer's
  // own crash retry (F4) to the window: the marker never exists for batches
  // the importer inserts.
  run importerCrashWithoutMarkerTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpDrop(rq(1, 0))).then(fpGiveUp(1))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impCrash)
      .then(otherInsert).then(otherInsert)
      .then(impStep(true))
      .expect(not(batchIngestedAtMostOnce))
}

module ledgerCheckTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "ledger", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  // The MV fails after the target part is committed (measured: the target
  // keeps the rows, the ledger has none). The importer trusts the ledger.
  run mvFailureReinsertsTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(flush(en(1, 0, true), false))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true))
      .expect(not(batchIngestedAtMostOnce))
}

module ledgerThenTargetTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "ledger", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run mvFailureFallsBackToTargetTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(flush(en(1, 0, true), false))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { fullyIn(1), safety })
}

module claimsNoCheckTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "none", CLAIMS = true,
    RESPECT_GRACE = false, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  // The exporter's claim lapses; the importer takes it over and, with no
  // check, re-inserts what the fast path already committed.
  run claimLapsesTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(flush(en(1, 0, true), true))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true))
      .expect(and { claims.get(1) == 2, not(batchIngestedAtMostOnce) })
}

module fastPathRetriesTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 2, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  // The first attempt commits partition 2 before central dies; the exporter
  // retries the whole batch after the token was evicted.
  run retryAfterPartialFlushTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(centralCrash(en(1, 0, true), Set(2)))
      .then(otherInsert).then(otherInsert)
      .then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(flush(en(1, 0, true), true))
      .expect(and { target.get((1, 2)) == 2, not(batchIngestedAtMostOnce) })
}

module lateArrivalTokenBackstopTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 8, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = false,
    MV_FAIL = true, REQ_BOUNDED = false, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  // A request outlives every bound, lands after the importer inserted, and
  // the shared token (same single block, still in the window) drops it.
  run lateCopyDeduplicatedTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpTimeout(1))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .then(fpArrive(rq(1, 0))).then(flush(en(1, 4, true), true))
      .expect(and { events.contains("dedupHit"), safety })
}

module blockMismatchTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 8, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = false,
    MV_FAIL = true, REQ_BOUNDED = false, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = false, REPAIR = true).* from "./fastPath"
  import fpScenarios.*

  run lateCopyNotRecognisedTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpTimeout(1))
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .then(fpArrive(rq(1, 0))).then(flush(en(1, 4, true), true))
      .expect(not(batchIngestedAtMostOnce))
}

module onePartitionTest {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).* from "./fastPath"
  import fpScenarios.*

  // One partition per batch: a crash either commits the batch or not.
  run crashIsAllOrNothingTest =
    init
      .then(write(1)).then(commit(1)).then(fpSend(1)).then(fpArrive(rq(1, 0)))
      .then(centralCrash(en(1, 0, true), Set()))
      .then(otherInsert).then(otherInsert)
      .then(tick).then(tick).then(tick).then(tick)
      .then(impStep(true)).then(impAdvance)
      .expect(and { fullyIn(1), safety })
}
