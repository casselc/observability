// -*- mode: Bluespec; -*-
// Deterministic scenarios for partLifetime: the minimal story behind each
// lifecycle hazard, and the configuration that rules it out.

// chDB's default old_parts_lifetime = 0: a merge's cleanup deletes the parts
// a running query snapshotted (the ClickHouse server test's
// "data.bin does not exist").
module lifetime0Test {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 5, LIFETIME = 0, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).* from "./partLifetime"

  run mergeDeletesUnderQueryTest =
    init
      .then(insertPart(1)).then(insertPart(2))
      .then(attach(1)).then(startQuery(1))
      .then(merge(1, 2, 3)).then(cleanup(1))
      .then(readPart(1, 1))
      .expect(not(noReadOfDeleted))
}

// A lifetime no longer than the query bound plus the refresh interval is not
// enough: a query may start from a view up to one refresh old.
module lifetimeEqQmaxTest {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 7, LIFETIME = 2, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).* from "./partLifetime"

  run staleViewOutlivesLifetimeTest =
    init
      .then(insertPart(1)).then(insertPart(2))
      .then(attach(1))                    // view {1, 2} at t0
      .then(merge(1, 2, 3))               // 1 and 2 outdated at t0
      .then(tick)                         // t1: the view is stale but within its refresh interval
      .then(startQuery(1))                // snapshots {1, 2}
      .then(refresh(1))                   // the next query would see {3}; this one keeps its snapshot
      .then(tick)                         // t2: part 1 is 2 ticks old
      .then(cleanup(1))
      .then(readPart(1, 1))               // 1 tick into a 2-tick query
      .expect(not(noReadOfDeleted))
}

// The same story, with lifetime > QMAX + REFRESH: cleanup cannot fire in time.
module lifetimeSafeTest {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).* from "./partLifetime"

  run cleanupWaitsTest =
    init
      .then(insertPart(1)).then(insertPart(2))
      .then(attach(1)).then(merge(1, 2, 3))
      .then(tick).then(startQuery(1)).then(refresh(1)).then(tick)
      .then(cleanup(1))
      .fail()

  // The query outlives QMAX instead: it is timed out, never reading a deleted part.
  run longQueryTimesOutTest =
    init
      .then(insertPart(1)).then(insertPart(2))
      .then(attach(1)).then(startQuery(1)).then(merge(1, 2, 3))
      .then(refresh(1)).then(tick).then(refresh(1)).then(tick).then(refresh(1)).then(tick)
      .expect(and { not(readers.get(1).q.running), noReadOfDeleted })
}

// A reader that stops refreshing keeps a view of parts that are cleaned up
// long after; its next query fails however long the lifetime is.
module stoppedRefreshTest {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = true, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).* from "./partLifetime"

  run staleReaderFailsTest =
    init
      .then(insertPart(1)).then(insertPart(2))
      .then(attach(1)).then(stopRefreshing(1))
      .then(merge(1, 2, 3))
      .then(tick).then(tick).then(tick).then(tick)
      .then(cleanup(1))
      .then(startQuery(1)).then(readPart(1, 1))
      .expect(not(noReadOfDeleted))
}

// The writer exits before old_parts_lifetime has passed since a merge: nobody
// ever deletes the merged-away parts (the leak measured in bench/central).
module writerExitTest {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = true, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).* from "./partLifetime"

  run exitLeaksOutdatedPartsTest =
    init
      .then(insertPart(1)).then(insertPart(2)).then(merge(1, 2, 3))
      .then(writerExit)
      .expect(and { not(noLeakAfterExit), outdated(parts) == Set(1, 2) })
}

// Consumer GC that only waits for its own acknowledgement deletes a
// generation under another reader's running query.
module gcWithoutLeasesTest {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = true, GC_WAITS_FOR_LEASES = false, GC_ENABLED = true).* from "./partLifetime"

  run gcUnderQueryTest =
    init
      .then(insertPart(1)).then(insertPart(2))
      .then(attach(1)).then(startQuery(1))
      .then(ack).then(gc)
      .then(readPart(1, 1))
      .expect(not(noReadOfDeleted))
}

// The proposed lifecycle: GC waits for every reader to detach, and removes
// the whole prefix, leaked parts included.
module proposedLifecycleTest {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1, 2), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = true, GC_WAITS_FOR_LEASES = true, GC_ENABLED = true).* from "./partLifetime"

  run gcWaitsForLeaseTest =
    init
      .then(insertPart(1)).then(insertPart(2))
      .then(attach(1)).then(startQuery(1))
      .then(ack).then(gc)
      .fail()

  run gcAfterDetachReclaimsLeakTest =
    init
      .then(insertPart(1)).then(insertPart(2)).then(merge(1, 2, 3)).then(writerExit)
      .then(ack).then(gc)
      .expect(and { noLeakAfterExit, parts.keys().forall(p => parts.get(p).st == Absent or parts.get(p).st == Deleted) })
}
