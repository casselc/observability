// -*- mode: Bluespec; -*-
/**
 * partLifetime: object lifetime on one published s3_plain_rewritable table,
 * between the writer that inserts, merges and cleans up parts, read-only
 * readers that refresh their part list and run queries, and the central
 * consumer's garbage collection of a finished generation.
 *
 * The question: can any reader read an object that has been deleted, and
 * can objects be left behind forever? Time is an integer tick; the timing
 * bounds (lifetime, refresh interval, query length) are exactly what is
 * being verified, so they are modelled explicitly.
 */
module partLifetime {
  const PARTS: Set[int]         // part ids the writer can create (ordered: higher = newer)
  const READERS: Set[int]
  const MAX_TIME: int
  const LIFETIME: int           // old_parts_lifetime, in ticks
  const REFRESH: int            // refresh_parts_interval, in ticks; readers refresh at least this often
  const QMAX: int               // queries run at most this many ticks (then time out)
  const READERS_MAY_STOP_REFRESH: bool // a reader can stop refreshing (refresh_parts_interval = 0)
  const WRITER_MAY_EXIT: bool   // the writer process can exit (shutdown, crash)
  const GC_WAITS_FOR_LEASES: bool // consumer GC deletes a generation only when no reader holds it
  const GC_ENABLED: bool

  type PartState = Absent | Active | Outdated | Deleted
  type Part = { st: PartState, since: int, covers: Set[int] }   // covers: parts merged into it
  type Query = { running: bool, started: int, parts: Set[int], read: Set[int] }
  type Reader = { attached: bool, view: Set[int], refreshedAt: int, refreshing: bool, q: Query }

  var now: int
  var parts: int -> Part
  var writerUp: bool
  var readers: int -> Reader
  var acked: bool               // the consumer has ingested and acknowledged the generation
  var gcDone: bool
  var badRead: bool             // a query touched a deleted object (the violation, as state)
  var events: Set[str]

  // ---- pure logic ------------------------------------------------------------
  pure val NOQ: Query = { running: false, started: 0, parts: Set(), read: Set() }

  pure def active(ps: int -> Part): Set[int] = ps.keys().filter(p => ps.get(p).st == Active)
  pure def outdated(ps: int -> Part): Set[int] = ps.keys().filter(p => ps.get(p).st == Outdated)

  // What a refresh loads: every part whose objects still exist, minus the
  // ones covered by another listed part (ClickHouse's covering rule).
  pure def listed(ps: int -> Part): Set[int] =
    ps.keys().filter(p => ps.get(p).st == Active or ps.get(p).st == Outdated)
  pure def visible(ps: int -> Part): Set[int] = {
    val l = listed(ps)
    l.filter(p => not(l.exists(q => ps.get(q).covers.contains(p))))
  }

  pure def nextFree(ps: int -> Part): Set[int] = ps.keys().filter(p => ps.get(p).st == Absent)

  // ---- writer ----------------------------------------------------------------
  action insertPart(p: int): bool = all {
    writerUp, parts.get(p).st == Absent,
    // parts are created in id order, so a merged part is newer than its sources
    nextFree(parts).forall(q => q >= p),
    parts' = parts.set(p, { st: Active, since: now, covers: Set() }),
    now' = now, writerUp' = writerUp, readers' = readers, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events.union(Set("insert")),
  }

  // Merge two active parts into a new one; the sources become outdated.
  action merge(a: int, b: int, m: int): bool = all {
    writerUp, a != b, parts.get(a).st == Active, parts.get(b).st == Active,
    parts.get(m).st == Absent, nextFree(parts).forall(q => q >= m),
    parts' = parts.set(m, { st: Active, since: now, covers: Set(a, b) })
                  .set(a, { ...parts.get(a), st: Outdated, since: now })
                  .set(b, { ...parts.get(b), st: Outdated, since: now }),
    now' = now, writerUp' = writerUp, readers' = readers, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events.union(Set("merge")),
  }

  // The cleanup thread deletes an outdated part's objects once it is older
  // than old_parts_lifetime. Only a running writer cleans up.
  action cleanup(p: int): bool = all {
    writerUp, parts.get(p).st == Outdated, now - parts.get(p).since >= LIFETIME,
    parts' = parts.set(p, { ...parts.get(p), st: Deleted }),
    now' = now, writerUp' = writerUp, readers' = readers, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events.union(Set("cleanup")),
  }

  // Exit only after some merge: an exit before any merge can leak nothing
  // and only starves the random walk of the interesting traces.
  action writerExit: bool = all {
    WRITER_MAY_EXIT, writerUp, events.contains("merge"),
    writerUp' = false,
    now' = now, parts' = parts, readers' = readers, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events.union(Set("exit")),
  }

  // ---- readers -----------------------------------------------------------------
  action attach(r: int): bool = all {
    not(readers.get(r).attached), not(gcDone),
    readers' = readers.set(r, { attached: true, view: visible(parts), refreshedAt: now, refreshing: true, q: NOQ }),
    now' = now, parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events,
  }

  action refresh(r: int): bool = all {
    readers.get(r).attached, readers.get(r).refreshing,
    readers' = readers.set(r, { ...readers.get(r), view: visible(parts), refreshedAt: now }),
    now' = now, parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events,
  }

  action stopRefreshing(r: int): bool = all {
    READERS_MAY_STOP_REFRESH, readers.get(r).attached, readers.get(r).refreshing,
    readers' = readers.set(r, { ...readers.get(r), refreshing: false }),
    now' = now, parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events.union(Set("stopRefresh")),
  }

  // A query snapshots the reader's current view of parts.
  action startQuery(r: int): bool = all {
    readers.get(r).attached, not(readers.get(r).q.running), readers.get(r).view != Set(),
    readers' = readers.set(r, { ...readers.get(r), q: { running: true, started: now, parts: readers.get(r).view, read: Set() } }),
    now' = now, parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events,
  }

  // Reading one part's objects; if they are gone, the query fails (the
  // "File .../data.bin does not exist" of the ClickHouse server test).
  action readPart(r: int, p: int): bool = {
    val q = readers.get(r).q
    all {
      q.running, q.parts.contains(p), not(q.read.contains(p)),
      val gone = parts.get(p).st == Deleted
      all {
        badRead' = (badRead or gone),   // parenthesised: without them Quint parses (badRead' = badRead) or gone
        readers' = readers.set(r, { ...readers.get(r), q: if (gone) NOQ else { ...q, read: q.read.union(Set(p)) } }),
        events' = if (gone) events.union(Set("badRead")) else events.union(Set("read")),
      },
      now' = now, parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone,
    }
  }

  action finishQuery(r: int): bool = {
    val q = readers.get(r).q
    all {
      q.running, q.read == q.parts,
      readers' = readers.set(r, { ...readers.get(r), q: NOQ }),
      now' = now, parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone,
      badRead' = badRead, events' = events.union(Set("queryDone")),
    }
  }

  action detach(r: int): bool = all {
    readers.get(r).attached, not(readers.get(r).q.running),
    readers' = readers.set(r, { attached: false, view: Set(), refreshedAt: 0, refreshing: false, q: NOQ }),
    now' = now, parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone,
    badRead' = badRead, events' = events,
  }

  // ---- consumer ------------------------------------------------------------------
  action ack: bool = all {
    not(acked), parts.keys().exists(p => parts.get(p).st == Active),
    acked' = true,
    now' = now, parts' = parts, writerUp' = writerUp, readers' = readers, gcDone' = gcDone,
    badRead' = badRead, events' = events,
  }

  // Garbage collection deletes the whole generation prefix.
  action gc: bool = all {
    GC_ENABLED, acked, not(gcDone),
    GC_WAITS_FOR_LEASES implies READERS.forall(r => not(readers.get(r).attached)),
    parts' = parts.keys().mapBy(p => if (parts.get(p).st == Absent) parts.get(p) else { ...parts.get(p), st: Deleted }),
    gcDone' = true,
    now' = now, writerUp' = writerUp, readers' = readers, acked' = acked,
    badRead' = badRead, events' = events.union(Set("gc")),
  }

  // ---- time ----------------------------------------------------------------------
  // Time advances only while every refreshing reader is within its refresh
  // interval and every query within QMAX; a query at QMAX times out first.
  action tick: bool = all {
    now < MAX_TIME,
    READERS.forall(r => readers.get(r).attached and readers.get(r).refreshing implies now + 1 - readers.get(r).refreshedAt <= REFRESH),
    readers' = READERS.mapBy(r => {
      val rd = readers.get(r)
      if (rd.q.running and now + 1 - rd.q.started > QMAX) { ...rd, q: NOQ } else rd
    }),
    now' = now + 1,
    parts' = parts, writerUp' = writerUp, acked' = acked, gcDone' = gcDone, badRead' = badRead,
    events' = events,
  }

  action init = all {
    now' = 0,
    parts' = PARTS.mapBy(_ => { st: Absent, since: 0, covers: Set() }),
    writerUp' = true,
    readers' = READERS.mapBy(_ => { attached: false, view: Set(), refreshedAt: 0, refreshing: false, q: NOQ }),
    acked' = false, gcDone' = false, badRead' = false, events' = Set(),
  }

  action step = {
    nondet r = READERS.oneOf()
    nondet p = PARTS.oneOf()
    nondet a = PARTS.oneOf()
    nondet b = PARTS.oneOf()
    any {
      insertPart(p), merge(a, b, p), cleanup(p), writerExit,
      attach(r), refresh(r), stopRefreshing(r), startQuery(r), readPart(r, p), finishQuery(r), detach(r),
      ack, gc, tick,
    }
  }

  // ---- properties ------------------------------------------------------------------
  // No query ever reads an object that has been deleted.
  val noReadOfDeleted: bool = not(badRead)

  // A reader never double counts: its view never holds a part and one that covers it.
  val noDoubleCount: bool =
    READERS.forall(r => readers.get(r).view.forall(p => not(readers.get(r).view.exists(q => parts.get(q).covers.contains(p)))))

  // No leak: once nothing more can clean up (writer gone, no GC to come),
  // no outdated part's objects remain.
  val noLeakAfterExit: bool =
    (not(writerUp) and not(GC_ENABLED and not(gcDone))) implies outdated(parts) == Set()

  // ---- witnesses ---------------------------------------------------------------------
  val wMerged: bool = events.contains("merge")
  val wCleaned: bool = events.contains("cleanup")
  val wQueryDone: bool = events.contains("queryDone")
  val wReadAfterMerge: bool = events.contains("merge") and events.contains("read")
  val wGc: bool = events.contains("gc")
  val wExit: bool = events.contains("exit")
}

// ---- instances ---------------------------------------------------------------------
// Time: 1 tick ~ 1 refresh interval at refresh_parts_interval = 1 s.

// chDB's default (old_parts_lifetime 0), readers refreshing every tick, queries up to 2 ticks.
module lifetime0 {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 5, LIFETIME = 0, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).*
}
// Lifetime equal to the query bound: still unsafe (a refresh lag is added on top).
module lifetimeEqQmax {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 7, LIFETIME = 2, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).*
}
// Lifetime > query bound + refresh interval: safe.
module lifetimeSafe {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).*
}
// ...unless a reader stops refreshing.
module stoppedRefresh {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = true, WRITER_MAY_EXIT = false, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).*
}
// The writer may exit: merged-away parts leak unless the consumer's GC removes the prefix.
module writerExitNoGc {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = true, GC_WAITS_FOR_LEASES = true, GC_ENABLED = false).*
}
// Consumer GC without leases: deletes under a running query.
module gcWithoutLeases {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = true, GC_WAITS_FOR_LEASES = false, GC_ENABLED = true).*
}
// The proposed lifecycle: lifetime > QMAX + REFRESH, readers always refresh, GC waits for leases.
module proposedLifecycle {
  import partLifetime(PARTS = Set(1, 2, 3), READERS = Set(1, 2), MAX_TIME = 8, LIFETIME = 4, REFRESH = 1, QMAX = 2,
    READERS_MAY_STOP_REFRESH = false, WRITER_MAY_EXIT = true, GC_WAITS_FOR_LEASES = true, GC_ENABLED = true).*
}
