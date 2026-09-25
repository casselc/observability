// -*- mode: Bluespec; -*-
/**
 * fastPath: central ingestion of committed edge batches, with an optional
 * FAST PATH in which the exporter, once its batch is committed in S3, also
 * posts it to central ClickHouse as an async insert, and a background
 * IMPORTER that walks the commit order and ingests whatever the fast path
 * did not. See FASTPATH.md for the design and the ClickHouse evidence.
 *
 * S3 publication is abstracted to "data written, then committed" (the
 * manifest of edgePublish, the log entry of s3Native, or the create-only data
 * object of s3Inline): the importer reads commits in order, and a batch may be
 * written and never committed. Everything below that is central ingestion.
 *
 * Actors, sharing state rather than messages:
 *  - one exporter per batch: after the commit (or, as a mutation, after the
 *    data write) it sends the batch as an HTTP async insert, until a
 *    deadline D after the commit; it may be partitioned from central, give
 *    up, time out while its entry is still queued, and retry;
 *  - the network: a request reaches central within REQ_BOUND of being sent
 *    (the timing assumption; REQ_BOUNDED = false breaks it), or is lost;
 *  - central's async-insert buffer: entries are flushed within BUSY; a crash
 *    loses every unflushed entry and can stop a flush, or a sync insert,
 *    between the partitions of a batch (INSERT is atomic per partition only);
 *  - the target table, per (batch, partition), behind a count-based dedup
 *    window of DEDUP_WINDOW tokens that unrelated inserts can evict (F4);
 *  - optionally a ledger fed by a materialized view (it fires even for a
 *    deduplicated block, and can fail after the target is written: both
 *    measured), fast-path ack markers in S3, and per-batch claims;
 *  - one importer (a lease holder: one instance at a time), with a durable
 *    checkpoint into the commit order; it checks, inserts, advances, and can
 *    crash between any two of those.
 *
 * Time is an integer tick. A tick is only possible while no bounded
 * obligation would be overdue, which is how the timing assumptions
 * (requests within REQ_BOUND, flushes within BUSY, and a healthy exporter
 * sending within D) are imposed.
 */
module fastPath {
  // ---- system model -------------------------------------------------------
  const BATCHES: Set[int]      // batches the edge writes (ids > 0)
  const PARTS: Set[int]        // target partitions each batch spans
  const D: int                 // the exporter tries the fast path only until commit + D
  const REQ_BOUND: int         // an HTTP insert reaches central within this of being sent
  const BUSY: int              // an async-insert entry is flushed (or lost) within this of arriving
  const GRACE: int             // the importer does not look at a batch before commit + GRACE
  const MAX_CLOCK: int
  const DEDUP_WINDOW: int      // non_replicated_deduplication_window, in blocks
  // environment
  const EDGE_FAULTS: bool      // exporter partitioned or killed: requests lost, give-ups, wait timeouts
  const CENTRAL_CRASH: bool    // central loses its async buffer; an insert can stop between partitions
  const IMPORTER_CRASH: bool   // the importer dies between check/insert and checkpoint
  const OTHER_TRAFFIC: bool    // unrelated inserts push tokens out of the dedup window
  const MV_FAIL: bool          // the ledger MV can fail after the target part is committed
  const REQ_BOUNDED: bool      // the REQ_BOUND assumption holds
  const COMMIT_FAILS: bool     // the edge can write a batch's data and never commit it
  // design under test
  const FAST_PATH: bool
  const FIRE_AFTER_COMMIT: bool // the fast path fires only after the S3 commit
  const WAIT: bool             // wait_for_async_insert = 1
  const MAX_ATTEMPTS: int      // fast-path sends per batch (1: fire once, never retry)
  const CHECK: str             // importer's "already ingested?" test:
                               //   "none"   rely on dedup tokens only (option e)
                               //   "any"    any row of the batch in the target
                               //   "full"   every partition of the batch present (row count = committed rows)
                               //   "ledger" a ledger table fed by an MV (option b)
                               //   "marker" a fast-path ack marker in S3 (option c)
  const CLAIMS: bool           // per-batch create-only claim with a lease of GRACE (option d)
  const RESPECT_GRACE: bool    // the importer waits out GRACE before checking a batch
  const SAME_BLOCKS: bool      // the importer's INSERT forms the same single block, so the same
                               // dedup identity, as the async entry (single-block settings)
  const REPAIR: bool           // a partly present batch gets only its missing partitions
                               // (rows) re-inserted, not the whole batch

  type Key = (int, int)                         // (batch, partition), and dedup token ids
  type Req = { b: int, sentAt: int }
  type Entry = { b: int, at: int, waiter: bool }
  // exporter phases: "idle" | "sent" | "queued" (WAIT, entry buffered) | "ok" | "gaveup"
  type Exp = { phase: str, attempts: int }
  type Imp = { phase: str, b: int }             // "idle" | "done" (checked and inserted or skipped)

  // ---- state --------------------------------------------------------------
  var clock: int
  // edge / S3
  var written: Set[int]
  var writtenAt: int -> int
  var committed: List[int]                      // commit order: what the importer walks
  var commitAt: int -> int
  var abandoned: Set[int]                       // written, will never commit
  var exps: int -> Exp
  var markers: Set[int]                         // fast-path ack markers (create-only objects)
  var claims: int -> int                        // 0 none, 1 exporter, 2 importer
  var claimAt: int -> int
  // network and central
  var net: Set[Req]
  var buffer: Set[Entry]
  var target: Key -> int                        // committed parts per (batch, partition)
  var window: List[Key]                         // dedup tokens remembered, oldest first
  var ledger: Set[int]
  // importer
  var imp: Imp
  var ckpt: int                                 // index into `committed`
  // metrics and witnesses
  var impInserts: int                           // INSERT statements the importer issued
  var impChecks: int
  var events: Set[str]

  // ---- pure logic ---------------------------------------------------------
  pure val OTHER: Key = (0, 0)
  pure val TOK_MISMATCH = 100                   // importer token ids when blocks differ

  pure def remember(w: List[Key], t: Key, cap: int): List[Key] = {
    val l = w.append(t)
    if (l.length() > cap) l.slice(l.length() - cap, l.length()) else l
  }

  pure def inWindow(w: List[Key], t: Key): bool = w.indices().exists(i => w[i] == t)

  pure def isIn(l: List[int], x: int): bool = l.indices().exists(i => l[i] == x)

  // Insert batch b's partitions `ps`, each block carrying token (b + off, p).
  // Returns the new target, window, and whether any block was deduplicated.
  pure def applyInsert(tgt: Key -> int, w: List[Key], b: int, ps: Set[int], off: int, cap: int): (Key -> int, List[Key], bool) =
    ps.fold((tgt, w, false), (acc, p) => {
      val tk = (b + off, p)
      if (inWindow(acc._2, tk)) (acc._1, acc._2, true)
      else (acc._1.setBy((b, p), c => c + 1), remember(acc._2, tk, cap), acc._3)
    })

  // ---- derived ------------------------------------------------------------
  def isCommitted(b: int): bool = isIn(committed, b)
  def base(b: int): int = if (FIRE_AFTER_COMMIT) commitAt.get(b) else writtenAt.get(b)
  def eligible(b: int): bool = if (FIRE_AFTER_COMMIT) isCommitted(b) else written.contains(b)
  def fullyIn(b: int): bool = PARTS.forall(p => target.get((b, p)) >= 1)
  def anyIn(b: int): bool = PARTS.exists(p => target.get((b, p)) >= 1)
  def impOff: int = if (SAME_BLOCKS) 0 else TOK_MISMATCH
  def toInsert(b: int): Set[int] = if (REPAIR) PARTS.filter(p => target.get((b, p)) == 0) else PARTS

  def present(b: int): bool =
    if (CHECK == "any") anyIn(b)
    else if (CHECK == "full") fullyIn(b)
    else if (CHECK == "ledger") ledger.contains(b)
    else if (CHECK == "marker") markers.contains(b)
    else false

  // ---- frame helpers --------------------------------------------------------
  action keepEdge = all {
    written' = written, writtenAt' = writtenAt, committed' = committed, commitAt' = commitAt,
    abandoned' = abandoned,
  }
  action keepImp = all { imp' = imp, ckpt' = ckpt, impInserts' = impInserts, impChecks' = impChecks }

  // ---- edge: write and commit ----------------------------------------------
  action write(b: int): bool = all {
    not(written.contains(b)),
    written' = written.union(Set(b)), writtenAt' = writtenAt.set(b, clock),
    committed' = committed, commitAt' = commitAt, abandoned' = abandoned,
    clock' = clock, exps' = exps, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  action commit(b: int): bool = all {
    written.contains(b), not(isCommitted(b)), not(abandoned.contains(b)),
    committed' = committed.append(b), commitAt' = commitAt.set(b, clock),
    written' = written, writtenAt' = writtenAt, abandoned' = abandoned,
    clock' = clock, exps' = exps, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  action abandon(b: int): bool = all {
    COMMIT_FAILS, written.contains(b), not(isCommitted(b)), not(abandoned.contains(b)),
    abandoned' = abandoned.union(Set(b)),
    written' = written, writtenAt' = writtenAt, committed' = committed, commitAt' = commitAt,
    clock' = clock, exps' = exps, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  // ---- exporter: the fast path ------------------------------------------------
  action fpSend(b: int): bool = all {
    FAST_PATH, eligible(b),
    exps.get(b).phase == "idle", exps.get(b).attempts < MAX_ATTEMPTS,
    clock <= base(b) + D,
    CLAIMS implies claims.get(b) != 2,
    net' = net.union(Set({ b: b, sentAt: clock })),
    exps' = exps.set(b, { phase: "sent", attempts: exps.get(b).attempts + 1 }),
    claims' = if (CLAIMS and claims.get(b) == 0) claims.set(b, 1) else claims,
    claimAt' = if (CLAIMS and claims.get(b) == 0) claimAt.set(b, clock) else claimAt,
    clock' = clock, keepEdge, markers' = markers,
    buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  // The request never reaches central; the exporter sees an error or a timeout.
  action fpDrop(r: Req): bool = all {
    EDGE_FAULTS, net.contains(r),
    net' = net.exclude(Set(r)),
    exps' = if (exps.get(r.b).phase == "sent") exps.set(r.b, { ...exps.get(r.b), phase: "idle" }) else exps,
    clock' = clock, keepEdge, markers' = markers, claims' = claims, claimAt' = claimAt,
    buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  // The request reaches central and becomes an async-insert entry. With
  // wait_for_async_insert = 0 the exporter is answered now ("ok" = queued).
  action fpArrive(r: Req): bool = all {
    net.contains(r),
    net' = net.exclude(Set(r)),
    buffer' = buffer.union(Set({ b: r.b, at: clock, waiter: WAIT })),
    exps' = if (exps.get(r.b).phase == "sent")
              exps.set(r.b, { ...exps.get(r.b), phase: if (WAIT) "queued" else "ok" })
            else exps,
    events' = if (exps.get(r.b).phase != "sent") events.union(Set("lateArrival")) else events,
    clock' = clock, keepEdge, markers' = markers, claims' = claims, claimAt' = claimAt,
    target' = target, window' = window, ledger' = ledger, keepImp,
  }

  // The exporter stops waiting (wait_for_async_insert_timeout, or a broken
  // connection) while its entry is still queued: an ambiguous outcome.
  action fpTimeout(b: int): bool = all {
    EDGE_FAULTS, Set("sent", "queued").contains(exps.get(b).phase),
    exps' = exps.set(b, { ...exps.get(b), phase: "idle" }),
    clock' = clock, keepEdge, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events.union(Set("fpTimeout")),
  }

  action fpGiveUp(b: int): bool = all {
    EDGE_FAULTS, exps.get(b).phase == "idle",
    exps' = exps.set(b, { ...exps.get(b), phase: "gaveup" }),
    clock' = clock, keepEdge, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  // Option c: after it was told "ok", the exporter writes a create-only ack marker.
  action writeMarker(b: int): bool = all {
    CHECK == "marker", exps.get(b).phase == "ok", not(markers.contains(b)),
    markers' = markers.union(Set(b)),
    clock' = clock, keepEdge, exps' = exps, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  // ---- central --------------------------------------------------------------
  // Flush one entry (a real flush combines several; each entry keeps its own
  // dedup identity, measured). The MV then writes the ledger row, even when
  // the block was deduplicated (measured), unless it fails (MV_FAIL).
  action flush(e: Entry, mvOk: bool): bool = all {
    buffer.contains(e), MV_FAIL or mvOk,
    val r = applyInsert(target, window, e.b, PARTS, 0, DEDUP_WINDOW)
    all {
      target' = r._1, window' = r._2,
      events' = events.union(Set(if (r._3) "dedupHit" else "fastLanded")),
    },
    ledger' = if (mvOk) ledger.union(Set(e.b)) else ledger,
    buffer' = buffer.exclude(Set(e)),
    exps' = if (e.waiter and exps.get(e.b).phase == "queued") exps.set(e.b, { ...exps.get(e.b), phase: "ok" }) else exps,
    clock' = clock, keepEdge, markers' = markers, claims' = claims, claimAt' = claimAt, net' = net, keepImp,
  }

  // Central crashes: every unflushed entry is lost, and one flush in progress
  // may have committed only some partitions (`ps`, a proper subset). Waiting
  // exporters get an error.
  action centralCrash(e: Entry, ps: Set[int]): bool = all {
    CENTRAL_CRASH, buffer.contains(e), ps != PARTS,
    val r = applyInsert(target, window, e.b, ps, 0, DEDUP_WINDOW)
    all { target' = r._1, window' = r._2 },
    buffer' = Set(),
    exps' = exps.keys().mapBy(b => if (exps.get(b).phase == "queued") { ...exps.get(b), phase: "idle" } else exps.get(b)),
    events' = events.union(Set(if (ps != Set()) "partialFlush" else "bufferLost")),
    ledger' = ledger, clock' = clock, keepEdge, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, keepImp,
  }

  action otherInsert: bool = all {
    OTHER_TRAFFIC,
    window' = remember(window, OTHER, DEDUP_WINDOW),
    clock' = clock, keepEdge, exps' = exps, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, ledger' = ledger, keepImp, events' = events,
  }

  // ---- importer -------------------------------------------------------------
  def nextBatch: int = committed[ckpt]
  def mayLook(b: int): bool = and {
    RESPECT_GRACE implies clock >= commitAt.get(b) + GRACE,
    CLAIMS implies (claims.get(b) != 1 or clock >= claimAt.get(b) + GRACE),
  }

  // Check, then skip or insert (INSERT ... SELECT FROM s3(<key>) with the token).
  action impStep(mvOk: bool): bool = all {
    imp.phase == "idle", ckpt < committed.length(), MV_FAIL or mvOk,
    val b = nextBatch
    all {
      mayLook(b),
      claims' = if (CLAIMS) claims.set(b, 2) else claims,
      claimAt' = claimAt,
      imp' = { phase: "done", b: b },
      impChecks' = impChecks + 1,
      if (present(b)) all {
        target' = target, window' = window, ledger' = ledger, impInserts' = impInserts,
        events' = events.union(Set("impSkipped")),
      } else {
        val r = applyInsert(target, window, b, toInsert(b), impOff, DEDUP_WINDOW)
        all {
          target' = r._1, window' = r._2,
          ledger' = if (mvOk) ledger.union(Set(b)) else ledger,
          impInserts' = impInserts + 1,
          events' = events.union(Set(if (r._3) "impDedupHit" else "impInserted")),
        }
      },
    },
    ckpt' = ckpt,
    clock' = clock, keepEdge, exps' = exps, markers' = markers, net' = net, buffer' = buffer,
  }

  // Central crashes during the importer's own insert: some partitions land;
  // the importer sees an error and will redo the batch. The buffer is lost too.
  action impPartial(ps: Set[int]): bool = all {
    CENTRAL_CRASH, imp.phase == "idle", ckpt < committed.length(),
    val b = nextBatch
    all {
      mayLook(b), not(present(b)), ps.subseteq(toInsert(b)), ps != toInsert(b),
      val r = applyInsert(target, window, b, ps, impOff, DEDUP_WINDOW)
      all { target' = r._1, window' = r._2 },
    },
    buffer' = Set(),
    exps' = exps.keys().mapBy(x => if (exps.get(x).phase == "queued") { ...exps.get(x), phase: "idle" } else exps.get(x)),
    impInserts' = impInserts + 1, impChecks' = impChecks + 1,
    imp' = imp, ckpt' = ckpt, ledger' = ledger, claims' = claims, claimAt' = claimAt,
    events' = events.union(Set("impPartial")),
    clock' = clock, keepEdge, markers' = markers, net' = net,
  }

  action impAdvance: bool = all {
    imp.phase == "done",
    ckpt' = ckpt + 1, imp' = { phase: "idle", b: 0 },
    impInserts' = impInserts, impChecks' = impChecks,
    clock' = clock, keepEdge, exps' = exps, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger, events' = events,
  }

  // Dies after inserting (or deciding to skip), before the checkpoint.
  action impCrash: bool = all {
    IMPORTER_CRASH, imp.phase == "done",
    imp' = { phase: "idle", b: 0 },
    ckpt' = ckpt, impInserts' = impInserts, impChecks' = impChecks,
    clock' = clock, keepEdge, exps' = exps, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    events' = events.union(Set("impCrash")),
  }

  // ---- time -----------------------------------------------------------------
  // Time moves on only while nothing bounded would become overdue.
  def exporterOwes(b: int): bool = and {
    FAST_PATH, not(EDGE_FAULTS), eligible(b), exps.get(b).phase == "idle", exps.get(b).attempts < MAX_ATTEMPTS,
  }
  action tick: bool = all {
    clock < MAX_CLOCK,
    REQ_BOUNDED implies net.forall(r => clock + 1 <= r.sentAt + REQ_BOUND),
    buffer.forall(e => clock + 1 <= e.at + BUSY),
    BATCHES.forall(b => exporterOwes(b) implies clock + 1 <= base(b) + D),
    clock' = clock + 1,
    keepEdge, exps' = exps, markers' = markers, claims' = claims, claimAt' = claimAt,
    net' = net, buffer' = buffer, target' = target, window' = window, ledger' = ledger,
    keepImp, events' = events,
  }

  // ---- state machine ----------------------------------------------------------
  action init = all {
    clock' = 0,
    written' = Set(), writtenAt' = BATCHES.mapBy(_ => -1), committed' = List(), commitAt' = BATCHES.mapBy(_ => -1),
    abandoned' = Set(),
    exps' = BATCHES.mapBy(_ => { phase: "idle", attempts: 0 }),
    markers' = Set(), claims' = BATCHES.mapBy(_ => 0), claimAt' = BATCHES.mapBy(_ => 0),
    net' = Set(), buffer' = Set(),
    target' = tuples(BATCHES, PARTS).mapBy(_ => 0),
    window' = List(), ledger' = Set(),
    imp' = { phase: "idle", b: 0 }, ckpt' = 0, impInserts' = 0, impChecks' = 0,
    events' = Set(),
  }

  action step = {
    nondet b = BATCHES.oneOf()
    nondet mvOk = Set(true, false).oneOf()
    nondet ps = PARTS.powerset().oneOf()
    any {
      write(b), commit(b), abandon(b),
      fpSend(b), fpTimeout(b), fpGiveUp(b), writeMarker(b),
      all { net != Set(), nondet r = net.oneOf() any { fpDrop(r), fpArrive(r) } },
      all { buffer != Set(), nondet e = buffer.oneOf() any { flush(e, mvOk), centralCrash(e, ps) } },
      otherInsert,
      impStep(mvOk), impPartial(ps), impAdvance, impCrash,
      tick,
    }
  }

  // ---- properties -------------------------------------------------------------
  // Nothing reaches the target unless its batch is committed in S3.
  val onlyCommittedIngested: bool =
    tuples(BATCHES, PARTS).forall(k => target.get(k) > 0 implies isCommitted(k._1))

  // After dedup, every partition of every batch is in the target at most once.
  val batchIngestedAtMostOnce: bool =
    tuples(BATCHES, PARTS).forall(k => target.get(k) <= 1)

  // Completeness: the importer never moves its checkpoint past a batch that
  // is not wholly in the target. So it never skips a batch whose fast-path
  // insert was lost, and at quiescence (checkpoint at the end of the commit
  // order) every committed batch is ingested.
  val noLostBehindCheckpoint: bool =
    committed.indices().forall(i => i < ckpt implies fullyIn(committed[i]))

  // Happy-path efficiency (checked on fault-free instances): the importer
  // never issues an INSERT; the fast path did them all.
  val importerNeverInserts: bool = impInserts == 0

  val safety: bool = onlyCommittedIngested and batchIngestedAtMostOnce and noLostBehindCheckpoint

  // ---- witnesses (expect traces that violate them) ---------------------------
  val allIngested: bool = and {
    committed.length() == BATCHES.size(), ckpt == committed.length(),
    BATCHES.forall(b => fullyIn(b)),
  }
  val wAllIngested: bool = not(allIngested)
  val wAllIngestedNoImporterInsert: bool = not(allIngested and impInserts == 0 and impChecks == BATCHES.size())
  val wFastLanded: bool = not(events.contains("fastLanded"))
  val wImporterInserted: bool = not(events.contains("impInserted"))
  val wImporterSkipped: bool = not(events.contains("impSkipped"))
  val wDedupHit: bool = not(events.contains("dedupHit") or events.contains("impDedupHit"))
  val wPartialFlush: bool = not(events.contains("partialFlush"))
  val wBufferLost: bool = not(events.contains("bufferLost"))
  val wLateArrival: bool = not(events.contains("lateArrival"))
  val wImporterCrash: bool = not(events.contains("impCrash"))
}

// ---- instances -------------------------------------------------------------------
// Common numbers: 2 batches x 2 partitions; exporter deadline D = 1, request
// bound 1, flush bound 1, so a fast-path insert can land up to commit + 3.
// GRACE = 4 > D + REQ_BOUND + BUSY. Dedup window 2, with unrelated traffic.

// Recommended: fast path after commit, wait=1, importer waits out the grace
// then checks the target for the full batch (projection), same single block.
module recommended {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// The same design with the fast path off: the importer alone.
module recommendedNoFastPath {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = false, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Recommended with wait_for_async_insert = 0: the exporter learns nothing.
module recommendedNoWait {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = false, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Fault-free: the recommended design should leave the importer nothing to insert.
module happyPath {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = false, CENTRAL_CRASH = false, IMPORTER_CRASH = false, OTHER_TRAFFIC = true,
    MV_FAIL = false, REQ_BOUNDED = true, COMMIT_FAILS = false,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Fault-free, but the importer does not wait out the grace period.
module happyPathNoGrace {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = false, CENTRAL_CRASH = false, IMPORTER_CRASH = false, OTHER_TRAFFIC = true,
    MV_FAIL = false, REQ_BOUNDED = true, COMMIT_FAILS = false,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = false, SAME_BLOCKS = true, REPAIR = true).*
}

// Mutation: no grace, hostile environment. Correctness now rests on the window.
module noGrace {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = false, SAME_BLOCKS = true, REPAIR = true).*
}

// Mutation: the grace equals D + REQ_BOUND + BUSY exactly (no margin).
module graceNoMargin {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 3, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Mutation of the environment: a fast-path request can reach central
// arbitrarily late (no bound on request lifetime), so no grace suffices.
module unboundedRequests {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = false, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Mutation: the fast path fires once the data object is written, before the commit.
module fireBeforeCommit {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = false, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Option e: no check, dedup tokens only.
module tokenOnly {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "none", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// Option e with the fast path off (the importer alone, F4).
module tokenOnlyNoFastPath {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = false, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "none", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// Option a, but "any row present" instead of "every partition present".
module checkAnyRow {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "any", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Option b: the ledger table fed by an MV.
module ledgerCheck {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "ledger", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// Option b (ledger only, no target consult) if the MV never fails after the target write.
module ledgerCheckMvReliable {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = false, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "ledger", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// Option c: fast-path ack markers, written after wait=1 confirms the flush.
module markerWait {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "marker", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// Option c with wait=0: the marker means "queued", not "committed".
module markerNoWait {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = false, CHECK = "marker", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// Option d: claims, with no check (a claim says who may insert, not whether anyone did).
module claimsNoCheck {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "none", CLAIMS = true,
    RESPECT_GRACE = false, SAME_BLOCKS = true, REPAIR = false).*
}

// Option d plus the full check, and no separate grace: the claim lease is the grace.
module claimsWithCheck {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = true,
    RESPECT_GRACE = false, SAME_BLOCKS = true, REPAIR = true).*
}

// With REPAIR and a respected grace, the recommended design never needs a
// dedup hit. The token is the backstop only when the timing assumption
// breaks: a fast-path request that lands after the importer inserted.
// Here requests are unbounded but the window is never evicted: the shared
// token catches the late copy...
module lateArrivalTokenBackstop {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 8, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = false,
    MV_FAIL = true, REQ_BOUNDED = false, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// ...unless the importer's INSERT forms different blocks from the async entry
// (default Parquet reader chunking on a big batch), so the tokens never match.
module blockMismatch {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 8, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = false,
    MV_FAIL = true, REQ_BOUNDED = false, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = false, REPAIR = true).*
}

// Mutation: a partly present batch is re-inserted whole (same tokens), so the
// partitions that did land are deduplicated only while still in the window.
module fullNoRepair {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// The same batch always in one partition (partition key from batch-constant
// envelope columns, e.g. toDate(received_at)): every insert is atomic.
module onePartition {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = false).*
}

// Mutation: the exporter retries the fast path (twice) after an error or a
// wait timeout. A retry is deduplicated only while its token is in the window.
module fastPathRetries {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 2, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// ...the same, but a batch never spans partitions: retries still depend on the window.
module fastPathRetriesOnePartition {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 2, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// ...and with retries but no unrelated traffic (a window that is never evicted
// within the retry horizon): the window covers them.
module fastPathRetriesWindowHolds {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 8, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = false,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 2, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "full", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}

// Option b as a filter in front of option a: a ledger hit skips the batch;
// a miss consults the target and inserts only what is missing.
module ledgerThenTarget {
  import fastPath(BATCHES = Set(1, 2), PARTS = Set(1, 2), D = 1, REQ_BOUND = 1, BUSY = 1, GRACE = 4, MAX_CLOCK = 7,
    DEDUP_WINDOW = 2, EDGE_FAULTS = true, CENTRAL_CRASH = true, IMPORTER_CRASH = true, OTHER_TRAFFIC = true,
    MV_FAIL = true, REQ_BOUNDED = true, COMMIT_FAILS = true,
    MAX_ATTEMPTS = 1, FAST_PATH = true, FIRE_AFTER_COMMIT = true, WAIT = true, CHECK = "ledger", CLAIMS = false,
    RESPECT_GRACE = true, SAME_BLOCKS = true, REPAIR = true).*
}
