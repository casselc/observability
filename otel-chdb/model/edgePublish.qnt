// -*- mode: Bluespec; -*-
/**
 * edgePublish: the chdb exporter's publish protocol (chdbexporter/publish.go)
 * and the central consumer it is designed for, as one shared-state model.
 *
 * Actors, coordinating through shared state (S3 objects, the central table,
 * the ledger), not messages:
 *  - the collector's persistent sending queue, holding OTLP payloads until an
 *    export succeeds; a failed export is retried (the same payload, which the
 *    exporter publishes as a NEW batch id);
 *  - one writer process (one exporter instance per signal), which may crash
 *    and restart with a new epoch at any time;
 *  - S3, where each write (table insert, Parquet object, manifest) may
 *    succeed, fail, or fail AMBIGUOUSLY: the object lands but the writer sees
 *    an error (a timeout after the PUT was applied; or a crash between the
 *    manifest PUT and the queue ack);
 *  - central workers that claim manifested batches, INSERT ... SELECT them
 *    with a deduplication token, and record them in a ledger, and that may
 *    crash between insert and ledger update.
 *
 * Design choices are flags, so the current design, fixes, and mutations are
 * instances of one spec (see the Analysis modules at the end).
 */
module edgePublish {
  import basicSpells.* from "./basicSpells"

  // ---- system model -------------------------------------------------------
  const PAYLOADS: Set[int]    // OTLP requests entering the queue
  const WORKERS: Set[int]     // central ingest workers
  const MAX_EPOCH: int        // writer incarnations explored
  const MAX_BATCH: int        // batch ids per epoch explored
  const MAX_GEN: int          // generations per epoch explored
  const DEDUP_WINDOW: int     // central insert-dedup tokens remembered
  const AMBIGUOUS_WRITES: bool // S3 may apply a write yet report failure
  const OTHER_TRAFFIC: bool   // unrelated inserts can push tokens out of the dedup window
  // design under test
  const ROTATION_LOCK: bool   // rotation waits for in-flight pushes (publish.go acquire)
  const SEAL_FROM_LISTING: bool // seal lists S3 manifests instead of trusting in-memory counters
  const CONTENT_KEY: bool     // dedup token / ledger keyed on batch content, not (epoch, batch_id)
  const CONTENT_LEASE: bool   // a claim leases the content key, not just the batch
  const CHECK_CENTRAL: bool   // before inserting, check the central table for the key
                              // (assumes that read sees committed inserts: sequential consistency)
  const CONTENT_BATCH_ID: bool // the batch id is a hash of the payload, so a retry rewrites the same
                              // table block (insert_deduplication_token + a dedup window on the edge
                              // table), the same Parquet key and the same manifest key
  const CRASHES: bool         // the writer process can crash and restart with a new epoch

  type BatchKey = { epoch: int, batch: int }
  type GenKey = { epoch: int, gen: int }
  type Phase = Encoded | TableDone | ParquetDone
  type Push = { payload: int, bk: BatchKey, gk: GenKey, phase: Phase }
  type Outcome = Ok | Fail | Ambiguous
  type Manifest = { bk: BatchKey, gk: GenKey, payload: int }
  type Seal = { gk: GenKey, batches: Set[BatchKey] }
  // A published source: a batch in a generation's tables. With content-derived
  // batch ids one batch id can be published in two generations (a retry
  // after rotation), so central rows are counted per source, not per id.
  type SourceKey = { bk: BatchKey, gk: GenKey }
  type Writer = {
    epoch: int,
    nextBatch: int,
    gen: int,
    pushes: Set[Push],
    // batches whose export returned success, per generation: what the
    // in-memory counters in publish.go (g.batches, g.rows) reflect
    succeeded: Set[{ gk: GenKey, bk: BatchKey }],
    rotated: Set[GenKey],       // rotated away, seal not yet written
  }
  type Worker = { claim: Set[Manifest], inserted: bool }  // claim: empty or one manifest

  // ---- state --------------------------------------------------------------
  var queue: Set[int]                   // unacknowledged payloads (persistent)
  var writer: Writer
  var tableRows: Set[{ src: SourceKey, payload: int }]  // what readers attached live see
  var parquet: Set[SourceKey]                           // objects under each generation's prefix
  var manifests: Set[Manifest]
  var seals: Set[Seal]
  var workers: int -> Worker
  var ledger: Set[BatchKey]             // batches the consumer has acknowledged
  var ledgerContent: Set[int]           // their content keys (used when CONTENT_KEY)
  var centralCount: SourceKey -> int    // inserts into central, per source (batch, generation)
  var dedupWindow: List[int]            // last DEDUP_WINDOW insert tokens (FIFO)
  var events: Set[str]                  // for witnesses only

  // ---- pure logic ---------------------------------------------------------
  pure val OTHER_TOKEN = 0              // a token from unrelated traffic

  pure def allSourceKeys(maxEpoch: int, maxBatch: int, maxGen: int): Set[SourceKey] =
    tuples(1.to(maxEpoch), 1.to(maxBatch), 1.to(maxGen))
      .map(t => { bk: { epoch: t._1, batch: t._2 }, gk: { epoch: t._1, gen: t._3 } })

  pure def src(m: Manifest): SourceKey = { bk: m.bk, gk: m.gk }

  // The insert deduplication token a worker uses for a batch.
  pure def token(m: Manifest, contentKey: bool): int =
    if (contentKey) m.payload else 100 * m.bk.epoch + m.bk.batch

  pure def remember(w: List[int], t: int, cap: int): List[int] = {
    val l = w.append(t)
    if (l.length() > cap) l.slice(l.length() - cap, l.length()) else l
  }

  pure def inWindow(w: List[int], t: int): bool = w.indices().exists(i => w[i] == t)

  pure def canStartPush(w: Writer, queue: Set[int], p: int, maxBatch: int, contentId: bool): bool = and {
    queue.contains(p),
    not(w.pushes.exists(x => x.payload == p)),  // the queue hands an item to one consumer at a time
    contentId or w.nextBatch <= maxBatch,
  }

  // With a content-derived id the batch is named by its payload (here the
  // payload id stands for the hash), so every attempt at it has one name.
  pure def startPush(w: Writer, p: int, contentId: bool): Writer = {
    ...w,
    nextBatch: if (contentId) w.nextBatch else w.nextBatch + 1,
    pushes: w.pushes.union(Set({ payload: p, bk: { epoch: w.epoch, batch: if (contentId) p else w.nextBatch },
                                 gk: { epoch: w.epoch, gen: w.gen }, phase: Encoded })),
  }

  pure def advance(w: Writer, x: Push, ph: Phase): Writer =
    { ...w, pushes: w.pushes.exclude(Set(x)).union(Set({ ...x, phase: ph })) }

  pure def abort(w: Writer, x: Push): Writer = { ...w, pushes: w.pushes.exclude(Set(x)) }

  pure def complete(w: Writer, x: Push): Writer =
    { ...w, pushes: w.pushes.exclude(Set(x)), succeeded: w.succeeded.union(Set({ gk: x.gk, bk: x.bk })) }

  pure def canRotate(w: Writer, maxGen: int, lock: bool): bool = and {
    w.gen < maxGen,
    lock implies not(w.pushes.exists(x => x.gk.gen == w.gen)),
  }

  pure def rotate(w: Writer): Writer =
    { ...w, gen: w.gen + 1, rotated: w.rotated.union(Set({ epoch: w.epoch, gen: w.gen })) }

  pure def sealOf(w: Writer, ms: Set[Manifest], gk: GenKey, fromListing: bool): Seal =
    if (fromListing) { gk: gk, batches: ms.filter(m => m.gk == gk).map(m => m.bk) }
    else { gk: gk, batches: w.succeeded.filter(s => s.gk == gk).map(s => s.bk) }

  pure def restarted(w: Writer): Writer =
    { epoch: w.epoch + 1, nextBatch: 1, gen: 1, pushes: Set(), succeeded: Set(), rotated: Set() }

  pure def payloadCount(counts: SourceKey -> int, ms: Set[Manifest], p: int): int =
    ms.filter(m => m.payload == p).fold(0, (acc, m) => acc + counts.get(src(m)))

  // Central inserts of one batch id, over every generation it was published in.
  pure def batchCount(counts: SourceKey -> int, bk: BatchKey): int =
    counts.keys().filter(k => k.bk == bk).fold(0, (acc, k) => acc + counts.get(k))

  // ---- actions: writer ----------------------------------------------------
  action unchangedConsumer = all {
    workers' = workers, ledger' = ledger, ledgerContent' = ledgerContent,
    centralCount' = centralCount, dedupWindow' = dedupWindow,
  }

  action pushStart(p: int): bool = all {
    canStartPush(writer, queue, p, MAX_BATCH, CONTENT_BATCH_ID),
    writer' = startPush(writer, p, CONTENT_BATCH_ID),
    queue' = queue, tableRows' = tableRows, parquet' = parquet, manifests' = manifests, seals' = seals,
    events' = events, unchangedConsumer,
  }

  // One S3 write of a push. `lands` is whether the object exists afterwards,
  // `ok` whether the writer is told so.
  action writeTable(x: Push, o: Outcome): bool = all {
    writer.pushes.contains(x), x.phase == Encoded,
    AMBIGUOUS_WRITES or o != Ambiguous,
    val lands = o != Fail
    all {
      tableRows' = if (lands) tableRows.union(Set({ src: { bk: x.bk, gk: x.gk }, payload: x.payload })) else tableRows,
      writer' = if (o == Ok) advance(writer, x, TableDone) else abort(writer, x),
    },
    events' = if (o == Ambiguous) events.union(Set("ambiguous")) else events,
    queue' = queue, parquet' = parquet, manifests' = manifests, seals' = seals, unchangedConsumer,
  }

  action writeParquet(x: Push, o: Outcome): bool = all {
    writer.pushes.contains(x), x.phase == TableDone,
    AMBIGUOUS_WRITES or o != Ambiguous,
    parquet' = if (o != Fail) parquet.union(Set({ bk: x.bk, gk: x.gk })) else parquet,
    writer' = if (o == Ok) advance(writer, x, ParquetDone) else abort(writer, x),
    events' = if (o == Ambiguous) events.union(Set("ambiguous")) else events,
    queue' = queue, tableRows' = tableRows, manifests' = manifests, seals' = seals, unchangedConsumer,
  }

  // The commit point. Ok: the export returns success and the queue drops the
  // payload. Otherwise the payload stays queued and will be retried.
  action writeManifest(x: Push, o: Outcome): bool = all {
    writer.pushes.contains(x), x.phase == ParquetDone,
    AMBIGUOUS_WRITES or o != Ambiguous,
    manifests' = if (o != Fail) manifests.union(Set({ bk: x.bk, gk: x.gk, payload: x.payload })) else manifests,
    writer' = if (o == Ok) complete(writer, x) else abort(writer, x),
    queue' = if (o == Ok) queue.exclude(Set(x.payload)) else queue,
    events' = if (o == Ambiguous) events.union(Set("ambiguous")) else events,
    tableRows' = tableRows, parquet' = parquet, seals' = seals, unchangedConsumer,
  }

  action rotateGen: bool = all {
    canRotate(writer, MAX_GEN, ROTATION_LOCK),
    writer' = rotate(writer),
    queue' = queue, tableRows' = tableRows, parquet' = parquet, manifests' = manifests, seals' = seals,
    events' = events, unchangedConsumer,
  }

  // The background seal of a rotated generation writes _sealed.json.
  action sealGen(gk: GenKey): bool = all {
    writer.rotated.contains(gk),
    seals' = seals.union(Set(sealOf(writer, manifests, gk, SEAL_FROM_LISTING))),
    writer' = { ...writer, rotated: writer.rotated.exclude(Set(gk)) },
    events' = events.union(Set("sealed")),
    queue' = queue, tableRows' = tableRows, parquet' = parquet, manifests' = manifests, unchangedConsumer,
  }

  // Crash and restart: in-flight pushes and unsealed generations are lost;
  // the queue is persistent; the new incarnation has a new epoch.
  action crash: bool = all {
    CRASHES, writer.epoch < MAX_EPOCH,
    writer' = restarted(writer),
    events' = events.union(Set("crash")),
    queue' = queue, tableRows' = tableRows, parquet' = parquet, manifests' = manifests, seals' = seals,
    unchangedConsumer,
  }

  // ---- actions: central consumer ------------------------------------------
  action unchangedEdge = all {
    queue' = queue, writer' = writer, tableRows' = tableRows, parquet' = parquet,
    manifests' = manifests, seals' = seals,
  }

  def claimable(m: Manifest): bool = and {
    not(ledger.contains(m.bk)),
    not(WORKERS.exists(w => workers.get(w).claim.contains(m))),   // leased by another worker
    CONTENT_LEASE implies not(WORKERS.exists(w => workers.get(w).claim.exists(c => c.payload == m.payload))),
    CONTENT_KEY implies not(ledgerContent.contains(m.payload)),
  }

  action claim(w: int, m: Manifest): bool = all {
    manifests.contains(m), workers.get(w).claim == Set(), claimable(m),
    workers' = workers.set(w, { claim: Set(m), inserted: false }),
    ledger' = ledger, ledgerContent' = ledgerContent, centralCount' = centralCount,
    dedupWindow' = dedupWindow, events' = events, unchangedEdge,
  }

  // With a content key, a batch whose content is already ingested is
  // acknowledged without inserting.
  action skipDuplicate(m: Manifest): bool = all {
    CONTENT_KEY, manifests.contains(m), not(ledger.contains(m.bk)), ledgerContent.contains(m.payload),
    ledger' = ledger.union(Set(m.bk)),
    workers' = workers, ledgerContent' = ledgerContent, centralCount' = centralCount,
    dedupWindow' = dedupWindow, events' = events.union(Set("skipped")), unchangedEdge,
  }

  // INSERT INTO central ... SELECT ... WHERE batch = m, with insert_deduplication_token.
  action insert(w: int): bool = {
    val ws = workers.get(w)
    all {
      ws.claim != Set(), not(ws.inserted),
      val m = ws.claim.fold({ bk: { epoch: 0, batch: 0 }, gk: { epoch: 0, gen: 0 }, payload: 0 }, (_, x) => x)
      val t = token(m, CONTENT_KEY)
      val present = if (CONTENT_KEY) payloadCount(centralCount, manifests, m.payload) > 0
                    else batchCount(centralCount, m.bk) > 0
      val dup = inWindow(dedupWindow, t) or (CHECK_CENTRAL and present)
      all {
        centralCount' = if (dup) centralCount else centralCount.setBy(src(m), c => c + 1),
        dedupWindow' = if (dup) dedupWindow else remember(dedupWindow, t, DEDUP_WINDOW),
        events' = if (dup) events.union(Set("dedupHit")) else events.union(Set("inserted")),
      },
      workers' = workers.set(w, { ...ws, inserted: true }),
      ledger' = ledger, ledgerContent' = ledgerContent, unchangedEdge,
    }
  }

  action ack(w: int): bool = {
    val ws = workers.get(w)
    all {
      ws.inserted,
      val m = ws.claim.fold({ bk: { epoch: 0, batch: 0 }, gk: { epoch: 0, gen: 0 }, payload: 0 }, (_, x) => x)
      all {
        ledger' = ledger.union(Set(m.bk)),
        ledgerContent' = ledgerContent.union(Set(m.payload)),
      },
      workers' = workers.set(w, { claim: Set(), inserted: false }),
      centralCount' = centralCount, dedupWindow' = dedupWindow, events' = events, unchangedEdge,
    }
  }

  // A worker dies after its insert may have committed, before the ledger
  // update: the lease lapses and the batch will be claimed again.
  action workerCrash(w: int): bool = all {
    workers.get(w).claim != Set(),
    workers' = workers.set(w, { claim: Set(), inserted: false }),
    ledger' = ledger, ledgerContent' = ledgerContent, centralCount' = centralCount,
    dedupWindow' = dedupWindow, events' = events.union(Set("workerCrash")), unchangedEdge,
  }

  action otherInsert: bool = all {
    OTHER_TRAFFIC,
    dedupWindow' = remember(dedupWindow, OTHER_TOKEN, DEDUP_WINDOW),
    workers' = workers, ledger' = ledger, ledgerContent' = ledgerContent, centralCount' = centralCount,
    events' = events, unchangedEdge,
  }

  // ---- state machine --------------------------------------------------------
  action init = all {
    queue' = PAYLOADS,
    writer' = { epoch: 1, nextBatch: 1, gen: 1, pushes: Set(), succeeded: Set(), rotated: Set() },
    tableRows' = Set(), parquet' = Set(), manifests' = Set(), seals' = Set(),
    workers' = WORKERS.mapBy(_ => { claim: Set(), inserted: false }),
    ledger' = Set(), ledgerContent' = Set(),
    centralCount' = allSourceKeys(MAX_EPOCH, MAX_BATCH, MAX_GEN).mapBy(_ => 0),
    dedupWindow' = List(),
    events' = Set(),
  }

  action step = {
    nondet o = Set(Ok, Fail, Ambiguous).oneOf()
    nondet w = WORKERS.oneOf()
    any {
      nondet p = PAYLOADS.oneOf()
      pushStart(p),
      all { writer.pushes != Set(), nondet x = writer.pushes.oneOf() any { writeTable(x, o), writeParquet(x, o), writeManifest(x, o) } },
      rotateGen,
      all { writer.rotated != Set(), nondet gk = writer.rotated.oneOf() sealGen(gk) },
      crash,
      all { manifests != Set(), nondet m = manifests.oneOf() any { claim(w, m), skipDuplicate(m) } },
      insert(w),
      ack(w),
      workerCrash(w),
      otherInsert,
    }
  }

  // ---- properties -----------------------------------------------------------
  // A manifest is only written after the batch's table rows and Parquet exist.
  val commitImpliesData: bool =
    manifests.forall(m => tableRows.contains({ src: src(m), payload: m.payload }) and parquet.contains(src(m)))

  // Nothing uncommitted reaches the central table.
  val onlyCommittedIngested: bool =
    centralCount.keys().forall(k => centralCount.get(k) > 0 implies manifests.exists(m => src(m) == k))

  // A committed batch is inserted centrally at most once.
  val batchIngestedAtMostOnce: bool =
    centralCount.keys().forall(k => batchCount(centralCount, k.bk) <= 1)

  // End to end: each OTLP request's rows land centrally at most once.
  val payloadIngestedAtMostOnce: bool =
    PAYLOADS.forall(p => payloadCount(centralCount, manifests, p) <= 1)

  // A reader attached live to the published table sees each request once.
  val liveTableNoDuplicatePayload: bool =
    PAYLOADS.forall(p => tableRows.filter(r => r.payload == p).size() <= 1)

  // A reader that only trusts manifested batches sees each request once.
  val committedNoDuplicatePayload: bool =
    PAYLOADS.forall(p => manifests.filter(m => m.payload == p).size() <= 1)

  // _sealed.json names exactly the batches committed in its generation, and
  // no batch is committed into a generation after it is sealed.
  val sealMatchesManifests: bool =
    seals.forall(s => s.batches == manifests.filter(m => m.gk == s.gk).map(m => m.bk))

  // ---- witnesses (reachability; expect > 0 traces) ---------------------------
  val wCommitted: bool = manifests != Set()
  val wSealed: bool = events.contains("sealed")
  val wIngested: bool = events.contains("inserted")
  val wAcked: bool = ledger != Set()
  val wCrashRecovered: bool = events.contains("crash") and manifests.exists(m => m.bk.epoch == 2)
  val wAmbiguous: bool = events.contains("ambiguous")
  val wDedupHit: bool = events.contains("dedupHit")
  val wAllDelivered: bool = queue == Set() and PAYLOADS.forall(p => ledgerContent.contains(p))
}

// ---- instances -----------------------------------------------------------------

// The design as built: rotation lock on; seal from in-memory counters; batch-id tokens.
module currentDesign {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = false, CONTENT_KEY = false, CONTENT_LEASE = false, CHECK_CENTRAL = false, CONTENT_BATCH_ID = false, CRASHES = true).*
}

// The same, in an ideal environment: S3 never fails ambiguously, no other
// traffic evicts dedup tokens. Which properties does the design get right?
module currentDesignIdealEnv {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = false, OTHER_TRAFFIC = false,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = false, CONTENT_KEY = false, CONTENT_LEASE = false, CHECK_CENTRAL = false, CONTENT_BATCH_ID = false, CRASHES = true).*
}

// First fix: seal from an S3 listing, content-keyed tokens and ledger.
// Relies on the dedup window.
module contentKeyOnly {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = false, CHECK_CENTRAL = false, CONTENT_BATCH_ID = false, CRASHES = true).*
}

// Full fix: plus content leases and a central check before insert, so
// nothing depends on the dedup window. Same hostile environment.
module fixedDesign {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = true, CHECK_CENTRAL = true, CONTENT_BATCH_ID = false, CRASHES = true).*
}

// Mutation: no rotation lock. Does sealing still hold with listing-based seals?
module noRotationLock {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = false, OTHER_TRAFFIC = false,
    ROTATION_LOCK = false, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = true, CHECK_CENTRAL = true, CONTENT_BATCH_ID = false, CRASHES = true).*
}

// Content-derived batch ids at the edge (retries are idempotent: same table
// block with a dedup token, same Parquet and manifest keys), nothing else
// changed at the consumer. Without crashes: every retry is the same batch.
module contentBatchIdNoCrash {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = false, CONTENT_KEY = false, CONTENT_LEASE = false,
    CHECK_CENTRAL = false, CONTENT_BATCH_ID = true, CRASHES = false).*
}

// The same with crashes: a request committed just before a crash, and
// retried by the queue after it, is committed again in the next epoch.
module contentBatchIdWithCrash {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = false, CONTENT_KEY = false, CONTENT_LEASE = false,
    CHECK_CENTRAL = false, CONTENT_BATCH_ID = true, CRASHES = true).*
}

// Recommended: content batch ids at the edge, and content keys with leases
// and a check before insert at the consumer.
module recommended {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = true,
    CHECK_CENTRAL = true, CONTENT_BATCH_ID = true, CRASHES = true).*
}
