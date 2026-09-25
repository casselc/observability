// -*- mode: Bluespec; -*-
/**
 * s3Inline: edge publishing with no manifests. The data object IS the commit
 * record. See ../awss3/README.md for the design and the evidence, and
 * s3Native.qnt for the variant it simplifies (a separate log entry per batch).
 *
 * Layout: one log per writer incarnation (epoch), keyed
 *   {producer}/{epoch}/{slot:020d}.parquet
 * Every slot is written with If-None-Match: * (create-only). A slot holds:
 *  - Data: one batch, a Parquet object whose S3 user metadata (and footer)
 *    carry {epoch, content hash, rows, ...}. Creating it is the commit: the
 *    single PUT makes data + commit one step.
 *  - Tomb: a zero-byte object the CONSUMER creates, create-only, in the first
 *    free slot of an epoch it has decided is over. It closes the epoch: if the
 *    writer (or a late request of it) gets there first, the consumer's PUT
 *    gets 412 and it ingests the data instead; if the tombstone wins, every
 *    later request for that slot gets 412, and a writer that finds a
 *    tombstone in its slot halts (it has been fenced).
 *
 * The writer appends one batch at a time per log (a "lane"; a real exporter
 * runs one lane per queue consumer). It never abandons a slot: on a timeout
 * or a 412 it HEADs the slot and acts on what is there (its own batch: done;
 * free: resend the identical request; another batch: that one is committed,
 * append after it; a tombstone: halt). So a live writer leaves no gaps, and
 * the only free slot below a later occupied one can be the head of a dead
 * epoch, which the consumer resolves by racing a tombstone into it.
 *
 * Design choices are `const` flags; each configuration is an instance module
 * at the end, as in edgePublish.qnt and s3Native.qnt.
 */
module s3Inline {
  // ---- system model -----------------------------------------------------------
  const PAYLOADS: Set[int]     // OTLP requests (content hashes) in the persistent queue
  const MAX_EPOCH: int         // writer incarnations explored
  const MAX_SLOT: int          // slots 0 .. MAX_SLOT-1 per epoch log
  // environment
  const AMBIGUOUS: bool        // requests time out at the client, land late, or are lost
  const ZOMBIES: bool          // a superseded writer can keep running
  const CONSUMER_CRASH: bool   // the consumer can crash between any two of its steps
  // design under test
  const COND_PUT: bool         // slots are written with If-None-Match: * (false: plain PUT)
  const ATOMIC_COND: bool      // the store decides the condition when it applies the write
                               // (false: at the gateway when the request arrives, then writes
                               // unconditionally: SeaweedFS 4.47's conditional multipart completion)
  const RESOLVE_BY_HEAD: bool  // on a timeout or 412 the writer reads the slot (false: it treats
                               // the attempt as failed and retries under a new key, which is what
                               // stock awss3exporter + exporterhelper retry do)
  const HALT_ON_TOMB: bool     // a writer that finds a tombstone in its slot halts
                               // (false: it skips the tombstone and appends after it)
  const TOMB_GAPS: bool        // the consumer closes a free slot of a dead epoch with a create-only
                               // tombstone (false: after a while it treats the free slot as lost
                               // and moves past it)
  const CHECK_CENTRAL: bool    // before inserting, check central for the content key

  type Kind = Free | Data | Tomb
  type Entry = { kind: Kind, epoch: int, payload: int }
  type WPhase = WIdle | WReady | WWaiting | WUnresolved | WHalted
  type Writer = { alive: bool, phase: WPhase, nextSlot: int, entry: Entry }
  // sawFree: whether the slot was free when the request reached the store
  // (what a non-atomic conditional check looks at).
  type Req = { epoch: int, slot: int, entry: Entry, sawFree: bool }
  type Resp = { epoch: int, slot: int, entry: Entry, ok: bool }
  type CPhase = CIdle | CChecked | CInserted | CTombWait | CTombUnresolved
  type Consumer = { phase: CPhase, epoch: int, slot: int, sawPresent: bool }

  // ---- state ------------------------------------------------------------------
  // edge
  var queue: Set[int]                      // unacknowledged payloads (persistent queue)
  var acked: Set[int]                      // payloads the queue was told are exported
  var lease: int                           // incarnations started so far (epochs 1..lease)
  var writers: int -> Writer
  // S3
  var log: int -> (int -> Entry)           // epoch -> slot -> object
  var inflight: Set[Req]
  var responses: Set[Resp]
  var everLog: Set[{ epoch: int, slot: int, entry: Entry }]  // every object any slot ever held
  // consumer (its checkpoint and closed-epoch set are one CAS'd object in practice)
  var ckpt: int -> int                     // epoch -> next slot to ingest
  var closed: int -> bool                  // epoch -> closed by a tombstone at ckpt
  var consumer: Consumer
  var central: int -> int                  // central inserts per payload
  var events: Set[str]

  // ---- pure logic ---------------------------------------------------------------
  pure val FREE: Entry = { kind: Free, epoch: 0, payload: 0 }
  pure def dataE(e: int, p: int): Entry = { kind: Data, epoch: e, payload: p }
  pure def tombE(e: int): Entry = { kind: Tomb, epoch: e, payload: 0 }
  pure val C0: Consumer = { phase: CIdle, epoch: 0, slot: 0, sawPresent: false }

  def epochs: Set[int] = 1.to(MAX_EPOCH)
  def slots: Set[int] = 0.to(MAX_SLOT - 1)
  def at(e: int, s: int): Entry = log.get(e).get(s)
  def putSlot(e: int, s: int, x: Entry): int -> (int -> Entry) = log.set(e, log.get(e).set(s, x))
  def committedAnywhere(p: int): bool =
    epochs.exists(e => slots.exists(s => at(e, s) == dataE(e, p)))
  def slotsOf(e: int, p: int): Set[int] = slots.filter(s => at(e, s).kind == Data and at(e, s).payload == p)

  // The writer learns that its slot holds x (from a 200, or a HEAD after a
  // 412 or a timeout) and decides what to do next.
  def observe(w: Writer, x: Entry): Writer =
    if (x == w.entry) { ...w, phase: WIdle, nextSlot: w.nextSlot + 1, entry: FREE }  // ours: committed
    else if (x.kind == Free) { ...w, phase: WReady }                                 // not landed (yet): resend
    else if (x.kind == Tomb)
      (if (HALT_ON_TOMB) { ...w, phase: WHalted } else { ...w, phase: WReady, nextSlot: w.nextSlot + 1 })
    else { ...w, phase: WReady, nextSlot: w.nextSlot + 1 }                           // another batch: append after it

  // Payloads the queue may acknowledge after that observation: any batch
  // found committed (ours, or an earlier attempt of another queued payload).
  pure def learned(x: Entry): Set[int] = if (x.kind == Data) Set(x.payload) else Set()

  // ---- helpers --------------------------------------------------------------------
  action keepConsumer = all { ckpt' = ckpt, closed' = closed, consumer' = consumer, central' = central }
  action keepEdge = all { queue' = queue, acked' = acked, lease' = lease, writers' = writers }

  // ---- writer ---------------------------------------------------------------------
  // A new incarnation takes a fresh epoch and starts its own log at slot 0.
  // Nothing has to be read first: no other writer uses its keys. The one it
  // replaces dies, or with ZOMBIES may keep running.
  action newIncarnation(zombie: bool): bool = all {
    lease < MAX_EPOCH, zombie implies ZOMBIES,
    val ne = lease + 1
    all {
      lease' = ne,
      writers' = writers.keys().mapBy(k =>
        if (k == ne) { alive: true, phase: WIdle, nextSlot: 0, entry: FREE }
        else if (zombie) writers.get(k)
        else { ...writers.get(k), alive: false }),
      events' = events.union(Set(if (zombie) "zombie" else "restart")),
    },
    queue' = queue, acked' = acked,
    log' = log, inflight' = inflight, responses' = responses, everLog' = everLog, keepConsumer,
  }

  // The queue hands payload p to incarnation e (after a crash, the next
  // incarnation gets every unacknowledged payload again). The batch is
  // encoded, and its slot is the next one in e's log.
  action startPush(e: int, p: int): bool = {
    val w = writers.get(e)
    all {
      w.alive, w.phase == WIdle, queue.contains(p), w.nextSlot < MAX_SLOT,
      writers' = writers.set(e, { ...w, phase: WReady, entry: dataE(e, p) }),
      queue' = queue, acked' = acked, lease' = lease, events' = events,
      log' = log, inflight' = inflight, responses' = responses, everLog' = everLog, keepConsumer,
    }
  }

  // PUT {epoch}/{slot}.parquet, If-None-Match: * (COND_PUT) or plain.
  action send(e: int): bool = {
    val w = writers.get(e)
    all {
      w.alive, w.phase == WReady, w.nextSlot < MAX_SLOT,
      inflight' = inflight.union(Set({ epoch: e, slot: w.nextSlot, entry: w.entry,
                                        sawFree: at(e, w.nextSlot).kind == Free })),
      writers' = writers.set(e, { ...w, phase: WWaiting }),
      queue' = queue, acked' = acked, lease' = lease, events' = events,
      log' = log, responses' = responses, everLog' = everLog, keepConsumer,
    }
  }

  // The response arrives: 200 (ours) or 412 (something is there).
  action receive(e: int, r: Resp): bool = {
    val w = writers.get(e)
    val x = if (r.ok) w.entry else at(e, r.slot)
    all {
      w.alive, w.phase == WWaiting, responses.contains(r),
      r.epoch == e, r.slot == w.nextSlot, r.entry == w.entry,
      responses' = responses.exclude(Set(r)),
      if (r.ok or RESOLVE_BY_HEAD) all {
        writers' = writers.set(e, observe(w, x)),
        queue' = queue.exclude(learned(x)),
        acked' = acked.union(learned(x)),
        events' = events.union(
          if (not(r.ok) and x.kind == Tomb) Set("halted")
          else if (not(r.ok) and x.kind == Data and x != w.entry) Set("learnedOther")
          else if (not(r.ok) and x == w.entry) Set("resolvedOwn412")
          else Set()),
      } else all {
        // stock behaviour: an error is an error; the retry takes a new key
        writers' = writers.set(e, { ...w, phase: WReady, nextSlot: w.nextSlot + 1 }),
        queue' = queue, acked' = acked, events' = events.union(Set("retryNewKey")),
      },
      lease' = lease, log' = log, inflight' = inflight, everLog' = everLog, keepConsumer,
    }
  }

  // No response in time (exporter timeout, connection reset, crash of the
  // HTTP client). The request may have landed, may land later, or never will.
  action timeout(e: int): bool = {
    val w = writers.get(e)
    all {
      AMBIGUOUS, w.alive, w.phase == WWaiting,
      writers' = writers.set(e,
        if (RESOLVE_BY_HEAD) { ...w, phase: WUnresolved }
        else { ...w, phase: WReady, nextSlot: w.nextSlot + 1 }),
      events' = events.union(Set(if (RESOLVE_BY_HEAD) "timeout" else "retryNewKey")),
      queue' = queue, acked' = acked, lease' = lease,
      log' = log, inflight' = inflight, responses' = responses, everLog' = everLog, keepConsumer,
    }
  }

  // HEAD the slot and act on what is there.
  action resolve(e: int): bool = {
    val w = writers.get(e)
    val x = at(e, w.nextSlot)
    all {
      w.alive, w.phase == WUnresolved,
      writers' = writers.set(e, observe(w, x)),
      queue' = queue.exclude(learned(x)),
      acked' = acked.union(learned(x)),
      events' = events.union(
        if (x == w.entry) Set("resolvedOwn")
        else if (x.kind == Free) Set("resend")
        else if (x.kind == Tomb) Set("halted")
        else Set("learnedOther")),
      lease' = lease, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog, keepConsumer,
    }
  }

  // The exporter returned an error for an unresolved slot (its deadline
  // passed), and the queue's next call brings another request (or the same
  // one) to this lane: it goes to the same slot, while the earlier request
  // may still be in flight. Whichever lands first wins; the other gets 412
  // and is resolved by HEAD like any other.
  action switchPayload(e: int, p: int): bool = {
    val w = writers.get(e)
    all {
      RESOLVE_BY_HEAD, w.alive, w.phase == WUnresolved, queue.contains(p),
      writers' = writers.set(e, { ...w, phase: WReady, entry: dataE(e, p) }),
      events' = events.union(Set(if (dataE(e, p) != w.entry) "switched" else "retriedSameSlot")),
      queue' = queue, acked' = acked, lease' = lease,
      log' = log, inflight' = inflight, responses' = responses, everLog' = everLog, keepConsumer,
    }
  }

  // ---- S3 ---------------------------------------------------------------------------
  // S3 applies a PUT. With ATOMIC_COND the condition is decided here; without,
  // it was decided when the request arrived (sawFree).
  action apply(q: Req): bool = {
    val cur = at(q.epoch, q.slot)
    val wins = if (not(COND_PUT)) true else if (ATOMIC_COND) cur.kind == Free else q.sawFree
    all {
      inflight.contains(q),
      inflight' = inflight.exclude(Set(q)),
      log' = if (wins) putSlot(q.epoch, q.slot, q.entry) else log,
      everLog' = if (wins) everLog.union(Set({ epoch: q.epoch, slot: q.slot, entry: q.entry })) else everLog,
      responses' = responses.union(Set({ epoch: q.epoch, slot: q.slot, entry: q.entry, ok: wins })),
      events' = events.union(
        if (wins and cur.kind != Free and cur != q.entry) Set("overwrote")
        else if (not(wins) and q.entry.kind == Tomb) Set("tombLost")
        else Set()),
      keepEdge, keepConsumer,
    }
  }

  action lose(q: Req): bool = all {
    AMBIGUOUS, inflight.contains(q),
    inflight' = inflight.exclude(Set(q)),
    log' = log, responses' = responses, everLog' = everLog, events' = events, keepEdge, keepConsumer,
  }

  // ---- consumer -----------------------------------------------------------------------
  // It follows every epoch's log in slot order: LIST {epoch}/ StartAfter the
  // checkpoint, then per object HEAD (content hash, rows) and INSERT ... FROM s3().
  action cCheck(e: int): bool = {
    val x = at(e, ckpt.get(e))
    all {
      consumer.phase == CIdle, e <= lease, not(closed.get(e)), ckpt.get(e) < MAX_SLOT, x.kind == Data,
      consumer' = { phase: CChecked, epoch: e, slot: ckpt.get(e),
                    sawPresent: CHECK_CENTRAL and central.get(x.payload) > 0 },
      ckpt' = ckpt, closed' = closed, central' = central, events' = events,
      keepEdge, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
    }
  }

  // INSERT INTO central ... FROM s3('{epoch}/{slot}.parquet'), content key H.
  action cInsert: bool = {
    val x = at(consumer.epoch, consumer.slot)
    all {
      consumer.phase == CChecked,
      central' = if (consumer.sawPresent) central else central.setBy(x.payload, c => c + 1),
      consumer' = { ...consumer, phase: CInserted },
      events' = events.union(Set(if (consumer.sawPresent) "dedupHit" else "inserted")),
      ckpt' = ckpt, closed' = closed,
      keepEdge, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
    }
  }

  action cAdvance: bool = all {
    consumer.phase == CInserted,
    ckpt' = ckpt.set(consumer.epoch, consumer.slot + 1),
    consumer' = C0,
    closed' = closed, central' = central, events' = events,
    keepEdge, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
  }

  // A tombstone is at the checkpoint (ours, possibly one that landed late
  // after a crash): the epoch is closed there.
  action cSeeTomb(e: int): bool = all {
    consumer.phase == CIdle, not(closed.get(e)), ckpt.get(e) < MAX_SLOT, at(e, ckpt.get(e)).kind == Tomb,
    closed' = closed.set(e, true),
    events' = events.union(Set("closed")),
    ckpt' = ckpt, consumer' = consumer, central' = central,
    keepEdge, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
  }

  // The epoch is superseded (a newer one exists) and its head slot is free.
  // Design: race a create-only tombstone into it. When it does this is a
  // liveness choice (quiet for T); safety does not depend on T.
  action cTomb(e: int): bool = {
    val s = ckpt.get(e)
    all {
      TOMB_GAPS, consumer.phase == CIdle, e < lease, not(closed.get(e)), s < MAX_SLOT, at(e, s).kind == Free,
      inflight' = inflight.union(Set({ epoch: e, slot: s, entry: tombE(e), sawFree: true })),
      consumer' = { phase: CTombWait, epoch: e, slot: s, sawPresent: false },
      ckpt' = ckpt, closed' = closed, central' = central, events' = events,
      keepEdge, log' = log, responses' = responses, everLog' = everLog,
    }
  }

  // Mutation: a free slot of a superseded epoch is taken as lost, and skipped.
  action cSkipGap(e: int): bool = {
    val s = ckpt.get(e)
    all {
      not(TOMB_GAPS), consumer.phase == CIdle, e < lease, not(closed.get(e)), s < MAX_SLOT, at(e, s).kind == Free,
      ckpt' = ckpt.set(e, s + 1),
      events' = events.union(Set("skippedGap")),
      consumer' = consumer, closed' = closed, central' = central,
      keepEdge, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
    }
  }

  // The tombstone's response: 200, the epoch is closed at s; 412, read the slot.
  action cTombReceive(r: Resp): bool = {
    val x = if (r.ok) r.entry else at(r.epoch, r.slot)
    all {
      consumer.phase == CTombWait, responses.contains(r), r.entry.kind == Tomb,
      r.epoch == consumer.epoch, r.slot == consumer.slot,
      responses' = responses.exclude(Set(r)),
      closed' = if (x.kind == Tomb) closed.set(r.epoch, true) else closed,
      consumer' = C0,
      events' = events.union(Set(if (x.kind == Tomb) "closed" else "tombLostToData")),
      ckpt' = ckpt, central' = central,
      keepEdge, log' = log, inflight' = inflight, everLog' = everLog,
    }
  }

  action cTombTimeout: bool = all {
    AMBIGUOUS, consumer.phase == CTombWait,
    consumer' = { ...consumer, phase: CTombUnresolved },
    ckpt' = ckpt, closed' = closed, central' = central, events' = events,
    keepEdge, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
  }

  action cTombResolve: bool = {
    val x = at(consumer.epoch, consumer.slot)
    all {
      consumer.phase == CTombUnresolved,
      closed' = if (x.kind == Tomb) closed.set(consumer.epoch, true) else closed,
      inflight' = if (x.kind == Free)
                    inflight.union(Set({ epoch: consumer.epoch, slot: consumer.slot, entry: tombE(consumer.epoch), sawFree: true }))
                  else inflight,
      consumer' = if (x.kind == Free) { ...consumer, phase: CTombWait } else C0,
      events' = events.union(Set(if (x.kind == Tomb) "closed" else if (x.kind == Data) "tombLostToData" else "tombResend")),
      ckpt' = ckpt, central' = central,
      keepEdge, log' = log, responses' = responses, everLog' = everLog,
    }
  }

  action cCrash: bool = all {
    CONSUMER_CRASH, consumer.phase != CIdle,
    consumer' = C0,
    events' = events.union(Set("consumerCrash")),
    ckpt' = ckpt, closed' = closed, central' = central,
    keepEdge, log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
  }

  // ---- state machine -----------------------------------------------------------------------
  action init = all {
    queue' = PAYLOADS,
    acked' = Set(),
    lease' = 1,
    writers' = 1.to(MAX_EPOCH).mapBy(e => { alive: e == 1, phase: WIdle, nextSlot: 0, entry: FREE }),
    log' = 1.to(MAX_EPOCH).mapBy(_ => 0.to(MAX_SLOT - 1).mapBy(_ => FREE)),
    inflight' = Set(), responses' = Set(), everLog' = Set(),
    ckpt' = 1.to(MAX_EPOCH).mapBy(_ => 0),
    closed' = 1.to(MAX_EPOCH).mapBy(_ => false),
    consumer' = C0,
    central' = PAYLOADS.mapBy(_ => 0),
    events' = Set(),
  }

  action step = {
    nondet e = 1.to(MAX_EPOCH).oneOf()
    nondet p = PAYLOADS.oneOf()
    nondet z = Set(true, false).oneOf()
    any {
      startPush(e, p), send(e), timeout(e), resolve(e), switchPayload(e, p),
      all { responses != Set(), nondet r = responses.oneOf() any { receive(e, r), cTombReceive(r) } },
      newIncarnation(z),
      all { inflight != Set(), nondet q = inflight.oneOf() any { apply(q), lose(q) } },
      cCheck(e), cInsert, cAdvance, cSeeTomb(e), cTomb(e), cSkipGap(e), cTombTimeout, cTombResolve, cCrash,
    }
  }

  // ---- properties ----------------------------------------------------------------------------
  // End to end: each OTLP request's rows land centrally at most once.
  val payloadIngestedAtMostOnce: bool = PAYLOADS.forall(p => central.get(p) <= 1)

  // Within one epoch's log a request is committed once, across timeouts,
  // late requests and 412s (edgePublish's F1, per incarnation).
  val epochNoDuplicatePayload: bool = epochs.forall(e => PAYLOADS.forall(p => slotsOf(e, p).size() <= 1))

  // Nothing uncommitted reaches central.
  val onlyCommittedIngested: bool =
    PAYLOADS.forall(p => central.get(p) > 0 implies everLog.exists(x => x.entry.kind == Data and x.entry.payload == p))

  // Create-only: an object, once in a slot, stays (no GC modelled). This is
  // commitImpliesData and "a tombstone is final" in one.
  val noCommitLost: bool = everLog.forall(x => at(x.epoch, x.slot) == x.entry)

  // The queue is only told "exported" for requests that are committed, and
  // nothing leaves the queue uncommitted (success => commit; no data loss).
  val ackedImpliesCommitted: bool = acked.forall(p => committedAnywhere(p))
  val noPayloadLost: bool = PAYLOADS.forall(p => queue.contains(p) or committedAnywhere(p))

  // The consumer moves past a slot only when it holds data it has ingested:
  // a free slot (a gap) is never taken for a loss.
  val gapNeverTakenForLoss: bool =
    epochs.forall(e => slots.forall(s => s < ckpt.get(e) implies at(e, s).kind == Data))

  // The consumer never skips a committed object: every data slot is ingested,
  // or still ahead of an open epoch's checkpoint. With the tombstone this is
  // also sealMatchesManifests: a closed epoch holds exactly the data below its
  // tombstone.
  val consumerNeverSkipsCommitted: bool =
    epochs.forall(e => slots.forall(s =>
      at(e, s).kind == Data implies (central.get(at(e, s).payload) >= 1 or (s >= ckpt.get(e) and not(closed.get(e))))))

  // A closed epoch takes no more data: nothing lands at or after its tombstone.
  val noCommitAfterClose: bool =
    epochs.forall(e => closed.get(e) implies slots.forall(s => s >= ckpt.get(e) implies at(e, s).kind != Data))

  val safety: bool = and {
    payloadIngestedAtMostOnce, epochNoDuplicatePayload, onlyCommittedIngested, noCommitLost,
    ackedImpliesCommitted, noPayloadLost, gapNeverTakenForLoss, consumerNeverSkipsCommitted, noCommitAfterClose,
  }

  // ---- witnesses (reachability; expect > 0 traces) ----------------------------------------------
  val wCommitted: bool = epochs.exists(e => slots.exists(s => at(e, s).kind == Data))
  val wResolvedOwn: bool = events.contains("resolvedOwn") or events.contains("resolvedOwn412")
  val wResend: bool = events.contains("resend")
  val wSwitched: bool = events.contains("switched")
  val wLearnedOther: bool = events.contains("learnedOther")
  val wHalted: bool = events.contains("halted")
  val wClosed: bool = events.contains("closed")
  val wTombLostToData: bool = events.contains("tombLostToData")
  val wIngested: bool = events.contains("inserted")
  val wDedupHit: bool = events.contains("dedupHit")
  val wCrossEpochCopy: bool = PAYLOADS.exists(p => epochs.filter(e => slotsOf(e, p).size() > 0).size() > 1)
  val wAllIngested: bool = queue == Set() and PAYLOADS.forall(p => central.get(p) >= 1)
  // every superseded epoch closed, everything ingested
  val wAllClosed: bool = wAllIngested and lease > 1 and 1.to(lease - 1).forall(e => closed.get(e))
}

// ---- instances ----------------------------------------------------------------------------------
// Hostile environment everywhere: ambiguous and late S3 writes, zombie
// writers, consumer crashes.

module s3InlineDesign {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).*
}

// Mutation: slots written with a plain PUT (stock awss3exporter never sends If-None-Match).
module plainPut {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = false, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).*
}

// Mutation: the store checks the condition on arrival and writes later
// (SeaweedFS 4.47 conditional CompleteMultipartUpload, i.e. objects over the
// transfer manager's 16 MiB multipart threshold).
module nonAtomicCond {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = false, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).*
}

// Mutation: stock awss3exporter under exporterhelper retry. A failed or
// timed-out attempt is retried under a new key.
module retryNewKey {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = false, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).*
}

// Mutation: a writer that finds a tombstone in its slot appends after it.
module noHalt {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = false, TOMB_GAPS = true,
    CHECK_CENTRAL = true).*
}

// Mutation: the consumer takes a free slot of a superseded epoch as lost.
module skipGaps {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = false,
    CHECK_CENTRAL = true).*
}

// Mutation: no check of central before insert (a crash between insert and
// checkpoint, or a copy of the request in another epoch, is ingested twice).
module noCheckCentral {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = false).*
}

// The design in an ideal environment (no ambiguity, no zombies, no consumer crashes).
module idealEnv {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = false, ZOMBIES = false, CONSUMER_CRASH = false,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).*
}
