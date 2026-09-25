// -*- mode: Bluespec; -*-
/**
 * s3Native: an S3-native variant of the edge-publishing protocol, in which
 * S3's conditional operations (If-None-Match: * and If-Match: <etag>) carry
 * the commit, the fencing, the consumer's checkpoint and the GC/reader
 * coordination. See S3NATIVE.md for the design and edgePublish.qnt for the
 * protocol as built.
 *
 * S3 is the shared state. Actors:
 *  - writer incarnations, one per epoch. A new incarnation takes the next
 *    epoch from a CAS'd lease counter; the one it replaces either dies or,
 *    with ZOMBIES, keeps running (a paused or partitioned pod) until it learns
 *    it has been fenced;
 *  - the collector's persistent queue of payloads (content hashes);
 *  - S3 itself: each append to the per-producer commit log is a request that
 *    sits in flight until S3 applies it (atomically: If-None-Match is decided
 *    at apply time) or loses it; the writer may time out first, so it cannot
 *    tell "applied", "will apply later" and "never" apart (AMBIGUOUS);
 *  - consumer workers that hold a CAS'd lease on the producer's log, insert
 *    each committed batch into the central table with a content token, and
 *    advance a CAS'd checkpoint; a worker can crash, or keep running after
 *    its lease is taken over;
 *  - readers that lease a committed batch through the CAS'd GC-state object;
 *  - GC, which advances a horizon in the same object and deletes data below it.
 *
 * The log (`log/{slot:020}.json`, create-only slots) is the single commit
 * record: commit entries name a batch's content hash, seal entries close a
 * generation, and fence entries close an epoch. Data objects live in a
 * namespace per (epoch, generation) and are keyed by content hash inside it.
 *
 * Design choices are `const` flags so the design and its mutations are
 * instances of one spec (see the instance modules at the end).
 */
module s3Native {
  import basicSpells.* from "./basicSpells"

  // ---- system model -----------------------------------------------------------
  const PAYLOADS: Set[int]     // OTLP requests (their content hashes)
  const WORKERS: Set[int]      // consumer workers
  const READERS: Set[int]
  const MAX_EPOCH: int         // writer incarnations explored
  const MAX_SLOT: int          // log slots 0 .. MAX_SLOT-1
  const MAX_GEN: int           // generations 1 .. MAX_GEN (the clock's range)
  const MAX_CEPOCH: int        // consumer lease takeovers explored
  const DEDUP_WINDOW: int      // central insert-dedup tokens remembered (count window)
  // environment
  const AMBIGUOUS: bool        // requests time out at the client, land late, or are lost
  const ZOMBIES: bool          // a superseded writer can keep running
  const CLOCK_SKEW: bool       // the wall clock can step back (equivalently: a stale read)
  const OTHER_TRAFFIC: bool    // unrelated inserts push tokens out of the dedup window
  const ZOMBIE_INSERT_BOUNDED: bool // a worker inserts only while its lease is valid
                               // (lease-time bound: stop inserting before expiry)
  // design under test
  const COND_LOG: bool         // log slots are written with If-None-Match: *
  const SHARED_LOG: bool       // every epoch appends to one log and reads the slots it collides
                               // with; false: each epoch has its own log (keys under an epoch
                               // prefix, as publish.go's manifests), and only the lease excludes
  const FENCE_ENTRY: bool      // a new epoch appends a fence entry before anything else
  const NS_PER_EPOCH: bool     // the namespace (table, endpoint) of a generation includes the epoch
  const GEN_FROM_LOG: bool     // the generation only moves forward, by a seal entry;
                               // false: the push reads it from the clock (publish.go)
  const SEAL_RETRY: bool       // a seal append is retried until resolved, like any append
  const CKPT_CAS: bool         // the consumer checkpoint is written If-Match its read ETag
  const CHECK_CENTRAL: bool    // before inserting, check central for the content key
  const GC_WAITS_FOR_LEASES: bool // the GC horizon stays below every reader lease
  const GC_CAS: bool           // GC writes the GC-state object If-Match its read ETag

  type Kind = Free | Commit | Seal | Fence
  type Entry = { kind: Kind, epoch: int, payload: int, gen: int }
  type WPhase = WIdle | WData | WReady | WWaiting | WUnresolved | WHalted
  type Writer = {
    alive: bool,
    phase: WPhase,
    nextSlot: int,      // where the next append goes
    known: Set[int],    // content hashes this incarnation knows are committed
    gen: int,           // current generation
    opened: Set[int],   // generations this incarnation has written into
    cur: int,           // payload being pushed (0: none)
    curGen: int,        // its generation; for a seal, the generation after it
    entry: Entry,       // the entry being appended
  }
  type Req = { slot: int, entry: Entry }
  type Resp = { slot: int, entry: Entry, ok: bool }
  type NsKey = { epoch: int, gen: int }
  type DataKey = { ns: NsKey, payload: int }
  type CPhase = CIdle | CBegun | CChecked | CInserted
  type Worker = { epoch: int, phase: CPhase, slot: int, ver: int, sawPresent: bool }
  type Lease = { reader: int, slot: int }
  type Gc = { horizon: int, ver: int, leases: Set[Lease],
              planned: bool, planH: int, planVer: int, planLeases: Set[Lease] }

  // ---- state ------------------------------------------------------------------
  // edge
  var queue: Set[int]                    // unacknowledged payloads (persistent)
  var lease: int                         // writer lease object: the latest epoch (CAS'd counter)
  var writers: int -> Writer             // incarnations by epoch
  var tables: int -> int                 // chDB's local tables that survive a restart: gen -> epoch
                                         // whose namespace the table writes to (0: none)
  var clock: int                         // wall clock, in generations
  // S3
  var log: int -> Entry
  var inflight: Set[Req]
  var responses: Set[Resp]
  var data: Set[DataKey]
  var dataWriters: Set[{ ns: NsKey, epoch: int }]  // who wrote into which namespace
  var everLog: Set[{ slot: int, entry: Entry }]    // every entry any slot ever held
  // consumer
  var clease: { owner: int, epoch: int }
  var ckpt: { next: int, ver: int }
  var workers: int -> Worker
  var window: List[int]
  var central: int -> int                // central inserts per payload
  // GC and readers
  var gc: Gc
  var readers: int -> int                // leased slot, -1: none
  var badRead: bool                      // someone read a deleted data object
  var events: Set[str]

  // ---- pure logic ---------------------------------------------------------------
  pure val FREE: Entry = { kind: Free, epoch: 0, payload: 0, gen: 0 }
  pure val OTHER_TOKEN = 0

  pure def commitE(e: int, p: int, g: int): Entry = { kind: Commit, epoch: e, payload: p, gen: g }
  pure def sealE(e: int, g: int): Entry = { kind: Seal, epoch: e, payload: 0, gen: g }
  pure def fenceE(e: int): Entry = { kind: Fence, epoch: e, payload: 0, gen: 0 }

  // The data object a commit entry names: its epoch's namespace for its generation.
  pure def dataOf(x: Entry): DataKey = { ns: { epoch: x.epoch, gen: x.gen }, payload: x.payload }

  pure def committed(lg: int -> Entry): Set[int] =
    lg.keys().filter(s => lg.get(s).kind == Commit).map(s => lg.get(s).payload)

  pure def firstFree(lg: int -> Entry, maxSlot: int): int =
    lg.keys().filter(s => lg.get(s).kind == Free).fold(maxSlot, (a, s) => if (s < a) s else a)

  // Does log entry x close generation (epoch, gen)? Its seal does, and so does
  // any fence of a later epoch: a fence seals every generation of the epochs before it.
  pure def closes(x: Entry, epoch: int, gen: int): bool =
    (x.kind == Seal and x.epoch == epoch and x.gen == gen) or (x.kind == Fence and x.epoch > epoch)

  pure def closedGen(lg: int -> Entry, epoch: int, gen: int): bool =
    lg.keys().exists(s => closes(lg.get(s), epoch, gen))

  // A namespace is closed once every epoch that wrote into it has that
  // generation closed. Data in a closed namespace that no commit names can
  // never be committed later: it is garbage.
  pure def closedNs(lg: int -> Entry, dw: Set[{ ns: NsKey, epoch: int }], ns: NsKey): bool =
    dw.filter(d => d.ns == ns).forall(d => closedGen(lg, d.epoch, ns.gen))

  pure def referenced(lg: int -> Entry, k: DataKey): bool =
    lg.keys().exists(s => lg.get(s).kind == Commit and dataOf(lg.get(s)) == k)

  pure def remember(w: List[int], t: int, cap: int): List[int] = {
    val l = w.append(t)
    if (l.length() > cap) l.slice(l.length() - cap, l.length()) else l
  }
  pure def inWindow(w: List[int], t: int): bool = w.indices().exists(i => w[i] == t)

  // An append is done: its entry is in its slot.
  pure def finish(w: Writer, lg: int -> Entry): Writer = {
    val e = w.entry
    { ...w, phase: WIdle, nextSlot: w.nextSlot + 1, cur: 0,
      // a fence is followed by a replay of the log below it
      known: if (e.kind == Commit) w.known.union(Set(e.payload))
             else if (e.kind == Fence) committed(lg)
             else w.known,
      gen: if (e.kind == Seal) w.curGen else w.gen,
      opened: if (e.kind == Commit) w.opened.union(Set(e.gen)) else w.opened }
  }

  // The writer learns that slot nextSlot holds x (from a response, a 412 and
  // a read, or a read after a timeout) and decides what to do next.
  pure def observe(w: Writer, me: int, x: Entry, lg: int -> Entry, shared: bool): Writer =
    if (x == w.entry) finish(w, lg)
    else if (x.kind == Free) { ...w, phase: WReady }            // not landed (yet): resend
    else if (not(shared) and x.epoch != me) { ...w, phase: WReady, nextSlot: w.nextSlot + 1 } // not in my log
    else if (x.epoch > me) { ...w, phase: WHalted }             // fenced
    else if (w.entry.kind == Commit and x.kind == Commit and x.payload == w.entry.payload)
      { ...w, phase: WIdle, nextSlot: w.nextSlot + 1, cur: 0, known: w.known.union(Set(x.payload)) }
    else { ...w, phase: WReady, nextSlot: w.nextSlot + 1,
           known: if (x.kind == Commit) w.known.union(Set(x.payload)) else w.known }

  // Payloads the queue may acknowledge after that observation.
  pure def acks(w: Writer, me: int, x: Entry, shared: bool): Set[int] =
    if (w.entry.kind == Commit and x.kind == Commit and x.payload == w.entry.payload
        and (if (shared) x.epoch <= me else x.epoch == me))
      Set(w.entry.payload)
    else Set()

  // ---- helpers --------------------------------------------------------------------
  action keepEdge = all {
    queue' = queue, lease' = lease, writers' = writers, tables' = tables, clock' = clock,
  }
  action keepS3 = all {
    log' = log, inflight' = inflight, responses' = responses, data' = data,
    dataWriters' = dataWriters, everLog' = everLog,
  }
  action keepConsumer = all {
    clease' = clease, ckpt' = ckpt, workers' = workers, window' = window, central' = central,
  }
  action keepGc = all { gc' = gc, readers' = readers, badRead' = badRead }

  // ---- writer ---------------------------------------------------------------------
  def fresh(e: int): Writer = {
    val base = { alive: true, phase: WIdle, nextSlot: firstFree(log, MAX_SLOT), known: Set(),
                 gen: clock, opened: Set(), cur: 0, curGen: clock, entry: FREE }
    if (FENCE_ENTRY) { ...base, phase: WReady, entry: fenceE(e) }
    else { ...base, known: committed(log) }                  // replays, but nothing stops a zombie
  }

  // A new incarnation takes the next epoch (a CAS on the lease counter). The
  // one it replaces dies, or with ZOMBIES may keep running.
  action newIncarnation(zombie: bool): bool = all {
    lease < MAX_EPOCH, zombie implies ZOMBIES,
    val ne = lease + 1
    all {
      lease' = ne,
      writers' = writers.keys().mapBy(k =>
        if (k == ne) fresh(ne)
        else if (zombie) writers.get(k)
        else { ...writers.get(k), alive: false }),
      events' = events.union(Set(if (zombie) "zombie" else "restart")),
    },
    queue' = queue, tables' = tables, clock' = clock, keepS3, keepConsumer, keepGc,
  }

  action tick: bool = all {
    clock < MAX_GEN,
    clock' = clock + 1,
    queue' = queue, lease' = lease, writers' = writers, tables' = tables, events' = events,
    keepS3, keepConsumer, keepGc,
  }

  action tickBack: bool = all {
    CLOCK_SKEW, clock > 1,
    clock' = clock - 1,
    queue' = queue, lease' = lease, writers' = writers, tables' = tables, events' = events.union(Set("clockBack")),
    keepS3, keepConsumer, keepGc,
  }

  // The queue hands payload p to incarnation e. A payload it knows is
  // committed is acknowledged without publishing again.
  action startPush(e: int, p: int): bool = {
    val w = writers.get(e)
    all {
      w.alive, w.phase == WIdle, queue.contains(p),
      if (w.known.contains(p)) all {
        queue' = queue.exclude(Set(p)),
        writers' = writers,
        events' = events.union(Set("skipKnown")),
      } else {
        // publish.go reads the generation from the clock at push time; the
        // S3-native design only moves it forward, by a seal (rotate below).
        val g = if (GEN_FROM_LOG) w.gen else clock
        all {
          queue' = queue,
          writers' = writers.set(e, { ...w, phase: WData, cur: p, curGen: g, gen: g }),
          events' = if (g < w.gen) events.union(Set("reopened")) else events,
        }
      },
      lease' = lease, tables' = tables, clock' = clock, keepS3, keepConsumer, keepGc,
    }
  }

  // Write the batch's data (Parquet object / table part) into the
  // generation's namespace. It is keyed by content inside the namespace, so a
  // retry rewrites the same object. `lands`: the object exists afterwards.
  action writeData(e: int, lands: bool, ok: bool): bool = {
    val w = writers.get(e)
    // Which namespace chDB's local table writes to. With a persistent chDB
    // path and table names without the epoch, CREATE TABLE IF NOT EXISTS
    // finds the previous incarnation's table for the same generation.
    val te = if (NS_PER_EPOCH) e else if (tables.get(w.curGen) > 0) tables.get(w.curGen) else e
    val k = { ns: { epoch: te, gen: w.curGen }, payload: w.cur }
    all {
      w.alive, w.phase == WData, ok implies lands,
      data' = if (lands) data.union(Set(k)) else data,
      dataWriters' = if (lands) dataWriters.union(Set({ ns: k.ns, epoch: e })) else dataWriters,
      tables' = if (NS_PER_EPOCH) tables else tables.set(w.curGen, te),
      writers' = writers.set(e, if (ok) { ...w, phase: WReady, entry: commitE(e, w.cur, w.curGen),
                                          opened: w.opened.union(Set(w.curGen)) }
                                else { ...w, phase: WIdle, cur: 0 }),
      queue' = queue, lease' = lease, clock' = clock, events' = events,
      log' = log, inflight' = inflight, responses' = responses, everLog' = everLog,
      keepConsumer, keepGc,
    }
  }

  // Rotation, under the push lock (the writer is idle): once the clock has
  // passed the generation, append its seal; the generation after it is gen + 1
  // (design) or the clock's (publish.go).
  action rotate(e: int): bool = {
    val w = writers.get(e)
    val target = if (GEN_FROM_LOG) w.gen + 1 else clock
    all {
      w.alive, w.phase == WIdle, clock > w.gen, target <= MAX_GEN,
      writers' = writers.set(e, { ...w, phase: WReady, entry: sealE(e, w.gen), curGen: target }),
      queue' = queue, lease' = lease, tables' = tables, clock' = clock, events' = events,
      keepS3, keepConsumer, keepGc,
    }
  }

  // Send the append: PUT log/{nextSlot} with If-None-Match: * (COND_LOG) or plainly.
  action send(e: int): bool = {
    val w = writers.get(e)
    all {
      w.alive, w.phase == WReady, w.nextSlot < MAX_SLOT,
      inflight' = inflight.union(Set({ slot: w.nextSlot, entry: w.entry })),
      writers' = writers.set(e, { ...w, phase: WWaiting }),
      queue' = queue, lease' = lease, tables' = tables, clock' = clock, events' = events,
      log' = log, responses' = responses, data' = data, dataWriters' = dataWriters, everLog' = everLog,
      keepConsumer, keepGc,
    }
  }

  // The response arrives. 200: the entry is in its slot. 412: read the slot
  // and act on what is there.
  action receive(e: int, r: Resp): bool = {
    val w = writers.get(e)
    val x = if (r.ok) w.entry else log.get(r.slot)
    all {
      w.alive, w.phase == WWaiting, responses.contains(r), r.slot == w.nextSlot, r.entry == w.entry,
      responses' = responses.exclude(Set(r)),
      writers' = writers.set(e, observe(w, e, x, log, SHARED_LOG)),
      queue' = queue.exclude(acks(w, e, x, SHARED_LOG)),
      events' = if (not(r.ok) and x.epoch > e) events.union(Set("halted")) else events,
      lease' = lease, tables' = tables, clock' = clock,
      log' = log, inflight' = inflight, data' = data, dataWriters' = dataWriters, everLog' = everLog,
      keepConsumer, keepGc,
    }
  }

  // No response in time. The request may have landed, may land later, or
  // never will. Design: resolve by reading the slot. Without SEAL_RETRY, a
  // timed-out seal is abandoned and the writer moves on (publish.go never
  // retries a failed seal).
  action timeout(e: int): bool = {
    val w = writers.get(e)
    all {
      AMBIGUOUS, w.alive, w.phase == WWaiting,
      writers' = writers.set(e,
        if (not(SEAL_RETRY) and w.entry.kind == Seal) { ...w, phase: WIdle, gen: w.curGen }
        else { ...w, phase: WUnresolved }),
      events' = events.union(Set("timeout")),
      queue' = queue, lease' = lease, tables' = tables, clock' = clock, keepS3, keepConsumer, keepGc,
    }
  }

  action resolve(e: int): bool = {
    val w = writers.get(e)
    val x = log.get(w.nextSlot)
    all {
      w.alive, w.phase == WUnresolved,
      writers' = writers.set(e, observe(w, e, x, log, SHARED_LOG)),
      queue' = queue.exclude(acks(w, e, x, SHARED_LOG)),
      events' = if (x == w.entry) events.union(Set("resolvedOwn"))
                else if (x.kind == Free) events.union(Set("resend"))
                else events,
      lease' = lease, tables' = tables, clock' = clock, keepS3, keepConsumer, keepGc,
    }
  }

  // ---- S3 ---------------------------------------------------------------------------
  // S3 applies an append. If-None-Match is decided here, atomically.
  action apply(q: Req): bool = {
    val wins = not(COND_LOG) or log.get(q.slot).kind == Free
    all {
      inflight.contains(q),
      inflight' = inflight.exclude(Set(q)),
      log' = if (wins) log.set(q.slot, q.entry) else log,
      everLog' = if (wins) everLog.union(Set({ slot: q.slot, entry: q.entry })) else everLog,
      responses' = responses.union(Set({ slot: q.slot, entry: q.entry, ok: wins })),
      events' = if (wins and log.get(q.slot).kind != Free) events.union(Set("overwrote")) else events,
      data' = data, dataWriters' = dataWriters, keepEdge, keepConsumer, keepGc,
    }
  }

  action lose(q: Req): bool = all {
    AMBIGUOUS, inflight.contains(q),
    inflight' = inflight.exclude(Set(q)),
    log' = log, responses' = responses, data' = data, dataWriters' = dataWriters, everLog' = everLog,
    events' = events, keepEdge, keepConsumer, keepGc,
  }

  // ---- consumer -----------------------------------------------------------------------
  // Take (or take over) the consumer lease on this producer's log: a CAS on
  // the lease object. The previous holder is not told.
  action takeLease(w: int): bool = all {
    clease.epoch < MAX_CEPOCH, clease.owner != w or workers.get(w).epoch == 0,
    val ne = clease.epoch + 1
    all {
      clease' = { owner: w, epoch: ne },
      workers' = workers.set(w, { epoch: ne, phase: CIdle, slot: -1, ver: 0, sawPresent: false }),
    },
    ckpt' = ckpt, window' = window, central' = central, events' = events,
    keepEdge, keepS3, keepGc,
  }

  // Read the checkpoint (and its ETag) and the slot it points at.
  action cBegin(w: int): bool = {
    val ws = workers.get(w)
    all {
      ws.epoch > 0, ws.phase == CIdle, ckpt.next < MAX_SLOT, log.get(ckpt.next).kind != Free,
      workers' = workers.set(w, { ...ws, phase: CBegun, slot: ckpt.next, ver: ckpt.ver }),
      clease' = clease, ckpt' = ckpt, window' = window, central' = central, events' = events,
      keepEdge, keepS3, keepGc,
    }
  }

  // SELECT count() FROM central WHERE content_key = H: a separate statement
  // from the insert.
  action cCheck(w: int): bool = {
    val ws = workers.get(w)
    val x = log.get(ws.slot)
    all {
      ws.phase == CBegun,
      workers' = workers.set(w, { ...ws, phase: CChecked,
        sawPresent: CHECK_CENTRAL and x.kind == Commit and central.get(x.payload) > 0 }),
      clease' = clease, ckpt' = ckpt, window' = window, central' = central, events' = events,
      keepEdge, keepS3, keepGc,
    }
  }

  // INSERT INTO central SELECT ... FROM s3(data) SETTINGS
  // insert_deduplication_token = H (one block). Nothing S3 does can fence
  // this: with ZOMBIE_INSERT_BOUNDED the worker only inserts inside its lease.
  action cInsert(w: int): bool = {
    val ws = workers.get(w)
    val x = log.get(ws.slot)
    all {
      ws.phase == CChecked,
      ZOMBIE_INSERT_BOUNDED implies ws.epoch == clease.epoch,
      if (x.kind != Commit) all {
        workers' = workers.set(w, { ...ws, phase: CInserted }),
        window' = window, central' = central, badRead' = badRead, events' = events,
      } else if (not(data.contains(dataOf(x)))) all {
        workers' = workers.set(w, { ...ws, phase: CIdle }),
        window' = window, central' = central, badRead' = true,
        events' = events.union(Set("consumerMissingData")),
      } else {
        val dup = ws.sawPresent or inWindow(window, x.payload)
        all {
          workers' = workers.set(w, { ...ws, phase: CInserted }),
          central' = if (dup) central else central.setBy(x.payload, c => c + 1),
          window' = if (dup) window else remember(window, x.payload, DEDUP_WINDOW),
          badRead' = badRead,
          events' = events.union(Set(if (dup) "dedupHit" else "inserted")),
        }
      },
      clease' = clease, ckpt' = ckpt, gc' = gc, readers' = readers, keepEdge, keepS3,
    }
  }

  // Advance the checkpoint past the slot: PUT If-Match the ETag read in cBegin.
  action cAdvance(w: int): bool = {
    val ws = workers.get(w)
    val ok = not(CKPT_CAS) or ckpt.ver == ws.ver
    all {
      ws.phase == CInserted,
      ckpt' = if (ok) { next: ws.slot + 1, ver: ckpt.ver + 1 } else ckpt,
      workers' = workers.set(w, { ...ws, phase: CIdle }),
      events' = events.union(Set(if (ok) "advanced" else "ckptCasFailed")),
      clease' = clease, window' = window, central' = central, keepEdge, keepS3, keepGc,
    }
  }

  action workerCrash(w: int): bool = all {
    workers.get(w).epoch > 0, workers.get(w).phase != CIdle,
    clease.epoch < MAX_CEPOCH,     // someone can still take over (keeps the bounded run live)
    workers' = workers.set(w, { epoch: 0, phase: CIdle, slot: -1, ver: 0, sawPresent: false }),
    clease' = clease, ckpt' = ckpt, window' = window, central' = central,
    events' = events.union(Set("workerCrash")), keepEdge, keepS3, keepGc,
  }

  // Only when it can evict something: other inserts while the window holds
  // no batch token change nothing this model observes.
  action otherInsert: bool = all {
    OTHER_TRAFFIC, window.indices().exists(i => window[i] != OTHER_TOKEN),
    window' = remember(window, OTHER_TOKEN, DEDUP_WINDOW),
    clease' = clease, ckpt' = ckpt, workers' = workers, central' = central, events' = events,
    keepEdge, keepS3, keepGc,
  }

  // ---- GC and readers --------------------------------------------------------------------
  // A reader leases a committed slot: CAS on the GC-state object, only at or
  // above the horizon.
  action readerLease(r: int, s: int): bool = all {
    readers.get(r) == -1, s >= gc.horizon, log.get(s).kind == Commit,
    gc' = { ...gc, leases: gc.leases.union(Set({ reader: r, slot: s })), ver: gc.ver + 1 },
    readers' = readers.set(r, s),
    badRead' = badRead, events' = events, keepEdge, keepS3, keepConsumer,
  }

  action readerRead(r: int): bool = {
    val s = readers.get(r)
    val x = log.get(s)
    val gone = x.kind != Commit or not(data.contains(dataOf(x)))
    all {
      s >= 0,
      badRead' = (badRead or gone),
      events' = events.union(Set(if (gone) "readerBadRead" else "readerRead")),
      gc' = gc, readers' = readers, keepEdge, keepS3, keepConsumer,
    }
  }

  action readerRelease(r: int): bool = all {
    readers.get(r) >= 0,
    gc' = { ...gc, leases: gc.leases.exclude(Set({ reader: r, slot: readers.get(r) })), ver: gc.ver + 1 },
    readers' = readers.set(r, -1),
    badRead' = badRead, events' = events, keepEdge, keepS3, keepConsumer,
  }

  // GC reads the GC state (and its ETag) and the checkpoint, and plans a horizon.
  action gcPlan: bool = {
    val minLease = gc.leases.fold(ckpt.next, (a, l) => if (l.slot < a) l.slot else a)
    val h = if (GC_WAITS_FOR_LEASES) minLease else ckpt.next
    all {
      not(gc.planned), h > gc.horizon,
      gc' = { ...gc, planned: true, planH: h, planVer: gc.ver, planLeases: gc.leases },
      readers' = readers, badRead' = badRead, events' = events, keepEdge, keepS3, keepConsumer,
    }
  }

  // ...and writes it back: If-Match the ETag it read (GC_CAS), or blindly,
  // which also writes back the lease set it read.
  action gcCommit: bool = {
    val ok = not(GC_CAS) or gc.ver == gc.planVer
    all {
      gc.planned,
      gc' = if (ok) { ...gc, planned: false, horizon: max(gc.horizon, gc.planH), ver: gc.ver + 1,
                      leases: if (GC_CAS) gc.leases else gc.planLeases }
            else { ...gc, planned: false },
      events' = events.union(Set(if (ok) "gcHorizon" else "gcCasFailed")),
      readers' = readers, badRead' = badRead, keepEdge, keepS3, keepConsumer,
    }
  }

  // Delete data below the horizon, and orphans: data in a closed namespace
  // that no commit names.
  action gcDelete: bool = {
    val below = data.filter(k =>
      log.keys().exists(s => s < gc.horizon and log.get(s).kind == Commit and dataOf(log.get(s)) == k)
      and not(log.keys().exists(s => s >= gc.horizon and log.get(s).kind == Commit and dataOf(log.get(s)) == k)))
    val orphans = data.filter(k => not(referenced(log, k)) and closedNs(log, dataWriters, k.ns))
    all {
      below.union(orphans) != Set(),
      data' = data.exclude(below.union(orphans)),
      events' = events.union(if (orphans != Set()) Set("orphanSwept", "gcDeleted") else Set("gcDeleted")),
      log' = log, inflight' = inflight, responses' = responses, dataWriters' = dataWriters, everLog' = everLog,
      keepEdge, keepConsumer, keepGc,
    }
  }

  // ---- state machine -----------------------------------------------------------------------
  action init = all {
    queue' = PAYLOADS,
    lease' = 1,
    writers' = 1.to(MAX_EPOCH).mapBy(e =>
      { alive: e == 1, phase: WIdle, nextSlot: 0, known: Set(), gen: 1, opened: Set(), cur: 0, curGen: 1, entry: FREE }),
    tables' = 1.to(MAX_GEN).mapBy(_ => 0),
    clock' = 1,
    log' = 0.to(MAX_SLOT - 1).mapBy(_ => FREE),
    inflight' = Set(), responses' = Set(), data' = Set(), dataWriters' = Set(), everLog' = Set(),
    clease' = { owner: 0, epoch: 0 },
    ckpt' = { next: 0, ver: 0 },
    workers' = WORKERS.mapBy(_ => { epoch: 0, phase: CIdle, slot: -1, ver: 0, sawPresent: false }),
    window' = List(),
    central' = PAYLOADS.mapBy(_ => 0),
    gc' = { horizon: 0, ver: 0, leases: Set(), planned: false, planH: 0, planVer: 0, planLeases: Set() },
    readers' = READERS.mapBy(_ => -1),
    badRead' = false,
    events' = Set(),
  }

  action step = {
    nondet e = 1.to(MAX_EPOCH).oneOf()
    nondet p = PAYLOADS.oneOf()
    nondet w = WORKERS.oneOf()
    nondet r = READERS.oneOf()
    nondet s = 0.to(MAX_SLOT - 1).oneOf()
    nondet lands = Set(true, false).oneOf()
    nondet ok = Set(true, false).oneOf()
    nondet z = Set(true, false).oneOf()
    any {
      startPush(e, p), writeData(e, lands, ok), rotate(e), send(e), timeout(e), resolve(e),
      all { responses != Set(), nondet rs = responses.oneOf() receive(e, rs) },
      newIncarnation(z), tick, tickBack,
      all { inflight != Set(), nondet q = inflight.oneOf() any { apply(q), lose(q) } },
      takeLease(w), cBegin(w), cCheck(w), cInsert(w), cAdvance(w), workerCrash(w), otherInsert,
      readerLease(r, s), readerRead(r), readerRelease(r),
      gcPlan, gcCommit, gcDelete,
    }
  }

  // ---- properties ----------------------------------------------------------------------------
  def commitsOf(p: int): Set[int] = log.keys().filter(s => log.get(s).kind == Commit and log.get(s).payload == p)

  // End to end: each OTLP request's rows land centrally at most once.
  val payloadIngestedAtMostOnce: bool = PAYLOADS.forall(p => central.get(p) <= 1)

  // The log commits each request once, across retries, ambiguity, crashes
  // and zombies (edgePublish's committedNoDuplicatePayload).
  val logNoDuplicatePayload: bool = PAYLOADS.forall(p => commitsOf(p).size() <= 1)

  // Nothing is committed into a generation after its seal, or after a fence
  // closed its epoch (edgePublish's sealMatchesManifests: a seal is a log
  // position, so what it covers is exactly the commits before it).
  val sealMatchesManifests: bool =
    log.keys().forall(s1 => log.keys().forall(s2 =>
      (log.get(s1).kind == Commit and closes(log.get(s2), log.get(s1).epoch, log.get(s1).gen)) implies s1 < s2))

  // A generation is sealed at most once (publish.go can re-seal a reopened
  // generation, overwriting _sealed.json).
  val sealOnce: bool =
    log.keys().forall(s1 => log.keys().forall(s2 =>
      (log.get(s1).kind == Seal and log.get(s2).kind == Seal and log.get(s1).epoch == log.get(s2).epoch
       and log.get(s1).gen == log.get(s2).gen) implies s1 == s2))

  // A commit's data exists where it says (until GC has passed it).
  val commitImpliesData: bool =
    log.keys().forall(s => (s >= gc.horizon and log.get(s).kind == Commit) implies data.contains(dataOf(log.get(s))))

  // Nothing uncommitted reaches central.
  val onlyCommittedIngested: bool =
    PAYLOADS.forall(p => central.get(p) > 0 implies everLog.exists(x => x.entry.kind == Commit and x.entry.payload == p))

  // No write from a fenced writer: no entry follows an entry of a later epoch.
  val noWriteFromFencedWriter: bool =
    log.keys().forall(s1 => log.keys().forall(s2 =>
      (s1 < s2 and log.get(s1).kind != Free and log.get(s2).kind != Free) implies log.get(s1).epoch <= log.get(s2).epoch))

  // A committed entry is never overwritten.
  val noCommitLost: bool = everLog.forall(x => x.entry.kind != Commit or log.get(x.slot) == x.entry)

  // Each namespace (generation's table/endpoint) is written by one epoch only.
  val nsSingleWriter: bool =
    dataWriters.forall(a => dataWriters.forall(b => a.ns == b.ns implies a.epoch == b.epoch))

  // GC never deletes data that is unacknowledged by the consumer or leased by a reader.
  val gcKeepsLiveData: bool =
    log.keys().forall(s => (log.get(s).kind == Commit
      and (s >= ckpt.next or READERS.exists(r => readers.get(r) == s))) implies data.contains(dataOf(log.get(s))))

  val noReadOfDeleted: bool = not(badRead)

  // The checkpoint never passes a batch that was not ingested.
  val ackedImpliesIngested: bool =
    log.keys().forall(s => (s < ckpt.next and log.get(s).kind == Commit) implies central.get(log.get(s).payload) >= 1)

  // A live writer never leaves a generation behind unsealed: every generation
  // it has written into, other than its current one, is closed in the log or
  // is the seal it is still appending.
  val generationsSealedOrPending: bool =
    1.to(MAX_EPOCH).forall(e => {
      val w = writers.get(e)
      (w.alive and w.phase != WHalted) implies w.opened.forall(g =>
        g == w.gen or closedGen(log, e, g)
        or (w.entry.kind == Seal and w.entry.gen == g and Set(WReady, WWaiting, WUnresolved).contains(w.phase)))
    })

  val safety: bool = and {
    payloadIngestedAtMostOnce, logNoDuplicatePayload, sealMatchesManifests, sealOnce, commitImpliesData,
    onlyCommittedIngested, noWriteFromFencedWriter, noCommitLost, nsSingleWriter, gcKeepsLiveData,
    noReadOfDeleted, ackedImpliesIngested, generationsSealedOrPending,
  }

  // ---- witnesses (reachability; expect > 0 traces) ----------------------------------------------
  val wCommitted: bool = committed(log) != Set()
  val wSealed: bool = log.keys().exists(s => log.get(s).kind == Seal)
  val wFenced: bool = log.keys().exists(s => log.get(s).kind == Fence)
  val wZombieHalted: bool = events.contains("halted")
  val wResolvedOwn: bool = events.contains("resolvedOwn")
  val wResend: bool = events.contains("resend")
  val wSkipKnown: bool = events.contains("skipKnown")
  val wIngested: bool = events.contains("inserted")
  val wDedupHit: bool = events.contains("dedupHit")
  val wCkptCasFailed: bool = events.contains("ckptCasFailed")
  val wGcDeleted: bool = events.contains("gcDeleted")
  val wOrphanSwept: bool = events.contains("orphanSwept")
  val wReaderRead: bool = events.contains("readerRead")
  val wClockBack: bool = events.contains("clockBack") and committed(log) != Set()
  val wCommitAfterRestart: bool = log.keys().exists(s => log.get(s).kind == Commit and log.get(s).epoch == 2)
  // Liveness-flavoured: every payload committed and ingested, and every
  // generation that holds a commit closed (sealed, or its epoch fenced).
  val wAllCommitted: bool = queue == Set() and PAYLOADS.forall(p => committed(log).contains(p))
  val wAllIngested: bool = wAllCommitted and PAYLOADS.forall(p => central.get(p) >= 1)
  val wAllClosed: bool = wAllCommitted and log.keys().forall(s =>
    log.get(s).kind == Commit implies closedGen(log, log.get(s).epoch, log.get(s).gen))
}

// ---- instances ----------------------------------------------------------------------------------
// Hostile environment everywhere: ambiguous and late S3 writes, zombie
// writers, clock steps back, workers that crash or outlive their lease,
// unrelated inserts evicting dedup tokens.

// The S3-native design.
module s3NativeDesign {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// Mutation: log slots written with a plain PUT (no If-None-Match).
module nonConditionalLog {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = false, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// Mutation: a lease without fencing. Each epoch appends under its own prefix
// (as publish.go's manifests do) and a CAS'd lease is the only exclusion: a
// zombie never collides with its successor, so nothing stops it.
module leaseWithoutFencing {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = false, FENCE_ENTRY = false, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// Mutation: one shared log, but no fence entry. Collisions still fence a
// zombie; what is lost is the closing of the predecessor's generations.
module noFenceEntry {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = false, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// PBT finding 1: namespaces without the epoch and persistent local tables:
// a restart in the same generation reuses the previous epoch's table.
module restartReusesTable {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = false, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// PBT finding 2: the generation comes from a clock read at push time.
module genFromClock {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = false, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// PBT finding 3: a seal that fails (times out) is not retried.
module noSealRetry {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = false,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// Mutation: the consumer checkpoint is written blindly (no If-Match).
module blindCheckpoint {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = false, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// Mutation: GC writes its state blindly (read, then write back without If-Match).
module gcBlindWrite {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = false).*
}

// Mutation: GC ignores reader leases.
module gcIgnoresLeases {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = false, GC_CAS = true).*
}

// The gap S3 cannot close: a worker whose lease was taken over still inserts
// (no time bound). Only insert idempotency can cover it, and the count window
// is evicted by other traffic here.
module unboundedZombieInsert {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = false,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// ...the same gap when the dedup window is never evicted: the content token covers it.
module unboundedZombieTokenHolds {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = false, ZOMBIE_INSERT_BOUNDED = false,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}

// F4 again: S3 CAS alone does not make insert + checkpoint atomic. Without
// the check before insert, a crash between them depends on the dedup window.
module noCheckCentral {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = false, GC_WAITS_FOR_LEASES = true, GC_CAS = true).*
}
