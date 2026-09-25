// -*- mode: Bluespec; -*-
/**
 * s3InlineConsumerCompact: checkpoint compaction for the consumer fleet
 * (../otap-rs/src/consumer/coord.rs `CkptDoc::compact`, README "Consumer",
 * "Checkpoint compaction").
 *
 * It is s3InlineConsumer.qnt restated, plus compaction: that file and
 * s3Inline.qnt are unchanged. (Not a wrapper that imports s3InlineConsumer:
 * quint 0.32 doesn't re-export a nested instance's names, so a wrapper can't
 * reach s3Inline's log, writers or requests, which GC's retirement and the
 * invariants read; and a second `import s3Inline as L` is a separate copy of
 * the state. So this module imports s3Inline itself, with the same
 * parameters, and repeats the consumer's actions verbatim, except for the
 * guards marked below.) It adds:
 *
 *  - `retired`: GC removes a closed epoch entirely (its data slots and its
 *    tombstone) once no writer of it is left: the zombie bound, which here
 *    is "its writer is dead or halted, and none of its requests is in
 *    flight". (s3InlineConsumer's `gcTombs` mutant is the same deletion
 *    without that bound.)
 *  - `floor` and compaction: the lane's holder drops from the checkpoint
 *    every epoch that is closed there AND retired, and moves the floor up
 *    over them. An epoch at or below the floor is forgotten: its entry is
 *    reset, and no worker (by its own view of the floor) or GC looks at it
 *    again. Epochs are numbered in the order they are made, which is the
 *    order of their names at the edge (a name is minted at the epoch's
 *    first write).
 *  - `viewFloor`: each worker's view of the floor (a stale worker acts on
 *    its own view, as the code does).
 *
 * The changed guards: a worker never checks, tombstones or closes an epoch
 * at or below its view of the floor; GC never touches one at or below the
 * floor. Taking the lane reads the floor with the checkpoint.
 *
 * Every s3InlineConsumer invariant is checked unchanged (`safety`), plus
 * the compaction-aware forms: an epoch at or below the floor no longer has
 * a checkpoint entry to be "still ahead of", so there every committed batch
 * must be in central.
 *
 * Mutation EARLY_COMPACT: compaction drops every superseded epoch (a newer
 * one exists), closed and retired or not: a committed batch of an epoch
 * compacted before it was ingested is never ingested (neverSkipsCommitted).
 *
 * The bound on the checkpoint's size needs many epochs; s3Inline explores
 * two. `compactBound` below is an abstract model of one lane's epochs for
 * that.
 */
module s3InlineConsumerCompact {
  const TIME_BOUND: bool
  const VERIFY: bool
  const GC_TOMBS: bool
  const ANNOUNCE_EARLY: bool
  // The environment (the design runs with everything on; a mutant's
  // instance switches off what its counterexample doesn't need, so that
  // simulation finds it in reasonable time).
  const WRITER_FAULTS: bool    // ambiguous and lost PUTs, zombie writers
  const SERIES: bool           // the series lane's actions
  const TTL: int               // the lease TTL, in ticks
  // Checkpoint compaction (this module): the mutation.
  const EARLY_COMPACT: bool    // compaction drops superseded epochs, closed and retired or not

  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 2, MAX_SLOT = 3,
    AMBIGUOUS = WRITER_FAULTS, ZOMBIES = WRITER_FAULTS, CONSUMER_CRASH = false,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true) as L from "./s3Inline"

  pure val WORKERS: Set[int] = Set(1, 2)
  pure val MARGIN: int = 1
  pure val BUDGET: int = 1
  pure val MAX_TIME: int = 24
  pure val NEVER: int = 1000
  pure val REQS: Set[int] = Set(1, 2)

  type WPhase = WIdle | WChecked | WSent
  type Obj = { epoch: int, slot: int, payload: int }
  type Ckpt = { version: int, leaseEpoch: int, next: int -> int, closed: int -> bool }
  type Worker = {
    holds: bool, inc: int, leaseEpoch: int, sent: int,
    // since when this process has seen the current lease version (a
    // restarted process has seen nothing before its restart)
    seen: int,
    view: Ckpt, phase: WPhase, epoch: int, objs: Set[Obj], pending: Set[Obj],
  }
  type Stmt = { worker: int, inc: int, objs: Set[Obj], fence: int }
  type Lease = { owner: int, epoch: int, sent: int }
  type SObj = { epoch: int, r: int }

  // ---- state ----------------------------------------------------------------------
  var time: int
  var lease: Lease                 // owner 0: none yet
  var workers: int -> Worker
  var ckpt: Ckpt
  var central: int -> int          // inserts per payload
  var stmts: Set[Stmt]             // statements in flight
  var cevents: Set[str]
  // the series lane (edge side)
  var sEpoch: int
  var sCache: Set[int]             // series announced in this epoch
  var sInflight: Set[SObj]         // series objects sent, not yet landed or lost
  var sCommitted: Set[SObj]        // series objects committed
  var sPoints: Set[int]            // requests whose points object committed
  // compaction
  var floor: int                   // epochs 1..floor are compacted out of the checkpoint
  var retired: Set[int]            // epochs GC removed entirely (tombstone included)
  var viewFloor: int -> int        // each worker's view of the floor (read with the checkpoint)

  // ---- pure logic -------------------------------------------------------------------
  pure val C0: Ckpt = { version: 0, leaseEpoch: 0, next: 1.to(2).mapBy(_ => 0), closed: 1.to(2).mapBy(_ => false) }
  pure def idle(w: Worker): Worker = { ...w, phase: WIdle, epoch: 0, objs: Set(), pending: Set() }

  // The holder's window (coord.rs: Held::safe_until, may_start, fence_wall_ms, lapsed).
  pure def safeUntil(w: Worker): int = w.sent + TTL - MARGIN
  def mayStart(w: Worker): bool = not(TIME_BOUND) or time + BUDGET <= safeUntil(w)
  pure def fenceOf(w: Worker): int = if (TIME_BOUND) safeUntil(w) - BUDGET else NEVER
  def lapsed(w: Worker): bool = TIME_BOUND and time >= safeUntil(w)
  // A worker may take the lane (coord.rs: Observer::may_take) once it has
  // seen the same version for TTL + MARGIN. Workers see each version the
  // moment it is written (the earliest possible takeover); a restarted
  // worker only from its restart on.
  def takeable(w: int): bool = lease.owner == 0 or time >= workers.get(w).seen + TTL + MARGIN
  // Every worker sees a new lease version at once.
  def seeAll(ws: int -> Worker): int -> Worker = ws.keys().mapBy(x => { ...ws.get(x), seen: time })
  // Its lease is still the one it wrote (a CAS on it would succeed).
  def current(w: int): bool =
    lease == { owner: w, epoch: workers.get(w).leaseEpoch, sent: workers.get(w).sent }

  // The consecutive done prefix from `from` (worker.rs: advance).
  pure def advanceTo(from: int, objs: Set[Obj], done: Set[Obj]): int =
    0.to(2).fold(from, (n, i) =>
      if (n == from + i and objs.exists(o => o.slot == n) and done.exists(o => o.slot == n)) n + 1 else n)

  // ---- helpers ------------------------------------------------------------------------
  action keepK = all { floor' = floor, retired' = retired, viewFloor' = viewFloor }
  action keepL = all { keepL0, keepK }
  action keepLog = all { keepLog0, keepK }
  action keepL0 = all {
    L::queue' = L::queue, L::acked' = L::acked, L::lease' = L::lease, L::writers' = L::writers,
    L::log' = L::log, L::inflight' = L::inflight, L::responses' = L::responses, L::everLog' = L::everLog,
    L::ckpt' = L::ckpt, L::closed' = L::closed, L::consumer' = L::consumer, L::central' = L::central,
    L::events' = L::events,
  }
  action keepLog0 = all {
    L::queue' = L::queue, L::acked' = L::acked, L::lease' = L::lease, L::writers' = L::writers,
    L::inflight' = L::inflight, L::responses' = L::responses,
    L::ckpt' = L::ckpt, L::closed' = L::closed, L::consumer' = L::consumer, L::central' = L::central,
    L::events' = L::events,
  }
  action keepSeries = all {
    sEpoch' = sEpoch, sCache' = sCache, sInflight' = sInflight, sCommitted' = sCommitted, sPoints' = sPoints,
  }
  action keepC = all {
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts,
    cevents' = cevents,
  }

  // ---- the log: s3Inline's writer, S3 and network ---------------------------------------
  action lStartPush(e: int, p: int): bool = all { L::startPush(e, p), keepC, keepSeries, keepK }
  action lSend(e: int): bool = all { L::send(e), keepC, keepSeries, keepK }
  action lTimeout(e: int): bool = all { L::timeout(e), keepC, keepSeries, keepK }
  action lResolve(e: int): bool = all { L::resolve(e), keepC, keepSeries, keepK }
  action lSwitchPayload(e: int, p: int): bool = all { L::switchPayload(e, p), keepC, keepSeries, keepK }
  action lReceive(e: int, r: L::Resp): bool = all { L::receive(e, r), keepC, keepSeries, keepK }
  action lNewIncarnation(z: bool): bool = all { L::newIncarnation(z), keepC, keepSeries, keepK }
  action lApply(q: L::Req): bool = all { L::apply(q), keepC, keepSeries, keepK }
  action lLose(q: L::Req): bool = all { L::lose(q), keepC, keepSeries, keepK }

  // ---- time ---------------------------------------------------------------------------------
  action tick: bool = all {
    time < MAX_TIME, time' = time + 1,
    lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents,
    keepL, keepSeries,
  }

  // ---- workers -------------------------------------------------------------------------------
  // Take the lane (lease CAS: epoch + 1), then fence the checkpoint: rewrite
  // it under the new lease epoch, so any older holder's CAS fails.
  action wAcquire(w: int): bool = {
    val wk = workers.get(w)
    val ne = lease.epoch + 1
    val nc = { ...ckpt, version: ckpt.version + 1, leaseEpoch: ne }
    all {
      not(wk.holds), wk.phase == WIdle, takeable(w),
      lease' = { owner: w, epoch: ne, sent: time },
      ckpt' = nc,
      workers' = seeAll(workers.set(w, { ...idle(wk), holds: true, leaseEpoch: ne, sent: time, view: nc })),
      cevents' = cevents.union(if (lease.owner != 0) Set("takeover") else Set()),
      time' = time, central' = central, stmts' = stmts, keepL0, keepSeries,
      floor' = floor, retired' = retired, viewFloor' = viewFloor.set(w, floor),
    }
  }

  action wRenew(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds, current(w), not(lapsed(wk)),
      lease' = { ...lease, sent: time },
      workers' = seeAll(workers.set(w, { ...wk, sent: time })),
      time' = time, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents, keepL, keepSeries,
    }
  }

  // Past its window by its own clock: stop, without waiting to be told.
  action wLapse(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds, lapsed(wk),
      workers' = workers.set(w, { ...idle(wk), holds: false }),
      cevents' = cevents.union(Set("lapsed")),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, keepL, keepSeries,
    }
  }

  // Check the next k slots of epoch e (all data) against central.
  action wCheck(w: int, e: int, k: int): bool = {
    val wk = workers.get(w)
    val s0 = wk.view.next.get(e)
    val objs = s0.to(s0 + k - 1).map(s => { epoch: e, slot: s, payload: L::at(e, s).payload })
    all {
      wk.holds, wk.phase == WIdle, mayStart(wk), e <= L::lease, not(wk.view.closed.get(e)), e > viewFloor.get(w),
      s0 + k - 1 < L::MAX_SLOT, s0.to(s0 + k - 1).forall(s => L::at(e, s).kind == L::Data),
      // absent ones only, one object per payload (a copy rides on it)
      val absent = objs.filter(o => central.get(o.payload) == 0)
      val pending = absent.filter(o => absent.forall(x => x.payload == o.payload implies x.slot >= o.slot))
      workers' = workers.set(w, { ...wk, phase: WChecked, epoch: e, objs: objs, pending: pending }),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents,
      keepL, keepSeries,
    }
  }

  // Send the statement (the window is checked again right before).
  action wSend(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds, wk.phase == WChecked, mayStart(wk),
      stmts' = if (wk.pending == Set()) stmts
               else stmts.union(Set({ worker: w, inc: wk.inc, objs: wk.pending, fence: fenceOf(wk) })),
      workers' = workers.set(w, { ...wk, phase: WSent }),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, cevents' = cevents, keepL, keepSeries,
    }
  }

  // Central runs a statement: only by its fence; any part of it may land.
  action cApply(q: Stmt, sub: Set[Obj]): bool = all {
    stmts.contains(q), time <= q.fence, sub.subseteq(q.objs), sub != Set(),
    central' = central.keys().mapBy(p => central.get(p) + (if (sub.exists(o => o.payload == p)) 1 else 0)),
    stmts' = stmts.exclude(Set(q)),
    cevents' = cevents.union(
      (if (sub != q.objs) Set("partial") else Set()).union(
       if (not(workers.get(q.worker).holds and workers.get(q.worker).inc == q.inc and current(q.worker)))
         Set("zombieLanded") else Set())),
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, keepL, keepSeries,
  }

  // A statement fails whole, or is fenced by the server (a no-op).
  action cDrop(q: Stmt): bool = all {
    stmts.contains(q),
    stmts' = stmts.exclude(Set(q)),
    cevents' = cevents.union(if (time > q.fence) Set("fenced") else Set()),
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, keepL, keepSeries,
  }

  // Once its statement is gone: count what landed and CAS the checkpoint past the done prefix.
  action wAdvance(w: int): bool = {
    val wk = workers.get(w)
    val done = if (VERIFY) wk.objs.filter(o => central.get(o.payload) >= 1) else wk.objs
    val n = advanceTo(wk.view.next.get(wk.epoch), wk.objs, done)
    val nc = { ...ckpt, version: ckpt.version + 1, next: ckpt.next.set(wk.epoch, n) }
    all {
      wk.holds, wk.phase == WSent, stmts.forall(q => not(q.worker == w and q.inc == wk.inc)),
      if (ckpt.version == wk.view.version) all {
        ckpt' = nc,
        workers' = workers.set(w, { ...idle(wk), view: nc }),
        cevents' = cevents,
      } else all {
        ckpt' = ckpt,
        workers' = workers.set(w, { ...idle(wk), holds: false }),
        cevents' = cevents.union(Set("casLost")),
      },
      time' = time, lease' = lease, central' = central, stmts' = stmts, keepL, keepSeries,
    }
  }

  // A superseded epoch's head is free: tombstone it (create-only) and close it.
  // (`s` is the worker's checkpoint slot, passed in: quint 0.32 resolves a
  // state variable read on the right of `L::x' = ...` in L's namespace, and
  // L has variables of its own called `ckpt` and `central`.)
  action wTomb(w: int, e: int, s: int): bool = {
    val wk = workers.get(w)
    val nc = { ...ckpt, version: ckpt.version + 1, closed: ckpt.closed.set(e, true) }
    all {
      wk.holds, wk.phase == WIdle, e < L::lease, not(wk.view.closed.get(e)), s == wk.view.next.get(e), s < L::MAX_SLOT,
      e > viewFloor.get(w),
      L::at(e, s).kind == L::Free, ckpt.version == wk.view.version,
      L::log' = L::putSlot(e, s, L::tombE(e)),
      L::everLog' = L::everLog.union(Set({ epoch: e, slot: s, entry: L::tombE(e) })),
      ckpt' = nc,
      workers' = workers.set(w, { ...wk, view: nc }),
      cevents' = cevents.union(Set("closed")),
      time' = time, lease' = lease, central' = central, stmts' = stmts, keepLog, keepSeries,
    }
  }

  // A tombstone (anyone's) is at the checkpoint: close the epoch there.
  action wSeeTomb(w: int, e: int): bool = {
    val wk = workers.get(w)
    val nc = { ...ckpt, version: ckpt.version + 1, closed: ckpt.closed.set(e, true) }
    all {
      wk.holds, wk.phase == WIdle, not(wk.view.closed.get(e)), wk.view.next.get(e) < L::MAX_SLOT, e > viewFloor.get(w),
      L::at(e, wk.view.next.get(e)).kind == L::Tomb, ckpt.version == wk.view.version,
      ckpt' = nc, workers' = workers.set(w, { ...wk, view: nc }),
      time' = time, lease' = lease, central' = central, stmts' = stmts, cevents' = cevents, keepL, keepSeries,
    }
  }

  // The process dies: its statements may still be in flight.
  action wCrash(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds or wk.phase != WIdle,
      workers' = workers.set(w, { ...idle(wk), holds: false, inc: wk.inc + 1, seen: time }),
      cevents' = cevents.union(Set("workerCrash")),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, keepL, keepSeries,
    }
  }

  // ---- GC ---------------------------------------------------------------------------------------
  // Delete epoch e's data slots below the checkpoint, once no request for
  // them is in flight (the request-lifetime delay). GC_TOMBS: also a closed
  // epoch's tombstone.
  // (`n` and `closed` are the checkpoint's, passed in; see wTomb.)
  action gc(e: int, n: int, closed: bool, doomed: Set[int]): bool = {
    all {
      e > floor,
      n == ckpt.next.get(e), closed == ckpt.closed.get(e),
      doomed == L::slots.filter(s =>
        (s < n and L::at(e, s).kind == L::Data) or (GC_TOMBS and closed and s == n and L::at(e, s).kind == L::Tomb)),
      doomed != Set(),
      L::inflight.forall(q => not(q.epoch == e and q.slot < n)),
      L::log' = L::log.set(e, L::log.get(e).keys().mapBy(s => if (doomed.contains(s)) L::FREE else L::log.get(e).get(s))),
      L::everLog' = L::everLog,
      cevents' = cevents.union(Set("gc")),
      keepLog, time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts,
      keepSeries,
    }
  }

  // ---- GC retires a closed epoch (this module) ------------------------------------------------
  // Everything of it goes, tombstone included, once the zombie bound has
  // passed: no writer of it is left that could write again (dead, or
  // halted by the tombstone) and none of its requests is in flight.
  // (s3InlineConsumer's `gcTombs` mutant is this deletion without the bound.)
  action gcRetire(e: int): bool = all {
    e > floor, not(retired.contains(e)), ckpt.closed.get(e),
    not(L::writers.get(e).alive) or L::writers.get(e).phase == L::WHalted,
    L::inflight.forall(q => q.epoch != e),
    L::log' = L::log.set(e, L::log.get(e).keys().mapBy(s => L::FREE)),
    L::everLog' = L::everLog,
    keepLog0, keepSeries,
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts,
    cevents' = cevents.union(Set("retired")),
    retired' = retired.union(Set(e)), floor' = floor, viewFloor' = viewFloor,
  }

  // ---- compaction (coord.rs: CkptDoc::compact), a checkpoint CAS by the holder ----------------
  // The epochs to drop: closed in the checkpoint AND retired (EARLY_COMPACT:
  // every superseded one).
  pure def dropsOf(fl: int, lease: int, closed: int -> bool, ret: Set[int]): Set[int] =
    1.to(2).filter(e => e > fl and (if (EARLY_COMPACT) e < lease else closed.get(e) and ret.contains(e)))
  // The floor moves over the dropped epochs just above it. Epochs are
  // numbered in the order they are made, which is the order of their names
  // (an edge names an epoch at its first write): every epoch made later is
  // above all of these.
  pure def floorAfter(fl: int, drops: Set[int]): int =
    1.to(2).fold(fl, (f, e) => if (e == f + 1 and drops.contains(e)) e else f)

  // (`nf`, the new floor, is passed in.) Dropped entries are forgotten: reset.
  action wCompact(w: int, nf: int): bool = {
    val wk = workers.get(w)
    val nc = { ...ckpt, version: ckpt.version + 1,
               next: ckpt.next.keys().mapBy(e => if (e <= nf) 0 else ckpt.next.get(e)),
               closed: ckpt.closed.keys().mapBy(e => if (e <= nf) false else ckpt.closed.get(e)) }
    all {
      wk.holds, wk.phase == WIdle, ckpt.version == wk.view.version,
      nf == floorAfter(floor, dropsOf(floor, L::lease, ckpt.closed, retired)), nf > floor,
      ckpt' = nc,
      workers' = workers.set(w, { ...wk, view: nc }),
      floor' = nf, viewFloor' = viewFloor.set(w, nf), retired' = retired,
      cevents' = cevents.union(Set("compacted")),
      time' = time, lease' = lease, central' = central, stmts' = stmts, keepL0, keepSeries,
    }
  }

  // ---- the series lane (edge) --------------------------------------------------------------------
  // Request r's points object commits; its series goes out in a series
  // object unless it is announced in this epoch.
  action sPush(r: int): bool = all {
    SERIES,
    not(sPoints.contains(r)),
    sPoints' = sPoints.union(Set(r)),
    sInflight' = if (sCache.contains(r)) sInflight else sInflight.union(Set({ epoch: sEpoch, r: r })),
    sCache' = if (ANNOUNCE_EARLY) sCache.union(Set(r)) else sCache,
    sEpoch' = sEpoch, sCommitted' = sCommitted, keepC, keepL,
  }
  // A retry of a request whose series object is unresolved sends it again.
  action sResend(r: int): bool = all {
    SERIES,
    sPoints.contains(r), not(sCache.contains(r)), not(sInflight.exists(q => q.r == r)),
    sInflight' = sInflight.union(Set({ epoch: sEpoch, r: r })),
    sEpoch' = sEpoch, sCache' = sCache, sCommitted' = sCommitted, sPoints' = sPoints, keepC, keepL,
  }
  action sLand(q: SObj): bool = all {
    SERIES,
    sInflight.contains(q),
    sInflight' = sInflight.exclude(Set(q)), sCommitted' = sCommitted.union(Set(q)),
    sEpoch' = sEpoch, sCache' = sCache, sPoints' = sPoints, keepC, keepL,
  }
  action sLose(q: SObj): bool = all {
    SERIES,
    sInflight.contains(q),
    sInflight' = sInflight.exclude(Set(q)),
    sEpoch' = sEpoch, sCache' = sCache, sCommitted' = sCommitted, sPoints' = sPoints, keepC, keepL,
  }
  // The series object has committed (in this epoch): now announced.
  action sAnnounce(r: int): bool = all {
    SERIES,
    sCommitted.contains({ epoch: sEpoch, r: r }), not(sCache.contains(r)),
    sCache' = sCache.union(Set(r)),
    sEpoch' = sEpoch, sInflight' = sInflight, sCommitted' = sCommitted, sPoints' = sPoints, keepC, keepL,
  }
  // A new epoch starts with an empty cache.
  action sRestart: bool = all {
    SERIES,
    sEpoch < 2,
    sEpoch' = sEpoch + 1, sCache' = Set(),
    sInflight' = sInflight, sCommitted' = sCommitted, sPoints' = sPoints, keepC, keepL,
  }

  // ---- state machine --------------------------------------------------------------------------------
  action init = all {
    L::init,
    time' = 0,
    lease' = { owner: 0, epoch: 0, sent: 0 },
    workers' = WORKERS.mapBy(_ => { holds: false, inc: 0, leaseEpoch: 0, sent: 0, seen: 0, view: C0,
                                     phase: WIdle, epoch: 0, objs: Set(), pending: Set() }),
    ckpt' = C0,
    central' = L::PAYLOADS.mapBy(_ => 0),
    stmts' = Set(),
    cevents' = Set(),
    sEpoch' = 1, sCache' = Set(), sInflight' = Set(), sCommitted' = Set(), sPoints' = Set(),
    floor' = 0, retired' = Set(), viewFloor' = WORKERS.mapBy(_ => 0),
  }

  action step = {
    nondet e = 1.to(2).oneOf()
    nondet p = L::PAYLOADS.oneOf()
    nondet z = Set(true, false).oneOf()
    nondet w = WORKERS.oneOf()
    nondet k = Set(1, 2).oneOf()
    nondet r = REQS.oneOf()
    any {
      lStartPush(e, p), lSend(e), lTimeout(e), lResolve(e), lSwitchPayload(e, p), lNewIncarnation(z),
      all { L::responses != Set(), nondet q = L::responses.oneOf() lReceive(e, q) },
      all { L::inflight != Set(), nondet q = L::inflight.oneOf() any { lApply(q), lLose(q) } },
      tick,
      wAcquire(w), wRenew(w), wLapse(w), wCheck(w, e, k), wSend(w), wAdvance(w), wSeeTomb(w, e), wCrash(w),
      wTomb(w, e, workers.get(w).view.next.get(e)),
      all { stmts != Set(), nondet q = stmts.oneOf() all {
        nondet sub = q.objs.powerset().oneOf()
        any { cApply(q, sub), cDrop(q) } } },
      all {
        val n = ckpt.next.get(e)
        val c = ckpt.closed.get(e)
        gc(e, n, c, L::slots.filter(s =>
          (s < n and L::at(e, s).kind == L::Data) or (GC_TOMBS and c and s == n and L::at(e, s).kind == L::Tomb)))
      },
      gcRetire(e),
      wCompact(w, floorAfter(floor, dropsOf(floor, L::lease, ckpt.closed, retired))),
      sPush(r), sResend(r), sAnnounce(r), sRestart,
      all { sInflight != Set(), nondet q = sInflight.oneOf() any { sLand(q), sLose(q) } },
    }
  }

  // ---- properties ---------------------------------------------------------------------------------
  // Every batch is inserted into central at most once.
  val atMostOnce: bool = L::PAYLOADS.forall(p => central.get(p) <= 1)

  // Nothing uncommitted is ingested.
  val onlyCommittedIngested: bool = L::PAYLOADS.forall(p =>
    central.get(p) > 0 implies L::everLog.exists(x => x.entry.kind == L::Data and x.entry.payload == p))

  // Nothing committed is skipped: every batch ever committed is in central,
  // or still ahead of an open epoch's checkpoint (GC only removes ingested slots).
  val neverSkipsCommitted: bool = L::everLog.forall(x => x.entry.kind == L::Data implies
    (central.get(x.entry.payload) >= 1 or (x.slot >= ckpt.next.get(x.epoch) and not(ckpt.closed.get(x.epoch)))))

  // A closed epoch takes no data at or after its tombstone.
  val noCommitAfterClose: bool = 1.to(2).forall(e => ckpt.closed.get(e) implies
    L::slots.forall(s => s >= ckpt.next.get(e) implies L::at(e, s).kind != L::Data))

  // The writer side keeps its guarantees (GC-aware: committed means ever committed).
  val ackedImpliesCommitted: bool = L::acked.forall(p => L::everLog.exists(x => x.entry == L::dataE(x.epoch, p)))
  val noPayloadLost: bool = L::PAYLOADS.forall(p => L::queue.contains(p) or L::everLog.exists(x => x.entry.kind == L::Data and x.entry.payload == p))

  // The series lane: a series is marked announced only after its series object committed.
  val announcedOnlyAfterCommit: bool = sCache.forall(r => sCommitted.exists(q => q.r == r))

  val safety: bool = and {
    atMostOnce, onlyCommittedIngested, neverSkipsCommitted, noCommitAfterClose,
    ackedImpliesCommitted, noPayloadLost, L::epochNoDuplicatePayload, announcedOnlyAfterCommit,
  }

  // ---- witnesses ------------------------------------------------------------------------------------
  val wTakeover: bool = cevents.contains("takeover")
  val wLapsed: bool = cevents.contains("lapsed")
  val wFenced: bool = cevents.contains("fenced")
  val wPartial: bool = cevents.contains("partial")
  val wCasLost: bool = cevents.contains("casLost")
  val wClosed: bool = cevents.contains("closed")
  val wGc: bool = cevents.contains("gc")
  val wIngested: bool = L::PAYLOADS.exists(p => central.get(p) == 1)
  val wAllIngested: bool = L::PAYLOADS.forall(p => central.get(p) == 1)
  val wTakeoverIngest: bool = wTakeover and wAllIngested
  val wAnnounced: bool = sCache != Set()

  // ---- compaction's properties (this module) --------------------------------------------------------
  // neverSkipsCommitted, compaction-aware: a batch ever committed is in
  // central, or still ahead of the checkpoint of an open epoch ABOVE the
  // floor (at or below it there is no entry left to be ahead of).
  val neverSkipsCommittedCompact: bool = L::everLog.forall(x => x.entry.kind == L::Data implies
    (central.get(x.entry.payload) >= 1 or
     (x.epoch > floor and x.slot >= ckpt.next.get(x.epoch) and not(ckpt.closed.get(x.epoch)))))
  // Nothing is committed in an epoch at or below the floor.
  val noCommitBelowFloor: bool = 1.to(2).forall(e => e <= floor implies L::slots.forall(s => L::at(e, s).kind != L::Data))
  // The floor only passes retired epochs, and they have no entry left.
  val floorSound: bool = 1.to(2).forall(e => e <= floor implies
    (retired.contains(e) and ckpt.next.get(e) == 0 and not(ckpt.closed.get(e))))
  // A worker's view of the floor never runs ahead of the checkpoint's.
  val viewFloorSound: bool = WORKERS.forall(w => viewFloor.get(w) <= floor)

  val compactSafety: bool = and { safety, neverSkipsCommittedCompact, noCommitBelowFloor, floorSound, viewFloorSound }

  val wRetired: bool = retired != Set()
  val wCompacted: bool = floor > 0
  val wCompactedAllIngested: bool = floor > 0 and wAllIngested
}

// The design, in the full hostile environment.
module compactDesign {
  import s3InlineConsumerCompact(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    EARLY_COMPACT = false, WRITER_FAULTS = true, SERIES = true, TTL = 6).*
}
// The design with no writer faults and no series lane: most steps go to the workers.
module compactQuiet {
  import s3InlineConsumerCompact(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    EARLY_COMPACT = false, WRITER_FAULTS = false, SERIES = false, TTL = 6).*
}
// Mutation: compaction drops superseded epochs before they are closed, ingested and retired.
module earlyCompact {
  import s3InlineConsumerCompact(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    EARLY_COMPACT = true, WRITER_FAULTS = false, SERIES = false, TTL = 6).*
}

/**
 * compactBound: why the checkpoint stays bounded however many epochs a lane
 * has had, on an abstract lane (no slots, no workers): epochs are made by
 * edge restarts, closed (by a tombstone, once ingested) or never (a gap
 * keeps an epoch open forever), retired by GC, and compacted out of the
 * checkpoint.
 *
 * Environment assumptions (what the soak's timing gives):
 *  - restarts are rate-limited: at most K epochs are unretired at once (an
 *    epoch retires a bounded time after it is closed: quiet + zombie bound);
 *  - the worker compacts at least once between a retirement and the next
 *    restart (a full listing every `--full-list`, faster than restarts).
 *
 * Invariant `bounded`: the checkpoint holds at most K epochs.
 * Mutation FLOOR_ONLY: a plain low watermark (drop only the retired epochs
 * below the floor). An epoch that stays open (a gap) pins the floor, and
 * every epoch retired above it stays in the checkpoint: `bounded` fails.
 */
module compactBound {
  const MAX_E: int
  const K: int
  const FLOOR_ONLY: bool

  type St = Open | Closed | Retired
  var made: int                    // epochs 1..made exist
  var st: int -> St
  var explicit: Set[int]           // the checkpoint's entries
  var floor: int
  var compacted: bool              // compaction ran since the last retirement

  def unretired: Set[int] = 1.to(made).filter(e => st.get(e) != Retired)

  action init = all {
    made' = 1, st' = 1.to(MAX_E).mapBy(e => Open), explicit' = Set(), floor' = 0, compacted' = true,
  }
  // An edge restart: a new epoch, above every epoch made before it.
  action restart = all {
    made < MAX_E, unretired.size() < K, compacted,
    made' = made + 1, st' = st, explicit' = explicit, floor' = floor, compacted' = compacted,
  }
  // The worker makes progress in an epoch: it gets an entry.
  action track(e: int): bool = all {
    e <= made, e > floor, st.get(e) != Retired, not(explicit.contains(e)),
    explicit' = explicit.union(Set(e)), made' = made, st' = st, floor' = floor, compacted' = compacted,
  }
  // A superseded epoch is closed (tombstone, after everything below it was ingested).
  action close(e: int): bool = all {
    e < made, st.get(e) == Open, explicit.contains(e),
    st' = st.set(e, Closed), made' = made, explicit' = explicit, floor' = floor, compacted' = compacted,
  }
  action retire(e: int): bool = all {
    e <= made, st.get(e) == Closed,
    st' = st.set(e, Retired), made' = made, explicit' = explicit, floor' = floor, compacted' = false,
  }
  action compact: bool = {
    val nf = 1.to(made).fold(floor, (f, e) => if (e == f + 1 and st.get(e) == Retired) e else f)
    all {
      explicit' = explicit.filter(e => e > nf and (FLOOR_ONLY or st.get(e) != Retired)),
      floor' = nf, made' = made, st' = st, compacted' = true,
    }
  }
  action step = {
    nondet e = 1.to(made).oneOf()
    any { restart, track(e), close(e), retire(e), compact }
  }

  val bounded: bool = explicit.size() <= K
  val floorSound: bool = 1.to(floor).forall(e => st.get(e) == Retired)
  val aboveFloor: bool = explicit.forall(e => e > floor)
  val boundSafety: bool = and { bounded, floorSound, aboveFloor }
  // witnesses
  val wManyEpochs: bool = made == MAX_E
  val wPinned: bool = floor == 0 and made == MAX_E
}

module bound { import compactBound(MAX_E = 10, K = 3, FLOOR_ONLY = false).* }
module floorOnly { import compactBound(MAX_E = 10, K = 3, FLOOR_ONLY = true).* }
