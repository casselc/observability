// -*- mode: Bluespec; -*-
/**
 * s3InlineConsumer: the production-shaped consumer of the manifest-less
 * layout (../otap-rs/src/consumer/, README "Consumer"), on top of one
 * s3Inline log, unchanged: its writers, S3 and network are s3Inline's own
 * actions; its single consumer is replaced by what this module adds.
 *
 *  - A fleet of workers sharing the lane through a lease (owner, fencing
 *    epoch, TTL) and a checkpoint, both CAS'd objects. Time is a discrete
 *    clock. A holder counts its window from the moment it wrote its lease
 *    version (`sent`): it may START a statement only while
 *    time + BUDGET <= sent + TTL - MARGIN, and drops the lease at
 *    sent + TTL - MARGIN. Another worker may take it only at
 *    sent + TTL + MARGIN (it has seen the same version for TTL + MARGIN).
 *  - Statements over several objects, checked against central first and
 *    in flight afterwards (a paused worker, a slow network): one lands only
 *    by its server-side fence, sent + TTL - MARGIN - BUDGET, and it may land
 *    only in part (one part per object, or per partition).
 *  - The verify: after its statement is gone (answered, or killed), the
 *    worker counts what landed and advances its checkpoint past the done
 *    prefix only, by CAS. A takeover rewrites the checkpoint (a new
 *    version), so a zombie's CAS fails.
 *  - Tombstones: a superseded epoch's free head gets a create-only
 *    tombstone (atomic here; s3Inline models the race), and the checkpoint
 *    closes it.
 *  - GC: deletes ingested data slots below the checkpoint once no request
 *    for them can still be in flight (the request-lifetime delay); never the
 *    tombstone, which is what halts a zombie writer; and (2026-09-26) never
 *    the slot just below the checkpoint while the epoch isn't retired: its
 *    writer may not have resolved it (a late-landing PUT), and would put its
 *    next batch there, create-only, if the slot had been deleted.
 *  - The series lane of metrics layout B, as the edge runs it: a series is
 *    announced (not sent again in this epoch) only once the series object
 *    that carried it has committed.
 *  - Release (load balancing): a holder gives its lane back (the lease CAS
 *    to "no owner", fencing epoch + 1), and anyone may take it at once. It
 *    does so only once none of its statements can still land.
 *  - A statement's commit can outlive its budget by SLACK (on a replicated
 *    central, one Keeper request): it lands by fence + BUDGET + SLACK. The
 *    design's rule is SLACK <= MARGIN (the code refuses to start otherwise).
 *  - Partitions and the check's range. Every object of epoch e was
 *    received on day eDay(e) (an epoch's day is the calendar day when its
 *    incarnation started; a copy of a request, resent into a later epoch,
 *    carries that later day). Central keeps rows per payload and day; a
 *    check or verify of objects of day d reads only days d - HORIZON ..
 *    d + HORIZON (the code: the objects' own received time, widened by the
 *    horizon). The environment makes every copy within MAX_DAY days of its
 *    original; the design takes HORIZON >= MAX_DAY.
 *
 * Mutations (design flags):
 *  - TIME_BOUND = false: workers ignore their windows and statements carry
 *    no fence. A paused worker's statement lands after another worker
 *    checked and inserted the same batch: ingested twice.
 *  - VERIFY = false: the checkpoint moves past a statement's objects
 *    whether they landed or not: a partial statement skips a batch.
 *  - GC_TOMBS: GC also deletes a closed epoch's tombstone: a zombie writer
 *    re-creates that slot with a batch it is told is committed, in an epoch
 *    the consumer has closed.
 *  - ANNOUNCE_EARLY: the edge marks a series announced when it sends the
 *    series object; if that PUT is lost, later requests' points reference a
 *    series row that never lands.
 *  - RELEASE_GUARD = false: a worker releases its lane while its statement
 *    may still land; the next holder checks and inserts the same batch.
 *  - SLACK > MARGIN: a commit outlives the margin (a Keeper request past
 *    max_execution_time with a margin below it) and lands after another
 *    worker took the lane and inserted the batch.
 *  - HORIZON < MAX_DAY: the check misses the original of a copy received on
 *    a later day, and ingests the copy too.
 *  - GC_KEEP = false: GC also deletes the slot below the checkpoint; a
 *    writer still unresolved there re-creates it with its next batch, below
 *    the checkpoint: committed, acked, never ingested (neverSkipsCommitted).
 *  - WALL_RANGE: the check reads today's partition by the worker's clock:
 *    a batch received on an earlier day, whose first statement landed, is
 *    checked again after midnight and inserted twice.
 */
module s3InlineConsumer {
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
  const SLACK: int             // how long a commit can outlive its budget (the design: MARGIN)
  const RELEASE_GUARD: bool    // release only once no statement of the worker can still land
  const HORIZON: int           // the check reads the objects' own day +- HORIZON
  const WALL_RANGE: bool       // mutation: the check reads today's day only
  const MAX_DAY: int           // the environment: the calendar runs days 0..MAX_DAY
  const GC_KEEP: bool          // GC keeps the slot below the position until retirement (the design)

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
  pure val DAYS: Set[int] = 0.to(MAX_DAY)
  type Lease = { owner: int, epoch: int, sent: int }
  type SObj = { epoch: int, r: int }

  // ---- state ----------------------------------------------------------------------
  var time: int
  var lease: Lease                 // owner 0: none yet
  var workers: int -> Worker
  var ckpt: Ckpt
  var central: int -> int          // inserts per payload
  var crows: int -> (int -> int)   // inserts per payload and day (the partitions)
  var day: int                     // the calendar (the workers' and the edges' clocks agree)
  var eDay: int -> int             // the day each epoch's objects were received
  var stmts: Set[Stmt]             // statements in flight
  var cevents: Set[str]
  // the series lane (edge side)
  var sEpoch: int
  var sCache: Set[int]             // series announced in this epoch
  var sInflight: Set[SObj]         // series objects sent, not yet landed or lost
  var sCommitted: Set[SObj]        // series objects committed
  var sPoints: Set[int]            // requests whose points object committed

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

  // The check's range for objects of epoch e (plan.rs: check_range), and
  // what central shows of payload p there.
  def rangeLo(e: int): int = if (WALL_RANGE) day else eDay.get(e) - HORIZON
  def rangeHi(e: int): int = if (WALL_RANGE) MAX_DAY else eDay.get(e) + HORIZON
  def visible(p: int, e: int): int =
    DAYS.filter(d => rangeLo(e) <= d and d <= rangeHi(e)).fold(0, (acc, d) => acc + crows.get(p).get(d))
  // A statement's objects can land until fence + BUDGET + SLACK; the worker
  // treats it as settled from fence + MARGIN + BUDGET + SLACK on (coord.rs:
  // Held::settled_by = sent + TTL + SLACK, the server clock allowed MARGIN behind).
  pure def landBy(q: Stmt): int = q.fence + BUDGET + SLACK
  pure def settledBy(q: Stmt): int = q.fence + MARGIN + BUDGET + SLACK
  def mayRelease(w: int): bool = {
    val wk = workers.get(w)
    wk.holds and current(w) and
      (RELEASE_GUARD implies stmts.forall(q => (q.worker == w and q.inc == wk.inc) implies time > settledBy(q)))
  }

  // The consecutive done prefix from `from` (worker.rs: advance).
  pure def advanceTo(from: int, objs: Set[Obj], done: Set[Obj]): int =
    0.to(2).fold(from, (n, i) =>
      if (n == from + i and objs.exists(o => o.slot == n) and done.exists(o => o.slot == n)) n + 1 else n)

  // ---- helpers ------------------------------------------------------------------------
  action keepL = all {
    L::queue' = L::queue, L::acked' = L::acked, L::lease' = L::lease, L::writers' = L::writers,
    L::log' = L::log, L::inflight' = L::inflight, L::responses' = L::responses, L::everLog' = L::everLog,
    L::ckpt' = L::ckpt, L::closed' = L::closed, L::consumer' = L::consumer, L::central' = L::central,
    L::events' = L::events,
  }
  action keepLog = all {
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
    cevents' = cevents, keepDays, crows' = crows,
  }
  action keepDays = all { day' = day, eDay' = eDay }

  // ---- the log: s3Inline's writer, S3 and network ---------------------------------------
  action lStartPush(e: int, p: int): bool = all { L::startPush(e, p), keepC, keepSeries }
  action lSend(e: int): bool = all { L::send(e), keepC, keepSeries }
  action lTimeout(e: int): bool = all { L::timeout(e), keepC, keepSeries }
  action lResolve(e: int): bool = all { L::resolve(e), keepC, keepSeries }
  action lSwitchPayload(e: int, p: int): bool = all { L::switchPayload(e, p), keepC, keepSeries }
  action lReceive(e: int, r: L::Resp): bool = all { L::receive(e, r), keepC, keepSeries }
  // A new incarnation's epoch is named (and its objects received) today.
  action lNewIncarnation(z: bool): bool = all {
    L::newIncarnation(z), eDay' = eDay.set(L::lease + 1, day), day' = day,
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts,
    cevents' = cevents, crows' = crows, keepSeries,
  }
  action lApply(q: L::Req): bool = all { L::apply(q), keepC, keepSeries }
  action lLose(q: L::Req): bool = all { L::lose(q), keepC, keepSeries }

  // ---- time ---------------------------------------------------------------------------------
  action tick: bool = all {
    time < MAX_TIME, time' = time + 1,
    lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents,
    keepL, keepSeries, keepDays, crows' = crows,
  }

  // Midnight.
  action newDay: bool = all {
    day < MAX_DAY, day' = day + 1, eDay' = eDay,
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts,
    cevents' = cevents.union(Set("midnight")), crows' = crows, keepL, keepSeries,
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
      time' = time, central' = central, stmts' = stmts, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  action wRenew(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds, current(w), not(lapsed(wk)),
      lease' = { ...lease, sent: time },
      workers' = seeAll(workers.set(w, { ...wk, sent: time })),
      time' = time, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Past its window by its own clock: stop, without waiting to be told.
  action wLapse(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds, lapsed(wk),
      workers' = workers.set(w, { ...idle(wk), holds: false }),
      cevents' = cevents.union(Set("lapsed")),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Give the lane back (load balancing): a lease CAS to no owner, fencing
  // epoch + 1; anyone may take it at once. Only once none of its
  // statements can still land (RELEASE_GUARD; worker.rs: coord::may_act).
  action wRelease(w: int): bool = {
    val wk = workers.get(w)
    all {
      mayRelease(w),
      lease' = { owner: 0, epoch: lease.epoch + 1, sent: time },
      workers' = seeAll(workers.set(w, { ...idle(wk), holds: false })),
      cevents' = cevents.union(Set("released")),
      time' = time, ckpt' = ckpt, central' = central, stmts' = stmts, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Check the next k slots of epoch e (all data) against central.
  action wCheck(w: int, e: int, k: int): bool = {
    val wk = workers.get(w)
    val s0 = wk.view.next.get(e)
    val objs = s0.to(s0 + k - 1).map(s => { epoch: e, slot: s, payload: L::at(e, s).payload })
    all {
      wk.holds, wk.phase == WIdle, mayStart(wk), e <= L::lease, not(wk.view.closed.get(e)),
      s0 + k - 1 < L::MAX_SLOT, s0.to(s0 + k - 1).forall(s => L::at(e, s).kind == L::Data),
      // absent ones only (in the check's range), one object per payload (a copy rides on it)
      val absent = objs.filter(o => visible(o.payload, e) == 0)
      val pending = absent.filter(o => absent.forall(x => x.payload == o.payload implies x.slot >= o.slot))
      workers' = workers.set(w, { ...wk, phase: WChecked, epoch: e, objs: objs, pending: pending }),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents,
      keepL, keepSeries, keepDays, crows' = crows,
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
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, cevents' = cevents, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Central runs a statement: it starts only by its fence, and its commit
  // lands by fence + BUDGET + SLACK; any part of it may land. Rows go to
  // the partition of the day their object was received.
  action cApply(q: Stmt, sub: Set[Obj]): bool = all {
    stmts.contains(q), time <= landBy(q), sub.subseteq(q.objs), sub != Set(),
    central' = central.keys().mapBy(p => central.get(p) + (if (sub.exists(o => o.payload == p)) 1 else 0)),
    crows' = crows.keys().mapBy(p => crows.get(p).keys().mapBy(d =>
      crows.get(p).get(d) + (if (sub.exists(o => o.payload == p and eDay.get(o.epoch) == d)) 1 else 0))),
    keepDays,
    stmts' = stmts.exclude(Set(q)),
    cevents' = cevents.union(
      (if (sub != q.objs) Set("partial") else Set()).union(
      (if (time > q.fence) Set("lateLanding") else Set())).union(
       if (not(workers.get(q.worker).holds and workers.get(q.worker).inc == q.inc and current(q.worker)))
         Set("zombieLanded") else Set())),
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, keepL, keepSeries,
  }

  // A statement fails whole, or is fenced by the server (a no-op).
  action cDrop(q: Stmt): bool = all {
    stmts.contains(q),
    stmts' = stmts.exclude(Set(q)),
    cevents' = cevents.union(if (time > q.fence) Set("fenced") else Set()),
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, keepL, keepSeries, keepDays, crows' = crows,
  }

  // Once its statement is gone: count what landed and CAS the checkpoint past the done prefix.
  action wAdvance(w: int): bool = {
    val wk = workers.get(w)
    val done = if (VERIFY) wk.objs.filter(o => visible(o.payload, o.epoch) >= 1) else wk.objs
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
      time' = time, lease' = lease, central' = central, stmts' = stmts, keepL, keepSeries, keepDays, crows' = crows,
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
      L::at(e, s).kind == L::Free, ckpt.version == wk.view.version,
      L::log' = L::putSlot(e, s, L::tombE(e)),
      L::everLog' = L::everLog.union(Set({ epoch: e, slot: s, entry: L::tombE(e) })),
      ckpt' = nc,
      workers' = workers.set(w, { ...wk, view: nc }),
      cevents' = cevents.union(Set("closed")),
      time' = time, lease' = lease, central' = central, stmts' = stmts, keepLog, keepSeries, keepDays, crows' = crows,
    }
  }

  // A tombstone (anyone's) is at the checkpoint: close the epoch there.
  action wSeeTomb(w: int, e: int): bool = {
    val wk = workers.get(w)
    val nc = { ...ckpt, version: ckpt.version + 1, closed: ckpt.closed.set(e, true) }
    all {
      wk.holds, wk.phase == WIdle, not(wk.view.closed.get(e)), wk.view.next.get(e) < L::MAX_SLOT,
      L::at(e, wk.view.next.get(e)).kind == L::Tomb, ckpt.version == wk.view.version,
      ckpt' = nc, workers' = workers.set(w, { ...wk, view: nc }),
      time' = time, lease' = lease, central' = central, stmts' = stmts, cevents' = cevents, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // The process dies: its statements may still be in flight.
  action wCrash(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds or wk.phase != WIdle,
      workers' = workers.set(w, { ...idle(wk), holds: false, inc: wk.inc + 1, seen: time }),
      cevents' = cevents.union(Set("workerCrash")),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // ---- GC ---------------------------------------------------------------------------------------
  // Delete epoch e's data slots below the checkpoint, once no request for
  // them is in flight (the request-lifetime delay). GC_TOMBS: also a closed
  // epoch's tombstone.
  // (`n` and `closed` are the checkpoint's, passed in; see wTomb.)
  action gc(e: int, n: int, closed: bool, doomed: Set[int]): bool = {
    all {
      n == ckpt.next.get(e), closed == ckpt.closed.get(e),
      doomed == L::slots.filter(s =>
        (s < n - (if (GC_KEEP) 1 else 0) and L::at(e, s).kind == L::Data) or (GC_TOMBS and closed and s == n and L::at(e, s).kind == L::Tomb)),
      doomed != Set(),
      L::inflight.forall(q => not(q.epoch == e and q.slot < n)),
      L::log' = L::log.set(e, L::log.get(e).keys().mapBy(s => if (doomed.contains(s)) L::FREE else L::log.get(e).get(s))),
      L::everLog' = L::everLog,
      cevents' = cevents.union(Set("gc")),
      keepLog, time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts,
      keepSeries, keepDays, crows' = crows,
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
    crows' = L::PAYLOADS.mapBy(_ => DAYS.mapBy(_ => 0)),
    day' = 0,
    eDay' = 1.to(2).mapBy(_ => 0),
    stmts' = Set(),
    cevents' = Set(),
    sEpoch' = 1, sCache' = Set(), sInflight' = Set(), sCommitted' = Set(), sPoints' = Set(),
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
      tick, newDay,
      wAcquire(w), wRenew(w), wLapse(w), wRelease(w), wCheck(w, e, k), wSend(w), wAdvance(w), wSeeTomb(w, e), wCrash(w),
      wTomb(w, e, workers.get(w).view.next.get(e)),
      all { stmts != Set(), nondet q = stmts.oneOf() all {
        nondet sub = q.objs.powerset().oneOf()
        any { cApply(q, sub), cDrop(q) } } },
      all {
        val n = ckpt.next.get(e)
        val c = ckpt.closed.get(e)
        gc(e, n, c, L::slots.filter(s =>
          (s < n - (if (GC_KEEP) 1 else 0) and L::at(e, s).kind == L::Data) or (GC_TOMBS and c and s == n and L::at(e, s).kind == L::Tomb)))
      },
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
  val wReleased: bool = cevents.contains("released")
  val wReleasedIngest: bool = wReleased and wAllIngested
  val wMidnight: bool = cevents.contains("midnight")
  // A copy on a later day than its original, both ingested once.
  val wCopyAcrossDays: bool = L::PAYLOADS.exists(p => central.get(p) == 1 and
    L::everLog.exists(x => x.entry.kind == L::Data and x.entry.payload == p and
      L::everLog.exists(y => y.entry.kind == L::Data and y.entry.payload == p and eDay.get(y.epoch) > eDay.get(x.epoch))))
  // A statement landed after its fence (inside the slack).
  val wLateLanding: bool = cevents.contains("lateLanding")

  // ---- scripted scenarios (s3InlineConsumer_test.sh) -------------------------------------
  // Each mutant's counterexample as a fixed run (`*BreaksTest`: run on the
  // mutant's instance, it ends with the invariant broken), and the same
  // prefix on the design (`*DesignTest`: the step the counterexample needs
  // is disabled there). Random simulation needs about a dozen specific steps
  // among ~30 kinds of action for these; the runs make them certain.

  // Writer e commits payload p at slot s (not acked).
  action lCommit(e: int, p: int): bool =
    lStartPush(e, p).then(lSend(e)).then(all { L::inflight != Set(), nondet q = L::inflight.oneOf() lApply(q) })
  // Worker w's statement lands whole.
  action cLand(w: int): bool = all {
    stmts.exists(q => q.worker == w), nondet q = stmts.filter(x => x.worker == w).oneOf() cApply(q, q.objs)
  }

  // Release: w1 sends its statement, then gives the lane back.
  run releasePrefix = init.then(lCommit(1, 1)).then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1))
  run releaseInFlightDesignTest = releasePrefix.expect(not(mayRelease(1)))
  run releaseInFlightBreaksTest =
    releasePrefix.then(wRelease(1)).then(wAcquire(2)).then(wCheck(2, 1, 1)).then(wSend(2)).then(cLand(2)).then(cLand(1))
      .expect(not(atMostOnce))
  // ... while after its statement has settled, a release is harmless.
  run releaseSettledDesignTest =
    releasePrefix.then((TTL + SLACK + 1).reps(_ => tick)).then(wRelease(1)).then(wAcquire(2)).then(wCheck(2, 1, 1)).then(wSend(2))
      .then(cLand(2)).expect(and { atMostOnce, stmts.forall(q => q.worker == 1 implies time > landBy(q)) })

  // The commit slack: w1's statement is still in flight when w2 takes the
  // lane over at the earliest (seen + TTL + MARGIN).
  run slackPrefix =
    init.then(lCommit(1, 1)).then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then((TTL + MARGIN).reps(_ => tick))
      .then(wAcquire(2)).then(wCheck(2, 1, 1)).then(wSend(2)).then(cLand(2))
  run keeperOverrunDesignTest = slackPrefix.expect(stmts.forall(q => q.worker == 1 implies time > landBy(q)))
  run keeperOverrunBreaksTest = slackPrefix.then(cLand(1)).expect(not(atMostOnce))

  // The copy horizon: payload 1 is committed on day 0 (epoch 1) and, not
  // acked, again on day 1 (epoch 2, after a restart); the day-0 copy is ingested first.
  run horizonPrefix =
    init.then(lCommit(1, 1)).then(newDay).then(lNewIncarnation(false)).then(lCommit(2, 1))
      .then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(wAdvance(1))
      .then(wCheck(1, 2, 1)).then(wSend(1))
  run noHorizonDesignTest = horizonPrefix.expect(and { stmts == Set(), atMostOnce })
  run noHorizonBreaksTest = horizonPrefix.then(cLand(1)).expect(not(atMostOnce))

  // The range from the data, not the clock: payload 1 (day 0) lands, its
  // worker dies before advancing, midnight passes, another worker takes over.
  run wallPrefix =
    init.then(lCommit(1, 1)).then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(wCrash(1))
      .then(newDay).then((TTL + MARGIN).reps(_ => tick)).then(wAcquire(2)).then(wCheck(2, 1, 1)).then(wSend(2))
  run wallRangeDesignTest = wallPrefix.expect(and { stmts == Set(), atMostOnce })
  run wallRangeBreaksTest = wallPrefix.then(cLand(2)).expect(not(atMostOnce))

  // GC and a writer that never learnt its PUT landed: it gives up on slot 0
  // (payload 2, answer lost) and switches to payload 1 for the same slot;
  // the late PUT of payload 2 lands and is ingested (checkpoint at 1).
  run gcPrefix =
    init.then(lStartPush(1, 2)).then(lSend(1)).then(lTimeout(1)).then(lSwitchPayload(1, 1))
      .then(all { L::inflight != Set(), nondet q = L::inflight.oneOf() lApply(q) })
      .then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(wAdvance(1))
  // The design keeps slot 0 (below the checkpoint): nothing to delete yet.
  run gcReopensDesignTest =
    gcPrefix.expect(and { ckpt.next.get(1) == 1, L::at(1, 0).kind == L::Data,
      L::slots.forall(s => (s < ckpt.next.get(1) - (if (GC_KEEP) 1 else 0)) implies L::at(1, s).kind != L::Data) })
  // Deleted, slot 0 takes the writer's next batch, create-only, below the checkpoint.
  run gcReopensBreaksTest =
    gcPrefix.then(gc(1, 1, false, Set(0))).then(lSend(1))
      .then(all { L::inflight != Set(), nondet q = L::inflight.oneOf() lApply(q) })
      .expect(not(neverSkipsCommitted))
}

// The design, in the full hostile environment.
module s3InlineConsumerDesign {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = true, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true).*
}
// The design in the mutants' narrower environment (must pass too).
module designQuiet {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true).*
}
// ... and with the mutants' short lease.
module designShortLease {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true).*
}
// ... and with writer faults (copies of a request in a later epoch, on a
// later day), the short lease, and no series lane. (Before GC_KEEP this
// instance broke neverSkipsCommitted: see the gcReopens mutant.)
module designCopies {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true).*
}
// The same at the MBT's lease (TTL 6: tests/mbt_s3inline_consumer.rs has one timing).
module designDays {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true).*
}
// Mutation: no lease time bound (and no server-side fence).
module noTimeBound {
  import s3InlineConsumer(TIME_BOUND = false, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true).*
}
// Mutation: the checkpoint advances past a statement without verifying it.
module noVerify {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = false, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true).*
}
// Mutation: GC deletes a closed epoch's tombstone (a zombie writer is needed).
module gcTombs {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = true, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true).*
}
// Mutation: the edge announces a series when it sends the series object.
module announceEarly {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = true,
    WRITER_FAULTS = false, SERIES = true, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true).*
}
// Mutation: a worker releases its lane while its statement may still land.
module releaseInFlight {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = false, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true).*
}
// Mutation: a commit outlives the margin (SLACK 2 > MARGIN 1).
module keeperOverrun {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 2, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true).*
}
// Mutation: the check reads only the objects' own day (no copy horizon).
module noHorizon {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 0, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true).*
}
// Mutation: the check reads today's partition by the worker's clock.
module wallRange {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = true,
    MAX_DAY = 1, GC_KEEP = true).*
}
// Mutation: GC deletes the slot below the checkpoint (as before 2026-09-26).
module gcReopens {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = false).*
}
