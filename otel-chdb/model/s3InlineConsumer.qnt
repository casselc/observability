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
 *  - The verify: after its statement is gone, the
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
 *  - A statement's commit can outlive its budget by SLACK: it lands by
 *    fence + BUDGET + SLACK. The design's rule is SLACK <= MARGIN (the code
 *    refuses to start otherwise). On a replicated central (2026-09-26,
 *    central-replicated/README.md "Keeper overrun") a commit whose Keeper
 *    request hung is resolved by the server's retry loop, up to the Keeper
 *    session timeout after the statement started (measured 19 s past a 10 s
 *    budget), and the statement may answer with an error (TIMEOUT_EXCEEDED)
 *    and land all the same. So "gone" is not "answered": an error answer
 *    ends a statement only if it was raised before anything was written;
 *    otherwise the worker waits for the statement's SLACK to pass. The model
 *    has no answers: a worker advances only once its statement is gone.
 *  - Partitions and the check's range. An object's rows carry its
 *    received_at, and central keeps rows per payload and received day; a
 *    check or verify of objects of day d reads only days d - HORIZON ..
 *    d + HORIZON (the code: the objects' own received time, widened by the
 *    horizon). The received day (2026-09-27, the edge's 8efc34f / 1fa3651,
 *    modelled edge-side by s3Inline.qnt's s3InlineReceived) is the day the
 *    request entered the edge's durable custody (the WAL write, the
 *    persistent queue's enqueue): `pDay(p)`, day 0 for the queued PAYLOADS.
 *    The incarnation that takes p stamps its objects with it, once
 *    (`oDay(e)(p)`, s3InlineReceived's `stamp`), so an edge replay in a later
 *    epoch, on a later day, lands in its original's partition. What still
 *    moves a copy to a later day is new custody of the same bytes: a sender
 *    that resends the request after its own outage (`senderResend`, the
 *    environment's RESENDS), which the edge receives and stamps anew.
 *    The environment makes every such copy within MAX_DAY days of its
 *    original; the design takes HORIZON >= MAX_DAY. That is an assumption
 *    about the senders, not something the consumer can enforce (the code's
 *    default horizon is 3 days), so the code watches for its violation: the
 *    horizon audit reports a payload in central on two days more than
 *    HORIZON apart (`auditLate`). The design never raises it
 *    (`auditSilent`), and under noHorizon every duplicate raises it
 *    (`dupAudited`). With edge replays only (no resends), HORIZON 0 is
 *    enough (`noHorizonReplays`).
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
 *  - ERROR_SETTLES: a worker takes an error answer for the end of its
 *    statement (the code before 2026-09-26): it verifies while the statement
 *    is still in flight, finds its objects missing, inserts them again, and
 *    the first statement lands too.
 *  - HORIZON < MAX_DAY (noHorizon): the check misses the original of a
 *    sender's resend received on a later day, and ingests the copy too.
 *  - RESTAMP: the edge before 8efc34f: an incarnation stamps its objects
 *    with the day it started (eDay), so an edge replay after midnight is on
 *    a later day than its original; with HORIZON < MAX_DAY it is ingested
 *    twice without any resend (`restamp`).
 *  - GC_KEEP = false: GC also deletes the slot below the checkpoint; a
 *    writer still unresolved there re-creates it with its next batch, below
 *    the checkpoint: committed, acked, never ingested (neverSkipsCommitted).
 *  - WALL_RANGE: the check reads today's partition by the worker's clock:
 *    a batch received on an earlier day, whose first statement landed, is
 *    checked again after midnight and inserted twice.
 *
 * Durations (2026-09-27; the DST CAST, ../STPA.md issues 13 and 14, and
 * ../AMBIGUITY.md S3; TEMPLATE.md "Durations"). A Quint step is atomic and
 * instantaneous; three environment flags give the steps that matter a
 * duration (all off in the instances above, which keep their meaning):
 *  - SLOW_OBS: a worker sees lease versions only through a LIST (wListReq)
 *    whose answer (wListAnswer) comes any number of steps later, the lease
 *    possibly rewritten meanwhile. The design dates a new version when the
 *    answer came. Mutation OBSERVE_AT_REQUEST (observeAtRequest, the code
 *    before 6b1a5f5): dated when the round started; a renewal the answer
 *    carries is backdated and a live lease is taken (noLiveTakeover,
 *    atMostOnce).
 *  - SCAN: the step's HEADs (wHead) each cost a tick of the holder's
 *    window, and a check covers only HEADed slots. The design stops a scan
 *    once the renewal is due and renews mid-scan, keeping its HEADs.
 *    Mutation SCAN_BY_TIME = false (renewOnlyAtInsert, the code before
 *    6b1a5f5): the whole backlog is scanned first, and renewals come only
 *    at a step's start or the insert; a backlog longer than the window
 *    lapses the lane before every insert, a livelock (workFitsWindow, and
 *    renewOnlyAtInsertBreaksTest).
 *  - CAS_AMBIG: the lease renewal and the checkpoint advance are a PUT
 *    If-Match sent (wRenewSend, wAdvanceSend), taken or refused by the
 *    store at a later step or lost (wCasLand, wCasLose; a dead process's
 *    write too), and answered 200, not at all, or 412 (wCasAnswer). The
 *    design (034f577) reads the doc back on anything but a 200, and keeps
 *    the lane if it holds its doc or still its previous version (a lost
 *    request: until 2026-09-27 the code dropped the lane then, an MBT
 *    finding; lostRenewalKeepsTest). Mutation
 *    OWN412_TAKEOVER (own412IsTakeover, the code before): a 412 means the
 *    lane is lost, also when it is object_store's retry meeting our own
 *    applied write; safe, but the lane idles until its lease expires
 *    (noOwnDrop, and own412IsTakeoverBreaksTest).
 * The take, release, checkpoint fence and tombstone close stay atomic.
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
  const ERROR_SETTLES: bool    // mutation: an error answer ends a statement that may still land
  const RESTAMP: bool          // mutation: an object's received day is its incarnation's start day, not its custody day
  const RESENDS: bool          // the environment: senders resend a request (new custody, a later day)
  // Durations (2026-09-27, the DST CAST, STPA.md issues 13 and 14; AMBIGUITY.md S3).
  const SLOW_OBS: bool         // the environment: a worker sees lease versions only through a LIST whose answer comes later
  const OBSERVE_AT_REQUEST: bool // mutation: a listed version is dated when the round started, not when the answer came
  const SCAN: bool             // the environment: each HEAD of a slot costs a tick of the holder's window
  const SCAN_BY_TIME: bool     // the design: stop a scan once the renewal is due, renew mid-scan (false: renewOnlyAtInsert)
  const CAS_AMBIG: bool        // the environment: a lease renewal / checkpoint write is request, effect, answer (lost, or 412 for our own write)
  const OWN412_TAKEOVER: bool  // mutation: a 412 on our own applied write is taken as a lost lane

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
  pure val UNSET: int = -1

  type WPhase = WIdle | WChecked | WSent
  type Obj = { epoch: int, slot: int, payload: int }
  type Ckpt = { version: int, leaseEpoch: int, next: int -> int, closed: int -> bool }
  type Worker = {
    holds: bool, inc: int, leaseEpoch: int, sent: int,
    // since when this process has seen the current lease version (a
    // restarted process has seen nothing before its restart)
    seen: int,
    view: Ckpt, phase: WPhase, epoch: int, objs: Set[Obj], pending: Set[Obj],
    // SLOW_OBS: the lease version this process last saw (NOLEASE: none), and
    // its discovery round: a LIST sent at listAt whose answer hasn't come yet
    obs: Lease, listing: bool, listAt: int,
    // SCAN: per epoch, how many slots from its checkpoint view it has HEADed
    // (kept across renewals, dropped with the lane), and the ticks of HEADs
    // since its lease version was written
    scanned: int -> int, work: int,
  }
  type Stmt = { worker: int, inc: int, objs: Set[Obj], fence: int }
  pure val DAYS: Set[int] = 0.to(MAX_DAY)
  type Lease = { owner: int, epoch: int, sent: int }
  // CAS_AMBIG: a lease renewal or checkpoint write (PUT If-Match) in flight.
  // CPending: sent; CApplied: it took effect (the answer may still be lost,
  // or be a 412 from object_store's retry of it); CRejected: its If-Match
  // failed (someone else wrote); CLost: it never applies.
  type CasKind = CRenew | CAdvance
  type CasSt = CPending | CApplied | CRejected | CLost
  type CasAns = A200 | ANone | A412
  type Write = { worker: int, inc: int, kind: CasKind, st: CasSt,
                 prev: Lease, lease: Lease, base: int, ckpt: Ckpt, epoch: int, from: int }
  type SObj = { epoch: int, r: int }

  // ---- state ----------------------------------------------------------------------
  var time: int
  var lease: Lease                 // owner 0: none yet
  var workers: int -> Worker
  var ckpt: Ckpt
  var central: int -> int          // inserts per payload
  var crows: int -> (int -> int)   // inserts per payload and day (the partitions)
  var day: int                     // the calendar (the workers' and the edges' clocks agree)
  var eDay: int -> int             // the day each epoch's incarnation started (RESTAMP: its objects' received day)
  var pDay: int -> int             // the day each payload entered the edge's custody (a resend: again, later)
  var oDay: int -> (int -> int)    // epoch -> payload -> the received day its objects carry (UNSET: none yet)
  var stmts: Set[Stmt]             // statements in flight
  var writes: Set[Write]           // CAS_AMBIG: lease / checkpoint writes in flight or unanswered
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
  // SLOW_OBS: a worker knows only what its LISTs showed it (`obs`, seen
  // since `seen`), and its take is a CAS on that version (the code's GET
  // then PUT If-Match: a newer version fails it).
  def takeable(w: int): bool =
    if (SLOW_OBS) workers.get(w).obs == lease and (lease.owner == 0 or time >= workers.get(w).seen + TTL + MARGIN)
    else lease.owner == 0 or time >= workers.get(w).seen + TTL + MARGIN
  // Every worker sees a new lease version at once (SLOW_OBS: only by listing).
  def seeAll(ws: int -> Worker): int -> Worker =
    if (SLOW_OBS) ws else ws.keys().mapBy(x => { ...ws.get(x), seen: time })
  pure val NOLEASE: Lease = { owner: -1, epoch: -1, sent: -1 }
  pure val NOSCAN: int -> int = 1.to(2).mapBy(_ => 0)
  // CAS_AMBIG: the worker waits for the answer to its write (one at a time).
  def blocked(w: int): bool = writes.exists(q => q.worker == w and q.inc == workers.get(w).inc)
  // SCAN (coord.rs: Held::renew_due): the renewal is due a third into the TTL.
  def renewDue(w: Worker): bool = time >= w.sent + TTL / 3
  // When may a holder renew? The design (SCAN_BY_TIME, worker.rs since
  // 6b1a5f5): any time, between lanes and mid-scan. renewOnlyAtInsert (the
  // code before): at a step's start (no HEAD since the last renewal) or
  // right before its insert, never in the middle of a scan.
  def mayRenewNow(wk: Worker): bool = not(SCAN) or SCAN_BY_TIME or wk.phase == WChecked or wk.work == 0
  // The HEADs of epoch e reach a slot that is not data (or the end).
  def scanDone(wk: Worker, e: int): bool = {
    val s = wk.view.next.get(e) + wk.scanned.get(e)
    s >= L::MAX_SLOT or L::at(e, s).kind != L::Data
  }
  // The check may cover k slots only once they have been HEADed; with the
  // count-bounded scan (not SCAN_BY_TIME) only once the whole backlog has.
  def scanReady(wk: Worker, e: int, k: int): bool =
    not(SCAN) or (wk.scanned.get(e) >= k and (SCAN_BY_TIME or scanDone(wk, e)))
  // May it HEAD the next slot of e? The design stops once the renewal is due.
  def mayHead(w: int, e: int): bool = {
    val wk = workers.get(w)
    val s = wk.view.next.get(e) + wk.scanned.get(e)
    and {
      SCAN, wk.holds, wk.phase == WIdle, not(blocked(w)), e <= L::lease, not(wk.view.closed.get(e)),
      s < L::MAX_SLOT, L::at(e, s).kind == L::Data, time < MAX_TIME,
      SCAN_BY_TIME implies not(renewDue(wk)),
    }
  }
  // HEADs above the new checkpoint are kept (a slot there never changes).
  pure def keptScan(wk: Worker, e: int, from: int, n: int): int -> int =
    wk.scanned.set(e, if (wk.scanned.get(e) > n - from) wk.scanned.get(e) - (n - from) else 0)
  // The worker's process state once it no longer holds the lane.
  pure def dropped(wk: Worker): Worker = { ...idle(wk), holds: false, scanned: NOSCAN, work: 0 }
  // Its lease is still the one it wrote (a CAS on it would succeed).
  def current(w: int): bool =
    lease == { owner: w, epoch: workers.get(w).leaseEpoch, sent: workers.get(w).sent }

  // An object's received day: what its incarnation stamped it with.
  def dayOf(o: Obj): int = oDay.get(o.epoch).get(o.payload)
  // Incarnation e takes p: it stamps p's objects once, with p's custody day
  // (the design) or the day it started (RESTAMP). (s3InlineReceived's `stamped`.)
  def stamped(e: int, p: int): int -> (int -> int) =
    if (oDay.get(e).get(p) != UNSET) oDay
    else oDay.set(e, oDay.get(e).set(p, if (RESTAMP) eDay.get(e) else pDay.get(p)))
  // The check's range for an object received on day d0 (plan.rs:
  // check_range), and what central shows of payload p there.
  def rangeLo(d0: int): int = if (WALL_RANGE) day else d0 - HORIZON
  def rangeHi(d0: int): int = if (WALL_RANGE) MAX_DAY else d0 + HORIZON
  def visible(p: int, d0: int): int =
    DAYS.filter(d => rangeLo(d0) <= d and d <= rangeHi(d0)).fold(0, (acc, d) => acc + crows.get(p).get(d))
  // A statement's objects can land until fence + BUDGET + SLACK; the worker
  // treats it as settled from fence + MARGIN + BUDGET + SLACK on (coord.rs:
  // Held::settled_by = sent + TTL + SLACK, the server clock allowed MARGIN behind).
  pure def landBy(q: Stmt): int = q.fence + BUDGET + SLACK
  pure def settledBy(q: Stmt): int = q.fence + MARGIN + BUDGET + SLACK
  // A worker verifies once its statement is gone. ERROR_SETTLES: also while
  // it is still in flight (the server answered with an error, and the worker
  // took that for the end of it).
  def mayAdvance(w: int): bool =
    ERROR_SETTLES or stmts.forall(q => not(q.worker == w and q.inc == workers.get(w).inc))
  def mayRelease(w: int): bool = {
    val wk = workers.get(w)
    wk.holds and current(w) and not(blocked(w)) and
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
  action keepCore = all {
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes,
    cevents' = cevents, crows' = crows,
  }
  action keepC = all { keepCore, keepDays }
  action keepDays = all { day' = day, eDay' = eDay, pDay' = pDay, oDay' = oDay }

  // ---- the log: s3Inline's writer, S3 and network ---------------------------------------
  // Taking p stamps its objects in this incarnation (once).
  action lStartPush(e: int, p: int): bool = all {
    L::startPush(e, p), oDay' = stamped(e, p), day' = day, eDay' = eDay, pDay' = pDay, keepCore, keepSeries,
  }
  action lSend(e: int): bool = all { L::send(e), keepC, keepSeries }
  action lTimeout(e: int): bool = all { L::timeout(e), keepC, keepSeries }
  action lResolve(e: int): bool = all { L::resolve(e), keepC, keepSeries }
  action lSwitchPayload(e: int, p: int): bool = all {
    L::switchPayload(e, p), oDay' = stamped(e, p), day' = day, eDay' = eDay, pDay' = pDay, keepCore, keepSeries,
  }
  action lReceive(e: int, r: L::Resp): bool = all { L::receive(e, r), keepC, keepSeries }
  // A new incarnation's epoch is named today (RESTAMP: and its objects received today).
  action lNewIncarnation(z: bool): bool = all {
    L::newIncarnation(z), eDay' = eDay.set(L::lease + 1, day), day' = day, pDay' = pDay, oDay' = oDay,
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes,
    cevents' = cevents, crows' = crows, keepSeries,
  }
  action lApply(q: L::Req): bool = all { L::apply(q), keepC, keepSeries }
  action lLose(q: L::Req): bool = all { L::lose(q), keepC, keepSeries }
  // A sender resends p (its own outage or timeout): the edge takes the same
  // bytes into custody again, today. An incarnation that takes p from now on
  // stamps it with today; objects already stamped keep their day.
  action senderResend(p: int): bool = all {
    L::queue.contains(p), day > pDay.get(p),
    pDay' = pDay.set(p, day), day' = day, eDay' = eDay, oDay' = oDay,
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes,
    cevents' = cevents.union(Set("resend")), crows' = crows, keepL, keepSeries,
  }

  // ---- time ---------------------------------------------------------------------------------
  action tick: bool = all {
    time < MAX_TIME, time' = time + 1,
    lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, cevents' = cevents,
    keepL, keepSeries, keepDays, crows' = crows,
  }

  // Midnight.
  action newDay: bool = all {
    day < MAX_DAY, day' = day + 1, eDay' = eDay, pDay' = pDay, oDay' = oDay,
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes,
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
      not(wk.holds), wk.phase == WIdle, takeable(w), not(blocked(w)),
      lease' = { owner: w, epoch: ne, sent: time },
      ckpt' = nc,
      workers' = seeAll(workers.set(w, { ...idle(wk), holds: true, leaseEpoch: ne, sent: time, view: nc,
                                          scanned: NOSCAN, work: 0 })),
      // liveTakeover: the version it replaces is younger than TTL + MARGIN
      // (its holder may still be inside its window): noLiveTakeover.
      cevents' = cevents.union(if (lease.owner != 0) Set("takeover") else Set()).union(
        if (lease.owner != 0 and time < lease.sent + TTL + MARGIN) Set("liveTakeover") else Set()),
      time' = time, central' = central, stmts' = stmts, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  action wRenew(w: int): bool = {
    val wk = workers.get(w)
    all {
      not(CAS_AMBIG), wk.holds, current(w), not(lapsed(wk)), not(blocked(w)), mayRenewNow(wk),
      lease' = { ...lease, sent: time },
      workers' = seeAll(workers.set(w, { ...wk, sent: time, work: 0 })),
      cevents' = cevents.union(if (wk.work > 0) Set("renewMidScan") else Set()),
      time' = time, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Past its window by its own clock: stop, without waiting to be told.
  action wLapse(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds, lapsed(wk), not(blocked(w)),
      workers' = workers.set(w, dropped(wk)),
      cevents' = cevents.union(Set("lapsed")),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
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
      workers' = seeAll(workers.set(w, dropped(wk))),
      cevents' = cevents.union(Set("released")),
      time' = time, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Check the next k slots of epoch e (all data) against central.
  action wCheck(w: int, e: int, k: int): bool = {
    val wk = workers.get(w)
    val s0 = wk.view.next.get(e)
    val objs = s0.to(s0 + k - 1).map(s => { epoch: e, slot: s, payload: L::at(e, s).payload })
    all {
      wk.holds, wk.phase == WIdle, mayStart(wk), e <= L::lease, not(wk.view.closed.get(e)),
      not(blocked(w)), scanReady(wk, e, k),
      s0 + k - 1 < L::MAX_SLOT, s0.to(s0 + k - 1).forall(s => L::at(e, s).kind == L::Data),
      // absent ones only (in the check's range), one object per payload (a copy rides on it)
      val absent = objs.filter(o => visible(o.payload, dayOf(o)) == 0)
      val pending = absent.filter(o => absent.forall(x => x.payload == o.payload implies x.slot >= o.slot))
      workers' = workers.set(w, { ...wk, phase: WChecked, epoch: e, objs: objs, pending: pending }),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, cevents' = cevents,
      keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Send the statement (the window is checked again right before).
  action wSend(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds, wk.phase == WChecked, mayStart(wk), not(blocked(w)),
      stmts' = if (wk.pending == Set()) stmts
               else stmts.union(Set({ worker: w, inc: wk.inc, objs: wk.pending, fence: fenceOf(wk) })),
      workers' = workers.set(w, { ...wk, phase: WSent }),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, cevents' = cevents, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // Central runs a statement: it starts only by its fence, and its commit
  // lands by fence + BUDGET + SLACK; any part of it may land. Rows go to
  // the partition of the day their object was received (its stamp).
  action cApply(q: Stmt, sub: Set[Obj]): bool = all {
    stmts.contains(q), time <= landBy(q), sub.subseteq(q.objs), sub != Set(),
    central' = central.keys().mapBy(p => central.get(p) + (if (sub.exists(o => o.payload == p)) 1 else 0)),
    crows' = crows.keys().mapBy(p => crows.get(p).keys().mapBy(d =>
      crows.get(p).get(d) + (if (sub.exists(o => o.payload == p and dayOf(o) == d)) 1 else 0))),
    keepDays,
    stmts' = stmts.exclude(Set(q)),
    cevents' = cevents.union(
      (if (sub != q.objs) Set("partial") else Set()).union(
      (if (time > q.fence) Set("lateLanding") else Set())).union(
       if (not(workers.get(q.worker).holds and workers.get(q.worker).inc == q.inc and current(q.worker)))
         Set("zombieLanded") else Set())),
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, writes' = writes, keepL, keepSeries,
  }

  // A statement fails whole, or is fenced by the server (a no-op).
  action cDrop(q: Stmt): bool = all {
    stmts.contains(q),
    stmts' = stmts.exclude(Set(q)),
    cevents' = cevents.union(if (time > q.fence) Set("fenced") else Set()),
    time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
  }

  // Once its statement is gone: count what landed and CAS the checkpoint past the done prefix.
  // (The verify's count, `done`, and the new position; wAdvanceSend uses them too.)
  def doneOf(wk: Worker): Set[Obj] = if (VERIFY) wk.objs.filter(o => visible(o.payload, dayOf(o)) >= 1) else wk.objs
  def advanceOf(wk: Worker): int = advanceTo(wk.view.next.get(wk.epoch), wk.objs, doneOf(wk))
  action wAdvance(w: int): bool = {
    val wk = workers.get(w)
    val n = advanceOf(wk)
    val nc = { ...ckpt, version: ckpt.version + 1, next: ckpt.next.set(wk.epoch, n) }
    all {
      not(CAS_AMBIG), wk.holds, wk.phase == WSent, mayAdvance(w), not(blocked(w)),
      if (ckpt.version == wk.view.version) all {
        ckpt' = nc,
        workers' = workers.set(w, { ...idle(wk), view: nc, scanned: keptScan(wk, wk.epoch, wk.view.next.get(wk.epoch), n) }),
        cevents' = cevents,
      } else all {
        ckpt' = ckpt,
        workers' = workers.set(w, dropped(wk)),
        cevents' = cevents.union(Set("casLost")),
      },
      time' = time, lease' = lease, central' = central, stmts' = stmts, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
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
      wk.holds, wk.phase == WIdle, not(blocked(w)), e < L::lease, not(wk.view.closed.get(e)), s == wk.view.next.get(e), s < L::MAX_SLOT,
      L::at(e, s).kind == L::Free, ckpt.version == wk.view.version,
      L::log' = L::putSlot(e, s, L::tombE(e)),
      L::everLog' = L::everLog.union(Set({ epoch: e, slot: s, entry: L::tombE(e) })),
      ckpt' = nc,
      workers' = workers.set(w, { ...wk, view: nc }),
      cevents' = cevents.union(Set("closed")),
      time' = time, lease' = lease, central' = central, stmts' = stmts, writes' = writes, keepLog, keepSeries, keepDays, crows' = crows,
    }
  }

  // A tombstone (anyone's) is at the checkpoint: close the epoch there.
  action wSeeTomb(w: int, e: int): bool = {
    val wk = workers.get(w)
    val nc = { ...ckpt, version: ckpt.version + 1, closed: ckpt.closed.set(e, true) }
    all {
      wk.holds, wk.phase == WIdle, not(blocked(w)), not(wk.view.closed.get(e)), wk.view.next.get(e) < L::MAX_SLOT,
      L::at(e, wk.view.next.get(e)).kind == L::Tomb, ckpt.version == wk.view.version,
      ckpt' = nc, workers' = workers.set(w, { ...wk, view: nc }),
      time' = time, lease' = lease, central' = central, stmts' = stmts, writes' = writes, cevents' = cevents, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // The process dies: its statements may still be in flight.
  action wCrash(w: int): bool = {
    val wk = workers.get(w)
    all {
      wk.holds or wk.phase != WIdle or wk.listing,
      // (a write it has in flight may still apply: wCasLand of an orphan)
      workers' = workers.set(w, { ...dropped(wk), inc: wk.inc + 1, seen: time, obs: NOLEASE, listing: false }),
      cevents' = cevents.union(Set("workerCrash")),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // ---- slow observation (SLOW_OBS) ------------------------------------------------------------
  // Discovery (worker.rs: heartbeat_and_leases; try_take's GET is the same
  // shape): the LIST goes out now ...
  action wListReq(w: int): bool = {
    val wk = workers.get(w)
    all {
      SLOW_OBS, not(wk.holds), not(wk.listing),
      workers' = workers.set(w, { ...wk, listing: true, listAt: time }),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, cevents' = cevents,
      keepL, keepSeries, keepDays, crows' = crows,
    }
  }
  // ... and its answer comes at any later step, time passed and the lease
  // possibly rewritten meanwhile. It shows the lease as the store holds it
  // now (served at the last moment: the case the dating matters for; an
  // answer served earlier shows an older version, dated later still, which
  // only delays a take). A version it hadn't seen is dated now, when the
  // answer came (the fix of 6b1a5f5); OBSERVE_AT_REQUEST dates it when the
  // round started, before a renewal the answer carries was even written.
  action wListAnswer(w: int): bool = {
    val wk = workers.get(w)
    all {
      SLOW_OBS, wk.listing,
      workers' = workers.set(w, { ...wk, listing: false, obs: lease,
        seen: if (lease == wk.obs) wk.seen else if (OBSERVE_AT_REQUEST) wk.listAt else time }),
      cevents' = cevents.union(if (lease.sent > wk.listAt) Set("listRace") else Set()),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes,
      keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // ---- step duration (SCAN) -------------------------------------------------------------------
  // HEAD the next slot of epoch e above the checkpoint view (worker.rs:
  // the step's scan): it takes a tick of the holder's own time, so a
  // backlog costs time against the lease window.
  action wHead(w: int, e: int): bool = {
    val wk = workers.get(w)
    all {
      mayHead(w, e),
      time' = time + 1,
      workers' = workers.set(w, { ...wk, scanned: wk.scanned.set(e, wk.scanned.get(e) + 1), work: wk.work + 1 }),
      lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes, cevents' = cevents,
      keepL, keepSeries, keepDays, crows' = crows,
    }
  }

  // ---- ambiguous lease / checkpoint writes (CAS_AMBIG) --------------------------------------------
  // The renewal's PUT If-Match goes out; the holder's window will count from
  // now (worker.rs: write_lease takes sent_ms before the PUT). The worker
  // waits for the answer (`blocked`).
  action wRenewSend(w: int): bool = {
    val wk = workers.get(w)
    all {
      CAS_AMBIG, wk.holds, current(w), not(lapsed(wk)), not(blocked(w)), mayRenewNow(wk),
      // (a tick after its last write: a lease record is (owner, epoch, sent),
      // and the read-back must tell two versions apart, as the code's beat does)
      time > wk.sent,
      writes' = writes.union(Set({ worker: w, inc: wk.inc, kind: CRenew, st: CPending, prev: lease,
                                   lease: { ...lease, sent: time }, base: 0, ckpt: C0, epoch: 0, from: 0 })),
      cevents' = cevents.union(if (wk.work > 0) Set("renewMidScan") else Set()),
      time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts,
      keepL, keepSeries, keepDays, crows' = crows,
    }
  }
  // The checkpoint's PUT If-Match (on the version it read) goes out, past
  // the verified done prefix.
  action wAdvanceSend(w: int): bool = {
    val wk = workers.get(w)
    val n = advanceOf(wk)
    all {
      CAS_AMBIG, wk.holds, wk.phase == WSent, mayAdvance(w), not(blocked(w)),
      writes' = writes.union(Set({ worker: w, inc: wk.inc, kind: CAdvance, st: CPending, prev: NOLEASE, lease: NOLEASE,
        base: wk.view.version, ckpt: { ...wk.view, version: wk.view.version + 1, next: wk.view.next.set(wk.epoch, n) },
        epoch: wk.epoch, from: wk.view.next.get(wk.epoch) })),
      time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents,
      keepL, keepSeries, keepDays, crows' = crows,
    }
  }
  // The store takes the write at some later step: its If-Match holds (it
  // applies) or not (someone else wrote: 412). A write whose process died
  // may still apply (a late effect); nobody hears its answer.
  action wCasLand(q: Write): bool = {
    val ok = if (q.kind == CRenew) lease == q.prev else ckpt.version == q.base
    val orphan = workers.get(q.worker).inc != q.inc
    all {
      writes.contains(q), q.st == CPending,
      lease' = if (ok and q.kind == CRenew) q.lease else lease,
      ckpt' = if (ok and q.kind == CAdvance) q.ckpt else ckpt,
      workers' = if (ok and q.kind == CRenew) seeAll(workers) else workers,
      writes' = writes.exclude(Set(q)).union(if (orphan) Set() else Set({ ...q, st: if (ok) CApplied else CRejected })),
      cevents' = cevents.union(if (orphan and ok) Set("orphanWrite") else Set()),
      time' = time, central' = central, stmts' = stmts, keepL, keepSeries, keepDays, crows' = crows,
    }
  }
  // ... or the request is lost: it never applies.
  action wCasLose(q: Write): bool = {
    val orphan = workers.get(q.worker).inc != q.inc
    all {
      writes.contains(q), q.st == CPending,
      writes' = writes.exclude(Set(q)).union(if (orphan) Set() else Set({ ...q, st: CLost })),
      time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, cevents' = cevents,
      keepL, keepSeries, keepDays, crows' = crows,
    }
  }
  // The answer (`ans`). An applied write is answered 200 (A200), or its
  // answer is lost (ANone: a timeout), or it is an error after the write
  // applied (a 5xx; a 409 on AWS) and object_store's retry, still on the
  // old ETag, gets 412 for our own write (A412). A write that did not apply
  // is answered 412 (another's write) or not at all. On anything but a 200
  // the design (034f577) reads the doc back and keeps the lane iff the
  // store holds the doc it wrote, or still the version its write was
  // conditional on (the request was lost: `prevKept`; the worker keeps its
  // old window and sends again. Before the MBT's finding, 2026-09-27, the
  // code dropped the lane then too). OWN412_TAKEOVER (the code before) took
  // any 412 for a lost lane: `ownDrop`, dropping a lane whose lease or
  // checkpoint is its own latest write; the lane then idles until that
  // lease expires (a stall, not a duplicate).
  action wCasAnswer(q: Write, ans: CasAns): bool = {
    val wk = workers.get(q.worker)
    val readBack = if (q.kind == CRenew) lease == q.lease else ckpt == q.ckpt
    // the store still holds the version the write was conditional on: the
    // worker's own previous one (a lost request)
    val prevHeld = if (q.kind == CRenew) lease == q.prev else ckpt.version == q.base
    val keep = if (ans == A200) true else if (ans == A412 and OWN412_TAKEOVER) false else readBack or prevHeld
    all {
      writes.contains(q), q.st != CPending, wk.inc == q.inc,
      ans == A200 implies q.st == CApplied, q.st == CLost implies ans == ANone,
      writes' = writes.exclude(Set(q)),
      workers' = workers.set(q.worker,
        if (not(keep)) dropped(wk)
        else if (not(readBack) and ans != A200) wk
        else if (q.kind == CRenew) { ...wk, sent: q.lease.sent, work: 0 }
        else { ...idle(wk), view: q.ckpt, scanned: keptScan(wk, q.epoch, q.from, q.ckpt.next.get(q.epoch)) }),
      cevents' = cevents.union(
        if (not(keep) and readBack) Set("ownDrop")
        else if (not(keep) and prevHeld) Set("prevDrop")
        else if (not(keep)) Set("casLost")
        else if (ans == A412) Set("own412Kept")
        else if (ans != A200 and not(readBack)) Set("prevKept") else Set()),
      time' = time, lease' = lease, ckpt' = ckpt, central' = central, stmts' = stmts,
      keepL, keepSeries, keepDays, crows' = crows,
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
      keepLog, time' = time, lease' = lease, workers' = workers, ckpt' = ckpt, central' = central, stmts' = stmts, writes' = writes,
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
                                     phase: WIdle, epoch: 0, objs: Set(), pending: Set(),
                                     obs: NOLEASE, listing: false, listAt: 0, scanned: NOSCAN, work: 0 }),
    ckpt' = C0,
    central' = L::PAYLOADS.mapBy(_ => 0),
    crows' = L::PAYLOADS.mapBy(_ => DAYS.mapBy(_ => 0)),
    day' = 0,
    eDay' = 1.to(2).mapBy(_ => 0),
    pDay' = L::PAYLOADS.mapBy(_ => 0),
    oDay' = 1.to(2).mapBy(_ => L::PAYLOADS.mapBy(_ => UNSET)),
    stmts' = Set(),
    writes' = Set(),
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
      wListReq(w), wListAnswer(w), wHead(w, e), wRenewSend(w), wAdvanceSend(w),
      all { writes != Set(), nondet q = writes.oneOf() all {
        nondet ans = Set(A200, ANone, A412).oneOf()
        any { wCasLand(q), wCasLose(q), wCasAnswer(q, ans) } } },
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
      all { RESENDS, senderResend(p) },
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

  // No worker takes a lease version younger than TTL + MARGIN (by the time
  // it was written): its holder may still be inside its window. (Holds by
  // construction when every worker sees a version when it is written; with
  // SLOW_OBS it rests on dating each observation at its answer.)
  val noLiveTakeover: bool = not(cevents.contains("liveTakeover"))

  val safety: bool = and {
    noLiveTakeover,
    atMostOnce, onlyCommittedIngested, neverSkipsCommitted, noCommitAfterClose,
    ackedImpliesCommitted, noPayloadLost, L::epochNoDuplicatePayload, announcedOnlyAfterCommit,
  }

  // The horizon audit (otap-rs consumer/audit.rs), at day granularity: a
  // payload in central on two days more than HORIZON apart, which no check
  // could have seen together. The code confirms it by repeated rows; here a
  // payload's inserts are its rows. `auditSilent`: the design raises no
  // alarm while copies stay within the horizon. `dupAudited`: under
  // noHorizon (the horizon's assumption broken), every duplicate is one the
  // audit reports; a same-day duplicate would not be, and would be a bug
  // of the check itself.
  val auditLate: bool = L::PAYLOADS.exists(p => DAYS.exists(d1 => DAYS.exists(d2 =>
    d2 - d1 > HORIZON and crows.get(p).get(d1) > 0 and crows.get(p).get(d2) > 0)))
  val auditSilent: bool = not(auditLate)
  val dupAudited: bool = atMostOnce or auditLate

  // Progress (SCAN, CAS_AMBIG). Quint simulation checks state invariants on
  // finite runs, not liveness, so each liveness hazard is checked as the
  // state property whose violation is its mechanism, and pinned by a
  // scripted run (`renewOnlyAtInsertBreaksTest`, `own412IsTakeoverBreaksTest`):
  //  - workFitsWindow: the holder's own work (HEADs) since its lease version
  //    was written still leaves room to start a statement in the window.
  //    Pauses (ticks) can still end a window; work alone never does. Broken,
  //    a backlog longer than the window is scanned, the window ends before
  //    the insert, the lane lapses and is rescanned: a livelock (issue 14).
  //  - noOwnDrop: no worker gives up a lane whose lease or checkpoint holds
  //    its own latest write, applied or (a lost request) not: its previous
  //    one. Broken, the lane idles until that lease expires.
  val workFitsWindow: bool = WORKERS.forall(w => workers.get(w).holds implies workers.get(w).work + BUDGET <= TTL - MARGIN)
  val noOwnDrop: bool = not(cevents.contains("ownDrop")) and not(cevents.contains("prevDrop"))
  val slowSafety: bool = and { safety, workFitsWindow, noOwnDrop }

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
      L::everLog.exists(y => y.entry.kind == L::Data and y.entry.payload == p and
        dayOf({ epoch: y.epoch, slot: y.slot, payload: p }) > dayOf({ epoch: x.epoch, slot: x.slot, payload: p }))))
  // An edge replay in a later epoch, on a later calendar day, that kept its
  // original's received day, both committed; the payload in central once.
  val wReplayKeepsDay: bool = L::PAYLOADS.exists(p => central.get(p) == 1 and
    L::everLog.exists(x => x.entry.kind == L::Data and x.entry.payload == p and
      L::everLog.exists(y => y.entry.kind == L::Data and y.entry.payload == p and
        eDay.get(y.epoch) > eDay.get(x.epoch) and oDay.get(y.epoch).get(p) == oDay.get(x.epoch).get(p))))
  // A statement landed after its fence (inside the slack).
  val wLateLanding: bool = cevents.contains("lateLanding")
  // A LIST answer carried a lease version written after the LIST was sent.
  val wListRace: bool = cevents.contains("listRace")
  // A renewal between HEADs of a scan (the design's, SCAN_BY_TIME).
  val wRenewMidScan: bool = cevents.contains("renewMidScan")
  // A 412 for our own applied write, read back and kept.
  val wOwn412Kept: bool = cevents.contains("own412Kept")
  // A worker kept a lane whose lease or checkpoint is still the version its
  // write was conditional on (its own previous one): that write's request
  // was lost. (The code before 2026-09-27 dropped it: `prevDrop`.)
  val wPrevKept: bool = cevents.contains("prevKept")
  // A dead process's write applied after it died.
  val wOrphanWrite: bool = cevents.contains("orphanWrite")
  // A batch ingested after a LIST answer carried a version written after its request.
  val wSlowIngest: bool = wIngested and wListRace

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

  // An edge replay: payload 1 is committed on day 0 (epoch 1) and, not
  // acked, again on day 1 (epoch 2, after a restart: the buffer replays
  // it); the day-0 copy is ingested first. The replay keeps its custody day
  // (0): the check finds the original with any horizon. RESTAMP: the replay
  // is stamped day 1, and a check limited to its own day misses the original.
  run replayPrefix =
    init.then(lCommit(1, 1)).then(newDay).then(lNewIncarnation(false)).then(lCommit(2, 1))
      .then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(wAdvance(1))
      .then(wCheck(1, 2, 1)).then(wSend(1))
  run restampDesignTest = replayPrefix.expect(and { stmts == Set(), atMostOnce, oDay.get(2).get(1) == 0 })
  run restampBreaksTest = replayPrefix.then(cLand(1)).expect(and { not(atMostOnce), auditLate })
  // The copy horizon: the same, but on day 1 the sender resends payload 1
  // (new custody) before the restart, so the copy carries day 1.
  run horizonPrefix =
    init.then(lCommit(1, 1)).then(newDay).then(senderResend(1)).then(lNewIncarnation(false)).then(lCommit(2, 1))
      .then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(wAdvance(1))
      .then(wCheck(1, 2, 1)).then(wSend(1))
  run noHorizonDesignTest = horizonPrefix.expect(and { stmts == Set(), atMostOnce, oDay.get(2).get(1) == 1 })
  run noHorizonBreaksTest = horizonPrefix.then(cLand(1)).expect(and { not(atMostOnce), auditLate })

  // The range from the data, not the clock: payload 1 (day 0) lands, its
  // worker dies before advancing, midnight passes, another worker takes over.
  run wallPrefix =
    init.then(lCommit(1, 1)).then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(wCrash(1))
      .then(newDay).then((TTL + MARGIN).reps(_ => tick)).then(wAcquire(2)).then(wCheck(2, 1, 1)).then(wSend(2))
  run wallRangeDesignTest = wallPrefix.expect(and { stmts == Set(), atMostOnce })
  run wallRangeBreaksTest = wallPrefix.then(cLand(2)).expect(not(atMostOnce))

  // An error answer that isn't the end: w1's statement is in flight (its
  // commit still resolving in Keeper, the server answered TIMEOUT_EXCEEDED).
  run errorPrefix = init.then(lCommit(1, 1)).then(wAcquire(1)).then(wCheck(1, 1, 1)).then(wSend(1))
  run errorSettlesDesignTest = errorPrefix.expect(not(mayAdvance(1)))
  run errorSettlesBreaksTest =
    // (a tick and a renewal first: the retry is a new statement, with a new fence)
    errorPrefix.then(wAdvance(1)).then(tick).then(wRenew(1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(cLand(1))
      .expect(not(atMostOnce))

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

  // ---- durations (2026-09-27; STPA.md issues 13 and 14, AMBIGUITY.md S3) ------------------
  // Helpers valid in any instance: a HEAD costs the tick a pause would;
  // a renewal / checkpoint write is one step or request, effect and 200.
  action headOrTick(w: int, e: int): bool = if (SCAN) wHead(w, e) else tick
  action landOf(w: int): bool = all {
    writes.exists(q => q.worker == w and q.st == CPending),
    nondet q = writes.filter(x => x.worker == w and x.st == CPending).oneOf() wCasLand(q)
  }
  action answerOf(w: int, a: CasAns): bool = all {
    writes.exists(q => q.worker == w and q.st != CPending),
    nondet q = writes.filter(x => x.worker == w and x.st != CPending).oneOf() wCasAnswer(q, a)
  }
  action renewRun(w: int): bool = if (CAS_AMBIG) wRenewSend(w).then(landOf(w)).then(answerOf(w, A200)) else wRenew(w)
  action advanceRun(w: int): bool = if (CAS_AMBIG) wAdvanceSend(w).then(landOf(w)).then(answerOf(w, A200)) else wAdvance(w)
  action listRun(w: int): bool = wListReq(w).then(wListAnswer(w))
  // Writer e commits p and hears its 200 (so it moves to its next slot).
  action lCommitAck(e: int, p: int): bool =
    lCommit(e, p).then(all { L::responses != Set(), nondet q = L::responses.oneOf() lReceive(e, q) })

  // Slow observation (SLOW_OBS, TTL 6): w2's LIST goes out at 0; w1 renews
  // at 2; the answer, arriving after the renewal, carries it. w1 inserts
  // payload 1 at 6 (fence 6, lands by 8). At 7 = 0 + TTL + MARGIN a worker
  // that dated the renewal at its round's start (0) takes the lane.
  run observePrefix =
    init.then(lCommit(1, 1)).then(listRun(1)).then(wAcquire(1))
      .then(wListReq(2)).then(tick).then(tick)
      .then(renewRun(1))
      .then(wListAnswer(2))
      .then(headOrTick(1, 1)).then(3.reps(_ => tick))
      .then(wCheck(1, 1, 1)).then(wSend(1))
      .then(tick)
  // Dated at the answer (2): no take before 2 + 7 = 9, after w1's statement can land.
  run observeDesignTest = observePrefix.expect(and {
    not(takeable(2)), workers.get(2).seen == 2, stmts.forall(q => landBy(q) < 2 + TTL + MARGIN) })
  run observeAtRequestBreaksTest =
    observePrefix.then(wAcquire(2)).then(headOrTick(2, 1)).then(wCheck(2, 1, 1)).then(wSend(2))
      .then(cLand(1)).then(cLand(2))
      .expect(and { not(atMostOnce), not(noLiveTakeover) })

  // Step duration (SCAN, TTL 3: the window is 2 ticks, the renewal due
  // after 1): a backlog of two slots. The design HEADs one, stops (renewal
  // due), inserts it, renews, HEADs the next and inserts it.
  run scanPrefix = init.then(lCommitAck(1, 1)).then(lCommit(1, 2)).then(listRun(1)).then(wAcquire(1))
  run scanDesignTest =
    scanPrefix.then(wHead(1, 1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(advanceRun(1))
      .then(renewRun(1)).then(wHead(1, 1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(advanceRun(1))
      .expect(and { ckpt.next.get(1) == 2, central.get(1) == 1, central.get(2) == 1, workFitsWindow, time == 2 })
  // renewOnlyAtInsert: the scan covers the whole backlog before the insert
  // and no renewal comes in between; two HEADs end the window, the lane
  // lapses, its HEADs go with it. Retaken (after TTL + MARGIN on the lease
  // it lapsed), the same again: every round of this, the most eager
  // schedule there is, ends with the checkpoint where it was.
  action starveRound: bool = wHead(1, 1).then(wHead(1, 1)).then(wLapse(1))
  run renewOnlyAtInsertBreaksTest =
    scanPrefix.then(wHead(1, 1)).then(wHead(1, 1)).expect(and { not(workFitsWindow), not(mayStart(workers.get(1))) })
      .then(wLapse(1))
      .then(listRun(1)).then(4.reps(_ => tick)).then(wAcquire(1)).then(starveRound)
      .then(listRun(1)).then(4.reps(_ => tick)).then(wAcquire(1)).then(starveRound)
      .expect(and { ckpt.next.get(1) == 0, central.get(1) == 0, time == 14, safety })

  // Own-write conflict on retry (CAS_AMBIG, TTL 3): w1 renews at 1; the PUT
  // applies, its answer is a 5xx, object_store retries on the old ETag and
  // gets 412. The design reads the lease back, finds its own, and keeps the
  // lane; own412IsTakeover drops it, and nobody may take it before its own
  // renewal is TTL + MARGIN old: the lane idles.
  run own412Prefix =
    init.then(lCommit(1, 1)).then(listRun(1)).then(wAcquire(1)).then(listRun(2)).then(tick)
      .then(wRenewSend(1)).then(landOf(1)).then(answerOf(1, A412))
  run own412DesignTest = own412Prefix.then(headOrTick(1, 1)).then(wCheck(1, 1, 1)).then(wSend(1)).then(cLand(1)).then(advanceRun(1))
    .expect(and { workers.get(1).holds, current(1), noOwnDrop, ckpt.next.get(1) == 1, central.get(1) == 1 })
  // A lost renewal request (the PUT never applies; no answer): the read-back
  // finds the previous version, the worker's own, still current. The design
  // keeps the lane on its old window and renews again. (The code before
  // 2026-09-27 dropped it, and the lane idled until that version was TTL +
  // MARGIN old: the same stall as own412IsTakeover's. A finding of the MBT,
  // designSlow seed 0x29e8aebd.)
  run lostRenewalKeepsTest =
    init.then(lCommit(1, 1)).then(listRun(1)).then(wAcquire(1)).then(listRun(2)).then(tick)
      .then(wRenewSend(1)).then(all { writes != Set(), nondet q = writes.oneOf() wCasLose(q) }).then(answerOf(1, ANone))
      .expect(and { workers.get(1).holds, workers.get(1).sent == 0, lease.owner == 1, wPrevKept, noOwnDrop, safety })
      .then(renewRun(1))
      .expect(and { workers.get(1).holds, workers.get(1).sent == 1, lease.sent == 1, noOwnDrop, safety })
  run own412IsTakeoverBreaksTest =
    own412Prefix.expect(and { not(workers.get(1).holds), lease.owner == 1, not(noOwnDrop) })
      .then(listRun(1)).then(listRun(2)).then(3.reps(_ => tick))
      .expect(and { WORKERS.forall(x => not(workers.get(x).holds) and not(takeable(x))), ckpt.next.get(1) == 0, safety })
}

// The design, in the full hostile environment.
module s3InlineConsumerDesign {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = true, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// The design in the mutants' narrower environment (must pass too).
module designQuiet {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// ... and with the mutants' short lease.
module designShortLease {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// ... and with writer faults (copies of a request in a later epoch, on a
// later day), the short lease, and no series lane. (Before GC_KEEP this
// instance broke neverSkipsCommitted: see the gcReopens mutant.)
module designCopies {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// The same at the MBT's lease (TTL 6: tests/mbt_s3inline_consumer.rs has one timing).
module designDays {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: no lease time bound (and no server-side fence).
module noTimeBound {
  import s3InlineConsumer(TIME_BOUND = false, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: the checkpoint advances past a statement without verifying it.
module noVerify {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = false, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: GC deletes a closed epoch's tombstone (a zombie writer is needed).
module gcTombs {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = true, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: the edge announces a series when it sends the series object.
module announceEarly {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = true,
    WRITER_FAULTS = false, SERIES = true, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: a worker releases its lane while its statement may still land.
module releaseInFlight {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = false, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: a commit outlives the margin (SLACK 2 > MARGIN 1).
module keeperOverrun {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 2, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: the check reads only the objects' own day (no copy horizon),
// and senders resend requests (new custody, a later day).
module noHorizon {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 0, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: the check reads today's partition by the worker's clock.
module wallRange {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = true,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: GC deletes the slot below the checkpoint (as before 2026-09-26).
module gcReopens {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = false, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: an error answer ends a statement that may still land (the code
// before 2026-09-26 took any error answer for the end of its statement).
module errorSettles {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = true,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// The design needs no horizon for edge replays: without sender resends every
// copy of a request carries its custody day, the original's partition.
module noHorizonReplays {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 0, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// Mutation: the edge before 8efc34f: a replay is stamped with its
// incarnation's day, and with no copy horizon it is ingested twice (no
// resend needed).
module restamp {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 0, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = true, RESENDS = false,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false).*
}
// ---- durations (2026-09-27): the DST CAST's gaps, STPA.md issues 13 and 14,
// and AMBIGUITY.md S3's own-write 412. Every instance below has all three
// behaviours on (slow observation, step duration, ambiguous lease and
// checkpoint writes); each mutant differs from its design twin by one flag.
// The design with the slow behaviours, writer faults and days (designDays'
// environment; the MBT replays it).
module designSlow {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 1, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = true, OBSERVE_AT_REQUEST = false, SCAN = true, SCAN_BY_TIME = true, CAS_AMBIG = true, OWN412_TAKEOVER = false).*
}
// ... in the mutants' quiet environment, at TTL 6 (observeAtRequest's twin) ...
module designSlowQuiet {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = true, OBSERVE_AT_REQUEST = false, SCAN = true, SCAN_BY_TIME = true, CAS_AMBIG = true, OWN412_TAKEOVER = false).*
}
// ... and at TTL 3 (renewOnlyAtInsert's and own412IsTakeover's twin).
module designSlowShort {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = true, OBSERVE_AT_REQUEST = false, SCAN = true, SCAN_BY_TIME = true, CAS_AMBIG = true, OWN412_TAKEOVER = false).*
}
// Mutation (the code before 6b1a5f5): a listed lease version is dated when
// the discovery round started. A renewal the answer carries is backdated,
// and the lane is taken while its holder can still insert.
module observeAtRequest {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 6, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = true, OBSERVE_AT_REQUEST = true, SCAN = true, SCAN_BY_TIME = true, CAS_AMBIG = true, OWN412_TAKEOVER = false).*
}
// Mutation (the code before 6b1a5f5): the scan covers the whole backlog
// (count-bounded) and the lease is renewed only at a step's start or the
// insert. Safe, but a backlog longer than the window livelocks the lane.
module renewOnlyAtInsert {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = true, OBSERVE_AT_REQUEST = false, SCAN = true, SCAN_BY_TIME = false, CAS_AMBIG = true, OWN412_TAKEOVER = false).*
}
// Mutation (the code before 034f577): a 412 on a lease or checkpoint write
// means the lane is lost, even when it is object_store's retry meeting our
// own applied write. Safe, but the lane idles until the lease expires.
module own412IsTakeover {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = false, SERIES = false, TTL = 3, SLACK = 1, RELEASE_GUARD = true, HORIZON = 1, WALL_RANGE = false,
    MAX_DAY = 0, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = false,
    SLOW_OBS = true, OBSERVE_AT_REQUEST = false, SCAN = true, SCAN_BY_TIME = true, CAS_AMBIG = true, OWN412_TAKEOVER = true).*
}
