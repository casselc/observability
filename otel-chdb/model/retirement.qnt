// -*- mode: Bluespec; -*-
/**
 * retirement: taking a dead lane out of `complete_through` without making it
 * unsound (DECISIONS.md D35, FORMAT.md §3.1). completeness.qnt's lane rule,
 * one cluster, one reader (central), and the lifecycle of the publisher
 * behind a lane: an orderly shutdown, an unorderly death with custody left on
 * its volume (the PVC), the volume deleted, the volume adopted by a new
 * incarnation of the same producer (StatefulSet ordinal reuse), and an
 * operator's retirement.
 *
 * Today a lane that stops advancing holds its cluster's complete_through at
 * its last low forever (it is paged as stale, D29 limit 4): from S3 an idle
 * lane, a slow one and a dead one with a full buffer look the same. The
 * design lets a lane stop counting only on a proof that its custody is
 * empty, and bounds what may still come from it:
 *
 *  - **Orderly close.** The publisher stops taking requests, drains its
 *    buffer (every request committed and acked), and only then commits a
 *    last slot, `oscope-kind: close`, whose low is the close time: nothing
 *    is in custody, nothing will enter it (the process is exiting), and no
 *    PUT is in flight (every one was acked). Once the lane's holder has
 *    passed the close slot (it and every slot before it ingested), the lane
 *    is retired at R = the close's low.
 *  - **Operator retirement.** For a publisher that died without a close:
 *    the operator's evidence is that the volume is deleted (its custody is
 *    lost, acknowledged: `lost`) and no PUT of the dead process can still
 *    land (the pod gone for longer than a request lifetime, as GC's
 *    zombie_ms; the consumer then tombstones the epoch's head). The lane is
 *    retired at R = the time of the retirement.
 *  - A retired lane counts as +inf in the minimum, until an object of a
 *    later epoch appears (a new incarnation's birth): then it counts again,
 *    from its birth. Every request of a new incarnation that was received
 *    after its birth is above anything published before the birth was
 *    listed (completeness.qnt's "born after the LIST").
 *  - **Quarantine.** An object of a retired lane whose received_at is below
 *    R is never ingested into central: it can only be custody the
 *    retirement said did not exist (an operator who retired a lane whose
 *    volume was kept after all, and a new pod adopted it and replayed it).
 *    Ingesting it would put rows below a complete_through already published
 *    and read (`lateBelow`): answers at an earlier basis change, alert
 *    windows already evaluated OK were wrong. It is reported instead
 *    (paged), for an operator to admit knowingly.
 *
 * Properties: `completeSound` (every request below the published value is
 * ingested, lost with its volume, or quarantined), `noLateBelow` (nothing
 * is ever ingested below the value published before it: the published
 * value is final), `quarantineOnlyOnMistake` (the quarantine stays empty
 * unless an operator retired a lane whose custody was not empty) and
 * `wmBounded`. Mutants:
 *  - RETIRE_STALE (`retireStale`): the reader retires a lane that stopped
 *    advancing, without a close or an operator's evidence;
 *  - CLOSE_UNDRAINED (`closeUndrained`): the close is committed with
 *    requests still in custody;
 *  - QUARANTINE = false (`ingestBelow`): a retired lane's objects below R
 *    are ingested like any other;
 *  - UNRETIRE = false (`staysRetired`): a retired lane stays out of the
 *    minimum when a new incarnation writes to it;
 *  - ZOMBIE_WAIT = false (`retireInFlight`): the operator retires while a
 *    PUT of the dead process is still in flight;
 *  - OP_EVIDENCE = false (`opMistake`): not a mutant of the reader but the
 *    operator being wrong (the volume kept): safe, and the quarantine is
 *    where its requests go.
 */
module retirement {
  const LANES: Set[int]
  const RETIRE_STALE: bool
  const CLOSE_UNDRAINED: bool
  const QUARANTINE: bool
  const UNRETIRE: bool
  const ZOMBIE_WAIT: bool
  const OP_EVIDENCE: bool

  pure val MAX_T: int = 6
  pure val MAX_REQ: int = 3
  pure val MAX_EPOCH: int = 3
  pure val MAX_SEQ: int = 4
  pure val NEVER: int = 1000
  pure val NOT_RETIRED: int = -1

  type Kind = Data | Beat | Close
  type Obj = { lane: int, epoch: int, seq: int, kind: Kind, req: int, r: int, low: int }
  type Req = { id: int, lane: int, r: int }
  // Up: running; Closed: exited after an orderly close; Dead: gone without one
  type Life = Up | Closed | Dead
  // `since`: when `life` last changed (a death's time).
  type Writer = { epoch: int, seq: int, busy: bool, cur: Obj, life: Life, since: int }

  var now: int
  var nextId: int
  var buffer: int -> Set[Req]      // lane -> custody (on the publisher's volume)
  var history: Set[Req]
  var lost: Set[int]               // request ids destroyed with a deleted volume
  var writers: int -> Writer
  var puts: Set[Obj]
  var objs: Set[Obj]
  var ing: Set[Obj]                // ingested by central
  var quar: Set[Obj]               // quarantined (never ingested)
  var retiredAt: int -> int        // lane -> R (NOT_RETIRED)
  var retEpoch: int -> int         // lane -> the epoch it was retired in
  var wm: int                      // published complete_through (running max)
  var events: Set[str]

  pure val NOOBJ: Obj = { lane: 0, epoch: 0, seq: 0, kind: Beat, req: 0, r: 0, low: 0 }
  pure def imax(a: int, b: int): int = if (a >= b) a else b
  pure def imin(a: int, b: int): int = if (a <= b) a else b
  pure def before(p: Obj, o: Obj): bool = p.epoch < o.epoch or (p.epoch == o.epoch and p.seq < o.seq)
  pure def birth(l: int): Obj = { lane: l, epoch: 1, seq: 0, kind: Beat, req: 0, r: 0, low: 0 }

  def custodyFloor(l: int, except: int): int = buffer.get(l).filter(x => x.id != except).fold(now, (m, x) => imin(m, x.r))
  def passed(o: Obj): bool = ing.contains(o) or quar.contains(o)
  def retired(l: int): bool = retiredAt.get(l) != NOT_RETIRED
  // A retired lane counts again once a later incarnation wrote to it.
  def reborn(l: int): bool = UNRETIRE and objs.exists(o => o.lane == l and o.epoch > retEpoch.get(l))
  def counts(l: int): bool = not(retired(l)) or reborn(l)
  // The lane's watermark as implemented (completeness.qnt PENDING): the
  // highest low passed, capped by the lowest received_at of the data not
  // passed yet. Quarantined objects are passed (they will never be ingested).
  def laneWm(l: int): int = {
    val lo = objs.filter(o => o.lane == l)
    imin(lo.filter(o => passed(o)).fold(-1, (m, o) => imax(m, o.low)),
         lo.filter(o => not(passed(o)) and o.kind == Data).fold(NEVER, (m, o) => imin(m, o.r)))
  }
  def computedWm: int = LANES.filter(l => counts(l)).fold(now, (m, l) => imin(m, laneWm(l)))
  // A request is accounted for below the published value.
  def ingestedReq(id: int): bool = ing.exists(o => o.kind == Data and o.req == id)
  def quarantinedReq(id: int): bool = quar.exists(o => o.req == id)

  action keepEdge = all { nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, writers' = writers, puts' = puts, objs' = objs }
  action keepRead = all { ing' = ing, quar' = quar, retiredAt' = retiredAt, retEpoch' = retEpoch, wm' = wm }

  // ---- the publisher -------------------------------------------------------------------
  action receive(l: int): bool = {
    val q = { id: nextId, lane: l, r: now }
    all {
      writers.get(l).life == Up, nextId <= MAX_REQ,
      buffer' = buffer.setBy(l, b => b.union(Set(q))), history' = history.union(Set(q)), nextId' = nextId + 1,
      now' = now, lost' = lost, writers' = writers, puts' = puts, objs' = objs, events' = events, keepRead,
    }
  }
  action send(l: int, q: Req): bool = {
    val w = writers.get(l)
    val o = { lane: l, epoch: w.epoch, seq: w.seq, kind: Data, req: q.id, r: q.r, low: custodyFloor(l, q.id) }
    all {
      w.life == Up, not(w.busy), w.seq < MAX_SEQ, buffer.get(l).contains(q),
      puts' = puts.union(Set(o)), writers' = writers.set(l, { ...w, busy: true, cur: o }),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, objs' = objs, events' = events, keepRead,
    }
  }
  action land(o: Obj): bool = all {
    puts.contains(o), puts' = puts.exclude(Set(o)), objs' = objs.union(Set(o)),
    events' = events.union(if (writers.get(o.lane).life != Up or o.epoch < writers.get(o.lane).epoch) Set("zombieLanded") else Set()),
    now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, writers' = writers, keepRead,
  }
  action lose(o: Obj): bool = all {
    puts.contains(o), puts' = puts.exclude(Set(o)),
    now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, writers' = writers, objs' = objs, events' = events, keepRead,
  }
  action ack(l: int): bool = {
    val w = writers.get(l)
    all {
      w.life == Up, w.busy, objs.contains(w.cur),
      buffer' = buffer.setBy(l, b => b.filter(x => x.id != w.cur.req)),
      writers' = writers.set(l, { ...w, busy: false, seq: w.seq + 1, cur: NOOBJ }),
      now' = now, nextId' = nextId, history' = history, lost' = lost, puts' = puts, objs' = objs, events' = events, keepRead,
    }
  }
  action resend(l: int): bool = {
    val w = writers.get(l)
    all {
      w.life == Up, w.busy, not(objs.contains(w.cur)), not(puts.contains(w.cur)),
      puts' = puts.union(Set(w.cur)),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, writers' = writers, objs' = objs, events' = events, keepRead,
    }
  }
  action beat(l: int): bool = {
    val w = writers.get(l)
    val o = { lane: l, epoch: w.epoch, seq: w.seq, kind: Beat, req: 0, r: now, low: now }
    all {
      w.life == Up, not(w.busy), w.seq < MAX_SEQ, buffer.get(l) == Set(),
      objs.filter(x => x.lane == l).forall(x => x.low < now),
      objs' = objs.union(Set(o)), writers' = writers.set(l, { ...w, seq: w.seq + 1 }),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, puts' = puts, events' = events, keepRead,
    }
  }
  // Orderly shutdown: receivers stopped, the buffer drained (design), then
  // the close slot, whose low is the close time.
  action close(l: int): bool = {
    val w = writers.get(l)
    val o = { lane: l, epoch: w.epoch, seq: w.seq, kind: Close, req: 0, r: now, low: custodyFloor(l, 0) }
    all {
      w.life == Up, not(w.busy), w.seq < MAX_SEQ, CLOSE_UNDRAINED or buffer.get(l) == Set(),
      objs' = objs.union(Set(o)), writers' = writers.set(l, { ...w, seq: w.seq + 1, life: Closed, since: now }),
      events' = events.union(Set(if (buffer.get(l) != Set()) "closeUndrained" else "close")),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, puts' = puts, keepRead,
    }
  }
  // Death without a close: custody stays on the volume; a PUT in flight may
  // still land (a zombie).
  action die(l: int): bool = all {
    writers.get(l).life == Up,
    writers' = writers.set(l, { ...writers.get(l), life: Dead, since: now }),
    events' = events.union(Set(if (buffer.get(l) != Set()) "diedWithCustody" else "died")),
    now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, puts' = puts, objs' = objs, keepRead,
  }
  // The volume is deleted: its custody is lost (acknowledged). Only once no
  // PUT of the dead process can still land (ZOMBIE_WAIT: the pod gone for
  // longer than a request lifetime): a lost request's PUT landing after
  // complete_through passed it would put rows below it (the model's second
  // finding, without any retirement: a rule for the scale-down runbook).
  action deleteVolume(l: int): bool = all {
    writers.get(l).life != Up, buffer.get(l) != Set(),
    not(ZOMBIE_WAIT) or not(puts.exists(o => o.lane == l)),
    lost' = lost.union(buffer.get(l).map(q => q.id)), buffer' = buffer.set(l, Set()),
    events' = events.union(Set("volumeDeleted")),
    now' = now, nextId' = nextId, history' = history, writers' = writers, puts' = puts, objs' = objs, keepRead,
  }
  // A new incarnation of the producer mounts the volume (ordinal reuse): a
  // new epoch whose first slot is its birth (low: its custody floor), then
  // it replays whatever the volume holds, received_at kept (D19).
  action adopt(l: int): bool = {
    val w = writers.get(l)
    val b = { lane: l, epoch: w.epoch + 1, seq: 0, kind: Beat, req: 0, r: now, low: custodyFloor(l, 0) }
    all {
      w.life != Up, w.epoch < MAX_EPOCH,
      objs' = objs.union(Set(b)), writers' = writers.set(l, { epoch: w.epoch + 1, seq: 1, busy: false, cur: NOOBJ, life: Up, since: now }),
      events' = events.union(Set(if (retired(l)) "adoptedRetired" else "adopted")),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, lost' = lost, puts' = puts, keepRead,
    }
  }
  action tick: bool = all { now < MAX_T, now' = now + 1, keepEdge, keepRead, events' = events }

  // ---- the reader -------------------------------------------------------------------------
  // A retired lane's data below its R: quarantined, never ingested.
  def isBelow(o: Obj): bool = o.kind == Data and retired(o.lane) and o.r < retiredAt.get(o.lane)
  def quarantines(o: Obj): bool = QUARANTINE and isBelow(o)
  // Ingest committed slots (any subset: a statement may land in part), or
  // quarantine them.
  action ingestSet(os: Set[Obj]): bool = {
    val qs = os.filter(o => quarantines(o))
    val is = os.exclude(qs)
    all {
      os != Set(), os.forall(o => objs.contains(o) and not(passed(o))),
      ing' = ing.union(is), quar' = quar.union(qs),
      events' = events.union(
        (if (is.exists(o => o.kind == Data and o.r < wm and not(ingestedReq(o.req)))) Set("lateBelow") else Set())
          .union(if (qs != Set()) Set("quarantined") else Set())),
      now' = now, keepEdge, retiredAt' = retiredAt, retEpoch' = retEpoch, wm' = wm,
    }
  }
  action ingest(o: Obj): bool = ingestSet(Set(o))
  // The lane's holder passed its close slot: every slot of the lane up to it
  // ingested, and nothing after it (in its epoch or a later one) visible.
  action retireClosed(l: int): bool = {
    val lo = objs.filter(o => o.lane == l)
    val cs = lo.filter(o => o.kind == Close)
    all {
      not(retired(l)), cs != Set(),
      nondet c = cs.oneOf()
      all {
        passed(c), lo.forall(o => before(o, c) implies passed(o)), lo.forall(o => not(before(c, o))),
        retiredAt' = retiredAt.set(l, c.low), retEpoch' = retEpoch.set(l, c.epoch),
      },
      events' = events.union(Set("retiredClosed")),
      now' = now, keepEdge, ing' = ing, quar' = quar, wm' = wm,
    }
  }
  // The operator retires a lane whose publisher died without a close: the
  // volume deleted (OP_EVIDENCE; otherwise the operator is wrong), no PUT
  // of it in flight (ZOMBIE_WAIT), and, as for a close, the lane's holder
  // has passed every slot the lane shows: a zombie PUT that landed before
  // the retirement is ingested, not quarantined (the model's first finding:
  // retiring with it still pending quarantined a request that did reach S3).
  action retireOperator(l: int): bool = {
    val w = writers.get(l)
    all {
      not(retired(l)), w.life == Dead,
      // after the death: R is above every received_at the dead process held
      // (the model's third finding: retiring at the death's own instant let
      // a replay at exactly R through, ingested below the published value)
      now > w.since,
      not(OP_EVIDENCE) or buffer.get(l) == Set(),
      not(ZOMBIE_WAIT) or not(puts.exists(o => o.lane == l)),
      objs.filter(o => o.lane == l).forall(o => passed(o)),
      retiredAt' = retiredAt.set(l, now), retEpoch' = retEpoch.set(l, w.epoch),
      events' = events.union(Set(if (buffer.get(l) != Set()) "opRetiredWithCustody" else "opRetired")),
      now' = now, keepEdge, ing' = ing, quar' = quar, wm' = wm,
    }
  }
  // The mutant: a lane that stopped advancing is retired at the clock.
  action retireStale(l: int): bool = all {
    RETIRE_STALE, not(retired(l)), writers.get(l).life != Up,
    retiredAt' = retiredAt.set(l, now), retEpoch' = retEpoch.set(l, writers.get(l).epoch),
    events' = events.union(Set("retiredStale")),
    now' = now, keepEdge, ing' = ing, quar' = quar, wm' = wm,
  }
  action publish: bool = {
    val nw = imax(wm, computedWm)
    all {
      nw != wm, wm' = nw,
      events' = events.union(if (LANES.exists(l => retired(l) and not(reborn(l)) and nw > laneWm(l))) Set("pastRetired") else Set()),
      now' = now, keepEdge, ing' = ing, quar' = quar, retiredAt' = retiredAt, retEpoch' = retEpoch,
    }
  }

  action init = all {
    now' = 0, nextId' = 1,
    buffer' = LANES.mapBy(_ => Set()), history' = Set(), lost' = Set(),
    writers' = LANES.mapBy(_ => { epoch: 1, seq: 1, busy: false, cur: NOOBJ, life: Up, since: 0 }),
    puts' = Set(), objs' = LANES.map(l => birth(l)),
    ing' = Set(), quar' = Set(),
    retiredAt' = LANES.mapBy(_ => NOT_RETIRED), retEpoch' = LANES.mapBy(_ => 0),
    wm' = 0, events' = Set(),
  }

  action step = {
    nondet l = LANES.oneOf()
    any {
      receive(l),
      all { buffer.get(l) != Set(), nondet q = buffer.get(l).oneOf() send(l, q) },
      all { puts != Set(), nondet o = puts.oneOf() any { land(o), lose(o) } },
      ack(l), resend(l), beat(l), close(l), die(l), deleteVolume(l), adopt(l), tick,
      all { objs != Set(), nondet o = objs.oneOf() ingest(o) },
      retireClosed(l), retireOperator(l), retireStale(l), publish,
    }
  }

  // ---- properties -----------------------------------------------------------------------
  // Every request below the published value is ingested, lost with its
  // volume, or quarantined.
  val completeSound: bool =
    history.forall(q => q.r < wm implies (ingestedReq(q.id) or lost.contains(q.id) or quarantinedReq(q.id)))
  // Nothing new is ever ingested below the value published before it.
  val noLateBelow: bool = not(events.contains("lateBelow"))
  // The quarantine is empty unless an operator retired a lane whose custody
  // was not empty (or a close was committed with custody: the mutant).
  val quarantineOnlyOnMistake: bool =
    quar == Set() or events.contains("opRetiredWithCustody") or events.contains("closeUndrained")
  val wmBounded: bool = wm <= now
  val safety: bool = and { completeSound, noLateBelow, quarantineOnlyOnMistake, wmBounded }
  // What holds whatever the operator's evidence: the published value is
  // final (nothing lands below it later). Completeness below it rests on the
  // evidence: a kept volume's requests are below it, unread, until they are
  // replayed into the quarantine or the volume is deleted.
  val stable: bool = and { noLateBelow, quarantineOnlyOnMistake, wmBounded }

  // ---- witnesses (a violation of not(w) means w was reached) --------------------------------
  // A lane retired by its close, and complete_through past its watermark.
  val wRetiredClosed: bool = events.contains("retiredClosed") and events.contains("pastRetired")
  // The operator retired a dead lane, and complete_through went past it.
  val wRetiredOperator: bool = events.contains("opRetired") and events.contains("pastRetired")
  // A retired lane came back (a new incarnation) and its new data was ingested.
  val wReborn: bool = LANES.exists(l => retired(l) and ing.exists(o => o.lane == l and o.kind == Data and o.epoch > retEpoch.get(l)))
  // A request lost with its volume.
  val wLost: bool = lost != Set()
  // An adopted volume's custody (a replay) was quarantined.
  val wQuarantined: bool = quar != Set()
  // A zombie PUT landed after its writer died or closed.
  val wZombie: bool = events.contains("zombieLanded")
  // A publisher died with custody, and a new incarnation replayed it (not retired).
  val wAdoptedReplay: bool = events.contains("diedWithCustody") and events.contains("adopted") and
    ing.exists(o => o.kind == Data and o.epoch > 1)

  // ---- scripted runs ---------------------------------------------------------------------
  def req(i: int): Req = history.filter(q => q.id == i).fold({ id: 0, lane: 0, r: 0 }, (a, q) => q)
  def objOf(i: int): Obj = objs.filter(o => o.req == i).fold(NOOBJ, (a, o) => o)
  def closeOf(l: int): Obj = objs.filter(o => o.lane == l and o.kind == Close).fold(NOOBJ, (a, o) => o)
  action commit(l: int, i: int): bool =
    send(l, req(i)).then(all { puts != Set(), nondet o = puts.oneOf() land(o) }).then(ack(l))
  action ingestAll = ingestSet(objs.filter(o => not(passed(o))))

  // Lane 1 closes in order at t=1 after committing a request; lane 2 lives on.
  run closePrefix =
    init.then(receive(1)).then(commit(1, 1)).then(tick).then(close(1)).then(ingestAll).then(retireClosed(1))
  // Retired at its close (1); lane 2 heartbeats at t=3 and carries complete_through past lane 1's last low.
  run closedRetiresDesignTest =
    closePrefix.then(tick).then(tick).then(beat(2)).then(ingestAll).then(publish)
      .expect(and { retiredAt.get(1) == 1, wm == 3, wRetiredClosed, safety })
  // The same lane later adopted (its volume empty, a new epoch): it counts again from its birth.
  run closedRebornDesignTest =
    closePrefix.then(tick).then(tick).then(beat(2)).then(ingestAll).then(publish)
      .then(adopt(1)).then(receive(1)).then(commit(1, 2)).then(ingestAll).then(tick).then(beat(1)).then(beat(2)).then(ingestAll).then(publish)
      .expect(and { wReborn, counts(1), wm == 4, safety })

  // Lane 1 dies with an error in custody at t=0; its volume is deleted; the operator retires it at t=2.
  run deathPrefix = init.then(receive(1)).then(die(1)).then(tick).then(tick)
  run operatorRetiresDesignTest =
    deathPrefix.then(deleteVolume(1)).then(ingestAll).then(retireOperator(1)).then(tick).then(beat(2)).then(ingestAll).then(publish)
      .expect(and { wLost, retiredAt.get(1) == 2, wm == 3, wRetiredOperator, safety })
  // Without the volume deleted the design refuses the retirement (OP_EVIDENCE) and the lane holds.
  run keptVolumeHoldsDesignTest =
    deathPrefix.then(tick).then(beat(2)).then(ingestAll)
      .expect(and { buffer.get(1) != Set(), not(retired(1)), computedWm == 0, safety })
  // The operator is wrong (opMistake): the volume was kept, retired anyway,
  // then adopted and replayed: the replay is quarantined, never ingested
  // below the published value.
  run opMistakeQuarantinesTest =
    deathPrefix.then(ingestAll).then(retireOperator(1)).then(tick).then(beat(2)).then(ingestAll).then(publish)
      .then(adopt(1)).then(commit(1, 1)).then(ingestAll)
      .expect(and { wQuarantined, quar.exists(o => o.req == 1), not(ingestedReq(1)), stable })
  // ... and without the quarantine (ingestBelow) the replay lands below it.
  run ingestBelowBreaksTest =
    deathPrefix.then(ingestAll).then(retireOperator(1)).then(tick).then(beat(2)).then(ingestAll).then(publish)
      .then(adopt(1)).then(commit(1, 1))
      .then(ingest(objOf(1)))
      .expect(not(noLateBelow))
  // The reader retires a stale lane (retireStale): the custody it held, replayed later, lands below.
  run retireStaleBreaksTest =
    deathPrefix.then(retireStale(1)).then(tick).then(beat(2)).then(ingestAll).then(publish)
      .expect(not(completeSound))
  // A close committed with a request still in custody (closeUndrained).
  run closeUndrainedBreaksTest =
    init.then(receive(1)).then(tick).then(close(1)).then(ingestAll).then(retireClosed(1)).then(tick).then(beat(2))
      .then(ingestAll).then(publish)
      .expect(not(completeSound))
}

// The design: an orderly close, or an operator with evidence; quarantine; a
// reborn lane counts again.
module retirementDesign {
  import retirement(LANES = Set(1, 2), RETIRE_STALE = false, CLOSE_UNDRAINED = false, QUARANTINE = true, UNRETIRE = true,
    ZOMBIE_WAIT = true, OP_EVIDENCE = true).*
}
// The operator retires a lane whose volume was kept: safe; its requests go to the quarantine.
module opMistake {
  import retirement(LANES = Set(1, 2), RETIRE_STALE = false, CLOSE_UNDRAINED = false, QUARANTINE = true, UNRETIRE = true,
    ZOMBIE_WAIT = true, OP_EVIDENCE = false).*
}
module retireStale {
  import retirement(LANES = Set(1, 2), RETIRE_STALE = true, CLOSE_UNDRAINED = false, QUARANTINE = true, UNRETIRE = true,
    ZOMBIE_WAIT = true, OP_EVIDENCE = true).*
}
module closeUndrained {
  import retirement(LANES = Set(1, 2), RETIRE_STALE = false, CLOSE_UNDRAINED = true, QUARANTINE = true, UNRETIRE = true,
    ZOMBIE_WAIT = true, OP_EVIDENCE = true).*
}
module ingestBelow {
  import retirement(LANES = Set(1, 2), RETIRE_STALE = false, CLOSE_UNDRAINED = false, QUARANTINE = false, UNRETIRE = true,
    ZOMBIE_WAIT = true, OP_EVIDENCE = false).*
}
module staysRetired {
  import retirement(LANES = Set(1, 2), RETIRE_STALE = false, CLOSE_UNDRAINED = false, QUARANTINE = true, UNRETIRE = false,
    ZOMBIE_WAIT = true, OP_EVIDENCE = true).*
}
module retireInFlight {
  import retirement(LANES = Set(1, 2), RETIRE_STALE = false, CLOSE_UNDRAINED = false, QUARANTINE = true, UNRETIRE = true,
    ZOMBIE_WAIT = false, OP_EVIDENCE = true).*
}
