// -*- mode: Bluespec; -*-
/**
 * bitemporalCatalog: the entity catalog as append-only bitemporal events,
 * resolved at query time (research/bitemporal.md §5, DECISIONS D32 proposed,
 * entities/bitemp/). Small on purpose: two entities, valid times 0..4, up to
 * five events.
 *
 * An event is (ent, vf, vt, st, seq, src, kind, a):
 *  - ent: an entity (1, 2), or ALL (0): every entity of the scope (a relist
 *    or a restart gap, a full-state sync's "nothing else exists");
 *  - [vf, vt): its valid-time interval (vt = INF: open);
 *  - st: its system time, when the aggregator took it in (ingest_log.put_at);
 *    seq orders events of one st (the arrival index; unique);
 *  - src: "controller" (the cluster's controller: the authority),
 *    "overseer" (the central multicluster aggregator: the fallback) or
 *    "announce" (an edge's announcement: evidence only);
 *  - kind: "assert" (the entity existed, with attributes a), "retract" (it
 *    did not), "unknown" (the source does not know: a gap). Announcements
 *    only assert, and only a named entity.
 *
 * Resolution at (entity, VT, ST): only events with st <= ST count.
 *  - The AUTHORITY tier (controller, overseer) is ordered by
 *    eff = st + (W if controller): within the trust window W the controller
 *    wins; an overseer event newer than the controller's latest word by more
 *    than W wins (a controller that has gone silent loses its authority; a
 *    live one re-asserts every open version at each sync, so it keeps it).
 *    Ties: the controller, then the later arrival.
 *  - The latest authority event covering (entity, VT) decides if it is an
 *    assert or a retract. If it is `unknown`, or there is none, the latest
 *    covering ANNOUNCEMENT fills in (an assertion, flagged uncertain when an
 *    authority said unknown); else the answer is unknown (or absent).
 *  - Implemented as XTDB's backwards replay with a ceiling: events in
 *    descending (tier, eff, seq); a definite authority event closes the
 *    point; an authority `unknown` closes it to the authority tier only (the
 *    evidence tier may still fill it).
 *
 * The current view (VT = now, ST = now) is a materialised partition `live`:
 * an event leaves it once it can no longer decide any answer at VT >= now
 * (its vt <= now, or every VT of it from now on is hidden by a higher event
 * of its own tier: authority by authority of any kind, an announcement only
 * by a later announcement, since a later authority `unknown` can reopen the
 * evidence tier).
 *
 * Properties:
 *  (i)   resolvesAsSpec: the replay equals the declarative rule above (the
 *        highest-precedence latest covering event), at every (ent, VT, ST);
 *  (ii)  unknownNeverAsserts: where the top authority event is `unknown`,
 *        the answer is unknown, or an announcement's assertion flagged
 *        uncertain: never a certain assertion or retraction;
 *  (iii) monotoneHistory: the answers at every system time S < now, frozen
 *        when the clock passed S, never change (what makes a D30 basis sound
 *        for the catalog);
 *  (iv)  currentIsResolved: the current partition resolves (VT = now,
 *        ST = now) exactly as all events do.
 *
 * Instances: `design` (W = 2); `strict` (W = 100: the controller is never
 * overridden, a design alternative). Mutants: `noCeiling` (backwards replay,
 * every covering event overwrites), `forward` (forward replay, overwrite:
 * latest wins but no fill), `sourceBlind` (the replay orders by st alone:
 * precedence ignored), `unknownAsRetract`, `ignoreST` (resolves over every
 * event: SCD2 with rows rewritten in place), `pruneAny` (the current
 * partition drops an announcement hidden by an authority assert),
 * `pruneBySt` (the current partition orders by st, not by precedence).
 */
module bitemporalCatalog {
  const W: int            // trust window of the controller over the overseer (the resolution's)
  const W_REPLAY: int     // the window the replay uses (= W; mutant sourceBlind: 0)
  const REPLAY: str       // "ceiling" | "noCeiling" | "forward"
  const UNKNOWN_AS_RETRACT: bool
  const IGNORE_ST: bool
  const PRUNE: str        // "tier" (design) | "any" | "st"
  const TRACE: bool       // keep `view` (for trace export to the Go resolver's test; slows simulation)

  pure val ALL = 0
  pure val ENT: Set[int] = Set(1, 2)
  pure val INF = 9
  pure val VTS: Set[int] = 0.to(4)      // elementary intervals [0,1) .. [3,4), [4, INF)
  pure val MAX_T = 4
  pure val MAX_EV = 5

  type Event = { ent: int, vf: int, vt: int, st: int, seq: int, src: str, kind: str, a: int }
  // kind: "assert" | "retract" | "unknown" | "absent"; src: "" when none
  type Res = { kind: str, a: int, src: str, unc: bool }

  pure val ABSENT: Res = { kind: "absent", a: 0, src: "", unc: false }
  pure val UNKNOWN: Res = { kind: "unknown", a: 0, src: "", unc: false }
  pure val NOEV: Event = { ent: -1, vf: 0, vt: 0, st: -1, seq: -1, src: "", kind: "", a: 0 }

  var now: int
  var events: Set[Event]
  var live: Set[Event]                  // the current partition
  var hist: int -> ((int, int) -> Res)  // S -> (ent, VT) -> the answer at ST = S, frozen when the clock passed S
  var view: (int, int, int) -> Res      // (ent, VT, S) -> the answer now, S <= now (for trace replay)

  // ---- resolution ----------------------------------------------------------------------
  pure def auth(e: Event): bool = e.src != "announce"
  pure def covers(e: Event, ent: int, v: int): bool = (e.ent == ent or e.ent == ALL) and e.vf <= v and v < e.vt
  // the order: tier, then eff (controller + w), then the controller on a tie, then arrival
  pure def key(e: Event, w: int): int =
    if (auth(e)) 1000000 + (e.st + (if (e.src == "controller") w else 0)) * 1000 + (if (e.src == "controller") 500 else 0) + e.seq
    else e.st * 1000 + e.seq
  // descending by key (keys are unique: seq is)
  pure def ordered(evs: Set[Event], w: int): List[Event] =
    evs.fold(List(), (l, e) => l.select(x => key(x, w) > key(e, w)).append(e).concat(l.select(x => key(x, w) < key(e, w))))

  pure def kindOf(e: Event): str = if (UNKNOWN_AS_RETRACT and e.kind == "unknown") "retract" else e.kind
  pure def says(e: Event): Res =
    if (kindOf(e) == "unknown") UNKNOWN else { kind: kindOf(e), a: if (kindOf(e) == "assert") e.a else 0, src: e.src, unc: false }

  type Ceil = { done: bool, unk: bool, r: Res }
  pure def stepCeil(s: Ceil, e: Event): Ceil =
    if (s.done) s
    else if (auth(e))
      if (s.unk) s                                   // under an authority `unknown`: closed to the authority tier
      else if (kindOf(e) == "unknown") s.with("unk", true)
      else { done: true, unk: false, r: says(e) }
    else { done: true, unk: s.unk, r: { kind: "assert", a: e.a, src: "announce", unc: s.unk } }

  // XTDB's backwards replay for one point (the Go resolver does it per interval)
  pure def replay(evs: Set[Event], ent: int, v: int, S: int): Res =
    val vis = evs.filter(e => (IGNORE_ST or e.st <= S) and covers(e, ent, v))
    val ord = ordered(vis, W_REPLAY)
    if (REPLAY == "ceiling")
      val c = ord.foldl({ done: false, unk: false, r: ABSENT }, (s, e) => stepCeil(s, e))
      if (c.done) c.r else if (c.unk) UNKNOWN else ABSENT
    else if (REPLAY == "noCeiling") ord.foldl(ABSENT, (_, e) => says(e))       // the oldest, lowest event wins
    else ord.foldr(ABSENT, (e, _) => says(e))                                    // forward: the top event wins, no fill

  // the declarative rule, with the design's W
  pure def top(evs: Set[Event]): Event = evs.fold(NOEV, (m, e) => if (m == NOEV or key(e, W) > key(m, W)) e else m)
  pure def topAuth(evs: Set[Event], ent: int, v: int, S: int): Event = top(evs.filter(e => auth(e) and e.st <= S and covers(e, ent, v)))
  pure def spec(evs: Set[Event], ent: int, v: int, S: int): Res =
    val ta = topAuth(evs, ent, v, S)
    val tn = top(evs.filter(e => not(auth(e)) and e.st <= S and covers(e, ent, v)))
    if (ta != NOEV and ta.kind != "unknown") { kind: ta.kind, a: if (ta.kind == "assert") ta.a else 0, src: ta.src, unc: false }
    else if (tn != NOEV) { kind: "assert", a: tn.a, src: "announce", unc: ta != NOEV }
    else if (ta != NOEV) UNKNOWN
    else ABSENT

  pure def table(evs: Set[Event], S: int): (int, int) -> Res = tuples(ENT, VTS).mapBy(p => replay(evs, p._1, p._2, S))
  pure def viewOf(evs: Set[Event], t: int): (int, int, int) -> Res =
    if (TRACE) tuples(ENT, VTS, 0.to(t)).mapBy(p => replay(evs, p._1, p._2, p._3)) else Map()

  // ---- the current partition ----------------------------------------------------------------
  pure def hides(h: Event, e: Event): bool = and {
    h.seq != e.seq,
    h.ent == ALL or h.ent == e.ent,                  // an ALL event is hidden only by ALL events
    if (PRUNE == "any") key(h, W) > key(e, W) and (auth(h) implies h.kind != "unknown")
    else if (PRUNE == "st") auth(h) == auth(e) and (h.st > e.st or (h.st == e.st and h.seq > e.seq))
    else auth(h) == auth(e) and key(h, W) > key(e, W),
  }
  pure def prunable(e: Event, evs: Set[Event], t: int): bool =
    e.vt <= t or VTS.filter(v => v >= t and v >= e.vf and v < e.vt).forall(v => evs.exists(h => hides(h, e) and h.vf <= v and v < h.vt))
  pure def prune(evs: Set[Event], t: int): Set[Event] = evs.filter(e => not(prunable(e, evs, t)))

  // ---- actions ----------------------------------------------------------------------------------
  action init = all {
    now' = 0, events' = Set(), live' = Set(), hist' = Map(), view' = viewOf(Set(), 0),
  }
  // The clock passes now: no event will have st = now any more; the answers at ST = now are frozen.
  action tick = all {
    now < MAX_T,
    now' = now + 1, events' = events, live' = prune(live, now + 1),
    hist' = hist.put(now, table(events, now)), view' = viewOf(events, now + 1),
  }
  action add(ent: int, vf: int, vt: int, src: str, kind: str, a: int): bool = {
    val e = { ent: ent, vf: vf, vt: vt, st: now, seq: events.size() + 1, src: src, kind: kind, a: if (kind == "assert") a else 0 }
    all {
      events.size() < MAX_EV, vf < vt,
      src == "announce" implies (kind == "assert" and ent != ALL),
      events' = events.union(Set(e)), live' = prune(live.union(Set(e)), now),
      now' = now, hist' = hist, view' = viewOf(events.union(Set(e)), now),
    }
  }
  // the bounds reached: nothing more happens
  action stutter = all { now == MAX_T or events.size() == MAX_EV, now' = now, events' = events, live' = live, hist' = hist, view' = view }
  action step = {
    nondet ent = Set(ALL, 1, 2).oneOf()
    nondet vf = 0.to(3).oneOf()
    nondet vt = Set(1, 2, 3, 4, INF).oneOf()
    nondet src = Set("controller", "overseer", "announce").oneOf()
    nondet kind = Set("assert", "retract", "unknown").oneOf()
    nondet a = Set(1, 2).oneOf()
    any { tick, add(ent, vf, vt, src, kind, a), stutter }
  }

  // ---- properties --------------------------------------------------------------------------------
  pure val points: Set[(int, int)] = tuples(ENT, VTS)
  val resolvesAsSpec: bool = 0.to(now).forall(S => points.forall(p => replay(events, p._1, p._2, S) == spec(events, p._1, p._2, S)))
  val unknownNeverAsserts: bool = 0.to(now).forall(S => points.forall(p => {
    val r = replay(events, p._1, p._2, S)
    topAuth(events, p._1, p._2, S).kind == "unknown" implies
      (r.kind == "unknown" or (r.kind == "assert" and r.src == "announce" and r.unc))
  }))
  val monotoneHistory: bool = hist.keys().forall(S => points.forall(p => replay(events, p._1, p._2, S) == hist.get(S).get(p)))
  val currentIsResolved: bool = ENT.forall(i => replay(live, i, now, now) == replay(events, i, now, now))
  val safety: bool = and { resolvesAsSpec, unknownNeverAsserts, monotoneHistory, currentIsResolved }

  // ---- witnesses (expect `not(w)` VIOLATED) ----------------------------------------------------------
  def resNow(p: (int, int)): Res = replay(events, p._1, p._2, now)
  def covering(p: (int, int), src: str): Set[Event] = events.filter(e => e.src == src and covers(e, p._1, p._2))
  // a pod the authority does not know about (unknown), filled by an edge's announcement
  val wAnnounceFills: bool = points.exists(p => resNow(p) == { kind: "assert", a: resNow(p).a, src: "announce", unc: true })
  // the overseer decides over a controller event (the controller was silent for more than W)
  val wOverseerTakesOver: bool = points.exists(p => resNow(p).src == "overseer" and covering(p, "controller").exists(e => e.kind != "unknown"))
  // the controller decides although a newer overseer event disagrees (within W)
  val wControllerHolds: bool = points.exists(p => resNow(p).src == "controller" and
    covering(p, "overseer").exists(o => o.kind != "unknown" and says(o) != resNow(p) and covering(p, "controller").forall(c => c.st < o.st)))
  // a restart or relist gap hides an older assertion: unknown, not "alive"
  val wUnknownHidesAssert: bool = points.exists(p => resNow(p) == UNKNOWN and covering(p, "controller").exists(e => e.kind == "assert"))
  // an announcement that an authority assert/retract overrides
  val wAnnounceOverridden: bool = points.exists(p => resNow(p).src != "announce" and resNow(p).kind != "unknown" and covering(p, "announce") != Set())
  // history was corrected after S, and the answer at S is kept
  val wCorrected: bool = hist.keys().exists(S => points.exists(p => hist.get(S).get(p) != resNow(p)))
  // the current partition dropped an event that is still valid now
  val wPruned: bool = events.exists(e => e.vt > now and not(live.contains(e)))

  // ---- scripted runs ------------------------------------------------------------------------------------
  def at(i: int, v: int): Res = replay(events, i, v, now)
  def atS(i: int, v: int, S: int): Res = replay(events, i, v, S)
  pure def ctl(a: int): Res = { kind: "assert", a: a, src: "controller", unc: false }
  pure def ov(a: int): Res = { kind: "assert", a: a, src: "overseer", unc: false }
  pure val RETRACTED: Res = { kind: "retract", a: 0, src: "controller", unc: false }
  pure def ann(a: int): Res = { kind: "assert", a: a, src: "announce", unc: true }

  // Controller restart. Pod 1 asserted at t0; the controller dies at 1; pod 1 is
  // deleted, pod 2 born and dead inside the outage (an edge announces it at 1).
  // The new incarnation at 3: unknown ALL over [1, 3), its full-state sync
  // retracts ALL from 3. The history at ST = 2 still says pod 1 lived.
  run restartPrefix =
    init.then(add(1, 0, INF, "controller", "assert", 1)).then(tick)
      .then(add(2, 1, 2, "announce", "assert", 1)).then(tick).then(tick)
      .then(add(ALL, 1, 3, "controller", "unknown", 0)).then(add(ALL, 3, INF, "controller", "retract", 0))
  run restartDesignTest = restartPrefix.expect(and {
    safety, at(1, 0) == ctl(1), at(1, 1) == UNKNOWN, at(1, 2) == UNKNOWN, at(1, 3) == RETRACTED,
    at(2, 1) == ann(1), at(2, 2) == UNKNOWN, atS(1, 2, 2) == ctl(1), wCorrected,
  })
  // Relist gap with a late announcement (after the gap record).
  run lateAnnounceDesignTest =
    init.then(add(1, 0, INF, "controller", "assert", 1)).then(tick)
      .then(add(ALL, 1, 3, "controller", "unknown", 0)).then(add(1, 0, INF, "controller", "assert", 1)).then(tick)
      .then(add(2, 1, 3, "announce", "assert", 2))
      .expect(and { safety, at(1, 1) == ctl(1), at(2, 1) == ann(2), at(2, 3) == ABSENT, atS(2, 1, 1) == UNKNOWN })
  // Controller against overseer: a newer overseer event within W does not
  // override the controller; one newer by more than W does.
  run overseerPrefix =
    init.then(add(1, 0, INF, "controller", "assert", 1)).then(tick).then(add(1, 0, INF, "overseer", "assert", 2))
  run overseerDesignTest = overseerPrefix.expect(and { safety, at(1, 0) == ctl(1) })
    .then(tick).then(tick).then(add(1, 0, INF, "overseer", "retract", 0))
    .expect(and { safety, at(1, 0) == { kind: "retract", a: 0, src: "overseer", unc: false }, atS(1, 0, 2) == ctl(1) })
  // ... unless the controller re-asserted (a sync) meanwhile.
  run controllerSyncDesignTest = overseerPrefix.then(tick).then(add(1, 0, INF, "controller", "assert", 1)).then(tick)
    .then(add(1, 0, INF, "overseer", "retract", 0)).expect(and { safety, at(1, 0) == ctl(1) })
  // The evidence tier is kept in the current partition under an authority assert.
  run keepAnnouncePrefix =
    init.then(add(1, 0, INF, "announce", "assert", 2)).then(add(1, 0, INF, "controller", "assert", 1)).then(tick)
      .then(add(1, 0, INF, "controller", "unknown", 0))
  run keepAnnounceDesignTest = keepAnnouncePrefix.expect(and { safety, at(1, 1) == ann(2) })

  // mutants, each breaking its invariant
  run noCeilingBreaksTest = restartPrefix.expect(not(resolvesAsSpec) and not(unknownNeverAsserts))
  run forwardBreaksTest = restartPrefix.expect(not(resolvesAsSpec))
  run sourceBlindBreaksTest = overseerPrefix.expect(not(resolvesAsSpec))
  run unknownAsRetractBreaksTest = restartPrefix.expect(not(unknownNeverAsserts))
  run ignoreSTBreaksTest = restartPrefix.expect(not(monotoneHistory))
  run pruneAnyBreaksTest = keepAnnouncePrefix.expect(not(currentIsResolved))
  run pruneByStBreaksTest = overseerPrefix.expect(not(currentIsResolved))
}

module design {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false).*
}
// design, keeping `view` for trace export (entities/bitemp's model-based test)
module designTrace {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = true).*
}
// strict precedence: the controller is never overridden by the overseer
module strict {
  import bitemporalCatalog(W = 100, W_REPLAY = 100, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false).*
}
module noCeiling {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "noCeiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false).*
}
module forward {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "forward", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false).*
}
module sourceBlind {
  import bitemporalCatalog(W = 2, W_REPLAY = 0, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false).*
}
module unknownAsRetract {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = true, IGNORE_ST = false, PRUNE = "tier", TRACE = false).*
}
module ignoreST {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = true, PRUNE = "tier", TRACE = false).*
}
module pruneAny {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "any", TRACE = false).*
}
module pruneBySt {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "st", TRACE = false).*
}
