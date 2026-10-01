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
 * Pseudonymise on departure (O-G9, DECISIONS D38 items 11-17, research/grants.md
 * §9.6): an event (src "steward", kind "pseudonymise", a = the pseudonym) for
 * one entity over its whole valid time. It is an OVERLAY, not a lifecycle
 * event: the replay ignores it; an answer that asserts a version of that
 * entity shows the pseudonym instead, at EVERY basis S (also S before the
 * correction's st), and the first correction (lowest st, seq) wins. Names are
 * the versions 1, 2; pseudonyms 7, 8 (two keys: a duplicate signal may differ).
 *  (v)    pseudonymHidesName: once a correction is recorded, no answer at any
 *         basis resolves a name of that entity;
 *  (vi)   noNameAtOrAfter: the weaker reading (bases at or after it only);
 *  (vii)  monotoneRedacted: an answer frozen at S changes, if at all, only
 *         by the name becoming the pseudonym (state, source, flag kept);
 *  (viii) pseudonymStable: the pseudonym shown never changes once shown;
 *  (ix)   idempotent: duplicate corrections change no answer;
 *  (x)    stateUnchanged: the correction changes no lifecycle state.
 *
 * Instances: `design` (W = 2); `strict` (W = 100: the controller is never
 * overridden, a design alternative). Mutants: `noCeiling` (backwards replay,
 * every covering event overwrites), `forward` (forward replay, overwrite:
 * latest wins but no fill), `sourceBlind` (the replay orders by st alone:
 * precedence ignored), `unknownAsRetract`, `ignoreST` (resolves over every
 * event: SCD2 with rows rewritten in place), `pruneAny` (the current
 * partition drops an announcement hidden by an authority assert),
 * `pruneBySt` (the current partition orders by st, not by precedence);
 * O-G9: `basisScoped` (pure bitemporal: the correction only at bases at or
 * after it), `lastWins` (the latest correction's pseudonym), `asAssert` (the
 * correction as a controller assertion of the pseudonym over all valid time).
 */
module bitemporalCatalog {
  const W: int            // trust window of the controller over the overseer (the resolution's)
  const W_REPLAY: int     // the window the replay uses (= W; mutant sourceBlind: 0)
  const REPLAY: str       // "ceiling" | "noCeiling" | "forward"
  const UNKNOWN_AS_RETRACT: bool
  const IGNORE_ST: bool
  const PRUNE: str        // "tier" (design) | "any" | "st"
  const TRACE: bool       // keep `view` (for trace export to the Go resolver's test; slows simulation)
  const PSEU: str         // "overlay" (design) | "basisScoped" | "lastWins" | "asAssert"

  pure val ALL = 0
  pure val ENT: Set[int] = Set(1, 2)
  pure val INF = 9
  pure val VTS: Set[int] = 0.to(4)      // elementary intervals [0,1) .. [3,4), [4, INF)
  pure val MAX_T = 4
  pure val MAX_EV = 5
  pure val NAMES: Set[int] = Set(1, 2)      // asserted versions: a person's names
  pure val PSEUDOS: Set[int] = Set(7, 8)    // pseudonyms (7: one key, 8: a rotated key)

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
  var rview: (int, int, int) -> Res     // the same, pseudonymised (the answer a reader gets)
  var firstPseu: int -> int             // ghost: the pseudonym of each entity's first correction

  // ---- resolution ----------------------------------------------------------------------
  pure def isPseu(e: Event): bool = e.kind == "pseudonymise"
  // the lifecycle events: the correction is not one (mutant asAssert: a controller assertion)
  pure def life(evs: Set[Event]): Set[Event] =
    if (PSEU == "asAssert") evs.map(e => if (isPseu(e)) e.with("src", "controller").with("kind", "assert") else e)
    else evs.filter(e => not(isPseu(e)))
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
    val vis = life(evs).filter(e => (IGNORE_ST or e.st <= S) and covers(e, ent, v))
    val ord = ordered(vis, W_REPLAY)
    if (REPLAY == "ceiling")
      val c = ord.foldl({ done: false, unk: false, r: ABSENT }, (s, e) => stepCeil(s, e))
      if (c.done) c.r else if (c.unk) UNKNOWN else ABSENT
    else if (REPLAY == "noCeiling") ord.foldl(ABSENT, (_, e) => says(e))       // the oldest, lowest event wins
    else ord.foldr(ABSENT, (e, _) => says(e))                                    // forward: the top event wins, no fill

  // the declarative rule, with the design's W
  pure def top(evs: Set[Event]): Event = evs.fold(NOEV, (m, e) => if (m == NOEV or key(e, W) > key(m, W)) e else m)
  pure def topAuth(evs: Set[Event], ent: int, v: int, S: int): Event = top(life(evs).filter(e => auth(e) and e.st <= S and covers(e, ent, v)))
  pure def spec(evs: Set[Event], ent: int, v: int, S: int): Res =
    val ta = topAuth(evs, ent, v, S)
    val tn = top(life(evs).filter(e => not(auth(e)) and e.st <= S and covers(e, ent, v)))
    if (ta != NOEV and ta.kind != "unknown") { kind: ta.kind, a: if (ta.kind == "assert") ta.a else 0, src: ta.src, unc: false }
    else if (tn != NOEV) { kind: "assert", a: tn.a, src: "announce", unc: ta != NOEV }
    else if (ta != NOEV) UNKNOWN
    else ABSENT

  pure def table(evs: Set[Event], S: int): (int, int) -> Res = tuples(ENT, VTS).mapBy(p => replay(evs, p._1, p._2, S))
  pure def viewOf(evs: Set[Event], t: int): (int, int, int) -> Res =
    if (TRACE) tuples(ENT, VTS, 0.to(t)).mapBy(p => replay(evs, p._1, p._2, p._3)) else Map()
  pure def rviewOf(evs: Set[Event], t: int): (int, int, int) -> Res =
    if (TRACE) tuples(ENT, VTS, 0.to(t)).mapBy(p => answer(evs, p._1, p._2, p._3)) else Map()

  // ---- pseudonymise (O-G9) ---------------------------------------------------------------------
  // the corrections of ent a reader at basis S applies: every recorded one (design), or
  // only those at or before S (mutant basisScoped)
  pure def corrections(evs: Set[Event], ent: int, S: int): Set[Event] =
    evs.filter(e => isPseu(e) and e.ent == ent and (PSEU != "basisScoped" or e.st <= S))
  pure def pkey(e: Event): int = e.st * 1000 + e.seq
  // the pseudonym: the first correction's (mutant lastWins: the latest's); -1 when none
  pure def pseudonymOf(cs: Set[Event]): int =
    cs.fold(NOEV, (m, e) => if (m == NOEV or (if (PSEU == "lastWins") pkey(e) > pkey(m) else pkey(e) < pkey(m))) e else m).a
  pure def redactWith(r: Res, cs: Set[Event]): Res =
    if (PSEU == "asAssert" or cs == Set() or r.kind != "assert") r else r.with("a", pseudonymOf(cs))
  // what a reader gets at (ent, VT, basis S)
  pure def answer(evs: Set[Event], ent: int, v: int, S: int): Res = redactWith(replay(evs, ent, v, S), corrections(evs, ent, S))

  // ---- the current partition ----------------------------------------------------------------
  pure def hides(h: Event, e: Event): bool = and {
    h.seq != e.seq,
    not(isPseu(h)),
    h.ent == ALL or h.ent == e.ent,                  // an ALL event is hidden only by ALL events
    if (PRUNE == "any") key(h, W) > key(e, W) and (auth(h) implies h.kind != "unknown")
    else if (PRUNE == "st") auth(h) == auth(e) and (h.st > e.st or (h.st == e.st and h.seq > e.seq))
    else auth(h) == auth(e) and key(h, W) > key(e, W),
  }
  // a correction is never pruned (it applies at every valid and system time)
  pure def prunable(e: Event, evs: Set[Event], t: int): bool = not(isPseu(e)) and (
    e.vt <= t or VTS.filter(v => v >= t and v >= e.vf and v < e.vt).forall(v => evs.exists(h => hides(h, e) and h.vf <= v and v < h.vt)))
  pure def prune(evs: Set[Event], t: int): Set[Event] = evs.filter(e => not(prunable(e, evs, t)))

  // ---- actions ----------------------------------------------------------------------------------
  action init = all {
    now' = 0, events' = Set(), live' = Set(), hist' = Map(), view' = viewOf(Set(), 0),
    rview' = rviewOf(Set(), 0), firstPseu' = Map(),
  }
  // The clock passes now: no event will have st = now any more; the answers at ST = now are frozen.
  action tick = all {
    now < MAX_T,
    now' = now + 1, events' = events, live' = prune(live, now + 1),
    hist' = hist.put(now, table(events, now)), view' = viewOf(events, now + 1),
    rview' = rviewOf(events, now + 1), firstPseu' = firstPseu,
  }
  action add(ent: int, vf: int, vt: int, src: str, kind: str, a: int): bool = {
    val e = { ent: ent, vf: vf, vt: vt, st: now, seq: events.size() + 1, src: src, kind: kind, a: if (kind == "assert") a else 0 }
    all {
      events.size() < MAX_EV, vf < vt,
      src == "announce" implies (kind == "assert" and ent != ALL),
      events' = events.union(Set(e)), live' = prune(live.union(Set(e)), now),
      now' = now, hist' = hist, view' = viewOf(events.union(Set(e)), now),
      rview' = rviewOf(events.union(Set(e)), now), firstPseu' = firstPseu,
    }
  }
  // The steward records a departure: the pseudonym p (a keyed hash; 8 under a rotated key)
  // for one entity over all valid time. A duplicate or late signal is just another one.
  action pseudonymise(ent: int, p: int): bool = {
    val e = { ent: ent, vf: 0, vt: INF, st: now, seq: events.size() + 1, src: "steward", kind: "pseudonymise", a: p }
    all {
      events.size() < MAX_EV, ent != ALL,
      events' = events.union(Set(e)), live' = prune(live.union(Set(e)), now),
      now' = now, hist' = hist, view' = viewOf(events.union(Set(e)), now),
      rview' = rviewOf(events.union(Set(e)), now),
      firstPseu' = if (firstPseu.keys().contains(ent)) firstPseu else firstPseu.put(ent, p),
    }
  }
  // the bounds reached: nothing more happens
  action stutter = all { now == MAX_T or events.size() == MAX_EV, now' = now, events' = events, live' = live, hist' = hist, view' = view,
    rview' = rview, firstPseu' = firstPseu }
  action step = {
    nondet ent = Set(ALL, 1, 2).oneOf()
    nondet vf = 0.to(3).oneOf()
    nondet vt = Set(1, 2, 3, 4, INF).oneOf()
    nondet src = Set("controller", "overseer", "announce").oneOf()
    nondet kind = Set("assert", "retract", "unknown").oneOf()
    nondet a = Set(1, 2).oneOf()
    nondet pick = 0.to(4).oneOf()          // a correction one time in five
    nondet pe = ENT.oneOf()
    nondet pa = PSEUDOS.oneOf()
    any { tick, if (pick == 0) pseudonymise(pe, pa) else add(ent, vf, vt, src, kind, a), stutter }
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
  val currentIsResolved: bool = ENT.forall(i => answer(live, i, now, now) == answer(events, i, now, now))
  // O-G9
  pure def hasPseu(evs: Set[Event], ent: int): bool = evs.exists(e => isPseu(e) and e.ent == ent)
  val pseudonymHidesName: bool = 0.to(now).forall(S => points.forall(p =>
    hasPseu(events, p._1) implies not(NAMES.contains(answer(events, p._1, p._2, S).a))))
  val noNameAtOrAfter: bool = events.filter(isPseu).forall(c => c.st.to(now).forall(S => VTS.forall(v =>
    not(NAMES.contains(answer(events, c.ent, v, S).a)))))
  // the answer frozen at S (redacted with the corrections recorded by then: st <= S) against now
  val monotoneRedacted: bool = hist.keys().forall(S => points.forall(p => {
    val old = redactWith(hist.get(S).get(p), corrections(events.filter(e => e.st <= S), p._1, S))
    val cur = answer(events, p._1, p._2, S)
    old == cur or (old.kind == "assert" and hasPseu(events, p._1) and cur == old.with("a", pseudonymOf(corrections(events, p._1, S))))
  }))
  val pseudonymStable: bool = firstPseu.keys().forall(i => 0.to(now).forall(S =>
    corrections(events, i, S) != Set() implies pseudonymOf(corrections(events, i, S)) == firstPseu.get(i)))
  pure def firstOnly(evs: Set[Event]): Set[Event] =
    evs.filter(e => not(isPseu(e)) or evs.forall(o => not(isPseu(o) and o.ent == e.ent and pkey(o) < pkey(e))))
  val idempotent: bool = 0.to(now).forall(S => points.forall(p =>
    answer(events, p._1, p._2, S) == answer(firstOnly(events), p._1, p._2, S)))
  val stateUnchanged: bool = 0.to(now).forall(S => points.forall(p => {
    val noCorr = events.filter(e => not(isPseu(e)))
    answer(events, p._1, p._2, S).kind == answer(noCorr, p._1, p._2, S).kind
  }))
  val pseudonymSafety: bool = and { pseudonymHidesName, noNameAtOrAfter, monotoneRedacted, pseudonymStable, idempotent, stateUnchanged }
  val d32Safety: bool = and { resolvesAsSpec, unknownNeverAsserts, monotoneHistory, currentIsResolved }
  val safety: bool = and { d32Safety, pseudonymSafety }

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
  // O-G9: a pseudonym shown; an older basis that named the person now shows the pseudonym;
  // a name asserted after the correction (a late Graph delta) hidden; a duplicate correction
  def ansNow(p: (int, int)): Res = answer(events, p._1, p._2, now)
  val wPseudonymised: bool = points.exists(p => PSEUDOS.contains(ansNow(p).a))
  val wOldBasisRedacted: bool = hist.keys().exists(S => points.exists(p =>
    NAMES.contains(hist.get(S).get(p).a) and PSEUDOS.contains(answer(events, p._1, p._2, S).a)))
  val wLateNameHidden: bool = points.exists(p => PSEUDOS.contains(ansNow(p).a) and
    covering(p, "controller").exists(e => e.kind == "assert" and events.exists(c => isPseu(c) and c.ent == p._1 and c.st < e.st)))
  val wDuplicate: bool = ENT.exists(i => events.filter(e => isPseu(e) and e.ent == i).map(e => e.a).size() > 1)

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

  // O-G9: two people named at t0; person 1 leaves (Graph deletion: retract from 2) and is
  // pseudonymised at st 2. Every basis, the earlier ones included, shows the pseudonym;
  // person 2 keeps their name; state is unchanged (retracted from 2).
  def ans(i: int, v: int): Res = answer(events, i, v, now)
  def ansS(i: int, v: int, S: int): Res = answer(events, i, v, S)
  run departurePrefix =
    init.then(add(1, 0, INF, "controller", "assert", 1)).then(add(2, 0, INF, "controller", "assert", 2)).then(tick).then(tick)
      .then(add(1, 2, INF, "controller", "retract", 0)).then(pseudonymise(1, 7))
  run departureDesignTest = departurePrefix.expect(and {
    safety, ans(1, 0) == ctl(7), ans(1, 1) == ctl(7), ans(1, 3) == RETRACTED, ansS(1, 0, 0) == ctl(7),
    hist.get(0).get((1, 0)) == ctl(1), ans(2, 0) == ctl(2), wOldBasisRedacted, wPseudonymised,
  })
  // a late Graph delta re-asserts the name (one round of lag), a second steward signals again
  // under a rotated key: nothing changes
  run lateSignalsDesignTest =
    init.then(add(1, 0, INF, "controller", "assert", 1)).then(tick).then(tick)
      .then(add(1, 2, INF, "controller", "retract", 0)).then(pseudonymise(1, 7))
      .then(tick).then(add(1, 0, INF, "controller", "assert", 1)).then(pseudonymise(1, 8))
    .expect(and { safety, ans(1, 0) == ctl(7), ans(1, 3) == ctl(7), ansS(1, 0, 1) == ctl(7), wLateNameHidden, wDuplicate })
  // an announcement (evidence tier) with a name is hidden too
  run announceHiddenDesignTest =
    init.then(add(1, 1, 3, "announce", "assert", 2)).then(tick).then(pseudonymise(1, 8))
      .expect(and { safety, ans(1, 1) == ann(2).with("unc", false).with("a", 8), ansS(1, 1, 0) == ann(2).with("unc", false).with("a", 8) })
  run basisScopedBreaksTest = departurePrefix.expect(and { not(pseudonymHidesName), noNameAtOrAfter })
  run lastWinsBreaksTest = departurePrefix.then(pseudonymise(1, 8)).expect(and { not(pseudonymStable), not(idempotent) })
  run asAssertBreaksTest = departurePrefix.then(tick).then(add(1, 0, INF, "controller", "assert", 1))
    .expect(and { not(noNameAtOrAfter), not(stateUnchanged) })

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
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "overlay").*
}
// design, keeping `view` for trace export (entities/bitemp's model-based test)
module designTrace {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = true, PSEU = "overlay").*
}
// strict precedence: the controller is never overridden by the overseer
module strict {
  import bitemporalCatalog(W = 100, W_REPLAY = 100, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "overlay").*
}
module noCeiling {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "noCeiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "overlay").*
}
module forward {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "forward", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "overlay").*
}
module sourceBlind {
  import bitemporalCatalog(W = 2, W_REPLAY = 0, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "overlay").*
}
module unknownAsRetract {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = true, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "overlay").*
}
module ignoreST {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = true, PRUNE = "tier", TRACE = false, PSEU = "overlay").*
}
module pruneAny {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "any", TRACE = false, PSEU = "overlay").*
}
module pruneBySt {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "st", TRACE = false, PSEU = "overlay").*
}
// O-G9 mutants
module basisScoped {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "basisScoped").*
}
module lastWins {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "lastWins").*
}
module asAssert {
  import bitemporalCatalog(W = 2, W_REPLAY = 2, REPLAY = "ceiling", UNKNOWN_AS_RETRACT = false, IGNORE_ST = false, PRUNE = "tier", TRACE = false, PSEU = "asAssert").*
}
