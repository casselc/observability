// -*- mode: Bluespec; -*-
/**
 * wmHistory: the history of the published `complete_through` (DECISIONS.md
 * D29, amendment 2026-10-01; FORMAT.md §4.1). The watermark publisher
 * (`consume gc`, several may run) writes one document by CAS; besides its
 * running max the document now holds the open hour's steps (`open`), each
 * a pair (at, v): "by wall time `at`, everything received below `v` was in
 * central". A run whose step falls in a later hour first FREEZES the open
 * hour (moves it into `sealing`, in the same CAS); after the CAS the frozen
 * hour is written create-only as the hour's object, then dropped from
 * `sealing`. A reader answers "as of T" with the largest `v` of a step whose
 * `at <= T`, from the objects and the document; the answer is FINAL when
 * T's hour is older than the document's open hour, PROVISIONAL otherwise.
 *
 * Time: `rt` is real time; a publisher's clock reads rt + off[p], |off| <=
 * SKEW (the configured bound). The truth is `ing`, the value central is
 * really complete through, and `ingLog(t)` its value at the end of real
 * time t. One publisher run is observe (reads the truth: its lanes'
 * checkpoints), GET (reads the document), CAS (writes it if unchanged).
 *
 * Properties:
 *  - `sound`: every step's v was true at its at: v <= ingLog(at) (a step
 *    stamped in the future is checked against the truth now);
 *  - `monotone`: a step stamped later never carries a lower value;
 *  - `answersHold`: a FINAL answer never changes; a PROVISIONAL one never
 *    falls (it may rise: a CAS that lands late adds a step to the open hour);
 *  - `historyBounded`: no step is above the published value.
 * Mutants:
 *  - STAMP_AFTER_GET = false (`stampEarly`): the stamp is the clock read at
 *    observation, before the document's GET: the GET may bring a value
 *    another publisher observed later (the running max), stamped too early;
 *  - ADD_SKEW = false (`noSkew`): the stamp is the bare clock reading: a
 *    publisher whose clock is behind claims a value before it was true;
 *  - CLAMP = false (`noClamp`): the stamp is not raised to the last step's:
 *    a publisher whose clock is behind appends a step that is earlier than
 *    the last one, into an hour already answered as final;
 *  - FREEZE_FIRST = false (`sealFromRead`): a run writes the hour's object
 *    from the document as it READ it, before (and whatever the outcome of)
 *    its CAS, and drops the hour from the document: a step another publisher
 *    then adds to that hour is in no object, and an answer given from it
 *    falls.
 */
module wmHistory {
  const PUBS: Set[int]
  const SKEW: int
  const HOUR: int
  const MAXT: int
  const MAXV: int
  const STAMP_AFTER_GET: bool
  const ADD_SKEW: bool
  const CLAMP: bool
  const FREEZE_FIRST: bool

  type Step = { at: int, v: int }
  type Doc = { ver: int, ct: int, hour: int, lastAt: int, open: Set[Step], sealing: int -> Set[Step] }
  type Answer = { t: int, v: int, fin: bool }

  pure val NODOC: Doc = { ver: 0, ct: 0, hour: 0, lastAt: 0, open: Set(), sealing: Map() }
  // publisher phases
  pure val IDLE = 0
  pure val OBSERVED = 1
  pure val READ = 2

  var rt: int
  var ing: int
  var ingLog: int -> int
  var off: int -> int
  var pc: int -> int
  var obs: int -> int
  var tObs: int -> int
  var tGet: int -> int
  var rd: int -> Doc
  var doc: Doc
  var objects: int -> Set[Step]
  var answers: Set[Answer]

  pure def maxOf(s: Set[int], d: int): int = s.fold(d, (a, x) => if (x > a) x else a)
  def clk(p: int): int = rt + off.get(p)
  // every step the history holds: the objects and the document
  def allSteps: Set[Step] =
    objects.keys().fold(doc.open, (a, h) => a.union(objects.get(h)))
      .union(doc.sealing.keys().fold(Set(), (a, h) => a.union(doc.sealing.get(h))))
  def ingAt(t: int): int = if (t >= rt) ing else ingLog.get(t)
  def asOf(t: int): int = maxOf(allSteps.filter(s => s.at <= t).map(s => s.v), -1)
  def final(t: int): bool = t / HOUR < doc.hour

  action init = all {
    rt' = 0,
    ing' = 0,
    ingLog' = Map(),
    nondet o = PUBS.setOfMaps((-SKEW).to(SKEW)).oneOf()
    off' = o,
    pc' = PUBS.mapBy(p => IDLE),
    obs' = PUBS.mapBy(p => 0),
    tObs' = PUBS.mapBy(p => 0),
    tGet' = PUBS.mapBy(p => 0),
    rd' = PUBS.mapBy(p => NODOC),
    doc' = NODOC,
    objects' = Map(),
    answers' = Set(),
  }

  action unchangedPubs = all { pc' = pc, obs' = obs, tObs' = tObs, tGet' = tGet, rd' = rd }

  action tick = all {
    rt < MAXT,
    rt' = rt + 1,
    ingLog' = ingLog.put(rt, ing),
    ing' = ing, off' = off, unchangedPubs, doc' = doc, objects' = objects, answers' = answers,
  }

  // central ingests: the truth rises
  action ingest = all {
    ing < MAXV,
    ing' = ing + 1,
    rt' = rt, ingLog' = ingLog, off' = off, unchangedPubs, doc' = doc, objects' = objects, answers' = answers,
  }

  // a run reads its lanes' checkpoints: what it sees is true now
  action observe(p: int): bool = all {
    pc.get(p) == IDLE,
    pc' = pc.set(p, OBSERVED),
    obs' = obs.set(p, ing),
    tObs' = tObs.set(p, clk(p)),
    tGet' = tGet, rd' = rd,
    rt' = rt, ing' = ing, ingLog' = ingLog, off' = off, doc' = doc, objects' = objects, answers' = answers,
  }

  // the GET of the document (a retry after a lost CAS comes back here)
  action getDoc(p: int): bool = all {
    pc.get(p) == OBSERVED,
    pc' = pc.set(p, READ),
    rd' = rd.set(p, doc),
    tGet' = tGet.set(p, clk(p)),
    obs' = obs, tObs' = tObs,
    rt' = rt, ing' = ing, ingLog' = ingLog, off' = off, doc' = doc, objects' = objects, answers' = answers,
  }

  def stampOf(p: int): int = {
    val s0 = if (STAMP_AFTER_GET) tGet.get(p) else tObs.get(p)
    val s1 = if (ADD_SKEW) s0 + SKEW else s0
    if (CLAMP and rd.get(p).lastAt > s1) rd.get(p).lastAt else s1
  }

  // the document the CAS writes, from the one read; `rec`: whether this run
  // records a step (the consumer records at most one per
  // --wm-history-every; a run in a later hour always does)
  def nextDoc(p: int, rec: bool): Doc = {
    val r = rd.get(p)
    val at = stampOf(p)
    val ct = if (obs.get(p) > r.ct) obs.get(p) else r.ct
    val h = at / HOUR
    val roll = h > r.hour
    val sealing = if (roll and r.open != Set() and FREEZE_FIRST) r.sealing.put(r.hour, r.open) else r.sealing
    val open = if (roll) Set({ at: at, v: ct }) else if (rec) r.open.union(Set({ at: at, v: ct })) else r.open
    val lastAt = if ((roll or rec) and at > r.lastAt) at else r.lastAt
    { ver: r.ver + 1, ct: ct, hour: if (roll) h else r.hour, lastAt: lastAt, open: open, sealing: sealing }
  }

  // sealFromRead: the hour's object from the document as read, before the CAS
  def earlyObjects(p: int): int -> Set[Step] = {
    val r = rd.get(p)
    val roll = stampOf(p) / HOUR > r.hour
    if (not(FREEZE_FIRST) and roll and r.open != Set() and not(objects.keys().contains(r.hour))) objects.put(r.hour, r.open) else objects
  }

  action cas(p: int, rec: bool): bool = all {
    pc.get(p) == READ,
    objects' = earlyObjects(p),
    if (doc.ver == rd.get(p).ver) all { doc' = nextDoc(p, rec), pc' = pc.set(p, IDLE) }
    else all { doc' = doc, pc' = pc.set(p, OBSERVED) },
    obs' = obs, tObs' = tObs, tGet' = tGet, rd' = rd,
    rt' = rt, ing' = ing, ingLog' = ingLog, off' = off, answers' = answers,
  }

  // after the freeze: the frozen hour, create-only (an existing object is
  // the same frozen content: left as it is), then dropped from `sealing`
  // (two steps in the consumer; one here, since the frozen content no
  // longer changes and the object's create is idempotent)
  action seal(h: int): bool = all {
    doc.sealing.keys().contains(h),
    objects' = if (objects.keys().contains(h)) objects else objects.put(h, doc.sealing.get(h)),
    doc' = { ...doc, ver: doc.ver + 1, sealing: doc.sealing.keys().exclude(Set(h)).mapBy(k => doc.sealing.get(k)) },
    rt' = rt, ing' = ing, ingLog' = ingLog, off' = off, unchangedPubs, answers' = answers,
  }

  // a reader asks "as of t" for a time up to now
  action ask(t: int): bool = all {
    t <= rt,
    asOf(t) >= 0,
    answers' = answers.union(Set({ t: t, v: asOf(t), fin: final(t) })),
    rt' = rt, ing' = ing, ingLog' = ingLog, off' = off, unchangedPubs, doc' = doc, objects' = objects,
  }

  action step = any {
    tick,
    ingest,
    nondet p = PUBS.oneOf()
    any { observe(p), getDoc(p), nondet rec = Set(true, false).oneOf() cas(p, rec) },
    all { doc.sealing.keys() != Set(), nondet h = doc.sealing.keys().oneOf() seal(h) },
    nondet t = 0.to(MAXT).oneOf()
    ask(t),
  }

  // ---- properties ----
  val sound = allSteps.forall(s => s.v <= ingAt(s.at))
  val monotone = allSteps.forall(a => allSteps.forall(b => a.at < b.at implies a.v <= b.v))
  val answersHold = answers.forall(a => if (a.fin) asOf(a.t) == a.v else asOf(a.t) >= a.v)
  val historyBounded = allSteps.forall(s => s.v <= doc.ct)
  val safety = sound and monotone and answersHold and historyBounded

  // ---- witnesses (expect VIOLATED for not(w)) ----
  val wSealed = objects.keys().size() >= 2
  val wFinalAnswer = answers.exists(a => a.fin and a.v > 0)
  val wProvisionalRose = answers.exists(a => not(a.fin) and asOf(a.t) > a.v)
  val wConcurrent = PUBS.forall(p => pc.get(p) == READ)

  // ---- scripted runs ----
  action setOff(p: int, o: int): bool = all {
    off' = off.set(p, o),
    rt' = rt, ing' = ing, ingLog' = ingLog, unchangedPubs, doc' = doc, objects' = objects, answers' = answers,
  }
  // (init picks every clock offset at random: a scripted run sets both)
  action run1(p: int): bool = observe(p).then(getDoc(p)).then(cas(p, true))

  // Two hours of runs by two publishers (1's clock 1 behind), sealed;
  // answers taken final stay final; the design is safe.
  run historyDesignTest =
    init.then(setOff(1, -1)).then(setOff(2, 0)).then(ingest).then(run1(1)).then(tick).then(ingest).then(run1(2))
      .then(tick).then(tick).then(ingest).then(run1(1)).then(seal(0)).then(ask(1)).then(tick).then(run1(2))
      .then(tick).then(tick).then(tick).then(run1(1)).then(seal(1)).then(ask(4))
      .expect(and { objects.keys() == Set(0, 1), answers.forall(a => a.fin), safety })

  // stampEarly: publisher 1 observes 1 at time 0; publisher 2 (its clock 1
  // behind) observes 2, ingested at time 2, and publishes it without a step
  // (downsampled; its stamp, 2, keeps hour 0 open); publisher 1's GET reads
  // 2 and records it under its early stamp, 1.
  run stampEarlyPrefix =
    init.then(setOff(1, 0)).then(setOff(2, -1)).then(ingest).then(observe(1)).then(tick).then(tick).then(ingest)
      .then(observe(2)).then(getDoc(2)).then(cas(2, false))
      .then(getDoc(1)).then(cas(1, true))
  run stampEarlyBreaksTest = stampEarlyPrefix.expect(not(sound))
  run stampEarlyDesignTest = stampEarlyPrefix.expect(safety)

  // noSkew: publisher 1's clock is 1 behind; it observes 1 at real time 1
  // (ingested during 1) and, without the skew, stamps 0.
  run noSkewPrefix = init.then(setOff(1, -1)).then(setOff(2, 0)).then(tick).then(ingest).then(run1(1))
  run noSkewBreaksTest = noSkewPrefix.expect(not(sound))
  run noSkewDesignTest = noSkewPrefix.expect(safety)

  // noClamp (SKEW 2): publisher 1 (2 behind) records hour 0; publisher 2 (2
  // ahead) records at 4, freezing hour 0, which is sealed; "as of 1" is
  // answered final; publisher 1 then stamps 1 with a higher value.
  run noClampPrefix =
    init.then(setOff(1, -2)).then(setOff(2, 2)).then(ingest).then(run1(1))
      .then(ingest).then(run1(2)).then(seal(0)).then(tick).then(ask(1))
      .then(ingest).then(run1(1))
  run noClampBreaksTest = noClampPrefix.expect(not(answersHold))
  run noClampDesignTest = noClampPrefix.expect(safety)

  // sealFromRead (SKEW 2): publisher 2 records (2, 1) in hour 0; at time 1
  // it reads the document (its next stamp, 3, is in hour 1); publisher 1 (2
  // behind, clamped to 2) adds (2, 2) to hour 0; publisher 2's CAS loses,
  // but the mutant already wrote hour 0's object from its read. "As of 2" is
  // answered 2 (provisional); publisher 2's retry drops hour 0 from the
  // document (its object exists), and the answer falls to 1.
  run sealFromReadPrefix =
    init.then(setOff(1, -2)).then(setOff(2, 0)).then(ingest).then(run1(2)).then(tick)
      .then(observe(2)).then(getDoc(2)).then(ingest).then(run1(1))
      .then(cas(2, true)).then(tick).then(ask(2)).then(getDoc(2)).then(cas(2, true))
  run sealFromReadBreaksTest = sealFromReadPrefix.expect(not(answersHold))
  run sealFromReadDesignTest = sealFromReadPrefix.then(seal(0)).expect(and { objects.get(0).size() == 2, safety })
}

// The design.
module historyDesign {
  import wmHistory(PUBS = Set(1, 2), SKEW = 1, HOUR = 3, MAXT = 8, MAXV = 4,
    STAMP_AFTER_GET = true, ADD_SKEW = true, CLAMP = true, FREEZE_FIRST = true).*
}
module stampEarly {
  import wmHistory(PUBS = Set(1, 2), SKEW = 1, HOUR = 3, MAXT = 8, MAXV = 4,
    STAMP_AFTER_GET = false, ADD_SKEW = true, CLAMP = true, FREEZE_FIRST = true).*
}
module noSkew {
  import wmHistory(PUBS = Set(1, 2), SKEW = 1, HOUR = 3, MAXT = 8, MAXV = 4,
    STAMP_AFTER_GET = true, ADD_SKEW = false, CLAMP = true, FREEZE_FIRST = true).*
}
module noClamp {
  import wmHistory(PUBS = Set(1, 2), SKEW = 2, HOUR = 3, MAXT = 8, MAXV = 4,
    STAMP_AFTER_GET = true, ADD_SKEW = true, CLAMP = false, FREEZE_FIRST = true).*
}
module sealFromRead {
  import wmHistory(PUBS = Set(1, 2), SKEW = 2, HOUR = 3, MAXT = 8, MAXV = 4,
    STAMP_AFTER_GET = true, ADD_SKEW = true, CLAMP = true, FREEZE_FIRST = false).*
}
