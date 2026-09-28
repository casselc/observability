// -*- mode: Bluespec; -*-
/**
 * completeness: `complete_through`, the watermark a reader publishes so that
 * an alert window is evaluated only once every request received in it is
 * readable (research/central-optional.md §5.4; STPA LS-6, LS-7, LS-8;
 * requirements R-S1 and R-S3).
 *
 * The edge side is s3Inline's writer reduced to what the watermark needs:
 * one lane per edge, one incarnation (epoch) at a time, slots written in
 * order and never abandoned (s3Inline's resolve-by-HEAD: a lost PUT is resent
 * with identical bytes), a durable custody buffer (D19) that survives a
 * crash, and PUTs that can land after their writer died (zombie PUTs).
 * received_at (`r`) is the custody time and a replay keeps it (D19). The
 * writer may take any request from its buffer, not only the oldest: an
 * exporter runs several queue consumers, and a failed request goes back.
 *
 * What the reader knows is only what S3 shows it (LIST + HEAD metadata) and
 * what it has ingested. So the model's question is: can `complete_through`
 * be computed from that? It can, if every object carries one more metadata
 * field, and not otherwise:
 *
 *   low(k) = min(the PUT time of slot k, received_at over the requests in
 *            the edge's custody when that PUT was first sent, other than
 *            k's own)                                      x-amz-meta-oscope-low
 *
 * Claim: every request with received_at < low(k) is committed at k or at a
 * slot before it. (Any other one left custody before k was sent, and a
 * request leaves custody only once its slot is seen committed; a request
 * received later has received_at >= the PUT time >= low(k).) A resend
 * carries the same bytes, so the same low: still a lower bound. low is
 * monotone along a lane: the custody minimum only rises, since new requests
 * arrive at `now`.
 *
 * So, per reader (central's consumer, or the lake's sealer):
 *
 *   laneWm(l)  = max low(k) over the lane's visible objects k such that k and
 *                every visible object before k (by (epoch, seq)) are ingested
 *   complete_through = min over known lanes of laneWm(l), published as a
 *                running max (it never goes back)
 *
 * and every request with received_at < complete_through is ingested
 * (`completeSound`). What does NOT block the prefix, and why that is sound:
 *  - free slots (gaps): a PUT not yet landed is for a request still in
 *    custody, so it is below no later object's low;
 *  - a zombie PUT landing late in a superseded epoch: its request stayed in
 *    custody across the crash, so the next incarnation replays it and the
 *    lows stay at or below its received_at until the replay commits; the late
 *    copy only makes the recomputed value dip (`wRegress`), which is why the
 *    published value is a running max;
 *  - tombstones carry no low and are not modelled here (they bound where a
 *    zombie can land; s3Inline and s3InlineConsumer cover them).
 *
 * Idle lanes: a lane with nothing in custody commits a heartbeat slot, an
 * empty object whose low is its PUT time (HEARTBEAT). Without heartbeats, an
 * idle lane holds complete_through at its last low forever. Counting a lane
 * with nothing new as "caught up to the LIST time" (§5.4 as first written) is
 * unsound: from S3 an idle lane and an offline edge with a full buffer look
 * the same. A lane that stops advancing while windows close is paged after
 * STALE (an edge offline is an incident), never excluded silently.
 *
 * Lanes must be known before they take custody: each lane's first object (a
 * birth heartbeat, low 0) is committed before its edge accepts data (BIRTH).
 * A reader that computes the minimum over the lanes it happens to have seen
 * skips an edge that has data but has not published yet.
 *
 * The alert evaluator (R-S3): a window [start, end) is evaluated only once
 * end <= the serving source's complete_through; the verdict is FIRE if any
 * error request of the window is ingested, else OK. With no source reachable
 * it pages after DEADLINE; with a source whose watermark stalls it pages
 * after STALE. The query service (R-S1) prefers central and falls back to the
 * lake, and every result carries its source and that source's own
 * complete_through.
 *
 * Mutations (design flags):
 *  - LOW_META = false (`lastReceived`): an object's low is its own
 *    received_at, as if the reader used "the lane's last received_at seen";
 *    an older request still in custody is passed.
 *  - IDLE_AS_LIST (`listTimeIdle`): a lane with nothing un-ingested counts as
 *    complete to the LIST time (now).
 *  - PREFIX = false (`maxNotPrefix`): the lane's watermark is the highest
 *    ingested object's low even with an earlier object not yet ingested (a
 *    partial statement, D10).
 *  - BIRTH = false (`noBirth`): the minimum is over lanes that have published.
 *  - GATE = false (`evalPastComplete`, LS-7): the evaluator runs once the
 *    wall clock passes the window's end.
 *  - NODATA_OK (`noDataOk`, LS-8): with no source reachable the evaluation
 *    reads an empty result, OK.
 *  - LABEL = false (`unlabeledFallback`, LS-6): a result served by the lake
 *    carries central's watermark.
 *  - HEARTBEAT = false (`noHeartbeat`): no safety violation; an idle lane
 *    stalls every window until it pages (a scripted run shows it).
 *
 * Per cluster (D29). Each lane belongs to a cluster (CLUSTER_OF): one lane
 * per cluster in the instances above, and in the cluster instances below
 * lanes 1 and 2 are cluster 1's and lane 3 is cluster 2's. Each
 * publication also publishes, per cluster, the running max of the minimum
 * over that cluster's lanes, floored by the new fleet value (`cwm`);
 * `clusterSound` is completeSound per cluster: no lane of the cluster can
 * still commit a request below it. The evaluator's rule covers the clusters
 * SCOPE, counts only their requests, and (PER_CLUSTER) gates on the minimum
 * of their `cwm`, so a rule on cluster 2 evaluates while cluster 1's lane
 * holds the fleet value back (`wIsolatedEval`, `stalledClusterScopedTest`);
 * PER_CLUSTER = false is the fleet gate before D29 (safe, and in the same run
 * it cannot evaluate). Mutants:
 *  - SCOPE_MAX (`scopeMax`): the label takes the maximum over the scope's
 *    clusters instead of the minimum;
 *  - SPLIT (`clusterSplit`): a cluster's value misses one of its lanes (lane
 *    2 counted in no cluster: a lane id parsed into the wrong cluster).
 */
module completeness {
  const LOW_META: bool
  const IDLE_AS_LIST: bool
  const PREFIX: bool
  const PENDING: bool
  const BIRTH: bool
  const HEARTBEAT: bool
  const GATE: bool
  const NODATA_OK: bool
  const LABEL: bool
  const LANES: Set[int]
  const CLUSTER_OF: int -> int
  const PER_CLUSTER: bool
  const SCOPE_MAX: bool
  const SPLIT: bool
  const SCOPE: Set[int]

  val CLUSTERS: Set[int] = LANES.map(l => CLUSTER_OF.get(l))
  def clusterOf(l: int): int = CLUSTER_OF.get(l)
  pure val SOURCES: Set[int] = Set(1, 2)     // 1: central (the consumer), 2: the lake (the sealer)
  pure val CENTRAL: int = 1
  pure val LAKE: int = 2
  pure val NOSRC: int = 0
  pure val MAX_T: int = 6
  pure val MAX_REQ: int = 3                  // requests received, over all lanes
  pure val MAX_EPOCH: int = 2
  pure val MAX_SEQ: int = 4                  // slots 0 .. MAX_SEQ - 1 per epoch
  pure val WIN: int = 2                      // alert windows [k * WIN, (k + 1) * WIN)
  pure val WINDOWS: Set[int] = Set(0, 1)
  pure val DEADLINE: int = 1                 // no source: page by end + DEADLINE
  pure val STALE: int = 2                    // a stalled watermark: page by end + STALE
  pure val NEVER: int = 1000

  type Kind = Data | Beat
  // A slot of lane `lane`: `req` is the request id (0 for a heartbeat).
  type Obj = { lane: int, epoch: int, seq: int, kind: Kind, req: int, r: int, low: int }
  type Req = { id: int, lane: int, r: int, err: bool }
  // The writer of the lane's current incarnation; `busy`: its slot `cur` is
  // sent and not yet seen committed.
  type Writer = { epoch: int, seq: int, busy: bool, cur: Obj }
  type Verdict = Pending | Ok | Fire | PageStale | PageNoSource
  type Result = { window: int, src: int, through: int, trueWm: int, count: int }

  // ---- state ----------------------------------------------------------------------
  var now: int
  var nextId: int
  var buffer: int -> Set[Req]      // lane -> requests in the edge's custody (durable)
  var history: Set[Req]            // every request ever received
  var writers: int -> Writer
  var puts: Set[Obj]               // PUTs in flight (they may land after their writer died)
  var objs: Set[Obj]               // committed slots (what LIST shows)
  var ing: int -> Set[Obj]         // source -> slots it has ingested / sealed
  var up: int -> bool              // source reachable
  var wm: int -> int               // source -> published complete_through
  var cwm: int -> (int -> int)     // source -> cluster -> published per-cluster complete_through (D29)
  var verdict: int -> Verdict      // window -> verdict
  var results: Set[Result]
  var events: Set[str]

  // ---- pure logic --------------------------------------------------------------------
  pure val NOOBJ: Obj = { lane: 0, epoch: 0, seq: 0, kind: Beat, req: 0, r: 0, low: 0 }
  pure def imax(a: int, b: int): int = if (a >= b) a else b
  pure def imin(a: int, b: int): int = if (a <= b) a else b
  pure def before(p: Obj, o: Obj): bool = p.epoch < o.epoch or (p.epoch == o.epoch and p.seq < o.seq)
  pure def wStart(k: int): int = k * WIN
  pure def wEnd(k: int): int = (k + 1) * WIN
  pure def inWin(r: int, k: int): bool = wStart(k) <= r and r < wEnd(k)
  pure def birth(l: int): Obj = { lane: l, epoch: 1, seq: 0, kind: Beat, req: 0, r: 0, low: 0 }

  // The low an object carries: the oldest received_at in custody besides its
  // own request, or now (design); or its own received_at (mutant).
  def lowOf(l: int, q: Req): int =
    if (LOW_META) buffer.get(l).filter(x => x.id != q.id).fold(now, (m, x) => imin(m, x.r)) else q.r

  // The lane's watermark as source s can compute it.
  // With PENDING (the implementation, ../FORMAT.md §3, otap-rs
  // `plan::lane_wm`): the highest low over every ingested object, capped by
  // the lowest received_at of the visible data objects not ingested yet; no
  // order between objects is needed (several writer lanes share a custody).
  def laneWm(s: int, l: int): int = {
    val lo = objs.filter(o => o.lane == l)
    val done = ing.get(s)
    if (IDLE_AS_LIST and lo.forall(o => done.contains(o))) now
    else if (PENDING)
      imin(lo.filter(o => done.contains(o)).fold(-1, (m, o) => imax(m, o.low)),
           lo.filter(o => not(done.contains(o)) and o.kind == Data).fold(NEVER, (m, o) => imin(m, o.r)))
    else
      lo.filter(o => done.contains(o) and (not(PREFIX) or lo.forall(p => before(p, o) implies done.contains(p))))
        .fold(-1, (m, o) => imax(m, o.low))
  }
  def knownLanes: Set[int] = LANES.filter(l => BIRTH or objs.exists(o => o.lane == l))
  def computedWm(s: int): int = knownLanes.fold(now, (m, l) => imin(m, laneWm(s, l)))
  // A cluster's lanes (SPLIT: lane 2 lands in no cluster).
  def member(l: int, c: int): bool = clusterOf(l) == c and not(SPLIT and l == 2)
  def clusterComputed(s: int, c: int): int = knownLanes.filter(l => member(l, c)).fold(now, (m, l) => imin(m, laneWm(s, l)))
  // The rule's scope: its clusters' requests.
  def inScope(l: int): bool = SCOPE.contains(clusterOf(l))
  // The scope's sound value: the minimum of its clusters' published values.
  def trueScope(s: int): int = SCOPE.fold(NEVER, (m, c) => imin(m, cwm.get(s).get(c)))
  // What the label says for the scope.
  def scopeWm(s: int): int =
    if (not(PER_CLUSTER)) wm.get(s)
    else if (SCOPE_MAX) SCOPE.fold(-1, (m, c) => imax(m, cwm.get(s).get(c)))
    else trueScope(s)

  // What an alert on window k counts from source s: ingested error requests in it.
  def errorsIn(s: int, k: int): int =
    history.filter(q => q.err and inScope(q.lane) and inWin(q.r, k) and ing.get(s).exists(o => o.kind == Data and o.req == q.id)).size()

  // The query service: central if reachable, else the lake.
  def served: int = if (up.get(CENTRAL)) CENTRAL else if (up.get(LAKE)) LAKE else NOSRC
  // The freshness a result is labelled with.
  def labelOf(s: int): int = if (LABEL) scopeWm(s) else scopeWm(CENTRAL)
  // The evaluator's gate.
  def gateOpen(k: int, s: int): bool = if (GATE) wEnd(k) <= labelOf(s) else now >= wEnd(k)
  def mayEval(k: int): bool = verdict.get(k) == Pending and served != NOSRC and gateOpen(k, served)

  // ---- helpers ----------------------------------------------------------------------
  action keepEdge = all { nextId' = nextId, buffer' = buffer, history' = history, writers' = writers, puts' = puts, objs' = objs }
  action keepRead = all { ing' = ing, up' = up, wm' = wm, cwm' = cwm, verdict' = verdict, results' = results }

  // ---- the edge ---------------------------------------------------------------------
  // A request enters the edge's custody now (received_at = now).
  action receive(l: int, err: bool): bool = {
    val q = { id: nextId, lane: l, r: now, err: err }
    all {
      nextId <= MAX_REQ,
      buffer' = buffer.setBy(l, b => b.union(Set(q))), history' = history.union(Set(q)),
      nextId' = nextId + 1,
      now' = now, writers' = writers, puts' = puts, objs' = objs, events' = events, keepRead,
    }
  }

  // The writer takes request q from custody (any of them) and PUTs it to its
  // next slot, create-only; the object's metadata carries received_at and low.
  action send(l: int, q: Req): bool = {
    val w = writers.get(l)
    val o = { lane: l, epoch: w.epoch, seq: w.seq, kind: Data, req: q.id, r: q.r, low: lowOf(l, q) }
    all {
      not(w.busy), w.seq < MAX_SEQ, buffer.get(l).contains(q),
      puts' = puts.union(Set(o)),
      writers' = writers.set(l, { ...w, busy: true, cur: o }),
      events' = events.union(if (buffer.get(l).exists(x => x.r < q.r)) Set("notOldestFirst") else Set()),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, objs' = objs, keepRead,
    }
  }

  // S3 applies a PUT (create-only: a resend carries identical bytes).
  action land(o: Obj): bool = all {
    puts.contains(o),
    puts' = puts.exclude(Set(o)),
    objs' = objs.union(Set(o)),
    events' = events.union(if (o.epoch < writers.get(o.lane).epoch) Set("zombieLanded") else Set()),
    now' = now, nextId' = nextId, buffer' = buffer, history' = history, writers' = writers, keepRead,
  }
  action lose(o: Obj): bool = all {
    puts.contains(o),
    puts' = puts.exclude(Set(o)),
    now' = now, nextId' = nextId, buffer' = buffer, history' = history, writers' = writers, objs' = objs, events' = events, keepRead,
  }

  // The writer sees its slot committed (a 200, or a HEAD after a timeout):
  // the request leaves custody (the buffer's ACK).
  action ack(l: int): bool = {
    val w = writers.get(l)
    all {
      w.busy, objs.contains(w.cur),
      buffer' = buffer.setBy(l, b => b.filter(x => x.id != w.cur.req)),
      writers' = writers.set(l, { ...w, busy: false, seq: w.seq + 1, cur: NOOBJ }),
      now' = now, nextId' = nextId, history' = history, puts' = puts, objs' = objs, events' = events, keepRead,
    }
  }

  // The PUT was lost; HEAD finds the slot free: resend identical bytes.
  action resend(l: int): bool = {
    val w = writers.get(l)
    all {
      w.busy, not(objs.contains(w.cur)), not(puts.contains(w.cur)),
      puts' = puts.union(Set(w.cur)),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, writers' = writers, objs' = objs, events' = events, keepRead,
    }
  }

  // The edge process dies and restarts: a new epoch; custody survives (the
  // WAL); PUTs already sent may still land in the old epoch.
  action crash(l: int): bool = {
    val w = writers.get(l)
    all {
      w.epoch < MAX_EPOCH,
      writers' = writers.set(l, { epoch: w.epoch + 1, seq: 0, busy: false, cur: NOOBJ }),
      events' = events.union(Set(if (w.busy) "crashInFlight" else "crash")),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, puts' = puts, objs' = objs, keepRead,
    }
  }

  // Nothing in custody: commit a heartbeat slot whose low is now.
  action beat(l: int): bool = {
    val w = writers.get(l)
    val o = { lane: l, epoch: w.epoch, seq: w.seq, kind: Beat, req: 0, r: now, low: now }
    all {
      HEARTBEAT, not(w.busy), w.seq < MAX_SEQ, buffer.get(l) == Set(),
      objs.filter(x => x.lane == l).forall(x => x.low < now),
      objs' = objs.union(Set(o)),
      writers' = writers.set(l, { ...w, seq: w.seq + 1 }),
      events' = events.union(Set("beat")),
      now' = now, nextId' = nextId, buffer' = buffer, history' = history, puts' = puts, keepRead,
    }
  }

  action tick: bool = all { now < MAX_T, now' = now + 1, keepEdge, keepRead, events' = events }

  // ---- the readers --------------------------------------------------------------------
  // Source s ingests (central) or seals (the lake) a committed slot, in any
  // order (a statement may land in part).
  action ingest(s: int, o: Obj): bool = all {
    up.get(s), objs.contains(o), not(ing.get(s).contains(o)),
    ing' = ing.setBy(s, x => x.union(Set(o))),
    now' = now, keepEdge, up' = up, wm' = wm, cwm' = cwm, verdict' = verdict, results' = results, events' = events,
  }

  // Publish complete_through (in the checkpoint, or the snapshot summary).
  action publish(s: int): bool = {
    val c = computedWm(s)
    val nw = imax(wm.get(s), c)
    // D29: each cluster's running max, floored by the new fleet value
    val nc = CLUSTERS.mapBy(k => imax(cwm.get(s).get(k), imax(nw, clusterComputed(s, k))))
    all {
      up.get(s), c != wm.get(s) or nc != cwm.get(s),
      wm' = wm.set(s, nw),
      cwm' = cwm.set(s, nc),
      events' = events.union(if (c < wm.get(s)) Set("regress") else Set()),
      now' = now, keepEdge, ing' = ing, up' = up, verdict' = verdict, results' = results,
    }
  }

  action flip(s: int): bool = all {
    up' = up.set(s, not(up.get(s))),
    events' = events.union(Set(if (up.get(s)) "down" else "up")),
    now' = now, keepEdge, ing' = ing, wm' = wm, cwm' = cwm, verdict' = verdict, results' = results,
  }

  // ---- the alert evaluator ----------------------------------------------------------------
  action evaluate(k: int): bool = {
    val s = served
    val n = errorsIn(s, k)
    all {
      mayEval(k),
      verdict' = verdict.set(k, if (n > 0) Fire else Ok),
      results' = results.union(Set({ window: k, src: s, through: labelOf(s), trueWm: trueScope(s), count: n })),
      events' = events.union(Set(if (s == LAKE) "lakeServed" else "centralServed")),
      now' = now, keepEdge, ing' = ing, up' = up, wm' = wm, cwm' = cwm,
    }
  }

  // No source answers: page (design), or read no data as OK (NODATA_OK).
  action noSource(k: int): bool = all {
    verdict.get(k) == Pending, served == NOSRC,
    if (NODATA_OK) all { now >= wEnd(k), verdict' = verdict.set(k, Ok) }
    else all { now >= wEnd(k) + DEADLINE, verdict' = verdict.set(k, PageNoSource) },
    events' = events.union(Set("noSource")),
    now' = now, keepEdge, ing' = ing, up' = up, wm' = wm, cwm' = cwm, results' = results,
  }

  // A source answers but its watermark has not reached the window: page once it is STALE.
  action stale(k: int): bool = all {
    verdict.get(k) == Pending, served != NOSRC, not(gateOpen(k, served)), now >= wEnd(k) + STALE,
    verdict' = verdict.set(k, PageStale),
    events' = events.union(Set("stale")),
    now' = now, keepEdge, ing' = ing, up' = up, wm' = wm, cwm' = cwm, results' = results,
  }

  // ---- state machine ------------------------------------------------------------------------
  action init = all {
    now' = 0,
    nextId' = 1,
    buffer' = LANES.mapBy(_ => Set()),
    history' = Set(),
    // with BIRTH each lane's first slot is a heartbeat committed before it takes custody
    writers' = LANES.mapBy(_ => { epoch: 1, seq: if (BIRTH) 1 else 0, busy: false, cur: NOOBJ }),
    puts' = Set(),
    objs' = if (BIRTH) LANES.map(l => birth(l)) else Set(),
    ing' = SOURCES.mapBy(_ => Set()),
    up' = SOURCES.mapBy(_ => true),
    wm' = SOURCES.mapBy(_ => 0),
    cwm' = SOURCES.mapBy(_ => CLUSTERS.mapBy(_ => 0)),
    verdict' = WINDOWS.mapBy(_ => Pending),
    results' = Set(),
    events' = Set(),
  }

  action step = {
    nondet l = LANES.oneOf()
    nondet s = SOURCES.oneOf()
    nondet k = WINDOWS.oneOf()
    nondet err = Set(true, false).oneOf()
    any {
      receive(l, err),
      all { buffer.get(l) != Set(), nondet q = buffer.get(l).oneOf() send(l, q) },
      all { puts != Set(), nondet o = puts.oneOf() any { land(o), lose(o) } },
      ack(l), resend(l), crash(l), beat(l), tick,
      all { objs != Set(), nondet o = objs.oneOf() ingest(s, o) },
      publish(s), flip(s),
      evaluate(k), noSource(k), stale(k),
    }
  }

  // ---- properties -----------------------------------------------------------------------------
  // The derivation: every request received before a source's published
  // complete_through is ingested there (by some copy).
  val completeSound: bool = SOURCES.forall(s =>
    history.forall(q => q.r < wm.get(s) implies ing.get(s).exists(o => o.kind == Data and o.req == q.id)))
  // LS-7: an OK verdict means the window had no error request, ever.
  val okMeansNoErrors: bool = WINDOWS.forall(k =>
    verdict.get(k) == Ok implies not(history.exists(q => q.err and inScope(q.lane) and inWin(q.r, k))))
  // D29: per cluster, every request of the cluster received before its
  // published value is ingested (no lane of it can still commit below it).
  val clusterSound: bool = SOURCES.forall(s => CLUSTERS.forall(c =>
    history.forall(q => clusterOf(q.lane) == c and q.r < cwm.get(s).get(c) implies ing.get(s).exists(o => o.kind == Data and o.req == q.id))))
  // R-S3: the evaluator never evaluates past the serving source's complete_through.
  val evalWithinComplete: bool = results.forall(x => wEnd(x.window) <= x.trueWm)
  // R-S1: every result is labelled with its source and no more freshness than that source has.
  val resultLabeled: bool = results.forall(x => x.src != NOSRC and x.through <= x.trueWm)
  // LS-8: an OK comes only from a source's answer.
  val noSilentOk: bool = WINDOWS.forall(k => verdict.get(k) == Ok implies results.exists(x => x.window == k))
  // The published watermark never passes the clock.
  val wmBounded: bool = SOURCES.forall(s => wm.get(s) <= now and CLUSTERS.forall(c => cwm.get(s).get(c) <= now))
  val safety: bool = and { completeSound, clusterSound, okMeansNoErrors, evalWithinComplete, resultLabeled, noSilentOk, wmBounded }

  // ---- witnesses (a violation of not(w) means w was reached) ------------------------------------
  val wFire: bool = WINDOWS.exists(k => verdict.get(k) == Fire)
  val wOk: bool = WINDOWS.exists(k => verdict.get(k) == Ok)
  // A window evaluated OK in which some lane committed no data (it was idle; its heartbeat carried it).
  val wOkIdleLane: bool = WINDOWS.exists(k => verdict.get(k) == Ok and
    LANES.exists(l => not(objs.exists(o => o.lane == l and o.kind == Data and inWin(o.r, k)))))
  val wLakeServed: bool = events.contains("lakeServed") and wFire
  val wPageNoSource: bool = WINDOWS.exists(k => verdict.get(k) == PageNoSource)
  val wPageStale: bool = WINDOWS.exists(k => verdict.get(k) == PageStale)
  // A request committed in two epochs (a replay after a crash) inside an evaluated window.
  val wReplayEvaluated: bool = objs.exists(a => objs.exists(b => a.req == b.req and a.req != 0 and a.epoch < b.epoch and
    WINDOWS.exists(k => inWin(a.r, k) and verdict.get(k) != Pending)))
  val wZombieLanded: bool = events.contains("zombieLanded")
  // The recomputed watermark dipped below the published one (a late copy in a superseded epoch).
  val wRegress: bool = events.contains("regress")
  val wNotOldestFirst: bool = events.contains("notOldestFirst") and wFire
  val wAllWindows: bool = WINDOWS.forall(k => verdict.get(k) == Ok or verdict.get(k) == Fire)
  // D29: a cluster's published value passed the fleet's (another cluster holds the fleet back).
  val wClusterAhead: bool = SOURCES.exists(s => CLUSTERS.exists(c => cwm.get(s).get(c) > wm.get(s)))
  // D29: a window evaluated past the serving source's fleet value (its scope's clusters were complete).
  val wIsolatedEval: bool = results.exists(x => wEnd(x.window) > wm.get(x.src))

  // ---- scripted scenarios ----------------------------------------------------------------------------
  // (Each mutant's counterexample as a fixed run: `*BreaksTest` on the mutant
  // ends with the invariant broken; `*DesignTest` runs the same prefix on the
  // design, where the fatal step is disabled.)
  def req(i: int): Req = history.filter(q => q.id == i).fold({ id: 0, lane: 0, r: 0, err: false }, (a, q) => q)
  def obj(i: int): Obj = objs.filter(o => o.req == i).fold(NOOBJ, (a, o) => o)
  def beatOf(l: int, t: int): Obj = objs.filter(o => o.lane == l and o.kind == Beat and o.low == t).fold(NOOBJ, (a, o) => o)
  // Lane l commits request i: put, land, ack.
  action commit(l: int, i: int): bool =
    send(l, req(i)).then(all { puts != Set(), nondet o = puts.oneOf() land(o) }).then(ack(l))
  // Source s ingests everything committed.
  action ingestAll(s: int): bool = all {
    up.get(s),
    ing' = ing.set(s, objs),
    now' = now, keepEdge, up' = up, wm' = wm, cwm' = cwm, verdict' = verdict, results' = results, events' = events,
  }

  // An error received at t=0 on lane 1 stays in custody (the edge is offline
  // or slow); lane 2 is idle and heartbeats at t=2; central ingests all it sees.
  run custodyPrefix =
    init.then(receive(1, true)).then(tick).then(tick).then(beat(2)).then(ingestAll(CENTRAL))
  // Design: lane 1's watermark is its birth low 0, so window 0 can't be evaluated.
  run listTimeIdleDesignTest =
    custodyPrefix.expect(and { computedWm(CENTRAL) == 0, wm.get(CENTRAL) == 0, not(mayEval(0)) })
  // ... and once lane 1 publishes, the window fires.
  run custodyThenFiresDesignTest =
    custodyPrefix.then(commit(1, 1)).then(ingestAll(CENTRAL)).then(publish(CENTRAL)).then(evaluate(0))
      .expect(and { verdict.get(0) == Fire, safety })
  run listTimeIdleBreaksTest =
    custodyPrefix.then(publish(CENTRAL)).then(evaluate(0)).expect(and { not(completeSound), not(okMeansNoErrors) })
  run noBirthBreaksTest = listTimeIdleBreaksTest

  // Not oldest first: an error at t=0 and a request at t=2 on lane 1; the
  // writer sends the newer one first.
  run orderPrefix =
    init.then(receive(1, true)).then(tick).then(tick).then(receive(1, false)).then(commit(1, 2)).then(beat(2))
      .then(ingestAll(CENTRAL))
  run lastReceivedDesignTest = orderPrefix.expect(and { obj(2).low == 0, computedWm(CENTRAL) == 0, not(mayEval(0)) })
  run lastReceivedBreaksTest =
    orderPrefix.then(publish(CENTRAL)).then(evaluate(0)).expect(and { not(completeSound), not(okMeansNoErrors) })

  // A partial statement: lane 1 commits an error (t=0) and a request (t=2);
  // central ingests the second and not the first.
  run prefixPrefix =
    init.then(receive(1, true)).then(commit(1, 1)).then(tick).then(tick).then(receive(1, false)).then(commit(1, 2))
      .then(beat(2)).then(ingest(CENTRAL, birth(1))).then(ingest(CENTRAL, obj(2))).then(ingest(CENTRAL, birth(2)))
      .then(ingest(CENTRAL, beatOf(2, 2)))
  run maxNotPrefixDesignTest = prefixPrefix.expect(and { computedWm(CENTRAL) == 0, not(mayEval(0)) })
  run maxNotPrefixBreaksTest =
    prefixPrefix.then(publish(CENTRAL)).then(evaluate(0)).expect(and { not(completeSound), not(okMeansNoErrors) })

  // The wall clock passes a window whose error is still in custody.
  run evalPastCompleteBreaksTest =
    init.then(receive(1, true)).then(tick).then(tick).then(evaluate(0))
      .expect(and { not(evalWithinComplete), not(okMeansNoErrors) })
  run evalPastCompleteDesignTest = init.then(receive(1, true)).then(tick).then(tick).expect(not(mayEval(0)))

  // No source reachable after the window closed.
  run noSourcePrefix = init.then(receive(1, true)).then(flip(CENTRAL)).then(flip(LAKE)).then(tick).then(tick)
  run noDataOkBreaksTest = noSourcePrefix.then(noSource(0)).expect(and { not(noSilentOk), not(okMeansNoErrors) })
  run noDataOkDesignTest =
    noSourcePrefix.expect(now < wEnd(0) + DEADLINE).then(tick).then(noSource(0)).expect(and { verdict.get(0) == PageNoSource, safety })

  // Central goes down after reaching t=2; the lake has sealed only the births.
  run fallbackPrefix =
    init.then(receive(1, true)).then(tick).then(tick).then(commit(1, 1)).then(beat(2))
      .then(ingestAll(CENTRAL)).then(publish(CENTRAL)).then(ingest(LAKE, birth(1))).then(ingest(LAKE, birth(2)))
      .then(flip(CENTRAL))
  run unlabeledFallbackBreaksTest =
    fallbackPrefix.then(evaluate(0)).expect(and { not(resultLabeled), not(evalWithinComplete), not(okMeansNoErrors) })
  // Design: the lake's own label (0) keeps the window closed; it pages when stale, or fires once sealed.
  run unlabeledFallbackDesignTest =
    fallbackPrefix.expect(and { wm.get(CENTRAL) == 2, wm.get(LAKE) == 0, not(mayEval(0)) })
  run fallbackSealedDesignTest =
    fallbackPrefix.then(ingestAll(LAKE)).then(publish(LAKE)).then(evaluate(0))
      .expect(and { verdict.get(0) == Fire, results.forall(x => x.src == LAKE), safety })
  run fallbackStaleDesignTest =
    fallbackPrefix.then(tick).then(tick).then(stale(0)).expect(and { verdict.get(0) == PageStale, safety })

  // Idle lanes: lane 1 commits a request at t=2 (its low is 2: nothing else in
  // custody); lane 2 has nothing at all.
  run idlePrefix = init.then(tick).then(tick).then(receive(1, false)).then(commit(1, 1))
  // With heartbeats lane 2 carries the watermark to t=2 and window 0 closes OK.
  run idleLaneBeatsDesignTest =
    idlePrefix.then(beat(2)).then(ingestAll(CENTRAL)).then(publish(CENTRAL)).then(evaluate(0))
      .expect(and { verdict.get(0) == Ok, wOkIdleLane, safety })
  // Without them (noHeartbeat) lane 2 holds it at 0: the window can only page, once stale.
  run idleLaneStallsTest =
    idlePrefix.then(ingestAll(CENTRAL)).expect(computedWm(CENTRAL) == 0).then(tick).then(tick).then(stale(0))
      .expect(and { wm.get(CENTRAL) == 0, verdict.get(0) == PageStale, safety })

  // A crash with a PUT in flight: the request (an error, t=0) is replayed in
  // epoch 2 and the zombie PUT lands later in epoch 1. The published
  // watermark holds; the recomputed one dips, and nothing is unsound.
  run zombieDesignTest =
    init.then(receive(1, true)).then(send(1, req(1))).then(crash(1)).then(tick).then(tick)
      .then(send(1, req(1))).then(land(writers.get(1).cur)).then(ack(1)).then(beat(2))
      .then(ingestAll(CENTRAL)).then(publish(CENTRAL))
      .then(all { puts != Set(), nondet o = puts.oneOf() land(o) }).then(publish(CENTRAL)).then(evaluate(0))
      .expect(and { wZombieLanded, wRegress, wm.get(CENTRAL) == 2, verdict.get(0) == Fire, safety })

  // ---- per cluster (D29; the three-lane instances) ------------------------------------------------
  // Cluster 1's lane 1 holds an error received at t=0 in custody (its edge
  // stalled); lane 2 (cluster 1) and lane 3 (cluster 2) heartbeat at t=2.
  run stalledClusterPrefix =
    init.then(receive(1, true)).then(tick).then(tick).then(beat(2)).then(beat(3)).then(ingestAll(CENTRAL)).then(publish(CENTRAL))
  // Cluster 2 is complete through 2, cluster 1 and the fleet through 0: a
  // rule on cluster 2 alone may evaluate window 0; one on both clusters, or
  // one gated on the fleet value (before D29), may not.
  run stalledClusterScopedTest =
    stalledClusterPrefix
      .expect(and { cwm.get(CENTRAL).get(2) == 2, cwm.get(CENTRAL).get(1) == 0, wm.get(CENTRAL) == 0, wClusterAhead, safety,
                    mayEval(0) == (PER_CLUSTER and not(SCOPE.contains(1))) })
  // ... and it does, OK (cluster 2 had no error), past the fleet value.
  run stalledClusterEvalTest =
    stalledClusterPrefix.then(evaluate(0)).expect(and { verdict.get(0) == Ok, wIsolatedEval, safety })
  // The mutant: the scope's maximum opens window 0 for a rule on both
  // clusters, and it reads cluster 1's error, still in custody, as OK.
  run scopeMaxBreaksTest =
    stalledClusterPrefix.then(evaluate(0)).expect(and { not(resultLabeled), not(evalWithinComplete), not(okMeansNoErrors) })
  // Lane 2 (cluster 1) holds the error instead: cluster 1's value stays at 0
  // (lane 2 is one of its lanes); the split cluster 1 misses it.
  run splitPrefix =
    init.then(receive(2, true)).then(tick).then(tick).then(beat(1)).then(beat(3)).then(ingestAll(CENTRAL)).then(publish(CENTRAL))
  run clusterSplitHeldTest =
    splitPrefix.expect(and { cwm.get(CENTRAL).get(1) == 0, cwm.get(CENTRAL).get(2) == 2, clusterSound, not(mayEval(0)) })
  run clusterSplitBreaksTest = splitPrefix.expect(and { cwm.get(CENTRAL).get(1) == 2, not(clusterSound) })
}

// The rule as implemented (min of the ingested max low and the pending received_at).
module completenessImpl {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = true, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// The design.
module completenessDesign {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = false, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// §5.4 as first written: an idle lane is caught up to the LIST time.
module listTimeIdle {
  import completeness(LOW_META = true, IDLE_AS_LIST = true, PREFIX = true, PENDING = false, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// The lane's watermark from received_at alone (no low in the metadata).
module lastReceived {
  import completeness(LOW_META = false, IDLE_AS_LIST = false, PREFIX = true, PENDING = false, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// The highest ingested object, not the ingested prefix.
module maxNotPrefix {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = false, PENDING = false, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// The minimum over the lanes seen so far (lanes not registered before custody).
module noBirth {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = false, BIRTH = false, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// LS-7: the evaluator runs when the wall clock passes the window.
module evalPastComplete {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = false, BIRTH = true, HEARTBEAT = true,
    GATE = false, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// LS-8: no source reachable reads as no data, OK.
module noDataOk {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = false, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = true, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// LS-6: the lake's results carry central's freshness.
module unlabeledFallback {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = false, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = false,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// No heartbeats: safe, but an idle lane stalls every window until it pages.
module noHeartbeat {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = false, BIRTH = true, HEARTBEAT = false,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2), CLUSTER_OF = Map(1 -> 1, 2 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}

// D29: three lanes in two clusters (lanes 1 and 2 are cluster 1's); a rule on both clusters.
module clusterDesign {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = true, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2, 3), CLUSTER_OF = Map(1 -> 1, 2 -> 1, 3 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(1, 2)).*
}
// A rule on cluster 2 alone, gated on cluster 2's value (as implemented).
module clusterScoped {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = true, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2, 3), CLUSTER_OF = Map(1 -> 1, 2 -> 1, 3 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(2)).*
}
// The same rule gated on the fleet value (before D29): safe, and a stalled cluster 1 stops it.
module clusterScopedFleetGate {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = true, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2, 3), CLUSTER_OF = Map(1 -> 1, 2 -> 1, 3 -> 2), PER_CLUSTER = false, SCOPE_MAX = false, SPLIT = false, SCOPE = Set(2)).*
}
// A rule on both clusters labelled with the maximum of their values.
module scopeMax {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = true, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2, 3), CLUSTER_OF = Map(1 -> 1, 2 -> 1, 3 -> 2), PER_CLUSTER = true, SCOPE_MAX = true, SPLIT = false, SCOPE = Set(1, 2)).*
}
// A cluster's value computed without one of its lanes.
module clusterSplit {
  import completeness(LOW_META = true, IDLE_AS_LIST = false, PREFIX = true, PENDING = true, BIRTH = true, HEARTBEAT = true,
    GATE = true, NODATA_OK = false, LABEL = true,
    LANES = Set(1, 2, 3), CLUSTER_OF = Map(1 -> 1, 2 -> 1, 3 -> 2), PER_CLUSTER = true, SCOPE_MAX = false, SPLIT = true, SCOPE = Set(1, 2)).*
}
