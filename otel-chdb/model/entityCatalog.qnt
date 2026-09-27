// -*- mode: Bluespec; -*-
/**
 * entityCatalog: rows that carry a `resource_id` instead of their resource
 * attributes, and the catalog that puts the attributes back at query time
 * (entities/README.md §3.6, §6.2, §6.3; STPA LS-5: the entity controller is
 * down or lagging while agents keep producing telemetry for pods the catalog
 * lacks).
 *
 * The pieces:
 *  - Resources (a pod version x container) are born and die. The edge sees a
 *    resource when it is born and remembers when it first saw it (its LRU);
 *    a restart forgets, so everything is new again.
 *  - Grace window G: a row emitted within G of the edge first seeing its
 *    resource carries the whole covered set in its residual, so it is exact
 *    whatever the catalog knows (`full`).
 *  - The controller (informers on every cluster) writes a catalog entry for
 *    each live resource within D of its birth while it is up. It can be down
 *    for any time (CTRL_FAULTS); informers relist LIVE objects only, so a
 *    resource born and dead inside an outage is never seen by it.
 *  - Catalog lag: an entry reaches the query-time dictionaries LAG after it is
 *    written (LIFETIME(MIN 30 MAX 60); 18-52 s measured).
 *  - The announcement lane (ANNOUNCE, recommended in §6.3, not built): the
 *    edge writes (resource_id, covered set) as an object, like the series
 *    lane of metrics layout B (s3InlineConsumer's sPush/sAnnounce): it marks
 *    a resource announced (for this epoch) only once that object has
 *    committed, and emits rows for it only after that. The consumer inserts
 *    announcements into the catalog. A restart starts a new epoch with an
 *    empty cache, so every resource is announced again.
 *  - The data lane's own commit protocol is s3Inline's; here a row commits
 *    when it is emitted. The consumer ingests rows and announcements in any
 *    order across lanes; with SAME_LANE the announcement is an object in the
 *    data lane itself, before the rows that use it, so the consumer's
 *    in-order ingest takes it first.
 *
 * A row is exact at query time if it is `full` or its resource is in the
 * dictionaries.
 *
 * Properties:
 *  - noPermanentOrphan (LS-5): every committed row is exact, or can still
 *    become exact: its resource is alive (the controller may yet see it), in
 *    the catalog (the dictionaries will load it), or announced by a committed
 *    object (the consumer will ingest it). A row that is none of these stays
 *    residual-only for good.
 *  - exactAtQuery: every ingested row is exact now. It needs the controller
 *    up and G >= D + LAG (the README's "G must cover the controller's write
 *    delay plus this lag").
 *  - exactAfterLag: a row ingested at least LAG ago is exact. Whether the
 *    controller is up or not; it needs the announcement ingested no later
 *    than the rows (SAME_LANE).
 *
 * Instances: `catalogDesign` (announcements in their own lane, as §6.3
 * specifies, controller faults); `catalogHealthy` (no announcements, the
 * controller up: the grace window alone); `sameLane`. Mutants: `noAnnounce`
 * (the catalog without announcements, controller faults), `announceEarly`
 * (marked announced when sent; a lost PUT leaves the resource unannounced),
 * `shortGrace` (G < D + LAG).
 */
module entityCatalog {
  const ANNOUNCE: bool       // the edge announcement lane
  const EARLY: bool          // mutation: a resource is marked announced when its announcement is sent
  const SAME_LANE: bool      // announcements are objects in the data lane, before the rows that use them
  const CTRL_FAULTS: bool    // the controller can be down
  const G: int               // grace window

  pure val D: int = 1        // the controller writes a live resource within D of its birth (when up)
  pure val LAG: int = 1      // a catalog entry reaches the dictionaries LAG after it is written
  pure val MAX_T: int = 7
  pure val RES: Set[int] = Set(1, 2)
  pure val MAX_ROWS: int = 3
  pure val MAX_EPOCH: int = 2
  pure val NEVER: int = 1000

  type Life = Unborn | Alive | Dead
  type Row = { id: int, res: int, t: int, full: bool, epoch: int }
  type Ann = { res: int, epoch: int }

  // ---- state ----------------------------------------------------------------------
  var now: int
  var life: int -> Life
  var born: int -> int
  var firstSeen: int -> int        // the edge's LRU: when it first saw the resource (reset by a restart)
  var epoch: int                   // the edge's incarnation
  var cache: Set[int]              // resources announced in this epoch
  var annInflight: Set[Ann]
  var annCommitted: Set[Ann]
  var annIngested: Set[int]
  var rows: Set[Row]               // committed rows
  var ingested: int -> int         // row id -> when the consumer ingested it (NEVER: not yet)
  var catalog: int -> int          // resource -> when its catalog entry was written (NEVER: none)
  var ctrlUp: bool
  var events: Set[str]

  // ---- pure logic --------------------------------------------------------------------
  def dictHas(i: int): bool = catalog.get(i) + LAG <= now
  def exactRow(r: Row): bool = r.full or dictHas(r.res)
  def recoverable(i: int): bool =
    life.get(i) == Alive or catalog.get(i) != NEVER or annCommitted.exists(a => a.res == i)
  def ingestedRows: Set[Row] = rows.filter(r => ingested.get(r.id) != NEVER)
  // The controller is behind on a live resource it should have written by now.
  def ctrlOwes: bool = ctrlUp and RES.exists(i => life.get(i) == Alive and catalog.get(i) == NEVER and now >= born.get(i) + D)

  action keepAnn = all { cache' = cache, annInflight' = annInflight, annCommitted' = annCommitted, annIngested' = annIngested }
  action keepAll = all {
    life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, keepAnn, rows' = rows, ingested' = ingested,
    catalog' = catalog, ctrlUp' = ctrlUp,
  }

  // ---- the cluster and the edge --------------------------------------------------------
  action tick: bool = all { now < MAX_T, not(ctrlOwes), now' = now + 1, keepAll, events' = events }

  action spawn(i: int): bool = all {
    life.get(i) == Unborn,
    life' = life.set(i, Alive), born' = born.set(i, now), firstSeen' = firstSeen.set(i, now),
    events' = events.union(if (ctrlUp) Set() else Set("bornInOutage")),
    now' = now, epoch' = epoch, keepAnn, rows' = rows, ingested' = ingested, catalog' = catalog, ctrlUp' = ctrlUp,
  }

  action die(i: int): bool = all {
    life.get(i) == Alive,
    life' = life.set(i, Dead),
    events' = events.union(if (ctrlUp) Set() else Set("diedInOutage")),
    now' = now, born' = born, firstSeen' = firstSeen, epoch' = epoch, keepAnn, rows' = rows, ingested' = ingested,
    catalog' = catalog, ctrlUp' = ctrlUp,
  }

  // The edge restarts: a new epoch, an empty announcement cache, and every
  // live resource is new again (full rows for G).
  action restart: bool = all {
    epoch < MAX_EPOCH,
    epoch' = epoch + 1, cache' = Set(),
    firstSeen' = RES.mapBy(i => if (life.get(i) == Alive) now else firstSeen.get(i)),
    events' = events.union(Set("restart")),
    now' = now, life' = life, born' = born, annInflight' = annInflight, annCommitted' = annCommitted,
    annIngested' = annIngested, rows' = rows, ingested' = ingested, catalog' = catalog, ctrlUp' = ctrlUp,
  }

  // A row of a live resource commits (only once its resource is announced in this epoch).
  action emit(i: int): bool = {
    val r = { id: rows.size() + 1, res: i, t: now, full: now < firstSeen.get(i) + G, epoch: epoch }
    all {
      life.get(i) == Alive, rows.size() < MAX_ROWS,
      ANNOUNCE implies cache.contains(i),
      rows' = rows.union(Set(r)),
      now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, keepAnn, ingested' = ingested,
      catalog' = catalog, ctrlUp' = ctrlUp, events' = events,
    }
  }

  // ---- the announcement lane (edge) --------------------------------------------------------
  action annSend(i: int): bool = all {
    ANNOUNCE, life.get(i) == Alive, not(cache.contains(i)), not(annInflight.contains({ res: i, epoch: epoch })),
    annInflight' = annInflight.union(Set({ res: i, epoch: epoch })),
    cache' = if (EARLY) cache.union(Set(i)) else cache,
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, annCommitted' = annCommitted,
    annIngested' = annIngested, rows' = rows, ingested' = ingested, catalog' = catalog, ctrlUp' = ctrlUp, events' = events,
  }
  action annLand(a: Ann): bool = all {
    annInflight.contains(a),
    annInflight' = annInflight.exclude(Set(a)), annCommitted' = annCommitted.union(Set(a)),
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, cache' = cache,
    annIngested' = annIngested, rows' = rows, ingested' = ingested, catalog' = catalog, ctrlUp' = ctrlUp, events' = events,
  }
  action annLose(a: Ann): bool = all {
    annInflight.contains(a),
    annInflight' = annInflight.exclude(Set(a)),
    events' = events.union(Set("annLost")),
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, cache' = cache,
    annCommitted' = annCommitted, annIngested' = annIngested, rows' = rows, ingested' = ingested, catalog' = catalog, ctrlUp' = ctrlUp,
  }
  // Committed in this epoch: now announced (and rows may use it).
  action annMark(i: int): bool = all {
    ANNOUNCE, annCommitted.contains({ res: i, epoch: epoch }), not(cache.contains(i)),
    cache' = cache.union(Set(i)),
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, annInflight' = annInflight,
    annCommitted' = annCommitted, annIngested' = annIngested, rows' = rows, ingested' = ingested, catalog' = catalog,
    ctrlUp' = ctrlUp, events' = events,
  }

  // ---- central ------------------------------------------------------------------------------
  // The consumer inserts an announcement into the catalog.
  action annIngest(i: int): bool = all {
    annCommitted.exists(a => a.res == i), not(annIngested.contains(i)),
    annIngested' = annIngested.union(Set(i)),
    catalog' = if (catalog.get(i) == NEVER) catalog.set(i, now) else catalog,
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, cache' = cache,
    annInflight' = annInflight, annCommitted' = annCommitted, rows' = rows, ingested' = ingested, ctrlUp' = ctrlUp, events' = events,
  }
  // The consumer ingests a row (SAME_LANE: its announcement came first in the lane).
  action ingestRow(r: Row): bool = all {
    rows.contains(r), ingested.get(r.id) == NEVER,
    (ANNOUNCE and SAME_LANE) implies annIngested.contains(r.res),
    ingested' = ingested.set(r.id, now),
    events' = events.union(if (exactRow(r)) Set() else Set("inexactIngest")),
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, keepAnn, rows' = rows,
    catalog' = catalog, ctrlUp' = ctrlUp,
  }
  // The controller sees a live resource and writes its entry.
  action observe(i: int): bool = all {
    ctrlUp, life.get(i) == Alive, catalog.get(i) == NEVER,
    catalog' = catalog.set(i, now),
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, keepAnn, rows' = rows,
    ingested' = ingested, ctrlUp' = ctrlUp, events' = events,
  }
  action ctrlFlip: bool = all {
    CTRL_FAULTS,
    ctrlUp' = not(ctrlUp),
    events' = events.union(Set(if (ctrlUp) "ctrlDown" else "ctrlUp")),
    now' = now, life' = life, born' = born, firstSeen' = firstSeen, epoch' = epoch, keepAnn, rows' = rows,
    ingested' = ingested, catalog' = catalog,
  }

  // ---- state machine ----------------------------------------------------------------------------
  action init = all {
    now' = 0,
    life' = RES.mapBy(_ => Unborn), born' = RES.mapBy(_ => NEVER), firstSeen' = RES.mapBy(_ => NEVER),
    epoch' = 1, cache' = Set(), annInflight' = Set(), annCommitted' = Set(), annIngested' = Set(),
    rows' = Set(), ingested' = 1.to(MAX_ROWS).mapBy(_ => NEVER), catalog' = RES.mapBy(_ => NEVER),
    ctrlUp' = true, events' = Set(),
  }

  action step = {
    nondet i = RES.oneOf()
    any {
      tick, spawn(i), die(i), restart, emit(i),
      annSend(i), annMark(i), annIngest(i),
      all { annInflight != Set(), nondet a = annInflight.oneOf() any { annLand(a), annLose(a) } },
      all { rows != Set(), nondet r = rows.oneOf() ingestRow(r) },
      observe(i), ctrlFlip,
    }
  }

  // ---- properties ------------------------------------------------------------------------------
  val noPermanentOrphan: bool = rows.forall(r => exactRow(r) or recoverable(r.res))
  val exactAtQuery: bool = ingestedRows.forall(r => exactRow(r))
  val exactAfterLag: bool = ingestedRows.forall(r => ingested.get(r.id) + LAG <= now implies exactRow(r))
  // The edge marks a resource announced only after its announcement committed.
  val announcedAfterCommit: bool = cache.forall(i => annCommitted.contains({ res: i, epoch: epoch }))
  val safety: bool = and { noPermanentOrphan, announcedAfterCommit }

  // ---- witnesses ---------------------------------------------------------------------------------
  // A resource born and dead inside a controller outage, with rows, all exact.
  val wOutageLife: bool = events.contains("bornInOutage") and events.contains("diedInOutage") and
    RES.exists(i => life.get(i) == Dead and catalog.get(i) != NEVER and rows.exists(r => r.res == i and not(r.full))) and
    rows.forall(r => exactRow(r))
  // A row ingested before the dictionaries knew its resource, exact later.
  val wHealed: bool = events.contains("inexactIngest") and rows.size() > 0 and rows.forall(r => exactRow(r))
  val wGraceRow: bool = ingestedRows.exists(r => r.full) and ingestedRows.exists(r => not(r.full))
  val wReannounced: bool = annCommitted.exists(a => a.epoch == 2) and events.contains("restart")
  val wAnnLost: bool = events.contains("annLost") and rows.size() > 0

  // ---- scripted scenarios --------------------------------------------------------------------------
  def row(n: int): Row = rows.filter(r => r.id == n).fold({ id: 0, res: 0, t: 0, full: false, epoch: 0 }, (a, r) => r)
  // Resource 1 lives and dies inside a controller outage; its row is emitted
  // after the grace window.
  run outagePrefix = init.then(ctrlFlip).then(spawn(1))
  action announce(i: int): bool = annSend(i).then(all { annInflight != Set(), nondet a = annInflight.oneOf() annLand(a) }).then(annMark(i))
  run noAnnounceBreaksTest =
    outagePrefix.then(G.reps(_ => tick)).then(emit(1)).then(die(1)).then(ingestRow(row(1)))
      .expect(and { not(noPermanentOrphan), not(recoverable(1)) })
  // Design: the row waits for a committed announcement; the consumer puts it in the catalog.
  run outageAnnouncedDesignTest =
    outagePrefix.then(announce(1)).then(G.reps(_ => tick)).then(emit(1)).then(die(1)).then(ingestRow(row(1)))
      .then(annIngest(1)).then(LAG.reps(_ => tick)).expect(and { safety, exactAtQuery, wOutageLife })
  // A lost announcement that the edge already counts as announced.
  run announceEarlyBreaksTest =
    outagePrefix.then(annSend(1)).then(all { annInflight != Set(), nondet a = annInflight.oneOf() annLose(a) })
      .then(G.reps(_ => tick)).then(emit(1)).then(die(1))
      .expect(and { not(noPermanentOrphan), not(announcedAfterCommit) })
  // The separate lane: the row is ingested before the announcement (its
  // lane lags): residual-only past LAG, until the announcement lands.
  run laneOrderPrefix =
    outagePrefix.then(announce(1)).then(G.reps(_ => tick)).then(emit(1)).then(ingestRow(row(1))).then(LAG.reps(_ => tick))
  run separateLaneLagsTest = laneOrderPrefix.expect(and { safety, not(exactAfterLag) })
  // The same lane: the row can't be ingested before its announcement.
  run sameLaneDesignTest =
    outagePrefix.then(announce(1)).then(G.reps(_ => tick)).then(emit(1)).then(annIngest(1)).then(ingestRow(row(1)))
      .then(LAG.reps(_ => tick)).expect(and { safety, exactAfterLag, exactAtQuery })
  run separateLaneHealsTest =
    laneOrderPrefix.then(annIngest(1)).then(LAG.reps(_ => tick)).expect(and { safety, exactAfterLag, exactAtQuery })
  // The controller healthy, no announcements: a row just past the grace
  // window, of a resource the controller wrote at the latest (born + D).
  // (runs only: let the clock reach t)
  action waitUntil(t: int): bool = all { now <= t, now' = t, keepAll, events' = events }
  run gracePrefix = init.then(spawn(1)).then(waitUntil(D)).then(observe(1)).then(waitUntil(G))
    .then(emit(1)).then(ingestRow(row(1)))
  run shortGraceBreaksTest = gracePrefix.expect(and { not(row(1).full), not(exactAtQuery) })
  run graceDesignTest = gracePrefix.expect(exactAtQuery)
}

// §6.3 as specified: announcements in their own lane; the controller can be down.
module catalogDesign {
  import entityCatalog(ANNOUNCE = true, EARLY = false, SAME_LANE = false, CTRL_FAULTS = true, G = 2).*
}
// The announcement ahead of its rows in the data lane.
module sameLane {
  import entityCatalog(ANNOUNCE = true, EARLY = false, SAME_LANE = true, CTRL_FAULTS = true, G = 2).*
}
// No announcements, the controller always up: the grace window alone (G = D + LAG).
module catalogHealthy {
  import entityCatalog(ANNOUNCE = false, EARLY = false, SAME_LANE = false, CTRL_FAULTS = false, G = 2).*
}
// LS-5: the catalog without announcements, and a controller that can be down.
module noAnnounce {
  import entityCatalog(ANNOUNCE = false, EARLY = false, SAME_LANE = false, CTRL_FAULTS = true, G = 2).*
}
// Marked announced when sent.
module announceEarly {
  import entityCatalog(ANNOUNCE = true, EARLY = true, SAME_LANE = false, CTRL_FAULTS = true, G = 2).*
}
// A grace window shorter than the controller's delay plus the dictionary lag.
module shortGrace {
  import entityCatalog(ANNOUNCE = false, EARLY = false, SAME_LANE = false, CTRL_FAULTS = false, G = 1).*
}
