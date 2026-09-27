// -*- mode: Bluespec; -*-
/**
 * sealer: the lake's snapshot log (research/central-optional.md §5.1): a
 * consumer group whose sink is a table log. Every round it LISTs each lane
 * from its snapshot's frontier, deduplicates by content key from HEAD
 * metadata, and commits snapshot N+1 by a create-only PUT of
 * `metadata/v{N+1:020d}.metadata.json`. The metadata file is the commit and
 * the checkpoint; two sealers (a racing twin, or a zombie) can both try N+1,
 * and the loser (412) re-reads and plans again from the new latest.
 *
 * The lanes are s3Inline's slots reduced to what the sealer reads: each
 * committed slot has a content key (0: a heartbeat slot, no rows) and a
 * received_at `r`; a copy of a request (a replay into a later epoch) has the
 * same key and the same r (D19). Custody (requests not yet committed) is
 * the completeness model's subject: here a slot's low is its r, as for an
 * edge that drains in order with nothing queued behind (so the snapshot's
 * `complete_through` is over committed slots only).
 *
 * A snapshot: its data files, the copies it skipped, a per-lane frontier (the
 * next slot to read) and complete_through. A round seals at most BUDGET new
 * slots per lane (a bounded manifest per commit, §5.3).
 *
 * Properties:
 *  - monotone: a later snapshot contains every file and skipped copy of an
 *    earlier one, a frontier at least as far, and a complete_through at
 *    least as high;
 *  - repeatableAsOf: a version, once written, never changes (a page pinned
 *    to a snapshot id sees the same data every time);
 *  - atMostOncePerSnapshot: a content key is a file of a snapshot at most once
 *    ("each content key in at most one live snapshot" in §5.1's words: the
 *    consumer's atMostOnce);
 *  - neverSkipsSealed: every slot below a snapshot's frontier is in it as a
 *    file or a skipped copy (a heartbeat excepted);
 *  - wmSound: every committed request with received_at below a snapshot's
 *    complete_through is in it (file or skipped copy).
 *
 * Mutants: `blindCommit` (a plain PUT of v{N+1}: a racing sealer overwrites a
 * snapshot readers may have pinned); `noRebase` (on a 412 the sealer takes
 * the next free version number for the plan it has, built on the old base:
 * the new snapshot drops what the other sealer added); `wmFromList` (the
 * watermark from what the round LISTed, past slots over the round's budget
 * that it did not seal: "the watermark advances past an unsealed lane");
 * `noDedup` (a copy sealed as a second file).
 */
module sealer {
  const COND: bool          // create-only commit of v{N+1} (design)
  const REBASE: bool        // on 412: re-read the latest and plan again (design)
  const WM_SEALED: bool     // complete_through from the sealed prefix (design; false: from the LIST)
  const DEDUP: bool         // skip a content key already in the snapshot (design)

  pure val LANES: Set[int] = Set(1, 2)
  pure val SEALERS: Set[int] = Set(1, 2)
  pure val KEYS: Set[int] = Set(1, 2)       // content keys (0: a heartbeat)
  pure val MAX_OBJS: int = 4
  pure val MAX_V: int = 4
  pure val MAX_T: int = 4
  pure val BUDGET: int = 1                  // new slots sealed per lane per round

  type Obj = { lane: int, seq: int, key: int, r: int }
  type Snap = { files: Set[Obj], skipped: Set[Obj], front: int -> int, wm: int }
  type Phase = Idle | Planned
  type Sealer = { phase: Phase, base: int, plan: Snap }

  // ---- state ----------------------------------------------------------------------
  var now: int
  var objs: Set[Obj]                 // committed slots
  var meta: int -> Snap              // version -> snapshot (what S3 holds now)
  var ever: Set[(int, Snap)]         // every (version, content) ever written
  var sealers: int -> Sealer
  var events: Set[str]

  pure val S0: Snap = { files: Set(), skipped: Set(), front: Set(1, 2).mapBy(_ => 0), wm: 0 }
  pure def imin(a: int, b: int): int = if (a <= b) a else b
  pure def imax(a: int, b: int): int = if (a >= b) a else b
  def latest: int = meta.keys().fold(0, (m, v) => imax(m, v))
  def at(l: int, s: int): Set[Obj] = objs.filter(o => o.lane == l and o.seq == s)
  def nextSeq(l: int): int = objs.filter(o => o.lane == l).size()
  // The r of the slot just below `f` in lane l (-1 if none).
  def rBelow(l: int, f: int): int = objs.filter(o => o.lane == l and o.seq == f - 1).fold(-1, (m, o) => o.r)

  // One round's plan from snapshot `b` and what LIST shows now.
  def planFrom(b: Snap): Snap = {
    // per lane: the contiguous new slots, at most BUDGET of them
    val newFront = LANES.mapBy(l => 0.to(BUDGET).fold(b.front.get(l), (f, i) =>
      if (f == b.front.get(l) + i and i < BUDGET and at(l, f) != Set()) f + 1 else f))
    val fresh = objs.filter(o => o.seq >= b.front.get(o.lane) and o.seq < newFront.get(o.lane) and o.key != 0)
    // dedup by content key against the base and within the round (lowest lane/seq wins)
    val dup = fresh.filter(o => DEDUP and (b.files.exists(f => f.key == o.key) or
      fresh.exists(x => x.key == o.key and (x.lane < o.lane or (x.lane == o.lane and x.seq < o.seq)))))
    val laneWm = LANES.mapBy(l =>
      if (WM_SEALED) rBelow(l, newFront.get(l)) else rBelow(l, nextSeq(l)))
    val w = LANES.fold(now, (m, l) => imin(m, laneWm.get(l)))
    { files: b.files.union(fresh.exclude(dup)), skipped: b.skipped.union(dup), front: newFront, wm: imax(b.wm, w) }
  }

  // ---- the lanes (edges) ------------------------------------------------------------------
  // A slot commits: a request received now (a new key, or a copy of one
  // committed before, which keeps its r), or a heartbeat (key 0).
  action commit(l: int, k: int): bool = {
    val prior = objs.filter(o => o.key == k and k != 0)
    val r = if (prior != Set()) prior.fold(now, (m, o) => imin(m, o.r)) else now
    all {
      objs.size() < MAX_OBJS,
      objs' = objs.union(Set({ lane: l, seq: nextSeq(l), key: k, r: r })),
      events' = events.union(if (prior != Set()) Set("copy") else if (k == 0) Set("beat") else Set()),
      now' = now, meta' = meta, ever' = ever, sealers' = sealers,
    }
  }
  action tick: bool = all { now < MAX_T, now' = now + 1, objs' = objs, meta' = meta, ever' = ever, sealers' = sealers, events' = events }

  // ---- the sealers ------------------------------------------------------------------------
  // Read the latest metadata file (LIST metadata/ StartAfter) and plan the round.
  action plan(s: int): bool = all {
    sealers.get(s).phase == Idle,
    sealers' = sealers.set(s, { phase: Planned, base: latest, plan: planFrom(meta.get(latest)) }),
    now' = now, objs' = objs, meta' = meta, ever' = ever, events' = events,
  }
  // PUT v{base+1} (If-None-Match: * with COND).
  action seal(s: int): bool = {
    val x = sealers.get(s)
    val target = x.base + 1
    val taken = meta.keys().contains(target)
    val v = if (taken and COND and not(REBASE)) latest + 1 else target
    all {
      x.phase == Planned, v <= MAX_V,
      if (taken and COND and REBASE) all {
        // 412: re-read and plan again next round
        meta' = meta, ever' = ever, events' = events.union(Set("lost412")),
      } else all {
        meta' = meta.put(v, x.plan),
        ever' = ever.union(Set((v, x.plan))),
        events' = events.union(if (taken) Set(if (COND) "staleCommit" else "overwrote") else Set("sealed")),
      },
      sealers' = sealers.set(s, { ...x, phase: Idle }),
      now' = now, objs' = objs,
    }
  }

  // ---- state machine --------------------------------------------------------------------------
  action init = all {
    now' = 0, objs' = Set(), meta' = Map(0 -> S0), ever' = Set((0, S0)),
    sealers' = SEALERS.mapBy(_ => { phase: Idle, base: 0, plan: S0 }), events' = Set(),
  }
  action step = {
    nondet l = LANES.oneOf()
    nondet k = Set(0, 1, 2).oneOf()
    nondet s = SEALERS.oneOf()
    any { commit(l, k), tick, plan(s), seal(s) }
  }

  // ---- properties --------------------------------------------------------------------------------
  def vs: Set[int] = meta.keys()
  val monotone: bool = vs.forall(n => vs.forall(m => n < m implies and {
    meta.get(n).files.subseteq(meta.get(m).files), meta.get(n).skipped.subseteq(meta.get(m).skipped),
    LANES.forall(l => meta.get(n).front.get(l) <= meta.get(m).front.get(l)), meta.get(n).wm <= meta.get(m).wm }))
  val repeatableAsOf: bool = ever.forall(a => ever.forall(b => a._1 == b._1 implies a._2 == b._2))
  val atMostOncePerSnapshot: bool = vs.forall(n => meta.get(n).files.forall(a => meta.get(n).files.forall(b =>
    a.key == b.key implies a == b)))
  val neverSkipsSealed: bool = vs.forall(n => objs.forall(o =>
    (o.seq < meta.get(n).front.get(o.lane) and o.key != 0) implies
      (meta.get(n).files.contains(o) or meta.get(n).skipped.contains(o))))
  val wmSound: bool = vs.forall(n => objs.forall(o => (o.key != 0 and o.r < meta.get(n).wm) implies
    meta.get(n).files.union(meta.get(n).skipped).exists(f => f.key == o.key)))
  val safety: bool = and { monotone, repeatableAsOf, atMostOncePerSnapshot, neverSkipsSealed, wmSound }

  // ---- witnesses -------------------------------------------------------------------------------------
  val wLost412: bool = events.contains("lost412") and latest >= 2
  val wSkippedCopy: bool = vs.exists(n => meta.get(n).skipped != Set())
  val wWmAdvanced: bool = vs.exists(n => meta.get(n).wm >= 2 and meta.get(n).files.size() >= 2)
  val wBeatAdvances: bool = events.contains("beat") and vs.exists(n => meta.get(n).wm >= 1)
  val wBudgetLeft: bool = vs.exists(n => n == latest and LANES.exists(l => meta.get(n).front.get(l) < nextSeq(l)))

  // ---- scripted scenarios --------------------------------------------------------------------------------
  // Sealer 1 plans over lane 1's first slot; lane 2 gets one; sealer 2 plans
  // over both (from v0 too); sealer 1 commits v1, then v2 (a heartbeat in
  // lane 1 and lane 2's slot); then sealer 2 commits its stale plan.
  run racePrefix =
    init.then(commit(1, 1)).then(plan(1)).then(commit(2, 2)).then(plan(2)).then(seal(1)).then(tick)
      .then(commit(1, 0)).then(plan(1)).then(seal(1))
  run raceDesignTest = racePrefix.then(seal(2)).expect(and { safety, events.contains("lost412"), latest == 2 })
  run blindCommitBreaksTest = racePrefix.then(seal(2)).expect(and { not(repeatableAsOf), events.contains("overwrote") })
  run noRebaseBreaksTest = racePrefix.then(seal(2)).expect(and { not(monotone), latest == 3 })
  // Lane 2 holds a request (t=0), another (t=1) and a heartbeat (t=2);
  // lane 1 a heartbeat (t=3). A round seals one slot per lane, but its LIST
  // sees them all.
  run budgetPrefix =
    init.then(commit(2, 1)).then(tick).then(commit(2, 2)).then(tick).then(commit(2, 0)).then(tick)
      .then(commit(1, 0)).then(plan(1)).then(seal(1))
  run wmFromListBreaksTest = budgetPrefix.expect(and { not(wmSound), wBudgetLeft })
  run wmSealedDesignTest = budgetPrefix.expect(and { safety, wBudgetLeft, meta.get(1).wm == 0 })
  // A copy of request 1 in lane 2 (a replay), sealed in the same round.
  run copyPrefix = init.then(commit(1, 1)).then(commit(2, 1)).then(plan(1)).then(seal(1))
  run noDedupBreaksTest = copyPrefix.expect(not(atMostOncePerSnapshot))
  run dedupDesignTest = copyPrefix.expect(and { safety, wSkippedCopy })
}

module sealerDesign {
  import sealer(COND = true, REBASE = true, WM_SEALED = true, DEDUP = true).*
}
module blindCommit {
  import sealer(COND = false, REBASE = true, WM_SEALED = true, DEDUP = true).*
}
module noRebase {
  import sealer(COND = true, REBASE = false, WM_SEALED = true, DEDUP = true).*
}
module wmFromList {
  import sealer(COND = true, REBASE = true, WM_SEALED = false, DEDUP = true).*
}
module noDedup {
  import sealer(COND = true, REBASE = true, WM_SEALED = true, DEDUP = false).*
}
