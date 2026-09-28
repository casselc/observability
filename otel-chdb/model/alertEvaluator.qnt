// The alert evaluator (../alerts/, STPA R-S3, AMBIGUITY.md X5/X6), small:
// two replicas share one state document replaced only by compare-and-swap
// (a write can land with its answer lost, or not land); they evaluate
// windows only up to complete_through; the ledger's notices go to a pager
// that can take a send and lose the answer, or lose the send.
//
// Late data (D30): windows are evaluated at a BASIS (a custody time C:
// rows received before C). A window of LATEW gets one late row, received
// at a custody time after complete_through when it arrives; with it, the
// window's verdict is LATEV's (it may start or stop meeting the
// condition). The evaluator keeps each window's basis, and a late check
// counts the window's rows received between that basis and the current
// one (a delta), re-evaluates the window at the new basis, and moves the
// window's basis there (policy reevaluate: a window that now holds and is
// not in the live episode raises a LATE episode, sent firing then
// resolved). Late data never resolves a live episode.
//
// Windows are 0..W-1; BAD are the windows whose on-time rows meet the
// condition (for = 0). An episode is a maximal run of windows whose
// verdict (at the basis they were evaluated at) holds, named by its first
// window; a late episode for window w is named -(w+1). Mutants are const
// flags:
//   EVAL_PAST_CT      evaluate a window that is not complete yet (its rows are
//                     not all in: read as "no rows")      -> evalOnlyComplete, noLostEpisode
//   ACK_ON_NO_ANSWER  a send with no answer counts as delivered
//                                                         -> resolvedAfterFiringAck, noLostEpisode
//   BLIND_WRITE       state writes without compare-and-swap -> nextMonotone
//   LATE_DOUBLE       a late check does not move the window's basis
//                     forward (the next check counts the same rows again)
//                                                         -> noDoubleCount
//   LATE_RESOLVES     a late check that makes a window stop holding
//                     resolves the live episode           -> lateNeverResolves
module alertEvaluator {
  const W: int
  const BAD: Set[int]
  const LATEW: Set[int]
  const LATEV: Set[int]
  const REPLICAS: Set[str]
  const EVAL_PAST_CT: bool
  const ACK_ON_NO_ANSWER: bool
  const BLIND_WRITE: bool
  const LATE_DOUBLE: bool
  const LATE_RESOLVES: bool

  // want firing|resolved; acked ""|firing; endAt: the window whose
  // evaluation resolved it (-1 while firing)
  type Notice = { ep: int, want: str, acked: str, endAt: int }
  // a kept window: eb the basis it was evaluated at, b the basis it was
  // last checked at, evalBad its verdict at eb, bad its verdict at b,
  // counted the late rows counted, lateFired a late episode raised
  type Kept = { eb: int, b: int, evalBad: bool, bad: bool, counted: int, lateFired: bool }
  type St = { next: int, active: int, ledger: Set[Notice], recent: int -> Kept } // active: the open episode, -1 none
  type Local = { ver: int, st: St, clean: bool } // clean: st is the committed version ver

  var ct: int                    // complete_through: rows received at or before ct are in (windows <= ct are complete)
  var ver: int                   // the document's version (its ETag)
  var store: St
  var local: str -> Local
  var received: Set[(int, str)]  // what reached the pager (episode, phase)
  var acked: Set[(int, str)]     // what the pager answered 2xx to
  var maxNext: int               // the highest position ever committed
  var lateRecv: int -> int       // window -> the custody time of its late row, once it arrived

  pure val empty: St = { next: 0, active: -1, ledger: Set(), recent: Map() }

  // the window's verdict at basis c (rows received before c)
  def badAt(w: int, c: int): bool =
    if (lateRecv.keys().contains(w) and lateRecv.get(w) < c) LATEV.contains(w) else BAD.contains(w)

  pure def evalWindow(s: St, w: int, bad: bool, c: int): St = {
    val rec = { eb: c, b: c, evalBad: bad, bad: bad, counted: 0, lateFired: false }
    val s2 = s.with("recent", s.recent.put(w, rec))
    if (bad and s.active < 0)
      s2.with("next", w + 1).with("active", w).with("ledger", s.ledger.union(Set({ ep: w, want: "firing", acked: "", endAt: -1 })))
    else if (not(bad) and s.active >= 0)
      s2.with("next", w + 1).with("active", -1)
        .with("ledger", s.ledger.map(n => if (n.ep == s.active) n.with("want", "resolved").with("endAt", w) else n))
    else s2.with("next", w + 1)
  }

  action init = all {
    ct' = -1, ver' = 0, store' = empty,
    local' = REPLICAS.mapBy(_ => { ver: -1, st: empty, clean: false }),
    received' = Set(), acked' = Set(), maxNext' = 0, lateRecv' = Map(),
  }

  action advance = all {
    ct < W + 1, ct' = ct + 1,
    ver' = ver, store' = store, local' = local, received' = received, acked' = acked, maxNext' = maxNext, lateRecv' = lateRecv,
  }

  // a window's late row enters custody now: after complete_through, so no
  // answer at a basis issued so far holds it
  action lateArrive = {
    nondet w = LATEW.oneOf()
    all {
      not(lateRecv.keys().contains(w)),
      lateRecv' = lateRecv.put(w, ct + 1),
      ct' = ct, ver' = ver, store' = store, local' = local, received' = received, acked' = acked, maxNext' = maxNext,
    }
  }

  action read(r: str): bool = all {
    local' = local.set(r, { ver: ver, st: store, clean: true }),
    ct' = ct, ver' = ver, store' = store, received' = received, acked' = acked, maxNext' = maxNext, lateRecv' = lateRecv,
  }

  // the query service answers "complete" only for w <= ct, at the latest
  // basis ct + 1; a mutant reads an incomplete window anyway, where the BAD
  // rows have not arrived yet
  action evaluate(r: str): bool = {
    val l = local.get(r)
    val w = l.st.next
    all {
      l.ver >= 0, w < W, w <= ct or EVAL_PAST_CT,
      local' = local.set(r, { ver: l.ver, st: evalWindow(l.st, w, w <= ct and badAt(w, ct + 1), ct + 1), clean: false }),
      ct' = ct, ver' = ver, store' = store, received' = received, acked' = acked, maxNext' = maxNext, lateRecv' = lateRecv,
    }
  }

  // one window's late check: the delta [its basis, ct + 1), then the
  // window at the new basis
  action lateCheck(r: str): bool = {
    val l = local.get(r)
    val s = l.st
    all {
      l.ver >= 0, s.recent.keys() != Set(),
      nondet w = s.recent.keys().oneOf()
      val rec = s.recent.get(w)
      val cn = ct + 1
      val rows = if (lateRecv.keys().contains(w) and rec.b <= lateRecv.get(w) and lateRecv.get(w) < cn) 1 else 0
      val nb = badAt(w, cn)
      val inLive = s.active >= 0 and s.active <= w
      val fireLate = rows > 0 and nb and not(rec.bad) and not(rec.lateFired) and not(inLive)
      val rec2 = if (rows == 0) rec.with("b", cn)
                 else { eb: rec.eb, b: if (LATE_DOUBLE) rec.b else cn, evalBad: rec.evalBad, bad: nb,
                        counted: rec.counted + rows, lateFired: rec.lateFired or fireLate }
      val ledger1 = if (fireLate) s.ledger.union(Set({ ep: -(w + 1), want: "resolved", acked: "", endAt: w })) else s.ledger
      val resolveLive = LATE_RESOLVES and rows > 0 and not(nb) and inLive
      val s2 = s.with("recent", s.recent.set(w, rec2)).with("ledger", ledger1)
      val s3 = if (resolveLive)
                 s2.with("active", -1).with("ledger", ledger1.map(n => if (n.ep == s.active) n.with("want", "resolved").with("endAt", w) else n))
               else s2
      all {
        rec.b <= cn,
        local' = local.set(r, { ver: l.ver, st: s3, clean: false }),
      },
      ct' = ct, ver' = ver, store' = store, received' = received, acked' = acked, maxNext' = maxNext, lateRecv' = lateRecv,
    }
  }

  // compare-and-swap; the answer may be lost (then the replica must re-read
  // before it acts on anything: store.Commit's read-back)
  action commit(r: str): bool = {
    val l = local.get(r)
    all {
      l.ver >= 0, not(l.clean), l.ver == ver or BLIND_WRITE,
      nondet answered = Set(true, false).oneOf()
      all {
        store' = l.st, ver' = ver + 1,
        maxNext' = if (l.st.next > maxNext) l.st.next else maxNext,
        local' = local.set(r, if (answered) { ver: ver + 1, st: l.st, clean: true } else { ver: -1, st: l.st, clean: false }),
      },
      ct' = ct, received' = received, acked' = acked, lateRecv' = lateRecv,
    }
  }

  // a write that never lands, and no answer
  action lostWrite(r: str): bool = all {
    local.get(r).ver >= 0, not(local.get(r).clean),
    local' = local.set(r, local.get(r).with("ver", -1)),
    ct' = ct, ver' = ver, store' = store, received' = received, acked' = acked, maxNext' = maxNext, lateRecv' = lateRecv,
  }

  // send one due notice from a committed state: firing until acknowledged,
  // then its resolution
  action send(r: str): bool = {
    val l = local.get(r)
    all {
      l.clean, l.st.ledger != Set(),
      nondet n = l.st.ledger.oneOf()
      val phase = if (n.acked == "") "firing" else if (n.want == "resolved") "resolved" else ""
      nondet outcome = Set("acked", "takenNoAnswer", "lost").oneOf()
      val delivered = outcome == "acked" or ACK_ON_NO_ANSWER
      val rest = l.st.ledger.exclude(Set(n))
      all {
        phase != "",
        received' = if (outcome != "lost") received.union(Set((n.ep, phase))) else received,
        acked' = if (outcome == "acked") acked.union(Set((n.ep, phase))) else acked,
        local' = if (not(delivered)) local
                 else local.set(r, { ver: l.ver, clean: false,
                   st: l.st.with("ledger", if (phase == "resolved") rest else rest.union(Set(n.with("acked", "firing")))) }),
      },
      ct' = ct, ver' = ver, store' = store, maxNext' = maxNext, lateRecv' = lateRecv,
    }
  }

  action step = any {
    advance,
    lateArrive,
    nondet r = REPLICAS.oneOf()
    any { read(r), evaluate(r), lateCheck(r), commit(r), lostWrite(r), send(r) },
  }

  // the verdict a window SHOULD have had when evaluated: at its basis, if
  // the basis covered it; the on-time rows' otherwise (a window evaluated
  // before it was complete read "no rows")
  def refBad(w: int): bool = {
    val rec = store.recent.get(w)
    if (rec.eb > w) badAt(w, rec.eb) else BAD.contains(w)
  }

  // episodes among the windows evaluated so far
  def episodes(upTo: int): Set[int] =
    store.recent.keys().filter(w => w < upTo and refBad(w) and (w == 0 or not(refBad(w - 1))))

  // R-S3: nothing is evaluated past complete_through
  val evalOnlyComplete: bool = store.next <= ct + 1
  // X6: a resolution reaches the pager only after its firing was acknowledged
  val resolvedAfterFiringAck: bool = received.forall(x => x._2 == "resolved" implies acked.contains((x._1, "firing")))
  // X6: every episode stays in the ledger until the pager acknowledged its
  // resolution (never dropped on no answer)
  val noLostEpisode: bool = episodes(store.next).forall(e =>
    store.ledger.exists(n => n.ep == e) or acked.contains((e, "resolved")))
  // two replicas: the committed position never goes back
  val nextMonotone: bool = store.next >= maxNext
  // D30: a window's late rows are counted exactly once: what it counted is
  // the late rows received between the basis it was evaluated at and the
  // basis it was last checked at
  val noDoubleCount: bool = store.recent.keys().forall(w => {
    val rec = store.recent.get(w)
    rec.counted == (if (lateRecv.keys().contains(w) and rec.eb <= lateRecv.get(w) and lateRecv.get(w) < rec.b) 1 else 0)
  })
  // D30: late data never resolves a live episode: every live episode that
  // is resolved was resolved by a window evaluated in order whose verdict
  // (at its basis) did not hold
  val lateNeverResolves: bool = store.ledger.forall(n =>
    (n.ep >= 0 and n.want == "resolved") implies (store.recent.keys().contains(n.endAt) and not(store.recent.get(n.endAt).evalBad)))
  // D30: a late episode is raised only for a window that holds at its
  // newer basis, and at most once per window
  val lateOnlyIfHolds: bool = store.ledger.forall(n =>
    n.ep < 0 implies (store.recent.keys().contains(-n.ep - 1) and store.recent.get(-n.ep - 1).bad and store.recent.get(-n.ep - 1).lateFired))
  val safe: bool = evalOnlyComplete and resolvedAfterFiringAck and noLostEpisode and nextMonotone
    and noDoubleCount and lateNeverResolves and lateOnlyIfHolds

  // witnesses (expected VIOLATED: the scenario is reachable)
  val wResolvedAcked: bool = acked.forall(x => x._2 != "resolved")
  val wTakenNoAnswer: bool = not(received.exists(x => not(acked.contains(x))) and store.next == W)
  val wLateCounted: bool = store.recent.keys().forall(w => store.recent.get(w).counted == 0)
  val wLateEpisodeAcked: bool = not(acked.exists(x => x._1 < 0 and x._2 == "resolved"))
  val wLateChangedVerdict: bool = store.recent.keys().forall(w => store.recent.get(w).bad == store.recent.get(w).evalBad)
}

module design {
  import alertEvaluator(W = 4, BAD = Set(1, 2), LATEW = Set(0, 1, 3), LATEV = Set(0), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = false, BLIND_WRITE = false, LATE_DOUBLE = false, LATE_RESOLVES = false).* from "./alertEvaluator"
}
module evalPastCt {
  import alertEvaluator(W = 4, BAD = Set(1, 2), LATEW = Set(0, 1, 3), LATEV = Set(0), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = true, ACK_ON_NO_ANSWER = false, BLIND_WRITE = false, LATE_DOUBLE = false, LATE_RESOLVES = false).* from "./alertEvaluator"
}
module ackOnNoAnswer {
  import alertEvaluator(W = 4, BAD = Set(1, 2), LATEW = Set(0, 1, 3), LATEV = Set(0), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = true, BLIND_WRITE = false, LATE_DOUBLE = false, LATE_RESOLVES = false).* from "./alertEvaluator"
}
module blindWrite {
  import alertEvaluator(W = 4, BAD = Set(1, 2), LATEW = Set(0, 1, 3), LATEV = Set(0), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = false, BLIND_WRITE = true, LATE_DOUBLE = false, LATE_RESOLVES = false).* from "./alertEvaluator"
}
module lateDouble {
  import alertEvaluator(W = 4, BAD = Set(1, 2), LATEW = Set(0, 1, 3), LATEV = Set(0), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = false, BLIND_WRITE = false, LATE_DOUBLE = true, LATE_RESOLVES = false).* from "./alertEvaluator"
}
module lateResolves {
  import alertEvaluator(W = 4, BAD = Set(1, 2), LATEW = Set(0, 1, 3), LATEV = Set(0), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = false, BLIND_WRITE = false, LATE_DOUBLE = false, LATE_RESOLVES = true).* from "./alertEvaluator"
}
