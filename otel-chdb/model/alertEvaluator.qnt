// The alert evaluator (../alerts/, STPA R-S3, AMBIGUITY.md X5/X6), small:
// two replicas share one state document replaced only by compare-and-swap
// (a write can land with its answer lost, or not land); they evaluate
// windows only up to complete_through; the ledger's notices go to a pager
// that can take a send and lose the answer, or lose the send.
//
// Windows are 0..W-1; BAD are the windows whose rows meet the condition
// (for = 0). An episode is a maximal run of BAD windows, named by its first
// window. Mutants are const flags:
//   EVAL_PAST_CT      evaluate a window that is not complete yet (its rows are
//                     not all in: read as "no rows")      -> evalOnlyComplete
//   ACK_ON_NO_ANSWER  a send with no answer counts as delivered
//                                                         -> resolvedAfterFiringAck, noLostEpisode
//   BLIND_WRITE       state writes without compare-and-swap -> nextMonotone
module alertEvaluator {
  const W: int
  const BAD: Set[int]
  const REPLICAS: Set[str]
  const EVAL_PAST_CT: bool
  const ACK_ON_NO_ANSWER: bool
  const BLIND_WRITE: bool

  type Notice = { ep: int, want: str, acked: str } // want firing|resolved; acked ""|firing
  type St = { next: int, active: int, ledger: Set[Notice] } // active: the open episode, -1 none
  type Local = { ver: int, st: St, clean: bool } // clean: st is the committed version ver

  var ct: int                    // complete_through: windows <= ct are complete
  var ver: int                   // the document's version (its ETag)
  var store: St
  var local: str -> Local
  var received: Set[(int, str)]  // what reached the pager (episode, phase)
  var acked: Set[(int, str)]     // what the pager answered 2xx to
  var maxNext: int               // the highest position ever committed

  pure val empty: St = { next: 0, active: -1, ledger: Set() }

  pure def evalWindow(s: St, w: int, bad: bool): St =
    if (bad and s.active < 0)
      { next: w + 1, active: w, ledger: s.ledger.union(Set({ ep: w, want: "firing", acked: "" })) }
    else if (not(bad) and s.active >= 0)
      { next: w + 1, active: -1,
        ledger: s.ledger.map(n => if (n.ep == s.active) n.with("want", "resolved") else n) }
    else s.with("next", w + 1)

  action init = all {
    ct' = -1, ver' = 0, store' = empty,
    local' = REPLICAS.mapBy(_ => { ver: -1, st: empty, clean: false }),
    received' = Set(), acked' = Set(), maxNext' = 0,
  }

  action advance = all {
    ct < W - 1, ct' = ct + 1,
    ver' = ver, store' = store, local' = local, received' = received, acked' = acked, maxNext' = maxNext,
  }

  action read(r: str): bool = all {
    local' = local.set(r, { ver: ver, st: store, clean: true }),
    ct' = ct, ver' = ver, store' = store, received' = received, acked' = acked, maxNext' = maxNext,
  }

  // the query service answers "complete" only for w <= ct; a mutant reads
  // an incomplete window anyway, where the BAD rows have not arrived yet
  action evaluate(r: str): bool = {
    val l = local.get(r)
    val w = l.st.next
    all {
      l.ver >= 0, w < W, w <= ct or EVAL_PAST_CT,
      local' = local.set(r, { ver: l.ver, st: evalWindow(l.st, w, BAD.contains(w) and w <= ct), clean: false }),
      ct' = ct, ver' = ver, store' = store, received' = received, acked' = acked, maxNext' = maxNext,
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
      ct' = ct, received' = received, acked' = acked,
    }
  }

  // a write that never lands, and no answer
  action lostWrite(r: str): bool = all {
    local.get(r).ver >= 0, not(local.get(r).clean),
    local' = local.set(r, local.get(r).with("ver", -1)),
    ct' = ct, ver' = ver, store' = store, received' = received, acked' = acked, maxNext' = maxNext,
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
      ct' = ct, ver' = ver, store' = store, maxNext' = maxNext,
    }
  }

  action step = any {
    advance,
    nondet r = REPLICAS.oneOf()
    any { read(r), evaluate(r), commit(r), lostWrite(r), send(r) },
  }

  // episodes among the windows evaluated so far
  def episodes(upTo: int): Set[int] =
    BAD.filter(w => w < upTo and (w == 0 or not(BAD.contains(w - 1))))

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
  val safe: bool = evalOnlyComplete and resolvedAfterFiringAck and noLostEpisode and nextMonotone

  // witnesses (expected VIOLATED: the scenario is reachable)
  val wResolvedAcked: bool = acked.forall(x => x._2 != "resolved")
  val wTakenNoAnswer: bool = not(received.exists(x => not(acked.contains(x))) and store.next == W)
}

module design {
  import alertEvaluator(W = 4, BAD = Set(1, 2), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = false, BLIND_WRITE = false).* from "./alertEvaluator"
}
module evalPastCt {
  import alertEvaluator(W = 4, BAD = Set(1, 2), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = true, ACK_ON_NO_ANSWER = false, BLIND_WRITE = false).* from "./alertEvaluator"
}
module ackOnNoAnswer {
  import alertEvaluator(W = 4, BAD = Set(1, 2), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = true, BLIND_WRITE = false).* from "./alertEvaluator"
}
module blindWrite {
  import alertEvaluator(W = 4, BAD = Set(1, 2), REPLICAS = Set("a", "b"),
    EVAL_PAST_CT = false, ACK_ON_NO_ANSWER = false, BLIND_WRITE = true).* from "./alertEvaluator"
}
