// -*- mode: Bluespec; -*-
/**
 * s3InlineMetrics: a request whose rows go to several objects, each in its
 * own log. A metrics request becomes one object per metric type
 * (metrics_gauge, metrics_sum, ...), and each type has its own writer lane,
 * epochs and slots, as s3Inline.qnt models one of them. Two types are enough
 * to show the composition: G and S are two instances of s3Inline, unchanged.
 *
 * What this module adds is the request level, which s3Inline leaves to the
 * queue:
 *  - the sender (an OTLP client, or the collector's exporter queue in front
 *    of the edge) holds each request until it gets a 2xx, and then drops it
 *    (`reqAcked`);
 *  - the edge answers 2xx for a request only when every one of its objects
 *    has committed (`ackRequest`), and a NACK otherwise, which leaves the
 *    request with the sender;
 *  - the edge process has no persistent queue of its own. When it crashes
 *    (`crash`) every lane starts a new epoch at once, forgets what it had in
 *    hand and what it had found committed, and the sender resends every
 *    request it has no 2xx for, to every lane: a type that had committed
 *    commits again in the new epoch, and the consumer's content-key check
 *    skips the copy.
 *
 * The design flag ACK_ON_ANY is the mutation: 2xx once any one object has
 * committed. The sender then drops a request whose other object may still be
 * unresolved; a crash before it resolves loses it (S::noPayloadLost), and
 * even without one the 2xx precedes that object's commit
 * (reqAckedImpliesAllCommitted).
 */
module s3InlineMetrics {
  const ACK_ON_ANY: bool

  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = false, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true) as G from "./s3Inline"
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = false, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true) as S from "./s3Inline"

  pure val REQUESTS: Set[int] = Set(1, 2)

  var reqAcked: Set[int]   // requests the sender got a 2xx for (and dropped)

  action keepG = all {
    G::queue' = G::queue, G::acked' = G::acked, G::lease' = G::lease, G::writers' = G::writers,
    G::log' = G::log, G::inflight' = G::inflight, G::responses' = G::responses, G::everLog' = G::everLog,
    G::ckpt' = G::ckpt, G::closed' = G::closed, G::consumer' = G::consumer, G::central' = G::central,
    G::events' = G::events,
  }
  action keepS = all {
    S::queue' = S::queue, S::acked' = S::acked, S::lease' = S::lease, S::writers' = S::writers,
    S::log' = S::log, S::inflight' = S::inflight, S::responses' = S::responses, S::everLog' = S::everLog,
    S::ckpt' = S::ckpt, S::closed' = S::closed, S::consumer' = S::consumer, S::central' = S::central,
    S::events' = S::events,
  }

  // ---- the request level ----------------------------------------------------------
  // The edge answers the request: 2xx only when every object has committed
  // (a lane's `acked` is what that lane has found committed since the edge
  // started).
  action ackRequest(p: int): bool = all {
    not(reqAcked.contains(p)),
    if (ACK_ON_ANY) G::acked.contains(p) or S::acked.contains(p)
    else G::acked.contains(p) and S::acked.contains(p),
    reqAcked' = reqAcked.union(Set(p)),
    keepG, keepS,
  }

  // The edge process dies and restarts: every lane takes a new epoch, its
  // in-memory state is gone, and the sender resends each request it has no
  // 2xx for.
  def pending: Set[int] = REQUESTS.exclude(reqAcked)

  // (`pend` is always `pending`. It is a parameter because quint 0.32
  // resolves a state variable read on the right of `G::x' = ...` in G's
  // namespace, "Variable s3InlineMetrics::G::reqAcked not set".)
  action crash(pend: Set[int]): bool = all {
    G::lease < 3, S::lease < 3,
    val ng = G::lease + 1
    val ns = S::lease + 1
    all {
      G::lease' = ng,
      G::writers' = G::writers.keys().mapBy(k =>
        if (k == ng) { alive: true, phase: G::WIdle, nextSlot: 0, entry: G::FREE }
        else { ...G::writers.get(k), alive: false }),
      G::queue' = pend, G::acked' = Set(),
      G::events' = G::events.union(Set("restart")),
      S::lease' = ns,
      S::writers' = S::writers.keys().mapBy(k =>
        if (k == ns) { alive: true, phase: S::WIdle, nextSlot: 0, entry: S::FREE }
        else { ...S::writers.get(k), alive: false }),
      S::queue' = pend, S::acked' = Set(),
      S::events' = S::events.union(Set("restart")),
    },
    G::log' = G::log, G::inflight' = G::inflight, G::responses' = G::responses, G::everLog' = G::everLog,
    G::ckpt' = G::ckpt, G::closed' = G::closed, G::consumer' = G::consumer, G::central' = G::central,
    S::log' = S::log, S::inflight' = S::inflight, S::responses' = S::responses, S::everLog' = S::everLog,
    S::ckpt' = S::ckpt, S::closed' = S::closed, S::consumer' = S::consumer, S::central' = S::central,
    reqAcked' = reqAcked,
  }

  // ---- each type's log: s3Inline's actions, one instance at a time -----------------
  action gStartPush(e: int, p: int): bool = all { G::startPush(e, p), keepS, reqAcked' = reqAcked }
  action gSend(e: int): bool = all { G::send(e), keepS, reqAcked' = reqAcked }
  action gTimeout(e: int): bool = all { G::timeout(e), keepS, reqAcked' = reqAcked }
  action gResolve(e: int): bool = all { G::resolve(e), keepS, reqAcked' = reqAcked }
  action gSwitchPayload(e: int, p: int): bool = all { G::switchPayload(e, p), keepS, reqAcked' = reqAcked }
  action gReceive(e: int, r: G::Resp): bool = all { G::receive(e, r), keepS, reqAcked' = reqAcked }
  action gApply(q: G::Req): bool = all { G::apply(q), keepS, reqAcked' = reqAcked }
  action gLose(q: G::Req): bool = all { G::lose(q), keepS, reqAcked' = reqAcked }
  action gCCheck(e: int): bool = all { G::cCheck(e), keepS, reqAcked' = reqAcked }
  action gCInsert: bool = all { G::cInsert, keepS, reqAcked' = reqAcked }
  action gCAdvance: bool = all { G::cAdvance, keepS, reqAcked' = reqAcked }
  action gCSeeTomb(e: int): bool = all { G::cSeeTomb(e), keepS, reqAcked' = reqAcked }
  action gCTomb(e: int): bool = all { G::cTomb(e), keepS, reqAcked' = reqAcked }
  action gCTombReceive(r: G::Resp): bool = all { G::cTombReceive(r), keepS, reqAcked' = reqAcked }
  action gCTombTimeout: bool = all { G::cTombTimeout, keepS, reqAcked' = reqAcked }
  action gCTombResolve: bool = all { G::cTombResolve, keepS, reqAcked' = reqAcked }
  action gCCrash: bool = all { G::cCrash, keepS, reqAcked' = reqAcked }

  action sStartPush(e: int, p: int): bool = all { S::startPush(e, p), keepG, reqAcked' = reqAcked }
  action sSend(e: int): bool = all { S::send(e), keepG, reqAcked' = reqAcked }
  action sTimeout(e: int): bool = all { S::timeout(e), keepG, reqAcked' = reqAcked }
  action sResolve(e: int): bool = all { S::resolve(e), keepG, reqAcked' = reqAcked }
  action sSwitchPayload(e: int, p: int): bool = all { S::switchPayload(e, p), keepG, reqAcked' = reqAcked }
  action sReceive(e: int, r: S::Resp): bool = all { S::receive(e, r), keepG, reqAcked' = reqAcked }
  action sApply(q: S::Req): bool = all { S::apply(q), keepG, reqAcked' = reqAcked }
  action sLose(q: S::Req): bool = all { S::lose(q), keepG, reqAcked' = reqAcked }
  action sCCheck(e: int): bool = all { S::cCheck(e), keepG, reqAcked' = reqAcked }
  action sCInsert: bool = all { S::cInsert, keepG, reqAcked' = reqAcked }
  action sCAdvance: bool = all { S::cAdvance, keepG, reqAcked' = reqAcked }
  action sCSeeTomb(e: int): bool = all { S::cSeeTomb(e), keepG, reqAcked' = reqAcked }
  action sCTomb(e: int): bool = all { S::cTomb(e), keepG, reqAcked' = reqAcked }
  action sCTombReceive(r: S::Resp): bool = all { S::cTombReceive(r), keepG, reqAcked' = reqAcked }
  action sCTombTimeout: bool = all { S::cTombTimeout, keepG, reqAcked' = reqAcked }
  action sCTombResolve: bool = all { S::cTombResolve, keepG, reqAcked' = reqAcked }
  action sCCrash: bool = all { S::cCrash, keepG, reqAcked' = reqAcked }

  // ---- state machine ---------------------------------------------------------------
  action init = all { G::init, S::init, reqAcked' = Set() }

  action step = {
    nondet e = 1.to(3).oneOf()
    nondet p = REQUESTS.oneOf()
    any {
      ackRequest(p), crash(pending),
      gStartPush(e, p), gSend(e), gTimeout(e), gResolve(e), gSwitchPayload(e, p),
      all { G::responses != Set(), nondet r = G::responses.oneOf() any { gReceive(e, r), gCTombReceive(r) } },
      all { G::inflight != Set(), nondet q = G::inflight.oneOf() any { gApply(q), gLose(q) } },
      gCCheck(e), gCInsert, gCAdvance, gCSeeTomb(e), gCTomb(e), gCTombTimeout, gCTombResolve, gCCrash,
      sStartPush(e, p), sSend(e), sTimeout(e), sResolve(e), sSwitchPayload(e, p),
      all { S::responses != Set(), nondet r = S::responses.oneOf() any { sReceive(e, r), sCTombReceive(r) } },
      all { S::inflight != Set(), nondet q = S::inflight.oneOf() any { sApply(q), sLose(q) } },
      sCCheck(e), sCInsert, sCAdvance, sCSeeTomb(e), sCTomb(e), sCTombTimeout, sCTombResolve, sCCrash,
    }
  }

  // ---- properties --------------------------------------------------------------------
  // The request level: a 2xx means every object of the request is committed.
  val reqAckedImpliesAllCommitted: bool =
    reqAcked.forall(p => G::committedAnywhere(p) and S::committedAnywhere(p))

  // A request is never lost: each of its objects is committed, or the
  // request is still with the sender or in that type's lane.
  val noObjectLost: bool =
    REQUESTS.forall(p => (G::queue.contains(p) or G::committedAnywhere(p)) and (S::queue.contains(p) or S::committedAnywhere(p)))

  // Each type's log keeps every s3Inline guarantee (at most once in central,
  // no duplicate within an epoch, nothing uncommitted ingested, ...).
  val safety: bool = and { reqAckedImpliesAllCommitted, noObjectLost, G::safety, S::safety }

  // ---- witnesses ---------------------------------------------------------------------
  // a request acked
  val wAcked: bool = reqAcked != Set()
  // an object committed in two epochs (a crash after part of a request committed)
  val wCrossEpochCopy: bool = G::wCrossEpochCopy or S::wCrossEpochCopy
  // a request NACKed while one of its objects was committed: one lane has it, the other not
  val wHalfCommitted: bool = REQUESTS.exists(p => not(reqAcked.contains(p)) and
    ((G::acked.contains(p) and not(S::acked.contains(p))) or (S::acked.contains(p) and not(G::acked.contains(p)))))
  // everything acked and ingested once per type
  val wAllDone: bool = reqAcked == REQUESTS and REQUESTS.forall(p => G::central.get(p) == 1 and S::central.get(p) == 1)
}

// The design: 2xx only when every object has committed.
module s3InlineMetricsDesign {
  import s3InlineMetrics(ACK_ON_ANY = false).*
}

// Mutation: 2xx as soon as any one object has committed.
module ackOnAny {
  import s3InlineMetrics(ACK_ON_ANY = true).*
}
