// -*- mode: Bluespec; -*-
/**
 * s3InlineMetricsObs: ../../model/s3InlineMetrics.qnt, unchanged, its
 * actions and properties forwarded one for one (quintgo reads a module's
 * own declarations), plus what a recorded Go run needs to name its values: the
 * request, response and phase constructors of the per-type logs (an
 * instance K of s3Inline with the same sizes: its sum and record types are
 * the same structural types as G's and S's). G's and S's state is not
 * visible outside s3InlineMetrics, so the lanes' own states are not
 * compared here (s3inline.binding.yaml does, lane by lane); conformance
 * and the invariants are. Used by s3inlinemetrics.binding.yaml.
 */
module s3InlineMetricsObs {
  const ACK_ANY: bool

  import s3InlineMetrics(ACK_ON_ANY = ACK_ANY) as M from "../../model/s3InlineMetrics"

  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = false, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true) as K from "../../model/s3Inline"

  // kind: 0 Free, 1 Data, 2 Tomb. phase: 0 WIdle .. 4 WHalted.
  pure def kindOf(k: int): K::Kind = if (k == 1) K::Data else if (k == 2) K::Tomb else K::Free
  pure def ent(k: int, e: int, p: int): K::Entry = { kind: kindOf(k), epoch: e, payload: p }
  pure def rq(e: int, s: int, k: int, p: int, f: bool): K::Req = { epoch: e, slot: s, entry: ent(k, e, p), sawFree: f }
  pure def rs(e: int, s: int, k: int, p: int, ok: bool): K::Resp = { epoch: e, slot: s, entry: ent(k, e, p), ok: ok }
  pure def phaseOf(k: int): K::WPhase =
    if (k == 0) K::WIdle else if (k == 1) K::WReady else if (k == 2) K::WWaiting else if (k == 3) K::WUnresolved else K::WHalted
  pure def pendSet(a: bool, b: bool): Set[int] = Set(1, 2).filter(x => (x == 1 and a) or (x == 2 and b))

  // ---- s3InlineMetrics, forwarded ----
  action init = M::init
  action step = M::step
  action ackRequest(p: int): bool = M::ackRequest(p)
  action crash(pend: Set[int]): bool = M::crash(pend)
  action gStartPush(e: int, p: int): bool = M::gStartPush(e, p)
  action gSwitchPayload(e: int, p: int): bool = M::gSwitchPayload(e, p)
  action gSend(e: int): bool = M::gSend(e)
  action gReceive(e: int, r: K::Resp): bool = M::gReceive(e, r)
  action gTimeout(e: int): bool = M::gTimeout(e)
  action gResolve(e: int): bool = M::gResolve(e)
  action gApply(q: K::Req): bool = M::gApply(q)
  action gLose(q: K::Req): bool = M::gLose(q)
  action gCCheck(e: int): bool = M::gCCheck(e)
  action gCInsert: bool = M::gCInsert
  action gCAdvance: bool = M::gCAdvance
  action gCSeeTomb(e: int): bool = M::gCSeeTomb(e)
  action gCTomb(e: int): bool = M::gCTomb(e)
  action gCTombReceive(r: K::Resp): bool = M::gCTombReceive(r)
  action gCTombTimeout: bool = M::gCTombTimeout
  action gCTombResolve: bool = M::gCTombResolve
  action gCCrash: bool = M::gCCrash
  action sStartPush(e: int, p: int): bool = M::sStartPush(e, p)
  action sSwitchPayload(e: int, p: int): bool = M::sSwitchPayload(e, p)
  action sSend(e: int): bool = M::sSend(e)
  action sReceive(e: int, r: K::Resp): bool = M::sReceive(e, r)
  action sTimeout(e: int): bool = M::sTimeout(e)
  action sResolve(e: int): bool = M::sResolve(e)
  action sApply(q: K::Req): bool = M::sApply(q)
  action sLose(q: K::Req): bool = M::sLose(q)
  action sCCheck(e: int): bool = M::sCCheck(e)
  action sCInsert: bool = M::sCInsert
  action sCAdvance: bool = M::sCAdvance
  action sCSeeTomb(e: int): bool = M::sCSeeTomb(e)
  action sCTomb(e: int): bool = M::sCTomb(e)
  action sCTombReceive(r: K::Resp): bool = M::sCTombReceive(r)
  action sCTombTimeout: bool = M::sCTombTimeout
  action sCTombResolve: bool = M::sCTombResolve
  action sCCrash: bool = M::sCCrash
  val reqAckedImpliesAllCommitted: bool = M::reqAckedImpliesAllCommitted
  val noObjectLost: bool = M::noObjectLost
  val safety: bool = M::safety
  val wAcked: bool = M::wAcked
  val wCrossEpochCopy: bool = M::wCrossEpochCopy
  val wHalfCommitted: bool = M::wHalfCommitted
  val wAllDone: bool = M::wAllDone
}
