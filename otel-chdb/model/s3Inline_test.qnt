// -*- mode: Bluespec; -*-
// Deterministic scenarios for s3Inline: the edge findings (F1-F4, PBT 1-3)
// against the manifest-less design, and each mutation's counterexample.
//
//   quint test s3Inline_test.qnt --main inlineDesignTest --backend typescript
//
// Helper actions are repeated in each module: they must bind to that
// instance's state.

module inlineDesignTest {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).* from "./s3Inline"

  def req(e: int, s: int, p: int): Req = { epoch: e, slot: s, entry: dataE(e, p), sawFree: true }
  def tomb(e: int, s: int): Req = { epoch: e, slot: s, entry: tombE(e), sawFree: true }
  // Epoch e commits payload p at slot s, all going well.
  action publish(e: int, p: int, s: int): bool =
    startPush(e, p).then(send(e)).then(apply(req(e, s, p)))
      .then(receive(e, { epoch: e, slot: s, entry: dataE(e, p), ok: true }))
  action consume(e: int): bool = cCheck(e).then(cInsert).then(cAdvance)

  // F1: the PUT lands, the answer is lost. HEAD finds our batch: one commit,
  // acknowledged, and no second key for the retry to write.
  run ambiguousLandedResolvedOwnTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(apply(req(1, 0, 1)))
      .then(timeout(1)).then(resolve(1))
      .expect(and { queue == Set(2), slotsOf(1, 1) == Set(0), writers.get(1).nextSlot == 1,
                    events.contains("resolvedOwn"), safety })

  // F1, late: the exporter gave up on slot 0, and the queue's next request
  // (another payload) goes to the same slot while the first is in flight. The
  // first lands first: the second gets 412, learns that payload 1 is
  // committed there, and appends at slot 1. Both committed once.
  run switchedLoserLearnsOtherTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(timeout(1))
      .then(switchPayload(1, 2)).then(send(1))
      .then(apply(req(1, 0, 1)))                                          // the late first request wins
      .then(apply(req(1, 0, 2)))                                          // 412
      .then(receive(1, { epoch: 1, slot: 0, entry: dataE(1, 2), ok: false }))
      .then(send(1)).then(apply(req(1, 1, 2)))
      .then(receive(1, { epoch: 1, slot: 1, entry: dataE(1, 2), ok: true }))
      .expect(and { queue == Set(), slotsOf(1, 1) == Set(0), slotsOf(1, 2) == Set(1),
                    events.contains("learnedOther"), safety })

  // ...and the other order: the second lands first; the late first gets 412
  // and changes nothing. Payload 1 stays queued and is committed later.
  run switchedWinnerLateLoserTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(timeout(1))
      .then(switchPayload(1, 2)).then(send(1))
      .then(apply(req(1, 0, 2)))
      .then(receive(1, { epoch: 1, slot: 0, entry: dataE(1, 2), ok: true }))
      .then(apply(req(1, 0, 1)))                                          // late: 412, nobody waits for it
      .then(publish(1, 1, 1))
      .expect(and { queue == Set(), slotsOf(1, 2) == Set(0), slotsOf(1, 1) == Set(1), safety })

  // F1 across a crash: committed, not acknowledged, process died. The next
  // incarnation commits the request again in its own log; the consumer's
  // content-key check ingests it once.
  run crashRedeliveryDedupedByConsumerTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(apply(req(1, 0, 1)))
      .then(newIncarnation(false))
      .then(publish(2, 1, 0))
      .then(consume(1)).then(consume(2))
      .expect(and { central.get(1) == 1, events.contains("dedupHit"), wCrossEpochCopy, safety })

  // A zombie is fenced by the consumer's tombstone: the consumer closes
  // superseded epoch 1 at its first free slot; the zombie's next PUT gets
  // 412, HEAD shows the tombstone, and it halts. Nothing lands after it.
  run zombieFencedByTombstoneTest =
    init
      .then(publish(1, 1, 0))
      .then(newIncarnation(true))
      .then(consume(1))
      .then(cTomb(1)).then(apply(tomb(1, 1)))
      .then(cTombReceive({ epoch: 1, slot: 1, entry: tombE(1), ok: true }))
      .then(startPush(1, 2)).then(send(1)).then(apply(req(1, 1, 2)))
      .then(receive(1, { epoch: 1, slot: 1, entry: dataE(1, 2), ok: false }))
      .expect(and { closed.get(1), writers.get(1).phase == WHalted, queue == Set(2), noCommitAfterClose, safety })

  // The consumer races a tombstone into a dead epoch's head, but the dead
  // writer's late request lands first: the tombstone gets 412, the consumer
  // ingests the batch, and closes the epoch one slot later.
  run tombLosesToLateDataTest =
    init
      .then(startPush(1, 1)).then(send(1))
      .then(newIncarnation(false))
      .then(cTomb(1))
      .then(apply(req(1, 0, 1)))
      .then(apply(tomb(1, 0)))
      .then(cTombReceive({ epoch: 1, slot: 0, entry: tombE(1), ok: false }))
      .then(consume(1))
      .then(cTomb(1)).then(apply(tomb(1, 1)))
      .then(cTombReceive({ epoch: 1, slot: 1, entry: tombE(1), ok: true }))
      .expect(and { central.get(1) == 1, closed.get(1), ckpt.get(1) == 1, events.contains("tombLostToData"), safety })

  // The consumer's own tombstone times out, and lands anyway; the resolve
  // reads it and closes the epoch.
  run tombTimeoutResolvedTest =
    init
      .then(newIncarnation(false))
      .then(cTomb(1)).then(cTombTimeout).then(apply(tomb(1, 0))).then(cTombResolve)
      .expect(and { closed.get(1), safety })

  // F4: the consumer crashes between the insert and the checkpoint; the
  // check before insert finds the content key and skips the insert.
  run crashBetweenInsertAndCheckpointTest =
    init
      .then(publish(1, 1, 0))
      .then(cCheck(1)).then(cInsert).then(cCrash)
      .then(consume(1))
      .expect(and { central.get(1) == 1, ckpt.get(1) == 1, events.contains("dedupHit"), safety })

  // Everything committed, ingested, and the dead epoch closed.
  run everythingIngestedAndClosedTest =
    init
      .then(publish(1, 1, 0))
      .then(newIncarnation(false))
      .then(publish(2, 2, 0))
      .then(consume(1)).then(consume(2))
      .then(cTomb(1)).then(apply(tomb(1, 1)))
      .then(cTombReceive({ epoch: 1, slot: 1, entry: tombE(1), ok: true }))
      .expect(and { wAllClosed, safety })
}

// Stock awss3exporter: a timed-out attempt is retried under a new key. The
// first request lands late: the request is committed twice in one epoch, and
// the slot it left behind is a gap. (With random keys it would be two objects.)
module retryNewKeyTest {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = false, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).* from "./s3Inline"
  def req(e: int, s: int, p: int): Req = { epoch: e, slot: s, entry: dataE(e, p), sawFree: true }
  def tomb(e: int, s: int): Req = { epoch: e, slot: s, entry: tombE(e), sawFree: true }

  run lateCopyCommitsTwiceTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(timeout(1))
      .then(send(1)).then(apply(req(1, 1, 1)))
      .then(receive(1, { epoch: 1, slot: 1, entry: dataE(1, 1), ok: true }))
      .then(apply(req(1, 0, 1)))
      .expect(and { slotsOf(1, 1) == Set(0, 1), not(epochNoDuplicatePayload) })

  // The lost first request leaves slot 0 free below committed slot 1. A
  // tombstone at the first free slot no longer closes the epoch.
  run gapBreaksTombstoneCloseTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(timeout(1))
      .then(lose(req(1, 0, 1)))
      .then(send(1)).then(apply(req(1, 1, 1)))
      .then(receive(1, { epoch: 1, slot: 1, entry: dataE(1, 1), ok: true }))
      .then(newIncarnation(true))
      .then(cTomb(1)).then(apply(tomb(1, 0)))
      .then(cTombReceive({ epoch: 1, slot: 0, entry: tombE(1), ok: true }))
      .expect(and { closed.get(1), not(consumerNeverSkipsCommitted), not(noCommitAfterClose) })
}

// The consumer takes a dead epoch's free slot as lost; the late request lands behind it.
module skipGapsTest {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = false,
    CHECK_CENTRAL = true).* from "./s3Inline"
  def req(e: int, s: int, p: int): Req = { epoch: e, slot: s, entry: dataE(e, p), sawFree: true }

  run lateRequestBehindCheckpointTest =
    init
      .then(startPush(1, 1)).then(send(1))
      .then(newIncarnation(false))
      .then(cSkipGap(1))
      .then(apply(req(1, 0, 1)))
      .expect(and { central.get(1) == 0, not(gapNeverTakenForLoss), not(consumerNeverSkipsCommitted) })
}

// A writer that finds a tombstone steps over it and commits after the close.
module noHaltTest {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = false, TOMB_GAPS = true,
    CHECK_CENTRAL = true).* from "./s3Inline"
  def req(e: int, s: int, p: int): Req = { epoch: e, slot: s, entry: dataE(e, p), sawFree: true }
  def tomb(e: int, s: int): Req = { epoch: e, slot: s, entry: tombE(e), sawFree: true }

  run zombieWritesPastTombstoneTest =
    init
      .then(newIncarnation(true))
      .then(cTomb(1)).then(apply(tomb(1, 0)))
      .then(cTombReceive({ epoch: 1, slot: 0, entry: tombE(1), ok: true }))
      .then(startPush(1, 1)).then(send(1)).then(apply(req(1, 0, 1)))
      .then(receive(1, { epoch: 1, slot: 0, entry: dataE(1, 1), ok: false }))
      .then(send(1)).then(apply(req(1, 1, 1)))
      .then(receive(1, { epoch: 1, slot: 1, entry: dataE(1, 1), ok: true }))
      .expect(and { queue == Set(2), central.get(1) == 0, not(noCommitAfterClose), not(consumerNeverSkipsCommitted) })
}

// Conditional check at arrival, unconditional write later (SeaweedFS
// multipart completion): both the tombstone and the late batch "win".
module nonAtomicCondTest {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = false, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).* from "./s3Inline"
  def req(e: int, s: int, p: int): Req = { epoch: e, slot: s, entry: dataE(e, p), sawFree: true }
  def tomb(e: int, s: int): Req = { epoch: e, slot: s, entry: tombE(e), sawFree: true }

  run lateBatchOverwritesTombstoneTest =
    init
      .then(startPush(1, 1)).then(send(1))
      .then(newIncarnation(true))
      .then(cTomb(1)).then(apply(tomb(1, 0)))
      .then(cTombReceive({ epoch: 1, slot: 0, entry: tombE(1), ok: true }))
      .then(apply(req(1, 0, 1)))
      .then(receive(1, { epoch: 1, slot: 0, entry: dataE(1, 1), ok: true }))
      .expect(and { closed.get(1), queue == Set(2), events.contains("overwrote"),
                    not(noCommitLost), not(consumerNeverSkipsCommitted) })
}

// Plain PUT: a late request overwrites a committed, acknowledged batch.
module plainPutTest {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = false, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = true).* from "./s3Inline"
  def req(e: int, s: int, p: int): Req = { epoch: e, slot: s, entry: { kind: Data, epoch: e, payload: p }, sawFree: true }

  run lateRequestOverwritesAckedBatchTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(timeout(1))
      .then(switchPayload(1, 2)).then(send(1))
      .then(apply(req(1, 0, 2)))
      .then(receive(1, { epoch: 1, slot: 0, entry: dataE(1, 2), ok: true }))
      .then(apply(req(1, 0, 1)))
      .expect(and { acked == Set(2), not(noCommitLost), not(ackedImpliesCommitted), not(noPayloadLost) })
}

// No check before insert: a crash between insert and checkpoint ingests twice.
module noCheckCentralTest {
  import s3Inline(PAYLOADS = Set(1, 2), MAX_EPOCH = 3, MAX_SLOT = 4,
    AMBIGUOUS = true, ZOMBIES = true, CONSUMER_CRASH = true,
    COND_PUT = true, ATOMIC_COND = true, RESOLVE_BY_HEAD = true, HALT_ON_TOMB = true, TOMB_GAPS = true,
    CHECK_CENTRAL = false).* from "./s3Inline"
  def req(e: int, s: int, p: int): Req = { epoch: e, slot: s, entry: dataE(e, p), sawFree: true }

  run crashBetweenInsertAndCheckpointReingestsTest =
    init
      .then(startPush(1, 1)).then(send(1)).then(apply(req(1, 0, 1)))
      .then(receive(1, { epoch: 1, slot: 0, entry: dataE(1, 1), ok: true }))
      .then(cCheck(1)).then(cInsert).then(cCrash)
      .then(cCheck(1)).then(cInsert).then(cAdvance)
      .expect(and { central.get(1) == 2, not(payloadIngestedAtMostOnce) })
}
