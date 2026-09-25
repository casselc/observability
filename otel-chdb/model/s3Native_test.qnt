// -*- mode: Bluespec; -*-
// Deterministic scenarios for s3Native: each finding of edgePublish (F1-F4)
// and of the property-based tests (PBT.md, findings 1-3) against the S3-native
// design, and the same story against the mutation that breaks it.
//
//   quint test s3Native_test.qnt --main designTest --backend typescript
//
// Helper actions are repeated in each module: they must bind to that
// instance's state.

module designTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  // An append that S3 applies and answers.
  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  // Publish payload p by epoch e into generation g at slot s, all going well.
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))
  // A worker consumes the slot the checkpoint points at.
  action consume(w: int): bool = cBegin(w).then(cCheck(w)).then(cInsert(w)).then(cAdvance(w))

  // F1. The commit PUT lands but its response is lost. The writer reads the
  // slot, finds its own entry, and acknowledges: committed once, with no
  // second batch id to ingest.
  run ambiguousCommitResolvedOnceTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, true))
      .then(send(1))
      .then(apply({ slot: 0, entry: commitE(1, 1, 1) }))
      .then(timeout(1))
      .then(resolve(1))
      .expect(and { queue == Set(2), committed(log) == Set(1), logNoDuplicatePayload, events.contains("resolvedOwn") })

  // F1 when the request is still in flight at the timeout: the slot reads
  // free, the writer resends the same create-only PUT, and whichever copy
  // lands first wins; the other gets 412.
  run lateRequestLandsOnceTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, true))
      .then(send(1)).then(timeout(1))
      .then(resolve(1))                                  // slot 0 free: resend
      .then(send(1))
      .then(apply({ slot: 0, entry: commitE(1, 1, 1) }))
      .then(receive(1, { slot: 0, entry: commitE(1, 1, 1), ok: true }))
      .expect(and { queue == Set(2), commitsOf(1) == Set(0), writers.get(1).nextSlot == 1 })

  // F1 across a crash: the commit landed, the process died before the queue
  // acknowledged. The next epoch fences, replays the log, and acknowledges
  // the retried payload without publishing it again.
  run crashAfterCommitReplaySkipsTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, true))
      .then(send(1)).then(apply({ slot: 0, entry: commitE(1, 1, 1) }))
      .then(newIncarnation(false))
      .then(appended(2, 1, fenceE(2)))
      .then(startPush(2, 1))
      .expect(and { queue == Set(2), writers.get(2).known == Set(1), events.contains("skipKnown"), logNoDuplicatePayload })

  // A zombie (epoch 1 keeps running after epoch 2 starts) tries to commit:
  // its slot holds epoch 2's fence, so it halts. Nothing after the fence is epoch 1's.
  run zombieIsFencedTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(startPush(1, 2)).then(writeData(1, true, true))
      .then(newIncarnation(true))
      .then(appended(2, 1, fenceE(2)))
      .then(send(1))
      .then(apply({ slot: 1, entry: commitE(1, 2, 1) }))
      .then(receive(1, { slot: 1, entry: commitE(1, 2, 1), ok: false }))
      .expect(and { writers.get(1).phase == WHalted, noWriteFromFencedWriter, events.contains("halted") })

  // The zombie's request is in flight when epoch 2 fences the same slot:
  // If-None-Match is decided when S3 applies it, so the late request loses.
  run lateZombieRequestLosesTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, true))
      .then(send(1))                                     // to slot 0, delayed
      .then(newIncarnation(true))
      .then(appended(2, 0, fenceE(2)))
      .then(apply({ slot: 0, entry: commitE(1, 1, 1) }))
      .expect(and { log.get(0) == fenceE(2), committed(log) == Set(), noWriteFromFencedWriter, noCommitLost })

  // PBT 1. A restart within the same generation gets its own namespace: the
  // namespace carries the epoch, so nothing of epoch 1's is written again.
  run restartGetsOwnNamespaceTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(newIncarnation(false))
      .then(appended(2, 1, fenceE(2)))
      .then(publish(2, 2, 1, 2))
      .expect(and { nsSingleWriter, commitImpliesData,
                    data == Set({ ns: { epoch: 1, gen: 1 }, payload: 1 }, { ns: { epoch: 2, gen: 1 }, payload: 2 }) })

  // PBT 2. The clock steps back after a rotation. The generation only moves
  // forward (by a seal entry), so the push lands in generation 2, not the sealed 1.
  run clockBackDoesNotReopenTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(tick).then(rotate(1)).then(appended(1, 1, sealE(1, 1)))
      .then(tickBack)
      .then(publish(1, 2, 2, 2))
      .expect(and { writers.get(1).gen == 2, sealMatchesManifests, sealOnce, not(events.contains("reopened")) })

  // PBT 3. The seal append times out and never lands; the writer resolves it
  // like any append and resends, so the generation does get sealed.
  run failedSealIsRetriedTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(tick).then(rotate(1))
      .then(send(1)).then(lose({ slot: 1, entry: sealE(1, 1) }))
      .then(timeout(1))
      .expect(generationsSealedOrPending)
      .then(resolve(1))
      .then(appended(1, 1, sealE(1, 1)))
      .expect(and { writers.get(1).gen == 2, closedGen(log, 1, 1), generationsSealedOrPending })

  // PBT 3 for a crash: the new epoch's fence closes every generation of the old one.
  run fenceClosesAbandonedGenerationsTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(newIncarnation(false))
      .then(appended(2, 1, fenceE(2)))
      .expect(and { closedGen(log, 1, 1), sealMatchesManifests, generationsSealedOrPending })

  // F4. A worker inserts, then crashes before advancing the checkpoint; other
  // inserts evict its token. The next lease holder checks central first.
  run crashBetweenInsertAndCheckpointTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(takeLease(1)).then(cBegin(1)).then(cCheck(1)).then(cInsert(1))
      .then(otherInsert).then(otherInsert)
      .then(workerCrash(1))
      .then(takeLease(2)).then(consume(2))
      .expect(and { central.get(1) == 1, ckpt.next == 1, events.contains("dedupHit"), payloadIngestedAtMostOnce })

  // A zombie worker's checkpoint write loses to its successor's.
  run zombieCheckpointLosesTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(publish(1, 2, 1, 1))
      .then(takeLease(1)).then(cBegin(1)).then(cCheck(1)).then(cInsert(1))
      .then(takeLease(2)).then(consume(2)).then(consume(2))
      .then(cAdvance(1))
      .expect(and { ckpt.next == 2, events.contains("ckptCasFailed"), payloadIngestedAtMostOnce })

  // GC waits for a reader's lease, and a lease taken between GC's read and
  // its write makes the write fail (If-Match on the GC-state object).
  run readerLeaseStopsGcTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(publish(1, 2, 1, 1))
      .then(takeLease(1)).then(consume(1)).then(consume(1))
      .then(gcPlan)                                      // plans horizon 2
      .then(readerLease(1, 1))
      .then(gcCommit)
      .expect(and { gc.horizon == 0, events.contains("gcCasFailed") })
      .then(gcPlan)                                      // now plans 1
      .then(gcCommit)
      .then(gcDelete)
      .then(readerRead(1))
      .expect(and { gc.horizon == 1, noReadOfDeleted, gcKeepsLiveData, events.contains("readerRead") })

  // An orphan (data written, commit never made, the generation then closed) is swept.
  run orphanSweptAfterSealTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, false))   // data lands, writer sees an error
      .then(tick).then(rotate(1)).then(appended(1, 0, sealE(1, 1)))
      .then(gcDelete)
      .expect(and { data == Set(), events.contains("orphanSwept"), queue == Set(1, 2) })

  // Liveness, as a scenario: both payloads committed, the generation sealed,
  // both ingested once.
  run everyPayloadCommitsAndIsIngestedTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(publish(1, 2, 1, 1))
      .then(tick).then(rotate(1)).then(appended(1, 2, sealE(1, 1)))
      .then(takeLease(1)).then(consume(1)).then(consume(1)).then(consume(1))
      .expect(and { wAllCommitted, wAllIngested, wAllClosed, safety })
}

// PBT finding 1 as it is in publish.go: namespaces by generation only, local
// tables that survive the restart.
module restartReusesTableTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = false, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))

  // Epoch 2's rows go to epoch 1's table and endpoint; its commit names its own, empty one.
  run restartWritesIntoPreviousEpochTableTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(newIncarnation(false))
      .then(appended(2, 1, fenceE(2)))
      .then(publish(2, 2, 1, 2))
      .expect(and { not(nsSingleWriter), not(commitImpliesData),
                    data.contains({ ns: { epoch: 1, gen: 1 }, payload: 2 }) })
}

// PBT finding 2: the generation comes from the clock.
module genFromClockTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = false, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))

  // The clock steps back: the push reopens sealed generation 1 and commits
  // into it; when the clock moves on, generation 1 is sealed a second time.
  run clockBackReopensSealedGenerationTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(tick).then(rotate(1)).then(appended(1, 1, sealE(1, 1)))
      .then(tickBack)
      .then(publish(1, 2, 1, 2))
      .expect(and { events.contains("reopened"), not(sealMatchesManifests) })
      .then(tick).then(rotate(1)).then(appended(1, 3, sealE(1, 1)))
      .expect(not(sealOnce))
}

// PBT finding 3: a failed seal is not retried.
module noSealRetryTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = false,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))

  run failedSealLeavesGenerationOpenTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(tick).then(rotate(1))
      .then(send(1)).then(lose({ slot: 1, entry: sealE(1, 1) }))
      .then(timeout(1))
      .expect(and { writers.get(1).gen == 2, not(closedGen(log, 1, 1)), not(generationsSealedOrPending) })
}

// Mutation: plain PUTs for log slots. A zombie's late request overwrites the fence.
module nonConditionalLogTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = false, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))

  run lateZombieRequestOverwritesTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, true))
      .then(send(1))
      .then(newIncarnation(true))
      .then(appended(2, 0, fenceE(2)))
      .then(startPush(2, 1)).then(writeData(2, true, true))
      .then(appended(2, 1, commitE(2, 1, 1)))
      .then(apply({ slot: 0, entry: commitE(1, 1, 1) }))    // the late request overwrites the fence
      .expect(and { log.get(0) == commitE(1, 1, 1), events.contains("overwrote"), not(logNoDuplicatePayload) })
}

// Mutation: a lease without fencing (per-epoch logs). The zombie commits
// after its successor, which had replayed the log before: the request is
// committed twice and a fenced writer wrote.
module leaseWithoutFencingTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = false, FENCE_ENTRY = false, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))

  run zombieCommitsAfterSuccessorTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, true))
      .then(newIncarnation(true))                        // replays an empty log
      .then(publish(2, 1, 1, 0))                         // the successor commits payload 1
      // the zombie's slot 0 holds epoch 2's entry, which is not in its log: it moves on
      .then(send(1)).then(apply({ slot: 0, entry: commitE(1, 1, 1) }))
      .then(receive(1, { slot: 0, entry: commitE(1, 1, 1), ok: false }))
      .then(appended(1, 1, commitE(1, 1, 1)))
      .expect(and { not(noWriteFromFencedWriter), not(logNoDuplicatePayload) })
}

// One shared log without a fence entry: the zombie is still stopped by the
// collision, and the successor still skips what it collides with. What is
// lost is closing the predecessor's generations: they stay open (and their
// orphans are never swept) until something else seals them.
module noFenceEntryTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = false, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))

  run collisionStillFencesTest =
    init
      .then(startPush(1, 1)).then(writeData(1, true, true))
      .then(newIncarnation(true))
      .then(send(1)).then(apply({ slot: 0, entry: commitE(1, 1, 1) }))    // lands after the successor's replay
      .then(timeout(1))                                  // ...unacknowledged, so the queue hands it out again
      .then(startPush(2, 1)).then(writeData(2, true, true))
      .then(send(2)).then(apply({ slot: 0, entry: commitE(2, 1, 1) }))
      .then(receive(2, { slot: 0, entry: commitE(2, 1, 1), ok: false }))   // collides, sees payload 1: acks
      .expect(and { logNoDuplicatePayload, queue == Set(2), not(closedGen(log, 1, 1)) })
      .then(resolve(1))
      .then(startPush(1, 2)).then(writeData(1, true, true))
      .then(publish(2, 2, 1, 1))
      .then(send(1)).then(apply({ slot: 1, entry: commitE(1, 2, 1) }))
      .then(receive(1, { slot: 1, entry: commitE(1, 2, 1), ok: false }))
      .expect(and { writers.get(1).phase == WHalted, noWriteFromFencedWriter, not(closedGen(log, 1, 1)) })
}

// Mutation: blind checkpoint writes. A zombie worker moves the checkpoint back
// below data GC has already deleted.
module blindCheckpointTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = false, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))
  action consume(w: int): bool = cBegin(w).then(cCheck(w)).then(cInsert(w)).then(cAdvance(w))

  run zombieCheckpointRegressesTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(publish(1, 2, 1, 1))
      .then(takeLease(1)).then(cBegin(1)).then(cCheck(1)).then(cInsert(1))
      .then(takeLease(2)).then(consume(2)).then(consume(2))
      .then(gcPlan).then(gcCommit).then(gcDelete)
      .then(cAdvance(1))                                 // blind: next = 1
      .expect(and { ckpt.next == 1, not(gcKeepsLiveData) })
}

// Mutation: GC writes its state blindly, dropping a lease taken meanwhile.
module gcBlindWriteTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = false).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))
  action consume(w: int): bool = cBegin(w).then(cCheck(w)).then(cInsert(w)).then(cAdvance(w))

  run gcDeletesUnderReaderTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(takeLease(1)).then(consume(1))
      .then(gcPlan)
      .then(readerLease(1, 0))
      .then(gcCommit).then(gcDelete)
      .then(readerRead(1))
      .expect(and { not(noReadOfDeleted), not(gcKeepsLiveData) })
}

// The gap S3 cannot close: a worker that outlives its lease inserts anyway.
module unboundedZombieInsertTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = false,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = true, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))

  // Both check before either inserts; other traffic evicts the first token.
  run zombieInsertAfterEvictionTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(takeLease(1)).then(cBegin(1)).then(cCheck(1))
      .then(takeLease(2)).then(cBegin(2)).then(cCheck(2))
      .then(cInsert(2)).then(otherInsert).then(otherInsert)
      .then(cInsert(1))
      .expect(not(payloadIngestedAtMostOnce))

  // Within the window the content token absorbs the zombie's insert.
  run zombieInsertWithinWindowTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(takeLease(1)).then(cBegin(1)).then(cCheck(1))
      .then(takeLease(2)).then(cBegin(2)).then(cCheck(2))
      .then(cInsert(2))
      .then(cInsert(1))
      .expect(and { payloadIngestedAtMostOnce, events.contains("dedupHit") })
}

// F4 without the check before insert: insert + checkpoint is not atomic.
module noCheckCentralTest {
  import s3Native(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), READERS = Set(1), MAX_EPOCH = 2, MAX_SLOT = 6,
    MAX_GEN = 3, MAX_CEPOCH = 3, DEDUP_WINDOW = 2,
    AMBIGUOUS = true, ZOMBIES = true, CLOCK_SKEW = true, OTHER_TRAFFIC = true, ZOMBIE_INSERT_BOUNDED = true,
    COND_LOG = true, SHARED_LOG = true, FENCE_ENTRY = true, NS_PER_EPOCH = true, GEN_FROM_LOG = true, SEAL_RETRY = true,
    CKPT_CAS = true, CHECK_CENTRAL = false, GC_WAITS_FOR_LEASES = true, GC_CAS = true).* from "./s3Native"

  action appended(e: int, s: int, x: Entry): bool =
    send(e).then(apply({ slot: s, entry: x })).then(receive(e, { slot: s, entry: x, ok: true }))
  action publish(e: int, p: int, g: int, s: int): bool =
    startPush(e, p).then(writeData(e, true, true)).then(appended(e, s, commitE(e, p, g)))
  action consume(w: int): bool = cBegin(w).then(cCheck(w)).then(cInsert(w)).then(cAdvance(w))

  run crashBetweenInsertAndCheckpointReingestsTest =
    init
      .then(publish(1, 1, 1, 0))
      .then(takeLease(1)).then(cBegin(1)).then(cCheck(1)).then(cInsert(1))
      .then(otherInsert).then(otherInsert)
      .then(workerCrash(1))
      .then(takeLease(2)).then(consume(2))
      .expect(and { central.get(1) == 2, not(payloadIngestedAtMostOnce) })
}
