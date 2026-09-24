// -*- mode: Bluespec; -*-
// Deterministic scenarios for edgePublish: each is the minimal story behind
// one invariant violation `quint run` found in the current design, and the
// same story against the proposed fixes.

module scenarios {
  import edgePublish.* from "./edgePublish"
  // Pure helpers only: actions must be defined in each instance's module,
  // where they bind to that instance's state.

  // The push of payload p as batch b of epoch e in generation g, at a phase.
  pure def px(p: int, e: int, b: int, g: int, ph: Phase): Push =
    { payload: p, bk: { epoch: e, batch: b }, gk: { epoch: e, gen: g }, phase: ph }
  pure def mf(p: int, e: int, b: int, g: int): Manifest =
    { bk: { epoch: e, batch: b }, gk: { epoch: e, gen: g }, payload: p }

}

// The current design, in the environment where things go wrong.
module currentTest {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = false, CONTENT_KEY = false, CONTENT_LEASE = false, CHECK_CENTRAL = false).*
  import scenarios.*

  // Publish payload p as batch b, all three writes succeeding.
  action publish(p: int, b: int): bool =
    pushStart(p)
      .then(writeTable(px(p, 1, b, 1, Encoded), Ok))
      .then(writeParquet(px(p, 1, b, 1, TableDone), Ok))
      .then(writeManifest(px(p, 1, b, 1, ParquetDone), Ok))

  // A worker ingests a manifest to the end.
  action ingest(w: int, m: Manifest): bool = claim(w, m).then(insert(w)).then(ack(w))

  // F1. The manifest PUT lands but the exporter sees an error (a timeout,
  // or a crash before the queue acks). The queue retries the payload, which
  // is published again as batch 2. Both batches are committed, and the
  // consumer, keyed on (epoch, batch_id), ingests the request twice.
  run ambiguousManifestIngestsTwiceTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ambiguous))
      .then(publish(1, 2))
      .then(ingest(1, mf(1, 1, 1, 1)))
      .then(ingest(1, mf(1, 1, 2, 1)))
      .expect(and {
        queue == Set(2),
        manifests.size() == 2,
        payloadCount(centralCount, manifests, 1) == 2,
        not(payloadIngestedAtMostOnce),
      })

  // F2. Table insert succeeds, the Parquet PUT fails, the payload is retried:
  // the orphan rows of batch 1 stay in the table. A consumer selecting by
  // manifests never sees them, but a reader attached live does, twice.
  run orphanRowsVisibleToLiveReadersTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Fail))
      .then(publish(1, 2))
      .expect(and {
        not(liveTableNoDuplicatePayload),
        committedNoDuplicatePayload,
        onlyCommittedIngested,
      })

  // F3. The seal is built from the writer's in-memory success counters. An
  // ambiguous manifest write is committed in S3 but never counted, so
  // _sealed.json says the generation holds 1 batch while S3 holds 2.
  run sealUndercountsAmbiguousCommitTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ambiguous))
      .then(publish(2, 2))
      .then(rotateGen)
      .then(sealGen({ epoch: 1, gen: 1 }))
      .expect(and {
        seals == Set({ gk: { epoch: 1, gen: 1 }, batches: Set({ epoch: 1, batch: 2 }) }),
        manifests.filter(m => m.gk.gen == 1).size() == 2,
        not(sealMatchesManifests),
      })

  // F4. A worker inserts, then dies before the ledger update; meanwhile other
  // inserts push its token out of the 2-entry dedup window. The retry is not
  // recognised as a duplicate: the batch is ingested twice.
  run dedupWindowEvictionReingestsTest =
    init
      .then(publish(1, 1))
      .then(claim(1, mf(1, 1, 1, 1)))
      .then(insert(1))
      .then(otherInsert)
      .then(otherInsert)
      .then(workerCrash(1))
      .then(ingest(2, mf(1, 1, 1, 1)))
      .expect(and {
        centralCount.get({ epoch: 1, batch: 1 }) == 2,
        not(batchIngestedAtMostOnce),
      })

  // Control: within the window, the retried insert is deduplicated.
  run dedupWithinWindowHoldsTest =
    init
      .then(publish(1, 1))
      .then(claim(1, mf(1, 1, 1, 1)))
      .then(insert(1))
      .then(workerCrash(1))
      .then(ingest(2, mf(1, 1, 1, 1)))
      .expect(and { centralCount.get({ epoch: 1, batch: 1 }) == 1, batchIngestedAtMostOnce, events.contains("dedupHit") })
}

// The first fix (content keys, listing seals), same stories.
module fixedTest {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = false, CHECK_CENTRAL = false).*
  import scenarios.*

  // Publish payload p as batch b, all three writes succeeding.
  action publish(p: int, b: int): bool =
    pushStart(p)
      .then(writeTable(px(p, 1, b, 1, Encoded), Ok))
      .then(writeParquet(px(p, 1, b, 1, TableDone), Ok))
      .then(writeManifest(px(p, 1, b, 1, ParquetDone), Ok))

  // A worker ingests a manifest to the end.
  action ingest(w: int, m: Manifest): bool = claim(w, m).then(insert(w)).then(ack(w))

  // F1 fixed: the second batch carries the same content key; the consumer
  // acknowledges it without inserting.
  run ambiguousManifestIngestedOnceTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ambiguous))
      .then(publish(1, 2))
      .then(ingest(1, mf(1, 1, 1, 1)))
      .then(skipDuplicate(mf(1, 1, 2, 1)))
      .expect(and {
        ledger == Set({ epoch: 1, batch: 1 }, { epoch: 1, batch: 2 }),
        payloadCount(centralCount, manifests, 1) == 1,
        payloadIngestedAtMostOnce,
      })

  // F3 fixed: sealing from the manifest listing counts what S3 holds.
  run sealFromListingMatchesTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ambiguous))
      .then(publish(2, 2))
      .then(rotateGen)
      .then(sealGen({ epoch: 1, gen: 1 }))
      .expect(sealMatchesManifests)

  // F1 with a race the ledger check cannot see: two workers claim the two
  // copies at once. The content-keyed insert token deduplicates the second.
  run concurrentCopiesDeduplicatedByTokenTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ambiguous))
      .then(publish(1, 2))
      .then(claim(1, mf(1, 1, 1, 1)))
      .then(claim(2, mf(1, 1, 2, 1)))
      .then(insert(1))
      .then(insert(2))
      .expect(and { payloadIngestedAtMostOnce, events.contains("dedupHit") })

  // ...but it still relies on the dedup window: the same race with enough
  // other traffic in between ingests the request twice. The ledger closes
  // this only for copies that are claimed after the first is acknowledged.
  run concurrentCopiesOutsideWindowTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ambiguous))
      .then(publish(1, 2))
      .then(claim(1, mf(1, 1, 1, 1)))
      .then(claim(2, mf(1, 1, 2, 1)))
      .then(insert(1))
      .then(otherInsert)
      .then(otherInsert)
      .then(insert(2))
      .expect(not(payloadIngestedAtMostOnce))
}

// Mutation: without the rotation lock, a push that started in generation 1
// commits after generation 1 is sealed, even with listing-based seals.
module noLockTest {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = false, OTHER_TRAFFIC = false,
    ROTATION_LOCK = false, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = true, CHECK_CENTRAL = true).*
  import scenarios.*

  // Publish payload p as batch b, all three writes succeeding.
  action publish(p: int, b: int): bool =
    pushStart(p)
      .then(writeTable(px(p, 1, b, 1, Encoded), Ok))
      .then(writeParquet(px(p, 1, b, 1, TableDone), Ok))
      .then(writeManifest(px(p, 1, b, 1, ParquetDone), Ok))

  // A worker ingests a manifest to the end.
  action ingest(w: int, m: Manifest): bool = claim(w, m).then(insert(w)).then(ack(w))

  run commitAfterSealTest =
    init
      .then(pushStart(1))
      .then(rotateGen)
      .then(sealGen({ epoch: 1, gen: 1 }))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ok))
      .expect(not(sealMatchesManifests))
}

// With the lock (the current design), the same interleaving cannot happen:
// rotation is disabled while generation 1 has a push in flight.
module lockTest {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = false, OTHER_TRAFFIC = false,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = true, CHECK_CENTRAL = true).*
  import scenarios.*

  // Publish payload p as batch b, all three writes succeeding.
  action publish(p: int, b: int): bool =
    pushStart(p)
      .then(writeTable(px(p, 1, b, 1, Encoded), Ok))
      .then(writeParquet(px(p, 1, b, 1, TableDone), Ok))
      .then(writeManifest(px(p, 1, b, 1, ParquetDone), Ok))

  // A worker ingests a manifest to the end.
  action ingest(w: int, m: Manifest): bool = claim(w, m).then(insert(w)).then(ack(w))

  run rotationWaitsForInFlightPushTest =
    init
      .then(pushStart(1))
      .then(rotateGen)
      .fail()
}

// The full fix: content leases and a central check make the same stories
// safe without depending on the dedup window.
module fullFixTest {
  import edgePublish(PAYLOADS = Set(1, 2), WORKERS = Set(1, 2), MAX_EPOCH = 2, MAX_BATCH = 3, MAX_GEN = 2,
    DEDUP_WINDOW = 2, AMBIGUOUS_WRITES = true, OTHER_TRAFFIC = true,
    ROTATION_LOCK = true, SEAL_FROM_LISTING = true, CONTENT_KEY = true, CONTENT_LEASE = true, CHECK_CENTRAL = true).*
  import scenarios.*

  action publish(p: int, b: int): bool =
    pushStart(p)
      .then(writeTable(px(p, 1, b, 1, Encoded), Ok))
      .then(writeParquet(px(p, 1, b, 1, TableDone), Ok))
      .then(writeManifest(px(p, 1, b, 1, ParquetDone), Ok))

  // The second copy cannot be claimed while the first is leased.
  run copiesAreSerializedByContentLeaseTest =
    init
      .then(pushStart(1))
      .then(writeTable(px(1, 1, 1, 1, Encoded), Ok))
      .then(writeParquet(px(1, 1, 1, 1, TableDone), Ok))
      .then(writeManifest(px(1, 1, 1, 1, ParquetDone), Ambiguous))
      .then(publish(1, 2))
      .then(claim(1, mf(1, 1, 1, 1)))
      .then(claim(2, mf(1, 1, 2, 1)))
      .fail()

  // F4 with the central check: the retry finds its rows and skips.
  run retryAfterEvictionChecksCentralTest =
    init
      .then(publish(1, 1))
      .then(claim(1, mf(1, 1, 1, 1)))
      .then(insert(1))
      .then(otherInsert)
      .then(otherInsert)
      .then(workerCrash(1))
      .then(claim(2, mf(1, 1, 1, 1)))
      .then(insert(2))
      .expect(and { centralCount.get({ epoch: 1, batch: 1 }) == 1, payloadIngestedAtMostOnce, batchIngestedAtMostOnce })
}
