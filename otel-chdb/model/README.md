# Quint models of the edge-publishing design

Two executable [Quint](https://quint-lang.org) models of the chdb exporter's
publishing mode (`../chdbexporter/publish.go`) and the central consumer it is
meant for. They were built with the skills in
[quint-llm-kit](https://github.com/quint-co/quint-llm-kit): `quint-modeling`
(the from-code flow) and `quint-lang`. Each model is checked two ways:

- `quint run`, which simulates random traces and finds counterexamples;
- `quint verify`, which runs the Apalache model checker: an exhaustive,
  bounded check of every execution up to a given number of steps.

| File | What it models |
| --- | --- |
| `edgePublish.qnt` | The commit protocol end to end. The collector's persistent queue retries a payload after a failed export. The writer crashes and restarts with a new epoch. S3 writes succeed, fail, or fail **ambiguously** (the object lands but the writer sees an error). A batch goes to the table, then Parquet, then its manifest. Generations rotate under a lock and are sealed. Central workers claim manifested batches, insert them with a dedup token, and record them in a ledger; a worker can crash between insert and ledger. |
| `partLifetime.qnt` | Object lifetime on one published table. The writer inserts, merges, and cleans up after `old_parts_lifetime`, and may exit. Readers refresh their list of parts on an interval (or stop refreshing) and run queries of bounded length. The consumer garbage-collects a generation, with or without reader leases. Time is an integer tick, because the timing bounds are the point. |
| `*_test.qnt` | Deterministic scenarios: the minimal story behind each counterexample, and the same story against each fix. |
| `trace.py` | Summarises an ITF trace from `quint run --mbt --out-itf`: the action taken at each step and what changed. |

Design choices are `const` flags, and each configuration is an instance module
(`currentDesign`, `fixedDesign`, `recommended`, `noRotationLock`, …). That lets
one spec show a counterexample and its fix side by side.

```sh
npm i -g @informalsystems/quint     # 0.32; Apalache is fetched on first `quint verify` (needs Java)
quint test edgePublish_test.qnt --main currentTest --backend typescript
quint run  edgePublish.qnt --main recommended --invariant payloadIngestedAtMostOnce \
           --max-steps 40 --max-samples 5000 --backend typescript
quint verify edgePublish.qnt --main fixedDesign --invariant sealMatchesManifests --max-steps 10
```

(`--backend typescript` is only needed where the Rust evaluator can't be
downloaded, as in this sandbox.)

## What the models found

### Model A: publishing and ingest (`edgePublish`)

Each cell is `quint run` over 5,000 random traces of up to 40 steps, so ✓
means no violation was found in that sampling; it is not a proof. ✗ means a
counterexample was found. Where Apalache was also run, its result is noted.
The environment is hostile unless stated otherwise: S3 writes can fail
ambiguously, the writer can crash, workers can crash, and unrelated inserts
can push tokens out of the dedup window.

| Invariant | current | current, ideal env¹ | + listing seal, content key | fixed² | recommended³ |
| --- | --- | --- | --- | --- | --- |
| `commitImpliesData`: a manifest implies its rows and Parquet exist | ✓ | ✓ | ✓ | ✓ (Apalache ≤ 8 steps) | ✓ (Apalache ≤ 8 steps) |
| `onlyCommittedIngested` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `batchIngestedAtMostOnce` | ✗ F4 | ✓ | ✗ F4 | ✓ | ✓ |
| `payloadIngestedAtMostOnce` (end to end) | ✗ F1 | ✓ | ✗ F1′ | ✓ | ✓ |
| `sealMatchesManifests` | ✗ F3 (Apalache: counterexample) | ✓ | ✓ | ✓ (Apalache ≤ 10 steps) | ✓ (Apalache ≤ 8 steps) |
| `liveTableNoDuplicatePayload` | ✗ F2 | ✗ F2 | ✗ | ✗ | ✗ |
| `committedNoDuplicatePayload` | ✗ | ✓ | ✗ | ✗ | ✗ |

¹ No ambiguous writes and no token eviction. ² Listing seal, content key,
content lease, check before insert. ³ The fixed design plus content-derived
batch ids at the edge.

Mutation `noRotationLock`: `sealMatchesManifests` is violated (Apalache finds
the counterexample in 6 steps): without the lock, a push commits into a
generation after it's sealed. The lock in `publish.go` is load-bearing.

The last two rows can't be fixed at the edge. They describe what a reader
sees if it trusts raw rows (F2) or every manifest; only readers can remove
those duplicates (see the mitigations below).

**The findings.** Each has a deterministic scenario in `edgePublish_test.qnt`:

- **F1: ambiguous manifest write → the request is ingested twice.** The
  manifest PUT lands but the exporter sees an error (a timeout, or a crash
  before the persistent queue acks). The queue retries the payload, which the
  exporter publishes as a new batch id. Both batches are committed, so a
  consumer keyed on `(epoch, batch_id)` ingests both
  (`ambiguousManifestIngestsTwiceTest`). The README's earlier claim that
  "retries need no deduplication" is wrong for this case.
- **F1′: content keys alone still depend on the dedup window.** With
  content-keyed tokens, two workers claiming the two copies concurrently are
  deduplicated only while the first token is still in the window
  (`concurrentCopiesOutsideWindowTest`). It takes a content lease plus a
  check before insert to remove the dependence.
- **F2: orphan rows.** The table insert succeeds, then Parquet or the
  manifest fails, and the retry writes the rows again. A consumer that
  selects by manifest never sees the orphans, but a reader attached live
  sees the request twice, even in an ideal environment
  (`orphanRowsVisibleToLiveReadersTest`).
- **F3: the seal undercounts.** `_sealed.json` is built from in-memory
  success counters, so an ambiguous manifest write is committed in S3 but
  not counted (`sealUndercountsAmbiguousCommitTest`).
- **F4: dedup window eviction.** A worker inserts, then crashes before
  updating the ledger. Meanwhile other inserts evict its token, and the retry
  inserts again (`dedupWindowEvictionReingestsTest`).
- **Content-derived batch ids (edge fix):**
  - A retry within a generation is idempotent: the same table block, Parquet
    key and manifest key (`retryInSameGenerationIsIdempotentTest`).
  - A retry after a rotation lands in the next generation
    (`retryAfterRotationPublishesAgainTest`).
  - A consumer ledger keyed on batch id absorbs that copy
    (`ledgerOnBatchIdAbsorbsCrossGenerationCopyTest`).
  - A crash still starts a new epoch, so cross-epoch copies need content
    dedup at the consumer.

### Model B: object lifetime (`partLifetime`)

`quint run` sampled 3,000 traces per cell, plus Apalache where noted.

| Instance | `noReadOfDeleted` | `noLeakAfterExit` | `noDoubleCount` |
| --- | --- | --- | --- |
| `old_parts_lifetime` 0 (chDB's default) | ✗ (Apalache: counterexample ≤ 10) | ✓ | ✓ |
| lifetime = query bound (2), refresh 1 | ✗ (Apalache: counterexample ≤ 12) | ✓ | ✓ |
| lifetime 4 > query bound 2 + refresh 1 | ✓ (Apalache: ≤ 12 steps) | ✓ | ✓ |
| reader stops refreshing | ✗ | ✓ | ✓ |
| writer exits, no GC | ✓ | ✗ | ✓ |
| GC without reader leases | ✗ | ✓ | ✓ |
| proposed: lifetime > Q + R, leases, GC | ✓ (Apalache: ≤ 10 steps) | ✓ (Apalache: ≤ 10 steps) | ✓ |

The safety condition the model pins down is
**`old_parts_lifetime` > max query duration + `refresh_parts_interval`.** A
query can start from a list of parts up to one refresh old. The ClickHouse
server test in `../chdbexporter/publish_test.go` reproduces the lifetime-0
case against a real server.

## What can be controlled in ClickHouse, and what can't

Settings checked in chDB 26.7.3 and ClickHouse 26.10.1. **Verified** means
tried against the real engine here. **Model** means shown in the Quint model
only. **Docs** means per the ClickHouse documentation, not tried.

| Finding | ClickHouse table, query or server setting | Needs a protocol change too? |
| --- | --- | --- |
| F1, F4: duplicate central ingest | (1) Make insert dedup actually work. `non_replicated_deduplication_window` **defaults to 0** (off) for non-replicated MergeTree, in both builds; set it, or `_seconds`, to cover the retry horizon. Replicated tables default to 10,000 blocks / 3,600 s (`replicated_deduplication_window[_seconds]`). (2) Put a **content key** in `insert_deduplication_token`, not the batch id. (3) For native sources, **insert as one block**: `max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_block_size` and `max_block_size` ≥ the batch's rows. The dedup key is the token plus the block's index, and a merge on the writer changes how blocks split. **Verified:** a retry after `OPTIMIZE FINAL` of the source re-inserted all 300,000 rows by default and 0 with these settings. (4) Keep `deduplicate_blocks_in_dependent_materialized_views = 1` (the default) if central has MVs. (5) Last resort: a `ReplacingMergeTree` keyed by content or row identity; eventual, and queries need `FINAL` [docs]. | Yes. The window is best effort (F1′, F4). Correctness needs a content lease plus a check before insert **[model]**. On a replicated central, that check needs `select_sequential_consistency = 1`, or inserts with `insert_quorum`, to read its own writes [docs]. |
| F1, F2 at the edge: retries write twice | Edge tables get `non_replicated_deduplication_window` > 0, and each insert carries `insert_deduplication_token = <content hash>`. **Verified in chDB:** a retried batch through the Null staging table and MV into a table with a window of 1,000 was dropped, while the default-window table duplicated it. | Yes: the batch id must be derived from content too (hash of the RowBinary minus the envelope). Otherwise the kept rows carry the first attempt's id while the manifest names the retry's **[model + verified]**. |
| F2: live readers see orphans and cross-epoch copies | None: nothing makes an S3 manifest and a table insert atomic. | Readers either select `batch_id IN (manifested ids)` and deduplicate by an envelope `content_key` (e.g. `LIMIT 1 BY content_key`), or attach only sealed generations. |
| F3: the seal undercounts | None. | Build the seal from an S3 listing of manifests (chDB `s3('…/manifests/g…/0*.json', 'One')`), not in-memory counters **[model]**. |
| Reads of deleted objects (model B) | `old_parts_lifetime` (table setting; the exporter already sets 10 min) > query bound + refresh interval **[model + verified on server]**. Enforce the query bound on readers with `max_execution_time` in their profile, and the refresh with `refresh_parts_interval` in `ReaderDDL` (already 1). | The catalog must never attach a reader with `refresh_parts_interval = 0` (it breaks after the lifetime **[model]**; measured at ~14 min in `../bench/central`). |
| Leak when the writer exits | None: cleanup only runs in a live writer. Exiting within `old_parts_lifetime` of a merge leaves the replaced parts behind. | Consumer GC deletes the whole generation prefix after acknowledging it, and waits for reader leases **[model]**. Consider `seal_optimize: false`, which avoids a big merge just before exit. |
| GC under a running reader | None. | Leases in the catalog, with GC waiting for them **[model]**. An S3 lifecycle rule is only a backstop: it knows nothing of readers. |

## Model caveats and a tooling lesson

- **Bounded, abstract, small domains.** Two payloads, two workers, two epochs,
  three batch ids and two generations. Payloads stand in for content hashes,
  and time is an abstract tick. The exporter's Go code isn't linked to the
  model; a separate prototype in `../../quintgo` works toward checking real
  traces against it.
- **Apalache is bounded:** "≤ N steps" means every execution of up to N
  steps, not all executions. The payload-duplication counterexample needs
  about 12 steps, which timed out in Apalache at this model size, so it's
  shown by simulation and by a deterministic scenario instead.
- **Not modelled:** non-atomic S3 listings, part-level visibility in the
  reader's list (the covering rule is modelled as atomic), multi-replica
  central tables, and schema changes.
- **Operator precedence (a real bug, found by the scenario tests):**
  `x' = a or b` parses as `(x' = a) or b`. The action fires and leaves `x`
  unchanged. Model B's first sweep reported `noReadOfDeleted` holding at
  lifetime 0 because of it. Parenthesise boolean right-hand sides.
- **Witnesses first.** They caught a model where writer exit starved every
  interesting trace. The skill's order, reachability before invariants, paid
  for itself.
