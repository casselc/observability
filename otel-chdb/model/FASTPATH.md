# A fast path from the exporter to central ClickHouse

**Verdict: drop it for now.** Keep the current plan: the S3 commit, plus a
background importer. Build the importer the way this note specifies, so the
fast path can be added later without changing correctness.

**Is it safe?** Yes, if it is built as in section 1. The Quint model finds a
design that satisfies every invariant with the fast path on and with it off
(`recommended`, `recommendedNoFastPath`, `recommendedNoWait`), in a hostile
environment:

- ambiguous exporter timeouts, dropped requests and partitioned edges;
- central crashes, which lose the async buffer and can stop a flush between
  partitions;
- importer crashes, and ledger MV failures;
- batches written but never committed;
- a dedup window that other inserts evict.

With no faults, the importer issues zero inserts (`happyPath`). Safety rests
on these assumptions:

- **A1. Commit first.** The exporter sends only after its batch is committed
  in S3, and it never gates the collector's ack.
- **A2. One shot.** The exporter sends once, within `D` of the commit, and
  never retries.
- **A3. A timing bound.** Every fast-path request reaches central, and is
  flushed or lost, within `D + R + B` of the commit. `R` is the longest a
  request can live on any hop, and `B` bounds the async flush. The importer
  looks at a batch only after a grace `G > D + R + B` (strictly greater).
- **A4. A full check.** The importer checks the target itself: it compares
  the batch's row count with the committed count. It inserts only what is
  missing, never on "some rows present", and never on a ledger or marker
  alone.
- **A5. Read your writes.** The importer's check sees every committed part:
  the same assumption as `edgePublish`'s `CHECK_CENTRAL`. On a replicated
  central this needs `select_sequential_consistency = 1`.

The dedup token is not needed in this design. It is a backstop for
violations of A3, and it works only if both paths form the same single block
(section 4).

**Is it worth it? Not now.** The benefit is latency. A batch is visible about
0.3–1 s after commit [M, local], against the importer's poll period plus
about 0.1–0.3 s. The fast path doesn't reduce central work: it moves the
same insert from the importer to request time. The importer then pays a
cheap check instead (about 0.5 ms per batch when checked 100 at a time
[M]). Against that:

- **A new trust boundary.** Every edge needs a network path to central
  ClickHouse, and a per-edge credential and user. IRSA and Pod Identity
  don't cover ClickHouse auth. You'd need client certificates or passwords
  per edge, plus row validation, because an edge can write any
  `producer_id`.
- **More ClickHouse behaviour to depend on,** and to pin across upgrades:
  - async-insert semantics;
  - flush timing;
  - the `D + R + B` bound, which every proxy hop must respect;
  - per-partition atomicity.
- **Twice the upstream bytes** on the edge's link when sending from memory:
  about 180–310 KB more per 10k-span batch.
- **New failure modes to operate:**
  - silent buffer loss with `wait=0`;
  - `408` timeouts that still land;
  - a grace period that delays the importer on the slow path.
- **The "central pulls the committed key" variant isn't asynchronous.**
  `async_insert` doesn't apply to `INSERT … SELECT` [M]. That variant is
  just "run the importer's insert now". A notification to the importer,
  from the edge or from S3 event notifications, does the same with no
  ClickHouse credentials at the edge.

A 1–5 s importer poll, or an event-driven importer, gets visibility to within
a few seconds for about $100–500/month of S3 GETs at 500 producers [E]. It
adds no edge-to-central path.

**When to revisit.** A sub-second visibility SLO for edges that already have
an authenticated, low-latency path to central. Section 1 is then the design
to build, and the importer needs no change.

Labels: **[M]** measured here, **[D]** docs or source, **[E]** estimate,
**[Q]** Quint model.

---

## 1. The design, if it is built

**Exporter (fast path).** A best-effort side effect of a successful commit:

1. **Fire only after the commit.**
   - `edgePublish`: after the manifest PUT returns 200.
   - `s3Native`: after the log-slot append resolves as ours.
   - `s3Inline`: after the create-only data PUT resolves as ours.
   - Never after an ambiguous commit that hasn't resolved: a batch written
     but never committed must not reach central (`fireBeforeCommit`
     violates `onlyCommittedIngested` [Q]).
2. **Never gate the ack.** The collector's queue is acked on the S3 commit.
   The fast path runs in a bounded goroutine pool afterwards, and is dropped
   if the pool is full, central is unreachable, or more than `D` (for
   example 2 s) has passed since the commit PUT returned. That is the
   exporter's own monotonic clock, so edge clock skew doesn't matter.
3. **Send once.** `POST /?query=INSERT INTO otel_traces (<cols>) FORMAT
   Parquet` with the committed object's exact bytes as the body, and:
   - `async_insert = 1`;
   - `wait_for_async_insert = 1` with a short `wait_for_async_insert_timeout`;
   - `insert_deduplication_token = <token>`.

   Never retry: `fastPathRetries` violates `batchIngestedAtMostOnce` [Q].
   A retry is a second copy that only the dedup window can stop, and after
   a partial flush not even that. On any error or timeout, leave the batch
   to the importer.
4. **The token** is the importer's, derived from the commit identity:
   `{producer}/{epoch}/{batch or slot}/{content hash}`. Both paths must
   produce one block per partition with that token (section 4).

**Why from memory, not "central pulls the key".**

- `async_insert` is ignored for `INSERT … SELECT FROM s3()`. It ran
  synchronously in 0.22 s with rows visible on return and no queued entry
  [M]. The pull variant is therefore a synchronous importer insert,
  triggered from the edge.
- Pulling would also make central run `s3()` on an edge's request. Only a
  named collection pinned to the edge's prefix would keep that from being an
  SSRF into central's credentials [D].
- If what you want is "the importer, sooner", notify the importer instead.

**Why `wait_for_async_insert = 1`.** Correctness doesn't depend on it:
`recommendedNoWait` passes [Q], because the importer never trusts the
exporter. With `wait=1` the exporter learns the real outcome:

- flushed;
- a parse error (a truncated body got its own `400` while the other entry in
  the flush landed [M]);
- a timeout that may still land (`408` after `wait_for_async_insert_timeout`,
  and the rows appeared later [M]).

That feeds a fast-path success metric, and it gives backpressure: an
exporter that is waiting doesn't pile on more. With `wait=0` the answer
comes in about 5 ms, but only means "queued": an entry lost in a central
crash was already acknowledged [D]. Either way the cost at the edge is one
held connection for about the busy timeout (0.25 s at 200 ms, 1.06 s at
1 s [M]).

**Importer.** Identical with the fast path on or off, apart from the grace:

1. Walk the commit order (manifests, log slots or inline objects) from a
   durable checkpoint, under the consumer lease of `S3NATIVE.md`.
2. **Grace.** With the fast path on, don't look at a batch until commit time
   (S3 `LastModified`) + `G`. `G > D + R + B`, plus a margin for clock skew
   between S3 and central. `graceNoMargin` (`G = D + R + B`) and `noGrace`
   both violate `batchIngestedAtMostOnce` [Q]. The fast-path entry can still
   be buffered, or in flight, when the importer looks.
3. **Check** with an aggregating projection on the target. The projection
   maps (`producer_id`, `producer_epoch`, `batch_id`) to `count()`, and
   is batched per poll:
   `SELECT producer_id, producer_epoch, batch_id, count() … WHERE (…) IN (<the poll's batches>) GROUP BY ALL`.
   - count = committed rows: skip.
   - 0: insert with the token.
   - Anything between: a partition landed and another didn't. Repair only
     the missing rows (`row_ordinal NOT IN (…)`) under a derived token.

   `checkAnyRow` skips partial batches and violates `noLostBehindCheckpoint`.
   `fullNoRepair` re-inserts landed partitions once their token is evicted
   [Q].
4. **Insert** with `INSERT … SELECT FROM s3('<exact key>')`, the token, and
   the single-block settings. Then advance the checkpoint with a CAS.

**The target.**

- Plain `MergeTree` with the projection.
- `non_replicated_deduplication_window` large enough to cover
  `D + R + B` of insert traffic, as a backstop only.
- Ideally a partition key that is constant per batch, such as
  `toDate(received_at)`, so every insert is atomic: `onePartition` passes
  even without repair [Q]. The stock `toDate(Timestamp)` lets one batch span
  days.

**Choosing `R` and `B`.**

- **`R`** is the longest time between the exporter starting a request and
  the server enqueuing it, across every hop:
  - the exporter's total HTTP timeout;
  - any L7 proxy that buffers request bodies and retries upstream. nginx
    buffers by default, so a request can reach ClickHouse after the client
    gave up [D];
  - ClickHouse's own `http_receive_timeout`.

  Use a direct TCP load balancer (an NLB) or disable request buffering.
  Then `R` ≈ the client timeout (for example 10 s).
- **`B`** is `async_insert_busy_timeout_max_ms` (200 ms by default), plus
  `max_delay_to_insert` when parts pile up (1 s by default), plus the flush
  itself. ClickHouse doesn't guarantee a bound under overload [D]. Take
  `B` = 5–10 s.
- So `G` is about 30–60 s. The grace only delays the importer's handling of
  batches whose fast path failed. On the happy path they are already
  visible.

## 2. Design choices

| Choice | Options | Decision | Why |
| --- | --- | --- | --- |
| When it fires | after the data write / after the commit | after the commit | `fireBeforeCommit` ingests a batch that is never committed (`onlyCommittedIngested` ✗) [Q] |
| Collector ack | gated on central / tied to S3 | tied to the S3 commit | central may be unreachable for days; S3 is the durability point |
| Source of rows | from memory (async insert) / central pulls the key | from memory, exact object bytes | async insert doesn't apply to `INSERT … SELECT` [M]; the pull variant is a remote-triggered importer (and would expose `s3()` to edges) |
| Identity | per-attempt / (producer, epoch, batch or slot, content hash) | the commit identity, shared with the importer | dedup across the two paths holds only with one token and one block [M] |
| Retries | retry on error / one shot | one shot | a retry is a second copy guarded only by the window (`fastPathRetries` ✗) [Q] |
| `wait_for_async_insert` | 0 / 1 | 1 (either is correct) | real outcome, error visibility, backpressure; `wait=0` is correct but blind [Q, M] |
| Importer check | (a) target / (b) MV ledger / (c) S3 marker / (d) claims / (e) tokens only | (a), full count, via an aggregating projection | (b) lags when the MV fails [M], and has no row for a partly committed batch (`ledgerCheck`, `ledgerCheckMvReliable` ✗); (c) is unsound with `wait=0`, and with `wait=1` leaves F4 open (`markerNoWait` ✗, `markerWait` ✗); (d) says who may insert, not whether anyone did (`claimsNoCheck` ✗); (e) is F4 again (`tokenOnly` ✗). (b) as a filter in front of (a) is fine (`ledgerThenTarget` ✓) but buys little: the projection is as cheap and exact |
| Partial batches | "any row" = present / full count + repair | full count + repair, or one partition per batch | `checkAnyRow` loses rows; `fullNoRepair` duplicates [Q] |
| Visibility lag | wait out a bound / rely on tokens | wait out `G > D + R + B` | the check misses buffered rows [M]; tokens cover the race only while in the window (`noGrace`, `graceNoMargin` ✗) [Q] |
| Importer insert settings | defaults / single block | single block | default Parquet reader chunking makes a different block, and a full duplicate [M] |

## 3. The model

[`fastPath.qnt`](fastPath.qnt) (instances at the end) and
[`fastPath_test.qnt`](fastPath_test.qnt) (one scenario per counterexample,
plus the recommended design's recoveries).

**The abstraction.** S3 publication is just "written, then committed, or
abandoned". The importer walks the commit order. That covers the commit
point of every variant:

- the manifest in `edgePublish`;
- the log slot in `s3Native`;
- the create-only object in `s3Inline`.

**The actors:**

- one exporter per batch: send, drop, a late arrival, a wait timeout, give
  up, and optionally write a marker;
- the network, bounded by `REQ_BOUND` unless `REQ_BOUNDED = false`;
- central's async buffer: entries flush within `BUSY`, or a crash loses them
  and can commit some of a flush's partitions but not others;
- the target, per (batch, partition), behind a FIFO dedup window of
  `DEDUP_WINDOW` tokens that `otherInsert` evicts;
- a ledger fed by an MV that fires even on deduplicated blocks and may fail
  after the target is written (both measured);
- optional per-batch claims;
- one importer, with a durable checkpoint and crashes between check, insert
  and advance.

**Time.** Time is an integer tick, and a tick is only enabled while nothing
bounded would become overdue. The timing assumptions become enabling
conditions this way.

**Invariants:**

- `onlyCommittedIngested`.
- `batchIngestedAtMostOnce`: each (batch, partition) is in the target at
  most once, after dedup.
- `noLostBehindCheckpoint`: the checkpoint never passes a batch that is not
  wholly in the target. This one invariant is both "the importer never skips
  a batch whose fast-path insert was lost" and completeness at quiescence.
  At the end of the commit order, every committed batch is in.
- `importerNeverInserts`: the efficiency metric (`impInserts == 0`), meant
  to hold on fault-free instances only.

**Domains:** 2 batches × 2 partitions, `D = REQ_BOUND = BUSY = 1`,
`GRACE = 4` (3 in `graceNoMargin`), a window of 2 with unrelated traffic
(8 and none in the two backstop instances), and a clock up to 7.

### Results

`quint run --backend typescript`, 1,500 random traces of up to 40 steps per
cell, seed `0x5eed`; `fastpath/run_model.sh` reproduces the sweep. ✓ means no
violation was found (that isn't a proof); ✗ means a counterexample was found.
Every ✗ also has a deterministic scenario in `fastPath_test.qnt`. All 23
scenarios pass (`quint test fastPath_test.qnt --main <module>`).

| Instance | What it is | `onlyCommittedIngested` | `batchIngestedAtMostOnce` | `noLostBehindCheckpoint` | `importerNeverInserts` |
| --- | --- | --- | --- | --- | --- |
| `recommended` | **recommended**: after commit, one shot, wait=1, grace, full check + repair | ✓ | ✓ | ✓ | ✗ (faults) |
| `recommendedNoFastPath` | **recommended, fast path off** | ✓ | ✓ | ✓ | ✗ (faults) |
| `recommendedNoWait` | recommended with wait=0 | ✓ | ✓ | ✓ | ✗ (faults) |
| `happyPath` | recommended, no faults | ✓ | ✓ | ✓ | **✓** |
| `onePartition` | recommended with one partition per batch, no repair | ✓ | ✓ | ✓ | ✗ (faults) |
| `ledgerThenTarget` | ledger first, target on a miss (b → a) | ✓ | ✓ | ✓ | ✗ (faults) |
| `claimsWithCheck` | claims + full check, no separate grace (d + a) | ✓ | ✓ | ✓ | ✗ (faults) |
| `lateArrivalTokenBackstop` | unbounded requests, window never evicted, same blocks | ✓ | ✓ | ✓ | ✗ (faults) |
| `fastPathRetriesWindowHolds` | exporter retries, window never evicted | ✓ | ✓ | ✓ | ✗ (faults) |
| `happyPathNoGrace` | no faults, no grace | ✓ | ✗ | ✓ | **✗** |
| `noGrace` | no grace | ✓ | ✗ | ✓ | ✗ (faults) |
| `graceNoMargin` | grace = D + R + B exactly | ✓ | ✗¹ | ✓ | ✗ (faults) |
| `unboundedRequests` | requests can land arbitrarily late | ✓ | ✗ | ✓ | ✗ (faults) |
| `blockMismatch` | unbounded requests, importer forms different blocks | ✓ | ✗ | ✓ | ✗ (faults) |
| `fireBeforeCommit` | fast path fires before the commit | ✗ | ✓ | ✓ | ✗ (faults) |
| `fastPathRetries` | exporter retries the fast path | ✓ | ✗ | ✓ | ✗ (faults) |
| `fastPathRetriesOnePartition` | exporter retries, one partition per batch | ✓ | ✗ | ✓ | ✗ (faults) |
| `checkAnyRow` | check = any row present (a, weak) | ✓ | ✓ | ✗ | ✗ (faults) |
| `fullNoRepair` | full check, partial batch re-inserted whole | ✓ | ✗ | ✓ | ✗ (faults) |
| `tokenOnly` | tokens only (e) | ✓ | ✗ | ✓ | ✗ (faults) |
| `tokenOnlyNoFastPath` | tokens only, fast path off (F4) | ✓ | ✗ | ✓ | ✗ (faults) |
| `ledgerCheck` | ledger only (b) | ✓ | ✗ | ✓ | ✗ (faults) |
| `ledgerCheckMvReliable` | ledger only, MV never fails | ✓ | ✗ | ✓ | ✗ (faults) |
| `markerWait` | S3 ack marker, wait=1 (c) | ✓ | ✗ | ✓ | ✗ (faults) |
| `markerNoWait` | S3 ack marker, wait=0 (c) | ✓ | ✗ | ✗ | ✗ (faults) |
| `claimsNoCheck` | claims, no check (d) | ✓ | ✗ | ✓ | ✗ (faults) |

¹ Not found by the 1,500-trace sweep (seed `0x5eed`), because the
interleaving is narrow: the check and the last flush have to fall in the
same tick. Found by 10,000 traces with seed `0x1`, and shown by
`graceNoMarginTest.checkRacesLastFlushTest`.

`importerNeverInserts` is a metric: it should fail wherever faults exist,
and hold only on the fault-free `happyPath`. There it holds, and the
witness "every batch ingested with zero importer inserts" is reached.

**Witnesses** (1,500 traces each, `fastpath/results/model/witnesses.tsv`):

- **`recommended`** reaches:
  - every batch ingested;
  - fast-path landings;
  - importer inserts and skips;
  - partial flushes;
  - lost buffers;
  - late arrivals;
  - importer crashes.
- **It never reaches a dedup hit:** the recommended design never needs the
  token.
- **`happyPath`** reaches "all ingested, zero importer inserts" and never
  reaches an importer insert.

**Apalache:** `quint verify --main recommended --invariant safety`: no
violation within **10 steps** (881 s) or 8 steps (150 s). A 12-step run
crashed in the tool while another agent's Apalache was running. Ten steps is
a shallow bound, because the grace alone takes 4 ticks. It reaches a
committed batch that the importer checks and advances past, but not the
deeper crash-and-repair stories. Those are covered by simulation and by
the scenarios.

### The counterexamples in plain terms

- **`fireBeforeCommit`: `onlyCommittedIngested` ✗.** The exporter sends
  after the data PUT but before the commit record. The fast path lands, then
  the edge crashes and the batch is never committed. Central holds rows no
  commit names, and they will be re-published under a new identity after
  the restart.
- **`noGrace`, `graceNoMargin`: `batchIngestedAtMostOnce` ✗.** The fast-path
  entry is still in the async buffer, or still on the wire, when the
  importer checks. The check sees nothing, and the importer inserts. Other
  inserts evict the token, then the buffer flushes, and the batch is in
  twice.
  - With `G = D + R + B` exactly, the check and the last possible flush
    fall in the same tick, so the margin must be strict.
  - `happyPathNoGrace` shows the cost even with no faults: the importer
    inserts, so `importerNeverInserts` ✗. With traffic evicting the token,
    that is a duplicate too.
- **`unboundedRequests`: `batchIngestedAtMostOnce` ✗.** A request that
  outlives every bound lands after the importer inserted. Only the token can
  catch it:
  - `lateArrivalTokenBackstop` ✓: the window holds and the blocks match;
  - `blockMismatch` ✗: the importer formed a different block. This is the
    same event as the measured 64k-row duplicate in section 4.
- **`checkAnyRow`: `noLostBehindCheckpoint` ✗.** Central crashes mid-flush
  after committing one of the batch's two partitions. The importer sees
  rows, skips the batch, and the other partition is lost for good.
- **`fullNoRepair`: `batchIngestedAtMostOnce` ✗.** The same partial batch,
  re-inserted whole. The partition that landed is deduplicated only while
  its token is in the window. After eviction it's a duplicate.
  `onePartition` (a batch-constant partition key) removes the case.
- **`fastPathRetries`, `fastPathRetriesOnePartition`:
  `batchIngestedAtMostOnce` ✗.**
  - The first attempt times out ambiguously, or commits one partition
    before central dies.
  - The exporter retries after the token was evicted.
  - `fastPathRetriesWindowHolds` ✓ shows that it's purely a window
    dependence.
- **`tokenOnly`, `tokenOnlyNoFastPath`: `batchIngestedAtMostOnce` ✗.** With
  no check, the importer re-inserts every batch, and only the window stops
  the copy. Even on the fast path's happy path the importer does the whole
  read and parse: 63 ms against 86 ms for a real insert [M]. Without the
  fast path this is F4: an importer crash between insert and checkpoint.
- **`ledgerCheck`: `batchIngestedAtMostOnce` ✗.** The fast path's rows
  commit, and the MV that feeds the ledger fails. This is measured: the
  target keeps the 8,000 rows, the ledger gets none, and the client gets a
  500. The importer trusts the ledger and re-inserts.
  - `ledgerCheckMvReliable` ✗ too: the MV sees whole blocks, so a batch
    that is partly committed (a crash between partitions) has no ledger
    row. A ledger-only importer re-inserts it whole.
  - `ledgerThenTarget` ✓: a ledger miss falls through to the target check.
- **`markerNoWait`: `noLostBehindCheckpoint` ✗.** With `wait=0`, "ok" means
  queued. The exporter writes the marker, and central crashes before the
  flush. The importer trusts the marker and the batch is never ingested.
- **`markerWait`: `batchIngestedAtMostOnce` ✗.** Markers after a confirmed
  flush are sound, but only the fast path writes them. For a batch the
  importer inserts itself, a crash before the checkpoint is F4 again. (Also
  [D]: parts aren't fsynced by default, so an OS crash can lose a part after
  its marker.)
- **`claimsNoCheck`: `batchIngestedAtMostOnce` ✗.** The exporter's claim
  lapses, and the importer takes it over and inserts. The claim says who
  may insert, not whether anyone did. `claimsWithCheck` ✓ is just the grace
  and the check, plus a PUT per batch.

## 4. ClickHouse facts

All on the shared ClickHouse **26.10.1.618** server, in a private database
`fastpath_test` (since dropped). The batches were 8,000-span Parquet objects
under `s3://otel/fastpath/` (since deleted), re-stamped from the bench
generator's data. The target was the central-typed `otel_traces` from
`bench/central` (`PARTITION BY toDate(Timestamp)`,
`non_replicated_deduplication_window = 1000`), with merges stopped so that
parts show block formation. Scripts and raw JSONL are in
[`../fastpath/`](../fastpath/).

### Async-insert dedup on non-replicated MergeTree

| Case | Rows (8,000 = deduplicated) | |
| --- | --- | --- |
| async, same token twice, `wait=1`, Parquet body | 8,000 | [M] |
| same, Native body | 8,000 | [M] |
| same, `wait=0` (Parquet and Native), with `SYSTEM FLUSH ASYNC INSERT QUEUE` | 8,000 | [M] |
| `deduplicate_insert = backward_compatible_choice`, `async_insert_deduplicate = 0` (the pre-26 behaviour) | **16,000** | [M] |
| same with `async_insert_deduplicate = 1` | 8,000 | [M] |
| window 0 (the MergeTree default) | 16,000 | [M] |
| async twice, **no** token | 8,000 (content hash) | [M] |

- **Does async insert honour the token? Yes,** with both `wait=1` and
  `wait=0`, as long as the table's `non_replicated_deduplication_window` is
  above 0.
- **Is `async_insert_deduplicate` needed? Not in 26.10.** Its new
  `deduplicate_insert` defaults to `enable` and covers sync and async
  inserts. On older servers, or with `backward_compatible_choice`,
  `async_insert_deduplicate = 1` is required [M, D: setting descriptions].
  **Pin this.** An upgrade path through a version with the old default
  silently loses async dedup.

### The same batch via async insert and via `INSERT … SELECT FROM s3()`

| Case | Rows | |
| --- | --- | --- |
| async (`wait=1`), then importer, same token, single-block settings | 8,000 | [M] |
| importer, then async | 8,000 | [M] |
| async Native body, then importer | 8,000 | [M] |
| async `wait=0`, still buffered (the check sees 0 rows); importer commits; then the flush | 8,000: the buffered entry was dropped at flush | [M] |
| two-partition batch (4,000 + 4,000), both orders | 8,000 | [M] |

**Block formation decides it.** With a token, each block's dedup id is the
token plus the block's position in the insert. A block squashed from several
reader chunks also gets a different id:

| Importer insert after one async entry of the same batch | Rows |
| --- | --- |
| importer split into eight 1,000-row blocks (`max_insert_block_size = 1000`, strict limits) | **15,000**: block 0 deduplicated, blocks 1–7 inserted |
| importer with only `input_format_parquet_max_block_size = 1000`: chunks squashed back into one 8,000-row block | **16,000**: a different id |
| importer with only `input_format_parquet_prefer_block_bytes = 1 MiB` | **16,000** |
| **64,000-row batch (0.7 MB Parquet, about 32 MB decoded [E]), importer defaults** | **128,000**: the reader chunks at `input_format_parquet_prefer_block_bytes`, about 16 MB, by default |
| same batch, single-block settings | 64,000 |

- **Async side: parser chunking doesn't matter.** An async entry parsed in
  1,000-row chunks still dedups as one entry against a single-block importer
  insert [M].
- **Importer side: it matters.** The importer must add the Parquet reader's
  own limits to the single-block settings of `model/README.md`:
  `max_threads = 1, max_insert_threads = 1, max_block_size`,
  `max_insert_block_size ≥ rows, min_insert_block_size_rows = 0`,
  `min_insert_block_size_bytes = 0`, **and**
  `input_format_parquet_max_block_size ≥ rows`,
  `input_format_parquet_prefer_block_bytes` above the decoded batch size.
  `fp.py`'s `ONE_BLOCK` is the tested set.
- REPORT.md's "different block size gives partial duplicates" is the same
  mechanism.

### Flushes, atomicity and per-entry dedup

- **One flush combines exporters' entries.** Three concurrent exporters in
  one busy window produced **one** 24,000-row part [M].
- **Per-entry dedup still works inside a combined flush.** A duplicate of
  batch 2 and a new batch 4 flushed together inserted exactly batch 4:
  8,000 rows per batch, 32,000 in total [M].
- **An entry is all or nothing.**
  - A truncated Parquet body got its own `400`, and the good entry in the
    same window landed [M].
  - A JSONEachRow entry with one bad row (row 50 of 100) inserted 0 rows,
    not 50 [M].
- **Partitions are not atomic.**
  - A two-partition batch becomes two parts (4,000 + 4,000) [M].
  - ClickHouse documents an INSERT as atomic only within one partition
    (and up to `max_insert_block_size` rows); across partitions each part
    commits separately [D]. The `insert_deduplication_token` description
    confirms the token is tracked per partition [D].
  - A crash between parts can leave a partial batch. That can't be
    measured without killing the shared server, so the model's
    `partialFlush` is the [D] behaviour.

### What the client sees

- **Wait timeout [M].** `wait=1` with `wait_for_async_insert_timeout` (1 s)
  shorter than the busy timeout (5 s) returns **408 `TIMEOUT_EXCEEDED`**
  after 1.01 s, with 0 rows visible. The entry flushed anyway, and 8,000
  rows appeared 5 s later. A retry with the same token was deduplicated.
  So a timeout is ambiguous.
- **Central restart or crash [D]** (not tested: the server is shared):
  - Unflushed entries are in memory. A graceful shutdown flushes them
    (`async_insert_queue_flush_on_shutdown = 1` on this server). A crash
    or OOM kill loses them.
  - `wait=0` clients were already told 200, so they lose data silently.
  - `wait=1` clients see a connection reset or 5xx, which is ambiguous:
    the entry may have flushed just before.
  - Parts are not fsynced by default (`fsync_after_insert = 0`), so a
    host crash can also lose a recently committed part [D].
- **Visibility lag [M].** With `wait=0` the ack comes in 5–9 ms. Rows
  became visible after 0.27 s (busy timeout 200 ms) and 1.08 s (1,000 ms).
  `wait=1` acks came in 0.25 s and 1.06 s. A check in between sees nothing.

### The ledger MV [M]

- **It fires on every attempt,** including deduplicated ones: three
  attempts gave one copy in the target and three ledger rows. The ledger is
  a set, not a count.
- **Two-partition batches:** the MV sees the whole block, so it records one
  row with `rows = 8000`.
- **MV failure after the target write:** `throwIf` in the MV left **8,000
  rows in the target and none in the ledger**, and the client got a 500
  (both paths). The ledger can lag the target.
- **Target failure:** a CHECK constraint on the target left neither the
  target nor the ledger written. No case here showed the ledger ahead of
  the target.

### Cost of the check at central

The target: 10M spans, which is 1,250 batches × 8,000 across 50 producers
and 8 daily partitions. That's 122 MiB in 34 parts, with the query
condition cache and the query cache off.

| Check | ms (median / max) | rows read | bytes read |
| --- | --- | --- | --- |
| raw envelope columns, no index | 15.2 / 36 | 880k | 2.4 MB |
| + event-time range from the commit record | 15.5 / 53 | 733k | 2.4 MB |
| `bloom_filter` skip index on `batch_id` (14 KiB) | 12.6 / 23 | 74k | 0.53 MB |
| **aggregating projection** (producer, epoch, batch) → `count()` (74 KiB) | **12.0 / 21** | **220** | **6.9 KB** |
| ledger table (MV), `FINAL` | 7.2 / 16 | 1,250 | 15 KB |
| **projection, 100 batches in one query** | **52** (0.5 per batch) | 2,470 | 80 KB |
| ledger, 100 batches | 17 (0.17 per batch) | 1,250 | 25 KB |
| raw columns, 100 batches | 544 | 9.9M | 99 MB |
| *for scale:* importer insert of one 8k batch | 86 (69–121) | | |
| *for scale:* token-only re-insert that dedup drops (option e) | 63 (58–94) | | |

- **The raw scan grows with the table.** It read only 880k rows here because
  this target's batch ids are clustered by insert order. At billions of rows
  a per-batch scan is unacceptable [E].
- **The projection's cost is flat.** It is stored in each part, so it is
  exactly as atomic as the data (no MV lag), and merges maintain it. At
  74 KiB per 10M rows it's negligible.
- **Caveats [D]:** projections make lightweight `DELETE`/`UPDATE` throw
  unless `lightweight_mutation_projection_mode` is set; existing parts need
  `MATERIALIZE PROJECTION`; and projections are rejected on
  `ReplacingMergeTree` by default. The stock ClickStack traces table is a
  plain `MergeTree`.
- **About 12 ms is per-query overhead,** so batch the check per poll.

## 5. Costs and savings

Scenario: 500 producers, one 10k-span batch per producer every 10 s. That's
50 batches/s and 500k spans/s, the same as REPORT.md's estimate.

| | Importer only (recommended plan) | With the fast path |
| --- | --- | --- |
| Central insert CPU | about 5 µs/row: about 2.5 cores [R] | the same: the same insert, triggered by the edge [E] |
| Importer work per batch, happy path | read the commit, GET + HEAD the object, insert (86 ms wall per 8k batch [M]) | read the commit, check (0.5 ms/batch at 100 per query [M]) |
| Importer insert concurrency | about 5 inserts in flight (50/s × 86 ms) [E] | about 0 [Q: `happyPath`] |
| S3 requests at central | + 2 per batch (GET + HEAD): 100/s, about $100/month [E] | saved on the happy path |
| Parts created | 1 per batch per partition (50/s) unless the importer groups batches | async flushes combine: 3 exporters made 1 part [M]; at 50/s and a 200 ms window, about 10 batches per part [E]. The importer can get the same by issuing its own inserts with `async_insert = 1` |
| Edge upstream | the S3 PUT (180–310 KB per 10k batch [R: parquetgo]) | **2×**: plus the same bytes to central |
| Edge→central connections | none | 50 inserts/s fleet-wide; with `wait=1` each held about 0.25–1 s, so about 15–50 concurrent [E]. Per-edge auth and TLS, row validation |
| Latency to query visibility | poll period + insert: P/2 average, P + 0.3 s worst. P = 1 s: about 0.6–1.3 s; P = 5 s: about 2.6–5.3 s [E] | about 0.3–1.1 s after commit, set by the busy timeout [M, local] |
| Latency when the fast path fails | the same | G + P (G ≈ 30–60 s) [E] |
| Polling cost of a short P | the S3-native log: one GET per producer per poll: 500/s at P = 1 s, about $520/month; at P = 5 s, about $100/month [E, at $0.0004 per 1,000 GET] | the importer still polls, but can use a long P |

**Reading the table.**

- **Central compute:** the fast path saves nothing (the insert is the
  same), apart from the importer's S3 GETs, about $100/month at this scale.
- **Latency:** it saves roughly the importer's poll period, and a short
  poll or S3 event notifications recover most of that for a few hundred
  dollars a month.
- **What it costs:**
  - doubled edge upstream;
  - an authenticated edge-to-central path;
  - a grace period that slows recovery from fast-path failures;
  - ClickHouse behaviours to pin: async dedup defaults, flush bounds, and
    block formation.

  That's the basis of the verdict.

## 6. What to adopt regardless

These fall out of this work and apply to the importer-only plan:

1. **Check against the target with an aggregating projection** on
   (`producer_id`, `producer_epoch`, `batch_id`) → `count()`. Compare with
   the committed row count, and repair only missing rows. This closes F4
   without depending on the dedup window, and it costs about 0.5 ms per
   batch when batched. A ledger MV is optional; never trust it alone.
2. **Use single-block settings on every importer insert, including the
   Parquet reader's limits.** Otherwise the token doesn't even protect
   importer-vs-importer retries once a batch passes about 16 MB decoded
   [M: 128,000 rows for a 64,000-row batch].
3. **Prefer a batch-constant partition key**, `toDate(received_at)`, so a
   batch is one part and every insert is atomic. Otherwise keep the repair
   path.
4. **Pin `deduplicate_insert = enable`** (the 26.10 default) and a nonzero
   `non_replicated_deduplication_window` sized to the retry horizon. It's
   count-based: at 50 blocks/s, 1,000 blocks covers 20 s.
5. **For latency:** a short importer poll, or S3 event notifications.
   - AWS S3 → SQS or EventBridge, with IRSA or Pod Identity on EKS.
   - Nutanix Objects documents event notifications (to Kafka, NATS or
     syslog endpoints); verify on your version [D, not tested].
   - If an edge notifies central at all, have it notify the importer with
     the committed key, not ClickHouse.

## 7. Limits of this work

- **The model is small and bounded:** 2 batches × 2 partitions, integer
  time, and simulation (✓ isn't a proof). Apalache covers the recommended
  design only to 10 steps.
- **Not tested:**
  - a central crash mid-flush [D];
  - replicated or SharedMergeTree central (different dedup storage;
    `select_sequential_consistency` for the check) [D];
  - ClickHouse versions other than 26.10.1;
  - real network hops and proxies;
  - AWS and Nutanix endpoints.
- **Latencies are local:** one box, no network.
- **The shared box was loaded** (load average 12–14 during the model
  sweep), so wall times are medians of small samples.

## Files

- `model/fastPath.qnt`: the spec and its instances.
- `model/fastPath_test.qnt`: 23 scenarios.
- `fastpath/`: the ClickHouse experiments:
  - `setup.py`: batches and target;
  - `exp_dedup.py`: dedup, flushes, timeouts, visibility;
  - `exp_blocks.py`, `exp_bigbatch.py`: block formation;
  - `exp_ledger.py`: the MV ledger;
  - `exp_check.py`: check cost on 10M rows;
  - `exp_pull.py`: async insert and `INSERT … SELECT`;
  - `cleanup.py`: drops the database and deletes the objects;
  - `run_model.sh`: the Quint sweep;
  - `results/`: raw JSONL and `results/model/summary.tsv`.
