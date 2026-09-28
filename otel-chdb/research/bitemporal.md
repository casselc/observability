# research: bitemporal ideas from XTDB, applied to our design

A design note (2026-09-28), prompted by XTDB's series on its bitemporal
index. It records what we take from it, what we don't, and the order we build
in. Implementation status is kept in §7; decisions are D30–D32 in
[../DECISIONS.md](../DECISIONS.md).

Sources, read 2026-09-28 [D]:

- XTDB, "Building a bitemporal index, part 1: taxonomy"
  (https://xtdb.com/blog/building-a-bitemp-index-1-taxonomy);
- "part 2: resolution"
  (https://xtdb.com/blog/building-a-bitemp-index-2-resolution);
- "part 3: storage"
  (https://xtdb.com/blog/building-a-bitemp-index-3-storage);
- "40× query performance" (https://xtdb.com/blog/40x-query-perf).

Labels as elsewhere: **[D]** from a cited source, **[M]** measured here,
**[E]** estimate.

## 1. What XTDB does, briefly

- **Two clocks.** *Valid time* (VT): when a fact held in the world. *System
  time* (ST): when the database recorded it. System time is nearly always
  queried "as of now" (the corrected history); "as of an earlier system time"
  is the audit query [D, part 1].
- **Append-only events, resolved at query time.** Each event stores
  `valid_from`, `valid_to` and `system_from` only, never `system_to`. A query
  replays events **backwards in system time**, keeping a *ceiling*: for each
  valid-time interval, the latest system time not yet covered by a newer
  event. An older event emits rows only for the valid-time intervals no newer
  event already covers. As-of-now queries can stop early; nothing rewrites
  old rows [D, part 2].
- **Storage.** Stateless compute over object storage and a log; an LSM of
  immutable, deterministic Arrow files; deeper levels sharded by entity-id
  hash prefix (files ~100 MB, compaction tasks that need no coordination);
  per-file min/max and bloom metadata [D, part 3].
- **Recency partitioning.** A row's *recency* is the largest T at which it
  was believed valid for both VT = T and ST = T. From level 1 the LSM splits
  into a *current* partition (sharded by entity id only) and *historical*
  partitions sharded by recency and entity id, so as-of-now queries skip
  history by its maximum recency [D, part 3].
- **The 40× lesson.** A few long-lived entities with early `valid_from`
  stretched a small current file's temporal metadata across all history;
  pruning failed and as-of-now queries read 37 files instead of 2. The fix
  classifies files as *emit* (can return rows), *supersede* (can only hide
  older versions) and *constrain* (can only trim time ranges), and skips
  constrain files when the query projects no temporal column: 28–52×
  faster on the customer's queries [D, 40× post].

## 2. The mapping: we already have both clocks

| XTDB | Ours |
| --- | --- |
| valid time | **event time**: the row's `Timestamp` |
| system time | **custody time**: `received_at`, when the edge took durable custody (D19, FORMAT.md §2) |
| the log | the S3 lanes (create-only slots, FORMAT.md §1) |
| "latest completed" system time | `complete_through`, per fleet, cluster, signal and lane (D19, D29) |
| system-from only, never system-to | `received_at` is written once per row and never changes |
| corrections | none for telemetry rows; many for **entity-catalog** facts |

So telemetry is a *degenerate* bitemporal table (append-only, one event per
fact, never superseded), and the entity catalog is a *real* one. D26 already
made the two clocks explicit for completeness (`settled_through` is event
time, `complete_through` custody time); this note uses the second clock as
a query axis.

## 3. The basis: every answer at a named system time (D30)

**Idea.** A query can be run *as of* a custody time: only rows with
`received_at < C` (strict: the consumer promises only `received_at <
complete_through`, and a pending object can carry `received_at` equal to it;
FORMAT.md §3). If C is at or below the scope's `complete_through`, every
row that will ever have `received_at < C` is already in central, so the
answer at basis C **never changes** (until retention removes the rows).

**Shape.**

- A basis is a vector: `{cluster: C_cluster}` for the clusters a result
  reads (D29 publishes exactly this), encoded as an opaque token that also
  carries the signals and a version.
- Every `/v1/query` and `/v1/plan` answer returns the basis it was computed
  at. A client may send a basis back; the service then adds
  `received_at <= C_cluster` for each cluster to the per-table filter it
  already injects (`additional_table_filters`, D22), and the planner lists only
  objects whose `oscope-received` is below it. Objects the planner could not
  HEAD are kept when `LastModified + skew < C`, and otherwise marked
  `basis_check` for the reader to decide from the Parquet footer.
- A client asks for `"latest"`, sends a token back, or mints one with
  `POST /v1/basis`. `basis_from` + `basis` asks for the **delta** between two
  bases (rows with `C₁ ≤ received_at < C₂`).
- **Rules.** A basis naming a cluster outside the caller's scope is refused
  (`basis_not_in_scope`), never intersected. A basis above the scope's
  current `complete_through` is refused (`basis_ahead`: it would not be
  stable). A basis older than retention (custody age, D19) is refused as
  `basis_expired`, and a plan over a GC-truncated lane as 410, never answered
  with less data. A table without a received column cannot be served at a
  basis (`basis_unservable`). The token is `b1.<payload>.<HMAC-SHA256>` with
  rotating key ids; the MAC only proves the service minted it, and scope is
  re-checked on every use.

**What it gives.**

1. **Consistent dashboards.** All panels of one view run at one basis,
   instead of each carrying its own label (HyperDX adapter, lake UI).
2. **Reproducible alerts.** The evaluator records the basis each window was
   evaluated at; re-running the same query at that basis gives the same
   answer, so an alert decision can be audited and replayed.
3. **Late data as a precise delta.** D26 counts rows that arrive after a
   window was complete but does nothing with them. With a basis, the late
   rows of a window evaluated at C₁ are exactly those with
   `C₁ ≤ received_at < C₂`. The evaluator keeps each window's basis and
   verdict for `late_horizon`; one delta query per rule finds windows with
   late rows, and only those are re-evaluated at the new basis (a verdict
   plus a delta is not enough for conditions like averages). Per-rule
   `on_late`: `reevaluate` (default: a late episode, labelled
   `alert_late="true"`), `page` ("late data changed window W"), or `ignore`.
   Late data never resolves a firing alert, and a window's basis only moves
   forward.
4. **Caches keyed correctly.** A result or plan at a fixed basis is
   immutable, so it may be cached until retention. This is the rule CAST row
   33 (Mosaic's stale cubes) arrived at: every cache in the read path keys on
   the data version, and the basis *is* the data version.

**Limits.** Rows deleted by retention make old bases unanswerable (refused,
above). The consumer's exactly-once (fences, D9) means a row has one custody
time, so a basis never double-counts. Rows in ClickHouse replicas behind the
one serving the query are not covered by `complete_through` (AMBIGUITY C3):
unchanged by this proposal.

## 4. Late rows kept out of normal plans (D31)

**The same failure as XTDB's 40× case.** An edge object's
`oscope-min-time`/`oscope-max-time` span all its rows. One row with an old
event time (a late batch, a replay, a skewed node clock) stretches the range,
and the planner (and any min/max pruning in ClickHouse's parts) includes the
object for every window in between. D26's integration test produced exactly
this with a row 15 minutes late, and the plan already marks such objects
`late`; they are still read.

**Options.**

- (a) **Split at the edge.** Rows older than a policy bound (`max_lateness`,
  or a separate edge setting) go into a separate *late* object in the same
  lane, with its own time range. The bulk object keeps a tight range.
- (b) **Outlier metadata.** Keep one object but record the bulk range (for
  example the 0.1–99.9th percentile) plus an outlier count and range; the
  planner plans the object for a window only if the bulk or an outlier range
  overlaps it.

(a) keeps every reader simple, since each object's min/max is honest, and it
maps onto XTDB's split of recent from historical. (b) needs no change to the
object layout, but every reader of the metadata must understand it.

**Chosen: (a), built at both edges (D31).** Measured through the real Go edge
on 26 h of traces (9,360 objects; one sender 5 min behind; 1% of batches with
spans 15 min to 24 h old) [M]:

| Window | Before (planned / brute force) | After, 15 min bound |
| --- | --- | --- |
| 5 min, historical | ×1.44 objects, ×1.47 bytes | equal to brute force |
| 1 h, historical | ×1.06 | equal to brute force |

- At a 15 min bound both options reach brute force; (a) costs +0.9% objects.
  Below the fleet's clock skew both fail, so **the bound must stay above
  skew** (default 15 min, `late_split_after`, 0 disables).
- The cut is relative to the request's **newest row**, not `received_at`:
  without a persistent queue `received_at` is re-stamped per attempt, and a
  cut that moves between attempts under the same content key would lose
  rows. A changed bound between attempts gives duplicates, never loss.
- Metrics are not split yet.

**Central.** ClickHouse 26.10 prunes parts by per-part `Timestamp`
statistics, and late rows stretch them: 1.92 parts and 404 granules per
5-minute window against 1.00 and 211 without late rows [M]. Inserting late
objects separately does not help (merges rejoin them). Built since
(owner decision 2026-09-28, DECISIONS.md D34): an object-constant
`late_part` column and the partition key `(toDate(received_at),
late_part)`, with the consumer's range check reading the first element and
a migration (`otap-rs/scripts/migrate_late_part.py`). Through the real
consumer on the same shape (3 days, 5.19M spans, 1% of batches late),
fully merged: 2.75 parts and 214–230 granules per 5-minute window against
1.91 and 431 with the old key [M].

## 5. The entity catalog as bitemporal events (D32, proposed)

Catalog facts *are* corrected: relist gaps, controller restarts, late
announcements, and controller-versus-overseer disagreements. Today the
controller writes SCD2 rows (close the old version, open the new one), and
the aggregator marks versions `uncertain` after the fact
([../entities/README.md](../entities/README.md) §6.2).

**Proposal (refined by the model and the fleet replay, D32).**

- Store **events**, not closed rows: `(entity, valid_from, valid_to?,
  system_from, source, kind, attrs)`, where `system_from` is when the
  aggregator took the event in, and `kind` is `assert`, `retract` or
  `unknown`.
  - A relist gap or a controller restart is an `unknown` **for each entity
    alive at its start**, never one event over the window for everything.
  - A sync is a **retract per entity** it no longer lists, never "retract
    all". Records map in the order the controller wrote them.
  - A restart is a gap and needs the previous incarnation's end time; the
    controller now finds it and writes it (a `restart` gap record).
- **Resolve at query time** with XTDB's backwards replay and ceiling, and one
  precedence rule, "the highest-precedence latest event":
  - the controller and the overseer form one **authority tier**, ordered by
    `st + W` for the controller and `st` for the overseer: within the trust
    window W the controller wins (a live controller re-asserts every open
    version each sync); a silent controller loses to a newer overseer event
    once W has passed. W proposed: 20 min (2 × the sync interval);
  - an authority `unknown` blocks older authority events only;
  - announcements are the **evidence tier**: the latest fills in only where
    the authority said unknown (flagged uncertain) or nothing, and never
    overrides an authority assert or retract.

  Two query shapes: joins from telemetry (VT = the row's event time, ST =
  now) and audits and alert replays (ST = a basis).
- The basis (§3) needs the catalog's own per-cluster **`catalog_through`**
  (the aggregator's ingest clock), with the same strict bound:
  `system_from < C`. Compaction and retention of events must respect the
  oldest basis still answered. Joins by `resource_id` need nothing, because
  an id's attributes never change.
- Keep a maintained **current view** (recency partitioning). In the fleet
  replay it is 1.66% of all events, 84% of it tombstones for deleted
  resources (0.26% without them), so tombstones need a TTL.

**What it fixes, measured** (fleet replay: 1 cluster, 7 days, 20,261 pods,
3 controller restarts, 4 relist gaps; `entities/bitemp/results/`). On the
same inputs the resolver reproduces today's SCD2 views (518,269 vs 518,021
pod-hours). Where SCD2 loses information: a restart leaves no trace (131
pod-hours of deleted pods shown alive and certain; the resolver says unknown,
leaving 9.4 h of informer lag); `uncertain` marks whole versions (247
pod-hours flagged for 0.13 h inside a gap); no answer at an earlier system
time. The replay also found a bug in today's controller (CAST row 37).

**Built so far.** `model/bitemporalCatalog.qnt` (four invariants: the
replay equals the precedence rule; an unknown never resolves certain;
answers at system time S never change once time passes S; the current view
equals full resolution; 7 witnesses, 7 mutants caught) and the Go reference
resolver `entities/bitemp` (checked against brute force and 15,320 model
answers; point lookup 0.5–0.7 µs). The storage change waits on the
entity-schema decision, which waits on real-cluster data
([../deploy/validation/real-cluster-telemetry.md](../deploy/validation/real-cluster-telemetry.md)).

## 6. What we do not take

- **Full bitemporal SQL over telemetry.** Rows are never updated; event time
  and custody time are enough.
- **XTDB's in-memory hash tries and its Kafka log.** The S3 lanes already are
  our log, with create-only slots and custody times.
- **Storing `system_to`.** We already don't: `received_at` is written once.

Worth taking later:

- **Hash-prefix sharding of deeper index levels.** The lake index (D27) is
  per hour, so a 30-day trace lookup reads up to 720 segments. Sharding
  merged levels by trace-id prefix across hours, as XTDB does by entity id
  and Tempo by trace id, bounds that.
- **Metrics' last value.** Gauges behave like XTDB's pricing-feed persona:
  each sample is valid until the next one. A small latest-sample-per-series
  table (the current partition) answers "current value" without scanning
  history.
- **Emit / supersede / constrain file classes** for the resolved catalog.

## 7. Status

| Item | Decision | Status |
| --- | --- | --- |
| Basis token: query service, lake plan, alert evaluator's late-data delta, lake UI and adapter caches, fork patch 0003 | D30 | **built** 2026-09-28 (045929b); lake UI e2e does not exercise it yet |
| Late rows kept out of normal plans | D31 | **built** 2026-09-28 (dc90809): split at both edges, planner reports `part`; central partition key proposed; metrics not split |
| Catalog as bitemporal events: model and reference resolver | D32 (proposed) | **model and resolver built** 2026-09-28 (5a311c1); storage not started |
| Hash-prefix sharding of index levels; metrics' last-value table | — | not started |
