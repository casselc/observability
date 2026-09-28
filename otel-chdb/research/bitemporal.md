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
`received_at ≤ C`. If C is at or below the scope's `complete_through`, every
row that will ever have `received_at ≤ C` is already in central, so the
answer at basis C **never changes** (until retention removes the rows).

**Shape.**

- A basis is a vector: `{cluster: C_cluster}` for the clusters a result
  reads (D29 publishes exactly this), encoded as an opaque token that also
  carries the signals and a version.
- Every `/v1/query` and `/v1/plan` answer returns the basis it was computed
  at. A client may send a basis back; the service then adds
  `received_at <= C_cluster` for each cluster to the per-table filter it
  already injects (`additional_table_filters`, D22), and the planner lists only
  objects whose `oscope-received` is at or below it.
- **Rules.** A requested basis above the scope's current `complete_through`
  is refused (it would not be stable). A basis older than retention (custody
  age, D19) or than GC's truncation is refused as `basis_expired`, never
  answered with less data.

**What it gives.**

1. **Consistent dashboards.** All panels of one view run at one basis,
   instead of each carrying its own label (HyperDX adapter, lake UI).
2. **Reproducible alerts.** The evaluator records the basis each window was
   evaluated at; re-running the same query at that basis gives the same
   answer, so an alert decision can be audited and replayed.
3. **Late data as a precise delta.** D26 counts rows that arrive after a
   window was complete but does nothing with them. With a basis, the late
   rows of a window evaluated at C₁ are exactly those with
   `C₁ < received_at ≤ C₂`: the evaluator can evaluate that delta (cheap,
   incremental) and decide by policy whether it changes the verdict, instead
   of re-running the window.
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
object layout, but every reader of the metadata must understand it. The
implementation evaluates both and measures planned bytes on skewed data.

## 5. The entity catalog as bitemporal events (D32, proposed)

Catalog facts *are* corrected: relist gaps, controller restarts, late
announcements, and controller-versus-overseer disagreements. Today the
controller writes SCD2 rows (close the old version, open the new one), and
the aggregator marks versions `uncertain` after the fact
([../entities/README.md](../entities/README.md) §6.2).

**Proposal.**

- Store **events**, not closed rows: `(entity, valid_from, valid_to?,
  system_from, source, kind, attrs)`, where `system_from` is when the
  aggregator took the event in (its `ingest_log.put_at`), and `kind` includes
  `assert`, `retract` and `unknown` (a gap record is an `unknown` event over
  its window).
- **Resolve at query time** with XTDB's backwards replay and ceiling, plus
  **one precedence rule** between sources (the cluster controller is the
  authority, the central overseer the fallback, announcements weakest; the
  owner decided the authority split earlier). Two query shapes:
  - joins from telemetry: VT = the row's event time, ST = now (the
    corrected history);
  - audits and alert replays: ST = the basis of §3 ("what did the catalog
    say then").
- Keep a materialised *current* view (recency partitioning: about 60k live
  pods against 4.7M pod versions over 90 days in the fleet model, about 1%),
  so as-of-now lookups never read history.

**What it fixes.** Pods born and dead inside a relist gap can be asserted
later without rewriting anything; a controller restart is just another
source of events; the `uncertain` marking becomes a derived property (an
`unknown` event not yet superseded) instead of a separate pass.

**First step.** A Quint model of the resolution rule and precedence (safety:
the resolved state at (VT, ST) equals the one the latest covering event
says; an `unknown` event never resolves as an assertion; adding an event
with a newer ST never changes the answer at an older ST), and a Go reference
resolver checked against it. The storage change waits on the entity-schema
decision, which waits on real-cluster data
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
| Basis token: query service, lake plan, alert evaluator's late-data delta, lake UI and adapter caches | D30 | started 2026-09-28 |
| Late rows kept out of normal plans (edge split or outlier metadata) | D31 | started 2026-09-28 |
| Catalog as bitemporal events: model and reference resolver | D32 (proposed) | started 2026-09-28 |
| Hash-prefix sharding of index levels; metrics' last-value table | — | not started |
