# entities/bitemp: the entity catalog as bitemporal events

The first step of [D32](../../DECISIONS.md) (**proposed**;
[research/bitemporal.md](../../research/bitemporal.md) §5): a resolution rule
for catalog facts kept as append-only events, a Quint model of it
([`../../model/bitemporalCatalog.qnt`](../../model/bitemporalCatalog.qnt)), a
pure Go reference resolver checked against the model and a brute force, the
mapping from today's catalog inputs to events, and a replay of the fleet
model through both today's aggregator and the resolver. **No storage
change**: the catalog tables stay as they are until the entity-schema
decision, which waits on real-cluster data.

Labels as elsewhere: **[M]** measured here, **[Q]** from the Quint model,
**[D]** from a cited source, **[E]** estimate.

## 1. Why

The catalog is corrected all the time: relist gaps, controller restarts,
late announcements, and a controller that can disagree with the central
aggregator. Today the controller writes SCD2 rows (close the old version,
open the new one), the aggregator merges them (`valid_from` = min, the
latest observation decides open or closed) and marks versions `uncertain`
after the fact (`../controller/sql/aggregator.sql`). Two things are lost:

- **what the catalog said at an earlier time.** A close rewrites
  `valid_to`; `uncertain` is set in place. An alert replayed at a D30 basis
  cannot ask "which pods did the catalog list at 10:00, as of 10:05";
- **how sure it is, and about which instant.** `uncertain` marks a whole
  version (a pod alive for 30 days whose deletion fell in a 2-minute gap is
  uncertain for all 30 days); a restart marks nothing; an announcement is
  `uncertain` whether or not anything contradicts it.

Joins from telemetry are not the problem: rows carry `resource_id`, a content
address, and the dictionaries look it up with no time at all (../README.md
§3.1). What bitemporality buys is the **lifecycle**: which entities existed
at a valid time, as known at a system time.

## 2. Events and the rule

An event is `(entity, valid_from, valid_to?, system_from, seq, source, kind,
version)`:

- `system_from` is when the aggregator took the event in (its
  `ingest_log.put_at`), `seq` orders events of one `system_from` (arrival);
- `source`: **controller** (the cluster's controller, the authority),
  **overseer** (the central multicluster aggregator, the fallback),
  **announce** (an edge's announcement, evidence only);
- `kind`: **assert** (the entity existed, as `version`), **retract** (it did
  not), **unknown** (the source does not know: a gap);
- `entity` may be `All` (every entity of the event set's scope); the lane
  mapping below does not need it (§4).

**Resolution at (entity, VT, ST)** uses only events with `system_from ≤ ST`:

1. The **authority tier** (controller, overseer) is ordered by
   `eff = system_from + W` for the controller, `system_from` for the
   overseer (ties: the controller, then `seq`). The top authority event
   covering (entity, VT) decides if it is an assert or a retract.
2. If it is **unknown**, or there is none, the latest covering
   **announcement** fills in: an assertion, flagged **uncertain** when an
   authority said unknown. Otherwise the answer is unknown (or absent).

As XTDB's backwards replay with a ceiling ([D] "Building a bitemporal index,
part 2"): events in descending (tier, eff, seq); a definite authority event
closes its still-open valid time and emits rows for it; an authority
`unknown` closes its valid time **to the authority tier only**; an
announcement emits rows for what is still open. Emitted rows are final, so
the replay streams and stops when the queried range is closed.

### Precedence: source first within a trust window W, then system time

Checked against the failure cases (each is a scripted run in the model and
a case in `TestScenarios`):

| case | events | answer | why this rule |
|---|---|---|---|
| **controller restart** | old assert P [t0, ∞); new incarnation: unknown P [last word, first sync), then its sync (P absent: retract P [sync, ∞)) | alive to the last word, **unknown** in the outage, retracted after | unknown blocks the older assert of its own tier (invariant ii); today P is "alive" and certain until the sync (§5) |
| **relist gap** | unknown for what was alive, [gap from, gap to]; the relist's closes | survivors re-asserted by the next sync: certain; the deleted ones unknown in the window only | today the whole version is `uncertain`, all its life |
| **late announcement of a pod born and dead in a gap** | nothing from the authority, or its unknown; announce P [first seen, last seen] | **assert, from the announcement** (uncertain if an authority said unknown) | evidence fills what the authority does not know, whatever the order of arrival |
| **announcement against the controller** | controller retract P [t, ∞); announce P covering t | **retracted** | an announcement never overrides a definite authority answer, however late |
| **controller wrong, overseer right** | controller assert P (re-asserted at every sync while it lives); overseer retract P | **the controller** while it keeps speaking; **the overseer** once the controller's last word on P is older than its own by more than W | the owner's decision: the controller is the authority. A live controller re-asserts every open version at each full-state sync, so it keeps its authority; a silent one loses it W after it stopped |
| **stale overseer** | controller relabel at st1; overseer's lagging view asserts the old version at st2 > st1, st2 − st1 < W | **the controller's** new version | pure recency (W = 0) takes the stale view: mutant `sourceBlind` |

Rejected alternatives: **strict precedence** (W = ∞; model instance
`strict`: the overseer can never override, so a controller that died with
open assertions keeps them forever); **pure recency** (W = 0, XTDB's rule as
is: `sourceBlind`); **unknown as a retract** (loses the announcement of a
pod born inside a gap: `unknownAsRetract`); **unknown transparent to its own
tier** (the stale assertion under it would show through, flagged: that is
today's `uncertain`, and it answers "alive" for a pod the controller no
longer vouches for, violating ii).

**W** must exceed the controller's re-assertion period plus the aggregator's
lag: 2 × the full-state sync interval (10 min) = **20 min** [E]; the replay
used 2 h with hourly syncs. It matters only where the overseer writes
events, which nothing does yet (§6).

## 3. The model: `model/bitemporalCatalog.qnt`

Two entities, valid times 0..4, up to five events of any source, kind and
interval, a clock. State: the events, the current partition (`live`), and
the answers at every past system time frozen when the clock passed it.

| invariant | statement |
|---|---|
| (i) `resolvesAsSpec` | the replay equals the declarative rule (the top authority event, else the top announcement) at every (entity, VT, ST ≤ now) |
| (ii) `unknownNeverAsserts` | where the top authority event is `unknown`, the answer is unknown or an announcement's assertion flagged uncertain: never certain |
| (iii) `monotoneHistory` | the answers at every system time S < now never change as events arrive (what makes a D30 basis sound for the catalog) |
| (iv) `currentIsResolved` | the current partition resolves (VT = now, ST = now) as all events do |

`model/bitemp_model.sh` (one quint process at a time, ~3.5 min; nightly with a
random seed) [Q]:

```
sim  design           safety                       ok       (expect ok)   97s   10000 x 12
sim  design           not(w…) for the 7 witnesses  VIOLATED (expect VIOLATED)   (all reached in 1–2 s)
sim  strict           safety                       ok
sim  strict           not(wOverseerTakesOver)      ok       (the overseer never overrides)
sim  noCeiling        resolvesAsSpec               VIOLATED   backwards replay, every covering event overwrites
sim  forward          resolvesAsSpec               VIOLATED   forward replay, overwrite: no fill under unknown
sim  sourceBlind      resolvesAsSpec               VIOLATED   precedence ignored (W = 0)
sim  unknownAsRetract unknownNeverAsserts          VIOLATED
sim  ignoreST         monotoneHistory              VIOLATED   resolves over every event: SCD2 rewritten in place
sim  pruneAny         currentIsResolved            VIOLATED   the current partition drops an announcement under an authority assert
sim  pruneBySt        currentIsResolved            VIOLATED   the current partition orders by st, not precedence
runs design           DesignTest                   passed 5 failed 0
runs <mutant>         <mutant>BreaksTest           passed 1 failed 0   (each of the 7)
```

Witnesses: an announcement fills an authority's unknown; the overseer takes
over from a silent controller; the controller holds against a newer
overseer event; a gap hides an older assertion; an announcement is
overridden; history is corrected after S while the answer at S is kept; the
current partition drops an event still valid now.

**Pseudonymise on departure (O-G9, DECISIONS D38 items 11–17).** A steward
event (`kind: "pseudonymise"`, the pseudonym as its version) is an overlay,
not a lifecycle event: the replay ignores it; every assertion of the entity
shows the first correction's pseudonym at every basis, also one before the
correction. Properties: `pseudonymHidesName`, `noNameAtOrAfter`,
`monotoneRedacted`, `pseudonymStable`, `idempotent`, `stateUnchanged`
(`strict` checks `d32Safety` only: O-G9 does not depend on W); witnesses
`wPseudonymised`, `wOldBasisRedacted`, `wLateNameHidden`, `wDuplicate`;
mutants `basisScoped` (pure bitemporal: only bases at or after the
correction), `lastWins`, `asAssert` (the correction as a controller
assertion: a later Graph delta wins the name back), each with a scripted
`*BreaksTest`. The trace export carries `rview` (the pseudonymised answers).

## 4. The resolver (Go, pure)

```go
bitemp.Resolve(events, entity, vt, st, policy) Row           // one point: one scan, no sort
bitemp.ResolveRange(events, entity, vtFrom, vtTo, st, policy, emit) int  // the replay, streaming, early stop
bitemp.Replayer{...}.Push(e) / Finish()                        // the same, fed by a store in replay order
bitemp.NewCurrent(policy, now): Add, Advance, Get, Compact     // the current view (recency partition)
bitemp.FromLanes(objects, level, opts, &seq)                   // today's lane objects -> events
bitemp.FromAnnouncements(anns, tail, &seq)                     // the consumer's announcements -> events
```

**The current partition** keeps an event while it can still decide an
answer at VT ≥ now. It leaves when its `valid_to ≤ now`, or when every VT
of it from now on is hidden by a higher event **of its own tier**: an
authority event by any higher authority event (unknown hides too), an
announcement only by a later announcement, because a later authority
`unknown` can reopen the evidence tier (the `pruneAny` mutant is exactly
that mistake).

**Tests** [M] (`go test ./...`, 0.2 s; `-rapid.checks=3000` 4 s):

- `TestResolveMatchesBrute`: `Resolve`, the replay's rows and a brute-force
  resolver (explicit sort keys, independent code) agree at every point and
  every system time; rows never overlap;
- `TestUnknownNeverAsserts` (ii), `TestMonotoneHistory` (iii),
  `TestCurrentIsResolved` (iv, random arrivals, clock moves and
  compactions), `TestEarlyStop` (an as-of-now range over a 1,000-event
  history reads 10), `TestScenarios`, `TestIvset`;
- `TestModelTraces`: the final states of 30 traces of the model's
  `designTrace` instance (`testdata/model_traces.json`): every one of the
  model's answers (15,320 points over 390 states when run on the full
  traces) equals `Resolve` and the replay, **and `Current` keeps exactly the
  model's `live` partition**. The nightly job runs it on fresh traces
  (`TRACES=dir model/bitemp_model.sh`, then `BITEMP_TRACES=dir go test -run
  Model ./...`);
- the mutants re-done by hand in Go (the current partition hiding
  announcements under authority asserts; eff without W; unknown closing
  like a definite event) each fail a property test.
- `TestFromLanes`, `TestFromAnnouncements`: the mapping (§5's cases).
- O-G9 (`pseudonym.go`: `Steward`, `Pseudonymise`, `FirstCorrection`,
  `Redact`, `ResolveNamed`, `RowsNamed`; `Current` keeps corrections
  unpruned): `TestPseudonymHidesNameAtEveryBasis`,
  `TestPseudonymiseIsIdempotent` (duplicates, re-keyed signals, late names,
  any order), `TestCurrentIsPseudonymised`, `TestDepartureScenario`;
  `TestModelTraces` replays the model's `rview` through `ResolveNamed` and
  `RowsNamed`.

## 5. Today's inputs as events, and the fleet replay

**The mapping** (`scd2.go`). Every event's `system_from` is the
aggregator's `put_at` of its object; within an object: the closes' bounded
assertions, the retracts, the gaps' unknowns, then the opens.

- open of version K of entity E: assert(E, [valid_from, ∞), K);
- close of K at t: assert(E, [valid_from, t), K), and retract(E, [t, ∞))
  unless the object also opens a version of E (a relabel supersedes);
- a sync at `syncAt`: retract(E, [syncAt, ∞)) for every E alive before it
  and absent from it (the aggregator's sync close), then its records as
  opens;
- a gap record: unknown(E, [from, to]) for every E alive before it;
- an announcement seen at s (first seen f): announce(E, [f, s + tail), K),
  with `system_from` = its ingest time;
- option `RestartGaps` (what today's catalog lacks): at a new lane, unknown
  for every E alive, from the previous lane's last object to the first
  sync; and the new incarnation's opens dated honestly (a version asserted
  before keeps its `valid_from`; one that changed while nobody watched
  counts from when it was seen).

Two first attempts were wrong, and the replay showed it: **`unknown(All)`
for a gap** made every entity ever deleted "unknown" in the window (3,172
pod-hours of dead pods for 11 minutes of relist gaps; 57,221 with the
restarts), and **`retract(All, [syncAt, ∞))` for a sync** ("nothing else
exists") hid the announcements of pods born and gone before the next sync
(a snapshot says nothing about entities born after it). Both are per
entity now; the model keeps `All` as a mechanism.

**The replay** (`cmd/fleetreplay`, `results/fleetreplay{,-fixed}.{txt,json}`)
[M]. A fleet.py slice: 1 cluster, 7 days, 20,261 pods (20,474 versions),
~27,000 resources. A simulated controller with the real controller's rules
(a first-seen pod dated from its creation, a relabel from its observation,
closes at observation, 1–3 s informer lag, hourly full-state syncs,
5-minute deltas) and faults: **three restarts** (20 min, 45 min, 2 h) and
**four relist gaps** (1–5 min); every resource announced by its edge on its
first row and once an hour after (617,960 announcements). The lane objects
go through **today's aggregator SQL in ClickHouse** (`aggregator.sql`,
`announced.sql`, the sync close, `markGaps`: 53 s) and through the mapping
and the resolver (as of now). Both are compared with the truth over valid
time, in pod-hours (resource-hours for resources; ~520,000 alive pod-hours
in all):

| truth, and what the catalog says | `pods` view (SCD2) | resolver, today's inputs | resolver, + restart gaps | `pods`, fixed controller |
|---|---:|---:|---:|---:|
| alive: exact, certain | 518,021 | 518,269 | 520,227 | 507,186 |
| alive: exact, flagged `uncertain` | 247 | – | – | 13,143 |
| alive: **two versions at once** | **2,063** | – | – | 2.4 |
| alive: wrong version | 0.04 | **2,062** | 0.07 | 0.07 |
| alive: unknown | – | 0.13 | 104 | – |
| alive: missing (born and gone in a relist gap) | 21 | 21 | 21 | 21 |
| dead: **alive, certain** | **131** | **132** | 9.4 | 9.3 |
| dead: alive, flagged `uncertain` | 0.15 | – | – | 122 |
| dead: unknown | – | 0.12 | 122 | – |

| resources (with announcements), resource-hours | `resources` view | resolver, tail 0 | resolver, + restart gaps | + restart gaps, tail = window |
|---|---:|---:|---:|---:|
| alive, missing | 0.07 | 21 | 21 | 0.07 |
| alive, from an announcement | 23 (flagged) | 2 | 63 (61 filling an unknown) | 152 |
| dead, **alive from an announcement** | **473** (flagged) | – | – | 296 |

**Where they agree:** everything the controller observed. On the same
inputs the resolver reproduces the SCD2 views (518,269 vs 518,021 exact
pod-hours; the 248-hour difference is the X1 flag, which the resolver
narrows to the gap window) with the same 21 missing and 131 falsely alive
pod-hours. **At the end** both give the same current set: 3,461 live
resources in `resources` (`valid_to` past the end) and in `Current`, equal
to the truth.

**Where they differ, and why** (SCD2 loses information):

1. **Restarts are invisible to SCD2.** A pod deleted during an outage stays
   alive, certain, until the new incarnation's first sync (131 pod-hours);
   with restart gaps the resolver says unknown for exactly the outage (122
   dead and 104 alive pod-hours unknown; 9.4 left: informer lag).
2. **A restart re-dates relabelled pods** (the bug below): SCD2 shows two
   versions at once (2,063 pod-hours); the resolver on the same records
   takes the newer claim, which is as wrong (2,062); the mapping with
   restart gaps dates honestly (0.07).
3. **`uncertain` is per version, not per instant.** 72 versions marked
   cover 247 pod-hours, of which 0.13 were in a gap; with the fixed
   controller's restart gap record, 382 versions and 13,143 pod-hours are
   flagged for ~226 pod-hours of real doubt.
4. **Announcements.** `announced.sql` keeps a resource the controller lacks
   alive until 2 h after its last sighting: 473 resource-hours of dead
   resources, flagged. The resolver asserts only what an edge saw (tail 0:
   21 hours of short-lived pods missed) or up to the edges' re-announce
   window (296 false, 0.07 missed), and fills only what the authority does
   not know (61 resource-hours in the outages).
5. **As-of answers**: SCD2 has none; the resolver answers at any system
   time (invariant iii; the model and the property tests, not this replay).

**The current view** [M]: 21,703 events of 1.31 M (1.66%) after pruning:
3,461 live resources and 18,242 tombstones (retracted resources; a TTL at
the lateness bound would leave 0.26%). 92% of the events are hourly sync
re-assertions (1,111,334 of 1,208,585 records). Built incrementally in
0.3 s; its answers equal `Resolve` over every event for every entity
(checked in the run) and the truth.

**Cost** [M] (this box, loaded; ~60 events per entity): `Resolve` (a point
in history, one scan) **0.5–0.7 µs**; `ResolveRange` over the whole 7 days
**5–6 µs**; `Current.Get` **0.26–0.5 µs**. Over a flat event list without an
entity index a lookup is O(all events): the storage must be keyed by entity
(§6).

**Found in the mapping** (this package, fixed before the numbers above): a
close in an object that also opened the entity was taken for a relabel, so
a short pod opened and closed in one object stayed alive until the next
sync (2,074 false pod-hours with 5-minute deltas); records are now mapped
in the order the controller wrote them. Also `Unknown(All)` and
`Retract(All)`, above.

**Found in the controller (a bug, fixed): a restart re-dates relabelled
pods.** The controller dates a pod it has not seen from the pod's creation
(`vf := created`); a new incarnation has seen none, and the aggregator
merges `valid_from` as the minimum. So each restart moved a relabelled
pod's current version back to the pod's creation, over its earlier
versions: 2,063 pod-hours of `pods` rows with two versions valid at once in
this replay, through the real SQL. Fixed in `../controller/internal/ctrl`
(`Config.Since`: a pod first seen and created before the previous
incarnation's last object is dated from then) and
`../controller/internal/lane` (`PreviousEnd`), with the restart written as a
gap record (`restart`); `entityctl --restart-gap` (default on). With the fix
the same replay has 2.4 pod-hours of overlap (relabels inside the outage,
flagged). Tests: `restart_test.go` (fails without the fix),
`previous_test.go`.

## 6. Storage for later, and what D30 needs

Nothing here is built; these are the options once the entity schema is
decided.

**Tables** (ClickHouse, central) [E]:

- `catalog_events`: `MergeTree ORDER BY (cluster_key, level, entity,
  system_from, seq)`, append-only, the aggregator the only writer (it maps
  lane objects and announcements as `FromLanes` / `FromAnnouncements` do).
  One event per record: the replay's week of one cluster was 1.31 M
  resource events, 92% of them hourly sync re-assertions (six times that
  with the 10-minute sync). A re-assertion only refreshes the controller's
  trust window, so it can be **compacted** once W has passed and no overseer
  event for that entity falls in [previous word, re-assertion + W]: then it
  decides nothing at any system time. Compaction below the oldest D30 basis
  still answerable is not allowed (it would change as-of answers).
- `catalog_current`: the current partition, `ReplacingMergeTree(version)
  ORDER BY (cluster_key, level, entity)`, maintained by the aggregator as
  events arrive (`Current`): ~0.3% of the events in the replay, one row per
  entity that is live or recently retracted. Tombstones (retracted entities)
  are 84% of it in a week and grow with churn; they can leave once no
  announcement can still arrive for them (the lateness bound, D26), which
  changes their answer from "retracted" to "absent" only.
- `catalog_history`: per entity, the resolved valid-time rows **as of
  now** (`Rows(…, ST = now)`), rewritten per entity when an event for it
  arrives. Resolving one entity costs microseconds (§5), so this is the
  view the SCD2 tables are today (`pods`, `resources`), but derived, with
  `unknown` intervals instead of whole-version flags. Emit / supersede /
  constrain file classes (XTDB's 40× post) apply if history grows past a
  few files per scan.
- **As-of queries** (ST < now: audits, alert replays) resolve from
  `catalog_events` with `system_from ≤ ST`, per entity: a query-service
  endpoint, not SQL; ClickHouse can do a point lookup as `argMax` over
  (tier, eff, seq) of the covering events, but not the ceiling replay.
- **Recency partitions** as XTDB: `catalog_current` is the current one;
  history partitioned by month of recency (the last time a row was valid
  at VT = ST).
- **The dictionaries do not change.** They map `resource_id` to attributes,
  which never change for an id; they would be built from `catalog_current`
  plus a recent-history window (the hot/cold split of ../README.md §7.6)
  instead of `versions_final`.

**What D30's basis needs from the catalog.**

1. Its own completeness point: the aggregator's ingest is a separate clock
   from the edges' custody time, so a basis (D30, built) carries a catalog
   component, `catalog_through` per cluster (every lane object and
   announcement with `system_from` below it has been taken in), published
   like `complete_through` (D29), and checked like it (`basis_ahead`,
   `basis_expired`).
2. As-of answers read `system_from < catalog_through(basis)`, strictly
   below as D30's `received_at < C` (the model's ST = S counts events at S
   only once its clock has passed S); invariant (iii) is what makes them
   stable, and the SCD2 catalog cannot give them (the `ignoreST` mutant is
   SCD2 rewritten in place).
3. Retention and compaction of `catalog_events` respect the oldest basis the
   query service still answers (`basis_expired` otherwise).
4. Joins by `resource_id` need nothing: attributes of an id are immutable,
   so a result at an old basis joins the current dictionaries exactly, as
   long as the id is still in them.

## 7. Reproduce

```sh
go test ./...                                     # resolver, mapping, model traces (committed)
../../model/bitemp_model.sh                        # the model (quint 0.32, ~3.5 min)
TRACES=/tmp/t ../../model/bitemp_model.sh && BITEMP_TRACES=/tmp/t go test -run Model ./...
# the fleet replay: a slice of fleet.py (1 cluster, 7 days: 2.6 MB), then
CH_CLIENT=clickhouse python3 ../scripts/fleet.py --db btc_fleet --clusters 1 --days 7 --create
clickhouse client -q "SELECT p.pod_key, p.pod_uid, toUnixTimestamp64Milli(p.valid_from), toUnixTimestamp64Milli(p.valid_to),
  toUnixTimestamp64Milli(p.pod_start), toUnixTimestamp64Milli(p.pod_end), arrayStringConcat(arrayMap(c -> c.1, w.containers), ',')
  FROM btc_fleet.pods p LEFT JOIN btc_fleet.workloads w ON p.wl_key = w.wl_key ORDER BY p.valid_from FORMAT TSV" > pods.tsv
go run ./cmd/fleetreplay -pods pods.tsv -flush 5m -ch http://127.0.0.1:18123 -json results/fleetreplay.json
```

`fleetreplay` creates (and drops first) the databases `btc_agg` and
`btc_ann`; drop them and `btc_fleet` afterwards.
