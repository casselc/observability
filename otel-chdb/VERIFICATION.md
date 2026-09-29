# Verification plan, derived from the STPA and the CAST record

This plan starts from the hazards and the CAST record in [STPA.md](STPA.md),
not from the tools. For each hazard, unsafe control action (UCA), loss
scenario, STPA-Sec and STPA-Teaming item, requirement, and CAST row it
answers four questions:

1. Which failure class does it belong to?
2. Which verification techniques can see that class, and so are
   **required**? Which are **useful**? Which are **inappropriate**, meaning
   they look like evidence but cannot see the failure?
3. Which component implements the control, and is each required cell met
   (✅), **partial**, or missing (❌)? The evidence is test names and the
   mutants they catch.
4. Where are the gaps, which come first, what does each cost, and where
   should it run?

Audit of `claude/brave-pascal-0fecgh` at `e9011bf` (2026-09-29). It
reads code, tests, models and workflows only. **Nothing here was run for this
plan.** "✅" means the test exists and a workflow runs it. It does not mean
the test was re-run for this audit.

**Why this file and not `otap-rs/VERIFY.md`.** VERIFY.md is the Kani record
for one crate: its harnesses, bounds and proof times. This plan covers every
Go module and the Rust crate, the models, CI and process controls. It also
covers the LLM extension, which has no code yet. Putting it in VERIFY.md
would bury a Kani document under a cross-cutting one, or split the
traceability chain across two files. VERIFY.md stays the detail for the `K`
cells below and is linked, not repeated. Likewise, [otap-rs/DST.md](otap-rs/DST.md),
[otap-rs/HEGEL.md](otap-rs/HEGEL.md), [PBT.md](PBT.md), [model/README.md](model/README.md)
and [AMBIGUITY.md](AMBIGUITY.md) hold the details that the cells cite.

**Go tooling.** [research/go-verification.md](research/go-verification.md)
chose the Go tools (synctest, porcupine via `casreg`, goleak, rapid with
hand-rolled swarm, native fuzzing); §8 there lists what was built on
2026-09-29 and the cells below cite it. A Go bounded-proof tool is still
open (no Kani counterpart).

## 1. Techniques

Short codes used in every table. The two columns that matter are what each
technique can see and what it is blind to. Each blind spot is taken from a
CAST row where that blindness let a bug through.

| Code | Technique | In use here | Sees | Blind to (CAST evidence) |
|---|---|---|---|---|
| **M** | Quint model: random simulation, witnesses, mutants; Apalache for bounded exhaustive runs | 16 models (+ `basicSpells`), `model/*.qnt` | protocol-level safety over every interleaving of atomic actions in small instances | the duration of a step (#13, #14); topologies its instances omit (#21); code (a model is not the program) |
| **MN** | Model with **non-atomic** steps: request / effect / answer as separate actions, slow observations, step cost ([model/TEMPLATE.md](model/TEMPLATE.md), `ambiguousCall.qnt`) | `s3InlineConsumer` (since 2573cfc), `alertEvaluator`, `retirement` | ambiguous outcomes: a lost answer, a late effect, our own 412 (#20 found on the first run) | real latencies and real library retries (#15's retry inside object_store) |
| **MS** | Scripted model runs (`quint test`, `*BreaksTest`, scripted traces) | `retirement_model.sh`, `bitemp_model.sh`, `consumer_model.sh`, `*_test.qnt` | long, specific multi-actor sequences that random simulation does not reach (#47: 20,000 × 60 random samples passed with the mutant on) | anything not scripted |
| **ML** | Liveness or bounded-progress property: a temporal property in the model, or "after heal, progress within N" in a simulation | DST liveness rule; Hegel `heal`; `alerts` sim; `entityCatalog` (no row stays unattributable for ever) | livelock, unbounded retry, starvation (#14, #39) | anything a safety-only check accepts: a stalled system is safe |
| **MBT** | Model connected to the implementation: quint-connect replay (Rust), quintgo validate/replay (Go), model traces that drive the code | `mbt_s3inline*`, `parquetgo/modelcheck`, `chdbexporter` `PBT_QUINT`, `entities/bitemp` `TestModelTraces` | divergence between model and code at every step (both directions) | whatever the model abstracts (PBT.md: the restart bug conforms, because the model abstracts table identity) |
| **P** | Property test (Hegel `hegeltest`, rapid, hegel-go, fast-check) | Rust `hegel_props`; Go `query`, `alerts`, `entities/bitemp`, `chdbexporter`; `lakeui` | functions over a generated input space, with shrinking | whatever the generator never produces (#17–19: realistic generators avoid 0, −0.0, f16) |
| **PH** | Property test with **hostile** generators: every byte, invalid UTF-8, NUL, huge values, quotes, u64 edges, deep nesting | `prop_statements_keep_every_value_one_literal`, `prop_parquet_round_trip`, `prop_statistics_stay_small_with_huge_values`, `TestPBTEncodersMatchPdata`, `TestHostileStringValues` | injection, encoding, size blow-ups (#7, #9, #17–19, #24) | code paths reached only by coverage feedback |
| **P2C** | Generators where **event time and custody time diverge**, with clock skew, plus off-by-one **boundary mutants** (`≤`/`<`) | `TestCompleteMeansAllRowsWithinLateness`, `TestBasisSameAnswerWhileDataArrives` (with the `≤` mutant), `TestLateSplitProperty`, `complete_through_mutants_break_soundness` | two-clock confusions (#26, #34), wrong boundaries | fixtures where both clocks agree (#26: every test had event ≈ receive time) |
| **SM** | Stateful machine with **swarm** testing (each fault a rule, turned on or off per case), deterministic and shrinking | `hegel_dst` (consumer fleet), `TestPBTPublisherStateMachine` (chdbexporter) | fault combinations and orders, shrunk to a story (#13 in 2 rules, #15 in 3) | real concurrency (a case is one seeded simulation) |
| **CC** | Concurrent stateful test: real threads or processes racing on a real or emulated store; Go `-race` | `hegel_race` (nightly), `s3cas` `TestCreateRace` / `TestCASRace` / `TestZombieRace`, `lakeidx` `TestConcurrentIndexers`, `go test -race` | atomicity of conditional writes, lost updates, memory races | protocol races that need a specific timing (no shrink, no replay) |
| **DST** | Deterministic simulation: paused tokio (level 1), turmoil with real clients over emulators (level 2), seeded faults including **lost and delayed answers**, pauses, kills, skew | `dst_consumer`, `dst_net`, `dst/retire.rs`; `alerts/internal/runner` `TestSimTwoReplicas` / `TestSimLateData` (rapid-drawn schedules) | real code with time: slow rounds, late answers, backlogs (#13, #14, #23, #42) | components it does not host (the Go edge, the query service, replicas, the horizon audit) |
| **FI** | Fault injection at the answer boundary against **real** services: a proxy that applies and then errors, holds answers, drops connections; Keeper faults | `faultproxy2` (`faults.sh`, `ambig_s3.rs`), `central-replicated` soaks, `deploy/validation/central/keeper_faults.sh` | the real store's and client library's behaviour under ambiguity (#2, #5, #15) | rare interleavings (not deterministic) |
| **K** | Kani bounded model checking of the source | `otap-rs/verify` (VERIFY.md) | arithmetic over all inputs: overflow, window lemmas, range cover | I/O, ordering, anything past the bounds |
| **FZ** | Coverage-guided fuzzing (`cargo fuzz`, Go native `testing.F`) | Go: `sqlscope`, `hdxadapter`, `basis`, `rwproxy`, the aggregator, the ingress (`ci/fuzz.sh`; seeds per push, 60 s per target in the nightly `fuzz`); Rust: none | parser and decoder paths that property generators do not target | semantic properties (it needs an oracle or a crash) |
| **D** | Differential / conformance: two implementations or an emulator and the real thing on the same input | `conformance/` (Go = Rust rows, D1), `dst_net` fidelity checks, `hdxadapter` replay against ClickHouse, `parquetgo/compare`, OTAP vs OTLP property | divergence neither side's own tests assert (#40, #45) | cases outside the shared corpus; a comparison that skips rejected input (#17–19) |
| **MU** | Mutation analysis: planted mutants (`coord::Mutation`, model mutants, `--features mutants`) or a tool (cargo-mutants, a Go mutator) | planted only; **no tool** | whether the tests can fail at all | mutants nobody planted |
| **IT** | Integration / end-to-end on real services | `query-integration`, `alerts-integration`, `conformance`, `lakeui-e2e`, `close_e2e`, `hdxadapter-integration`, `rust-integration` | composition and real wire behaviour (#29, #31) | rare timings; the target platform when the test store is more permissive (#30) |
| **G** | A CI gate or process mechanism, not a test: strict `testgate`, heavy-job lock, worktrees, the verification gate, lints (`clippy -D`, `iam-lint`), `verdict.sh`, schema-derived checks | see §6 | process and shared-resource failures (#12, #27, #38, #41, #43) | product behaviour |

## 2. Failure classes from the CAST record, and what each requires

The owner's guidance, applied to all 49 rows. Every row is placed in at
least one class (§6). Classes A–H are the owner's. I–K are classes the
record shows but the guidance does not name.

| Class | CAST rows | Required | Useful | Inappropriate, and why |
|---|---|---|---|---|
| **A. Ambiguous outcomes / lost answers** | 1–6, 13–16, 20, 23, 42 | **FI** at the answer boundary, **DST** with lost and delayed answers, **MN** | SM with one rule per fault (swarm); MBT; K for settle arithmetic | example tests that pin one outcome; soaks against a store that answers in milliseconds (#13, #14: "slow is a different failure from failed"); models with atomic CAS alone (#15, #20) |
| **B. CAS / lease / replica races** | 4, 6, 13, 15, 20, 47 | **CC** + **SM** (swarm) + **M/MN** | K on the window arithmetic; DST with skew | `-race` alone (it sees memory races in one process, not protocol races across processes); single-threaded tests |
| **C. Long, specific multi-actor traces** | 21, 47 (also 14) | **MS** scripted model traces; model instances with the **deployed parallelism** (#21: `lanes: N`) | SM, whose shrinking produces the story | counting random samples as evidence (#47) |
| **D. Liveness gaps** | 14, 39 (also 25, 35) | **ML** liveness / bounded-progress properties; a bound on every retry loop on an unknown outcome | a long-outage-then-recovery soak (DST CAST factor 3) | safety-only invariants: a stalled lane is "safe" |
| **E. Hostile input; names from less-trusted writers** | 7, 9, 17–19, 24, 31, 32 | **PH** hostile generators + **FZ** | D against the real parser (hdxadapter decoder vs ClickHouse); IT with hostile keys (`TestHostileKeysAreData`) | realistic corpora; comparisons that skip rejected input (#17–19) |
| **F. Two clocks, and boundaries** | 1, 26, 34 (also 37, 49, SEC-8) | **P2C**: generators where event, custody and producer time diverge, and **off-by-one boundary mutants** | K on boundary arithmetic; DST with skew | fixtures with event ≈ receive time (#26); boundaries taken from prose (#34) |
| **G. Implementation divergence** | 40, 45, 46 (also 10, 16, 29, 30, 33) | **D** differential / conformance, with the compared columns **derived from the schema** (#45) | MBT of both implementations against one model (s3Inline has both) | each implementation's own tests; a check that restates a schema by hand |
| **H. Process and shared-resource failures** | 12, 22, 27, 28, 38, 41, 43 (also 8, 11, and 45/46's process half) | **G**: CI gates and mechanisms (lock, worktrees, strict gates, lints, verdicts) | an audit of skipped tests; provenance in every result | writing a test: these failures are about which tests run, where, on what |
| **I. Policy composition** (scope, grants, limits) | 11, 30, 32, 35, 36 | **P** over the composed policy (every group, every column); **IT on the target platform** (#30: the test store was more permissive) | G lint (`iam-lint.sh`) | a store more permissive than the target; testing each knob alone |
| **J. Combined parameters** | 5, 25, 35 | **P** that the validator accepts exactly the safe combinations; **K** for the derived arithmetic; a runtime check of the real bound (#5: Keeper session) | IT at the documented minimum (#25 was found at 60 s) | range checks per knob |
| **K. A new state that old readers misread** | 33, 44, 47, 48 | **M** with the lifecycle states (quarantine, dead-but-in-flight) and a re-check of every reader of the old state; **IT** total-equals-SQL for caches (#33) | MS for the new sequences | tests of the new feature alone |

## 3. Hazards × techniques

One table per hazard. **Req** is the required technique, from the classes
the hazard's causes fall into. Each row names the component that implements
the control and the evidence. CI placement: *PR* = `ci.yml`; *N* = a
`nightly.yml` job; *local* = a manual script.

### H-1 Acknowledged telemetry in no store (L-2, L-1)

Controls: edge ack after custody (Rust `exporter.rs` + Quiver, patch 0006;
Go `s3pqexporter` + `file_storage`); commit lanes (Rust `proto::Lane` /
`runner.rs`; Go `parquetgo/commit`); GC (`consumer/gc.rs`); retention (R-S6);
retirement / quarantine / admit (`consumer/retire.rs`).

| Req | Component | Status | Evidence / gap |
|---|---|---|---|
| MN + M | commit + consumer + GC | ✅ | `s3Inline.qnt`, `s3InlineConsumer.qnt` (`gcReopens`, `observeAtRequest`, `own412IsTakeover`), `retirement.qnt` (6 mutants incl. `closeUnsealed`); N `model` |
| M | retention (UCA-15, LS-10) | partial | `retention.qnt` (`retentionShort`, `cutDuringOutage`, `capSized`) is in `open_models.sh`, which **no workflow runs** |
| MBT | Rust lane, consumer, GC | ✅ | `mbt_s3inline` (mutants `retry_new_key`, `no_halt`, `no_check_central`), `mbt_s3inline_consumer` (`gc::doomed` = model's slots); N `rust-mbt` |
| MBT | Go lane | ✅ | `parquetgo/modelcheck` `TestLaneConformsToS3Inline`, `TestLaneMutantsCaught`; N `model` |
| DST | consumer + GC | ✅ | `neverSkipsCommitted` at every checkpoint; `early_compaction_would_skip_a_late_batch` (`EarlyCompact`); `admit_recovers_quarantined_objects_once_and_gc_keeps_them` (#48); PR + N `dst` |
| DST | Go edge | ✅ | `parquetgo/dst` `TestDSTEdgeCommit`: the real edge and aws-sdk-go-v2 against `parquetgo/internal/s3emu` in fake time (synctest); swarm fault menus incl. lost and late answers, hung PUTs, tombstones; exactly once, closed logs stay closed, calls bounded with no deadline (#39 as liveness), linearizable history; 4 mutants caught (`TestDSTCatchesMutants`); PR 24 seeds + N 20,000 new seeds (`edge-dst`); 3,000 seeds clean locally. Found the lane mutex held across S3 requests (queued callers ignored their deadlines) |
| FI | edge commit on real S3 | partial | `faults.sh` (N `faults-soak`: lost answers, crash + restart); `ambig_s3.rs` (apply-then-error, #15's shape) is **in no workflow** |
| FI (crash consistency) | durable buffer ack (UCA-1) | ❌ | only process SIGKILL (`faults.sh` scenario 4) and upstream unit tests (N `upstream-patches`); no fsync-loss or power-loss injection on the buffer volume |
| ML | edge resend bound (#39) | ✅ | `a_store_that_fails_every_put_is_not_resent_forever`, `TestAppendResendLimit`; PR |
| ML | retirement liveness | ✅ | `retirement_seeds`, `retirement_catches_mutants` (`dst_consumer`); PR |
| G | retention refused below custody age (R-S6) | ❌ | not built: the measure exists (`consumer_lane_watermark_lag_seconds`), the refusal at config time does not |

### H-2 A result presented complete while missing or duplicating data (L-3, L-1)

Controls: consumer lease, fence, settle and verify (`consumer/coord.rs`,
`worker.rs`, `sql.rs`); the count check (`NO_PARTIAL_RESULTS`); the lane
watermark and `complete_through` (`consumer/watermark.rs`); the query
service's label (`query/internal/completeness`, `basis`); both edges' row
identity (D1); OTAP conversion (patch 0005).

| Req | Component | Status | Evidence / gap |
|---|---|---|---|
| MN + M | consumer | ✅ | `s3InlineConsumer.qnt` + `Compact`; mutants `errorSettles`, `releaseInFlight`, `keeperOverrun`, `noHorizon`, `restamp`, `observeAtRequest`, `renewOnlyAtInsert`, `own412IsTakeover`; N `model` |
| MBT | consumer | ✅ | `mbt_s3inline_consumer` (designSlow found #20); N `rust-mbt` |
| DST | consumer | ✅ | level 1 + 2; `fleet_catches_mutants` (5), `net_catches_mutants` (3); regressions for #13, #14, #42; PR (40 + 8 seeds) + N (10,000 + 200) |
| SM | consumer | ✅ | `hegel_dst`; `hegel_dst_finds_mutants` (8 planted incl. #13–15); PR 100 cases, N 3,000 |
| CC | S3 conditional writes | ✅ | `hegel_race` (mutant `racy_conditional`), N; `s3cas` races, PR |
| K | lease window, check range | ✅ | 18 harnesses + 4 mutant harnesses (VERIFY.md); N `kani` |
| FI | consumer on real ClickHouse | partial | `faults.sh`, `consumer_soak.sh` (N `faults-soak`); `refused_credentials_are_unsettled_and_redacted` (real CH + S3, PR). Replicated central (#2, #5, #6: Keeper, replica lag) is in **no job** (`clickhouse-replicated` is opt-in; `keeper_faults.sh` is manual) |
| DST | replicas, horizon audit | ❌ | DST.md "not simulated": replicas, `SYNC REPLICA`, the audit's queries |
| M | completeness (LS-6..8, #21) | partial | `completeness.qnt` in `open_models.sh`: **no workflow runs it** |
| MBT | watermark / label | partial | not trace-connected. Mirrored by `complete_through_is_sound_and_advances` + `complete_through_mutants_break_soundness` (`WmIgnoresPending`, `OwnReceived`, `NoBirth`) in Rust, and rapid properties in `query/internal/completeness` |
| P2C | label, basis, late split | ✅ | `TestCompleteMeansAllRowsWithinLateness`, `TestLateRowNotComplete` (#26), `TestBasisSameAnswerWhileDataArrives` (`≤` mutant, #34), `TestPlanAtBasisProperty`, `TestLateSplitProperty`, `prop_check_range_holds_every_copy`; PR |
| PH | encoders, OTAP | ✅ | `prop_otap_rows_match_otlp_and_do_not_depend_on_the_process`, `prop_parquet_round_trip`, `regression_otap_*`; `TestPBTEncodersMatchPdata`; PR |
| D | Go = Rust edges | ✅ | `conformance/run.sh` (N), `compare_test.py` derives the columns from the DDL (#45); `TestLateSplitTraces` (#40) |
| MU (tool) | coord, plan, watermark, completeness | ❌ | planted mutants only; nobody measures which boundary mutants the suites kill |

### H-3 Telemetry attributed to the wrong entity, or none (L-3, L-1)

Controls: entity controller (`entities/controller/internal/ctrl`, lane
writer); aggregator (`cmd/aggregator`, `clusterFilter`); announcements
(edge + consumer `announce_first`); bitemporal resolver (`entities/bitemp`);
rewrite proxy (`entities/rwproxy`).

| Req | Component | Status | Evidence / gap |
|---|---|---|---|
| M + MS | bitemporal catalog | ✅ | `bitemporalCatalog.qnt`, 7 mutants, scripted runs; N `model` |
| MBT | resolver | ✅ | `entities/bitemp` `TestModelTraces` on fresh traces; N `model` |
| M | announcement lane, grace window (LS-5) | partial | `entityCatalog.qnt` in `open_models.sh`: **no workflow runs it**; not connected |
| DST | announcements (#23, #42) | ✅ | `dst_consumer` (`sameLane`), `a_server_fenced_announcement_is_not_taken_for_landed`; PR |
| P2C | controller restart dating (#37) | partial | `restart_test.go` (example); `TestMonotoneHistory`, `TestResolveMatchesBrute` (rapid) cover the resolver, not the controller's dating under restarts and skew |
| PH + FZ | aggregator keys and bodies (#24, SEC-1) | ✅ | `TestHostileKeysAreData` (IT), `TestClusterFilterRejectsForeignRecords`; `FuzzClusterFilter` (kept lines are whole input lines of the bound cluster key), `FuzzGapTime`; PR seeds + N `fuzz` |
| DST / FI | controller → lane PUT (X2), relists (X1) | partial | `TestPutTimeoutRetries`, `TestRelistsAndUnknownDeletionsAreCounted`; `TestTwoWritersOneLaneLinearizable` (lost answers, 500 after applying, requests landing 30 s late, fake time; each batch once, history linearizable); `TestAwaitRelistInFakeTime`, `TestResyncTickerInFakeTime` (synctest, production timings). Informer gaps together with lost PUT answers are still not simulated |

### H-4 An alert condition holds but no page reaches on-call, or a false page (L-1)

Controls: alert evaluator (`alerts/internal/engine`, `runner`, `store`,
`notify`); the query service's completeness (the gate R-S3 relies on);
Alertmanager (silences, grouping).

| Req | Component | Status | Evidence / gap |
|---|---|---|---|
| MN + M | evaluator | ✅ | `alertEvaluator.qnt` (`evalPastCt`, `ackOnNoAnswer`, `blindWrite`, `lateDouble`, `lateResolves`); N `model` |
| MBT | evaluator | ❌ | the model is **not connected**: `late.go` cites it, and the Go simulation restates its properties by hand |
| DST | two replicas, lost / late answers | ✅ | `TestSimTwoReplicas`, `TestSimLateData` (rapid schedules: lost writes, late copies, stalls, a tick between another's read and write); PR |
| ML | "cannot evaluate" pages and resolves | ✅ | `TestPropCannotEvaluate`, sim's "every cannot-evaluate page was resolved"; N `alerts-integration` (stalled lane pages) |
| P2C | windows vs `complete_through` + lateness | ✅ | `TestPropOnlyCompleteWindowsDecide`, `TestPropInterpretGate`; the query side under H-2 |
| M | retirement's false quarantine page (#44, #47) | ✅ | `retirement.qnt` + scripted `closeUnsealed` |
| — | silences with owner and expiry (R-S4, UCA-14, TM-4) | ❌ | not built (Alertmanager's). Nothing to verify yet |
| — | grouping, one page per incident (R-S10, TM-7) | ❌ | not built |
| I | evaluator load vs per-caller limit (#35) | ❌ | documented; a 429 is still a failed evaluation; no test that the configured limit covers the evaluator's peak |

### H-5 People act on a stale or wrong view without knowing it (L-1, L-3)

Controls: query service labels (source, `complete_through`, `settled_through`,
freshness); lake UI banner; HyperDX fork patches 0002–0004; Mosaic spike
cache; catalog lag (R-S5).

| Req | Component | Status | Evidence / gap |
|---|---|---|---|
| P2C | label | ✅ | as H-2 |
| P | lake UI completeness math, re-plan | ✅ | `lakeui` node:test + fast-check; PR |
| IT | lake UI, Mosaic | ✅ | N `lakeui-e2e`, `lakeui-mosaic-e2e` (total = SQL after each brush: #33) |
| G + IT | HyperDX banner (patches) | ✅ | the verification gate (#41): jest 39 suites; the adapter integration test (N `hdxadapter-integration`) |
| D | adapter vs ClickHouse | ✅ | 799 captured statements replayed and compared (N) |
| M | "lake result labelled with central's watermark" | partial | a `completeness.qnt` mutant, in no workflow |
| — | rows without an entity match (R-S5 second half) | ❌ | not built (query/README) |
| — | pins with owner and expiry (TM-4) | ❌ | not built |

### H-6 An unauthorized party can read or write telemetry, the catalog or control objects (L-4, L-5)

Controls: IAM / ABAC (`deploy/iam`, D18); `sqlscope` (parse, rebuild,
scope, metadata projection); `hdxadapter` binding; `rwproxy`; basis tokens
(KMS); secret redaction (`sql.rs` `redact`); aggregator `clusterFilter`.

| Req | Component | Status | Evidence / gap |
|---|---|---|---|
| PH | SQL rewrite and scope | ✅ | `TestRewriteProperties`, `TestScopeValueProperty`, `TestMetadataProjectedProperty` (#32), `TestDictionaryGuardProperty`, `TestBindProperty`, `TestHostileStringValues`; PR |
| FZ | `sqlscope` parser, `hdxadapter` decoder, `rwproxy`, basis token decode | ✅ | `FuzzPrepare`, `FuzzRewriteProperties` (rapid.MakeFuzz), `FuzzDecodeString`, `FuzzBind`, `FuzzDecode` (one canonical spelling), `FuzzRewrite`, `FuzzParseMultipart`; PR seeds + N 60 s each. The basis keyring machine found tokens with four accepted spellings (fixed) |
| D | adapter decoder vs ClickHouse | ✅ | the live differential caught two decoder differences before commit (STPA after #29) |
| P | basis tokens | ✅ | `TestTamperProperty`, `TestKMSCrossReplicaAndTamper`; N `query-kms-emulator` |
| P (policy composition) | grant vs limits groups (#36) | ❌ | no test that a limits-only group never changes scope |
| G | IAM policy shape (#30) | ✅ | `ci/iam-lint.sh`; PR |
| IT on target | ABAC on AWS (#30) | ❌ | [D]: `deploy/validation/eks/abac_matrix.sh` exists, never run on AWS; SeaweedFS is more permissive than AWS |
| G | no static keys in shipped configs (#11) | ❌ | no lint; the fix was an audit |
| IT | secret redaction on every channel (#31) | partial | `refused_credentials_are_unsettled_and_redacted` covers ClickHouse error bodies; other channels (logs of the query service, adapter errors) are not checked |
| PH | aggregator names (#24) | ✅ | `TestHostileKeysAreData`, `TestGapTimesAreParsed` |

### H-7 A component uses resources beyond its budget (L-6, L-2)

Controls: durable buffer cap + backpressure (UCA-3, LS-9); memory limiters;
statistics truncation (#9); consumer per-step time bound (#14); resend bound
(#39); per-caller limits in the query service (SEC-5); for the process, the
heavy-job lock (#38).

| Req | Component | Status | Evidence / gap |
|---|---|---|---|
| PH | object size under huge values (#9) | ✅ | `prop_statistics_stay_small_with_huge_values` (Rust), `parquetgo/stats_test.go` |
| ML | per-step time bound (#14) | ✅ | `a_backlog_longer_than_the_lease_window_is_still_ingested`; `renew_only_at_insert` mutant; PR |
| FI | buffer volume full (E3b, LS-9) | partial | `deploy/scripts/diskfull.sh` (manual); a measured result, not a CI check |
| P | per-caller limits (SEC-5, R-S9) | partial | limits exist and are unit-tested; no property over concurrent callers; #35 shows limits are set independently |
| G | shared disk and CPU (#38) | ✅ (mechanism) | heavy-job `flock` with a disk check inside it (AGENT-RULES); a mechanism, not a test |

## 4. UCAs, loss scenarios, STPA-Sec and STPA-Teaming

Class letters from §2. The status repeats the strongest cell of §3.

### Core UCAs

| Item | Hazard | Class | Required | Status | Evidence / gap |
|---|---|---|---|---|---|
| UCA-1 ack before durable | H-1 | A, H | FI (crash consistency), MBT | partial | `TestBatchAnswersAfterTheQueue`, `TestReceivedSurvivesTheQueue`, upstream `custody` / `durable_buffer` tests (N), `faults.sh` SIGKILL; no fsync-loss injection |
| UCA-2 resend with new receive time | H-2 | F, A | P2C, M (`restamp`, `noHorizon`) | ✅ | `received_at_is_the_custody_time_when_the_buffer_has_one`, `TestReplayKeepsReceived`, model mutants (N) |
| UCA-3 no 503 when full | H-1, H-7 | D, H | FI (disk full), ML | partial | E3b measured; `diskfull.sh` manual |
| UCA-4 insert after lease window | H-2 | A, B | K, DST, SM, M | ✅ | `fenced_statement_lands_before_takeover`, `the_server_fences_a_statement_sent_after_the_window`, `no_time_bound` mutant (DST + Hegel) |
| UCA-5 repair while original can land | H-2 | A | MN, DST, FI | ✅ | `errorSettles`/`releaseInFlight` in model, DST, Hegel, Kani; `an_error_answer_whose_commit_is_still_resolving_is_waited_out` |
| UCA-6 GC deletes too early | H-1 | A, B, K | M, MBT, DST | ✅ | `gcReopens`, `mbt_s3inline_consumer` `gc`/`gcRetire`, `EarlyCompact`, #48's admit test |
| UCA-7 no entity record | H-3 | D | M + ML, DST | partial | `entityCatalog.qnt` in no workflow; announcement DST ✅ |
| UCA-8 wrong identity | H-3 | E, I | PH, FZ, IT | partial | `TestClusterFilterRejectsForeignRecords`, `TestHostileKeysAreData`; `FuzzClusterFilter`; not on the target platform |
| UCA-9 route to uncovering source silently | H-2, H-5 | G | M, P, IT | partial | `completeness.qnt` mutant (no workflow); `TestMissingStaleAndErrorAreNeverComplete` |
| UCA-10 result without / wrong complete-through | H-2, H-5 | F | P2C | ✅ | §3 H-2 P2C row |
| UCA-11 evaluate incomplete window | H-4 | F | P2C, M | ✅ | `evalPastCt`, `TestPropOnlyCompleteWindowsDecide` |
| UCA-12 no page on failed evaluation | H-4 | A, D | DST, ML | ✅ | `TestPropCannotEvaluate`, sims, N `alerts-integration` |
| UCA-13 old snapshot believed live | H-5 | — (UI) | IT (UI shows basis) | partial | lake UI banner e2e; HyperDX banner jest; no test of a *pinned old* snapshot's display |
| UCA-14 silence too long | H-4 | — | (R-S4 not built) | ❌ | — |
| UCA-15 retention too short | H-1 | J, F | M, G (config refusal) | ❌ | model in no workflow; refusal not built |
| UCA-16 access too wide | H-6 | I | P (policy), IT on target | partial | sqlscope properties ✅; #36 untested; #30 [D] |

### Loss scenarios

| Item | Required | Status | Evidence / gap |
|---|---|---|---|
| LS-1 replay restamp | P2C, M | ✅ | as UCA-2 |
| LS-2 timeout then commit | FI, MN, DST | ✅ (single node) / ❌ (replicated) | `ch_timeout_commit`, `ch_late`; replicated only by manual soak |
| LS-3 retry into deleted slot | M, MBT | ✅ | `gcReopens`, `mbt_s3inline_consumer` |
| LS-4 paused worker | K, DST (pause) | ✅ | `holder_window_ends_before_takeover`; DST pauses |
| LS-5 controller down / lagging | M + ML, DST | partial | `entityCatalog.qnt` unrun; grace window unit tests |
| LS-6 lake fallback looks live | M, IT | partial | model unrun; `source` on every answer (IT) |
| LS-7 alert on partial window | P2C, M | ✅ | `evalPastCt`; query label properties |
| LS-8 no data treated as OK | DST, ML | ✅ | `TestPropCannotEvaluate` |
| LS-9 buffer volume full | FI | partial | measured, manual |
| LS-10 retention cut during outage | M, G | ❌ | `cutDuringOutage` in no workflow; refusal not built |

### STPA-Sec

| Item | Required | Status | Evidence / gap |
|---|---|---|---|
| SEC-1 forged identity | PH, FZ, IT on target | partial | `clusterFilter` + hostile keys; `FuzzClusterFilter`, ingress `FuzzVerify`/`FuzzBearer`; ABAC [D] |
| SEC-2 stolen edge key deletes | G (iam-lint), IT on target | partial | policy lint ✅; AWS run [D] |
| SEC-3 forged lease / checkpoint | IT on target, M (forged writer) | ❌ | no model has a hostile writer of control objects; ABAC [D] |
| SEC-4 data wider than role | P, IT | ✅ | sqlscope properties; `TestBasisNeverWidensScope`; hdxadapter scoped-token replay |
| SEC-5 query exhausts central | P, IT (load) | partial | limits exist; no adversarial load test |
| SEC-6 insider silences | — | ❌ | R-S4 not built |
| SEC-7 injection runs SQL | PH + FZ | ✅ | properties ✅; `FuzzPrepare`, `FuzzRewriteProperties` (PR seeds, N 60 s) |
| SEC-8 tampered edge time | P2C (skewed producer clock), DST | partial | skew within the D9 bound in DST; no generator with a *hostile* edge clock (beyond the bound) checked against the horizon audit, which is itself not simulated |

### STPA-Teaming

| Item | Required | Status | Evidence / gap |
|---|---|---|---|
| TM-1 mode awareness | IT (UI) | ✅ | lake UI e2e; HyperDX banner |
| TM-2 incomplete windows drawn | P, IT | ✅ | lakeui fast-check; `partial`/`incomplete_from` |
| TM-3 automation's health | DST, ML | ✅ | cannot-evaluate pages |
| TM-4 handoff (owner, expiry) | — | ❌ | not built |
| TM-5 health warnings on views | IT | partial | quarantine and late pages exist; not shown on affected views |
| TM-6 catalog lag, unmatched rows | IT | partial | lag ✅; unmatched count not built |
| TM-7 page flood | — | ❌ | R-S10 not built |
| TM-8 AI assistant sees labels | — | ❌ | no assistant; see R-L8 |

### LLM extension (UCA-L1..14, LS-L1..8, SEC-L1..8, TM-L1..7)

Designed, not built: [research/langfuse.md](research/langfuse.md) §1, D36,
AMBIGUITY X22–X25. Only the spike exists (`langfuse/spike`). **Every cell is
❌ today.** The table is therefore the **exit criteria** that phase 1 must
meet. Where existing machinery already carries the mechanism, the reuse is
named.

| Items | Class | Required when built | Reuse |
|---|---|---|---|
| UCA-L1, LS-L6, SEC-L5 (hash probe across tenants) | E, I | P: equal content in two tenants gives different keys; IT: a probe from tenant A cannot learn anything about tenant B | sqlscope scope properties |
| UCA-L2, UCA-L3, LS-L4, SEC-L7, TM-L5 (size, truncation marks, JSON bombs) | E, J | PH with MB values, deep nesting, invalid UTF-8; FZ on the offloader's parser; P that caps are validated together (#25) | `prop_statistics_stay_small_with_huge_values`, `TestPBTConfigValidateMatchesRules` pattern |
| UCA-L4, X22 (row before payload) | A | MN (payload part first, lost answer); DST with the payload insert under the lease (#23's discipline); D Go = Rust payload hashes | `dst_consumer` announcement path |
| UCA-L5, UCA-L6, LS-L1, TM-L4, TM-L6 (prices) | F | P2C: a price recorded after its effective time; the cost at a basis before and after | basis properties (`TestBasisSameAnswerWhileDataArrives`) |
| UCA-L7, UCA-L8, LS-L2, LS-L3, TM-L2, TM-L3 (settle, basis) | F, D | P2C + M: score only settled traces; ML: a received score is visible at every later basis (langfuse.md §1.9) | `alertEvaluator.qnt` gate pattern; basis |
| UCA-L9, LS-L5, SEC-L3, TM-L7 (prompt injection) | E | PH: adversarial content corpus through the judge harness; the judge's output schema check fuzzed; red-team corpus as IT | — |
| UCA-L10, UCA-L12, LS-L7, LS-L8, SEC-L6, TM-L1, #49 (fact precedence) | F, A | M + MS: `supersedes` chain under at-least-once reordering (#49), a mutant "latest by custody"; P2C with retried older corrections received later; MBT of the resolver | `bitemporalCatalog.qnt` + `TestModelTraces` pattern |
| UCA-L11, SEC-L8 (content to metadata role) | I | P over roles × endpoints: no content without `llm_content`; audit row per read | sqlscope, R-S8 audit |
| UCA-L13 (erasure under a basis) | K, F | M: purge epoch; P: an older basis reports the purge rather than answer differently | basis |
| UCA-L14, SEC-L4 (stored XSS) | E | PH + FZ over the renderer / sanitiser; IT in a browser | lakeui e2e rig |
| SEC-L1, SEC-L2 (claimed tenant, spliced refs) | E, I | PH: SDK-claimed project never changes tenant; refs parsed as 32 hex, all else data (FZ) | `TestHostileKeysAreData` pattern |

#### Phase exit criteria (policy; owner decision 2026-09-29)

A D36 phase counts as **done** only when every exit criterion of that phase
has a passing traceability record at the commit that declares it done:
a record from a tagged test, model run or workflow step (§8) that names one
of the criterion's IDs with one of its techniques, the rule §3's cells use.
The criteria are the table above, split by phase, in
[`ci/trace/exit-criteria.txt`](../ci/trace/exit-criteria.txt);
`ci/trace/trace.py exit --phase D36-1 --records <the run's trace records>`
judges one phase and fails while any criterion is unmet. The nightly's
traceability job prints D36-1's table in its summary on every run (report
only, until the phase is declared); `trace_test.py` checks that every
criterion names known IDs and techniques and that the judgement fails on a
missing, failed, other-technique or other-commit record.

| Phase | Criterion (IDs) | Techniques | Check | Automated |
|---|---|---|---|---|
| 1 | tenant-keyed payload hashes; no cross-tenant probe (UCA-L1, SEC-L5) | P, IT | a property on `offload::tenant_key` / `payload_hash` and the lake IT | yes, once tagged |
| 1 | hostile sizes and shapes through the offloader (UCA-L2, UCA-L3) | PH | `tests/offload.rs` `the_hostile_corpus_round_trips_through_the_object` (tagged) | **yes** (PR `rust`) |
| 1 | the offloader's JSON scanner fuzzed (UCA-L2, UCA-L3) | FZ | `otap-rs/fuzz` `offload_json` (nightly `fuzz`, step record) | **yes** (nightly) |
| 1 | the caps validated together (UCA-L2) | P | a property over `OffloadOptions::validate` (`the_policy_is_validated_together` is a unit test today) | yes, once written |
| 1 | a row never before its payload (UCA-L4, X22) | MN, DST | a model step with the payload part first and a lost answer; `dst_consumer` with payloads | yes, once tagged |
| 1 | Go and Rust edges write the same payload parts (UCA-L4) | D | conformance's GenAI corpus (`conformance/run.sh`) | yes, once recorded |
| 1 | exactly once and dangling = 0 under the fault menus (UCA-L4, LS-L4) | DST, FI | the DST fleet with payloads; `faults.sh` | yes, once tagged |
| 1 | an SDK-claimed project never changes the tenant; refs are 32 hex (SEC-L1, SEC-L2) | PH, FZ | hostile keys through both edges | yes, once written |
| 2 | prices, settle, prompt injection, fact precedence (CAST-49), content role, erasure, stored XSS (UCA-L5..L14, SEC-L4, SEC-L8) | as the table above | phase 2's own tests | when built |

**Not an exit criterion of phase 1:** the differential against Langfuse's
own mapper (research/langfuse.md §11 listed it). The owner deferred that job
on 2026-09-29; it becomes a criterion again only when the owner schedules it.
At this commit D36-1 has one criterion met on the PR path (hostile corpus)
and one on the nightly (the fuzz target); the phase is **not done** by this
rule, though D36 records its code as built.

## 5. Requirements

| Req | Required techniques | Status | Evidence / gap |
|---|---|---|---|
| R-S1 source + complete-through for its own scope | P2C, M, D (adapter vs service), IT | ✅ / partial | properties and IT ✅; `completeness.qnt` unrun; scope property `TestScopedWatermarkProperty` ✅ |
| R-S2 incomplete drawn as such | P, IT | ✅ | lakeui fast-check, e2e; HyperDX jest |
| R-S3 alerts only to complete-through; failed evaluation pages | M, DST, ML, P2C | ✅ | `alertEvaluator.qnt` (unconnected), sims, props, IT |
| R-S4 silences / pins owner + expiry | — | ❌ | not built |
| R-S5 catalog lag + unmatched rows | IT, M | partial | lag ✅; unmatched ❌; `entityCatalog.qnt` unrun |
| R-S6 retention ≥ custody age | M, G (config refusal), P (validator) | ❌ | measure only; model unrun |
| R-S7 write scope by prefix; aggregator rejects mismatches | G (lint), IT on target, PH, FZ | partial | lint ✅, filter ✅, `FuzzClusterFilter` ✅; AWS [D] |
| R-S8 reads role-scoped; audit log | P, IT, D | ✅ / partial | sqlscope, basis scope; #36 composition ❌ |
| R-S9 SQL from a parsed tree; per-user limits | PH, FZ, D | ✅ | properties + replay ✅; `FuzzPrepare` ✅ |
| R-S10 group pages | — | ❌ | not built |
| R-L1..R-L12 | §4 LLM table | ❌ | not built; R-L7 and R-L11 reuse `sqlscope` and the bitemporal pattern; R-L12's "run every consumer" is the G of #45/#46 |

## 6. Every CAST row: would a test catch it now, and the next one of its class?

"Now" names the test that fails on the original bug. "Next" asks whether
the technique generalises to the next instance of the same class, not only
this one. A regression pinned to one shape is "no" unless a generator or
model covers the class.

| # | Class | Catches it now | Catches the next of its class? |
|---|---|---|---|
| 1 | F, A | `received_at_is_the_custody_time_when_the_buffer_has_one`, `TestReplayKeepsReceived`; model `restamp`, `noHorizon` | **partly**: both edges covered; a new custody hop (gateway, LLM offloader) needs its own P2C |
| 2 | A | model `errorSettles`; DST `error_settles` mutant; `an_error_answer_whose_commit_is_still_resolving_is_waited_out` | **yes on one node** (DST fault menu has `ch_timeout_commit`); **no for replicated** (no job starts it) |
| 3 | A | model `releaseInFlight`; DST + Hegel `release_in_flight`; Kani `mutant_release_in_flight_acts_before_landing` | yes |
| 4 | A, B, K | model `gcReopens`; `EarlyCompact`; `mbt_s3inline_consumer` `gc` | yes for GC; a new deleter (lake sealer, index GC) needs the same model |
| 5 | A, J | Kani `mutant_slack_above_margin_lands_after_takeover`; model `keeperOverrun`; start-up check `keeper_session_timeout_ms` | **partly**: the arithmetic yes; the real Keeper bound is measured only by the manual replicated soak |
| 6 | A, B | `a_lagging_replica_is_synced_or_the_run_fails` against two replicas and a Keeper (nightly `replicated`, `ci/replicated.sh`) | yes, in the nightly; DST still has no replicas |
| 7 | E | `prop_statements_keep_every_value_one_literal`, `prop_sql_on_clickhouse` | **partly**: the Rust consumer yes; #24 was the same class in Go a month later. Needs FZ + PH in every module that builds SQL |
| 8 | H | `ci/clippy.sh` (`-D warnings`) | yes, for lints clippy has |
| 9 | E | `prop_statistics_stay_small_with_huge_values`, `parquetgo/stats_test.go` | yes for statistics; LLM payloads (H-L5) need their own |
| 10 | G | `prop_objects_depend_on_the_request_only`; conformance | yes for the edge's objects |
| 11 | H, I | `ci/creds-lint.sh` (PR `go-vet`, with a self-test on planted keys) | yes for the shipped configs it lists |
| 12 | H | G: separate target dirs per checkout; CI records build provenance | yes as a mechanism |
| 13 | A, B | `a_slow_discovery_round_does_not_backdate_lease_observations`; Hegel `backdate_observations`; model `observeAtRequest` | yes: `slow_list`/`slow_s3` rules and the non-atomic model generalise |
| 14 | D | `a_backlog_longer_than_the_lease_window_is_still_ingested`; `renew_only_at_insert`; DST liveness rule | yes for the consumer; yes for the Go edge since 2026-09-29 (`TestDSTEdgeCommit`: every call bounded with no deadline, the "hour on a hung PUT" mutant caught) |
| 15 | A | `a_412_for_our_own_lease_or_checkpoint_write_keeps_the_lane`; Hegel `own_412_is_takeover`; `ambig_s3.rs` | yes via Hegel's `put_own_412` rule (random DST did not find it in 500 seeds); `ambig_s3.rs` is in no workflow |
| 16 | A, G | two `sql.rs` tests; DST `ch_break_profile` | yes for the consumer; the query service pins overflow modes too (`server_test.go`); HyperDX by patch 0001 |
| 17 | E, G | `regression_otap_duration_with_a_zero_time`; `prop_otap_rows_match_otlp…` | yes: the whole value space |
| 18 | E, G | `regression_otap_zero_values`; same property | yes |
| 19 | E, G | `regression_otap_nested_half_float`; same property | yes |
| 20 | A | `a_lost_lease_or_checkpoint_write_keeps_the_lane`; `mbt_s3inline_consumer` designSlow | yes (three-outcome model) |
| 21 | C | `complete_through_mutants_break_soundness` (`WmIgnoresPending`) with 2 writer lanes | **partly**: the Rust mirror yes; `completeness.qnt` itself runs in no workflow and is not trace-connected |
| 22 | H | G: scripts purge only their own run prefix | **partly**: convention, no check that a script never deletes a bucket |
| 23 | A | `dst_consumer` (`statement lands after its lease epoch changed hands`) | yes: the invariant is per statement |
| 24 | E, I | `TestHostileKeysAreData`, `TestGapTimesAreParsed`, `FuzzClusterFilter`, `FuzzGapTime` | yes for the aggregator's parsers; PH elsewhere only where someone wrote it |
| 25 | J, D | `TestReplanMarginBelowTTL` (`internal/lake/config_test.go`) | **partly**: no property that the validator accepts exactly the combinations whose derived deadline is safe |
| 26 | F | `TestCompleteMeansAllRowsWithinLateness`, `TestLateRowNotComplete`; query-integration late row | yes for the label; new consumers of `complete_through` need P2C (§4 LLM) |
| 27 | H | G: worktrees + `git diff FETCH_HEAD --stat` rule | yes as a mechanism |
| 28 | H | G: the verification gate | yes (recurred as #41 before the gate existed) |
| 29 | G | jest test of the adapter token (nightly `hyperdx-fork` runs the fork's suites); IT with the real client | yes for that client version |
| 30 | I | `ci/iam-lint.sh`; `a_403_on_head_is_an_error_not_a_free_slot`, `TestHeadOnlyA404IsFree` | **no** for the class: nothing runs against the real target store ([D]) |
| 31 | E, I | `refused_credentials_are_unsettled_and_redacted` | **partly**: one channel measured |
| 32 | I | `TestMetadataProjectedProperty`, `TestCheckProjectedRefuses`, the replay's column check | yes for metadata reads |
| 33 | K, G | Mosaic e2e "stale cubes" (tagged through the Playwright helper `lakeui/test/pw-trace.js`) | yes in the spike; not a production path (D28 proposed) |
| 34 | F | `TestBasisSameAnswerWhileDataArrives` with the `≤` mutant | yes for the basis; **no general boundary-mutant sweep** |
| 35 | I, J | `TestEvaluatorPeakFitsTheQueryServiceLimit`: two replicas with late checks against the shipped `alert-evaluator` limit | yes for the shipped configs; the production value stays the owner's |
| 36 | I | `TestLimitsOnlyGroupsNeverChangeScope` (rapid, shipped and random configs) | yes |
| 37 | F | `restart_test.go`; fleet replay (manual) | **partly**: an example, not a generator of restarts × relabels × skew |
| 38 | H | G: heavy-job lock with a disk check | yes as a mechanism (it recurred before the lock existed) |
| 39 | D | `a_store_that_fails_every_put_is_not_resent_forever`, `TestAppendResendLimit` | **partly**: no liveness property in the commit model; no Go simulation |
| 40 | G | `TestLateSplitTraces`; conformance | yes while conformance's corpus covers the shape |
| 41 | H | G: the verification gate | yes as a mechanism |
| 42 | A | `a_server_fenced_announcement_is_not_taken_for_landed`; DST with server skew | yes: DST seeds skew the server clock |
| 43 | H | G: strict `testgate` (`OSCOPE_REQUIRE_SERVICES`) | yes for service gates; opt-in gates (`FAULTPROXY2`, `QS_IT_BIN`, …) still skip silently outside their jobs |
| 44 | K | `retirement.qnt` (operator mistake); runbook | **partly**: a model plus a runbook step; no test of the runbook |
| 45 | G, H | `conformance/compare_test.py` (N) | yes for that schema |
| 46 | G, H | `TestFlattenResourceAndEnvelope`; `TestSameRowsAsChdb`; strict gates | yes where the modules' tests run |
| 47 | C | scripted `closeUnsealed`; `a_close_proves_empty_custody_only_when_everything_is_sealed`; `prop_close_proof_needs_every_epoch_sealed` | yes: scripted runs are in `retirement_model.sh` (N) |
| 48 | K | `admit_recovers_quarantined_objects_once_and_gc_keeps_them` | **partly**: no model of quarantine × GC |
| 49 | F, A | none (design; not built) | **no**: a D36 phase-2 exit criterion (§4, "Phase exit criteria") |

Summary: 44 of the 49 rows have a test or mechanism that would catch the
original bug. The five that do not are #6, #11, #35, #36 and #49. For the
next instance of the class, 31 rows answer "yes" (some with a stated limit,
such as #2 on one node only), 12 "partly" and 6 "no". The holes recur: models that no workflow runs
(#21, LS-5..10), no fuzzing of the Rust decoders (#7, #31; the Go parsers are fuzzed since 2026-09-29), no simulation of Go components other than the edge (the edge's DST exists: #14 → #39),
no replicated central (#2, #5, #6), and no run on the target platform (#30).

## 7. Gaps and priorities

The order is hazard severity first (H-1, H-2 and H-6 before H-4, then H-3,
H-5 and H-7), then how many ❌ cells a gap closes. Effort: S ≤ 1 day,
M 2–4 days, L ≥ 1 week. **Where it runs:** *GH nightly* means a new or
existing `nightly.yml` job, callable on its own through the `jobs` input
(ci/README.md, "Running part of the nightly"). New job names must also be
added to `plan`'s list. *PR* means `ci.yml`. *box* means this shared machine
under the heavy-job lock, which should be avoided where GH can run it.

| P | Gap | Hazards / rows | Work | Effort | Where |
|---|---|---|---|---|---|
| **1** | Seven models are checked in no workflow: `completeness`, `entityCatalog`, `retention`, `sealer` (`open_models.sh`, 82 rows), `s3Native`, `fastPath`, `partLifetime`. **Done 2026-09-29**: nightly `model-open` (`ci/model-open.sh`) runs all seven with a new seed each night, the consumer designs at 4× the `model` job's samples plus wider instances (`model/openInstances.qnt`), and Apalache bounded runs; judged by `ci/model-verdict.sh`, the `model` job's rules | H-1, H-2, H-4, H-3; #21, LS-5..8, LS-10 | a `model-open` job running `open_models.sh` (the same fail and warn rules as `ci/model-check.sh`), `fastpath/run_model.sh`, and the s3Native / partLifetime runs from model/README | S | GH nightly (`model-open`) |
| **2** | No automated mutation analysis; boundary mutants are planted only after a bug. **Built 2026-09-29** (owner decision below): nightly `mutants`, cargo-mutants 27.1.0 over `consumer/{coord,plan,gc}.rs` and `proto.rs` (`cargo test --bin consume`) and `store.rs` (`--lib`), gremlins v0.5.1 over `parquetgo/commit`; fails only on a survivor `ci/mutants-baseline.txt` does not accept with a reason | H-2, H-4; #26, #34 class | next: `watermark.rs`, `retire.rs`, the Go query packages once the first scope's survivors are triaged | M | GH nightly (`mutants`) |
| **3** | No fuzzing of any parser facing less-trusted text. **Done 2026-09-29**: Go targets for sqlscope, hdxadapter, rwproxy, the aggregator, basis, the ingress; `cargo fuzz` targets `slot_key`, `slot_meta`, `offload_json` (`otap-rs/fuzz`, `ci/cargo-fuzz.sh`); both in nightly `fuzz` | H-6, H-3, H-2; #7, #24, SEC-7, R-S9 | Go native `Fuzz*` for `sqlscope` (parse → rebuild → re-parse is a fixed point, scope never widens), `hdxadapter` param decoder (differential against ClickHouse when `HEGEL_CH`-like env set), `rwproxy`, the aggregator's key and NDJSON parsing, `basis` token decode; `cargo fuzz` for OTAP/CBOR decode and `Slot::from_meta`; seed each from the hostile generators | M | GH nightly (`fuzz`, 10 min per target, corpus in the Actions cache like `hegel-db`) |
| **4** | The Go edge has no deterministic simulation (#39 was found by a goroutine dump); `lanes: N` in parallel, heartbeats vs the lane mutex, slow S3. **Done 2026-09-29**: `parquetgo/dst` with synctest (PR 24 seeds, nightly `edge-dst`); since the same day it also drives the exporter's heartbeat loop (`edge.Heartbeats`, a swarm entry): heartbeats bounded without a deadline, every lane beaten once the faults stop, no heartbeat after a tombstone | H-1, H-2, H-7; #14/#39 class | a DST of `parquetgo/commit` + `edge` with lost and late answers and a liveness rule; tool → go-verification (`testing/synctest` is the obvious candidate to evaluate) | M | PR (fixed seeds) + GH nightly (new seeds) |
| **5** | Replicated central untested in CI; DST has no replicas and does not run the horizon audit | H-2; #2, #5, #6; C3, C5, C6 | (a) replicas, `SYNC REPLICA` and the audit's queries in `chemu.rs` + fidelity checks; (b) a weekly job with a 2-replica + Keeper cluster running `keeper_faults.sh` | M (a), L (b) | (a) PR; (b) GH nightly weekly (a 16 GB runner is enough per `central-replicated/` measurements, **not verified**). **Started 2026-09-29**: nightly `replicated` (`ci/replicated.sh`: one Keeper, two replicas in Docker) runs the audit's lagging-replica test (#6) |
| **6** | `completeness.qnt` and `alertEvaluator.qnt` are not connected to their implementations | H-2, H-4; #21 | quint-connect MBT of `watermark.rs` (reuse `mbt_s3inline_consumer`'s harness); quintgo replay of the evaluator's recorded runs (the `chdbexporter` step recorder pattern) | M each | GH nightly (`rust-mbt` matrix entry; `model`) |
| **7** | Answer-boundary FI tests not in CI: `ambig_s3.rs` (#15's real-store shape) | H-2; #15 | add to `faults-soak` (it already builds `faultproxy2`) | S | GH nightly |
| **8** | Crash consistency of the durable buffer (UCA-1): no fsync or power-loss injection | H-1 | a LazyFS- or dm-flakey-style run of both edges: kill at every write/fsync boundary, then check "acked ⇒ in S3 after restart" | L | GH nightly if FUSE works on the runner (**unverified**); else box |
| **9** | Policy composition: #36 (grants vs limits), #11 (static keys), #35 (evaluator load vs limit). **Done 2026-09-29**: `TestLimitsOnlyGroupsNeverChangeScope` (rapid, shipped and random configs), `ci/creds-lint.sh` (with a self-test), `TestEvaluatorPeakFitsTheQueryServiceLimit` (two replicas with late checks against the shipped limit) | H-6, H-4 | — | S | PR |
| **10** | A generator of controller restarts × relabels × skew (#37); a hostile edge clock beyond the D9 bound (SEC-8) | H-3, H-2 | rapid over `ctrl` + the aggregator's merge; DST skew menu extended past the bound with the horizon audit simulated (needs 5a) | M | PR |
| **11** | Liveness property in the commit model (#39's lesson) and a model of quarantine × GC (#48) | H-1, H-7 | add to `s3Inline.qnt` / `retirement.qnt`; mutants | S–M | GH nightly (`model`) |
| **12** | ABAC on the real target (#30, SEC-1..3) | H-6 | run `deploy/validation/eks/abac_matrix.sh` on AWS | M | owner's AWS account; not GH or box |
| **13** | Opt-in gates still skip silently (#43's remainder) | process | each nightly job sets the opt-in variables it is supposed to satisfy (`FAULTPROXY2`, `QS_IT_BIN`, …), so a missing one fails | S | GH (workflow) |
| **14** | LLM extension: all ❌ | H-L1..L8 | **Adopted as policy 2026-09-29** (owner decision below): §4's "Phase exit criteria", judged by `trace.py exit`; the phase-1 tests still to be written or tagged | L (with the build) | PR + nightly as each part lands |
| — | R-S4, R-S10, R-S5 (second half), R-S6 refusal | H-4, H-5, H-1 | requirement gaps, not verification gaps: build first, then verify as §5 says | — | — |

**Top five by risk reduction per effort:** 1 (S, closes the "model checked
nowhere" cells on H-1, H-2 and H-4), 7 (S), 9 (S), 3 (M, the only defence
against the next #24), 2 (M, the only defence against the next #34).

### Owner decisions (2026-09-29)

- **Mutation testing (§7 item 2): approved** as a nightly job. Scope: the
  safety-critical modules only, so a run fits a runner (the consumer's lease,
  plan and collection code and the slot protocol; the Rust edge's commit; the
  Go edge's commit and lane code). Surviving mutants are reported as the
  `mutants` artifact and in the job summary; the job fails only on a
  regression against the checked-in baseline of accepted survivors
  (`ci/mutants-baseline.txt`, each with its reason). Selectable through the
  nightly `jobs` input (`mutants`), with the runner's free-disk step.
- **LLM exit criteria (§7 item 14): approved.** §4's exit criteria are policy:
  a D36 phase is done only when each of its criteria has a passing record
  (§4, "Phase exit criteria"). The Langfuse differential job is **deferred**,
  so it is not an exit criterion of phase 1.

### Go tooling judgements (2026-09-29)

- **staticcheck**: in `ci.yml` (job `staticcheck`, `ci/staticcheck.sh`, v2026.2.1)
  over every Go module. The first run found 92 findings; each is fixed or
  suppressed where it stands with its reason. Among them, five test and data
  generators meant negative zero and wrote `-0.0`, which Go reads as +0
  (`math.Copysign(0, -1)` now): the "nasty" datasets never carried a negative
  zero through the edges.
- **hegel-go machines: not built.** Where rapid with the hand-rolled swarm is
  weaker than hegel-go: hegel-go weights each rule per case (rapid's swarm is
  on/off), keeps an example database across runs, and reports per-rule
  statistics. The two rapid machines (the alerts engine, the basis keyring)
  are pure logic with a handful of rules; on/off swarm reaches their states
  and a failure file reproduces a case. The component where weighting and
  fault statistics matter, the Go edge's lanes under faults, is the edge DST,
  which already draws a fault menu per seed and runs 20,000 seeds a night.
  The cost of hegel-go stays a purego alpha behind a build tag and a second
  go.mod per module (PBT.md). Revisit when a Go component gets a stateful
  machine with more than a few fault rules and no DST.

**Load on this box.** Items 1, 2, 3, 6, 7 and 11 run on GH nightly and add
nothing here. Items 4, 9 and 10 are fast PR tests. Only item 8 (if FUSE is
missing on runners) and local development of 5(b) need the heavy-job lock.

## 8. Traceability: which tests ran, and passed, at this commit

The ✅ cells above say a test exists and a workflow runs it; they were not
re-run for this plan. The traceability job turns that into evidence per commit
(the mechanism and commands: [ci/README.md](../ci/README.md), "Traceability").

**How to tag.** First in the test, one line naming the §1 technique and the IDs
it verifies: `tracetag.Covers(t, "P2C", "CAST-26", "H-4", "R-S1")` (Go),
`let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-42", "LS-5"]);`
(Rust), `covers(t, 'P2C', 'CAST-54')` (node:test); a Quint check is an entry
in `ci/trace/models.txt`. Use the technique of the cell the test is evidence
for (an example regression carries its cell's code, e.g. `ML` for
`a_store_that_fails_every_put_is_not_resent_forever`), and the CAST row plus the
hazard, UCA, scenario and requirement IDs the row and the cell cite. Several
techniques: `"DST,ML"`. A test that can skip must skip through `testgate` (or
call `oscope_trace::skipped()`), so the skip is recorded as one.

**How the job decides.** A tag counts only through a record the test wrote when
it ran, with its outcome, at the run's commit. The job fails on a failed record,
an unknown ID or technique, a tagged test a selected job should run with no
passing record, and a CAST row or a required cell of §3 (hazard × technique,
"A + B" two cells, "A / B" one cell either satisfies) with no passing record.
A cell is judged per hazard and technique, not per component row: a covered
cell can still hold a ❌ row (H-1 DST covers the consumer, not the Go edge),
which the report prints beside it.

**Known gaps.** `ci/trace/known-gaps.txt` lists what may lack a passing
record, each with an owner and a reason: the CAST rows whose test cannot exist yet
(#49: the resolver is phase 2), those whose control is a process mechanism
(#12, #22, #27, #38, #41, #51, #55, #60, #63, #68, #69), and the §3 cells no
test is tagged for yet. Tests outside the Go, Rust and node:test runners are
tagged too: a Go fuzz target with `tracetag.Covers(f, …)` (it takes any
`testing.TB`), a Playwright test with `covers(test.info(), …)` from
`lakeui/test/pw-trace.js` (its reporter writes the record), and the fork's
jest suites by a workflow step record in nightly `hyperdx-fork`.
The job reports them and does not fail; a listed item that gains a passing
record is reported stale and should be removed in the same change. Adding a
line weakens a check and gets the same review as deleting a test.
A new CAST row fails the next run until its regression test is tagged with it
or it is listed there: the row's author adds one or the other with the row.

The first slice tags every CAST row's regression test that exists and the model
runs that pin CAST mutants; tagging the rest of §3's evidence closes the
"evidence exists, not yet tagged" lines of known-gaps.txt.

**Model evidence (2026-09-29).** The nightly `model-open` job (§7 P1) is traced
through `ci/trace/models.txt` (script `model_open`): the design rows of each
open model, the fastPath designs, the long and wide consumer runs (M) and the
scripted mutant runs (MS), for H-1, H-2, H-3, H-4 and H-5 with LS-5..8, LS-10
and #21; this closes the H-1, H-2, H-3 and H-5 × M lines of known-gaps.txt.
CAST-50's model step (`s3InlineConsumer.qnt`, LATE_CAS: `lateCkptLostBreaksTest`,
`lateCkptDesignTest`, `designLate`) is tagged MS/MN in the `model` job and
closes H-2 × MN. Apalache rows and random mutant simulations are not tagged:
a run that reaches its time limit (UNKNOWN, recorded skipped) or a mutant
simulation does not reach is an unknown, not evidence.

## Appendix A: tool inventory per module

"Tests" counts `func Test*` / `#[test]`. **Model link**: *connected* =
trace replay or model-driven execution; *mirrored* = a hand-written
simulation restating the model's properties; *cites* = a comment only.

### Rust crate `otel-chdb/otap-rs` (+ `verify/`)

| Tool | Where | CI |
|---|---|---|
| Hegel `hegeltest` 0.47.4: properties | `tests/hegel_props.rs` (17 properties, 3 regressions) | PR (ci profile), N |
| Hegel stateful + swarm | `tests/hegel_dst.rs` (fleet machine, 8 planted mutants) | PR, N (mutant finder) |
| Hegel concurrent | `tests/hegel_race.rs` (emulator, SeaweedFS, `racy_conditional`) | N only |
| quint-connect 0.1.2 MBT | `tests/mbt_s3inline.rs`, `mbt_s3inline_metrics.rs`, `mbt_s3inline_consumer.rs` | N `rust-mbt` ×3 |
| DST level 1 (paused tokio) | `tests/dst_consumer.rs`, `tests/dst/{fleet,retire,sim}.rs` | PR (40 seeds), N (10,000) |
| DST level 2 (turmoil 0.7.2) + emulators | `tests/dst_net.rs`, `tests/dst/{net,s3emu,chemu}.rs`; fidelity checks | PR (8 seeds), N (200) |
| madsim | **not used.** turmoil + the in-binary `getrandom`/`clock_gettime` overrides give determinism with real object_store and hyper (DST.md). madsim would need its own tokio/tonic shims across otel-arrow's graph. Fit: poor while level 2 exists; no gap it would close | — |
| Kani 0.68.0 | `verify/src/proofs_{lease,balance,plan}.rs` | N `kani` |
| unit + randomized | `src/consumer/tests.rs` (38 incl. `randomized_fleet*`, `complete_through_*`), `store.rs`, `runner.rs`, `sql.rs` (real CH + S3) | PR |
| FI | `tests/ambig_s3.rs` (faultproxy2), `scripts/faults.sh`, `consumer_soak.sh` | faults.sh N; ambig_s3 **none** |
| differential | `tests/determinism.rs`, `otap_view.rs`, `series.rs` (vs Go) | PR / N |
| fuzzing | `fuzz/` (cargo-fuzz): `slot_key`, `slot_meta`, `offload_json` | N `fuzz` |
| mutation | cargo-mutants over `consumer/{coord,plan,gc}.rs`, `proto.rs`, `store.rs`; baseline `ci/mutants-baseline.txt` | N `mutants` |
| lint gate | `ci/clippy.sh` | PR |

### Go modules

| Module | Tests | PBT | Model link | Other |
|---|---|---|---|---|
| `chdbexporter` | 39 | hegel-go (`pbt` tag, `go.pbt.mod`): properties + `TestPBTPublisherStateMachine` (swarm) | **connected** to `edgePublish.qnt` via quintgo both ways (`TestPBTPublisherConformsToModel`, `TestModelTracesDriveThePublisher`) | N `chdb` with `PBT_QUINT=1` |
| `parquetgo` (edge, commit) | 67 | none (`TestLateSplitProperty` is hand-rolled randomized) | via `modelcheck` | fault-injecting store in `lane_test.go`; **no DST** |
| `parquetgo/modelcheck` | 5 | — | **connected** to `s3Inline.qnt`, `s3InlineMetrics.qnt` (quintgo validate) | N `model` |
| `parquetgo/compare`, `s3pqexporter` | 5, 14 | — | — | differential vs chDB |
| `query` | 131 | rapid in 15 files (sqlscope, completeness, basis, lake, lakeidx, hdxadapter, server) | **cites** `completeness.qnt`, `retention.qnt`, `sealer.qnt` | IT jobs; KMS emulator |
| `alerts` | 40 | rapid: `engine/prop_test.go`, `qclient`, `runner` sims | **mirrored** (`alertEvaluator.qnt`) | DST-style rapid sims; N `alerts-integration` |
| `entities/bitemp` | 10 | rapid (5) | **connected**: `TestModelTraces` on `bitemporalCatalog.qnt` traces | N `model` |
| `entities/controller` | 15 | — | **cites** `entityCatalog.qnt` (via edge/consumer) | IT with real CH + SeaweedFS |
| `entities/rwproxy` | 12 | — | — | hdxadapter replay covers its statements |
| `s3cas` | 15 | — | **mirrored**: `protocol_test.go` runs `s3Native.qnt`'s log against a real endpoint | CC races vs SeaweedFS |
| `quintgo` (+ `examples/edgepublish`) | 16 + 4 | — | the Go bridge itself | — |
| `otap`, `awss3`, `chdb-go`, `testgate`, `metrics-layout`, `acceptance/s3accept`, `deploy/edgeprobe`, `conformance` (py) | 8, 12, 204, 3, 2, 5, 2, py | — | — | conformance N |

Go modules (2026-09-29, Go 1.27.1): `testing/synctest` for the timer code
(alerts `Run`, controller relist and resync, watermark `Reader`, ingress
`Drain`; each 50× under `-race`); goleak in the goroutine-heavy packages;
porcupine histories via `casreg` (alerts sim, edge lanes, controller lanes,
s3accept, the edge DST); the edge DST (`parquetgo/dst`, nightly
`edge-dst`); native fuzz targets (nightly `fuzz`); rapid machines with
hand-rolled swarm for the alerts engine and the basis keyring; gremlins
(mutation) over `parquetgo/commit` (nightly `mutants`); staticcheck over every
module (PR). `go test -race` runs in PR (`RACE=1`; `parquetgo/dst` is exempt,
see research/go-verification.md §8). Details and the gosim probe:
→ [research/go-verification.md](research/go-verification.md) §8.

### JavaScript

`lakeui` (node:test + fast-check), `lakeui/mosaic`, HyperDX fork patches
(jest, the verification gate), Playwright e2e (N).

## Appendix B: Quint models, connected or isolated

| Model | Checked by | In CI | Connected to code |
|---|---|---|---|
| `s3Inline.qnt` | `mbt_s3inline`; Go `modelcheck` | N | **yes**: Rust (quint-connect) and Go (quintgo) |
| `s3InlineMetrics.qnt` | `mbt_s3inline_metrics`; Go `modelcheck` | N | **yes**: both edges |
| `s3InlineConsumer.qnt`, `…Compact.qnt` | `consumer_model.sh`; `mbt_s3inline_consumer`; Kani cites it | N | **yes** (Rust consumer + GC) |
| `edgePublish.qnt` | `chdbexporter` `PBT_QUINT`; `quintgo/examples/edgepublish` | N `chdb` | **yes** (Go publisher, both directions) |
| `bitemporalCatalog.qnt` | `bitemp_model.sh`; `TestModelTraces` | N | **yes** (Go resolver) |
| `retirement.qnt` | `retirement_model.sh` | N | **mirrored** (`tests/dst/retire.rs` random walk against the real consumer) |
| `alertEvaluator.qnt` | `alert_model.sh` | N | **mirrored** (`runner/sim*_test.go`) |
| `completeness.qnt` | `open_models.sh` | **no** | **mirrored** (Rust `complete_through_*`); query only cites it |
| `entityCatalog.qnt` | `open_models.sh` | **no** | cites only |
| `retention.qnt` | `open_models.sh` | **no** | cites only (R-S6 refusal not built) |
| `sealer.qnt` | `open_models.sh` | **no** | cites only (sealer not built) |
| `s3Native.qnt` | manual | **no** | **mirrored** (`s3cas/protocol_test.go` on a real endpoint) |
| `fastPath.qnt` | `fastpath/run_model.sh` | **no** | isolated (a design study) |
| `partLifetime.qnt` | manual (`model/README.md`) | **no** | isolated (chdbexporter's publishing mode) |
| `ambiguousCall.qnt` | template | — | pattern for the others (TEMPLATE.md) |
| `*_test.qnt` | `quint test` inside the scripts above | with their model | — |
