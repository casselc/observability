# Property, stateful and concurrent testing with Hegel

[Hegel](https://hegel.dev) is Hypothesis for Rust: generators, a test-case
database, shrinking, and stateful testing. This crate uses `hegeltest`
0.47.4, pinned exactly (Hegel is in beta), with the `static-engine`
feature: the engine (`hegeltest-c`, pure Rust) is linked into the test
binaries, so nothing is downloaded or loaded at run time.

There are three test targets:

| target | kind | deterministic | runs |
|---|---|---|---|
| `tests/hegel_props.rs` | 14 properties and 3 regression tests | yes | every PR (100 cases each), nightly (3,000) |
| `tests/hegel_dst.rs` | `#[hegel::state_machine]` over the deterministic simulation of the consumer fleet (DST.md), with swarm testing of the fault menu | yes: every case is a seeded simulation | every PR (100 cases), nightly (3,000, plus the planted bugs) |
| `tests/hegel_race.rs` | `#[hegel::concurrent_state_machine]`: real threads racing conditional writes through the production S3 client | no: the OS schedules the threads | nightly only |

```sh
cargo test --release --test hegel_props --test hegel_dst          # the development profile: 200 cases
HEGEL_CH=1 cargo test --release --test hegel_props -- prop_sql_on_clickhouse   # + a local ClickHouse
HEGEL_DEFAULT_PROFILE=nightly cargo test --release --test hegel_dst   # the nightly budget and database
HEGEL_DST_MUTANT=own_412_is_takeover cargo test --release --test hegel_dst -- hegel_dst_fleet   # must fail
HEGEL_DST_MUTANTS=1 cargo test --release --test hegel_dst -- hegel_dst_finds_mutants --nocapture
cargo test --release --test hegel_race                                 # the S3 emulator
HEGEL_RACE_S3=http://127.0.0.1:18333/otel cargo test --release --test hegel_race -- race_conditional_writes
HEGEL_RACE_MUTANTS=1 cargo test --release --test hegel_race -- race_finds_a_racy_store --nocapture
```

A failing deterministic test prints its shrunk example, each draw as a
`let`, and a `#[hegel::reproduce_failure("…")]` line that replays it.
Locally, failures are also kept in `.hegel/examples` (gitignored) and
replayed first on the next run.

## Settings: `hegel.toml`

| profile | selected | settings |
|---|---|---|
| `development` | locally | 200 cases, fresh seed, the database on |
| `ci` | on CI servers (the shipped profile: derandomized, no database, no `too_slow` check) | 100 cases: the same cases on every run, so a PR's verdict does not depend on luck |
| `nightly` | `HEGEL_DEFAULT_PROFILE=nightly` | extends `ci`: 3,000 cases, a fresh seed each night, database `.hegel/examples`, which `nightly.yml` restores from the Actions cache (so the next night replays last night's failures first) and uploads as `hegel-db` |

`HEGEL_TEST_CASES`, `HEGEL_SEED` and the other `HEGEL_*` variables override
any profile.

## Build impact

- **Crates:** `hegeltest`, `hegeltest-c`, `hegeltest-macros`, `dashu-base`,
  `dashu-int`, `toml` 1.1.6 and `serde_spanned` 1.1.1 are added to
  `Cargo.lock` (dev-dependencies only). No existing version changed. `prost`
  0.14 is a direct dev-dependency now; it was already in the graph via pdata.
- **Time:** the first build of a Hegel test binary took ~6 min here, because
  adding a dev-dependency changes feature unification, so parts of upstream
  are rebuilt once. After that each target builds incrementally in ~30 s.
- **Disk:** about +470 MB in `target/`, mostly the three test binaries
  (36–41 MB each) and their dependencies.
- **Production code:** none. `coord::Mutation` gains three test-only
  variants (below), which the worker checks at four places. `plan::group`'s
  doc is corrected.

## Properties (`tests/hegel_props.rs`)

These go beyond the Kani bounds (VERIFY.md) and the fixed corpora:

- `own_range`, `check_range`, `scan_epoch`, `advance_to` and `group` (with
  up to 64–200 objects; Kani stops at 2–4, or runs out of memory at one or
  two), the dedup token, and the slot key round trip.
- The SQL the consumer splices strings into (CAST #7): every hostile value
  stays exactly one literal. With `HEGEL_CH=1`, `sq` and the statements are
  also checked against a real ClickHouse (`hex`, `EXPLAIN AST`).
- Double rendering (Go's `float64AsString`) round-trips.
- Content keys and objects depend on the request alone, not on encoder
  state (CAST #10).
- The OTAP path gives the OTLP path's rows and key, on two threads with
  different `HashMap` seeds.
- The Parquet round trip is lossless with hostile data (invalid UTF-8, NUL,
  huge values, u64 edges).
- One huge value does not blow up an object's statistics (CAST #9).

### What they found

| # | Finding | Where | Shrunk counterexample | Reaches ClickHouse? |
|---|---|---|---|---|
| 1 | OTLP→OTAP drops a span's duration when its start or end time is 0 | upstream `pdata/src/encode/mod.rs` | one span, start 0, end 1 | yes, on Rust-encoded OTAP: Duration 0, not 1000 |
| 2 | An OTAP log body of int 0 or double 0.0 reads as Empty, and an attribute's −0.0 as 0.0 | upstream `views/otap/logs.rs` `get_body_from_struct`; `encode/record/attributes.rs` | one record, body `IntValue(0)` | yes: Body `""`, not `"0"`; `"0"`, not `"-0"` |
| 3 | A half-precision float inside an array or map reads as null | upstream `views/otap/common.rs` `cbor_to_any_value` (no CBOR additional info 25) | one log attribute, `[0.0]` | yes: `[null,7]`, not `[1.5,7]` |
| 4 | `plan::group` emits a solo object's statement ahead of the shared one being filled; the doc said "in order" | `src/consumer/plan.rs` | – | no: harmless, because the checkpoint advances over done slots in slot order. Doc fixed. |

The first three are fixed by `patches/0005-pdata-otap-zero-values-and-half-floats.patch`
(UPSTREAM_ISSUES.md U24, not proposed upstream: awaiting owner review). The
`regression_otap_*` tests pin the fix, and the property now compares every
column with no exclusions.

`scripts/otap_values_e2e.sh` shows the damage end to end: an OTLP/HTTP
request into the edge, direct and `via_otap`; the objects are read by
ClickHouse through `s3()`, as the consumer's INSERT…SELECT reads them.
Before the patch, `via_otap` gave:

```
direct:    0 {}   b {'a':'[1.5,7]','z':'-0'}   zero-start 1000
via_otap:    {}   b {'a':'[null,7]','z':'0'}   zero-start 0
```

After the patch the two are identical. Who is affected: only OTAP that
upstream's Rust encoder wrote. That means `otlp_path: via_otap`, or an OTAP
receiver fed by a Rust otap-dataflow producer. The deployed publisher walks
OTLP directly: `otlp_path: direct`, batch `format: preserve`, the durable
buffer's `pass_through`. The Go otelarrow producer writes zeros, writes CBOR
floats as f64, and always stores the duration.

**Why the conformance checks missed them.** `correctness.py` compares
`via_otap` with the Go reference row by row. It passed on testgen and marked
the hostile corpus `FAIL ... no object (the request was rejected)`: the
OTLP→OTAP conversion rejects nasty-700 over invalid UTF-8 in CBOR
(`results/correctness.txt`, README "hostile batch rejected"). The OTAP input
from the Go producer rejects it too, over Arrow UTF-8. So the only corpus
that has spans with a zero start and nested 0 and ±1 doubles never reached
a row comparison on an OTAP path. The body case needs a batch whose int
bodies are all 0, which neither corpus has (nasty sets an int body only on
odd rows, whose ints are never 0). Go-vs-Rust would catch a Rust-only
divergence, but the Rust edge compared there walks OTLP directly: it never
runs upstream's encoder. The property generates each shape on its own, in a
request small enough that the conversion accepts it.

## The stateful machine (`tests/hegel_dst.rs`)

Each test case builds the level-1 simulated world (DST.md;
`tests/dst/fleet.rs` is `dst_consumer.rs`'s world, moved so both targets
share it). The case draws its setup: 1–3 workers, 1–2 producers (3 lanes
each), latency, the S3 client's timeout, scale mode, zombie time, and clock
skews within the D9 bound. Hegel's rules are then the environment's moves:

- **Load:** `write` (a lane commits n batches), `restart_edge`.
- **Time:** `advance`.
- **Process events:** `pause` (stop the world), `cut` (the network),
  `kill` (a new incarnation), `slow_list` (the next heartbeat LIST answers
  late: CAST #13's shape), `slow_s3` (every S3 answer late for a while), and
  a `gc` step.
- **The fault menu (AMBIGUITY.md), one rule per fault:** `put_drop`,
  `put_lost`, `put_own_412` (CAST #15's shape), `put_late`, `read_503`,
  `stmt_settled_err`, `stmt_partial`, `stmt_lost`, `stmt_late`,
  `stmt_timeout_commit`, `check_err`. Each queues the fault for one
  process's next matching request.
- **Liveness:** `heal` clears every fault; the fleet must then ingest
  everything committed so far.

Because each fault is its own rule, Hegel's swarm testing switches each one
on or off for a whole case. Some cases run with only put faults, some with
only statement faults, some with none.

- **Invariants,** checked after every rule (`#[invariant(always_run)]`):
  no takeover of a live lease; every statement inside its lease;
  `neverSkipsCommitted` and `noCommitAfterClose` at every checkpoint write;
  and no lane dropped over a renewal whose lease in the store is the
  worker's own.
- **At the end:** heal, drain, and let late statements land. Then
  `atMostOnce`, `onlyCommittedIngested`, and liveness.

The simulation runs on its own thread (seeded OS randomness and clocks, as
`sim::run`), and the rules send it closures. Between rules no simulated
time passes except what the rule itself draws (0–5 s, which the shrinker
can take back to 0). A case is deterministic, so failures shrink and replay.
A case takes 1–4 minutes of simulated time and 5–100 ms of real time.

### The planted bugs

`coord::Mutation` carries each known consumer bug behind a test-only
switch. Three are new, the bugs of STPA.md CAST #13–15:

- `BackdateObservations`: lease ETags dated when the round or the GET
  started.
- `RenewOnlyAtInsert`: no renewal between lanes, and no stop in a long HEAD
  scan.
- `Own412IsTakeover`: a 412 on the worker's own lease or checkpoint write
  counted as another's.

`hegel_dst_finds_mutants` plants each bug and runs Hegel until it fails
(derandomized, 2,000 cases, and 20,000 for `no_time_bound`). The DST column
is the first failing seed of `dst_consumer.rs`'s random fleet, with that
seed's trace length.

| mutant | found at execution | time | shrunk to | DST fleet (500 seeds) |
|---|---|---|---|---|
| `backdate_observations` (#13) | 16 | 11 s | 2 rules, 217 trace lines, 23 s simulated | seed 3, 2,939 lines |
| `renew_only_at_insert` (#14) | 454 | 28 s | 5 rules, 1,128 lines, 87 s | seed 15, 6,394 lines |
| `own_412_is_takeover` (#15) | 156 | 17 s | 3 rules, 69 lines, 3 s | **not found** |
| `no_time_bound` | 16,880 | 403 s | 6 rules, 822 lines, 39 s | seed 17, 5,415 lines |
| `no_verify` | 7 | 9 s | 2 rules, 22 lines, 0 s | seed 1, 2,171 lines |
| `release_in_flight` | 43 | 9 s | 2 rules, 180 lines, 27 s | seed 1, 1,506 lines |
| `error_settles` | 3 | 6 s | 2 rules, 173 lines, 26 s | seed 3, 2,772 lines |
| `early_compact` | 31 | 18 s | 3 rules, 2,023 lines, 84 s | seed 4, 7,978 lines |

"Execution" counts every run of the test body, shrink attempts included, so
the first failure is found within that many cases. The shrunk size is the
smallest failing case seen.

The shrunk examples read as the bug's story:

- **#13:** 2 workers, no fault at all. `write` 1 batch; `slow_list`, so the
  next heartbeat LIST answers 9.5 s late. Result: "w0 took p0/traces from
  w1, whose lease was written 935 ms ago". STPA.md records the original DST
  find as seeds 20 and 34, with a 15 s LIST.
- **#14:** 1 worker, 60 ms latency. Three `write`s (14, 64 and 52 batches)
  make a backlog whose HEAD scan outlasts the lease. Nothing is ingested
  after healing.
- **#15:** 1 worker. `write` 1 batch; `put_own_412` on the worker. Result:
  "w0 dropped p0/traces although the lease in the store is the one it just
  wrote". The random DST fleet does not reach it in 500 seeds: its
  own-412 rate is per request, and it needs the 412 on a lease or
  checkpoint write, not on a data PUT. The ambiguity audit found this bug by
  reading.

`renew_only_at_insert` needed the `slow_s3` rule or large writes: without
`slow_s3` it survived 1,000 cases, and with it Hegel found it at case 454
and shrank the slow store away again. `no_time_bound` needs a pause that
lands between a worker's decision and its unfenced statement, while another
worker takes over. Hegel needs ~17,000 cases for it, so the nightly
finder's budget is 20,000.

## The concurrent machine (`tests/hegel_race.rs`)

Rules run on up to four threads per round. Each thread has its own
`consumer::bucket::S3Bucket` (object_store's AmazonS3 client) and runtime,
as separate processes would. They race against the S3 emulator
(`tests/dst/s3emu.rs`, served on a real socket), or against SeaweedFS with
`HEGEL_RACE_S3`.

- **`create`:** `If-None-Match: *` on one of three slots; on 412, read the
  slot back.
- **`cas`:** GET a lease, then `If-Match` its ETag; on 412, read back.
- **`read`:** GET a slot.

The invariants are checked at every join point:

- At most one winner per slot, and the stored body is the winner's.
- Every loser read back the winner's body, never its own (CAST #15).
- A created object never changes.
- At most one winner per (lease, ETag held): no lost update.
- The lease holds the body of a writer that did not lose.

Results:

- **Emulator:** 100 cases pass.
- **SeaweedFS:** 100 cases in 17 s pass. Its create-only and If-Match
  PUTs are atomic under real concurrency, which U4's multipart CAS is not.
- **The mutant (`racy_conditional`):** the test server checks the
  precondition, yields 2 ms, then writes unconditionally. It is found at
  the second execution: "slot-2: 2 writers won a create-only PUT".

Hegel runs a concurrent machine without shrinking, replay, database or
reproducer line, and reports the failing execution as it was found. So this
target runs nightly only, with a small budget.

## CI

- **`ci.yml`, job `rust`, every PR:**
  `cargo test --release --locked --test hegel_props --test hegel_dst` with
  `HEGEL_CH=1`, under the `ci` profile: 100 derandomized cases each, ~10 s
  per binary after the build.
- **`nightly.yml`, job `hegel`:** the `nightly` profile (3,000 cases, a
  fresh seed), in these steps:
  1. the properties with ClickHouse;
  2. the fleet machine;
  3. `hegel_dst_finds_mutants` (up to 20,000 cases each, ~9 minutes);
  4. the concurrent machine and its mutant against the emulator, then 300
     cases against SeaweedFS.

  The database is restored from and saved to the Actions cache (a new key
  per run, restored from the newest), and uploaded as the `hegel-db`
  artifact.

See ci/README.md.

## Limits

- **Hegel is beta.** Its API may change between releases, so the pin is
  exact.
- **The stateful machine drives the level-1 simulation, not level 2**
  (turmoil, real object_store). The SQL, S3 wire and client error paths are
  covered by `dst_net.rs`, `hegel_race.rs` and the MBT tests.
- **Invariant timing.** An invariant runs between rules, and within a rule
  the simulation checks its own invariants as it goes. A violation shows up
  in the rule that caused it.
- **The concurrent machine does not replay,** and a real race it finds may
  need several nights to reappear. Its failure message and the case's
  `note`s (every attempt, with its outcome and what it read back) are the
  evidence.
