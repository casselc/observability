# Deterministic simulation testing of the consumer

A spike, in the style of S2's DST (<https://s2.dev/blog/dst>): the consumer
fleet (`src/consumer/`, bin `consume`) runs against simulated S3 and
ClickHouse, with faults, from a single seed. The same seed produces the same
run and the same trace, byte for byte. Any failure prints its seed and a
one-line repro.

| | level 1: `tests/dst_consumer.rs` | level 2: `tests/dst_net.rs` |
|---|---|---|
| runtime | one paused current-thread tokio runtime | turmoil 0.7.2: a runtime per host, a simulated TCP network |
| consumer | real `Worker`, `gc_step`; edge writers as the exporter runs them | same |
| S3 | `SimBucket`: the `Bucket` trait over `MemBucket`, with latency and faults | **real object_store** (AmazonS3: SigV4, XML, retries, error classes) → `HttpConnector` on turmoil TCP → hyper → **S3 emulator** (`tests/dst/s3emu.rs`) |
| ClickHouse | `SimCentral`: the `Central` trait, each statement a server task | **real `ClickHouseCentral`** (the SQL, the settings, error handling) → `central::Transport` on turmoil TCP → hyper → **ClickHouse emulator** (`tests/dst/chemu.rs`) |
| process faults | pause (stop-the-world), network cut, kill + new incarnation | pause (at the I/O boundary), partition, held links (delays), crash + restart |
| speed | ~60 ms per seed (2–4 min of simulated time) | ~0.7 s per seed (1–2 min of simulated time) |

## Determinism

- **OS randomness.** `getrandom(2)` and `getentropy(3)` are defined in the
  test binary (`tests/dst/sim.rs`), so they override libc's for every caller
  in the binary: std's `RandomState` (so every `HashMap`'s iteration order),
  `rand::random` / `ThreadRng` (worker nonces, query ids, object_store's
  backoff jitter), tokio's `select!` RNG, and turmoil's. This is mad-turmoil's
  technique. S2 publishes it as `mad-turmoil` 0.2.1 on crates.io, and it works
  here, but it holds one process-global RNG that can be set only once (one
  seed per process), and its `clock_gettime` returns 0 outside a simulation.
  The equivalent here is ~150 lines: a per-thread stream seeded at the start
  of each run, and the real syscall for threads that are not simulations.
  Each run gets a fresh thread, so thread-local state (`RandomState` keys,
  `ThreadRng`) starts from the seed.
- **Time.** `clock_gettime` is overridden too. Inside a simulation,
  `CLOCK_MONOTONIC` and `CLOCK_REALTIME` read simulated time: at level 1, the
  paused runtime's clock; at level 2, `turmoil::sim_elapsed()`. So
  `std::time::Instant` and `SystemTime` are simulated as well: object_store's
  `retry_timeout`, SigV4's date, and `consumer::wall_ms()`. Level 1 uses a
  current-thread runtime with `start_paused(true)`: time moves only when every
  task waits, and it jumps to the next timer.
- **Decisions.** Every harness choice (fault draws, latencies, which worker
  pauses, edge rates, the fault profile itself) comes from `Sim::rng`, seeded
  from the seed. That RNG is separate from the OS stream, so a new `HashMap`
  in the consumer does not shift the faults.
- **The trace.** Every request and its answer or fault, every statement and
  every landing, every chaos event, and every consumer log line (through a
  new `otap_s3pq::set_log_sink` seam, which replaces the wall-clock stderr
  prefix), each stamped with simulated time. Traces are hashed with blake3.
  With `DST_TRACE=1` they are also kept and written out.
- **The meta tests.** `fleet_is_deterministic` and `net_is_deterministic` run
  each seed twice, each run on a new thread, and compare the traces byte for
  byte. On a mismatch they write both traces and the first differing line.
  `sim_self_test` checks that the overrides are in the path: the same seed
  gives the same `HashMap` order and `rand::random`, another seed gives a
  different one, `getrandom` was served from the seed, and std's clocks
  advanced exactly 90 s over a 90 s simulated sleep.

**Verified:**

- Level 1: 50 seeds run twice in one process were identical.
- Level 1: 200 seeds run in two separate processes gave identical trace
  hashes (`DST_VERBOSE=1` prints them).
- Level 2: 5 seeds run twice in one process were identical.
- Level 2: 20 seeds run in two separate processes gave identical hashes.

## Emulators and their fidelity

**S3 (`s3emu.rs`)** speaks enough of the REST API for object_store's AmazonS3
(path-style, signature ignored):

- PUT with `If-None-Match: *` (412 if the key exists) and `If-Match` (412 on
  another ETag, 404 on a missing key), and `x-amz-meta-*`
- GET and HEAD (ETag, Last-Modified, Content-Length, metadata)
- LIST v2 (prefix, start-after, delimiter, continuation, max-keys)
- DeleteObjects

It is strongly consistent, as AWS S3 is, and SeaweedFS measured the same way
(AMBIGUITY.md S6). Faults, per request:

- `ErrorAfter`: applied, answered 500
- `DropAfter`: applied, then the connection closes
- `DropBefore`: the connection closes and nothing is applied
- `Refuse`: 503
- a LIST lag switch

**ClickHouse (`chemu.rs`)** accepts exactly the consumer's statement shapes:

- `ensure`'s DDL and its `system.tables` reads
- `INSERT … SELECT … FROM s3(…) WHERE now64(3) <= fence`, for one object or
  `{k1,k2}` with `transform(_path, …)`, and the repair's
  `row_ordinal NOT IN`
- the count check, with and without the partition range
- `KILL QUERY` and `SYSTEM SYNC REPLICA`

Any other statement is a syntax error, so a new statement shape fails loudly
rather than being guessed. An object's rows come from its S3 metadata:
`oscope-rows` and `oscope-received`. The fence is evaluated on the server's
clock, which is skewed by up to ±margin/2. Inserts deduplicate by
`insert_deduplication_token` per table (window 1,000). Faults:

- TOO_MANY_PARTS: nothing written, settled
- committed, then the connection drops
- committed late with no answer, up to `fence + budget + slack`
- TIMEOUT_EXCEEDED, then committed later
- a failed count check
- a server profile with `*_overflow_mode = 'break'`: a query that does not
  pin `throw` gets a short count with HTTP 200, and a pinned one gets
  Code 158

**Fidelity checks** (`cargo test --release --test dst_net -- matches --nocapture`).
Each check runs one scripted sequence through the real clients (object_store
over reqwest, and `ClickHouseCentral`) against the emulator, served on a real
socket, and against the local service. It asserts that the two normalized
transcripts are equal, and skips when the service is down.

- `s3_emulator_matches_seaweedfs`: SeaweedFS at `127.0.0.1:18333`, under a
  `otel/dst-diff/<nonce>` prefix that it deletes afterwards. 22 steps: create,
  create again, HEAD, GET, CAS, stale CAS, CAS on a missing key, a plain PUT,
  LIST all, LIST StartAfter a slot, LIST StartAfter `{epoch}/0`, LIST
  StartAfter `{floor}/~`, delimiter listings, HEAD and GET of a missing key,
  delete with a missing key, LIST after the delete. **All 22 match.**
- `ch_emulator_matches_clickhouse`: ClickHouse at `127.0.0.1:18123`, reading
  two real Parquet objects that the test writes to SeaweedFS. 16 steps:
  `ensure` twice, `ranged`, counts on an empty table, a 2-object insert, an
  exact retry (deduplicated), a fenced insert (0 rows), counts in the objects'
  partition range and in another range, a repair of a present object (0
  rows), an insert under a new token (a second copy). **All 16 match.**

Not covered by the fidelity checks:

- the repair of a partially present object
- TIMEOUT_EXCEEDED and the Keeper overrun (those are measured in
  `central-replicated/`, and the emulator reproduces the measured bound)
- replicas, SYNC REPLICA and replica switches
- the horizon audit's queries (the emulator does not serve them, so the audit
  does not run in the simulation)
- ETag format: the emulator's ETags are opaque. The consumer only compares
  them, and the S3 check normalizes them.

## Fault menu, by AMBIGUITY.md row

| row | fault | level 1 | level 2 |
|---|---|---|---|
| S1 data slot `If-None-Match` | answer lost after the write; 5xx/409 after the write, then 412 on the retry; request never delivered; request lands after the client gave up | `put_lost`, `put_own_412`, `put_drop`, `put_late_ms` (edges resolve by HEAD) | `DropAfter` (object_store retries it as unsent: our own 412), `ErrorAfter` (its 5xx retry: our own 412), `DropBefore` |
| S2 tombstone | as S1 | same faults on the consumer's tombstone PUT | same |
| S3 lease/checkpoint `If-Match` | **5xx/409 after a successful If-Match, then 412 for our own write**; lost answer; lost request | `put_own_412`, `put_lost`, `put_drop` | `ErrorAfter` / `DropAfter` on PUT-IFMATCH, through object_store's real retry |
| S4 `gc.json` | lost answers | all PUT faults apply to GC | same |
| S5 GET/HEAD | 503, timeout | `s3_read_err`, cuts | `Refuse`, `DropBefore`, partitions |
| S6 LIST | not yet showing a new object | `DST_LIST_LAG=1` switch (off by default; 300 seeds pass with it on) | same switch (30 seeds pass) |
| S7 DELETE | lost answer | cuts; `DropBefore` | same |
| C1 INSERT | refused before writing (252); committed, answer lost; no answer, lands late within `fence + budget + slack`; TIMEOUT_EXCEEDED, then commits; a statement arriving after its fence (fenced by the server); a first object only, then an error | `ch_settled_err`, `ch_lost`, `ch_late`, `ch_timeout_commit`, `ch_partial`; statements survive a killed client | same, except partial |
| C2 count check | error; **partial result with HTTP 200** under a break-mode profile | `ch_check_err` | `ch_check_err`; `ch_break_profile` (the pinned `throw` makes it an error) |
| C5 Keeper | a commit past `max_execution_time` | `ch_timeout_commit`, `ch_late` up to the slack | same |
| E6 clocks | server and worker skew | server ±margin/2, each worker ±margin/2 (the D9 assumption) | server ±margin/2 |
| processes | stop-the-world pause, kill/restart, network cut/partition, delayed links | pause, kill, cut | pause, crash + restart, partition, hold |

Every rate is drawn per seed from a menu such as {0, 2%, 8%}, so the seeds
range from calm to hostile. Liveness is judged healed: after the chaos,
faults stop.

## Invariants

These are checked while the simulation runs, on the emulators' state, as
well as at the end:

- **neverSkipsCommitted** (model `s3InlineConsumer.qnt`), at every checkpoint
  write: for each epoch, every committed slot below its `next` has all its
  rows in central.
- **noCommitAfterClose**: no data at or after a closed epoch's tombstone
  (level 1).
- **No takeover of a live lease**, at every lease write: a lease is taken
  from another owner only after its version has stood unchanged for
  ttl + margin.
- **Statements stay inside their lease.** A statement is issued (level 1),
  or starts inside its fence (level 2), only under its sender's current
  lease. It lands (commits) before that lease version changes hands, through
  a takeover or a release.
- At the end, **atMostOnce + onlyCommittedIngested + neverSkipsCommitted**:
  every committed content key holds exactly its committed rows, and nothing
  else is in central.
- **Liveness**: once healed, the fleet makes progress until everything is
  in. A minute with no new rows while something is missing is a failure.

**The invariants catch the model's mutants** (`coord::Mutation`), which is
evidence that the harness can find real bugs:

- `fleet_catches_mutants` (level 1): `no_time_bound` by seed 74, `no_verify`
  by seed 1, `release_in_flight` by seed 1, `error_settles` by seed 3,
  `early_compact` by seed 4. All five in under 5 s.
  Since D36 phase 1 (2026-09-29) the sim's trace and log objects also
  reference a payload and carry it when their edge's per-epoch payload cache
  lacks it; `payloadsFirst` (a row lands only after the payload it
  references) is checked at every statement landing, every carried payload
  must land, and the mutant `rows_before_payloads` (the payload statement
  after the rows) is caught by seed 1 ("rows of c9 … land before their
  payload").
- `net_catches_mutants` (level 2): `no_verify`, `release_in_flight` and
  `error_settles`, each by seed 1. `no_time_bound` survived 250 level-2
  seeds: it needs a pause to land between an insert's window check and its
  send, and level 1 finds it.

## Results

- Level 1: **10,000 seeds (1–10,000) pass**, in 647 s on this shared machine (about 65 ms per seed, 56.7 M trace lines). Before the liveness rule became "no progress for 60 s", 2 of 5,000 seeds were flagged, and both were a single worker still working through its backlog.
- Level 2: **300 seeds** (1000–1299) in 213 s, plus 100 more (1–100) in 71 s:
  all pass.
- With the LIST-lag switch on: 300 level-1 seeds and 30 level-2 seeds pass.

## Bugs found

**Fixed in `src/consumer/worker.rs`, each with a regression test in
`tests/dst_consumer.rs`** (commit `otel-chdb(consumer): deterministic
simulation of the fleet; two worker fixes`):

1. **A worker could take a live lease: a safety bug.** Found by seeds 20
   and 34. `heartbeat_and_leases` read the clock once at the start of the
   discovery round (heartbeat PUT, LIST of the heartbeats, their GETs, LIST
   of the leases) and recorded every lease ETag it then listed as first seen
   at that time. After a slow round (slow requests, or a process pause
   inside it), a renewal written during the round looked as if it had stood
   unchanged since the round began. `try_take` then took a lease its holder
   was still renewing, and that holder's statement landed after the takeover
   (seed 20, t = 60.5 s: statement #84 on p0/logs). In production that means
   two workers inserting the same lane, and the count check can miss the
   other worker's in-flight statement, so duplicates are possible. The
   window is the round's duration minus the headroom left of ttl + margin.
   The round is 3 + N requests, each up to the S3 timeout, and a process
   pause adds its whole length. The fix records an observation when its
   answer came (the LIST, and `try_take`'s GET). Repro, with no faults at
   all, only one heartbeat LIST answered 15 s late:
   `a_slow_discovery_round_does_not_backdate_lease_observations` (seed 7).
2. **A backlog longer than the lease window livelocked a worker.** About 10%
   of the first 200 seeds failed this way. A step HEADs up to `max_heads`
   (256) slots per lane, lane after lane, with no renewal until the insert.
   When the scan outlasts `safe_until` (ttl − margin after the last renewal),
   every lease lapses before the insert, the HEAD cache goes with the lanes,
   and the next step starts over: nothing is ingested, ever. In production:
   75 s of HEADs, for example 40 lanes × 256 slots at 20 ms, or fewer at an
   S3 brownout's latency, which is exactly after an outage. The fix renews
   (`maintain()`) between lanes, and a lane stops HEADing once its renewal is
   due. The HEADs made so far are kept, and the rest continue next step.
   Repro: `a_backlog_longer_than_the_lease_window_is_still_ingested`
   (1 worker, 3 producers, 60 ms latency, one 20 s pause, no faults).

**Findings that are not bugs:**

- object_store's `Path::from` percent-encodes `~`. So `StartAfter
  {floor}/~` (`coord::floor_start_after`) is sent as `{floor}/%7E`, which
  sorts before the floor epoch's own keys, and a listing from the floor also
  returns the floor epoch. This was seen identically on SeaweedFS and the
  emulator. It is harmless: `scan` drops epochs at or below the floor, and
  GC has deleted them before an epoch retires. But the intent, skipping the
  floor epoch, does not hold. `{floor}0` or `{floor}/\u{7f}` would, or
  `list` could build the raw path.
- The harness first flagged liveness on seeds whose single worker
  (4 objects per statement at 60 ms per request) simply could not keep up
  with 9 lanes. That is throughput, not a bug. The liveness rule is "no
  progress for 60 s", not a fixed deadline.

## Running and replaying

```sh
cargo test --release --test dst_consumer                       # all of level 1 (~10 s): 40 seeds, meta, mutants, regressions
DST_SEEDS=10000 cargo test --release --test dst_consumer -- fleet_seeds --nocapture
DST_SEED=851 DST_TRACE=1 DST_TRACE_DIR=/tmp cargo test --release --test dst_consumer -- fleet_seeds --nocapture   # replay one seed, keep its trace
DST_SEED_BASE=1000 DST_META_SEEDS=50 cargo test --release --test dst_consumer -- fleet_is_deterministic
cargo test --release --test dst_net                            # level 2 (~1 min): 8 seeds, meta, mutants, fidelity checks
DST_SEEDS=300 DST_SEED_BASE=1000 cargo test --release --test dst_net -- net_seeds --nocapture
```

`DST_VERBOSE=1` prints each seed's summary and trace hash, `DST_LIST_LAG=1`
turns on the LIST-lag fault, and `DST_MUTANT_SEEDS` bounds the mutant search.
A failing seed writes its trace to `$DST_TRACE_DIR` (default: the temp dir)
and prints, for example:

```
DST fleet_seeds seed 20 FAILED: invariant violations: ["t=60546 statement #84 lands on p0/logs after its lease epoch 2 changed hands …"]
  trace: /tmp/dst-fleet_seeds-20.trace
  repro: DST_SEED=20 DST_TRACE=1 cargo test --release --test dst_consumer -- fleet_seeds --nocapture
```

A trace line reads `  <s>.<ms> <process> <request> -> <answer>`.
`VIOLATION`, `PAUSE`, `KILL`, `CUT`, `PARTITION` and `LOG` lines mark the
rest. A seed's run is a function of the seed and the code, so a failing seed
replays the same way until the code changes. To keep a failure as a
regression, turn its shape into a scripted test (the `Profile::calm` plus one
scripted fault, as the two regressions do), because a fixed seed's meaning
drifts as soon as the harness draws differently.

## CI

- **Per PR** (`ci.yml`, the `rust` job, where the services already run):
  `cargo test --release --locked --test dst_consumer --test dst_net`. That
  covers 40 level-1 seeds, 8 level-2 seeds, both meta tests, the mutants, the
  regressions and the two fidelity checks, in about 20 s of test time (7 s and 10 s here) plus
  the two test binaries' build. The seeds are fixed (1–40, 1–8), so a PR
  failure is always reproducible and never flaky.
- **Nightly** (`nightly.yml`, job `dst`): **new seeds every night**. The seed
  base comes from the run number, and the run covers 10,000 level-1 seeds
  (about 11 min) and 200 level-2 seeds (~2.5 min). Failing seeds' traces
  are uploaded as an artifact, with the repro line in the log. Budget: under
  20 minutes. The suite has been stable at 10,000 + 400 seeds since the two
  fixes.
- **A failing nightly seed becomes a regression**: reproduce it with the
  printed line, fix the code, and add a scripted test for its shape, as the
  two above. Also add the seed to the PR list while the harness still draws
  the same way.

## Not simulated yet (next steps)

- The horizon audit: the ClickHouse emulator would need its queries.
- A replicated central: per-replica state, lag, SYNC REPLICA, replica
  switches and `switch_hold_ms`.
- Hints (event notifications), and the load-balancing mode at level 2 (it
  runs at level 1).
- Clock *rates* (drift). Only offsets are simulated.
- The Go edge. The edges here are the exporter's commit rule, written in the
  test.
- A process pause inside CPU work: pauses happen at I/O boundaries only.
- `no_time_bound` at level 2 (see Invariants).
