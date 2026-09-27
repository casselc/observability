# VERIFY: Kani proofs of the consumer's decision code

The consumer's safety rests on two small pieces of arithmetic: the lease
window (`src/consumer/coord.rs`: when the holder may still start a
statement, the wall-clock fence ClickHouse checks, when another worker may
take the lane over) and the check's partition range (`src/consumer/plan.rs`:
which `toDate(received_at)` partitions a count reads). The quint model
(`model/s3InlineConsumer.qnt`) checks the protocol over small abstract
numbers; the harnesses here check the Rust functions themselves, with
[Kani](https://github.com/model-checking/kani) (bounded model checking with
CBMC), over every input in the stated bounds, most of them all of `u64`.

## Layout

`verify/` is a crate of its own (its own `[workspace]` and `Cargo.lock`).
It mounts `src/proto.rs`, `src/consumer/coord.rs` and `src/consumer/plan.rs`
with `#[path]`, so what is proved is the source the worker compiles, byte
for byte; it needs only the four small dependencies those files name, not
otap-s3pq's graph (tokio, object_store, ring, ...), which Kani's nightly
would have to compile too. The harnesses (`verify/src/proofs_*.rs`) are
`#[cfg(kani)]` and use only the modules' public API; nothing in `src/` is
`cfg(kani)`.

## Running

```sh
cargo install --locked kani-verifier --version 0.68.0 && cargo kani setup
cd otel-chdb/otap-rs/verify
cargo kani                                        # every harness, ~20 min
cargo kani --exact --harness proofs_lease::timing_check_is_exact
cargo kani --features mutants --exact --harness proofs_plan::mutant_wall_range_misses_a_received_day   # expected to FAIL
```

Kani runs one harness at a time. Most need under 400 MB;
`own_range_holds_every_received_day_proof` 2.4 GB and
`check_range_holds_every_copy_within_horizon` 5.6 GB. CI: the `kani` job in
`.github/workflows/nightly.yml` (too slow for every push).

## What is proved

Times and peak memory are one run each on a 4-core workstation (Kani 0.68.0,
CBMC with CaDiCaL, or Kissat where the harness says so). "u64" means every
value of the type; `B` = 2^40 ms (≈ 34 years) bounds clock readings and
durations where a harness adds several of them itself.

### The lease window (`proofs_lease.rs`)

Time is a real-time axis nobody can read; each party's clock is real time
plus an arbitrary, unknown offset (so the monotonic-clock results hold for
ANY offset between the holder's and the observer's clocks), and the server's
wall clock is the holder's plus a skew within ±margin (the D9 clock
assumption). Clock rates are equal (the design's rate-drift allowance is not
modelled). The premises are those `Timing::check` implies
(`check_implies_the_safety_premises`): slack ≤ margin, budget + margin ≤ ttl.

| harness | property | bounds | result | time | peak |
|---|---|---|---|---|---|
| `holder_window_ends_before_takeover` | while the holder has not `lapsed` by its own clock, it is more than 2 × margin (real time) before any takeover `Observer::may_take` allows; so two holders' windows never overlap | timings, clocks, lags ≤ B | proved | 5 s | 257 MB |
| `started_statement_ends_before_takeover` | a statement `may_start` allows ends (by its budget) at least 2 × margin before any takeover | ≤ B | proved | 21 s | 257 MB |
| `fenced_statement_lands_before_takeover` | the fence alone (whatever the process did between deciding and sending): a statement that reaches the server at any instant, passes `now64(3) <= fence_wall_ms` on a server clock within ±margin, runs ≤ budget and commits ≤ slack later, has landed before any takeover; the bound is tight (cover) | ≤ B, skew ∈ [−margin, margin] | proved | 243 s | 257 MB |
| `lost_statement_settles_before_the_worker_acts` | a worker whose statement's answer was lost does not `may_act` before that statement can have landed (`settled_by`) | ≤ B | proved | 0.5 s | 256 MB |
| `timing_check_is_exact` | `Timing::check` accepts exactly budget + 2·margin + ttl/3 ≤ ttl (exact arithmetic) and slack ≤ margin; `check_production` adds margin ≥ `MIN_MARGIN_MS` unless allowed short | u64 | proved (after the fix below) | 25 s | 380 MB |
| `check_implies_the_safety_premises` | `check` ⇒ slack ≤ margin and budget + margin ≤ ttl | u64 | proved | 0.5 s | 379 MB |
| `check_leaves_room_after_renewal` | under `check`, when a renewal falls due (ttl/3) a statement still fits (`may_start`) and, with a budget > 0, the holder has not lapsed | u64 | proved | 139 s | 396 MB |
| `window_math_total_and_conservative` | no panic or overflow in `safe_until`, `may_start`, `fence_wall_ms`, `lapsed`, `renew_due`, `settled_by`, `may_act`, `expired`, `check`, for every mutation; wherever the arithmetic saturates it errs on the safe side (window ends no later, settles no earlier, expires no earlier than in exact arithmetic); `lapsed` is monotone | u64, all 7 mutations | proved (after the fix below) | 7 s | 398 MB |
| `fair_share_covers_every_lane` | `fair_share` ≥ 1 and is the least share that covers every lane (ceil) | lanes, workers ≤ 1024 | proved | 3 s | 237 MB |

### Load balancing (`proofs_balance.rs`)

Liveness and balance only (a take still needs `may_take`, a release
`may_act`).

| harness | property | bounds | result | time | peak |
|---|---|---|---|---|---|
| `ewma_finite` | two adds and a read, and a seeded EWMA and a read: the rate is finite and ≥ 0, for ANY decay factor exp may return in [0, 1] | window 1 ms..1 day, times ≤ B, rows ∈ [0, 1e9] | proved | 59 s | 344 MB |
| `take_by_load_liveness` | an orphaned lane is taken by anyone; a lane below target by the least-loaded worker | rows ∈ [0, 1e9] | proved | 0.5 s | 199 MB |

Kani's `f64::exp` is unconstrained (any f64, NaN and ∞ included), so the
EWMA harness stubs it with an arbitrary value in [0, 1] for arguments ≤ 0:
the result holds whatever libm's exp returns there, as long as it is a decay
factor. That the rate only decays between adds needs a monotone exp and so
the float division before it; with monotone stand-ins (1 / (1 − x) with two
adds, max(0, 1 + x) with one) CBMC did not finish in 15 min.

### The check range (`proofs_plan.rs`)

`covers(x)` is the partition test: `toDate(x)` ∈ [toDate(lo), toDate(hi)].
`unranged` is the empty set throughout: a non-empty one only turns a range
into `None` (read every partition), safe by construction, and std's SipHash
over a `String` does not unwind in CBMC.

| harness | property | bounds | result | time | peak |
|---|---|---|---|---|---|
| `own_range_holds_every_received_day_proof` | `own_range` contains every object's received instant (so its partition, by `interval_implies_covers`) and is exactly [min, max]; no objects, or one without a received time: `None` (read everything). (Containment at three objects: the next harness with a zero horizon; three objects here exceed 6 GB) | ≤ 2 objects, received times u64 | proved | 44 s | 2.4 GB |
| `check_range_holds_every_copy_within_horizon` | `check_range` (own range widened by the horizon) holds every instant a copy of any object can have within the horizon, either side; no horizon: `None` | ≤ 3 objects, u64 | proved | 127 s | 5.6 GB |
| `interval_implies_covers` | every instant in [lo, hi] is in a partition the range reads; hence (applied to [r.lo, s.lo] and [s.hi, r.hi]) a range inside another reads no partition the outer one does not, so `widen` and `union` never drop one | u64 | proved (Kissat) | 569 s | 231 MB |
| `union_is_the_interval_hull` | `union` is commutative, associative, idempotent and contains both operands | u64 | proved | 0.8 s | 231 MB |
| `widen_is_monotone_and_saturating` | `widen` never narrows, is monotone in its argument and in the range, saturates at the u64 edges, and composes: widen(a).widen(b) = widen(a ⊕ b) (saturating) | u64 | proved | 5 s | 232 MB |
| `advance_to_stops_at_the_first_undone_or_gap` | the checkpoint moves only over consecutive, done slots and stops at the first that is not | ≤ 4 slots, seq < 8 | proved | 2 s | 314 MB |
| `verdict_is_exact` | `verdict` is a total classification of central's count | u64 | proved | 0.1 s | 229 MB |

### Mutants (`--features mutants`, expected to FAIL)

Evidence that the properties can see the bugs the model's mutants stand for.
Not run in CI.

| harness | mutant | result | time |
|---|---|---|---|
| `mutant_wall_range_misses_a_received_day` | `Mutation::WallRange`: the range is "today" by the worker's clock | fails: "an object's partition is outside the check's range" | 7 s |
| `mutant_no_time_bound_lands_after_takeover` | `Mutation::NoTimeBound`: no fence | fails: "a statement landed after the takeover" | 0.6 s |
| `mutant_slack_above_margin_lands_after_takeover` | slack > margin, everything else as `check` wants (the model's keeperOverrun) | fails: "a statement landed after the takeover" | 0.8 s |
| `mutant_release_in_flight_acts_before_landing` | `Mutation::ReleaseInFlight`: acting once the fence is past, ignoring the slack | fails: `dr > ttl + slack` | 0.3 s |

## Found

- **`Timing::check` overflowed** (`timing_check_is_exact`). In u64,
  `budget + 2 × margin + ttl / 3` wraps in a release build (and panics in a
  debug one): a margin of 2^63 ms with a 3 ms TTL passed `check_production`. Only an absurd `--margin` /
  `--ttl` reaches it, but `check` is the one thing standing between a
  configuration and the window arithmetic's premises. Fixed by doing the sum
  in u128; unit test `timing_arithmetic_never_wraps`.
- **The window arithmetic could wrap or panic at the u64 edge**
  (`window_math_total_and_conservative`): `sent + ttl` in `safe_until`,
  `settled_by` and `fence_wall_ms`, `now + budget` in `may_start`,
  `sent + ttl/3` in `renew_due`, `ttl + margin` in `Observer::may_take`
  (debug panic, release wrap: a wrapped `ttl + margin` would let a lease
  expire early). Unreachable with sane clocks and a checked `Timing`, but now
  saturating, or failing closed where a comparison is involved (`may_start`,
  `expired`); `may_take`'s comparison is extracted as `coord::expired` so it
  can be proved on its own. Same unit test.

## Not proved here, and why

- `load_target` (a sort of f64s: over 10 min), `pick_release` (a
  `Vec<(String, f64, bool)>`: `drop` does not unwind within 6 GB) and
  `group` / `fills` (a `Vec<Vec<Obj>>` of four-`String` objects: out of
  memory) exceed the budget used here (10-15 min, 6 GB) already at two
  elements; the unit tests and the fleet tests cover them.
- `scan_epoch` (std's `BTreeMap`): out of memory (6 GB) already at one
  listed slot; over 10 min at two. `advance_to`, the checkpoint's decision,
  is proved above.
- That `covers`'s UTC day `x / DAY_NS` is ClickHouse's partition key
  `toDate(fromUnixTimestamp64Nano(x))`, which truncates to seconds first:
  ⌊⌊x / a⌋ / b⌋ = ⌊x / ab⌋ for positive integers, but two symbolic 64-bit
  divisions did not finish in 20 min (nor, in 15, a harness adding the
  server's time zone as a symbolic offset in i128). `covers` models the
  server in UTC; the SQL (`range_sql`) has the server compute `toDate` of
  both bounds, in the same zone as the partition key, and in any zone the
  local day is monotone in the instant, so [lo, hi] still reads every
  instant in it.
- Clock rate drift, the renewal protocol's message ordering, and everything
  with I/O: the quint model and the model-based tests
  (`tests/mbt_s3inline_consumer.rs`).
- Unbounded object counts: `own_range`/`check_range` are folds of `union`
  over the objects, so the three-object bound plus the `union` algebra
  (proved for all u64) is the inductive argument, not a Kani proof.

## What Verus would add

Verus proves functions for all inputs by deductive verification (SMT), with
loop invariants instead of unwinding bounds. Here that would mean:
`own_range`/`check_range` for any number of objects (the fold invariant
"r = hull of the prefix"), `scan_epoch` and `advance_to` for any listing,
`load_target`'s water level for any number of lanes, and the window lemmas
without the 2^40 bound. It would also make the specs part of the source
(`ensures` on `safe_until`, `covers`, ...) rather than a separate crate. The
cost: Verus verifies its own Rust subset (no `String` formatting, limited
std, `f64` only opaquely, so not the EWMA), the functions would need to be
written in or ported to that subset with proof annotations, and every
signature change re-opens the proofs. Kani checks the unmodified source;
Verus would prove more of a copy.
