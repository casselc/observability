# research: Go counterparts to madsim, turmoil, Kani and friends

A research note (2026-09-29). The owner asked for Go equivalents or
alternatives to the Rust tools the consumer uses: madsim/turmoil-style
deterministic simulation, Hegel, quint-connect model-based testing and Kani.
It covers what the Go toolchain provides, what third parties provide, what
each tool would check in *our* Go components, and a ranked recommendation.
Nothing in the repo was changed; the probes are toys in the scratchpad.

Labels as elsewhere: **[D]** from a cited source, read 2026-09-29, with
versions and dates; **[M]** a probe run here today (§7 has the commands);
**[E]** estimate or judgement.

**Status (2026-09-29, evening).** The owner approved the ranked items and a
half-day gosim probe (D37). Items 1 to 5 of the ranking are built and run
per push; §8 has what exists, the numbers, and the gosim probe's finding.

## 0. Summary

**Answer in one paragraph.** Go has no madsim. Nothing yet gives
madsim-level control (time, network, randomness and goroutine scheduling)
over arbitrary, unmodified Go code at production quality. What comes
closest:

- the standard library's `testing/synctest` (GA since Go 1.25) for **time
  and quiescence**. In **Go 1.27** it gets an **in-memory network**
  (`httptest.NewTestServer`). With it our S3 and HTTP clients run
  unmodified against in-process fakes, under fake time, in one ordinary
  `go test` [D, M];
- **gosim** for **time, TCP, disk, randomness, scheduling and machine
  crashes**, through source translation of the program and the whole
  standard library. The original project is abandoned (last commit
  2024-12-10) and does not build on Go 1.24+. A revived fork
  (`github.com/glycerine/gosim` v0.0.2) runs on Go 1.26 here, and runs
  deterministically [M], but it is experimental;
- **Antithesis**, commercial, which does all of it for whole containers
  from outside the process.

For Kani there is no Go counterpart that checks production Go at the source
level with bounded, all-inputs coverage. The closest are **Gobra**
(deductive, annotation-heavy), **Gomela** (bounded model checking of
channel protocols only, research grade) and history checking with
**porcupine**, which checks recorded runs, not all inputs. Our Go
equivalent of quint-connect already exists: `quintgo/`, together with
`parquetgo/modelcheck`. Our Go Hegel already exists as well: `hegel-go` in
`chdbexporter`.

**Ranked recommendation** (details in §5):

1. **porcupine over recorded CAS histories.** Model a single S3 object
   under `If-Match` / `If-None-Match`. Check every history the existing
   simulations and integration tests already produce: the alerts
   two-replica simulation, the edge commit lanes, the controller's lane
   records, and the SeaweedFS runs. It is cheap, has no dependencies, is
   MIT-licensed, and handles ambiguous (timed-out) writes. It turns
   "no window evaluated twice" into "the store behaved as one
   linearizable register". [M: the probe finds lost updates and stale
   reads, and accepts an ambiguous write that applied.]
2. **`testing/synctest` for the timer and polling loops** we test today
   with injected `Now` functions or not at all: `runner.Run`'s ticker
   (alerts), `awaitRelist` and the resync ticker (entity controller), and
   the watermark `Reader` TTL and max-age logic (query). Moving `query`,
   `alerts`, `entities/bitemp` and `testgate` from `go 1.24` to `go 1.25`
   is a precondition [M].
3. **A Go "level 2" DST on Go 1.27.** Run the real `aws-sdk-go-v2` client
   against a Go S3 emulator (port of `s3emu.rs` semantics) over
   `httptest.NewTestServer`, inside a synctest bubble, with rapid or
   hegel-go drawing faults. This is the Go analogue of `dst_net.rs`
   without turmoil. It needs our CI to move to Go 1.27 (`GO_VERSION` is
   `1.26.x` today).
4. **Stateful rapid / hegel-go machines with swarm, plus `go test -fuzz`
   through `rapid.MakeFuzz`** for the parsers (`sqlscope`, `hdxadapter`,
   rule and config loading). Swarm comes free in hegel-go and has to be
   hand-rolled in rapid.
5. **Goroutine-leak checks:** `goleak` in `TestMain` of the goroutine-heavy
   packages now. Later, Go 1.27's `goroutineleak` profile, which replaces
   `goleak` for most uses.
6. **Evaluate, don't adopt yet:** the gosim fork for the Go edge under
   crashes and partitions (one more probe needed: parquet-go and the AWS
   SDK under translation), Antithesis (only if the owner will pay for
   whole-system testing, and it needs cgo builds), failpoints for the
   ingress (only if seams are not enough), Gobra for one or two pure
   functions (costly; our pure decision code is small).

Not recommended: `testing/quick` (frozen), gopter (last release
2024-04), `fortytw2/leaktest` (2018), `benbjohnson/clock` (archived
2023-05), GFuzz and GCatch (research artifacts, 2022/2023), Temporal's
test suite (it applies only to Temporal workflows), Chaos Mesh inside unit
tests (Kubernetes-only; it belongs in `deploy/validation`), Dafny→Go
(we would be rewriting code in Dafny).

## 1. Our Go code, and what it already has

Go versions per `go.mod` [M: grep of every `go.mod`]:

| Module | `go` line | Notes |
|---|---|---|
| `parquetgo` (edge, commit lanes), `modelcheck`, `s3pqexporter`, `chdbexporter`, `awss3`, `entities/controller`, `otap`, `conformance`, `bench`, `metrics-layout` | 1.26.0 | |
| `query` (query service, lake indexer `lakeidx`), `alerts`, `entities/bitemp`, `entities/rwproxy`, `testgate` | **1.24** | synctest's GA API needs `go 1.25` in `go.mod` (§2.1) |
| `s3cas`, `acceptance/s3accept` | 1.24.7 | |
| `quintgo` | 1.25.0 | |
| `chdb-go` | 1.21 | purego, not cgo |

CI builds with `GO_VERSION: "1.26.x"` (`.github/workflows/{ci,nightly}.yml`).
The container has go1.24.7 installed and toolchains 1.25.0, 1.26.0 and
1.26.8 cached. Go 1.27.0 shipped 2026-08-19 and 1.27.1 2026-09-01 [D:
go.dev/doc/devel/release]. The Entra ingress prototype is being written in
parallel (`otel-chdb/ingress`); it is not in the tree yet.

What exists already on the Go side:

| Rust side | Go side today |
|---|---|
| hegeltest (Hegel) | **hegel-go** `hegel.dev/go/hegel` v0.9.8 in `chdbexporter` (PBT.md: `go.pbt.mod`, `pbt` tag, publisher state machine, automatic swarm). Latest v0.9.11, 2026-09-28 [D: proxy.golang.org]. It embeds `libhegel.so` and loads it with purego: no cgo, needs Go 1.26. |
| proptest-style properties | **rapid** v1.2.0 in `query`, `alerts`, `bitemp`. Used as a random-schedule driver (`alerts/internal/runner/sim_test.go`); no `t.Repeat` state machines and no fuzz targets anywhere [M: grep]. |
| quint-connect MBT + trace validation | **quintgo/** (`connect`, `itf`, `qobs`, `validate`, PObserve-style conformance) and **parquetgo/modelcheck** (Quint drives the Go edge). `informalsystems/itf-go` v0.0.1 (2023-10-17) is only an ITF decoder [D: proxy]; ours goes further. |
| turmoil + paused tokio DST | hand-written: alerts' two-replica simulation (a `hooked` store runs the other replica's tick between a read and a write; rapid draws faults) and `simlate_test.go`; trace replay in `entities/bitemp`. No fake network and no fake runtime. |
| getrandom/clock overrides | injected `Now func() time.Time` (`runner.Now`, `lakeidx.Indexer.Now`, `watermark.Reader.SetClock`, `lane.Now`); real `time.NewTicker` and `time.Sleep` in `runner.Run` and `ctrl.awaitRelist`. |
| Kani | none |

## 2. Deterministic simulation and concurrency

### 2.1 Runtime-provided

**`testing/synctest`** [D: pkg.go.dev/testing/synctest (page for go1.27.1); go.dev/blog/testing-time; release notes 1.26 and 1.27; M: §7.1]

- History: experimental in 1.24 (`GOEXPERIMENT=synctest`, `synctest.Run`);
  GA in **1.25** (`synctest.Test(t, f)`); **1.27** adds `synctest.Sleep`
  and `httptest.NewTestServer`, an in-memory network that works in a
  bubble.
- What it virtualises: the `time` package inside a *bubble*. Each bubble
  has its own clock starting at 2000-01-01 00:00 UTC. The clock advances
  only when every goroutine in the bubble is **durably blocked**.
  `synctest.Wait` waits for that quiescence. Channels, timers and tickers
  created in a bubble belong to it; using them from outside panics.
- What counts as durably blocked: send/receive or `select` on bubble
  channels, `sync.Cond.Wait`, `sync.WaitGroup.Wait`, `time.Sleep`.
- What does **not** count: `sync.Mutex`/`RWMutex` waits, network I/O
  (loopback included), system calls, cgo. A goroutine stuck there keeps
  the clock frozen.
- Not virtualised at all: randomness (`math/rand`, `crypto/rand`, map
  order), goroutine scheduling order (the real scheduler still runs, so
  two runs can interleave differently), the filesystem, and real network.
  Go 1.26's `testing/cryptotest.SetGlobalRandom` makes `crypto/rand`
  deterministic in a test; nothing does that for `math/rand/v2`'s global
  source.
- Probes [M]:
  - go1.25.0 and go1.26.8: a 30 s ticker loop observed 10 min 1 s of fake
    time in 0.00 s of wall time, with exactly 20 ticks;
    `context.WithTimeout(1h)` fires; a `net.Pipe` read deadline fires
    after 5 fake seconds.
  - A loopback `httptest.NewServer` inside a bubble works while the
    handler returns at once. When the handler sleeps 2 s, the test
    **hangs** (killed by `-timeout 20s`): the client goroutine sits in a
    socket read, which is not durable, so fake time never advances.
  - The same handler served over a hand-written `net.Pipe` listener
    (about 25 lines) returns after exactly 2 fake seconds.
  - On go1.27.1, `httptest.NewTestServer` does the same with no listener
    code. Any host routes to the server (`http://s3.us-east-1.amazonaws.com/...`
    worked), and a 5 s `http.Client.Timeout` fires in fake time.
  - go1.24.7 without the experiment: build error; with
    `GOEXPERIMENT=synctest`, `synctest.Run` works.
  - A `go 1.24` module using `synctest.Test` passes `go test` under the
    1.26.8 and 1.27.1 toolchains, but `go vet` reports "synctest.Test
    requires go1.25 or later (module is go1.24)". So the `go` line has to
    move to 1.25.
- Net: synctest is the Go counterpart to tokio's `start_paused(true)`,
  plus (on 1.27) a slice of turmoil's network. The network part is only
  for HTTP, and only between client and server in one process. It has no
  partitions and no latency model unless we write them into the fake,
  which is where they belong in any case.

**Race detector (`-race`)** [D: go.dev/doc/articles/race_detector, E]:
ThreadSanitizer-based and needs cgo. It finds only data races that occur in
the executed interleaving (typical overhead 5–10× CPU, 5–10× memory). CI
already runs the fast Go modules with it (`ci/go-modules.sh`, `RACE=1`). It
complements synctest: synctest does
not explore interleavings, and `-race` does not control them.

**Scheduler knobs** [D: runtime docs; E]:

- `GODEBUG=asyncpreemptoff=1` removes signal-based preemption.
  `GOMAXPROCS=1` serialises execution.
- Neither makes the scheduler deterministic: timers, the netpoller and
  `runtime.Gosched` points still vary between runs.
- `-race` builds randomise some scheduling decisions, which helps shake
  out ordering bugs but is not seedable.
- Polar Signals got "(mostly)" deterministic Go by compiling to
  `wasip1` (one thread, no async preemption), using the `faketime` build
  tag, and a <10-line runtime patch to seed randomness
  (`GORANDSEED`, fork `polarsignals/go@asubiotto-determinism`). Their own
  write-up reports remaining nondeterminism [D: polarsignals.com, 2024-05-28].
  This is not viable for us: aws-sdk and parquet-go under wasip1, and a
  forked toolchain.

**`runtime/trace` and `go tool trace`**: the flight recorder (1.25) and the
execution tracer give scheduler-level evidence after a flaky failure.
They are diagnostic only, not control.

**Goroutine leaks** [D: go1.26 and go1.27 release notes]:

- Go 1.26 added `GOEXPERIMENT=goroutineleakprofile`. It finds goroutines
  blocked on channels, mutexes or conds that no runnable goroutine can
  reach, using GC reachability.
- It is **GA in 1.27** (`pprof` profile `goroutineleak`,
  `/debug/pprof/goroutineleak`).
- Stated limit: it misses leaks reachable through globals or through the
  locals of runnable goroutines.

### 2.2 Third party

| Tool | Version / date [D: proxy.golang.org unless noted] | What it controls | Fit |
|---|---|---|---|
| **Antithesis** + `antithesis-sdk-go` | v0.8.0, 2026-08-28; commercial service | Whole containers (docker-compose/k8s) in a deterministic hypervisor: network faults, node crashes, clock skew, thread pausing, all replayable [D: antithesis.com/docs]. The Go SDK adds `assert` (Always/Sometimes/Reachable…), `random` and `lifecycle`; `antithesis-go-instrumentor` rewrites source for coverage and an assertion catalogue. **Needs `CGO_ENABLED=1`** to report [D: SDK docs]. Outside Antithesis it is local or no-op (`no_antithesis_sdk` tag). | The only madsim-plus option for arbitrary code, including the real collector, SeaweedFS and ClickHouse. Cost: money, container packaging, cgo builds of the Go edge (it is purego today). |
| **gosim** `jellevandenhooff/gosim` | pseudo-version 2024-12-10; `go 1.23.2` | Source-translates the program **and the standard library** to its own runtime: cooperative scheduling picked by a seeded RNG, simulated time, TCP (no UDP, no hostnames), files (no permissions or links), machines that crash and restart with fresh globals, `SetConnected`/`SetDelay` for partitions and latency [D: README, docs/design.md]. MIT. | **Fails on Go 1.24.7** [M]: "unknown linkname pkg=internal/sync …", "missing function body … runtime_SemacquireWaitGroup". The translator knows only 1.23's runtime linknames. |
| **gosim fork** `glycerine/gosim` | v0.0.2, 2026-03-31; revived 2025-09-25 "since original author has abandoned" | Same design, ported to go1.26 "without race detector" [D: README]. | **Works on go1.26.8** [M]: a two-writer CAS toy ran 3 seeds; the first run took 75 s (translating std, 337 MB build cache); seed 1 re-run gave byte-identical output (same simulated timestamps). The examples include etcd and bbolt. Not tried: parquet-go (assembly, `unsafe`) and aws-sdk-go-v2 under translation. One maintainer, experimental API. |
| **detsim** `arshnah/detsim` | v1.0.0, 2026-08-27; MPL-2.0 | `detsim-test` rewrites the AST (goroutines, channels, sync, time, math/rand, net, file I/O), or you use its `Sim`/`Network`/`FaultyStorage` API directly [D: README]. | Too new: 0 stars, 8 commits. Watch it. |
| **Temporal** Go SDK testsuite | `go.temporal.io/sdk` v1.49.0, 2026-09-14 | Time-skipping `TestWorkflowEnvironment` and `WorkflowReplayer`. They work only because workflows use `workflow.Now/Sleep/Go` [D: docs.temporal.io]. | Not applicable (no Temporal). The idea it demonstrates, replaying recorded histories against new code, is what `quintgo` and bitemp replay already do. |
| FoundationDB-style patterns | — | Everything nondeterministic behind an interface (clock, RNG, network, disk) and one seeded driver loop. In Go that means injected `Now` and `Store` interfaces, which alerts and lanes already have. | Keep doing it. synctest removes the need to inject the clock *into timers*. |
| **goleak** `go.uber.org/goleak` | v1.3.0, 2023-10-24 (stable) | `VerifyNone(t)` / `VerifyTestMain(m)`: fails if goroutines outlive the test. | Now, for `alerts/runner`, `lakeidx`, the controller, the edge. Go 1.27's profile can replace it later. |
| `fortytw2/leaktest` | v1.3.0, 2018-11-09 | Same idea | Unmaintained; use goleak. |
| **clockwork** | v0.5.0, 2024-11-29 | `FakeClock` with `Advance`, `BlockUntilContext` | Needs the clock threaded through all code. Superseded by synctest for new tests. |
| **quartz** `coder/quartz` | v0.3.1, 2026-04-09; MIT-0 | Mock clock with *traps* (intercept a `Clock` call, hold it, release it) and `Advance(...).MustWait` | The best injected clock if a component cannot run in a bubble (e.g. it blocks on a real socket). |
| `k8s.io/utils/clock/testing` | 2026-07-07 | `FakeClock`, `FakePassiveClock` | Already transitively in the controller via client-go; informers take it. |
| `benbjohnson/clock` | v1.3.5; **archived 2023-05-18** [D: GitHub] | — | Don't. |
| In-memory networks | stdlib `net.Pipe`; `grpc/test/bufconn` (grpc v1.84.0, 2026-09-17); Go 1.27 `httptest.NewTestServer` | Conns with no sockets | `bufconn` for the OTLP gRPC ingress (§4). `github.com/shoenig/test` (v1.13.2) is an assertion library, not a network. |
| **failpoint** `pingcap/failpoint` | pseudo-version 2026-08-11; Apache-2.0 | `failpoint.Inject("name", …)` markers rewritten by `failpoint-ctl` into `if failpoint.Eval(...)`; enabled by `GO_FAILPOINTS=pkg/name=return(x)` or HTTP; zero cost when not rewritten [D: README] | A code-generation step in the build. Our interfaces (`Store`, `Getter`, `Evaluator`) already give seams without it. |
| **toxiproxy** | v2.12.0, 2025-03-18 | TCP proxy with toxics (latency, bandwidth, timeout, reset_peer, slow_close, limit_data), driven over an HTTP API | Integration level only, with real time. We already have S3 fault proxies (`awss3/cmd/faultproxy`, `otap-rs/tools/cmd/faultproxy2`); toxiproxy adds nothing decisive. |
| **Chaos Mesh** | Go module tag v1.2.4 (2021); GitHub release page shows v2.8.4 (2024-08-18) as latest | Kubernetes CRDs: network, IO, time, stress, pod faults | Kubernetes-only; for `deploy/validation`, not for unit tests. |
| `sasha-s/go-deadlock` | v0.3.9, 2026-03-18 | Drop-in `sync.Mutex` that reports lock-order inversions and long waits | Cheap to try in tests of the controller and the lanes. |

**Is there madsim for Go?** [E] Only whole-program approaches get all four
axes (time, network, randomness, scheduling):

| | time | network | randomness | scheduling | crashes | arbitrary code | maturity |
|---|---|---|---|---|---|---|---|
| synctest (1.27) | yes | HTTP in-proc only | no | no | no | yes | GA, stdlib |
| gosim fork | yes | TCP | yes | yes | yes | mostly (no cgo; std translated) | experimental, one maintainer |
| detsim | yes | yes | yes | yes | ? | via rewrite | brand new |
| wasip1 + faketime | yes | no | patched runtime | 1 thread | no | limited | blog-grade |
| Antithesis | yes | yes | yes | yes | yes | yes (containers) | commercial |

## 3. Property-based, stateful and swarm testing

| Tool | Version / date | Notes |
|---|---|---|
| **rapid** `pgregory.net/rapid` | v1.3.0, 2026-03-30 (we pin v1.2.0); MPL-2.0; `go 1.23` | Hypothesis-style: generators over a byte stream, automatic shrinking, failure files in `testdata/rapid`. **State machines**: `t.Repeat(map[string]func(*rapid.T))` or `rapid.StateMachineActions(sm)`, with a `""` action as the invariant check. **Fuzz bridge**: `rapid.MakeFuzz(prop)` returns `func(*testing.T, []byte)` for `f.Fuzz`, so coverage-guided `go test -fuzz` drives rapid's generators [M: API in v1.3.0 source]. **No swarm testing** built in [M: no mention in source]. Emulate it by drawing an enabled-action subset at the start of each case and filtering the `Repeat` map. |
| **hegel-go** `hegel.dev/go/hegel` | v0.9.11, 2026-09-28 | Go client for the Hegel engine, in process via purego + an embedded `libhegel.so` (0.43.4 cached here). State machines with **automatic swarm** (PBT.md: e.g. manifest faults on in 39% of cases, crashes 40%), `WithConcurrency`, an example database, statistics. Cost: requires purego `v0.11.0-alpha.*`, so it lives in `go.pbt.mod` behind a build tag. It exists, so the "drive the Hegel protocol from Go" question is settled. |
| `go test -fuzz` | stdlib since 1.18 | Coverage-guided mutation over `[]byte`, string, ints, floats, bool; one target per `-fuzz` run; corpus under `testdata/fuzz`; `F.ArtifactDir` (1.26). Best for parsers and decoders; weak for protocols (no structure) unless fed through `MakeFuzz`. |
| `testing/quick` | stdlib | "Frozen and is not accepting new features" [D: pkg.go.dev]. No shrinking. |
| gopter | v0.2.11, 2024-04-03 | ScalaCheck port with `commands` for stateful tests and shrinking. Superseded by rapid. |

Swarm testing, concretely: hegel does it for us. In rapid, add
`enabled := rapid.SliceOfNDistinct(rapid.SampledFrom(names), 1, len(names), rapid.ID).Draw(t, "swarm")`
and build the `Repeat` map from `enabled` only. Shrinking then also
minimises the set of enabled faults [E].

## 4. Model checking and verification (Kani's role)

Kani here proves that `coord.rs` and `plan.rs` arithmetic holds for every
`u64` in bounds (VERIFY.md). Go has no CBMC front end and no Kani [E: none
found]. The options, by what they give:

| Tool | Version / status | What it would give us | Cost |
|---|---|---|---|
| **Gobra** (ETH, Viper) | active, 2,335 commits; MPL-2.0 [D: GitHub] | Deductive verification of annotated Go: memory safety, functional contracts, a permission logic for goroutines and channels. Used for VerifiedSCION. Unbounded proofs, stronger than Kani. | Java + sbt + Z3. Pre/postconditions and loop invariants by hand; only a subset of Go (generics support limited [E]). Worth it only for one or two pure functions, e.g. the lease or fence arithmetic if it were ported to Go (it is Rust today) or `completeness` settled-through arithmetic. |
| **Gomela** | research (ASE'21, ASE journal 2023) [D] | Bounded model checking of **channel/WaitGroup protocols**: extracts Promela and runs SPIN for global deadlocks, channel-safety errors and goroutine leaks. | Research tool, abstracts away data. Our concurrency is mostly mutex + CAS on S3, which it does not model. |
| **porcupine** `anishathalye/porcupine` | **v1.3.1, 2026-09-21**; MIT [D: proxy, GitHub] | Linearizability checking of recorded histories against a sequential `Model` (`Init`, `Step`, optional `Partition`/`Hash`). `NondeterministicModel`; `CheckOperationsTimeout` returns `Unknown` on timeout; `Visualize` writes an HTML timeline. Used by etcd, Amazon MemoryDB, S2 [D]. | Low. It needs histories with call and return timestamps. An ambiguous operation (timeout, 5xx, dropped connection) gets `Return = math.MaxInt64` and a `Step` that accepts both "applied" and "not applied" [M: §7.3]. |
| **Elle** (Jepsen) | Clojure; `elle-cli` front end; a Go port lived in `pingcap/tipocket` [D: search] | Transactional anomaly detection (cycles in dependency graphs) from list-append or rw-register histories | Our "transactions" are single-object CAS plus multi-object publish protocols. porcupine covers the first; the second is Quint's job. Elle would matter only for the central ClickHouse insert-dedup semantics, and it runs on the JVM. |
| **TLA+/Quint + trace validation** | ours: `quintgo`, `parquetgo/modelcheck` | Model checking of the design, and conformance of Go traces to the model (PObserve-style) | Have it. Extend bindings to alerts' state document and the controller lanes. |
| **P** (+ PObserve) | v0.4.1 on the Go proxy, 2025-05-21; MIT; C#/.NET toolchain [D: p-org] | Communicating state machines; PObserve checks production logs against P monitors (used by S3 at AWS) | Same niche as Quint + quintgo, which we already run. No Go codegen. |
| **Dafny → Go** | latest release seen 4.11.0 (2024-08-25) [D: GitHub releases page, as rendered] | Verified code compiled to Go | Means rewriting a component in Dafny; generated Go is not idiomatic. No. |
| `go vet`, **staticcheck** (v0.8.1, 2026-08-21), **nilaway** (pseudo-version 2026-09-18; "false positives and breaking changes can happen") | | Cheap static bugs: copylocks, loopclosure, `waitgroup` misuse, nil flows across packages | staticcheck in CI is near-free. nilaway once, triaged, not gating. |
| **GCatch / GFuzz** | GCatch 2023-06; GFuzz artifact 2022-01 (ASPLOS'22) [D] | Static (GCatch) and runtime-patched fuzzing (GFuzz) of channel/select bugs | Research artifacts tied to old Go runtimes. No. |

## 5. Fit to our components

What each tool would check, what it costs, and the limits that matter.

**Go edge / commit protocol** (`parquetgo/edge`, `parquetgo/commit`: `Store`
= `PutCreate` with `If-None-Match: *` + `Head`; aws-sdk-go-v2 v1.47,
parquet-go v0.32)

- *porcupine*: wrap `commit.Store` in a recorder (client id = lane or
  writer, call/return from a shared monotonic counter). Check each key's
  history against the one-object model: create-once, head sees the
  creator's metadata. It catches a fake or real store (SeaweedFS) that lets
  two `PutCreate` succeed, and a stale `Head` after a successful create. It
  runs over the existing `resend_test.go` and `lane_test.go` workloads and
  the s3accept runs against real S3 [E].
- *synctest (1.27)*: the edge's flush and rotation timers, and SDK
  retries and timeouts, against a Go S3 emulator served by
  `httptest.NewTestServer`. The real SDK stack (SigV4, retries, XML
  errors) runs, as in `dst_net.rs`. Limits:
  - randomness (SDK retry jitter, map order) is not seeded, so a failing
    seed may not replay byte for byte. Record the fault schedule, not the
    run;
  - parquet-go's internal goroutines, if any block on mutexes, delay
    quiescence but do not break it [E].
- *gosim fork*: full crash and restart of an edge "machine" with its
  local disk (the late-data spool) plus partitions. It is the only
  in-process way to test crash between parquet write and commit on
  disk [E]. Not yet known to translate parquet-go and aws-sdk: probe first.
- *Antithesis*: the whole collector + SeaweedFS + consumer. Needs cgo
  builds for SDK reporting.

**Alerts evaluator** (`alerts/internal/runner`, `store` with `PutIfMatch`/`PutIfAbsent`)

- *porcupine*: record the `hooked` store's operations in `TestSimTwoReplicas`
  and check that the state document behaves as a CAS register. That
  strengthens "every window once, in order" from a counter check to a
  store-level property, including answered-but-lost writes (ambiguous) [E].
- *synctest*: test `Run` itself: the real `time.NewTicker(r.Tick)`, the
  `min_gap` skip, stalled-lane paging after N minutes, shutdown with
  goleak. Two replicas as two goroutines in one bubble sharing an
  in-memory store. The sim's hand-driven interleavings stay; synctest adds
  the timer paths the sim does not execute. Needs `go 1.25` in
  `alerts/go.mod`.
- *rapid state machine + swarm*: turn the `steps` slice into `t.Repeat`
  actions (tick A, tick B, fault store, fault sink, advance clock) so
  shrinking reports actions, not integers.

**Query service** (`completeness/watermark.go` Reader with ttl/maxAge;
`lakeidx` indexer; `sqlscope`, `hdxadapter`)

- *synctest*: the watermark cache's ttl re-read, the `maxAge` staleness
  flip and the lateness bounds in fake time, without `SetClock`. Also the
  indexer's periodic loop and its header cache.
- *fuzz via `rapid.MakeFuzz`*: `sqlscope` (SQL scoping, a security
  boundary, SEC-class hazards) and `hdxadapter` binding. Their existing
  rapid properties become fuzz targets for free; run nightly with a time
  budget.
- *porcupine*: for lake index segments committed by conditional writes,
  as for the edge.

**Entity controller / aggregator** (`entities/controller`: client-go
informers, `awaitRelist` polling with `time.Sleep`, resync ticker, lane
records on S3)

- *synctest*: `awaitRelist`'s `relistWait` deadline and quiet-period
  logic, and resync, with a fake clientset. client-go's workqueue waits on
  `sync.Cond` (durable). Its rate limiters and informer resync go through
  `k8s.io/utils/clock`, whose real implementation calls `time`, so it is
  virtualised in a bubble [E]. This targets the flaky-timing class behind
  the recent "stopped controller sync" CI fix.
- *goleak*: informers and workers must stop on context cancel.
- *porcupine*: lane record CAS histories.
- *bitemp trace replay* stays as is; a rapid state machine over
  (event, valid time, system time) against the reference resolver is the
  natural next property.

**Entra ingress prototype** (being written; HTTP/gRPC front with token
validation)

- *synctest + `httptest.NewTestServer` (1.27) / `bufconn`*: token expiry,
  clock skew and JWKS refresh under fake time. Note the bubble starts at
  2000-01-01, so test tokens must be minted inside the bubble.
- *fuzz*: the token and header parsers.
- *failpoints*: only if a code path has no interface seam (e.g. inside a
  vendored verifier). Prefer seams.

**Cross-cutting limits** [E]:

- **cgo and libchdb**: `chdb-go` loads libchdb with purego. Calls into
  chDB are foreign calls, not durable blocking, so synctest cannot advance
  time across them. gosim cannot translate them. Keep chDB out of
  simulated tests (the `chdbexporter` fake session pattern).
- **Real S3 / ClickHouse**: never inside a bubble. Use emulators; keep
  the real-service runs (s3accept, integration) as the fidelity check,
  and feed their histories to porcupine.
- **Goroutine-heavy libraries**: synctest tolerates them if they block on
  channels or conds. Libraries that spin on mutexes or poll with real
  sockets stall quiescence. Tests then hang until `-timeout`, which is
  the failure mode to expect and diagnose (`GOTRACEBACK=all`).
- **Mutex-protected fakes**: a fake whose goroutines wait on
  `sync.Mutex` is fine (short critical sections). A fake that waits on a
  mutex *for an event* must use a channel or `sync.Cond` instead.

**Ranked, with costs** [E]:

| # | Action | Checks | Cost | Maturity / licence |
|---|---|---|---|---|
| 1 | porcupine recorder + CAS model in a small shared test package; wire into alerts sim, edge lanes, controller lanes, s3accept | linearizability of every conditional-write history we already generate, including ambiguous writes | ~1–2 days; 0 deps beyond porcupine | stable, MIT, v1.3.1 (2026-09-21) |
| 2 | `go 1.25` in query/alerts/bitemp/testgate; synctest tests for runner.Run, awaitRelist, watermark Reader | timer, ticker and deadline logic now untested or tested through injected `Now` | ~1 day per component; no deps | stdlib, GA since 1.25 |
| 3 | CI to Go 1.27; Go S3 emulator (If-Match/If-None-Match/LIST v2, ErrorAfter/DropAfter/DropBefore from s3emu.rs) over `httptest.NewTestServer`; DST of the edge and alerts with the real SDK | turmoil level-2 analogue: SDK retries, ambiguous writes, timeouts in fake time | ~1 week; Go 1.27 bump across modules | stdlib (1.27 API ~6 weeks old) |
| 4 | rapid `t.Repeat` machines with a swarm subset; `MakeFuzz` targets for sqlscope/hdxadapter/rule parsing, nightly | shrinkable action sequences; coverage-guided parser inputs | ~2–3 days | rapid MPL-2.0; hegel-go where swarm or stats matter |
| 5 | goleak in TestMain now; the `goroutineleak` profile after 1.27 | goroutines outliving shutdown | hours | goleak stable (2023); profile GA in 1.27 |
| 6 | gosim-fork probe on parquetgo/commit + aws-sdk | if it translates: crash/partition DST of the edge with disk | ½ day to probe; adoption risk high | experimental, MIT, single maintainer |
| 7 | Antithesis | whole-system, all faults, replayable | commercial contract; cgo builds; container images | commercial |
| 8 | Gobra on one pure function | unbounded functional proof | days of annotation per function; JVM toolchain | research-grade, MPL-2.0 |

## 6. Rust ↔ Go map

| Rust (otap-rs) | Go counterpart | Gap |
|---|---|---|
| turmoil (sim TCP, hosts, partitions) | synctest + `httptest.NewTestServer`/`bufconn`/`net.Pipe` (in-proc, no partitions unless faked); gosim fork (TCP, machines, partitions) | No stable Go equivalent with hosts and partitions |
| paused tokio (`start_paused`) | synctest bubble | Equivalent, and simpler |
| getrandom/clock_gettime overrides | clock: synctest; `crypto/rand`: `testing/cryptotest.SetGlobalRandom` (1.26); `math/rand`: none (seed explicit `rand.New` sources) | Map order and the global `math/rand` stay random outside gosim |
| hegeltest | hegel-go (in use) / rapid | none |
| quint-connect | quintgo `connect` + `itf` (ours) | none |
| Kani | Gobra (deductive), Gomela (channels, bounded), porcupine (histories, not all inputs) | No bounded all-inputs checker for Go source |
| mad-turmoil determinism meta-test | gosim: same seed, same output [M]; synctest: not byte-deterministic | Only gosim gives replay |

## 7. Probes

All in the scratchpad (`gov-probe/`), toys only; no repo code, no data.

**7.1 synctest** (`gov-probe/synctest`, `st124`, `st124b`, `st127`):

```
GOTOOLCHAIN=go1.25.0 go test -v ./...   # TestTicker, TestTimeoutContext, TestPipeDeadline, TestHTTPLoopback: PASS
GOTOOLCHAIN=go1.26.8 go test -v ./...   # same: PASS; start=2000-01-01 00:00:00 UTC, ticks=20
GOTOOLCHAIN=go1.26.8 go test -run HandlerSleeps -timeout 20s   # loopback + handler sleep: panic: test timed out after 20s
GOTOOLCHAIN=go1.26.8 go test -run OverPipe      # net.Pipe listener: "GET over pipe took fake 2s", PASS
GOTOOLCHAIN=go1.27.1 go test -v .               # NewTestServer: body="ok s3.us-east-1.amazonaws.com" fake elapsed=2s;
                                                # client timeout "after fake 5s: context deadline exceeded": PASS
GOTOOLCHAIN=local go test (go1.24.7)            # "build constraints exclude all Go files in .../testing/synctest"
GOTOOLCHAIN=local GOEXPERIMENT=synctest go test # synctest.Run: PASS
module go 1.24 + synctest.Test: go test PASS (1.26.8, 1.27.1); go vet (1.26.8):
  "synctest.Test requires go1.25 or later (module is go1.24)"
```

**7.2 gosim** (`gov-probe/gosim`, `gosim2`; heavy lock held):

```
jellevandenhooff/gosim@main, go1.24.7:  ERROR unknown linkname pkg=internal/sync name=runtime_rand ...
                                          ERROR missing function body pkg=sync name=runtime_SemacquireWaitGroup  (exit 1, 59 s)
glycerine/gosim v0.0.2, go1.26.8:        TestGosimToy seeds 1-3 PASS (3.70 s / 2.29 s / 2.99 s simulated), 75 s cold,
                                          337 MB build cache; seed 1 re-run: identical log line and timestamps
```

**7.3 porcupine** (`gov-probe/porc`, v1.3.1): a one-object model with
`put(if vN -> vM)` returning ok / 412 / ambiguous and `get -> vN`.

- `TestAmbiguousApplied` (A's put times out but applied; B's 412; B reads
  v1): Ok.
- `TestLostUpdateDetected` (two successful puts conditioned on v1):
  Illegal, and `Visualize` wrote the HTML.
- `TestStaleReadDetected` (a read of v0 after a completed write): Illegal.

All three pass in 0.003 s.

The go1.27.1 toolchain, the gosim build caches and the Go build cache were
deleted afterwards for disk.

## 8. Status: what was built (2026-09-29)

All Go modules and CI moved to **Go 1.27.1** (`go 1.27.0`, `toolchain
go1.27.1`; CI `GO_VERSION` 1.27.x). What the new toolchain changed:

- `go vet`'s printf check flagged two non-constant format strings in the
  chdb-go fork (one real: a corrupt head's detail was re-interpreted as a
  format).
- **Go 1.27 turns the jsonv2 experiment on by default**, and encoding/json
  v1 now runs on the v2 engine. An invalid UTF-8 byte in a string is
  written as a raw U+FFFD instead of the six-byte escape. pdata's
  `AsString` renders map and slice attributes with encoding/json, so the
  Go edge's `AttributesValues` and layout-B `series_id` changed for such
  attributes and no longer matched the Rust edge (CI run 36532538131,
  `tests/series.rs`). The edge renders them with its own encoder
  (`parquetgo/attrjson.go`), first pinned to Go 1.26's bytes; since
  2026-09-29 (AMBIGUITY E10, owner: follow Go 1.27) pinned to Go 1.27's,
  which the Rust edge writes too (shared vectors
  `parquetgo/testdata/attrjson_vectors.json`). The contrib and chdb
  exporters built with Go 1.27 match the edge byte for byte again, and
  `parquetgo/compare` no longer maps U+FFFD.

| Item (§5 rank) | Where | Per push | Nightly | Found |
|---|---|---|---|---|
| goleak (5) | `TestMain` in parquetgo (+commit, edge), controller (ctrl, lane, aggregator), alerts (runner, notify, qclient), query (app, server, lakeidx, completeness, hdxadapter, central), ingress | yes | — | the controller's work-queue goroutine outlived a failed `Run` (fixed: shut down on every exit) |
| porcupine (1) | `casreg` (model, recorder, `http.RoundTripper`); alerts `TestSimTwoReplicas`; `parquetgo/dst` lane histories (200 seeds); controller lane writer (two writers, lost/late answers, 20 seeds); s3accept check `linearizable`; the edge DST | yes | edge-dst | nothing in our code; the mutants "store ignores If-Match / If-None-Match" are caught |
| synctest (2) | alerts `Run` (ticker, min_gap, stalled-lane page, stop); controller `awaitRelist` and resync; watermark `Reader` TTL and max-age; ingress `Drain` deadline | yes; each 50× under `-race` without a failure | — | a first min_gap test was scheduler-dependent: replicas whose tickers fire together both evaluate (waste, one loses the CAS). The test now offsets them |
| Go DST (3) | `parquetgo/internal/s3emu` (port of s3emu.rs, MD5 ETags) + `parquetgo/dst` `TestDSTEdgeCommit`: the real edge and aws-sdk-go-v2 over `httptest.NewTestServer` in a bubble; swarm fault menus; exactly once, closed logs stay closed, calls bounded with no deadline (CAST 39), progress after heal, linearizable; mutants RetryNewKey, NoHalt, a store ignoring If-None-Match, a lane waiting an hour on a hung PUT | 24 seeds | 20,000 new seeds (`edge-dst`) | the lane held a `sync.Mutex` across its S3 requests: queued callers ignored their deadlines, and the bubble's clock froze behind them. The lane's lock is now a channel (context-aware) |
| rapid machines + swarm, fuzz (4) | alerts engine machine; basis keyring machine; fuzz targets for sqlscope, hdxadapter, basis tokens, rwproxy, the aggregator's NDJSON filter and gap times, the ingress (bearer, token, body) | seeds and corpus; 100 cases per machine | 60 s per target (`fuzz`, `ci/fuzz.sh`) | basis tokens had four accepted spellings (lenient base64 of the MAC's last character; now strict); `ingress.Bearer` accepted an empty token |

Not done: hegel-go machines (rapid with hand-rolled swarm instead), the
`goroutineleak` profile (goleak covers the packages above), staticcheck,
Gobra, Antithesis (no contact made).

`-race` and synctest: the DST module (`parquetgo/dst`) runs without
`-race` in CI (`NORACE`). Under `-race` it reports races between the
bubble's goroutines and requests still unwinding in the test server, at
addresses no code shares (in one report the two accesses are at
different addresses). The other synctest tests (controller, alerts,
watermark, ingress) run under `-race`.

**gosim fork probe** (`glycerine/gosim` v0.0.2, half a day, not adopted) [M]:

- On go1.27.1 the translator gets through the standard library as far as
  `internal/runtime/atomic` ("missing function body ... Xadd"): the fork
  follows go1.26's runtime. With go1.26.8 (a `go 1.26` copy of the
  modules) it translates.
- Assembly without a pure-Go fallback stops it: `zeebo/blake3` (our
  content hash) has AVX2 kernels that `-tags=purego` does not remove
  ("missing function body ... hash_avx2 HashF"). The probe swapped in a
  pure-Go hash.
- **net/http is not translated**: every package that imports it, which
  includes aws-sdk-go-v2 and smithy-go and so `parquetgo/commit`'s
  `S3Store`, `parquetgo` and `edge`, fails to build with type mismatches
  between translated and untranslated `context`, `time`, `bufio` and
  `http.Header`. The edge with the real SDK cannot run under gosim.
- With `S3Store` removed from the package, all of `parquetgo/commit`'s
  tests pass under gosim. A crash-and-restart probe (a lane on a
  simulated machine crashed mid-append and restarted in a new epoch,
  against a store outside the machine that loses answers and holds
  requests) passes for seeds 1-5, with identical output per seed. Cold
  cost: 118 s, 885 MB of build cache.
- Conclusion: gosim could simulate crashes of the lane's protocol logic
  if the S3 client lived in its own package. The SDK path, which the
  synctest DST covers, is out of its reach. Not adopted. The gosim
  toolchain and caches were deleted.

## Sources (read 2026-09-29)

- Go: https://pkg.go.dev/testing/synctest (go1.27.1), https://go.dev/blog/testing-time,
  https://go.dev/doc/go1.26, https://go.dev/doc/go1.27, https://go.dev/doc/devel/release,
  https://pkg.go.dev/net/http/httptest#NewTestServer, https://pkg.go.dev/testing/quick
- Module versions and dates: `https://proxy.golang.org/<module>/@latest` for every module in the tables
- gosim: https://github.com/jellevandenhooff/gosim (commits), https://github.com/glycerine/gosim,
  https://pkg.go.dev/github.com/glycerine/gosim; detsim: https://github.com/arshnah/detsim
- Polar Signals: https://www.polarsignals.com/blog/posts/2024/05/28/mostly-dst-in-go
- Antithesis: https://antithesis.com/docs/, https://antithesis.com/docs/using_antithesis/sdk/go/
- porcupine: https://github.com/anishathalye/porcupine; rapid: https://github.com/flyingmutant/rapid;
  hegel-go: https://github.com/hegeldev/hegel-go (and ../PBT.md)
- Gobra: https://github.com/viperproject/gobra; Gomela: https://github.com/nicolasdilley/Gomela;
  GFuzz: https://github.com/system-pclub/GFuzz; nilaway: https://github.com/uber-go/nilaway
- failpoint: https://github.com/pingcap/failpoint; quartz: https://github.com/coder/quartz;
  benbjohnson/clock: https://github.com/benbjohnson/clock; Chaos Mesh: https://github.com/chaos-mesh/chaos-mesh/releases
- Temporal: https://docs.temporal.io/develop/go/testing-suite; P: https://p-org.github.io/P/;
  Dafny: https://github.com/dafny-lang/dafny/releases; Elle: https://github.com/jepsen-io/elle, https://github.com/pingcap/tipocket
