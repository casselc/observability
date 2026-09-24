# quintgo: model-based testing and runtime conformance for Go against Quint

Status: spike. Everything under "Verified" was run in this sandbox (Go 1.25,
Quint 0.32 with `--backend typescript`, Orchestrion v1.13.1). Anything
marked **speculative** or **unverified** was not.

The ask had two parts:

1. **MBT for Go**, like quint-llm-kit's quint-connect flow for Rust: drive Go
   code from Quint traces and compare states; possibly via comment
   annotations, code generation or Orchestrion.
2. **Runtime conformance à la PObserve**: the running Go program emits the
   model-relevant facts as OpenTelemetry span events, a reconstructor turns
   live telemetry into a model trace, and that trace is checked against the
   Quint model: that each transition is valid, and the invariants.

Two later requirements: the toolkit must be generic (edgePublish is the first
binding, not the only target), and usable as a Go library from tests, e.g.
property-based state-machine tests.

## 1. Prior art

### quint-connect and the kit (Rust)

- **Trace generation.** Quint produces traces: `quint run --mbt` (random
  simulation) or a deterministic `run` test. `--mbt` adds two fields to every
  ITF state: `mbt::actionTaken`, the innermost action that fired, and
  `mbt::nondetPicks`, a record of `Option`s holding each `nondet` value
  picked on the way. (Checked on edgePublish: `writeTable` shows up with
  picks `x`, `o`, and also the step-level picks `w` and `p` that it did not use.)
- **Driver.** `quint-connect` is a Rust crate. `#[quint_test(spec, test, main)]`
  replays a named `run`; `#[quint_run(spec)]` replays random simulations. Both
  call the Quint CLI to get ITF. The user implements a `Driver`:
  `step(&mut self, &Step)` dispatches on the action with the `switch!` macro,
  as in `switch!(step { init => self.init(), MyAction(a, b?) => self.my_action(a, b) })`,
  and a state check compares the spec state (`step.get_in(path)`, deserialised
  with `itf`/serde) with the implementation's, converted with `From`.
  `QUINT_SEED` and `QUINT_VERBOSE` control reproduction.
- **The kit.** `agentic/scripts/quint_connect/project_scaffold.py` runs
  `quint compile --flatten false`, derives Rust types from the spec's type
  declarations, and fills templates (`driver.rs`, `state.rs`,
  `transition.rs`, `types.rs`). The templates assume a Choreo-style spec,
  where the state is `s.system` per process and each transition has a
  `transition.label`. The `mbt-validator` agent then adds one transition per
  TDD cycle: run the test, see "Unimplemented action", add the label, the
  `switch!` arm and a handler that asserts every effect the spec emits, and
  stop. The rule throughout: the spec is ground truth; fix the code, not the
  test. The template pins a private quint-connect branch named
  `connect-and-observe`, a hint that Informal is working on the observation
  direction as well (**speculative**).
- **Quint's docs** list "trace validation" as the inverse of MBT: production
  data, model expectations. They have not written that page yet.

### PObserve (P language, AWS)

A log parser turns each log line into `PObserveEvent{key, timestamp, event,
logLine, customPayload}`. A **sequencer** sorts events by timestamp into one
global stream. A **demultiplexer** routes them by `key` to instances of P
spec monitors, which assert safety (and bounded liveness) over the events
they observe. PObserve runs post hoc, on logs, in test and in production. Its
monitors are hand-written P `spec` machines that watch events; they do not
replay the system model.

What quintgo keeps from it:
- partition keys: `quint.actor`;
- post-hoc checking of real telemetry.

What it changes:
- It replays the model's own transitions, so it checks membership in the
  model and the model's invariants. No separate monitor has to be written.
- Ordering does not trust wall clocks, which is PObserve's weak point:
  see §4.

## 2. Options for Go

| | Annotations + codegen (`go generate`) | **Orchestrion aspects** (`//quint:action`) | Library only (`qobs.Record`) |
|---|---|---|---|
| Code change | comment, plus generated wrappers the code must call, or a rewritten copy of the source | a comment on a function | one call per step |
| Build | extra generate step; generated files to keep in sync | `orchestrion go build` / `go test`; a plain build ignores it | none |
| Off by default | only with build tags | **yes**: without Orchestrion the comment is inert and the binary is unchanged | needs a runtime switch (`qobs.SetGlobal(nil)` makes it about 22 ns a call) |
| Granularity | function | function (statement level is possible, **unverified**, see below) | any statement |
| Timing of the step | wrapper returns | function returns (deferred advice) | wherever the call is |
| Tooling cost | a Go AST rewriter to write and maintain | none (the aspect is 70 lines of YAML/template, and already written) | none |
| Build time | small | cold ~70 s (it weaves the standard library too), warm ~2 s | none |

**Recommendation: Orchestrion for the annotations, with qobs as the library
under it.** The directive is the whole integration. It costs nothing unless
you build with Orchestrion, and it is generic: the aspect ships with qobs, not
with the project. The library path stays open for steps that are not
function-shaped, and for tests that record explicitly. A `go generate`
rewriter would be a less capable Orchestrion that we would have to maintain
ourselves.

What makes it "generated from the model" is not advice generation. The
aspect needs nothing model-specific. The model drives:
- `quintgo scaffold`: a binding skeleton, with every action's signature taken
  from `quint typecheck`;
- `quintgo lint`: the binding against the model (action names, parameter
  names, constants, invariants), and the Go annotations against the binding
  (every field a template uses must be recorded).

Rules the annotation route imposes, learned in this spike:
- **Function granularity.** A model step must be a function whose return is
  the step's linearization point. In the stand-in, the three S3 writes, the
  batch start and the rotation are small functions. In the real
  `publish.go`, `writeParquet` and `seal` already are. The table insert and
  the manifest write are inline in a closure, and the rotation is inside
  `acquire`. Orchestrion's `directive` join point also matches expressions
  inside an annotated statement, so a statement-level `//quint:step` that
  wraps a call expression looks feasible. **Unverified**: not built.
- **What an expression may use.** Advice runs deferred at return, so an
  annotation may use parameters, the receiver and named results, but not
  locals.
- **Record before spawning dependent work.** Recording at return is correct
  only if nothing that depends on the step was started inside the function.
  The stand-in's `rotate` records before `acquire` starts the background
  seal. Otherwise the seal's step could be recorded before the rotation's,
  and the model has no seal of an unrotated generation.

## 3. Telemetry schema (generic)

One span event per model step, named `quint.step`, on a short span
`quint <action>` that is a child of the span in the context. Attributes are
on the event. For the non-argument keys, readers fall back to the span's and
then the resource's attributes, so a deployment can hoist e.g.
`quint.process` into the resource.

| Attribute | Type | Meaning |
|---|---|---|
| `quint.action` | string | model action name (required) |
| `quint.seq` | int | per-recorder sequence number, **dense** (1, 2, 3, …): a hole means a lost step |
| `quint.recorder` | string | id of the recorder, the scope of `quint.seq` |
| `quint.actor` | string | partition key: which model instance the step belongs to (e.g. `edge-1/traces`) |
| `quint.process` | string | incarnation of the actor (e.g. the producer epoch); a change means the process restarted |
| `quint.thread` | string | program-order key: steps with the same thread are totally ordered by seq (default: the qobs thread in ctx, else the parent span id) |
| `quint.outcome` | string | `ok`, `error`, or an error's own classification (`QuintOutcome() string`, e.g. `ambiguous`) |
| `quint.error` | string | error text |
| `quint.arg.<name>` | string, int, bool, double | an observed parameter, in implementation terms (ids, not model values) |
| `quint.obs.<name>` | same | an observed projection of the post-state (e.g. `obs.batches`: what `_sealed.json` said) |
| `quint.order.<domain>` | int | a domain logical clock: steps of one (actor, process) that share the domain are ordered by its value |

Observations stay in implementation terms. The binding holds the
abstraction function (interning, templates), so the same telemetry can be
checked against another model or design without redeploying.

**Sampling.** Step spans must not be sampled out. qobs takes its own
TracerProvider, and the demo uses `AlwaysSample`. With parent-based sampling,
a step under an unsampled request would be lost, so give qobs a provider of
its own. A dropped export, or a BatchSpanProcessor dropping on a full queue,
shows up as a seq gap. The validator reports it as an `error` issue: the
verdict is unsound. **Not built:** carrying steps as OTel log records
instead, which pipelines usually do not sample.

## 4. Reconstruction (`qtrace.Reconstruct`)

Input: an unordered bag of steps from any number of recorders, processes and
actors (OTLP/JSON files, or in-memory SDK spans).

1. **Dedupe** on (recorder, seq): exporters retry and collectors fan out.
2. **Gap detection.** seq is dense per recorder. Any hole is an `error`
   issue for every actor that recorder reported.
3. **Partition by `quint.actor`.** Each actor is replayed as its own model
   instance, like PObserve's key. Actors that share one model instance, such
   as edgePublish's writer and consumer, need the cross-process edges below.
4. **Order** each actor's steps by a topological sort of a happens-before
   graph:
   - hard edges, **program order**: steps with the same `quint.thread`, by seq;
   - hard edges, **domain clocks**: steps with the same
     `quint.order.<d>` in one (actor, process), by value. Example: batch ids
     come from an atomic counter, so their order is the order pushes
     started, even when the `pushStart` steps were recorded in the other
     order. The seq alone cannot be a hard edge: the id allocation and the
     recording are not one atomic moment. Treating seq as a hard edge next
     to the domain clock produces cycles (there is a unit test);
   - **preference** among steps with no hard edge between them: recording
     order, i.e. seq within a recorder and time across recorders, with time
     made monotone per recorder. Such steps were concurrent. Choosing either
     order is sound when their model actions commute, and the model's
     interleaving semantics already requires that of steps on disjoint state
     (e.g. two pushes' writes). If a recording A happens-before a recording
     B in the Go memory model, then seq(A) < seq(B), so causality inside a
     process that passes through a recorder is preserved;
   - a **cycle** (contradictory keys) is reported and broken at the
     earliest step.
5. **Incarnations.** Processes are numbered in order of first appearance.
   A process change becomes the binding's `restart` action (`crash` for
   edgePublish), the step a dead process cannot report. If the steps of two
   incarnations interleave, that is an `overlap` issue: two writers alive at
   once, which the model does not allow. The round trip in §6 caught a driver
   bug this way.
6. **Missing information.**
   - A missing step: a gap, reported.
   - A missing field: the template fails, naming the step.
   - An unobserved variable: the model computes it. Replay is a deterministic
     `run`, so the model fills in every hidden variable. Here that is the
     queue, the in-flight pushes, and the consumer, which stays at init.

Across processes (**not built**): the writer–consumer link would be a hard
edge from `writeManifest` to the `claim` that read that manifest. Carry the
writer's `traceparent` in the manifest JSON and emit a span link. This is
the one ordering fact wall clocks cannot supply.

## 5. Validation (`binding` + `validate`)

The binding generates a Quint module per actor:

```quint
module observed {
  import edgePublish(PAYLOADS = Set(1, 2, 3, 4, 5), MAX_BATCH = 5, MAX_GEN = 2, ..., CRASHES = true).* from "../../model/edgePublish"
  pure def px(...) = ...                       // binding defs
  run conformsTest =
    init
    // [9] seq 10 epoch-A pushStart(batch=4, gen=g20260924T100000, payload=req-1)
    .then(pushStart(2).expect(writer.pushes.contains(px(2, 1, 4, 1, Encoded))))
    // [15] seq 16 epoch-A rotateGen(gen=g20260924T100000)
    .then(rotateGen.expect(writer.rotated.contains({ epoch: 1, gen: 1 })))
    ...
  run sealMatchesManifestsInvTest = init.expect(sealMatchesManifests).then(pushStart(1).expect(sealMatchesManifests))...
}
```

`quint test --max-samples 1 --out-itf` runs it. Each step is on its own line,
so quint's error position maps back to the observed step (the test-time
`LineStep` table). Nested `A.expect(P)` inside `.then(...)` pinpoints the step
precisely. A top-level `.expect` would only point at the whole chain. Verdicts:

- **disabled** (QNT513, or QNT508 "Cannot continue to expect"): the model
  cannot take the observed step from the state it reached. The
  implementation did something the design does not allow.
- **state** (QNT508 in `conformsTest`): the step was possible, but the
  observed projection (`expect`) does not hold. The implementation's choice
  (a batch id, a generation, a seal's count) differs from the model's.
- **invariant**: the trace conforms, but a model invariant fails on it. This
  is a behaviour the design allows and the design's own properties reject.
  It is the model's known bug, happening in the implementation.
- **issues** from reconstruction (gaps, cycles, overlaps) make a pass
  unsound.

A conforming run's ITF has the module prefixes stripped, and
`mbt::actionTaken` and `mbt::observedSeq` added, so the model's `trace.py`
and the ITF viewer read it.

**Binding conventions**: what a model needs to be checkable this way.
- Actions parameterised by values that can be observed or derived. A value
  the model picks internally, such as the batch id in `pushStart(p)`, is
  checked with `expect` instead.
- Opaque ids need an abstraction function. `intern` renames them 1, 2, … by
  first appearance, per actor or per process, which covers ids that name
  things in creation order.
- One parameterless restart action, if processes can restart.
- A non-`ok` outcome must mean something: `outcomes` maps it to a model value
  used as `{{.outcome}}`, or to `skip` or `call` per action. If a failed step
  hits an action whose templates ignore `.outcome`, generation stops, so a
  failure is never replayed as a success. This rule came from the round
  trip: a failed seal was being replayed as a seal.
- Constants sized from the trace (`set`, `max`, `procs`), because model bounds
  like `MAX_BATCH` are small.

**Limits of the approach**:
- **Hidden nondeterminism.** The writer cannot tell `Fail` from `Ambiguous`,
  and only a test double knows. With a plain error mapped to `Fail`,
  `ambiguous-seal` would replay as a failed manifest write, and the seal
  undercount would be invisible. It is real in S3, missing in the replay.
  - A deterministic `run` cannot branch. The sound treatment is to replay the
    error as `any { writeManifest(x, Fail), writeManifest(x, Ambiguous) }`
    and ask an exhaustive checker whether some resolution matches every
    later observation, and whether all resolutions satisfy the invariants.
  - That is Apalache: a step-indexed encoding with an `obsIdx` variable, the
    TLA+ trace-validation technique of Cirstea, Kuppe, Merz et al.
    **Not built.**
  - The cheaper route is to observe more. An S3 listing or bucket
    notifications become a second recorder whose steps pin the outcome.
- **Soundness is relative to the binding.** The abstraction function is
  trusted. A wrong template can hide a bug or invent one.
- **Checks are per actor, bounded by the trace, post hoc.** Liveness is not
  checked. Long traces need chunking: validate in windows, starting each
  from the ITF state reached by the previous window (**not built**). The
  typescript backend took 4–8 s per trace of ~30 steps, most of it Node
  start-up.

## 6. Capability 1: driving Go from Quint traces (`connect`)

`connect.Generate` runs `quint run --mbt`. `itf` decodes the result into a
navigable `Value` tree, and `connect.Driver{Init, Actions, Skip, Check}`
replays it. This is the Go counterpart of quint-connect's `Driver` and
`switch!`, with a map standing in for the macro.

The per-project part is the handlers and the state check. For edgePublish
(`examples/edgepublish/mbt`):
- **Scheduling through the effect seam.** Every store write blocks in a gate
  until the handler for that model step releases it with the model's
  outcome (`Ok`/`Fail`/`Ambiguous`). That reproduces the model's
  interleaving of concurrent pushes, S3 faults, rotations and background
  seals exactly, in real goroutines. `crash` releases everything in flight
  as `Fail` and starts a new incarnation.
- **State check.** After every step, the store's table rows, Parquet
  objects, manifests and seals (with batch counts) are compared with the
  model's `tableRows`, `parquet`, `manifests` and `seals`, via learned maps
  from implementation generation ids to model generations.
- **Skipped actions.** The consumer's actions have no implementation here.

Verified: 5 random traces of 30 steps replay against the faithful publisher,
and the manifest-first mutant fails on all 5. Random traces from the full
`step` spend most of their steps in `otherInsert` and consumer actions. A
writer-focused step in the model, or `--step` with a projection, would give
better coverage per trace (roadmap).

**Round trip.** `TestRoundTrip`, under `orchestrion go test`, closes the
loop: model trace → Go → recorded steps → `Checker.Check` → the same model.
5/5 validate. On its first run it found two real problems:
- a driver race: the dead incarnation's seal was recorded after the restart,
  which the reconstructor flagged as an overlap;
- the failed-step-replayed-as-success hole in the binding semantics.

## 7. edgePublish mapping

| Model action | stand-in (`publisher/publish.go`) | real `chdbexporter/publish.go` | observed args | notes |
|---|---|---|---|---|
| `pushStart(p)` | `begin` (acquire + batch id) | `push`: after `acquire` and `nextBatch` | batch, gen, epoch; payload **not available** | order domain `batch` |
| `writeTable(x, o)` | `insertTable` | `s.Insert(g.ts.insert, …)`, inline in `withConn` | outcome ok/error | ambiguity not visible to the writer |
| `writeParquet(x, o)` | `writeParquet` | `p.writeParquet(s, signal, gen, batch, rb)`, already a function | same | annotatable as is |
| `writeManifest(x, o)` | `commit` | `p.writeObject(s, manifestPath…)`, inline; `writeObject` also writes seals | same | needs an extraction, or `thread:`/statement-level |
| `rotateGen` | `rotate` | `acquire`, `st.cur = g` under `mu.Lock` when `old != nil` | gen | record before the seal goroutine starts |
| `sealGen(gk)` | `seal` | `seal(signal, g)`, already a function | gen, `obs.batches` = `g.batches` | `close()` seals without rotating: emit `rotateGen` first |
| `crash` | inferred | inferred from a `producer_epoch` change | none | |
| consumer actions | none | not in the exporter | none | a separate recorder in the consumer (roadmap) |

**Observable vs abstracted**
- Observed: batch ids, generation ids, epochs, write outcomes (as the writer
  sees them), and the seal's batch count.
- Abstracted: generation ids and payload names (interned); epochs (numbered
  by appearance).
- Not observable by the writer:
  - whether a failed write landed;
  - payload identity across retries. The collector's queue does not expose an
    item id to the exporter. The options are a content hash of the pdata
    (CPU cost), an upstream change to exporterhelper, or dropping payload
    identity, which gives up the retry-duplication properties;
  - the queue itself (the model computes it);
  - the consumer.

**Model/implementation gaps the spike exposed**
- The model rotates on its own. `publish.go` rotates only when a push
  arrives, so an idle generation stays open until `Close`. The model is
  more permissive, which is fine for replay. MBT needed a `Tick` on the
  stand-in.
- `close()` seals the current generation without a rotation. The model can
  only seal rotated generations, so `Close` must be recorded as `rotateGen`
  followed by `sealGen`, which needs `MAX_GEN = observed + 1`.
- Configurations without `store_tables` or Parquet skip writes that the
  model always performs. They need a model flag, or a binding that emits the
  skipped writes as no-ops (neither built).
- The model's `nextBatch` starts at 1 per epoch, as do `publish.go`'s
  per-namespace counters. That coincidence lets batch ids pass through
  uninterned.

**Applying it to the real exporter** (not done, per the brief: no changes to
`chdbexporter`), in about five small edits:
1. Extract `insertTable`, `commitManifest` and `rotate` as functions.
2. Annotate those three and the existing `writeParquet` and `seal`,
   with `thread:env.batch` (no ctx is threaded through `push`).
3. Decide on payload identity.
4. Add `orchestrion.tool.go`.
5. Build the collector with `orchestrion go build`.

The stand-in's binding should then apply unchanged.

## 8. Roadmap

1. **Now (this spike):**
   - generic core: schema, recorder, OTLP I/O, reconstructor, binding with
     lint and scaffold, `quint test`-based validator, ITF, connect;
   - Orchestrion aspects;
   - the edgePublish binding;
   - demo scenarios, MBT and round-trip tests.
2. **Real exporter:** the five edits above, behind the Orchestrion build
   only. Run the collector's own tests woven, and validate their telemetry
   (`Checker` from `go test`). PBT/state-machine tests can feed
   `Checker.Check` directly.
3. **Hidden nondeterminism:** an Apalache mode (step-indexed module; error
   outcomes as `any {…}`), or S3 observation as a second recorder.
4. **Consumer side:**
   - a recorder in the central consumer;
   - `traceparent` in manifests, so each claim is linked to its manifest write;
   - multi-actor merge into one model instance.
5. **Live operation:**
   - a collector processor or connector that tails `quint.step` events;
   - validation in windows, seeded from the last conforming state;
   - alerting on disabled, state or invariant verdicts and on gaps.
   Budget ~7 µs per step (measured) on the hot path. A push is 4 steps,
   against S3 PUTs measured in milliseconds.
6. **Tooling:**
   - statement-level `//quint:step`;
   - `quintgo lint` also checking annotation expression types against
     `quint typecheck` parameter types;
   - a writer-only `step` in edgePublish for denser MBT traces;
   - the Rust evaluator backend for speed.
