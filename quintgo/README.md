# quintgo: check Go programs against Quint models

A spike that does two things for Go, with edgePublish as the first binding:

1. **Model-based testing.** Drive Go code from Quint traces, the way
   quint-connect does for Rust (`connect`, `itf`).
2. **Runtime conformance, à la PObserve.** Go code emits model steps.
   quintgo rebuilds a model trace and checks it against the Quint model:
   that each transition is valid, the observed state after each step, and
   the invariants (`qobs`, `otelio`, `qtrace`, `binding`, `validate`).
   The steps can travel three ways (see "Which trace path when"):
   - as OpenTelemetry span events;
   - as a native step log, with no OTel;
   - as a model-level ITF trace written by the app itself, in the same
     format `quint run --mbt` produces.

   `qtest.RecordAndCheck` does record-then-check inside a Go test.

The research, the design choices and their limits are in
[DESIGN.md](DESIGN.md). This file covers how to run it and how to use it.

```
quintgo/                      generic core (module github.com/chucklehead-dev/oscope/spike/quintgo)
  qtrace/     telemetry schema, Step, Reconstruct (ordering, gaps, restarts); native step log ReadJSONL/WriteJSONL
  qobs/       runtime recorder: Record, Thread; sinks MemorySink, JSONLSink/FileSink, OTelSink, Tee; orchestrion.yml
  otelio/     OTLP/JSON file exporter + reader; SDK spans -> steps
  load/       reads any input, detecting the format: OTLP/JSON, step log or ITF
  binding/    YAML binding: model signatures from `quint typecheck`, lint, annotation scan, Plan (steps -> model calls),
              Quint module generation, ObservedITF/StepsToITF (model-level ITF in-process)
  validate/   Checker / Validate (steps), ValidateITF (model-level ITF): run `quint test`, structured Result
  qtest/      Record / RecordAndCheck / Check for Go tests
  itf/        ITF decoder and encoder, Quint literal <-> ITF value;  connect/  MBT driver runtime (--mbt ITF -> handlers)
  cmd/quintgo CLI: scaffold | lint | steps | itf | validate
  examples/edgepublish/       first binding (its own module; depends on Orchestrion)
    binding.yaml              the whole edgePublish mapping: no Go code
    publisher/                stand-in for chdbexporter/publish.go, annotated with //quint:action
    scenarios/                the demo scenarios, plus a test checking every trace path agrees
    cmd/demo/                 a scenario -> OTLP/JSON spans, native step log, model-level ITF
    mbt/                      model traces drive the publisher; round trip back through the validator
    run-demo.sh
```

Requirements: Go 1.25 (the toolchain is fetched automatically), and
`quint` 0.32 on PATH. `quint test`/`quint run` default to the Rust backend.
Where it can't be downloaded, as in this sandbox, set
`QUINTGO_BACKEND=typescript` or pass `-backend typescript`. Orchestrion is
pinned in the example's `go.mod` and run with `go run`, so there is nothing
to install.

## Reproduce

```sh
cd spike/quintgo
go test ./...                                   # core: ordering, OTLP round trip, validator on a toy model
cd examples/edgepublish
QUINTGO_BACKEND=typescript OUT=/tmp/quintgo-demo ./run-demo.sh      # 5 scenarios x 3 trace paths, ~4 min cold
QUINTGO_BACKEND=typescript go test ./mbt/ -v                         # capability 1: model traces drive Go
QUINTGO_BACKEND=typescript go run github.com/DataDog/orchestrion go test ./mbt/ -run RoundTrip -v
QUINTGO_BACKEND=typescript go run github.com/DataDog/orchestrion go test ./scenarios/ -v   # direct emission, both checks, replay
```

Step by step, what `run-demo.sh` does:

```sh
(cd ../.. && go build -o /tmp/q/quintgo ./cmd/quintgo)
go run github.com/DataDog/orchestrion go build -o /tmp/q/demo ./cmd/demo   # weaves //quint:action
/tmp/q/quintgo lint -binding binding.yaml -src .
/tmp/q/demo -scenario happy -out /tmp/q/spans.jsonl -steps /tmp/q/steps.jsonl -itf /tmp/q/itf -binding binding.yaml
/tmp/q/quintgo steps /tmp/q/steps.jsonl                                    # the reconstructed order (any format)
/tmp/q/quintgo validate -binding binding.yaml -backend typescript /tmp/q/spans.jsonl   # OTel path
/tmp/q/quintgo validate -binding binding.yaml -backend typescript /tmp/q/steps.jsonl   # native step log
/tmp/q/quintgo validate -backend typescript /tmp/q/itf/edge-1_traces.itf.json          # model-level ITF: no binding needed
/tmp/q/quintgo itf -binding binding.yaml -out /tmp/q/itf2 /tmp/q/spans.jsonl           # any recorded steps -> model-level ITF
python3 ../../../otel-chdb/model/trace.py <the itf: path a validate run prints>
```

Scenarios and actual results (`validate` exits 0 or 1):

| scenario | what happens | result |
|---|---|---|
| `happy` | 3 concurrent pushes (steps interleave), a failed Parquet write and its retry, rotation, background seal, crash, new epoch, close | **PASS**, 28 steps (27 observed + inferred `crash`) |
| `rotation-race` | a push is held in its table insert while the clock moves on; the next push must wait for rotation | **PASS** |
| `ambiguous-seal` | a manifest PUT lands but reports failure; the retry commits again; the seal counts 1 of 2 | conforms, but `sealMatchesManifests` fails after step [15] and `committedNoDuplicatePayload` after step [7]: the model's known finding F1/F3, now seen in running code |
| `manifest-first` | MUTANT: the manifest is written before the table insert | **FAIL** at step [1]: `writeManifest(px(1, 1, 1, 1, ParquetDone), Ok)` is not a transition (QNT513) |
| `no-lock` | MUTANT: the push drops the shared lock before its writes | **FAIL** at step [1]: `rotateGen` is not enabled while a push is in flight; `quintgo steps` shows batch 1 committed into generation 1 after its `sealGen` |

```
=== scenario manifest-first
actor edge-1/traces: 6 steps
  conformance: FAIL
    step [1] is not a transition of the model from the state reached
      quint:    writeManifest(px(1, 1, 1, 1, ParquetDone), Ok)
      observed: seq 2 writeManifest(batch=1, gen=g20260924T100000, payload=req-1) (span efdc40821aced020)
      Error [QNT513]: Cannot continue in `then` because the highlighted expression evaluated to false
```

Each scenario, each of the three paths (`run-demo.sh` output, typescript backend):

```
=== scenario happy
  otel      OTLP/JSON spans              exit 0  conformance: PASS;invariant commitImpliesData: PASS;invariant sealMatchesManifests: PASS;invariant committedNoDuplicatePayload: PASS
  steps     native step log              exit 0  conformance: PASS;invariant commitImpliesData: PASS;invariant sealMatchesManifests: PASS;invariant committedNoDuplicatePayload: PASS
  itf       model-level ITF              exit 0  conformance: PASS;invariant commitImpliesData: PASS;invariant sealMatchesManifests: PASS;invariant committedNoDuplicatePayload: PASS
=== scenario ambiguous-seal
  otel      OTLP/JSON spans              exit 1  conformance: PASS;...;invariant sealMatchesManifests: FAIL;violated after step [15];invariant committedNoDuplicatePayload: FAIL;violated after step [7]
  steps     native step log              exit 1  conformance: PASS;...;invariant sealMatchesManifests: FAIL;violated after step [15];invariant committedNoDuplicatePayload: FAIL;violated after step [7]
  itf       model-level ITF              exit 1  conformance: PASS;...;invariant sealMatchesManifests: FAIL;violated after step [15];invariant committedNoDuplicatePayload: FAIL;violated after step [7]
=== scenario manifest-first
  otel      OTLP/JSON spans              exit 1  conformance: FAIL;step [1] disabled
  steps     native step log              exit 1  conformance: FAIL;step [1] disabled
  itf       model-level ITF              exit 1  conformance: FAIL;step [1] disabled
```

(`rotation-race` passes on all three; `no-lock` fails at step [1] on all three.)

## Which trace path when

| | OTel spans | Native step log | Model-level ITF |
|---|---|---|---|
| What the app writes | `quint.step` span events (`qobs.OTelSink`) | one JSON object per step, the same keys (`qobs.JSONLSink` / `FileSink`) | the binding applied in-process: `mbt::actionTaken`, `mbt::nondetPicks`, `quintgo::expect`, `obs::*` (`binding.StepsToITF`, `demo -itf`, `qtest`) |
| Needs | an OTel SDK and pipeline; spans never sampled out | a writable file or `io.Writer` | the binding file, and argument templates that render to Quint literals |
| Terms | implementation (ids, errors) | implementation | model (interned ids, model values) |
| Check with | `quintgo validate -binding b.yaml spans.jsonl` | `quintgo validate -binding b.yaml steps.jsonl` | `quintgo validate trace.itf.json` (the trace names its model; no binding) |
| Rebind later? | yes: same telemetry, another binding or design | yes | no: the abstraction is baked in |
| Use for | production and staging: live telemetry, cross-process, alongside the app's own traces | tests, simulations, CI, deterministic replays, anything without OTel | handing traces to Quint tooling (ITF viewer, `trace.py`); comparing with `quint run --mbt` traces; archiving a checked trace; driving the implementation again through `connect` |

All three give the same verdicts on the five scenarios, at the same step
(`scenarios.TestScenarios` checks this). A validated run also writes an ITF
with the model's full state at every step and `mbt::nondetPicks`, i.e. the
shape of `quint run --mbt`. `connect.Driver` replays such a trace on the
implementation: `scenarios.TestScenarios` replays each observed conforming
trace through the MBT driver. `validate.ValidateITF` also takes plain `quint
run --mbt` output (`quintgo validate -spec M.qnt -module M -instance
currentDesign trace.itf.json`) and compares every model variable in the
trace with the model's state after each step.

## Using it as a library (from Go tests)

```go
import (
    "github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
    "github.com/chucklehead-dev/oscope/spike/quintgo/validate"
)

// 1. Record: in memory (no OTel), or through OTel with qobs.NewOTelSink(tp).
sink := &qobs.MemorySink{}
qobs.SetGlobal(qobs.NewRecorder(sink))   // what woven //quint:action advice calls
defer qobs.SetGlobal(nil)
// ... exercise the code; or record explicitly:
//     rec.Record(ctx, "withdraw", err, "amount", 20, "obs.balance", 30)

// 2. Check: typechecks the model, lints the binding, then per actor
//    reconstruct -> generate -> quint test.
c, err := validate.NewChecker("binding.yaml", validate.Options{Backend: "typescript", Dir: t.TempDir()})
rep, err := c.Check(ctx, sink.Steps())
if !rep.OK() { t.Fatal(rep.Summary()) }
```

Entry points:

| | |
|---|---|
| `qobs.NewRecorder(sink, WithActor, WithProcess)`, `SetGlobal`, `Record(ctx, action, err, kv...)`, `Thread(ctx)` | record steps |
| `qobs.MemorySink`, `qobs.NewOTelSink(tp)`, `qobs.Tee` | where steps go |
| `otelio.NewFileExporter(w)` | SpanExporter writing OTLP/JSON lines |
| `otelio.ReadSteps(r)`, `otelio.StepsFromSpans(tracetest.SpanRecorder.Ended())` | telemetry -> `[]qtrace.Step` |
| `qtrace.Reconstruct(steps) ([]*qtrace.Trace, []qtrace.Issue)` | ordered per-actor traces |
| `validate.NewChecker(binding, opts)`, `(*Checker).Check(ctx, steps) (*Report, error)` | reconstruct + validate |
| `validate.Validate(ctx, binding, model, trace, opts) (*Result, error)` | one actor's trace |
| `Result{Conforms, Failure{Kind, Index, Call, Step}, Invariants, Issues, Module, ITF}` | structured verdict |
| `connect.Generate{Spec, Main, Traces, MaxSteps, Seed}.Run(ctx)`, `itf.ReadFile`, `connect.Driver{Init, Actions, Skip, Check}.Replay(trace)` | MBT: Quint traces (or observed, validated ones) drive Go |
| `qobs.NewJSONLSink(w)`, `qobs.FileSink(path)` | native step log, no OTel |
| `qtrace.ReadJSONL(r)`, `qtrace.WriteJSONL(w, steps)`, `(*Step).Attributes()` | step log I/O |
| `load.File(path, "auto")`, `load.Sniff(data)` | read OTLP/JSON, a step log or ITF, detected by content |
| `(*binding.Binding).StepsToITF(steps)`, `.ObservedITF(trace)`, `.Plan(model or nil, trace)` | the binding applied in-process: a model-level ITF per actor |
| `validate.ValidateITF(ctx, trace, ITFOptions{Binding, Spec, Module, Instance, NoVarCheck})` | replay a model-level ITF: picks as arguments, `quintgo::expect` and model variables checked per step |
| `qtest.RecordAndCheck(t, binding, scenario, opts...)`, `qtest.Record`, `qtest.Check` | record then check in a test |
| `itf.ParseQuint(expr)`, `Value.Quint()`, `json.Marshal(Value)`, `itf.Some` | Quint literal <-> ITF value |

Record then check, with no OTel and no files to manage (`qtest`):

```go
func TestScenario(t *testing.T) {
    qtest.RecordAndCheck(t, "binding.yaml", func(rec *qobs.Recorder) {
        runScenario()                                                    // woven //quint:action steps go to rec
        rec.Record(ctx, "deposit", nil, "amount", 5, "obs.balance", 35)  // or record explicitly
    }, qtest.WithOptions(validate.Options{Backend: "typescript"}))
}
```

It installs the recorder as the global one for the scenario. It fails the
test with the offending step and invariant, and it leaves a step log, a
model-level ITF per actor, and the generated modules in `t.TempDir()`
(`$QUINTGO_KEEP/<test>` keeps them). Other forms:
- `qtest.Record` returns the `Run` without failing, for expected violations;
- `qtest.Check(t, binding, steps)` checks steps recorded elsewhere.

These fit property-based and state-machine testing both ways:
- feed a PBT run's recorded steps to `Check`;
- seed command sequences from `quint run --mbt` ITF through `connect`/`itf`.

`validate.TestAccountModel` does the first on a model unrelated to
edgePublish. `examples/edgepublish/mbt` does the second, then the first.

## The annotation

```go
//quint:action action:writeTable actor:p.actor() process:p.epoch payload:b.payload batch:b.id gen:b.gen.id
func (p *Publisher) insertTable(ctx context.Context, b *batch, rows []byte) error { ... }
```

- Each `key:<Go expression>` is evaluated when the function returns. It may
  use the parameters, the receiver and named results (not locals). Quote
  values with spaces: `key:"f(a, b)"`.
- The function's final `error` result is the outcome. An error can classify
  itself with `QuintOutcome() string`, as `ambiguous` in the demo store does.
- Special keys:
  - `when:ok`/`when:error`: record only on success or failure; the
    expressions are not evaluated otherwise;
  - `actor`, `process`, `thread`, `outcome`: override the step's identity;
  - `obs.<name>`: an observed state projection;
  - `order.<domain>`: a domain clock.
- `//quint:thread` on a function with a `context.Context` parameter puts the
  steps recorded under that context in program order.
- A plain `go build` ignores both directives.

## Applying this to a new project

Per project:
- the annotations;
- one binding YAML;
- for MBT only, the driver handlers.

Everything else is generic.

1. **Model conventions.** A parameterised module; actions whose parameters
   can be observed or derived; one parameterless restart action if
   processes can restart; invariants as `val`s. Values the model picks
   itself are checked with `expect`, not passed as arguments.
2. **Scaffold the binding:**
   `quintgo scaffold -spec model.qnt -module M > binding.yaml`. It lists every
   constant, and every action with its parameter names and types.
3. **Fill it in:**
   - `constants`: Quint expressions, which may use `set "arg"`, `max "arg"`,
     `procs`, `add`, `atLeast`;
   - `intern`: opaque ids, renamed 1, 2, …, scoped per actor or per process;
   - `outcomes`: `ok`/`error`/custom mapped to Quint values; per action,
     `skip` or `call`;
   - `restart`;
   - `defs`: Quint helpers;
   - per action, `args` templates over the recorded fields (`{{.batch}}`,
     `{{.process}}`, `{{.outcome}}`, `{{.obs.x}}`) and an optional `expect`;
   - `invariants`.
4. **Annotate** one function per model step. If a step is inline, extract it
   into a function, making its return the moment the step takes effect. Add
   `orchestrion.tool.go` importing `github.com/DataDog/orchestrion` and
   `.../quintgo/qobs`.
5. **Lint:** `quintgo lint -binding binding.yaml -src ./your/pkg`. This checks
   the binding against `quint typecheck`'s view of the model, and that every
   field a template uses is recorded by the annotation.
6. **Emit.** Build with `orchestrion go build`, then install a recorder at
   startup. Pick the sink by where the trace should go (see "Which trace
   path when"):
   - `qobs.NewOTelSink(tp)`, on a TracerProvider that never samples these
     spans out;
   - `qobs.FileSink("steps.jsonl")` for a native step log;
   - in tests, `qtest.RecordAndCheck`, which also writes the model-level ITF.

   For ITF emitted in-process, write argument templates as Quint literals:
   records, `Set(...)` and constructors, not calls of model operators.
   `expect` may use any Quint.
7. **Validate.** Run `quintgo validate [-binding binding.yaml] FILE...`, where
   FILE is detected by content. In tests, use `validate.Checker`,
   `validate.ValidateITF` or `qtest`.
8. **Optional MBT:** write `connect.Driver` handlers, and put a gate at the
   effect seam so the model can schedule the implementation.

## Limitations (details in DESIGN.md §5)

- Hidden nondeterminism is not handled. A writer cannot tell a failed write
  from an ambiguous one; only the test double can. An Apalache mode, or S3
  observation, is roadmap.
- Payload identity across retries is not available in the real exporter.
- Validation is per actor, post hoc and bounded by trace length. There is no
  liveness checking and no windowing yet.
- Two limits of the model-level ITF:
  - its argument templates must render to Quint literals, so operator calls
    cannot be arguments there (they still work on the other paths);
  - it names its model by absolute path in `#meta.quintgo.spec`. Pass
    `-spec` to validate it on another machine.
- `-instance` expects an instance module shaped as a single
  `import M(...).*`; its constants are read from the source text.
- Measured cost per recorded step (4 vCPU VM, `go test ./otelio -bench .`):
  - recording off: ~22 ns (argument boxing);
  - MemorySink: ~1.5–2.9 µs;
  - OTel batch span processor to OTLP/JSON: ~7.1–8.9 µs, 37 allocations;
  - native step log (`JSONLSink`): ~7–7.5 µs, 39 allocations (reflection-based JSON).
  Orchestrion's cold build takes ~70 s. Validation with the typescript
  backend takes 4–8 s per trace.
- The example validates a stand-in of `publish.go` (same control flow, store
  behind an interface), not the real exporter, which was not modified.
