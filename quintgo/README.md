# quintgo: check Go programs against Quint models

A spike that does two things for Go, with edgePublish as the first binding:

1. **Model-based testing.** Drive Go code from Quint traces, the way
   quint-connect does for Rust (`connect`, `itf`).
2. **Runtime conformance, à la PObserve.** Go code emits model steps as
   OpenTelemetry span events. quintgo rebuilds a model trace from the
   telemetry and checks it against the Quint model: that each transition is
   valid, the observed state after each step, and the invariants (`qobs`,
   `otelio`, `qtrace`, `binding`, `validate`).

The research, the design choices and their limits are in
[DESIGN.md](DESIGN.md). This file covers how to run it and how to use it.

```
quintgo/                      generic core (module github.com/chucklehead-dev/oscope/spike/quintgo)
  qtrace/     telemetry schema, Step, Reconstruct (ordering, gaps, restarts)
  qobs/       runtime recorder: Record, Thread, MemorySink, OTelSink; orchestrion.yml (//quint:action, //quint:thread)
  otelio/     OTLP/JSON file exporter + reader; SDK spans -> steps
  binding/    YAML binding: model signatures from `quint typecheck`, lint, annotation scan, Quint module generation
  validate/   Checker / Validate: run `quint test` on the generated module, structured Result
  itf/        ITF decoder;  connect/  MBT driver runtime (quint run --mbt -> handlers)
  cmd/quintgo CLI: scaffold | lint | steps | validate
  examples/edgepublish/       first binding (its own module; depends on Orchestrion)
    binding.yaml              the whole edgePublish mapping: no Go code
    publisher/                stand-in for chdbexporter/publish.go, annotated with //quint:action
    cmd/demo/                 scenarios -> OTLP/JSON spans
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
QUINTGO_BACKEND=typescript OUT=/tmp/quintgo-demo ./run-demo.sh      # all five scenarios, ~2 min cold
QUINTGO_BACKEND=typescript go test ./mbt/ -v                         # capability 1: model traces drive Go
QUINTGO_BACKEND=typescript go run github.com/DataDog/orchestrion go test ./mbt/ -run RoundTrip -v
```

Step by step, what `run-demo.sh` does:

```sh
(cd ../.. && go build -o /tmp/q/quintgo ./cmd/quintgo)
go run github.com/DataDog/orchestrion go build -o /tmp/q/demo ./cmd/demo   # weaves //quint:action
/tmp/q/quintgo lint -binding binding.yaml -src .
/tmp/q/demo -scenario happy -out /tmp/q/happy.jsonl                        # OTLP/JSON, as the collector's file exporter writes
/tmp/q/quintgo steps /tmp/q/happy.jsonl                                    # the reconstructed order
/tmp/q/quintgo validate -binding binding.yaml -backend typescript -dir /tmp/q/happy /tmp/q/happy.jsonl
python3 ../../../otel-chdb/model/trace.py /tmp/q/happy/observed_edge_1_traces_conformsTest.itf.json
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
| `connect.Generate{Spec, Main, Traces, MaxSteps, Seed}.Run(ctx)`, `itf.ReadFile`, `connect.Driver{Init, Actions, Skip, Check}.Replay(trace)` | MBT: Quint traces drive Go |

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
6. **Emit:** build with `orchestrion go build`. Install a recorder at startup
   with `qobs.SetGlobal(qobs.NewRecorder(qobs.NewOTelSink(tp)))`, on a
   TracerProvider that never samples these spans out.
7. **Validate:** `quintgo validate -binding binding.yaml spans.jsonl`, on the
   collector's file exporter output, or `validate.Checker` in tests.
8. **Optional MBT:** write `connect.Driver` handlers, and put a gate at the
   effect seam so the model can schedule the implementation.

## Limitations (details in DESIGN.md §5)

- Hidden nondeterminism is not handled. A writer cannot tell a failed write
  from an ambiguous one; only the test double can. An Apalache mode, or S3
  observation, is roadmap.
- Payload identity across retries is not available in the real exporter.
- Validation is per actor, post hoc and bounded by trace length. There is no
  liveness checking and no windowing yet.
- Measured cost per recorded step (4 vCPU VM, `go test ./otelio -bench .`):
  - recording off: ~22 ns (argument boxing);
  - MemorySink: ~1.5–2.9 µs;
  - OTel batch span processor to OTLP/JSON: ~7.1 µs, 37 allocations.
  Orchestrion's cold build takes ~70 s. Validation with the typescript
  backend takes 4–8 s per trace.
- The example validates a stand-in of `publish.go` (same control flow, store
  behind an interface), not the real exporter, which was not modified.
