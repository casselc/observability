package chdbexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	execpkg "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Steps for trace validation against the Quint model.
//
// The publisher state machine's fake world observes every write the real
// publisher makes, so it can describe a run as the model's actions: the
// writer half of edgePublish.qnt (pushStart, writeTable, writeParquet,
// writeManifest with Ok / Fail / Ambiguous, rotateGen, sealGen, and crash,
// which quintgo infers from a change of process). The steps use quintgo's
// telemetry schema (spike/quintgo/qtrace: one "quint.step" span event per
// step, attributes quint.action, quint.seq, quint.arg.*, quint.obs.*, ...)
// and are written as OTLP/JSON, the format quintgo reads. That file is the
// whole interface: this package does not import quintgo, and quintgo's
// binding for the model (spike/quintgo/examples/edgepublish/binding.yaml)
// maps the steps to Quint calls.
//
// Mapping choices, where the implementation and the model differ in grain:
//   - pushStart is emitted at a push's first write, when its batch id is
//     known (from the envelope of the rows); a push that fails before any
//     write (the generation's DDL failed) is no step at all, and the model
//     keeps its payload queued, as the collector does;
//   - with no Parquet (or no tables) configured, that phase is a synthetic
//     Ok step;
//   - rotateGen(g) is emitted before the first step of a push into a
//     different generation, or before g's seal when shutdown seals it;
//   - sealGen is emitted when _sealed.json lands (Ok or Ambiguous), with
//     the batch count it states as obs.batches; a seal that never lands is
//     no step (the model has no failing seal);
//   - retention, detach, DDL and helper drops are not in the model.

type qStep struct {
	Action  string
	Actor   string
	Process string
	Outcome string
	Error   string
	Args    map[string]any
	Obs     map[string]any
}

type stepRecorder struct {
	steps   []qStep
	cur     map[string]string // actor|process -> current generation
	started map[string]bool   // process|batch
	now     func() time.Time
}

func newStepRecorder() *stepRecorder {
	return &stepRecorder{cur: map[string]string{}, started: map[string]bool{}}
}

func actorOf(signal string) string { return "edge/" + signal }

func (r *stepRecorder) add(s qStep) { r.steps = append(r.steps, s) }

func (w *fakeWorld) stepPushStart(signal, epoch string, batch uint64, gen string) {
	r := w.rec
	if r == nil {
		return
	}
	k := fmt.Sprintf("%s|%d", epoch, batch)
	if r.started[k] {
		return
	}
	r.started[k] = true
	actor := actorOf(signal)
	ck := actor + "|" + epoch
	if c := r.cur[ck]; c != "" && c != gen {
		r.add(qStep{Action: "rotateGen", Actor: actor, Process: epoch, Outcome: "ok", Args: map[string]any{"gen": c}})
	}
	r.cur[ck] = gen
	r.add(qStep{Action: "pushStart", Actor: actor, Process: epoch, Outcome: "ok",
		Args: map[string]any{"payload": w.payload, "batch": int64(batch), "gen": gen}})
}

func (w *fakeWorld) stepWrite(action, signal, epoch string, batch uint64, gen string, landed bool, err error) {
	r := w.rec
	if r == nil {
		return
	}
	s := qStep{Action: action, Actor: actorOf(signal), Process: epoch, Outcome: "ok",
		Args: map[string]any{"payload": w.payload, "batch": int64(batch), "gen": gen}}
	if err != nil {
		s.Outcome, s.Error = "error", err.Error()
		if landed {
			s.Outcome = "ambiguous"
		}
	}
	r.add(s)
}

func (w *fakeWorld) stepSeal(signal, epoch, gen string, batches uint64) {
	r := w.rec
	if r == nil {
		return
	}
	actor := actorOf(signal)
	ck := actor + "|" + epoch
	if r.cur[ck] == gen {
		r.add(qStep{Action: "rotateGen", Actor: actor, Process: epoch, Outcome: "ok", Args: map[string]any{"gen": gen}})
		r.cur[ck] = ""
	}
	r.add(qStep{Action: "sealGen", Actor: actor, Process: epoch, Outcome: "ok",
		Args: map[string]any{"gen": gen}, Obs: map[string]any{"batches": int64(batches)}})
}

// ---- OTLP/JSON -----------------------------------------------------------------

type otlpKV struct {
	Key   string         `json:"key"`
	Value map[string]any `json:"value"`
}

func kv(k string, v any) otlpKV {
	switch x := v.(type) {
	case int64:
		return otlpKV{k, map[string]any{"intValue": strconv.FormatInt(x, 10)}}
	case bool:
		return otlpKV{k, map[string]any{"boolValue": x}}
	}
	return otlpKV{k, map[string]any{"stringValue": fmt.Sprint(v)}}
}

// writeOTLP renders steps as one OTLP/JSON TracesData line: a span per step
// carrying one quint.step event, all in one thread (the machine is
// sequential), numbered by one recorder.
func writeOTLP(steps []qStep, recorder string) []byte {
	type event struct {
		TimeUnixNano string   `json:"timeUnixNano"`
		Name         string   `json:"name"`
		Attributes   []otlpKV `json:"attributes"`
	}
	type span struct {
		TraceID           string  `json:"traceId"`
		SpanID            string  `json:"spanId"`
		Name              string  `json:"name"`
		StartTimeUnixNano string  `json:"startTimeUnixNano"`
		EndTimeUnixNano   string  `json:"endTimeUnixNano"`
		Events            []event `json:"events"`
	}
	base := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC).UnixNano()
	var spans []span
	for i, s := range steps {
		attrs := []otlpKV{kv("quint.action", s.Action), kv("quint.seq", int64(i+1)), kv("quint.recorder", recorder),
			kv("quint.actor", s.Actor), kv("quint.process", s.Process), kv("quint.thread", "pbt"), kv("quint.outcome", s.Outcome)}
		if s.Error != "" {
			attrs = append(attrs, kv("quint.error", s.Error))
		}
		for _, k := range sortedMapKeys(s.Args) {
			attrs = append(attrs, kv("quint.arg."+k, s.Args[k]))
		}
		for _, k := range sortedMapKeys(s.Obs) {
			attrs = append(attrs, kv("quint.obs."+k, s.Obs[k]))
		}
		ts := strconv.FormatInt(base+int64(i)*1000, 10)
		spans = append(spans, span{TraceID: fmt.Sprintf("%032x", 1), SpanID: fmt.Sprintf("%016x", i+1), Name: "quint " + s.Action,
			StartTimeUnixNano: ts, EndTimeUnixNano: ts, Events: []event{{TimeUnixNano: ts, Name: "quint.step", Attributes: attrs}}})
	}
	td := map[string]any{"resourceSpans": []any{map[string]any{
		"resource":   map[string]any{"attributes": []otlpKV{kv("service.name", "chdbexporter-pbt")}},
		"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "pbt"}, "spans": spans}},
	}}}
	b, _ := json.Marshal(td)
	return append(b, '\n')
}

func sortedMapKeys(m map[string]any) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	for i := 1; i < len(ks); i++ {
		for j := i; j > 0 && ks[j] < ks[j-1]; j-- {
			ks[j], ks[j-1] = ks[j-1], ks[j]
		}
	}
	return ks
}

func renderSteps(steps []qStep) string {
	var b strings.Builder
	for i, s := range steps {
		fmt.Fprintf(&b, "  [%2d] %-8.8s %-13s", i, s.Process, s.Action)
		for _, k := range sortedMapKeys(s.Args) {
			fmt.Fprintf(&b, " %s=%v", k, s.Args[k])
		}
		for _, k := range sortedMapKeys(s.Obs) {
			fmt.Fprintf(&b, " obs.%s=%v", k, s.Obs[k])
		}
		if s.Outcome != "ok" {
			fmt.Fprintf(&b, " -> %s", s.Outcome)
		}
		b.WriteString("\n")
	}
	return b.String()
}

// ---- running quintgo ---------------------------------------------------------------

// quintgoDir is the quintgo checkout beside this spike.
func quintgoDir() string {
	d, _ := filepath.Abs("../../quintgo")
	return d
}

// quintgoValidate writes steps to dir as OTLP/JSON and runs `quintgo
// validate` on them with the edgePublish binding. It returns quintgo's
// report and whether every actor conformed and satisfied the binding's
// invariants. It skips the test when quint or quintgo is unavailable.
func quintgoValidate(t *testing.T, steps []qStep, dir string) (report string, ok bool) {
	t.Helper()
	if os.Getenv("PBT_QUINT") == "" {
		t.Skip("set PBT_QUINT=1 to validate traces with quintgo (needs quint and ../../quintgo)")
	}
	if _, err := execpkg.LookPath("quint"); err != nil {
		t.Skip("quint not installed")
	}
	qg := quintgoDir()
	binding := filepath.Join(qg, "examples", "edgepublish", "binding.yaml")
	if _, err := os.Stat(binding); err != nil {
		t.Skipf("no quintgo binding at %s", binding)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dir, "steps.jsonl")
	if err := os.WriteFile(f, writeOTLP(steps, "pbt"), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(os.TempDir(), "quintgo-pbt")
	build := execpkg.Command("go", "build", "-o", bin, "./cmd/quintgo")
	build.Dir = qg
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("quintgo does not build (it is in progress): %v\n%s", err, out)
	}
	backend := os.Getenv("QUINTGO_BACKEND")
	if backend == "" {
		backend = "typescript"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := execpkg.CommandContext(ctx, bin, "validate", "-binding", binding, "-backend", backend, "-dir", filepath.Join(dir, "quint"), f)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	if ee, isExit := err.(*execpkg.ExitError); isExit && ee.ExitCode() == 1 {
		return out.String(), false
	} else if err != nil {
		t.Fatalf("quintgo validate: %v\n%s", err, out.String())
	}
	return out.String(), true
}
