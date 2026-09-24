// Package qtest records a scenario's model steps and checks them against a
// Quint model inside a Go test: no OTel, no files to manage.
//
//	func TestWithdraw(t *testing.T) {
//		qtest.RecordAndCheck(t, "binding.yaml", func(rec *qobs.Recorder) {
//			acct.Withdraw(ctx, 20) // woven //quint:action steps go to rec too
//			rec.Record(ctx, "deposit", nil, "amount", 5, "obs.balance", 35)
//		})
//	}
//
// The recorder is installed as the global one for the scenario's duration,
// so annotated code built with `orchestrion go test` records into it.
// Every run leaves a native step log, a model-level ITF per actor, and the
// generated Quint modules in the run's directory (t.TempDir(), or
// $QUINTGO_KEEP/<test name> to keep them).
package qtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
	"github.com/chucklehead-dev/oscope/spike/quintgo/validate"
)

// Config of a recorded run.
type Config struct {
	Options     validate.Options // quint binary, backend (default $QUINTGO_BACKEND, else rust)
	Dir         string           // artifacts; default $QUINTGO_KEEP/<test> or t.TempDir()
	Recorder    []qobs.Option    // e.g. qobs.WithActor("acct-1")
	SkipIfEmpty bool             // skip (not fail) when nothing was recorded, e.g. an unwoven build
}

// Option changes a Config.
type Option func(*Config)

func WithOptions(o validate.Options) Option { return func(c *Config) { c.Options = o } }
func KeepIn(dir string) Option              { return func(c *Config) { c.Dir = dir } }
func RecorderOptions(opts ...qobs.Option) Option {
	return func(c *Config) { c.Recorder = append(c.Recorder, opts...) }
}
func SkipIfEmpty() Option { return func(c *Config) { c.SkipIfEmpty = true } }

// Run is the outcome of a recorded run.
type Run struct {
	Steps   []qtrace.Step
	StepLog string            // native step log (JSONL)
	ITF     map[string]string // actor -> model-level ITF written in-process
	Report  *validate.Report
}

// OK reports whether every actor conformed and satisfied the invariants.
func (r *Run) OK() bool { return r.Report != nil && r.Report.OK() }

var checkers sync.Map // binding path + options -> *validate.Checker

func checker(t testing.TB, bindingPath string, o validate.Options) *validate.Checker {
	t.Helper()
	abs, _ := filepath.Abs(bindingPath)
	key := fmt.Sprintf("%s|%s|%s", abs, o.Quint, o.Backend)
	if c, ok := checkers.Load(key); ok {
		return c.(*validate.Checker)
	}
	c, err := validate.NewChecker(abs, o) // typechecks the model and lints the binding once
	if err != nil {
		t.Fatalf("quintgo: %v", err)
	}
	checkers.Store(key, c)
	return c
}

func config(t testing.TB, opts []Option) Config {
	var c Config
	for _, o := range opts {
		o(&c)
	}
	if c.Dir == "" {
		if keep := os.Getenv("QUINTGO_KEEP"); keep != "" {
			c.Dir = filepath.Join(keep, strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
		} else {
			c.Dir = t.TempDir()
		}
	}
	return c
}

// Record runs scenario with a fresh recorder (also installed as the global
// one), then validates what it recorded. It does not fail the test on a
// violation: inspect the returned Run. It fails on tool errors.
func Record(t testing.TB, bindingPath string, scenario func(rec *qobs.Recorder), opts ...Option) *Run {
	t.Helper()
	c := config(t, opts)
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run := &Run{StepLog: filepath.Join(c.Dir, "steps.jsonl"), ITF: map[string]string{}}
	file, err := qobs.FileSink(run.StepLog)
	if err != nil {
		t.Fatal(err)
	}
	mem := &qobs.MemorySink{}
	rec := qobs.NewRecorder(qobs.Tee{mem, file}, c.Recorder...)
	prev := qobs.Global()
	qobs.SetGlobal(rec)
	func() {
		defer qobs.SetGlobal(prev)
		scenario(rec)
	}()
	if err := file.Close(); err != nil {
		t.Fatalf("quintgo: step log: %v", err)
	}
	run.Steps = mem.Steps()
	if len(run.Steps) == 0 {
		if c.SkipIfEmpty {
			t.Skip("quintgo: no steps recorded (build with `orchestrion go test` to weave //quint:action)")
		}
		t.Fatal("quintgo: the scenario recorded no steps")
	}
	return check(t, bindingPath, run, c)
}

// RecordAndCheck is Record, failing the test (with the offending step and
// invariant) unless every actor conforms and satisfies the invariants.
func RecordAndCheck(t testing.TB, bindingPath string, scenario func(rec *qobs.Recorder), opts ...Option) *Run {
	t.Helper()
	r := Record(t, bindingPath, scenario, opts...)
	fail(t, r)
	return r
}

// Check validates steps recorded elsewhere, failing the test on a violation.
func Check(t testing.TB, bindingPath string, steps []qtrace.Step, opts ...Option) *Run {
	t.Helper()
	c := config(t, opts)
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run := &Run{Steps: steps, StepLog: filepath.Join(c.Dir, "steps.jsonl"), ITF: map[string]string{}}
	f, err := os.Create(run.StepLog)
	if err == nil {
		err = qtrace.WriteJSONL(f, steps)
		f.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	r := check(t, bindingPath, run, c)
	fail(t, r)
	return r
}

func check(t testing.TB, bindingPath string, run *Run, c Config) *Run {
	t.Helper()
	o := c.Options
	o.Dir = filepath.Join(c.Dir, "quint")
	ch := checker(t, bindingPath, o)
	// The model-level trace, emitted in-process from the same steps.
	if itfs, _, err := ch.Binding.StepsToITF(run.Steps); err == nil {
		for actor, data := range itfs {
			p := filepath.Join(c.Dir, strings.NewReplacer("/", "_", " ", "_").Replace(actor)+".itf.json")
			if os.WriteFile(p, data, 0o644) == nil {
				run.ITF[actor] = p
			}
		}
	} else {
		t.Logf("quintgo: no model-level ITF: %v", err)
	}
	cc := *ch // per run: its own output directory
	cc.Options.Dir = o.Dir
	rep, err := cc.Check(context.Background(), run.Steps)
	if err != nil {
		t.Fatalf("quintgo: %v", err)
	}
	run.Report = rep
	return run
}

func fail(t testing.TB, r *Run) {
	t.Helper()
	if r.OK() {
		return
	}
	var itfs []string
	for _, p := range r.ITF {
		itfs = append(itfs, p)
	}
	t.Fatalf("quintgo: the recorded trace does not satisfy the model\n%s  step log: %s\n  model-level ITF: %s",
		r.Report.Summary(), r.StepLog, strings.Join(itfs, ", "))
}
