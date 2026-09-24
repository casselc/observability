package mbt

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/quintgo/connect"
	"github.com/chucklehead-dev/oscope/spike/quintgo/examples/edgepublish/publisher"
	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
	"github.com/chucklehead-dev/oscope/spike/quintgo/validate"
)

// QUINTGO_TRACES (default 5) traces of QUINTGO_STEPS (default 30) steps are
// generated from the model's currentDesign instance with seed QUINTGO_SEED.
func traces(t *testing.T) []string {
	if _, err := exec.LookPath("quint"); err != nil {
		t.Skip("quint not installed")
	}
	env := func(k string, def int) int {
		if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
			return v
		}
		return def
	}
	spec, _ := filepath.Abs("../../../../otel-chdb/model/edgePublish.qnt")
	files, err := connect.Generate{
		Spec: spec, Main: "currentDesign", Traces: env("QUINTGO_TRACES", 5), MaxSteps: env("QUINTGO_STEPS", 30),
		Seed: int64(env("QUINTGO_SEED", 42)), Backend: os.Getenv("QUINTGO_BACKEND"), Dir: t.TempDir(),
	}.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestModelTracesDriveThePublisher(t *testing.T) {
	for _, f := range traces(t) {
		tr, err := itf.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var acts []string
		for _, s := range tr.States[1:] {
			acts = append(acts, s.Action)
		}
		d := &Driver{}
		if err := d.Connect().Replay(tr); err != nil {
			t.Errorf("%s: %v\n  actions: %s", filepath.Base(f), err, strings.Join(acts, " "))
		} else {
			t.Logf("%s: %d steps replayed: %s", filepath.Base(f), len(tr.States)-1, strings.Join(acts, " "))
		}
	}
}

// The mutant must fail on some trace that starts a push and writes its table.
func TestModelTracesCatchManifestBeforeTable(t *testing.T) {
	caught := 0
	files := traces(t)
	for _, f := range files {
		tr, err := itf.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		d := &Driver{Mutation: publisher.ManifestBeforeTable}
		if err := d.Connect().Replay(tr); err != nil {
			caught++
			t.Logf("%s: rejected: %v", filepath.Base(f), err)
		}
	}
	if caught == 0 {
		t.Fatalf("no trace exposed the manifest-before-table mutant (%d traces)", len(files))
	}
}

// Round trip: drive the publisher from model traces with step recording on,
// then validate what it recorded against the model. Needs the annotations
// woven: `go run github.com/DataDog/orchestrion go test ./mbt/ -run RoundTrip`.
func TestRoundTrip(t *testing.T) {
	files := traces(t)
	checker, err := validate.NewChecker("../binding.yaml", validate.Options{Backend: os.Getenv("QUINTGO_BACKEND"), Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		tr, err := itf.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sink := &qobs.MemorySink{}
		rec := qobs.NewRecorder(sink)
		qobs.SetGlobal(rec)
		d := &Driver{Recorder: rec}
		err = d.Connect().Replay(tr)
		qobs.SetGlobal(nil)
		if err != nil {
			t.Fatalf("%s: replay: %v", filepath.Base(f), err)
		}
		steps := sink.Steps()
		if len(steps) == 0 {
			t.Skip("no steps recorded: build with orchestrion to weave //quint:action")
		}
		rep, err := checker.Check(context.Background(), steps)
		if err != nil {
			t.Fatal(err)
		}
		if !rep.OK() {
			t.Errorf("%s: recorded telemetry does not validate:\n%s", filepath.Base(f), rep.Summary())
		} else {
			t.Logf("%s: %d recorded steps validate against the model", filepath.Base(f), len(steps))
		}
	}
}
