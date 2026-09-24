package scenarios

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/quintgo/examples/edgepublish/mbt"
	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtest"
	"github.com/chucklehead-dev/oscope/spike/quintgo/validate"
)

// Each scenario, recorded directly (no OTel) and checked two ways: the
// recorded steps through the binding, and the model-level ITF emitted
// in-process. Both must give the expected verdict, at the same step.
// Needs the annotations woven:
//
//	go run github.com/DataDog/orchestrion go test ./scenarios/ -v
func TestScenarios(t *testing.T) {
	conforms := map[string]bool{"happy": true, "rotation-race": true, "ambiguous-seal": true, "manifest-first": false, "no-lock": false}
	invariantsHold := map[string]bool{"happy": true, "rotation-race": true}
	opt := validate.Options{Backend: os.Getenv("QUINTGO_BACKEND")}
	for _, name := range Names {
		t.Run(name, func(t *testing.T) {
			r := qtest.Record(t, "../binding.yaml", func(*qobs.Recorder) { Run(name, io.Discard) },
				qtest.SkipIfEmpty(), qtest.WithOptions(opt))
			if len(r.Report.Actors) != 1 {
				t.Fatalf("actors: %d", len(r.Report.Actors))
			}
			steps := r.Report.Actors[0]
			if (steps.Conforms == validate.Pass) != conforms[name] || r.OK() != invariantsHold[name] {
				t.Fatalf("steps path: conforms=%s ok=%v\n%s", steps.Conforms, r.OK(), r.Report.Summary())
			}
			tr, err := itf.ReadFile(r.ITF["edge-1/traces"])
			if err != nil {
				t.Fatal(err)
			}
			o := opt
			o.Dir = t.TempDir()
			fromITF, err := validate.ValidateITF(context.Background(), tr, validate.ITFOptions{Options: o})
			if err != nil {
				t.Fatal(err)
			}
			if fromITF.Conforms != steps.Conforms || fromITF.OK() != steps.OK() {
				t.Fatalf("ITF path disagrees: %s vs %s\n%s\n%s", fromITF.Conforms, steps.Conforms, fromITF.Summary(), steps.Summary())
			}
			if f, g := steps.Failure, fromITF.Failure; (f == nil) != (g == nil) || (f != nil && (f.Index != g.Index || f.Kind != g.Kind)) {
				t.Fatalf("failures differ: %+v vs %+v", f, g)
			}
			for i, inv := range steps.Invariants {
				if fromITF.Invariants[i].Verdict != inv.Verdict {
					t.Fatalf("invariant %s: %s vs %s", inv.Name, inv.Verdict, fromITF.Invariants[i].Verdict)
				}
			}
			// Symmetry with connect: the validated trace (model states plus
			// mbt::actionTaken/nondetPicks, like `quint run --mbt` output)
			// drives the implementation again, deterministically.
			if steps.Conforms == validate.Pass {
				replay, err := itf.ReadFile(steps.ITF)
				if err != nil {
					t.Fatal(err)
				}
				if err := (&mbt.Driver{}).Connect().Replay(replay); err != nil {
					t.Fatalf("replaying the observed trace on the publisher: %v", err)
				}
			}
			t.Logf("%s: %d steps; conformance %s, invariants ok=%v (steps and ITF paths agree)", name, len(r.Steps), steps.Conforms, r.OK())
		})
	}
}
