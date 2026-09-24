package validate

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
)

// Exercises the library entry points on a model unrelated to edgePublish:
// record with a MemorySink, Check with a Checker.
func TestAccountModel(t *testing.T) {
	if _, err := exec.LookPath("quint"); err != nil {
		t.Skip("quint not installed")
	}
	c, err := NewChecker("testdata/account.yaml", Options{Backend: "typescript", Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	run := func(ops ...any) []qtrace.Step {
		sink := &qobs.MemorySink{}
		r := qobs.NewRecorder(sink, qobs.WithActor("acct-1"))
		ctx := context.Background()
		for i := 0; i < len(ops); i += 3 {
			r.Record(ctx, ops[i].(string), nil, "amount", ops[i+1], "obs.balance", ops[i+2])
		}
		return sink.Steps()
	}
	cases := []struct {
		name    string
		steps   []qtrace.Step
		ok      bool
		failure string // Failure.Kind of the conformance check
		inv     string // an invariant that must fail
	}{
		{"conforms", run("deposit", 50, 50, "withdraw", 20, 30, "deposit", 5, 35), true, "", ""},
		{"overdraw", run("deposit", 10, 10, "withdraw", 20, -10), false, "disabled", ""},
		{"wrong balance", run("deposit", 10, 10, "deposit", 5, 16), false, "state", ""},
		{"over limit", run("deposit", 60, 60, "deposit", 60, 120), false, "", "underLimit"},
	}
	for _, tc := range cases {
		rep, err := c.Check(context.Background(), tc.steps)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if rep.OK() != tc.ok {
			t.Errorf("%s: OK()=%v\n%s", tc.name, rep.OK(), rep.Summary())
			continue
		}
		r := rep.Actors[0]
		if tc.failure != "" && (r.Failure == nil || r.Failure.Kind != tc.failure || r.Failure.Index != 1) {
			t.Errorf("%s: want %s failure at step 1\n%s", tc.name, tc.failure, rep.Summary())
		}
		if tc.inv != "" {
			found := false
			for _, i := range r.Invariants {
				if i.Name == tc.inv && i.Verdict == Fail {
					found = true
				}
			}
			if r.Conforms != Pass || !found || !strings.Contains(rep.Summary(), "violated after step [1]") {
				t.Errorf("%s: want conforming trace violating %s\n%s", tc.name, tc.inv, rep.Summary())
			}
		}
	}
}
