package tracetag

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

const fixtureEnv = "TRACETAG_FIXTURE"

// The fixtures run only inside TestRecordsEveryOutcome's child process.
func fixture(t *testing.T) {
	if os.Getenv(fixtureEnv) == "" {
		t.Skip("a fixture for TestRecordsEveryOutcome")
	}
}

func TestFixturePasses(t *testing.T) {
	fixture(t)
	Covers(t, "P", "H-2", "CAST-1")
}

func TestFixtureFails(t *testing.T) {
	fixture(t)
	Covers(t, "DST", "H-1")
	t.Error("planned failure")
}

func TestFixtureSkips(t *testing.T) {
	fixture(t)
	Covers(t, "IT", "H-6")
	t.Skip("planned skip")
}

func TestFixtureSubtest(t *testing.T) {
	fixture(t)
	t.Run("inner", func(t *testing.T) { Covers(t, "PH", "SEC-7") })
}

// A child run of the fixtures writes one record per tagged test, with the
// outcome the test really had.
func TestRecordsEveryOutcome(t *testing.T) {
	out := filepath.Join(t.TempDir(), "sub", "trace.jsonl")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestFixture", "-test.count=1")
	cmd.Env = append(os.Environ(), fixtureEnv+"=1", Env+"="+out, "GITHUB_SHA=abc123", "GITHUB_JOB=go-test")
	if b, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("the child run has a planned failure but exited 0:\n%s", b)
	}
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := map[string]Record{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%q: %v", sc.Text(), err)
		}
		got[r.Test] = r
	}
	want := map[string]string{
		"TestFixturePasses":        "passed",
		"TestFixtureFails":         "failed",
		"TestFixtureSkips":         "skipped",
		"TestFixtureSubtest/inner": "passed",
	}
	var names []string
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(got) != len(want) {
		t.Fatalf("records for %v, want %d", names, len(want))
	}
	for n, o := range want {
		r := got[n]
		if r.Outcome != o {
			t.Errorf("%s: outcome %q, want %q", n, r.Outcome, o)
		}
		if r.Commit != "abc123" || r.Job != "go-test" || r.Lang != "go" {
			t.Errorf("%s: commit %q job %q lang %q", n, r.Commit, r.Job, r.Lang)
		}
		if r.File != "otel-chdb/testgate/tracetag/tracetag_test.go" {
			t.Errorf("%s: file %q", n, r.File)
		}
		if r.Package != "github.com/casselc/observability/otel-chdb/testgate/tracetag" {
			t.Errorf("%s: package %q", n, r.Package)
		}
	}
	if r := got["TestFixtureSubtest/inner"]; r.Func != "TestFixtureSubtest" {
		t.Errorf("subtest func %q, want the enclosing TestFixtureSubtest", r.Func)
	}
	if r := got["TestFixturePasses"]; len(r.IDs) != 2 || r.IDs[0] != "H-2" || r.IDs[1] != "CAST-1" || r.Technique != "P" {
		t.Errorf("ids %v technique %q", r.IDs, r.Technique)
	}
}

func TestUnsetWritesNothing(t *testing.T) {
	t.Setenv(Env, "")
	Covers(t, "P", "H-2") // must not register anything or fail
}

func TestSplitFunc(t *testing.T) {
	for in, want := range map[string][2]string{
		"github.com/x/y/pkg.TestA":            {"github.com/x/y/pkg", "TestA"},
		"github.com/x/y/pkg.TestA.func1":      {"github.com/x/y/pkg", "TestA"},
		"github.com/x/y.v2/pkg.TestB.func1.2": {"github.com/x/y.v2/pkg", "TestB"},
		"main.TestC":                          {"main", "TestC"},
	} {
		if p, f := splitFunc(in); p != want[0] || f != want[1] {
			t.Errorf("splitFunc(%q) = %q, %q; want %q", in, p, f, want)
		}
	}
}
