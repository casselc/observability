// Package tracetag records, at run time, which STPA hazards, constraints,
// requirements and CAST rows a test verifies, and how the test ended.
//
// A static tag says only that someone meant a test to cover an item; a
// skipped or unrun test is an unknown, not evidence (STPA.md CAST rows 41,
// 43, 45, 46, 53). So the tag is a call that runs with the test:
//
//	func TestPairScopeIsUnionNeverProduct(t *testing.T) {
//		tracetag.Covers(t, "P", "CAST-52", "H-6", "H-E3")
//
// With OSCOPE_TRACE_OUT set, Covers registers a t.Cleanup that appends one
// JSON line to that file after the test finishes: the IDs, the technique
// (a code from VERIFICATION.md §1), the test's full name, its package and
// source file, the commit, the CI job, and the outcome (passed, failed or
// skipped, from t.Failed / t.Skipped). Unset, Covers does nothing.
// ci/trace/trace.py joins the records with the ID catalogue and with a static
// listing of every Covers call, and fails CI when a tagged test failed, did
// not run, or an ID it names is unknown (ci/README.md, "Traceability").
//
// Call it first in the test, before anything can skip or fail it.
//
// It lives in the testgate module so that every module that already requires
// testgate (by a filesystem replace) can use it without a new requirement.
// The Rust twin is otap-rs/src/oscope_trace.rs; the Node one is
// lakeui/test/trace.js.
package tracetag

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// Env names the JSONL file records are appended to.
const Env = "OSCOPE_TRACE_OUT"

// Record is one line of the trace file; the fields are shared with the Rust
// and Node helpers and ci/trace/trace.py.
type Record struct {
	IDs       []string `json:"ids"`
	Technique string   `json:"technique"`
	Test      string   `json:"test"`
	Func      string   `json:"func"`
	Package   string   `json:"package"`
	File      string   `json:"file"`
	Lang      string   `json:"lang"`
	Commit    string   `json:"commit"`
	Job       string   `json:"job"`
	Outcome   string   `json:"outcome"`
}

// Covers tags t as verifying ids with technique; see the package comment.
func Covers(t testing.TB, technique string, ids ...string) {
	t.Helper()
	out := os.Getenv(Env)
	if out == "" {
		return
	}
	r := Record{IDs: ids, Technique: technique, Test: t.Name(), Lang: "go", Commit: commit(), Job: os.Getenv("GITHUB_JOB")}
	if pc, file, _, ok := runtime.Caller(1); ok {
		r.File = repoPath(file)
		if fn := runtime.FuncForPC(pc); fn != nil {
			r.Package, r.Func = splitFunc(fn.Name())
		}
	}
	t.Cleanup(func() {
		switch {
		case t.Failed():
			r.Outcome = "failed"
		case t.Skipped():
			r.Outcome = "skipped"
		default:
			r.Outcome = "passed"
		}
		if err := appendRecord(out, r); err != nil {
			t.Errorf("tracetag: %s=%s: %v", Env, out, err)
		}
	})
}

// splitFunc turns "github.com/x/y/pkg.TestA.func1" into ("github.com/x/y/pkg", "TestA").
func splitFunc(name string) (pkg, fn string) {
	slash := strings.LastIndex(name, "/")
	dot := strings.Index(name[slash+1:], ".")
	if dot < 0 {
		return name, ""
	}
	pkg, fn = name[:slash+1+dot], name[slash+1+dot+1:]
	if i := strings.Index(fn, "."); i >= 0 {
		fn = fn[:i]
	}
	return pkg, fn
}

// repoPath makes an absolute source path relative to the repository root
// (the directory holding otel-chdb/), so it matches the static listing.
func repoPath(file string) string {
	file = filepath.ToSlash(file)
	if i := strings.LastIndex(file, "/otel-chdb/"); i >= 0 {
		return file[i+1:]
	}
	return file
}

var (
	commitOnce sync.Once
	commitSHA  string
	mu         sync.Mutex
)

// commit is GITHUB_SHA, or HEAD of the checkout the test runs in.
func commit() string {
	commitOnce.Do(func() {
		if s := os.Getenv("GITHUB_SHA"); s != "" {
			commitSHA = s
			return
		}
		if b, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
			commitSHA = strings.TrimSpace(string(b))
		}
	})
	return commitSHA
}

// appendRecord writes r as one line with a single write on an O_APPEND file,
// so records from parallel test binaries do not interleave.
func appendRecord(path string, r Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	mu.Lock()
	defer mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
