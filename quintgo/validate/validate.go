// Package validate checks reconstructed traces against a Quint model: it
// generates a replay module from a binding (package binding), runs it with
// `quint test`, and turns quint's verdict back into a structured result that
// points at the offending observed step.
package validate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/quintgo/binding"
	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
)

// Options configures a validation run.
type Options struct {
	Quint   string // quint binary; default "quint"
	Backend string // quint test --backend; default $QUINTGO_BACKEND, else "rust"
	Dir     string // where generated modules and ITF go; default a temp dir
}

// Verdict of one check.
type Verdict string

const (
	Pass         Verdict = "pass"
	Fail         Verdict = "fail"
	NotEvaluated Verdict = "n/a" // e.g. invariants of a trace that does not conform
)

// Result is the outcome of validating one actor's trace.
type Result struct {
	Actor string
	Steps int // replayed steps, including inferred ones

	// Conformance: is the observed trace a behaviour of the model, with the
	// observed state projections holding after each step?
	Conforms Verdict
	// On failure: which step, and why. Kind is "disabled" (the model cannot
	// take this step from the state reached: QNT513) or "state" (the step
	// was taken but the observed state projection does not hold: QNT508).
	Failure *Failure

	// Invariants, each evaluated in every state of the replay.
	Invariants []InvariantResult

	// Issues from reconstruction (gaps, cycles, overlaps). A trace with an
	// "error" issue was validated anyway, but a pass means little.
	Issues []qtrace.Issue

	Module string // the generated .qnt
	ITF    string // ITF of the replayed trace, when it conforms
	Calls  []binding.Call
	Output string // quint's output, verbatim
}

type Failure struct {
	Note  string // for ITF replays: which state, and what was observed
	Kind  string
	Index int    // into Calls
	Call  string // the Quint step
	Step  *qtrace.Step
	Quint string // quint's error message
}

type InvariantResult struct {
	Name    string
	Verdict Verdict
	Failure *Failure
}

// OK is true when the trace conforms and every invariant holds, and
// reconstruction reported no errors.
func (r *Result) OK() bool {
	if r.Conforms != Pass {
		return false
	}
	for _, i := range r.Invariants {
		if i.Verdict != Pass {
			return false
		}
	}
	for _, i := range r.Issues {
		if i.Severity == "error" {
			return false
		}
	}
	return true
}

// Validate checks one actor's trace.
func Validate(ctx context.Context, b *binding.Binding, m *binding.Model, t *qtrace.Trace, opt Options) (*Result, error) {
	opt, err := opt.resolve()
	if err != nil {
		return nil, err
	}
	base := "observed_" + sanitize(t.Actor)
	qnt := filepath.Join(opt.Dir, base+".qnt")
	g, err := b.Generate(m, t, qnt)
	if err != nil {
		return nil, err
	}
	res := &Result{Actor: t.Actor, Issues: t.Issues}
	return res, run(ctx, g, qnt, base, b.Invariants, opt, res)
}

func (opt Options) resolve() (Options, error) {
	if opt.Quint == "" {
		opt.Quint = "quint"
	}
	if opt.Backend == "" {
		opt.Backend = os.Getenv("QUINTGO_BACKEND")
	}
	if opt.Backend == "" {
		opt.Backend = "rust"
	}
	if opt.Dir == "" {
		d, err := os.MkdirTemp("", "quintgo-")
		if err != nil {
			return opt, err
		}
		opt.Dir = d
	}
	if err := os.MkdirAll(opt.Dir, 0o755); err != nil {
		return opt, err
	}
	if abs, err := filepath.Abs(opt.Dir); err == nil {
		opt.Dir = abs
	}
	return opt, nil
}

// run writes a generated module, runs quint test on it, and fills res.
func run(ctx context.Context, g *binding.Generated, qnt, base string, invariants []string, opt Options, res *Result) error {
	if err := os.WriteFile(qnt, []byte(g.Text), 0o644); err != nil {
		return err
	}
	res.Steps, res.Module, res.Calls = len(g.Calls), qnt, g.Calls
	itfPattern := filepath.Join(opt.Dir, base+"_{test}.itf.json")
	cmd := exec.CommandContext(ctx, opt.Quint, "test", filepath.Base(qnt), "--main", g.Module,
		"--backend", opt.Backend, "--max-samples", "1", "--out-itf", itfPattern)
	cmd.Dir = opt.Dir
	out, err := cmd.CombinedOutput()
	res.Output = string(out)
	if err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return fmt.Errorf("quint test: %w", err)
		}
	}
	verdicts, failures := parseQuintTest(res.Output, qnt, g)
	if _, ok := verdicts[g.Tests.Conforms]; !ok {
		return fmt.Errorf("quint test did not report %s:\n%s", g.Tests.Conforms, res.Output)
	}
	res.Conforms = verdicts[g.Tests.Conforms]
	res.Failure = failures[g.Tests.Conforms]
	for _, inv := range invariants {
		name := g.Tests.Invariants[inv]
		ir := InvariantResult{Name: inv, Verdict: verdicts[name], Failure: failures[name]}
		if res.Conforms != Pass {
			ir.Verdict, ir.Failure = NotEvaluated, nil
		}
		res.Invariants = append(res.Invariants, ir)
	}
	if res.Conforms == Pass {
		itf := strings.ReplaceAll(itfPattern, "{test}", g.Tests.Conforms)
		if err := annotateITF(itf, g); err == nil {
			res.ITF = itf
		}
	}
	return nil
}

var (
	reOK     = regexp.MustCompile(`(?m)^\s+ok (\S+) passed`)
	reFailed = regexp.MustCompile(`(?m)^\s+\d+\) (\S+) failed`)
	reBlock  = regexp.MustCompile(`(?m)^\s+\d+\) (\S+):\s*\n\s+(Error \[QNT\d+\]: [^\n]*)\n\s+at ([^\n]*):(\d+):(\d+)`)
)

func parseQuintTest(out, qnt string, g *binding.Generated) (map[string]Verdict, map[string]*Failure) {
	v := map[string]Verdict{}
	f := map[string]*Failure{}
	for _, m := range reOK.FindAllStringSubmatch(out, -1) {
		v[m[1]] = Pass
	}
	for _, m := range reFailed.FindAllStringSubmatch(out, -1) {
		v[m[1]] = Fail
	}
	for _, m := range reBlock.FindAllStringSubmatch(out, -1) {
		name, msg := m[1], m[2]
		line, _ := strconv.Atoi(m[4])
		fl := &Failure{Quint: msg, Index: -1}
		switch {
		case strings.Contains(msg, "QNT513"), strings.Contains(msg, "Cannot continue to"):
			// the action itself evaluated to false (with or without a
			// following .expect): the model cannot take this step
			fl.Kind = "disabled"
		case strings.Contains(msg, "QNT508"):
			fl.Kind = "state"
			if name != g.Tests.Conforms {
				fl.Kind = "invariant"
			}
		default:
			fl.Kind = "error"
		}
		if i, ok := g.LineStep[line]; ok {
			fl.Index = i
			fl.Call = g.Calls[i].Quint
			fl.Step = g.Calls[i].Step
			fl.Note = g.Calls[i].Note
		} else {
			fl.Call = "init" // init.expect(...) of an invariant run
		}
		f[name] = fl
	}
	return v, f
}

// annotateITF strips the module prefix from variable names (so tools like
// the model's trace.py read it) and records the action taken per state.
func annotateITF(path string, g *binding.Generated) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var t map[string]any
	if err := json.Unmarshal(b, &t); err != nil {
		return err
	}
	strip := func(k string) string {
		if i := strings.LastIndex(k, "::"); i >= 0 {
			return k[i+2:]
		}
		return k
	}
	if vars, ok := t["vars"].([]any); ok {
		for i, v := range vars {
			vars[i] = strip(fmt.Sprint(v))
		}
	}
	states, _ := t["states"].([]any)
	for i, s := range states {
		st, ok := s.(map[string]any)
		if !ok {
			continue
		}
		ns := map[string]any{}
		for k, v := range st {
			ns[strip(k)] = v
		}
		act := "init"
		if i > 0 && i-1 < len(g.Calls) {
			act = g.Calls[i-1].Quint
			if p := strings.IndexByte(act, '('); p >= 0 {
				act = act[:p]
			}
			c := g.Calls[i-1]
			if c.Step != nil {
				ns["mbt::observedSeq"] = map[string]any{"#bigint": strconv.FormatUint(c.Step.Seq, 10)}
			}
			// The arguments, as `quint run --mbt` records nondet picks, so
			// this trace can drive an implementation (package connect).
			picks := map[string]any{}
			for name, expr := range c.Args {
				if v, err := itf.ParseQuint(expr); err == nil {
					picks[name] = itf.Some(v)
				}
			}
			ns["mbt::nondetPicks"] = picks
		}
		ns["mbt::actionTaken"] = act
		if i == 0 {
			ns["mbt::nondetPicks"] = map[string]any{}
		}
		states[i] = ns
	}
	out, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, s)
}

// Summary renders a result for humans.
func (r *Result) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "actor %s: %d steps\n", r.Actor, r.Steps)
	for _, i := range r.Issues {
		fmt.Fprintf(&b, "  issue: %s\n", i)
	}
	fmt.Fprintf(&b, "  conformance: %s\n", strings.ToUpper(string(r.Conforms)))
	if f := r.Failure; f != nil {
		fmt.Fprintf(&b, "%s", f.describe("    "))
	}
	for _, i := range r.Invariants {
		fmt.Fprintf(&b, "  invariant %s: %s\n", i.Name, strings.ToUpper(string(i.Verdict)))
		if i.Failure != nil {
			fmt.Fprintf(&b, "%s", i.Failure.describe("    "))
		}
	}
	fmt.Fprintf(&b, "  module: %s\n", r.Module)
	if r.ITF != "" {
		fmt.Fprintf(&b, "  itf: %s\n", r.ITF)
	}
	return b.String()
}

func (f *Failure) describe(pfx string) string {
	var b strings.Builder
	switch f.Kind {
	case "disabled":
		fmt.Fprintf(&b, "%sstep [%d] is not a transition of the model from the state reached\n", pfx, f.Index)
	case "state":
		fmt.Fprintf(&b, "%safter step [%d] the model's state does not match what was observed\n", pfx, f.Index)
	case "invariant":
		if f.Index < 0 {
			fmt.Fprintf(&b, "%sviolated in the initial state\n", pfx)
		} else {
			fmt.Fprintf(&b, "%sviolated after step [%d]\n", pfx, f.Index)
		}
	default:
		fmt.Fprintf(&b, "%sfailed at step [%d]\n", pfx, f.Index)
	}
	if f.Call != "" {
		fmt.Fprintf(&b, "%s  quint:    %s\n", pfx, f.Call)
	}
	if f.Step == nil && f.Note != "" {
		fmt.Fprintf(&b, "%s  observed: %s\n", pfx, f.Note)
	}
	if s := f.Step; s != nil {
		fmt.Fprintf(&b, "%s  observed: seq %d %s (%s)\n", pfx, s.Seq, s.String(), s.Source)
		if s.Error != "" {
			fmt.Fprintf(&b, "%s  error:    %s\n", pfx, s.Error)
		}
	}
	fmt.Fprintf(&b, "%s  %s\n", pfx, f.Quint)
	return b.String()
}
