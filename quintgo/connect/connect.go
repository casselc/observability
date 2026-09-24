// Package connect drives Go code from Quint traces: model-based testing in
// the style of the Rust quint-connect crate. Quint generates traces
// (`quint run --mbt`), and a Driver replays each step on the implementation
// and compares the implementation's state with the model's after each step.
//
// The per-project part is the Driver: a map from action names to handlers,
// and a check of the state projection. Everything else is generic.
package connect

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
)

// Step is what a handler sees: the action, its nondeterministic picks, and
// the model state before and after it.
type Step struct {
	Index  int
	Action string
	Picks  map[string]itf.Value
	Before map[string]itf.Value
	After  map[string]itf.Value
}

// Pick returns a nondeterministic pick, failing the step if it is absent.
func (s *Step) Pick(name string) itf.Value {
	v, ok := s.Picks[name]
	if !ok {
		panic(fmt.Sprintf("step %d (%s): no nondet pick %q", s.Index, s.Action, name))
	}
	return v
}

// Handler runs one model action on the implementation.
type Handler func(s *Step) error

// Driver replays traces on an implementation.
type Driver struct {
	// Init resets the implementation to the model's initial state.
	Init func(state map[string]itf.Value) error
	// Actions maps model action names to handlers. Actions listed in Skip are
	// not implemented by this system under test (e.g. another component's).
	Actions map[string]Handler
	Skip    map[string]bool
	// Check compares the implementation with the model state after a step;
	// nil means no state comparison.
	Check func(s *Step) error
}

// Replay runs one trace. It stops at the first failing step.
func (d *Driver) Replay(t *itf.Trace) error {
	if len(t.States) == 0 {
		return fmt.Errorf("empty trace")
	}
	if d.Init != nil {
		if err := d.Init(t.States[0].Vars); err != nil {
			return fmt.Errorf("init: %w", err)
		}
	}
	for i := 1; i < len(t.States); i++ {
		s := &Step{Index: i, Action: t.States[i].Action, Picks: t.States[i].Picks, Before: t.States[i-1].Vars, After: t.States[i].Vars}
		if d.Skip[s.Action] {
			continue
		}
		h, ok := d.Actions[s.Action]
		if !ok {
			return fmt.Errorf("step %d: action %s is not implemented by the driver", i, s.Action)
		}
		if err := h(s); err != nil {
			return fmt.Errorf("step %d %s%s: %w", i, s.Action, picks(s), err)
		}
		if d.Check != nil {
			if err := d.Check(s); err != nil {
				return fmt.Errorf("step %d %s%s: state diverged: %w", i, s.Action, picks(s), err)
			}
		}
	}
	return nil
}

func picks(s *Step) string {
	var ks []string
	for k := range s.Picks {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var b strings.Builder
	for _, k := range ks {
		fmt.Fprintf(&b, " %s=%s", k, s.Picks[k])
	}
	return b.String()
}

// Generate runs `quint run --mbt` and returns the ITF files it wrote.
type Generate struct {
	Quint    string // default "quint"
	Spec     string
	Main     string
	Traces   int   // --n-traces
	Samples  int   // --max-samples
	MaxSteps int   // --max-steps
	Seed     int64 // 0: random
	Backend  string
	Dir      string // output directory (default temp)
	Extra    []string
}

func (g Generate) Run(ctx context.Context) ([]string, error) {
	if g.Quint == "" {
		g.Quint = "quint"
	}
	if g.Dir == "" {
		d, err := os.MkdirTemp("", "quintgo-mbt-")
		if err != nil {
			return nil, err
		}
		g.Dir = d
	}
	if g.Traces == 0 {
		g.Traces = 1
	}
	if g.Samples < g.Traces {
		g.Samples = g.Traces
	}
	out := filepath.Join(g.Dir, "trace.itf.json")
	args := []string{"run", filepath.Base(g.Spec), "--mbt", "--out-itf", out,
		"--n-traces", strconv.Itoa(g.Traces), "--max-samples", strconv.Itoa(g.Samples)}
	if g.Main != "" {
		args = append(args, "--main", g.Main)
	}
	if g.MaxSteps > 0 {
		args = append(args, "--max-steps", strconv.Itoa(g.MaxSteps))
	}
	if g.Seed != 0 {
		args = append(args, "--seed", strconv.FormatInt(g.Seed, 10))
	}
	if g.Backend != "" {
		args = append(args, "--backend", g.Backend)
	}
	args = append(args, g.Extra...)
	cmd := exec.CommandContext(ctx, g.Quint, args...)
	cmd.Dir = filepath.Dir(g.Spec)
	if b, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("quint run: %v\n%s", err, b)
	}
	files, _ := filepath.Glob(filepath.Join(g.Dir, "trace*.itf.json"))
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("quint run wrote no traces to %s", g.Dir)
	}
	return files, nil
}
