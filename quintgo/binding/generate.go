package binding

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
)

// Generated is a Quint module that replays one observed trace.
type Generated struct {
	Module   string         // module name
	Text     string         // the .qnt source
	LineStep map[int]int    // source line -> index into Calls
	Calls    []Call         // the replayed steps, in order, including inferred ones
	Tests    GeneratedTests // names of the runs in Text
}

// GeneratedTests names the runs a generated module holds.
type GeneratedTests struct {
	Conforms   string            // replays every step; checks each step's expect
	Invariants map[string]string // invariant -> run that checks it in every state
}

// Call is one step of the replay.
type Call struct {
	Quint    string            // e.g. writeTable({ payload: 1, ... }, Ok)
	Expect   string            // post-state check, or ""
	Step     *qtrace.Step      // the observed step; nil for inferred steps
	Inferred string            // why the step was inserted ("restart"), if inferred
	Action   string            // the model action
	Args     map[string]string // parameter -> the Quint expression passed
	Note     string            // description, for calls that come from an ITF state
}

// Plan is the binding applied to one actor's trace: the instance's
// constants and the calls, in model terms. It needs no Quint tooling.
type Plan struct {
	Actor         string
	Processes     []string
	ObservedSteps int
	Constants     map[string]string // constant -> Quint expression
	Calls         []Call
}

// ConstantList renders the constants as "NAME = expr", sorted.
func (p *Plan) ConstantList() []string {
	var out []string
	for _, c := range sortedKeys(p.Constants) {
		out = append(out, fmt.Sprintf("%s = %s", c, p.Constants[c]))
	}
	return out
}

// Generate builds the replay module for one actor's trace. qntPath is where
// the module will be written (the model import is made relative to it).
func (b *Binding) Generate(m *Model, t *qtrace.Trace, qntPath string) (*Generated, error) {
	p, err := b.Plan(m, t)
	if err != nil {
		return nil, err
	}
	return RenderModule(ModuleSpec{
		Spec: b.SpecPath(), Module: b.Module, Constants: p.ConstantList(), Defs: b.Defs, Invariants: b.Invariants,
		Header: []string{fmt.Sprintf("actor %q, %d observed steps, processes %v", t.Actor, len(t.Steps), t.Processes)},
	}, p.Calls, qntPath)
}

// Plan applies the binding to a trace. With a model, parameters are taken
// in the model's order and checked; with m == nil (emitting a model-level
// ITF in-process, without Quint) they are keyed by name only.
func (b *Binding) Plan(m *Model, t *qtrace.Trace) (*Plan, error) {
	// 1. Intern opaque identifiers, in trace order.
	type ikey struct{ arg, scope string }
	tables := map[ikey]map[string]int64{}
	procIdx := map[string]int{}
	for i, p := range t.Processes {
		procIdx[p] = i + 1
	}
	args := make([]map[string]any, len(t.Steps))
	for i := range t.Steps {
		s := &t.Steps[i]
		a := map[string]any{}
		for k, v := range s.Args {
			scope, ok := b.Intern[k]
			if !ok {
				a[k] = v
				continue
			}
			key := ikey{k, ""}
			if scope == "process" {
				key.scope = s.Process
			}
			tb := tables[key]
			if tb == nil {
				tb = map[string]int64{}
				tables[key] = tb
			}
			id := fmt.Sprint(v)
			if _, ok := tb[id]; !ok {
				tb[id] = int64(len(tb) + 1)
			}
			a[k] = tb[id]
		}
		args[i] = a
	}

	// 2. Constants, from statistics over the interned trace.
	funcs := template.FuncMap{
		"set": func(arg string) string {
			seen := map[string]bool{}
			var vs []string
			for _, a := range args {
				if v, ok := a[arg]; ok && !seen[quintLit(v)] {
					seen[quintLit(v)] = true
					vs = append(vs, quintLit(v))
				}
			}
			sort.Strings(vs)
			return "Set(" + strings.Join(vs, ", ") + ")"
		},
		"max": func(arg string) int64 {
			var mx int64
			for _, a := range args {
				if v, ok := a[arg].(int64); ok && v > mx {
					mx = v
				}
			}
			return mx
		},
		"count": func(action string) int {
			n := 0
			for _, s := range t.Steps {
				if s.Action == action {
					n++
				}
			}
			return n
		},
		"procs": func() int { return len(t.Processes) },
		"add":   func(a, b any) int64 { return toI(a) + toI(b) },
		"atLeast": func(lo, v any) int64 {
			if toI(v) < toI(lo) {
				return toI(lo)
			}
			return toI(v)
		},
	}
	plan := &Plan{Actor: t.Actor, Processes: t.Processes, ObservedSteps: len(t.Steps), Constants: map[string]string{}}
	for _, c := range sortedKeys(b.Constants) {
		v, err := render(b.Constants[c], nil, funcs)
		if err != nil {
			return nil, fmt.Errorf("constant %s: %w", c, err)
		}
		plan.Constants[c] = v
	}

	// 3. Calls.
	prevProc := ""
	for i := range t.Steps {
		s := &t.Steps[i]
		if prevProc != "" && s.Process != prevProc && b.Restart != "" {
			plan.Calls = append(plan.Calls, Call{Quint: b.Restart, Action: b.Restart, Args: map[string]string{},
				Inferred: fmt.Sprintf("restart: process %s -> %s", short(prevProc), short(s.Process))})
		} else if prevProc != "" && s.Process != prevProc {
			return nil, fmt.Errorf("step %d (%s): process changed from %s to %s and the binding has no restart action", i, s.Action, prevProc, s.Process)
		}
		prevProc = s.Process
		ab := b.Actions[s.Action]
		var params []string
		if m != nil {
			ps, ok := m.Actions[s.Action]
			if !ok {
				return nil, fmt.Errorf("step %d: %s is not an action of %s", i, s.Action, m.Module)
			}
			for _, p := range ps {
				if _, ok := ab.Args[p.Name]; !ok {
					return nil, fmt.Errorf("step %d: binding gives no template for %s", i, m.Signature(s.Action))
				}
				params = append(params, p.Name)
			}
		} else {
			params = sortedKeys(ab.Args)
		}
		outcome, err := b.outcome(ab, s)
		if err != nil {
			return nil, fmt.Errorf("step %d: %w", i, err)
		}
		if outcome == "skip" {
			continue
		}
		data := stepData(s, args[i], procIdx[s.Process], outcome)
		c := Call{Step: s, Action: s.Action, Args: map[string]string{}}
		var vals []string
		for _, p := range params {
			v, err := render(ab.Args[p], data, funcs)
			if err != nil {
				return nil, fmt.Errorf("step %d (%s, %s): arg %s: %w", i, s.Action, s.Source, p, err)
			}
			c.Args[p] = v
			vals = append(vals, v)
		}
		c.Quint = s.Action
		if len(vals) > 0 {
			c.Quint += "(" + strings.Join(vals, ", ") + ")"
		}
		if ab.Expect != "" {
			if c.Expect, err = render(ab.Expect, data, funcs); err != nil {
				return nil, fmt.Errorf("step %d (%s): expect: %w", i, s.Action, err)
			}
		}
		plan.Calls = append(plan.Calls, c)
	}
	return plan, nil
}

// ModuleSpec says what a generated replay module imports and checks.
type ModuleSpec struct {
	Spec       string   // the model file
	Module     string   // parameterised module, instantiated with Constants
	Constants  []string // "NAME = expr"
	Instance   string   // if set, import this (already instantiated) module instead
	Defs       string   // Quint helpers
	Invariants []string
	InitExpect string   // checked in the initial state, if set
	Header     []string // comment lines
}

// RenderModule writes the replay module for calls: a conformance run with
// each call's expect, and one run per invariant.
func RenderModule(ms ModuleSpec, calls []Call, qntPath string) (*Generated, error) {
	rel, err := filepath.Rel(filepath.Dir(qntPath), strings.TrimSuffix(ms.Spec, ".qnt"))
	if err != nil {
		return nil, err
	}
	g := &Generated{Module: "observed", LineStep: map[int]int{}, Calls: calls}
	var w strings.Builder
	line := 1
	pr := func(format string, a ...any) {
		s := fmt.Sprintf(format, a...)
		w.WriteString(s)
		w.WriteString("\n")
		line += strings.Count(s, "\n") + 1
	}
	pr("// Generated by quintgo from observed telemetry. Do not edit.")
	for _, h := range ms.Header {
		pr("// %s", h)
	}
	pr("module %s {", g.Module)
	if ms.Instance != "" {
		pr("  import %s.* from %q", ms.Instance, rel)
	} else {
		pr("  import %s(%s).* from %q", ms.Module, strings.Join(ms.Constants, ", "), rel)
	}
	if strings.TrimSpace(ms.Defs) != "" {
		pr("%s", indent(strings.TrimRight(ms.Defs, "\n"), "  "))
	}
	pr("")
	g.Tests.Conforms = "conformsTest"
	pr("  // The observed steps, each checked against the observed state.")
	pr("  run %s =", g.Tests.Conforms)
	if ms.InitExpect != "" {
		pr("    init.expect(%s)", ms.InitExpect)
	} else {
		pr("    init")
	}
	for i, c := range calls {
		pr("    // [%d] %s", i, describe(c))
		g.LineStep[line] = i
		if c.Expect != "" {
			pr("    .then(%s.expect(%s))", c.Quint, c.Expect)
		} else {
			pr("    .then(%s)", c.Quint)
		}
	}
	g.Tests.Invariants = map[string]string{}
	for _, inv := range ms.Invariants {
		name := inv + "InvTest"
		g.Tests.Invariants[inv] = name
		pr("")
		pr("  // The same steps; %s must hold in every state.", inv)
		pr("  run %s =", name)
		pr("    init.expect(%s)", inv)
		for i, c := range calls {
			g.LineStep[line] = i
			pr("    .then(%s.expect(%s))", c.Quint, inv)
		}
	}
	pr("}")
	g.Text = w.String()
	return g, nil
}

func (b *Binding) outcome(a Action, s *qtrace.Step) (string, error) {
	o := s.Outcome
	if o == "" {
		o = "ok"
	}
	if v, ok := a.Outcomes[o]; ok {
		if v == "call" {
			return "", nil // replay as if it succeeded
		}
		return v, nil
	}
	if o != "ok" && !a.usesOutcome() {
		// A failed step of an action whose call ignores the outcome would be
		// replayed as a success. Make the binding say what it means.
		return "", fmt.Errorf("%s failed (outcome %q: %s) but its templates do not use .outcome; map it in the action's outcomes (e.g. %s: skip)", s.Action, o, s.Error, o)
	}
	if v, ok := b.Outcomes[o]; ok {
		return v, nil
	}
	if o == "ok" {
		return "", nil // only an error if a template uses .outcome
	}
	return "", fmt.Errorf("%s: outcome %q has no mapping in the binding (error: %s)", s.Action, o, s.Error)
}

func describe(c Call) string {
	if c.Step == nil {
		if c.Inferred == "" {
			return c.Note
		}
		return "inferred " + c.Inferred
	}
	s := c.Step
	return fmt.Sprintf("seq %d %s %s", s.Seq, short(s.Process), s.String())
}

func short(s string) string {
	if len(s) > 24 {
		return s[:24] + "…"
	}
	return s
}

func indent(s, pfx string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = pfx + l
		}
	}
	return strings.Join(lines, "\n")
}

func toI(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	}
	return 0
}
