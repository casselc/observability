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
	Quint    string       // e.g. writeTable(px(1, 1, 3, 1, Encoded), Ok)
	Expect   string       // post-state check, or ""
	Step     *qtrace.Step // the observed step; nil for inferred steps
	Inferred string       // why the step was inserted ("restart"), if inferred
}

// Generate builds the replay module for one actor's trace. qntPath is where
// the module will be written (the model import is made relative to it).
func (b *Binding) Generate(m *Model, t *qtrace.Trace, qntPath string) (*Generated, error) {
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
	var consts []string
	for _, c := range sortedKeys(b.Constants) {
		v, err := render(b.Constants[c], nil, funcs)
		if err != nil {
			return nil, fmt.Errorf("constant %s: %w", c, err)
		}
		consts = append(consts, fmt.Sprintf("%s = %s", c, v))
	}

	// 3. Calls.
	g := &Generated{Module: "observed", LineStep: map[int]int{}}
	prevProc := ""
	for i := range t.Steps {
		s := &t.Steps[i]
		if prevProc != "" && s.Process != prevProc && b.Restart != "" {
			g.Calls = append(g.Calls, Call{Quint: b.Restart, Inferred: fmt.Sprintf("restart: process %s -> %s", short(prevProc), short(s.Process))})
		} else if prevProc != "" && s.Process != prevProc {
			return nil, fmt.Errorf("step %d (%s): process changed from %s to %s and the binding has no restart action", i, s.Action, prevProc, s.Process)
		}
		prevProc = s.Process
		ps, ok := m.Actions[s.Action]
		if !ok {
			return nil, fmt.Errorf("step %d: %s is not an action of %s", i, s.Action, m.Module)
		}
		ab := b.Actions[s.Action]
		outcome, err := b.outcome(ab, s)
		if err != nil {
			return nil, fmt.Errorf("step %d: %w", i, err)
		}
		if outcome == "skip" {
			continue
		}
		data := stepData(s, args[i], procIdx[s.Process], outcome)
		var call strings.Builder
		call.WriteString(s.Action)
		if len(ps) > 0 {
			call.WriteString("(")
			for j, p := range ps {
				tpl, ok := ab.Args[p.Name]
				if !ok {
					return nil, fmt.Errorf("step %d: binding gives no template for %s", i, m.Signature(s.Action))
				}
				v, err := render(tpl, data, funcs)
				if err != nil {
					return nil, fmt.Errorf("step %d (%s, %s): arg %s: %w", i, s.Action, s.Source, p.Name, err)
				}
				if j > 0 {
					call.WriteString(", ")
				}
				call.WriteString(v)
			}
			call.WriteString(")")
		}
		c := Call{Quint: call.String(), Step: s}
		if ab.Expect != "" {
			if c.Expect, err = render(ab.Expect, data, funcs); err != nil {
				return nil, fmt.Errorf("step %d (%s): expect: %w", i, s.Action, err)
			}
		}
		g.Calls = append(g.Calls, c)
	}

	// 4. Text.
	rel, err := filepath.Rel(filepath.Dir(qntPath), strings.TrimSuffix(b.SpecPath(), ".qnt"))
	if err != nil {
		return nil, err
	}
	var w strings.Builder
	line := 1
	pr := func(format string, a ...any) {
		s := fmt.Sprintf(format, a...)
		w.WriteString(s)
		w.WriteString("\n")
		line += strings.Count(s, "\n") + 1
	}
	pr("// Generated by quintgo from observed telemetry. Do not edit.")
	pr("// actor %q, %d observed steps, processes %v", t.Actor, len(t.Steps), t.Processes)
	pr("module %s {", g.Module)
	pr("  import %s(%s).* from %q", b.Module, strings.Join(consts, ", "), rel)
	if strings.TrimSpace(b.Defs) != "" {
		pr("%s", indent(strings.TrimRight(b.Defs, "\n"), "  "))
	}
	pr("")
	g.Tests.Conforms = "conformsTest"
	pr("  // The observed steps, each checked against the observed state.")
	pr("  run %s =", g.Tests.Conforms)
	pr("    init")
	for i, c := range g.Calls {
		pr("    // [%d] %s", i, describe(c))
		g.LineStep[line] = i
		if c.Expect != "" {
			pr("    .then(%s.expect(%s))", c.Quint, c.Expect)
		} else {
			pr("    .then(%s)", c.Quint)
		}
	}
	g.Tests.Invariants = map[string]string{}
	for _, inv := range b.Invariants {
		name := inv + "InvTest"
		g.Tests.Invariants[inv] = name
		pr("")
		pr("  // The same steps; %s must hold in every state.", inv)
		pr("  run %s =", name)
		pr("    init.expect(%s)", inv)
		for i, c := range g.Calls {
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
