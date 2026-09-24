// Package binding is the declarative, per-project half of quintgo: a YAML
// file that says how observed steps become calls of a Quint model's actions.
// It holds no Go code, so applying quintgo to a new project means writing
// annotations and one binding file (see the README's "applying to a new
// project").
package binding

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"text/template/parse"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
	"gopkg.in/yaml.v3"
)

// Binding is the parsed binding file.
type Binding struct {
	// Spec is the .qnt file (relative to the binding file) and Module the
	// parameterised module in it to instantiate.
	Spec   string `yaml:"spec"`
	Module string `yaml:"module"`
	// Constants of the instance, as Quint expressions. They are templates
	// over the whole trace (see Stats), so bounds can follow what was seen.
	Constants map[string]string `yaml:"constants"`
	// Intern maps an argument name to a scope ("actor" or "process"). Its
	// observed values (opaque ids like "g20260924T101500") are renamed
	// 1, 2, 3, ... in order of first appearance: the abstraction function
	// from implementation identifiers to the model's small integers.
	Intern map[string]string `yaml:"intern"`
	// Outcomes maps a recorded quint.outcome to a Quint expression, available
	// to templates as .outcome. Actions may override it.
	Outcomes map[string]string `yaml:"outcomes"`
	// Restart is the model action inserted where an actor's process changes:
	// the crash a dead process cannot report. Empty: a process change is an error.
	Restart string `yaml:"restart"`
	// Defs is Quint text added to the generated module (helpers for templates).
	Defs string `yaml:"defs"`
	// Actions maps a model action to the templates that build its call.
	Actions map[string]Action `yaml:"actions"`
	// Invariants are vals of the model checked in every observed state.
	Invariants []string `yaml:"invariants"`

	Dir string `yaml:"-"` // directory of the binding file
}

// Action says how to call one model action from an observed step.
type Action struct {
	// Args maps each model parameter to a Quint expression template over the
	// step (see StepData). Every parameter of the action must be given.
	Args map[string]string `yaml:"args"`
	// Expect is a Quint boolean over the post-state that must hold after the
	// step: the observed state projection (quint.obs.*) checked against the
	// model's state.
	Expect string `yaml:"expect"`
	// Outcomes overrides the binding's outcome map for this action. "skip"
	// drops steps with that outcome (the model has no such step); "call"
	// replays them like successful ones. A non-ok outcome of an action whose
	// templates ignore .outcome must be mapped here, or generation fails.
	Outcomes map[string]string `yaml:"outcomes"`
}

// Load reads a binding file.
func Load(path string) (*Binding, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var bd Binding
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&bd); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	abs, _ := filepath.Abs(path)
	bd.Dir = filepath.Dir(abs)
	if bd.Spec == "" || bd.Module == "" {
		return nil, fmt.Errorf("%s: spec and module are required", path)
	}
	return &bd, nil
}

// SpecPath is the absolute path of the model.
func (b *Binding) SpecPath() string {
	if filepath.IsAbs(b.Spec) {
		return b.Spec
	}
	return filepath.Join(b.Dir, b.Spec)
}

// Lint checks the binding against the model: every bound action exists and
// is given exactly its parameters, the restart action exists and takes none,
// invariants are vals, templates parse, and every constant is set. It returns
// all problems found.
func (b *Binding) Lint(m *Model) []string {
	var errs []string
	for _, name := range sortedKeys(b.Actions) {
		a := b.Actions[name]
		ps, ok := m.Actions[name]
		if !ok {
			errs = append(errs, fmt.Sprintf("action %s: not an action of %s (have: %s)", name, m.Module, strings.Join(m.ActionNames(), ", ")))
			continue
		}
		want := map[string]bool{}
		for _, p := range ps {
			want[p.Name] = true
			if _, ok := a.Args[p.Name]; !ok {
				errs = append(errs, fmt.Sprintf("action %s: parameter %s (%s) has no template; signature %s", name, p.Name, p.Type, m.Signature(name)))
			}
		}
		for arg, tpl := range a.Args {
			if !want[arg] {
				errs = append(errs, fmt.Sprintf("action %s: %q is not a parameter; signature %s", name, arg, m.Signature(name)))
			}
			if _, err := parseTpl(tpl); err != nil {
				errs = append(errs, fmt.Sprintf("action %s arg %s: %v", name, arg, err))
			}
		}
		if a.Expect != "" {
			if _, err := parseTpl(a.Expect); err != nil {
				errs = append(errs, fmt.Sprintf("action %s expect: %v", name, err))
			}
		}
	}
	if b.Restart != "" {
		if ps, ok := m.Actions[b.Restart]; !ok {
			errs = append(errs, fmt.Sprintf("restart: %s is not an action of %s", b.Restart, m.Module))
		} else if len(ps) != 0 {
			errs = append(errs, fmt.Sprintf("restart: %s takes parameters; a restart action must take none", m.Signature(b.Restart)))
		}
	}
	for _, inv := range b.Invariants {
		if !m.Vals[inv] {
			errs = append(errs, fmt.Sprintf("invariant %s: not a val of %s", inv, m.Module))
		}
	}
	for c := range m.Consts {
		if _, ok := b.Constants[c]; !ok {
			errs = append(errs, fmt.Sprintf("constant %s: no value (type %s)", c, m.Consts[c]))
		}
	}
	for c := range b.Constants {
		if _, ok := m.Consts[c]; !ok {
			errs = append(errs, fmt.Sprintf("constant %s: not a const of %s", c, m.Module))
		}
	}
	for arg, scope := range b.Intern {
		if scope != "actor" && scope != "process" {
			errs = append(errs, fmt.Sprintf("intern %s: scope must be actor or process, not %q", arg, scope))
		}
	}
	return errs
}

// Fields returns the step fields (.name) a template refers to: what the
// instrumentation must record for this action.
func (a Action) Fields() []string {
	set := map[string]bool{}
	for _, t := range a.Args {
		collectFields(t, set)
	}
	collectFields(a.Expect, set)
	delete(set, "outcome")
	delete(set, "process")
	delete(set, "action")
	delete(set, "seq")
	return sortedKeys(set)
}

func (a Action) usesOutcome() bool {
	set := map[string]bool{}
	for _, t := range a.Args {
		collectFields(t, set)
	}
	collectFields(a.Expect, set)
	return set["outcome"]
}

func parseTpl(s string) (*template.Template, error) {
	return template.New("").Option("missingkey=error").Parse(s)
}

func collectFields(s string, set map[string]bool) {
	t, err := parseTpl(s)
	if err != nil || t.Tree == nil {
		return
	}
	var walk func(n parse.Node)
	walk = func(n parse.Node) {
		switch x := n.(type) {
		case *parse.ListNode:
			if x != nil {
				for _, c := range x.Nodes {
					walk(c)
				}
			}
		case *parse.ActionNode:
			walk(x.Pipe)
		case *parse.PipeNode:
			if x != nil {
				for _, c := range x.Cmds {
					walk(c)
				}
			}
		case *parse.CommandNode:
			for _, a := range x.Args {
				walk(a)
			}
		case *parse.FieldNode:
			set[strings.Join(x.Ident, ".")] = true
		case *parse.IfNode:
			walk(x.Pipe)
			walk(x.List)
			walk(x.ElseList)
		}
	}
	walk(t.Tree.Root)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// quintLit renders an observed value as a Quint literal.
func quintLit(v any) string {
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	case bool:
		return strconv.FormatBool(x)
	case float64:
		return strconv.FormatInt(int64(x), 10)
	case string:
		return strconv.Quote(x)
	case qlit:
		return string(x)
	}
	return strconv.Quote(fmt.Sprint(v))
}

// qlit is a value that is already Quint text.
type qlit string

func (q qlit) String() string { return string(q) }

// StepData is what an action template sees: the step's arguments by name
// (interned where configured, rendered as Quint literals), plus
//
//	.process  the actor's incarnation, 1-based
//	.outcome  the outcome, mapped through the outcome table
//	.obs.X    an observed projection
//	.action   .seq
func stepData(s *qtrace.Step, args map[string]any, process int, outcome string) map[string]any {
	d := map[string]any{}
	for k, v := range args {
		d[k] = qlit(quintLit(v))
	}
	obs := map[string]any{}
	for k, v := range s.Obs {
		obs[k] = qlit(quintLit(v))
	}
	d["obs"] = obs
	d["process"] = qlit(strconv.Itoa(process))
	d["outcome"] = qlit(outcome)
	d["action"] = s.Action
	d["seq"] = s.Seq
	return d
}

func render(tpl string, data any, funcs template.FuncMap) (string, error) {
	t, err := template.New("").Option("missingkey=error").Funcs(funcs).Parse(tpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
