package validate

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/quintgo/binding"
	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
)

// ITFOptions says what to check a model-level ITF trace against. Every field
// is optional when the trace was written by quintgo (its #meta names the
// spec, module, constants, defs and invariants); set them to validate a
// trace from elsewhere, e.g. `quint run --mbt`.
type ITFOptions struct {
	Options
	Binding    *binding.Binding // supplies spec, module, defs and invariants when set
	Spec       string           // the model (overrides meta and binding)
	Module     string           // the parameterised module
	Instance   string           // take the constants from this instance module of Spec (e.g. currentDesign)
	Invariants []string         // overrides
	// NoVarCheck turns off the comparison of model variables present in the
	// trace's states with the model's state after each step.
	NoVarCheck bool
}

// ValidateITF replays a model-level ITF trace in Quint: each state's
// mbt::actionTaken is called with its mbt::nondetPicks as arguments (picks
// are matched to parameters by name). After each step it checks
//   - quintgo::expect, the observed projection the binding rendered, and
//   - every model variable the state carries (equality), so a trace from
//     `quint run --mbt`, or one quintgo validated earlier, is checked state
//     by state;
//
// and it checks the invariants in every state.
func ValidateITF(ctx context.Context, tr *itf.Trace, opt ITFOptions) (*Result, error) {
	meta, _ := binding.ReadITFMeta(tr.Meta)
	if meta == nil {
		meta = &binding.ITFMeta{}
	}
	spec, module, defs, invs := meta.Spec, meta.Module, meta.Defs, meta.Invariants
	if b := opt.Binding; b != nil {
		spec, module, defs, invs = b.SpecPath(), b.Module, b.Defs, b.Invariants
	}
	if opt.Spec != "" {
		spec = opt.Spec
	}
	if opt.Module != "" {
		module = opt.Module
	}
	if opt.Invariants != nil {
		invs = opt.Invariants
	}
	if spec == "" || module == "" {
		return nil, fmt.Errorf("the trace does not say which model it is for: give a binding, or spec and module")
	}
	if !filepath.IsAbs(spec) {
		if abs, err := filepath.Abs(spec); err == nil {
			spec = abs
		}
	}
	if opt.Instance == "" && len(meta.Constants) == 0 && opt.Binding == nil {
		return nil, fmt.Errorf("no constants for %s: validate against an instance module (Instance) or a quintgo-written trace", module)
	}
	o, err := opt.Options.resolve()
	if err != nil {
		return nil, err
	}
	m, err := binding.LoadModel(o.Quint, spec, module)
	if err != nil {
		return nil, err
	}
	isVar := map[string]bool{}
	for _, v := range m.Vars {
		isVar[v] = true
	}
	varCheck := func(st itf.State) []string {
		if opt.NoVarCheck {
			return nil
		}
		var names []string
		for k := range st.Vars {
			if isVar[k] {
				names = append(names, k)
			}
		}
		sort.Strings(names)
		var out []string
		for _, k := range names {
			out = append(out, fmt.Sprintf("%s == %s", k, st.Vars[k].Quint()))
		}
		return out
	}
	if len(tr.States) == 0 {
		return nil, fmt.Errorf("empty trace")
	}
	var calls []binding.Call
	for i, st := range tr.States[1:] {
		idx := i + 1
		ps, ok := m.Actions[st.Action]
		if !ok {
			return nil, fmt.Errorf("state %d: %q is not an action of %s", idx, st.Action, module)
		}
		c := binding.Call{Action: st.Action, Args: map[string]string{}, Note: fmt.Sprintf("state %d", idx)}
		if obs, ok := st.Meta["observed"].(string); ok {
			c.Note += ": " + obs
		}
		var vals []string
		for _, p := range ps {
			v, ok := st.Picks[p.Name]
			if !ok {
				return nil, fmt.Errorf("state %d: %s has no nondet pick named %s (picks are matched to parameters by name)", idx, m.Signature(st.Action), p.Name)
			}
			c.Args[p.Name] = v.Quint()
			vals = append(vals, v.Quint())
		}
		c.Quint = st.Action
		if len(vals) > 0 {
			c.Quint += "(" + strings.Join(vals, ", ") + ")"
		}
		var checks []string
		if e, ok := st.Extra["quintgo::expect"]; ok && e.Kind == itf.Str {
			checks = append(checks, e.S)
		}
		checks = append(checks, varCheck(st)...)
		if len(checks) == 1 {
			c.Expect = checks[0]
		} else if len(checks) > 1 {
			c.Expect = "and { " + strings.Join(checks, ", ") + " }"
		}
		calls = append(calls, c)
	}
	ms := binding.ModuleSpec{Spec: spec, Module: module, Defs: defs, Invariants: invs,
		Header: []string{fmt.Sprintf("replay of a model-level ITF trace, %d states", len(tr.States))}}
	if opt.Instance != "" {
		im, consts, err := binding.InstanceConstants(spec, opt.Instance)
		if err != nil {
			return nil, err
		}
		if im != module {
			return nil, fmt.Errorf("instance %s instantiates %s, not %s", opt.Instance, im, module)
		}
		ms.Constants = consts
		ms.Header = append(ms.Header, "constants of instance "+opt.Instance)
	} else {
		for _, k := range sortedKeys(meta.Constants) {
			ms.Constants = append(ms.Constants, fmt.Sprintf("%s = %s", k, meta.Constants[k]))
		}
	}
	if init := varCheck(tr.States[0]); len(init) > 0 {
		ms.InitExpect = "and { " + strings.Join(init, ", ") + " }"
	}
	actor := meta.Actor
	if actor == "" {
		actor = "itf"
	}
	base := "replay_" + sanitize(actor)
	qnt := filepath.Join(o.Dir, base+".qnt")
	g, err := binding.RenderModule(ms, calls, qnt)
	if err != nil {
		return nil, err
	}
	res := &Result{Actor: actor}
	return res, run(ctx, g, qnt, base, invs, o, res)
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
