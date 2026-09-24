package binding

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
)

// ITFMeta is what quintgo stores under "#meta"."quintgo" of a model-level
// trace, so the trace can be validated without the binding.
type ITFMeta struct {
	Spec       string            `json:"spec"` // absolute path when written
	Module     string            `json:"module"`
	Constants  map[string]string `json:"constants"`
	Defs       string            `json:"defs,omitempty"`
	Invariants []string          `json:"invariants,omitempty"`
	Actor      string            `json:"actor,omitempty"`
	Processes  []string          `json:"processes,omitempty"`
}

// ObservedITF applies the binding to one actor's trace in-process (no Quint
// needed) and returns it as a model-level ITF trace in the shape `quint run
// --mbt` writes:
//
//	"mbt::actionTaken"  the model action
//	"mbt::nondetPicks"  its arguments by parameter name, as Some(value)
//	"quintgo::expect"   the binding's post-state check, rendered (a Quint bool)
//	"obs::<name>"       the observed projections recorded with the step
//
// It holds no model variables: those only exist once Quint replays it (see
// validate.ValidateITF, whose output ITF has both). Argument templates must
// render to Quint literals (records, Set/List/Map, tuples, constructors);
// a call of a model operator cannot be turned into a value without Quint.
func (b *Binding) ObservedITF(t *qtrace.Trace) ([]byte, error) {
	p, err := b.Plan(nil, t)
	if err != nil {
		return nil, err
	}
	return b.planITF(p)
}

func (b *Binding) planITF(p *Plan) ([]byte, error) {
	states := []map[string]any{{
		"#meta": map[string]any{"index": 0}, "mbt::actionTaken": "init", "mbt::nondetPicks": map[string]any{},
	}}
	varSet := map[string]bool{}
	for i, c := range p.Calls {
		picks := map[string]any{}
		for name, expr := range c.Args {
			v, err := itf.ParseQuint(expr)
			if err != nil {
				return nil, fmt.Errorf("call [%d] %s, parameter %s: %w", i, c.Action, name, err)
			}
			picks[name] = itf.Some(v)
		}
		st := map[string]any{
			"#meta":            map[string]any{"index": i + 1, "observed": describe(c)},
			"mbt::actionTaken": c.Action,
			"mbt::nondetPicks": picks,
		}
		if c.Expect != "" {
			st["quintgo::expect"] = c.Expect
			varSet["quintgo::expect"] = true
		}
		if c.Step != nil {
			for k, x := range c.Step.Obs {
				v, err := itf.FromGo(x)
				if err != nil {
					return nil, fmt.Errorf("call [%d] obs.%s: %w", i, k, err)
				}
				st["obs::"+k] = v
				varSet["obs::"+k] = true
			}
		}
		states = append(states, st)
	}
	vars := make([]string, 0, len(varSet))
	for v := range varSet {
		vars = append(vars, v)
	}
	sort.Strings(vars)
	doc := map[string]any{
		"#meta": map[string]any{
			"format":             "ITF",
			"format-description": "https://apalache-mc.org/docs/adr/015adr-trace.html",
			"source":             "quintgo",
			"description":        fmt.Sprintf("observed trace of actor %q, emitted by quintgo on %s", p.Actor, time.Now().UTC().Format(time.RFC3339)),
			"quintgo": ITFMeta{Spec: b.SpecPath(), Module: b.Module, Constants: p.Constants, Defs: b.Defs,
				Invariants: b.Invariants, Actor: p.Actor, Processes: p.Processes},
		},
		"vars":   vars,
		"states": states,
	}
	return json.MarshalIndent(doc, "", " ")
}

// StepsToITF reconstructs steps (any order, any recorders) and returns one
// model-level ITF trace per actor, plus the reconstruction issues.
func (b *Binding) StepsToITF(steps []qtrace.Step) (map[string][]byte, []qtrace.Issue, error) {
	traces, issues := qtrace.Reconstruct(steps)
	out := map[string][]byte{}
	for _, t := range traces {
		issues = append(issues, t.Issues...)
		data, err := b.ObservedITF(t)
		if err != nil {
			return nil, issues, fmt.Errorf("actor %s: %w", t.Actor, err)
		}
		out[t.Actor] = data
	}
	return out, issues, nil
}

// ReadITFMeta returns the quintgo section of a trace's #meta, if any.
func ReadITFMeta(meta map[string]any) (*ITFMeta, bool) {
	raw, ok := meta["quintgo"]
	if !ok {
		return nil, false
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, false
	}
	var m ITFMeta
	if json.Unmarshal(b, &m) != nil {
		return nil, false
	}
	return &m, true
}
