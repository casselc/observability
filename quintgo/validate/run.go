package validate

import (
	"context"
	"fmt"

	"github.com/chucklehead-dev/oscope/spike/quintgo/binding"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
)

// Checker validates steps against one binding. Create it once (it typechecks
// the model) and call Check per run: this is the entry point for Go tests.
type Checker struct {
	Binding *binding.Binding
	Model   *binding.Model
	Options Options
}

// NewChecker loads a binding file, typechecks its model and lints the
// binding against it.
func NewChecker(bindingPath string, opt Options) (*Checker, error) {
	b, err := binding.Load(bindingPath)
	if err != nil {
		return nil, err
	}
	m, err := binding.LoadModel(opt.Quint, b.SpecPath(), b.Module)
	if err != nil {
		return nil, err
	}
	if errs := b.Lint(m); len(errs) > 0 {
		return nil, fmt.Errorf("binding %s does not fit %s: %v", bindingPath, b.Module, errs)
	}
	return &Checker{Binding: b, Model: m, Options: opt}, nil
}

// Report is the result of Check: global reconstruction issues and one
// Result per actor.
type Report struct {
	Issues []qtrace.Issue
	Actors []*Result
}

// OK is true when every actor's trace conforms and satisfies the invariants.
func (r *Report) OK() bool {
	for _, a := range r.Actors {
		if !a.OK() {
			return false
		}
	}
	return len(r.Actors) > 0
}

func (r *Report) Summary() string {
	s := ""
	for _, i := range r.Issues {
		s += fmt.Sprintf("issue: %s\n", i)
	}
	for _, a := range r.Actors {
		s += a.Summary()
	}
	return s
}

// Check reconstructs per-actor traces from steps (in any order, from any
// number of recorders) and validates each.
func (c *Checker) Check(ctx context.Context, steps []qtrace.Step) (*Report, error) {
	traces, issues := qtrace.Reconstruct(steps)
	rep := &Report{Issues: issues}
	for _, t := range traces {
		r, err := Validate(ctx, c.Binding, c.Model, t, c.Options)
		if err != nil {
			return rep, fmt.Errorf("actor %s: %w", t.Actor, err)
		}
		rep.Actors = append(rep.Actors, r)
	}
	return rep, nil
}
