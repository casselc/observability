// Command quintgo checks Go programs against Quint models from their
// telemetry.
//
//	quintgo scaffold -spec model.qnt -module M          print a binding skeleton for M
//	quintgo lint     -binding b.yaml [-src DIR]         check a binding (and annotations) against the model
//	quintgo steps    FILE...                            print the reconstructed per-actor traces
//	quintgo validate [-binding b.yaml] [-dir OUT] FILE...
//	                                                    reconstruct (or read), replay in Quint, report
//	quintgo itf      -binding b.yaml -out DIR FILE...   recorded steps -> model-level ITF, per actor
//
// FILE is detected by content: OTLP/JSON (e.g. the collector's file
// exporter output), a native step log (qobs.JSONLSink), or a model-level
// ITF trace (quintgo's, or `quint run --mbt` output with -spec/-module/-instance).
// Exit status of validate: 0 all actors conform and satisfy the invariants,
// 1 some do not, 2 usage or tool error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/quintgo/binding"
	"github.com/chucklehead-dev/oscope/spike/quintgo/load"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
	"github.com/chucklehead-dev/oscope/spike/quintgo/validate"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	code := 0
	switch os.Args[1] {
	case "scaffold":
		err = scaffold(os.Args[2:])
	case "lint":
		code, err = lint(os.Args[2:])
	case "steps":
		err = steps(os.Args[2:])
	case "validate":
		code, err = runValidate(os.Args[2:])
	case "itf":
		err = toITF(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "quintgo:", err)
		os.Exit(2)
	}
	os.Exit(code)
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  quintgo scaffold -spec model.qnt -module M
  quintgo lint     -binding b.yaml [-src DIR]
  quintgo steps    FILE.jsonl...
  quintgo validate [-binding b.yaml] [-dir OUT] [-backend rust|typescript] [-format auto|otlp|steps|itf]
                   [-spec M.qnt -module M -instance I] [-novars] FILE...
  quintgo itf      -binding b.yaml -out DIR FILE...      recorded steps -> model-level ITF per actor`)
	os.Exit(2)
}

func scaffold(args []string) error {
	fset := flag.NewFlagSet("scaffold", flag.ExitOnError)
	spec := fset.String("spec", "", "Quint spec")
	module := fset.String("module", "", "parameterised module to bind")
	quint := fset.String("quint", "quint", "quint binary")
	fset.Parse(args)
	if *spec == "" || *module == "" {
		return fmt.Errorf("scaffold needs -spec and -module")
	}
	m, err := binding.LoadModel(*quint, *spec, *module)
	if err != nil {
		return err
	}
	fmt.Printf("# quintgo binding for %s (generated skeleton: fill in the templates)\nspec: %s   # relative to this file\nmodule: %s\n\nconstants:\n", *module, *spec, *module)
	for _, c := range sortedKeys(m.Consts) {
		fmt.Printf("  %s: ''   # %s\n", c, m.Consts[c])
	}
	fmt.Printf("\nintern: {}      # arg: actor|process, for opaque ids the model names by small integers\n")
	fmt.Printf("outcomes: {}    # ok/error/<custom> -> Quint expression, used as {{.outcome}}\n")
	fmt.Printf("restart: ''     # parameterless action inserted when an actor's process changes\n\nactions:\n")
	for _, a := range m.ActionNames() {
		if a == "init" || a == "step" {
			continue // the state machine itself, not steps
		}
		fmt.Printf("  %s:   # %s\n", a, m.Signature(a))
		if ps := m.Actions[a]; len(ps) > 0 {
			fmt.Printf("    args:\n")
			for _, p := range ps {
				fmt.Printf("      %s: ''   # %s\n", p.Name, p.Type)
			}
		}
		fmt.Printf("    # expect: ''  # Quint bool over the post-state, from observed .obs.<name>\n")
	}
	fmt.Printf("\ninvariants: []  # vals of %s: %s\n", *module, strings.Join(sortedKeys(m.Vals), ", "))
	return nil
}

func lint(args []string) (int, error) {
	fset := flag.NewFlagSet("lint", flag.ExitOnError)
	bpath := fset.String("binding", "", "binding file")
	src := fset.String("src", "", "Go source tree to scan for //quint:action")
	quint := fset.String("quint", "quint", "quint binary")
	fset.Parse(args)
	b, err := binding.Load(*bpath)
	if err != nil {
		return 2, err
	}
	m, err := binding.LoadModel(*quint, b.SpecPath(), b.Module)
	if err != nil {
		return 2, err
	}
	errs := b.Lint(m)
	var warns []string
	if *src != "" {
		anns, err := binding.ScanAnnotations(*src)
		if err != nil {
			return 2, err
		}
		fmt.Printf("%d //quint:action annotations under %s\n", len(anns), *src)
		for _, a := range anns {
			fmt.Printf("  %-28s %-14s %s\n", a.Pos[strings.LastIndex(a.Pos, "/")+1:], a.Action, a.Func)
		}
		e, w := b.LintAnnotations(m, anns)
		errs, warns = append(errs, e...), w
	}
	for _, w := range warns {
		fmt.Println("warning:", w)
	}
	for _, e := range errs {
		fmt.Println("error:", e)
	}
	if len(errs) > 0 {
		return 1, nil
	}
	fmt.Printf("binding %s fits %s (%d actions bound)\n", *bpath, b.Module, len(b.Actions))
	return 0, nil
}

func readSteps(files []string) ([]qtrace.Step, error) {
	var all []qtrace.Step
	for _, f := range files {
		in, err := load.File(f, load.Unknown)
		if err != nil {
			return nil, err
		}
		if in.Format == load.ITF {
			return nil, fmt.Errorf("%s is an ITF trace, not recorded steps", f)
		}
		all = append(all, in.Steps...)
	}
	return all, nil
}

func steps(args []string) error {
	all, err := readSteps(args)
	if err != nil {
		return err
	}
	traces, issues := qtrace.Reconstruct(all)
	for _, i := range issues {
		fmt.Println("issue:", i)
	}
	for _, t := range traces {
		fmt.Printf("actor %s: %d steps, processes %v\n", t.Actor, len(t.Steps), t.Processes)
		for _, i := range t.Issues {
			fmt.Println("  issue:", i)
		}
		for i, s := range t.Steps {
			fmt.Printf("  [%2d] seq %-3d %-10.10s %s\n", i, s.Seq, s.Process, s.String())
		}
	}
	return nil
}

func runValidate(args []string) (int, error) {
	fset := flag.NewFlagSet("validate", flag.ExitOnError)
	bpath := fset.String("binding", "", "binding file (optional for ITF written by quintgo)")
	dir := fset.String("dir", "", "where to write generated modules and ITF (default: a temp dir)")
	backend := fset.String("backend", "", "quint test backend (default $QUINTGO_BACKEND or rust)")
	quint := fset.String("quint", "quint", "quint binary")
	format := fset.String("format", "auto", "input format: auto, otlp, steps (native step log) or itf")
	spec := fset.String("spec", "", "ITF input: the model, if neither the trace nor a binding names it")
	module := fset.String("module", "", "ITF input: the parameterised module")
	instance := fset.String("instance", "", "ITF input: replay against this instance module (e.g. currentDesign), e.g. for quint run --mbt traces")
	novars := fset.Bool("novars", false, "ITF input: do not compare model variables carried by the trace")
	verbose := fset.Bool("v", false, "print quint's output")
	fset.Parse(args)
	if fset.NArg() == 0 {
		usage()
	}
	opts := validate.Options{Quint: *quint, Backend: *backend, Dir: *dir}
	var b *binding.Binding
	if *bpath != "" {
		var err error
		if b, err = binding.Load(*bpath); err != nil {
			return 2, err
		}
	}
	var steps []qtrace.Step
	var itfs []*load.Input
	for _, f := range fset.Args() {
		in, err := load.File(f, load.Format(*format))
		if err != nil {
			return 2, err
		}
		fmt.Printf("input %s: %s\n", f, map[load.Format]string{load.OTLP: "OTLP/JSON spans", load.StepLog: "native step log",
			load.ITF: "model-level ITF", load.Unknown: "empty"}[in.Format])
		if in.Format == load.ITF {
			itfs = append(itfs, in)
		} else {
			steps = append(steps, in.Steps...)
		}
	}
	ok := true
	if len(steps) > 0 || len(itfs) == 0 {
		if *bpath == "" {
			return 2, fmt.Errorf("recorded steps need -binding")
		}
		c, err := validate.NewChecker(*bpath, opts)
		if err != nil {
			return 2, err
		}
		rep, err := c.Check(context.Background(), steps)
		if err != nil {
			return 2, err
		}
		fmt.Print(rep.Summary())
		if *verbose {
			for _, a := range rep.Actors {
				fmt.Println(a.Output)
			}
		}
		ok = ok && rep.OK()
	}
	for i, in := range itfs {
		o := opts
		if o.Dir != "" && len(itfs) > 1 {
			o.Dir = fmt.Sprintf("%s/itf%d", o.Dir, i)
		}
		r, err := validate.ValidateITF(context.Background(), in.ITF, validate.ITFOptions{Options: o, Binding: b,
			Spec: *spec, Module: *module, Instance: *instance, NoVarCheck: *novars})
		if err != nil {
			return 2, fmt.Errorf("%s: %w", in.Path, err)
		}
		fmt.Print(r.Summary())
		if *verbose {
			fmt.Println(r.Output)
		}
		ok = ok && r.OK()
	}
	if ok {
		fmt.Println("RESULT: trace conforms to the model and satisfies the invariants")
		return 0, nil
	}
	fmt.Println("RESULT: VIOLATION")
	return 1, nil
}

// toITF writes one model-level ITF per actor from recorded steps.
func toITF(args []string) error {
	fset := flag.NewFlagSet("itf", flag.ExitOnError)
	bpath := fset.String("binding", "", "binding file")
	out := fset.String("out", ".", "output directory: <actor>.itf.json per actor")
	fset.Parse(args)
	if *bpath == "" || fset.NArg() == 0 {
		usage()
	}
	b, err := binding.Load(*bpath)
	if err != nil {
		return err
	}
	all, err := readSteps(fset.Args())
	if err != nil {
		return err
	}
	traces, issues, err := b.StepsToITF(all)
	for _, i := range issues {
		fmt.Println("issue:", i)
	}
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	for actor, data := range traces {
		f := filepath.Join(*out, strings.Map(func(r rune) rune {
			if r == '/' || r == ' ' {
				return '_'
			}
			return r
		}, actor)+".itf.json")
		if err := os.WriteFile(f, data, 0o644); err != nil {
			return err
		}
		fmt.Printf("actor %s -> %s\n", actor, f)
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
