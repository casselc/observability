package binding

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// Annotation is one //quint:action directive found in Go source.
type Annotation struct {
	Pos    string // file:line
	Func   string
	Action string
	Keys   map[string]string // key -> Go expression
}

// ScanAnnotations finds //quint:action directives on functions under dir.
func ScanAnnotations(dir string) ([]Annotation, error) {
	var out []Annotation
	fset := token.NewFileSet()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "vendor" || strings.HasPrefix(d.Name(), ".")) && path != dir {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Doc == nil {
				continue
			}
			for _, c := range fn.Doc.List {
				if !strings.HasPrefix(c.Text, "//quint:action") || (len(c.Text) > 14 && c.Text[14] != ' ') {
					continue
				}
				a := Annotation{Pos: fset.Position(c.Pos()).String(), Func: fn.Name.Name, Keys: map[string]string{}}
				for _, arg := range splitDirective(strings.TrimPrefix(c.Text, "//quint:action")) {
					k, v, _ := strings.Cut(arg, ":")
					if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
						v = v[1 : len(v)-1]
					}
					if k == "action" {
						a.Action = v
					} else {
						a.Keys[k] = v
					}
				}
				out = append(out, a)
			}
		}
		return nil
	})
	return out, err
}

// splitDirective splits on spaces outside quotes, as Orchestrion does.
func splitDirective(s string) []string {
	var out []string
	var cur strings.Builder
	var q byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case q != 0:
			if c == q {
				q = 0
			}
			cur.WriteByte(c)
		case c == '"' || c == '\'':
			q = c
			cur.WriteByte(c)
		case c == ' ' || c == '\t':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// LintAnnotations checks annotations against the model and the binding:
// each names an action of the model, and records every field the binding's
// templates for that action use. It also reports bound actions that no
// annotation emits (a warning: they may be emitted by explicit calls).
func (b *Binding) LintAnnotations(m *Model, anns []Annotation) (errs, warns []string) {
	emitted := map[string]bool{}
	for _, a := range anns {
		if a.Action == "" {
			errs = append(errs, fmt.Sprintf("%s: %s: //quint:action without action:NAME", a.Pos, a.Func))
			continue
		}
		emitted[a.Action] = true
		if _, ok := m.Actions[a.Action]; !ok {
			errs = append(errs, fmt.Sprintf("%s: %s: %s is not an action of %s", a.Pos, a.Func, a.Action, m.Module))
			continue
		}
		ab, ok := b.Actions[a.Action]
		if !ok && len(m.Actions[a.Action]) > 0 {
			errs = append(errs, fmt.Sprintf("%s: %s: the binding has no entry for %s", a.Pos, a.Func, m.Signature(a.Action)))
			continue
		}
		for _, f := range ab.Fields() {
			if _, ok := a.Keys[f]; !ok { // obs fields are recorded as obs.<name> too
				errs = append(errs, fmt.Sprintf("%s: %s: the binding for %s uses .%s, which this annotation does not record", a.Pos, a.Func, a.Action, f))
			}
		}
	}
	var bound []string
	for name := range b.Actions {
		bound = append(bound, name)
	}
	sort.Strings(bound)
	for _, name := range bound {
		if !emitted[name] {
			warns = append(warns, fmt.Sprintf("action %s is bound but no //quint:action emits it (explicit qobs.Record calls are not scanned)", name))
		}
	}
	return errs, warns
}
