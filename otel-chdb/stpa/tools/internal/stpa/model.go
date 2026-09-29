// Package stpa loads the STPA records under otel-chdb/stpa/records, checks them, and renders
// the views (control-structure SVG, the PRD widget, Mermaid, Markdown tables) and the
// workbench-v0 export from them. Every function here is deterministic: the same records and
// views give byte-identical output.
package stpa

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Field types for the per-kind schema.
const (
	fText  = "text"  // a string
	fRef   = "ref"   // one record id
	fRefs  = "refs"  // a list of record ids
	fEnum  = "enum"  // a string from a fixed set
	fLabel = "label" // the human alias
)

type fieldSpec struct {
	typ      string
	required bool
	targets  []string // for ref/refs: allowed kinds
	values   []string // for enum
}

// Kinds is the normalized record schema: every fact has one field in one kind. Relations are
// stored on one side only (the dependent), and nothing derivable is stored: a UCA has no
// controller (it is its action's `from`), a component has no controller/process role (it is
// a controller iff it issues an action), a scenario has no hazards (they are its findings'),
// a constraint has no enforcement status (it has mechanisms or it has none).
var Kinds = map[string]map[string]fieldSpec{
	"loss": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
	},
	"hazard": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"losses":  {typ: fRefs, targets: []string{"loss"}},
		"refines": {typ: fRefs, targets: []string{"hazard"}},
	},
	"constraint": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"hazards": {typ: fRefs, required: true, targets: []string{"hazard"}},
	},
	"mechanism": {
		"label": {typ: fLabel}, "title": {typ: fText, required: true},
		"implements": {typ: fRefs, required: true, targets: []string{"constraint", "requirement"}},
	},
	"component": {
		"label": {typ: fLabel}, "title": {typ: fText, required: true}, "description": {typ: fText},
		"component_type": {typ: fEnum, required: true, values: []string{"human", "software", "organization"}},
	},
	"action": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText},
		"from": {typ: fRef, required: true, targets: []string{"component"}},
		"to":   {typ: fRef, required: true, targets: []string{"component"}},
	},
	"feedback": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText},
		"from": {typ: fRef, required: true, targets: []string{"component"}},
		"to":   {typ: fRef, required: true, targets: []string{"component"}},
	},
	"uca": {
		"label":    {typ: fLabel, required: true},
		"action":   {typ: fRef, required: true, targets: []string{"action"}},
		"category": {typ: fEnum, required: true, values: []string{"not-provided", "provided", "too-early-too-late-wrong-order", "stopped-too-soon-applied-too-long"}},
		"context":  {typ: fText, required: true},
		"hazards":  {typ: fRefs, required: true, targets: []string{"hazard"}},
	},
	"scenario": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"findings":   {typ: fRefs, required: true, targets: []string{"uca"}},
		"resolution": {typ: fText},
	},
	"sec": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"unsafe":     {typ: fText, required: true},
		"findings":   {typ: fRefs, targets: []string{"uca"}},
		"hazards":    {typ: fRefs, required: true, targets: []string{"hazard"}},
		"mitigation": {typ: fText, required: true},
	},
	"teaming": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"text":        {typ: fText, required: true},
		"hazards":     {typ: fRefs, required: true, targets: []string{"hazard"}},
		"requirement": {typ: fText, required: true},
	},
	"requirement": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"priority": {typ: fEnum, required: true, values: []string{"P0", "P1"}},
		"from":     {typ: fRefs, required: true, targets: []string{"constraint", "scenario", "uca", "sec", "teaming", "hazard", "incident", "requirement"}},
	},
	"incident": {
		"label": {typ: fLabel, required: true}, "batch": {typ: fText, required: true},
		"title": {typ: fText, required: true}, "found_by": {typ: fText, required: true},
		"hazard": {typ: fText, required: true}, "controller": {typ: fText, required: true},
		"why": {typ: fText, required: true}, "fix": {typ: fText, required: true}, "lesson": {typ: fText, required: true},
	},
}

var commonFields = map[string]fieldSpec{
	"id":    {typ: fText, required: true},
	"state": {typ: fEnum, required: true, values: []string{"draft", "submitted", "accepted", "rejected", "superseded"}},
}

// Prose fields whose citation labels (H-2, UCA-4, CAST-50, R-S1 …) are links derived by
// scanning, instead of being stored twice (once in the prose and once as a ref list).
var mentionFields = map[string][]string{
	"incident":    {"hazard", "controller", "fix", "lesson", "why"},
	"requirement": {"title"},
	"scenario":    {"resolution"},
	"teaming":     {"requirement"},
	"sec":         {"mitigation"},
}

// CitationLabel is the shape of a label other documents cite; such labels are unique.
var CitationLabel = regexp.MustCompile(`^(?:(?:L|H|SC|UCA|LS|SEC|TM|R)-[A-Z]?\d+|CAST-\d+)$`)
var mentionRE = regexp.MustCompile(`\b(?:L|H|SC|UCA|LS|SEC|TM|R)-[A-Z]?\d+\b`)

var idRE = regexp.MustCompile(`^([a-z]+)-([0-9a-f]{6})$`)

// Record is one file: YAML front matter, then optional prose.
type Record struct {
	ID, Kind, Path string
	F              map[string]any
	Body           string
}

func (r *Record) S(f string) string {
	v, _ := r.F[f].(string)
	return v
}

func (r *Record) L(f string) []string {
	switch v := r.F[f].(type) {
	case []any:
		out := make([]string, 0, len(v))
		for _, x := range v {
			out = append(out, fmt.Sprint(x))
		}
		return out
	case string:
		return []string{v}
	}
	return nil
}

// Name is how a record is shown when referred to: its label if it has one, else its title.
func (r *Record) Name() string {
	if l := r.S("label"); l != "" {
		return l
	}
	return r.S("title")
}

// Project is the loaded record set plus its views.
type Project struct {
	Root     string
	Manifest map[string]any
	Records  map[string]*Record
	ByLabel  map[string]*Record
	Views    []*View
}

// Load reads project.yaml, records/*.md and views/*.yaml under root.
func Load(root string) (*Project, error) {
	p := &Project{Root: root, Records: map[string]*Record{}, ByLabel: map[string]*Record{}}
	mb, err := os.ReadFile(filepath.Join(root, "project.yaml"))
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(mb, &p.Manifest); err != nil {
		return nil, fmt.Errorf("project.yaml: %w", err)
	}
	files, _ := filepath.Glob(filepath.Join(root, "records", "*.md"))
	sort.Strings(files)
	for _, f := range files {
		r, err := parseRecord(f)
		if err != nil {
			return nil, err
		}
		r.Path, _ = filepath.Rel(root, f)
		if p.Records[r.ID] != nil {
			return nil, fmt.Errorf("%s: duplicate id %s", r.Path, r.ID)
		}
		p.Records[r.ID] = r
	}
	vfiles, _ := filepath.Glob(filepath.Join(root, "views", "*.yaml"))
	sort.Strings(vfiles)
	for _, f := range vfiles {
		v, err := loadView(f)
		if err != nil {
			return nil, err
		}
		p.Views = append(p.Views, v)
	}
	for _, r := range p.Records {
		if l := r.S("label"); CitationLabel.MatchString(l) {
			p.ByLabel[l] = r
		}
	}
	return p, nil
}

func parseRecord(path string) (*Record, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s := string(b)
	if !strings.HasPrefix(s, "---\n") {
		return nil, fmt.Errorf("%s: no front matter", path)
	}
	end := strings.Index(s[4:], "\n---\n")
	if end < 0 {
		return nil, fmt.Errorf("%s: unterminated front matter", path)
	}
	fm, body := s[4:4+end+1], s[4+end+5:]
	r := &Record{Body: body}
	dec := yaml.NewDecoder(bytes.NewReader([]byte(fm)))
	dec.KnownFields(true)
	if err := dec.Decode(&r.F); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	r.ID = r.S("id")
	m := idRE.FindStringSubmatch(r.ID)
	if m == nil {
		return nil, fmt.Errorf("%s: id %q is not <kind>-<6 hex>", path, r.ID)
	}
	r.Kind = m[1]
	return r, nil
}

// Finding is one problem Check reports.
type Finding struct {
	Path, Rule, Msg string
	Warn            bool
}

func (f Finding) String() string {
	lvl := "error"
	if f.Warn {
		lvl = "warning"
	}
	return fmt.Sprintf("%s: %s [%s] %s", f.Path, lvl, f.Rule, f.Msg)
}

// Check validates the record set: schema per kind, references, unique citation labels, file
// name = id, the structural rules, and label mentions in prose.
func (p *Project) Check() []Finding {
	var out []Finding
	add := func(r *Record, rule, format string, a ...any) {
		out = append(out, Finding{Path: r.Path, Rule: rule, Msg: fmt.Sprintf(format, a...)})
	}
	labels := map[string]string{}
	hexes := map[string]string{}
	for _, id := range p.SortedIDs() {
		r := p.Records[id]
		spec, ok := Kinds[r.Kind]
		if !ok {
			add(r, "kind", "unknown kind %q", r.Kind)
			continue
		}
		if h := r.ID[len(r.Kind)+1:]; hexes[h] != "" {
			add(r, "id", "hex part %s also used by %s: ids are unique without their kind prefix, so a kind change cannot collide", h, hexes[h])
		} else {
			hexes[h] = r.ID
		}
		if filepath.Base(r.Path) != r.ID+".md" {
			add(r, "file-name", "file name must be %s.md (the id)", r.ID)
		}
		for k, v := range r.F {
			fs, ok := spec[k]
			if !ok {
				fs, ok = commonFields[k]
			}
			if !ok {
				add(r, "schema", "unknown field %q for kind %s", k, r.Kind)
				continue
			}
			switch fs.typ {
			case fText, fLabel, fRef, fEnum:
				s, isStr := v.(string)
				if !isStr || s == "" {
					add(r, "schema", "%s must be a non-empty string", k)
				} else if fs.typ == fEnum && !contains(fs.values, s) {
					add(r, "schema", "%s: %q is not one of %v", k, s, fs.values)
				}
			case fRefs:
				if _, isList := v.([]any); !isList {
					add(r, "schema", "%s must be a list", k)
				}
			}
			if fs.typ == fRef || fs.typ == fRefs {
				for _, t := range r.L(k) {
					tr := p.Records[t]
					if tr == nil {
						add(r, "reference", "%s -> %s does not exist", k, t)
					} else if !contains(fs.targets, tr.Kind) {
						add(r, "reference", "%s -> %s is a %s, expected %v", k, t, tr.Kind, fs.targets)
					}
				}
			}
		}
		for k, fs := range spec {
			if fs.required && r.F[k] == nil {
				add(r, "schema", "missing required field %q", k)
			}
			if fs.required && fs.typ == fRefs && r.F[k] != nil && len(r.L(k)) == 0 {
				add(r, "schema", "%s must not be empty", k)
			}
		}
		for k := range commonFields {
			if r.F[k] == nil {
				add(r, "schema", "missing required field %q", k)
			}
		}
		if l := r.S("label"); CitationLabel.MatchString(l) {
			if prev, dup := labels[l]; dup {
				add(r, "label", "citation label %s also used by %s", l, prev)
			}
			labels[l] = r.ID
		}
		if r.Kind == "feedback" {
			if t := p.Records[r.S("to")]; t != nil && !p.IsController(t) {
				add(r, "feedback-to-controller", "feedback goes to %s, which issues no control action", t.S("title"))
			}
		}
		if (r.Kind == "action" || r.Kind == "feedback") && r.S("from") == r.S("to") {
			add(r, "self-edge", "%s from a component to itself", r.Kind)
		}
		for _, f := range mentionFields[r.Kind] {
			for _, m := range mentionRE.FindAllString(r.S(f), -1) {
				if p.ByLabel[m] == nil {
					out = append(out, Finding{Path: r.Path, Rule: "mention", Warn: true,
						Msg: fmt.Sprintf("%s mentions %s, which is not a record (yet)", f, m)})
				}
			}
		}
	}
	for _, v := range p.Views {
		for _, e := range v.check(p) {
			out = append(out, Finding{Path: v.Path, Rule: "view", Msg: e})
		}
	}
	return out
}

// SortedIDs returns every record id in a stable order: by kind, then natural label order,
// then id.
func (p *Project) SortedIDs() []string {
	ids := make([]string, 0, len(p.Records))
	for id := range p.Records {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := p.Records[ids[i]], p.Records[ids[j]]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if c := NaturalCompare(a.S("label"), b.S("label")); c != 0 {
			return c < 0
		}
		return a.ID < b.ID
	})
	return ids
}

// NaturalCompare orders "UCA-2" before "UCA-10".
func NaturalCompare(a, b string) int {
	for a != "" && b != "" {
		da, db := digitsPrefix(a), digitsPrefix(b)
		if da != "" && db != "" {
			na, _ := strconv.Atoi(da)
			nb, _ := strconv.Atoi(db)
			if na != nb {
				if na < nb {
					return -1
				}
				return 1
			}
			a, b = a[len(da):], b[len(db):]
			continue
		}
		if a[0] != b[0] {
			if a[0] < b[0] {
				return -1
			}
			return 1
		}
		a, b = a[1:], b[1:]
	}
	return len(a) - len(b)
}

func digitsPrefix(s string) string {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return s[:i]
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// Of returns the records of one kind in SortedIDs order.
func (p *Project) Of(kind string) []*Record {
	var out []*Record
	for _, id := range p.SortedIDs() {
		if r := p.Records[id]; r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// Names maps a ref list to display names, joined as in the tables.
func (p *Project) Names(ids []string) string {
	var out []string
	for _, id := range ids {
		if r := p.Records[id]; r != nil {
			out = append(out, r.Name())
		} else {
			out = append(out, id)
		}
	}
	return strings.Join(out, ", ")
}

// Derived relations -------------------------------------------------------------------------

// Controller of a UCA is the issuer of its action (never stored on the UCA).
func (p *Project) UCAController(u *Record) *Record {
	if a := p.Records[u.S("action")]; a != nil {
		return p.Records[a.S("from")]
	}
	return nil
}

// ScenarioHazards are the hazards of the scenario's findings.
func (p *Project) ScenarioHazards(s *Record) []string {
	seen := map[string]bool{}
	var out []string
	for _, f := range s.L("findings") {
		if u := p.Records[f]; u != nil {
			for _, h := range u.L("hazards") {
				if !seen[h] {
					seen[h] = true
					out = append(out, h)
				}
			}
		}
	}
	return out
}

// Mechanisms implementing a constraint or requirement (the inverse of `implements`).
func (p *Project) Mechanisms(c *Record) []*Record {
	var out []*Record
	for _, m := range p.Of("mechanism") {
		if contains(m.L("implements"), c.ID) {
			out = append(out, m)
		}
	}
	return out
}

// IsController: a component is a controller iff it issues some action.
func (p *Project) IsController(c *Record) bool {
	for _, a := range p.Of("action") {
		if a.S("from") == c.ID {
			return true
		}
	}
	return false
}
