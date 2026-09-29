// Package stpa loads the STPA records under otel-chdb/stpa/records and the control structure
// (otel-chdb/stpa/structure.yaml), checks them, and renders the views (control-structure SVG,
// the PRD widget, Mermaid, Markdown tables) and the workbench-v0 export from them. Every
// function here is deterministic: the same records, structure and views give byte-identical
// output.
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
	fText    = "text"    // a string
	fRef     = "ref"     // one record id
	fRefs    = "refs"    // a list of record ids
	fEnum    = "enum"    // a string from a fixed set
	fLabel   = "label"   // the human alias
	fPath    = "path"    // one control-structure entry, <upper>-><lower>/<control|feedback>/<key>
	fPaths   = "paths"   // a list of them
	fNames   = "names"   // a list of process-model variable names (bare: the controller is derived)
	fQNames  = "qnames"  // a list of <node>/<variable> (qualified: nothing to derive the controller from)
	fLabels  = "labels"  // a list of citation labels (aliases that must stay resolvable)
	fSources = "sources" // a list of record ids or labels defined in a hand-kept source (project.yaml)
	fPM      = "pm"      // a controller's process model: [{name, meaning, updated_by, source}]
	fAlgo    = "algo"    // a controller's control algorithm: [{when, uses, issues}]
)

type fieldSpec struct {
	typ      string
	required bool
	targets  []string // for ref/refs/sources: allowed kinds; for path/paths: control | feedback
	values   []string // for enum
}

// Kinds is the normalized record schema: every fact has one field in one kind. Relations are
// stored on one side only (the dependent), and nothing derivable is stored: a UCA has no
// controller (it is the upper end of its action's link), a scenario with findings has no
// hazards (they are its findings'), a constraint has no enforcement status (it has mechanisms
// or it has none), a loss scenario has no status (its mechanisms and the requirements derived
// from it say it), a teaming or STPA-Sec row answered by a requirement has no requirement text
// of its own.
//
// Links between controllers and controlled processes are not records: they exist only in the
// control structure (structure.yaml), which records cite by path.
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
		"implements": {typ: fRefs, required: true, targets: []string{"constraint", "requirement", "scenario"}},
		// How a mechanism settles a loss scenario it implements (shown as the scenario's status).
		"effect": {typ: fEnum, values: []string{"fixed", "guarded", "measured"}},
	},
	"controller": {
		"label": {typ: fLabel}, "title": {typ: fText, required: true}, "description": {typ: fText},
		"component_type":    {typ: fEnum, required: true, values: []string{"human", "software", "organization"}},
		"process_model":     {typ: fPM},
		"control_algorithm": {typ: fAlgo},
	},
	"controlled_process": {
		"label": {typ: fLabel}, "title": {typ: fText, required: true}, "description": {typ: fText},
		"owner": {typ: fText},
	},
	"uca": {
		"label":     {typ: fLabel, required: true},
		"action":    {typ: fPath, required: true, targets: []string{"control"}},
		"category":  {typ: fEnum, required: true, values: []string{"not-provided", "provided", "too-early-too-late-wrong-order", "stopped-too-soon-applied-too-long"}},
		"context":   {typ: fText, required: true},
		"hazards":   {typ: fRefs, required: true, targets: []string{"hazard"}},
		"variables": {typ: fNames},
	},
	"scenario": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"findings": {typ: fRefs, targets: []string{"uca"}},
		// A feedback flaw: the entries whose feedback is missing, wrong or late.
		"feedback": {typ: fPaths, targets: []string{"feedback"}},
		"factor": {typ: fEnum, required: true, values: []string{"process-model", "algorithm", "feedback-missing",
			"feedback-wrong", "feedback-late", "controller-failure", "control-path", "controlled-process"}},
		"hazards":   {typ: fRefs, targets: []string{"hazard"}},
		"variables": {typ: fQNames},
		"formerly":  {typ: fLabels},
	},
	"sec": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"unsafe":     {typ: fText, required: true},
		"findings":   {typ: fRefs, targets: []string{"uca"}},
		"hazards":    {typ: fRefs, required: true, targets: []string{"hazard"}},
		"mitigation": {typ: fText},
	},
	"teaming": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"text":        {typ: fText, required: true},
		"hazards":     {typ: fRefs, required: true, targets: []string{"hazard"}},
		"requirement": {typ: fText},
	},
	"requirement": {
		"label": {typ: fLabel, required: true}, "title": {typ: fText, required: true},
		"priority": {typ: fEnum, required: true, values: []string{"P0", "P1"}},
		"from":     {typ: fSources, required: true, targets: []string{"constraint", "scenario", "uca", "sec", "teaming", "hazard", "incident", "requirement"}},
	},
	"incident": {
		"label": {typ: fLabel, required: true}, "batch": {typ: fText, required: true},
		"title": {typ: fText, required: true}, "found_by": {typ: fText, required: true},
		"hazard": {typ: fText, required: true}, "controller": {typ: fText, required: true},
		"why": {typ: fText, required: true}, "fix": {typ: fText, required: true}, "lesson": {typ: fText, required: true},
		"variables": {typ: fQNames},
	},
}

var commonFields = map[string]fieldSpec{
	"id":    {typ: fText, required: true},
	"state": {typ: fEnum, required: true, values: []string{"draft", "submitted", "accepted", "rejected", "superseded"}},
}

// Prose fields whose citation labels (H-2, UCA-4, R-S1 …) are links derived by scanning,
// instead of being stored twice (once in the prose and once as a ref list). A label that
// resolves to nothing is an error.
var mentionFields = map[string][]string{
	"incident":    {"hazard", "controller", "fix", "lesson", "why"},
	"requirement": {"title"},
	"teaming":     {"requirement"},
	"sec":         {"mitigation"},
}

// CitationLabel is the shape of a label other documents cite; such labels are unique.
var CitationLabel = regexp.MustCompile(`^(?:(?:L|H|SC|UCA|LS|SEC|TM|R)-[A-Z]?\d+|CAST-\d+)$`)
var mentionRE = regexp.MustCompile(`\b(?:L|H|SC|UCA|LS|SEC|TM|R)-[A-Z]?\d+\b`)

var idRE = regexp.MustCompile(`^([a-z_]+)-([0-9a-f]{6})$`)

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

// Hex is the identity part of an id: unique across kinds, so a record keeps it when its kind
// changes (the pilot's `component-8020ed` is `controller-8020ed`).
func Hex(id string) string { return id[strings.LastIndex(id, "-")+1:] }

// Project is the loaded record set, the control structure and the views.
type Project struct {
	Root      string
	Manifest  map[string]any
	Records   map[string]*Record
	ByLabel   map[string]*Record // citation labels and their `formerly` aliases
	ByHex     map[string]*Record
	Structure *Structure
	HandKept  map[string]string // label -> document, for labels still defined in hand-kept tables
	Views     []*View
}

// Load reads project.yaml, records/*.md, structure.yaml and views/*.yaml under root. A
// structure the parser refuses (a duplicate key, a controlled process at an upper end, an
// unknown node) is a load error: those forms cannot be expressed at all.
func Load(root string) (*Project, error) {
	p := &Project{Root: root, Records: map[string]*Record{}, ByLabel: map[string]*Record{}, ByHex: map[string]*Record{},
		HandKept: map[string]string{}}
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
		if p.ByHex[Hex(r.ID)] == nil {
			p.ByHex[Hex(r.ID)] = r
		}
	}
	for _, r := range p.Records {
		if l := r.S("label"); CitationLabel.MatchString(l) {
			p.ByLabel[l] = r
		}
	}
	for _, r := range p.Records {
		for _, l := range r.L("formerly") {
			if p.ByLabel[l] == nil {
				p.ByLabel[l] = r
			}
		}
	}
	sp := filepath.Join(root, "structure.yaml")
	if _, err := os.Stat(sp); err == nil {
		s, err := LoadStructure(sp, p.Records)
		if err != nil {
			return nil, err
		}
		s.Path, _ = filepath.Rel(root, sp)
		p.Structure = s
	} else {
		p.Structure = &Structure{Nodes: map[string]*Node{}, Entries: map[string]*Entry{}, Was: map[string]*Entry{}}
	}
	if err := p.loadHandKept(); err != nil {
		return nil, err
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
	return p, nil
}

// DocRoot is where the documents that splice or define labels live: project.yaml `docs_root`,
// relative to the project; default its parent (otel-chdb/).
func (p *Project) DocRoot() string {
	if d, _ := p.Manifest["docs_root"].(string); d != "" {
		return filepath.Join(p.Root, d)
	}
	return filepath.Join(p.Root, "..")
}

// loadHandKept reads the labels still defined in hand-kept documents (project.yaml
// `hand_kept`): the extension analyses' UCA, LS, SEC and TM tables, which no record duplicates,
// and the decision headings. A record may cite them in `from` and in prose until they are
// records themselves.
func (p *Project) loadHandKept() error {
	hk, _ := p.Manifest["hand_kept"].([]any)
	docRoot := p.DocRoot()
	heading := regexp.MustCompile(`^#{2,4} ([A-Z][\w-]*\d)[.:]? `)
	for _, x := range hk {
		m, _ := x.(map[string]any)
		doc, _ := m["doc"].(string)
		pat, _ := m["labels"].(string)
		re, err := regexp.Compile(pat)
		if err != nil {
			return fmt.Errorf("project.yaml hand_kept %s: %w", doc, err)
		}
		b, err := os.ReadFile(filepath.Join(docRoot, doc))
		if err != nil {
			return fmt.Errorf("project.yaml hand_kept: %w", err)
		}
		for _, line := range strings.Split(string(b), "\n") {
			var cand string
			if strings.HasPrefix(line, "|") {
				c := splitRow(line)
				cand = strings.Trim(c[0], "*")
			} else if h := heading.FindStringSubmatch(line); h != nil {
				cand = h[1]
			}
			if cand != "" && re.MatchString(cand) && p.HandKept[cand] == "" {
				p.HandKept[cand] = doc
			}
		}
	}
	return nil
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

// Resolves reports whether a citation label names something: a record, a former label of one,
// or a label a hand-kept document still defines.
func (p *Project) Resolves(label string) bool {
	return p.ByLabel[label] != nil || p.HandKept[label] != ""
}

// Check validates the record set: schema per kind, references, unique citation labels, file
// name = id, label mentions in prose, the control structure and the controllers' internals.
func (p *Project) Check() []Finding {
	var out []Finding
	add := func(r *Record, rule, format string, a ...any) {
		out = append(out, Finding{Path: r.Path, Rule: rule, Msg: fmt.Sprintf(format, a...)})
	}
	warn := func(r *Record, rule, format string, a ...any) {
		out = append(out, Finding{Path: r.Path, Rule: rule, Msg: fmt.Sprintf(format, a...), Warn: true})
	}
	labels := map[string]string{}
	hexes := map[string]string{}
	for _, e := range p.Structure.List {
		if e.Was != "" {
			hexes[Hex(e.Was)] = e.Was + " (alias of " + e.Path() + ")"
		}
	}
	for _, id := range p.SortedIDs() {
		r := p.Records[id]
		spec, ok := Kinds[r.Kind]
		if !ok {
			add(r, "kind", "unknown kind %q", r.Kind)
			continue
		}
		if h := Hex(r.ID); hexes[h] != "" {
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
			case fText, fLabel, fRef, fEnum, fPath:
				s, isStr := v.(string)
				if !isStr || s == "" {
					add(r, "schema", "%s must be a non-empty string", k)
				} else if fs.typ == fEnum && !contains(fs.values, s) {
					add(r, "schema", "%s: %q is not one of %v", k, s, fs.values)
				}
			case fRefs, fPaths, fNames, fQNames, fLabels, fSources:
				if _, isList := v.([]any); !isList {
					add(r, "schema", "%s must be a list", k)
				}
			case fPM, fAlgo:
				for _, e := range p.checkInternals(r, k, v) {
					add(r, "controller-internals", "%s", e)
				}
			}
			switch fs.typ {
			case fRef, fRefs:
				for _, t := range r.L(k) {
					tr := p.Records[t]
					if tr == nil {
						add(r, "reference", "%s -> %s does not exist", k, t)
					} else if !contains(fs.targets, tr.Kind) {
						add(r, "reference", "%s -> %s is a %s, expected %v", k, t, tr.Kind, fs.targets)
					}
				}
			case fSources:
				for _, t := range r.L(k) {
					if tr := p.Records[t]; tr != nil {
						if !contains(fs.targets, tr.Kind) {
							add(r, "reference", "%s -> %s is a %s, expected %v", k, t, tr.Kind, fs.targets)
						}
					} else if p.ByLabel[t] != nil {
						add(r, "reference", "%s cites %s by label; it is a record (%s): cite it by id", k, t, p.ByLabel[t].ID)
					} else if p.HandKept[t] == "" {
						add(r, "reference", "%s -> %s is neither a record id nor a label defined in a hand-kept document", k, t)
					}
				}
			case fPath, fPaths:
				for _, t := range r.L(k) {
					e := p.Structure.Entries[t]
					if e == nil {
						add(r, "reference", "%s -> %s is not an entry of the control structure", k, t)
					} else if !contains(fs.targets, e.Kind) {
						add(r, "reference", "%s -> %s is %s, expected %v", k, t, e.Kind, fs.targets)
					}
				}
			case fLabels:
				for _, t := range r.L(k) {
					if !CitationLabel.MatchString(t) {
						add(r, "schema", "%s: %q is not a citation label", k, t)
					}
				}
			case fQNames:
				for _, t := range r.L(k) {
					if _, err := p.QVar(t); err != nil {
						add(r, "reference", "%s: %v", k, err)
					}
				}
			}
		}
		for k, fs := range spec {
			if fs.required && r.F[k] == nil {
				add(r, "schema", "missing required field %q", k)
			}
			if fs.required && (fs.typ == fRefs || fs.typ == fSources) && r.F[k] != nil && len(r.L(k)) == 0 {
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
		for _, l := range r.L("formerly") {
			if prev, dup := labels[l]; dup {
				add(r, "label", "former label %s also used by %s", l, prev)
			}
			labels[l] = r.ID
		}
		for _, f := range mentionFields[r.Kind] {
			for _, m := range mentionRE.FindAllString(r.S(f), -1) {
				if !p.Resolves(m) {
					add(r, "mention", "%s mentions %s, which is neither a record nor defined in a hand-kept document", f, m)
				}
			}
		}
		p.checkKindRules(r, add, warn)
	}
	out = append(out, p.checkStructure()...)
	for _, v := range p.Views {
		for _, e := range v.check(p) {
			out = append(out, Finding{Path: v.Path, Rule: "view", Msg: e})
		}
	}
	return out
}

// checkKindRules are the rules that span fields: where one of two fields must hold a fact
// and the other must then be empty, so the fact has one home.
func (p *Project) checkKindRules(r *Record, add, warn func(*Record, string, string, ...any)) {
	switch r.Kind {
	case "uca":
		c := p.UCAController(r)
		if len(r.L("variables")) == 0 {
			warn(r, "process-model", "names no process-model variable of its controller (the context says what the controller believed; which variable?)")
		}
		if c != nil {
			for _, v := range r.L("variables") {
				if p.PMVar(c, v) == nil {
					add(r, "reference", "variables: %s is not a process-model variable of %s", v, c.S("title"))
				}
			}
		}
	case "scenario":
		hasF, hasFb := len(r.L("findings")) > 0, len(r.L("feedback")) > 0
		if !hasF && !hasFb {
			add(r, "scenario", "a loss scenario explains a UCA (findings) or a feedback flaw (feedback), or both")
		}
		if hasF && r.F["hazards"] != nil {
			add(r, "scenario", "hazards are derived from the findings; store them only on a scenario without findings")
		}
		if !hasF && len(r.L("hazards")) == 0 {
			add(r, "scenario", "a scenario without findings must name its hazards")
		}
		fbFactor := strings.HasPrefix(r.S("factor"), "feedback-")
		if fbFactor && !hasFb {
			add(r, "scenario", "factor %s needs the feedback entries it concerns", r.S("factor"))
		}
		if hasFb && !fbFactor {
			add(r, "scenario", "feedback entries are cited only by a feedback-* factor")
		}
		if (r.S("factor") == "process-model" || fbFactor) && len(r.L("variables")) == 0 {
			warn(r, "process-model", "a %s scenario names no process-model variable it concerns", r.S("factor"))
		}
	case "incident":
		if len(r.L("variables")) == 0 {
			warn(r, "process-model", "the flawed process model names no variable of a controller in the structure")
		}
	case "teaming", "sec":
		field := map[string]string{"teaming": "requirement", "sec": "mitigation"}[r.Kind]
		derived := p.RequirementsFrom(r)
		if len(derived) > 0 && r.S(field) != "" {
			add(r, "one-wording", "%s is answered by %s; its own %s text would be a second wording of that obligation", r.Name(), p.Names(ids(derived)), field)
		}
		if len(derived) == 0 && r.S(field) == "" {
			add(r, "one-wording", "no requirement derives from %s, so it needs its own %s text", r.Name(), field)
		}
	case "mechanism":
		for _, t := range r.L("implements") {
			if tr := p.Records[t]; tr != nil && tr.Kind == "scenario" && r.S("effect") == "" {
				add(r, "schema", "a mechanism implementing a loss scenario says its effect (fixed, guarded, measured)")
			}
		}
	}
}

func ids(rs []*Record) []string {
	var out []string
	for _, r := range rs {
		out = append(out, r.ID)
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

// Names maps a ref list (record ids, or hand-kept labels) to display names, joined as in the
// tables.
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

// UCAController is the controller record at the upper end of the UCA's action (never stored
// on the UCA).
func (p *Project) UCAController(u *Record) *Record {
	if e := p.Structure.Entries[u.S("action")]; e != nil {
		return e.Link.Upper.Rec
	}
	return nil
}

// ScenarioHazards are the hazards of the scenario's findings, or its own when it has none.
func (p *Project) ScenarioHazards(s *Record) []string {
	if len(s.L("findings")) == 0 {
		return s.L("hazards")
	}
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

// Mechanisms implementing a constraint, requirement or scenario (the inverse of `implements`).
func (p *Project) Mechanisms(c *Record) []*Record {
	var out []*Record
	for _, m := range p.Of("mechanism") {
		if contains(m.L("implements"), c.ID) {
			out = append(out, m)
		}
	}
	return out
}

// RequirementsFrom are the requirements whose `from` cites the record (the inverse link).
func (p *Project) RequirementsFrom(r *Record) []*Record {
	var out []*Record
	for _, q := range p.Of("requirement") {
		if contains(q.L("from"), r.ID) {
			out = append(out, q)
		}
	}
	return out
}

// IsController: a record is a controller iff it is of kind controller (the structure checks
// that every controller is the upper end of some link).
func (p *Project) IsController(c *Record) bool { return c != nil && c.Kind == "controller" }
