package stpa

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The control structure (structure.yaml) is the only place links exist. Its shape makes the
// redundant forms impossible to write:
//
//	nodes:                              # a local name per record: typed by the id's kind
//	  consumer: controller-8020ed
//	  clickhouse: controlled_process-ed00c7
//	links:                              # one entry per ordered pair, "<upper> -> <lower>"
//	  consumer -> clickhouse:
//	    control:  {insert: insert, repair: repair}    # runs down the arrow
//	    feedback: {counts: counts}                     # runs back up
//
// The parser refuses (these cannot be expressed): a duplicate key anywhere (so a pair, an
// action on a pair, a feedback on a pair or a node name is defined once), a pair key not
// spelled "<name> -> <name>", a name that is not a node, a controlled process at the upper end
// (a node's kind is its record id's kind), a link with no entries, and unknown fields. Check
// reports what can be written but is wrong: a node naming no record, two nodes for one record,
// a pair keyed in both orders, a self link, a cycle of control, a controller that controls
// nothing, a record in no node, and the controllers' internals (every feedback entry updates a
// process-model variable of its upper controller; every control entry is issued by one of its
// rules; a controller cites only links it is the upper end of).
type Structure struct {
	Path      string
	Nodes     map[string]*Node
	NodeOrder []*Node
	Links     []*Link
	Entries   map[string]*Entry // by path
	List      []*Entry          // in file order
	Was       map[string]*Entry // the pilot's action and feedback record ids, as aliases
}

// Node is one named reference to a controller or controlled_process record.
type Node struct {
	Name, ID   string
	Rec        *Record
	Controller bool // the id's kind is controller
	Line       int
}

// Link is every control action and feedback between one upper and one lower node.
type Link struct {
	Upper, Lower      *Node
	Control, Feedback []*Entry
	Line              int
}

// Entry is one control action (Kind control, from Upper to Lower) or one feedback (Kind
// feedback, from Lower to Upper). Label is the drawn text; Title the full name tables show.
type Entry struct {
	Kind, Key, Label, Title, Was string
	Link                         *Link
	Line                         int
}

// Path is the entry's stable reference: <upper>-><lower>/<control|feedback>/<key>.
func (e *Entry) Path() string {
	return e.Link.Upper.Name + "->" + e.Link.Lower.Name + "/" + e.Kind + "/" + e.Key
}

// Name is the full name if there is one, else the drawn label.
func (e *Entry) Name() string {
	if e.Title != "" {
		return e.Title
	}
	return e.Label
}

// From and To are the entry's ends in the direction it flows.
func (e *Entry) From() *Node {
	if e.Kind == "control" {
		return e.Link.Upper
	}
	return e.Link.Lower
}

func (e *Entry) To() *Node {
	if e.Kind == "control" {
		return e.Link.Lower
	}
	return e.Link.Upper
}

var (
	nameRE    = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	pairRE    = regexp.MustCompile(`^([a-z][a-z0-9_]*) -> ([a-z][a-z0-9_]*)$`)
	entryKey  = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	pathRE    = regexp.MustCompile(`^([a-z][a-z0-9_]*)->([a-z][a-z0-9_]*)/(control|feedback)/([a-z][a-z0-9_-]*)$`)
	pmNameRE  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	qualVarRE = regexp.MustCompile(`^([a-z][a-z0-9_]*)/([a-z][a-z0-9_]*)$`)
)

type parseErr struct {
	file string
	line int
	msg  string
}

func (e parseErr) Error() string { return fmt.Sprintf("%s:%d: %s", e.file, e.line, e.msg) }

// mapping returns a mapping node's pairs, refusing a duplicate key: in this document every
// mapping key is an identity, so a duplicate is a second definition of one thing.
func mapping(file string, n *yaml.Node, what string) ([][2]*yaml.Node, error) {
	if n.Kind != yaml.MappingNode {
		return nil, parseErr{file, n.Line, what + " must be a mapping"}
	}
	seen := map[string]int{}
	var out [][2]*yaml.Node
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Kind != yaml.ScalarNode {
			return nil, parseErr{file, k.Line, what + ": keys must be plain strings"}
		}
		if first, dup := seen[k.Value]; dup {
			return nil, parseErr{file, k.Line, fmt.Sprintf("%s: duplicate key %q (first at line %d): each is defined once", what, k.Value, first)}
		}
		seen[k.Value] = k.Line
		out = append(out, [2]*yaml.Node{k, v})
	}
	return out, nil
}

func scalar(file string, n *yaml.Node, what string) (string, error) {
	if n.Kind != yaml.ScalarNode || n.Value == "" {
		return "", parseErr{file, n.Line, what + " must be a non-empty string"}
	}
	return n.Value, nil
}

// LoadStructure parses structure.yaml. Records are used only to attach each node's record;
// a node whose record is missing is a check finding, not a parse error.
func LoadStructure(path string, records map[string]*Record) (*Structure, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseStructure(path, b, records)
}

// ParseStructure is LoadStructure on bytes.
func ParseStructure(file string, b []byte, records map[string]*Record) (*Structure, error) {
	s := &Structure{Nodes: map[string]*Node{}, Entries: map[string]*Entry{}, Was: map[string]*Entry{}}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	if len(doc.Content) == 0 {
		return nil, parseErr{file, 1, "empty document"}
	}
	top, err := mapping(file, doc.Content[0], "the document")
	if err != nil {
		return nil, err
	}
	var linksNode *yaml.Node
	for _, kv := range top {
		switch kv[0].Value {
		case "nodes":
			nodes, err := mapping(file, kv[1], "nodes")
			if err != nil {
				return nil, err
			}
			for _, nv := range nodes {
				name := nv[0].Value
				if !nameRE.MatchString(name) {
					return nil, parseErr{file, nv[0].Line, fmt.Sprintf("node name %q: lower case, digits and _", name)}
				}
				id, err := scalar(file, nv[1], "node "+name)
				if err != nil {
					return nil, err
				}
				m := idRE.FindStringSubmatch(id)
				if m == nil || (m[1] != "controller" && m[1] != "controlled_process") {
					return nil, parseErr{file, nv[1].Line, fmt.Sprintf("node %s: %q must be the id of a controller or controlled_process record", name, id)}
				}
				n := &Node{Name: name, ID: id, Rec: records[id], Controller: m[1] == "controller", Line: nv[0].Line}
				s.Nodes[name] = n
				s.NodeOrder = append(s.NodeOrder, n)
			}
		case "links":
			linksNode = kv[1]
		default:
			return nil, parseErr{file, kv[0].Line, fmt.Sprintf("unknown top-level key %q (nodes, links)", kv[0].Value)}
		}
	}
	if linksNode == nil {
		return s, nil
	}
	links, err := mapping(file, linksNode, "links")
	if err != nil {
		return nil, err
	}
	for _, lv := range links {
		m := pairRE.FindStringSubmatch(lv[0].Value)
		if m == nil {
			return nil, parseErr{file, lv[0].Line, fmt.Sprintf("link key %q must be spelled \"<upper> -> <lower>\" (one space each side)", lv[0].Value)}
		}
		up, lo := s.Nodes[m[1]], s.Nodes[m[2]]
		if up == nil || lo == nil {
			missing := m[1]
			if up != nil {
				missing = m[2]
			}
			return nil, parseErr{file, lv[0].Line, fmt.Sprintf("link %s: %q is not a node", lv[0].Value, missing)}
		}
		if !up.Controller {
			return nil, parseErr{file, lv[0].Line, fmt.Sprintf("link %s: the upper end %s is a controlled process (%s); only a controller issues control actions", lv[0].Value, up.Name, up.ID)}
		}
		l := &Link{Upper: up, Lower: lo, Line: lv[0].Line}
		body, err := mapping(file, lv[1], "link "+lv[0].Value)
		if err != nil {
			return nil, err
		}
		for _, kv := range body {
			kind := kv[0].Value
			if kind != "control" && kind != "feedback" {
				return nil, parseErr{file, kv[0].Line, fmt.Sprintf("link %s: unknown key %q (control, feedback)", lv[0].Value, kind)}
			}
			ents, err := mapping(file, kv[1], "link "+lv[0].Value+" "+kind)
			if err != nil {
				return nil, err
			}
			for _, ev := range ents {
				e := &Entry{Kind: kind, Key: ev[0].Value, Link: l, Line: ev[0].Line}
				if !entryKey.MatchString(e.Key) {
					return nil, parseErr{file, ev[0].Line, fmt.Sprintf("entry key %q: lower case, digits, _ and -", e.Key)}
				}
				switch ev[1].Kind {
				case yaml.ScalarNode:
					if e.Label, err = scalar(file, ev[1], "entry "+e.Key); err != nil {
						return nil, err
					}
				case yaml.MappingNode:
					fields, err := mapping(file, ev[1], "entry "+e.Key)
					if err != nil {
						return nil, err
					}
					for _, f := range fields {
						v, err := scalar(file, f[1], "entry "+e.Key+" "+f[0].Value)
						if err != nil {
							return nil, err
						}
						switch f[0].Value {
						case "label":
							e.Label = v
						case "title":
							e.Title = v
						case "was":
							e.Was = v
						default:
							return nil, parseErr{file, f[0].Line, fmt.Sprintf("entry %s: unknown key %q (label, title, was)", e.Key, f[0].Value)}
						}
					}
					if e.Label == "" {
						return nil, parseErr{file, ev[1].Line, "entry " + e.Key + ": label is required"}
					}
				default:
					return nil, parseErr{file, ev[1].Line, "entry " + e.Key + ": a label, or {label, title, was}"}
				}
				if kind == "control" {
					l.Control = append(l.Control, e)
				} else {
					l.Feedback = append(l.Feedback, e)
				}
				s.Entries[e.Path()] = e
				s.List = append(s.List, e)
				if e.Was != "" {
					s.Was[e.Was] = e
				}
			}
		}
		if len(l.Control)+len(l.Feedback) == 0 {
			return nil, parseErr{file, lv[0].Line, "link " + lv[0].Value + " has no control and no feedback entry"}
		}
		s.Links = append(s.Links, l)
	}
	return s, nil
}

// checkStructure reports what the parser cannot refuse (see Structure).
func (p *Project) checkStructure() []Finding {
	s := p.Structure
	var out []Finding
	add := func(line int, rule, format string, a ...any) {
		out = append(out, Finding{Path: fmt.Sprintf("%s:%d", s.Path, line), Rule: rule, Msg: fmt.Sprintf(format, a...)})
	}
	byRec := map[string]*Node{}
	for _, n := range s.NodeOrder {
		if n.Rec == nil {
			add(n.Line, "structure", "node %s: %s is not a record", n.Name, n.ID)
		}
		if prev := byRec[n.ID]; prev != nil {
			add(n.Line, "structure", "nodes %s and %s name the same record %s", prev.Name, n.Name, n.ID)
		}
		byRec[n.ID] = n
	}
	for _, kind := range []string{"controller", "controlled_process"} {
		for _, r := range p.Of(kind) {
			if byRec[r.ID] == nil {
				out = append(out, Finding{Path: r.Path, Rule: "structure", Msg: "is no node of the control structure"})
			}
		}
	}
	pairs := map[[2]string]*Link{}
	upper := map[string]bool{}
	for _, l := range s.Links {
		if l.Upper == l.Lower {
			add(l.Line, "structure", "link %s -> %s: a controller does not control itself", l.Upper.Name, l.Lower.Name)
		}
		if rev := pairs[[2]string{l.Lower.Name, l.Upper.Name}]; rev != nil {
			add(l.Line, "structure", "link %s -> %s: the pair is also keyed the other way round (line %d); control runs one way, feedback comes back on the same link",
				l.Upper.Name, l.Lower.Name, rev.Line)
		}
		pairs[[2]string{l.Upper.Name, l.Lower.Name}] = l
		if len(l.Control) > 0 {
			upper[l.Upper.Name] = true
		}
	}
	for _, n := range s.NodeOrder {
		if n.Controller && !upper[n.Name] {
			add(n.Line, "structure", "node %s is a controller that issues no control action: make it a controlled_process record", n.Name)
		}
	}
	if cyc := s.controlCycle(); cyc != nil {
		add(1, "structure", "control runs in a cycle: %s", strings.Join(cyc, " -> "))
	}
	// Controllers' internals: every feedback entry updates a process-model variable of its
	// upper controller, and every control entry is issued by one of its rules.
	for _, e := range s.List {
		c := e.Link.Upper.Rec
		if c == nil {
			continue
		}
		cited := false
		if e.Kind == "feedback" {
			for _, v := range pmOf(c) {
				cited = cited || contains(strList(v["updated_by"]), e.Path())
			}
			if !cited {
				add(e.Line, "controller-internals", "feedback %s updates no process-model variable of %s (%s)", e.Path(), c.S("title"), c.Path)
			}
		} else {
			for _, r := range algoOf(c) {
				cited = cited || contains(strList(r["issues"]), e.Path())
			}
			if !cited {
				add(e.Line, "controller-internals", "control action %s is issued by no rule of %s's control algorithm (%s)", e.Path(), c.S("title"), c.Path)
			}
		}
	}
	return out
}

// controlCycle returns a cycle of control links, if there is one.
func (s *Structure) controlCycle() []string {
	next := map[string][]string{}
	for _, l := range s.Links {
		if len(l.Control) > 0 {
			next[l.Upper.Name] = append(next[l.Upper.Name], l.Lower.Name)
		}
	}
	state := map[string]int{}
	var stack []string
	var found []string
	var visit func(n string) bool
	visit = func(n string) bool {
		state[n] = 1
		stack = append(stack, n)
		for _, m := range next[n] {
			if state[m] == 1 {
				for i, x := range stack {
					if x == m {
						found = append(append([]string{}, stack[i:]...), m)
					}
				}
				return true
			}
			if state[m] == 0 && visit(m) {
				return true
			}
		}
		state[n] = 2
		stack = stack[:len(stack)-1]
		return false
	}
	var names []string
	for n := range next {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if state[n] == 0 && visit(n) {
			return found
		}
	}
	return nil
}

// Controllers' internals --------------------------------------------------------------------

func strList(v any) []string {
	switch x := v.(type) {
	case []any:
		var out []string
		for _, e := range x {
			out = append(out, fmt.Sprint(e))
		}
		return out
	case string:
		return []string{x}
	}
	return nil
}

func listOfMaps(v any) []map[string]any {
	xs, _ := v.([]any)
	var out []map[string]any
	for _, x := range xs {
		if m, ok := x.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// ruleSlug is a rule's name as the stable part of its text ids in the diagrams: lower case,
// runs of anything but letters and digits as one hyphen. Rule names are unique within a
// controller by this key (checkInternals), so the ids are too, and they survive a reordering
// of the rules.
func ruleSlug(name string) string {
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			if dash && b.Len() > 0 {
				b.WriteByte('-')
			}
			b.WriteRune(r)
			dash = false
		} else {
			dash = true
		}
	}
	return b.String()
}

// ruleSlugMax keeps "<controller id>-rule-<slug>-l9" within the PRD's 64-character text ids.
const ruleSlugMax = 36

func pmOf(c *Record) []map[string]any   { return listOfMaps(c.F["process_model"]) }
func algoOf(c *Record) []map[string]any { return listOfMaps(c.F["control_algorithm"]) }

// PMVar returns the named process-model variable of a controller record.
func (p *Project) PMVar(c *Record, name string) map[string]any {
	for _, v := range pmOf(c) {
		if v["name"] == name {
			return v
		}
	}
	return nil
}

// QVar resolves a qualified variable, <node>/<variable>.
func (p *Project) QVar(q string) (map[string]any, error) {
	m := qualVarRE.FindStringSubmatch(q)
	if m == nil {
		return nil, fmt.Errorf("%q is not <node>/<variable>", q)
	}
	n := p.Structure.Nodes[m[1]]
	if n == nil || n.Rec == nil || !n.Controller {
		return nil, fmt.Errorf("%q: %s is not a controller node of the structure", q, m[1])
	}
	v := p.PMVar(n.Rec, m[2])
	if v == nil {
		return nil, fmt.Errorf("%q: %s has no process-model variable %s", q, n.Rec.S("title"), m[2])
	}
	return v, nil
}

// NodeOf returns the structure node of a record, if any.
func (p *Project) NodeOf(r *Record) *Node {
	for _, n := range p.Structure.NodeOrder {
		if n.ID == r.ID {
			return n
		}
	}
	return nil
}

// checkInternals validates a controller's process_model or control_algorithm field.
func (p *Project) checkInternals(r *Record, field string, v any) []string {
	var errs []string
	xs, ok := v.([]any)
	if !ok {
		return []string{field + " must be a list"}
	}
	own := func(path, kind string) string {
		e := p.Structure.Entries[path]
		switch {
		case e == nil:
			return fmt.Sprintf("%s is not an entry of the control structure", path)
		case e.Kind != kind:
			return fmt.Sprintf("%s is a %s entry, expected %s", path, e.Kind, kind)
		case e.Link.Upper.ID != r.ID:
			return fmt.Sprintf("%s is a link whose upper end is %s, not this controller: a controller cites only its own links", path, e.Link.Upper.Name)
		}
		return ""
	}
	names := map[string]bool{}
	for _, m := range pmOf(r) {
		if n, _ := m["name"].(string); n != "" {
			names[n] = true
		}
	}
	allowed := map[string][]string{"process_model": {"name", "meaning", "updated_by", "source"}, "control_algorithm": {"name", "when", "uses", "issues"}}[field]
	seen := map[string]bool{}
	ruleNames := map[string]int{}
	for i, x := range xs {
		m, ok := x.(map[string]any)
		if !ok {
			errs = append(errs, fmt.Sprintf("%s[%d] must be a mapping", field, i))
			continue
		}
		for k := range m {
			if !contains(allowed, k) {
				errs = append(errs, fmt.Sprintf("%s[%d]: unknown key %q (%s)", field, i, k, strings.Join(allowed, ", ")))
			}
		}
		if field == "process_model" {
			name, _ := m["name"].(string)
			if !pmNameRE.MatchString(name) {
				errs = append(errs, fmt.Sprintf("process_model[%d]: name %q: lower case, digits and _", i, name))
			}
			if seen[name] {
				errs = append(errs, fmt.Sprintf("process_model: variable %s defined twice", name))
			}
			seen[name] = true
			if s, _ := m["meaning"].(string); s == "" {
				errs = append(errs, fmt.Sprintf("process_model %s: meaning is required", name))
			}
			ub := strList(m["updated_by"])
			src, _ := m["source"].(string)
			if len(ub) == 0 && src == "" {
				errs = append(errs, fmt.Sprintf("process_model %s: no source: name the feedback that updates it (updated_by) or, if none does, say where it comes from (source)", name))
			}
			for _, pth := range ub {
				if e := own(pth, "feedback"); e != "" {
					errs = append(errs, fmt.Sprintf("process_model %s: updated_by %s", name, e))
				}
			}
		} else {
			// A rule is named for what it decides; the name heads it in the detail diagram, so
			// two rules of one controller may not share one (the actions they issue may).
			name, _ := m["name"].(string)
			if strings.TrimSpace(name) == "" {
				errs = append(errs, fmt.Sprintf("control_algorithm[%d]: name is required (what the rule decides, e.g. \"advance the checkpoint\")", i))
			} else if key := ruleSlug(name); key == "" {
				errs = append(errs, fmt.Sprintf("control_algorithm[%d]: rule name %q has no letters or digits", i, name))
			} else if len(key) > ruleSlugMax {
				errs = append(errs, fmt.Sprintf("control_algorithm[%d]: rule name %q is longer than %d characters as a text id (%s)", i, name, ruleSlugMax, key))
			} else if ruleNames[key] > 0 {
				errs = append(errs, fmt.Sprintf("control_algorithm[%d]: rule name %q is already the name of rule %d: rule names are unique within a controller", i, name, ruleNames[key]-1))
			} else {
				ruleNames[key] = i + 1
			}
			if s, _ := m["when"].(string); s == "" {
				errs = append(errs, fmt.Sprintf("control_algorithm[%d]: when is required", i))
			}
			uses := strList(m["uses"])
			if len(uses) == 0 {
				errs = append(errs, fmt.Sprintf("control_algorithm[%d]: uses names no process-model variable (a rule is a condition over them)", i))
			}
			for _, u := range uses {
				if !names[u] {
					errs = append(errs, fmt.Sprintf("control_algorithm[%d]: uses %s, which is no process-model variable of this controller", i, u))
				}
			}
			iss := strList(m["issues"])
			if len(iss) == 0 {
				errs = append(errs, fmt.Sprintf("control_algorithm[%d]: issues no control action", i))
			}
			for _, pth := range iss {
				if e := own(pth, "control"); e != "" {
					errs = append(errs, fmt.Sprintf("control_algorithm[%d]: issues %s", i, e))
				}
			}
		}
	}
	return errs
}
