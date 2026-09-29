package stpa

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var ucaCode = map[string]string{
	"not-provided": "NP", "provided": "P", "too-early-too-late-wrong-order": "T", "stopped-too-soon-applied-too-long": "D",
}

// columnValue resolves one table cell. Plain field names show the field (ref lists as their
// labels); the names below are derived values, computed from other records instead of stored.
func (p *Project) columnValue(r *Record, value string) (string, bool) {
	if _, isField := Kinds[r.Kind][value]; isField {
		value = "field:" + value
	}
	switch value {
	case "controller": // UCA: the upper end of its action's link, by its short name
		if c := p.UCAController(r); c != nil {
			return c.Name(), true
		}
		return "", true
	case "action-name": // UCA: the action's full name
		if e := p.Structure.Entries[r.S("action")]; e != nil {
			return e.Name(), true
		}
		return "", true
	case "type": // UCA category as its one-letter code
		return ucaCode[r.S("category")], true
	case "mechanisms": // constraint, requirement: the mechanisms implementing it (the inverse of implements)
		var names []string
		for _, m := range p.Mechanisms(r) {
			names = append(names, m.S("title"))
		}
		return strings.Join(names, "; "), true
	case "unsafe-with-findings": // SEC: the unsafe action, then the UCAs it is, in parentheses
		s := r.S("unsafe")
		if f := r.L("findings"); len(f) > 0 {
			s += " (" + p.Names(f) + ")"
		}
		return s, true
	case "title-with-priority": // requirement: P0 is the default and unmarked
		s := r.S("title")
		if r.S("priority") != "P0" {
			s += " (" + r.S("priority") + ")"
		}
		return s, true
	case "number": // incident: its CAST row number
		return strings.TrimPrefix(r.S("label"), "CAST-"), true
	case "scenario-hazards":
		return p.Names(p.ScenarioHazards(r)), true
	case "findings-or-feedback": // scenario: the UCAs it explains, then the feedback it finds flawed
		var parts []string
		if f := r.L("findings"); len(f) > 0 {
			parts = append(parts, p.Names(f))
		}
		for _, path := range r.L("feedback") {
			if e := p.Structure.Entries[path]; e != nil {
				parts = append(parts, fmt.Sprintf("feedback %q", e.Label))
			}
		}
		s := strings.Join(parts, "; ")
		if f := r.L("formerly"); len(f) > 0 {
			s += " (was " + strings.Join(f, ", ") + ")"
		}
		return s, true
	case "status": // scenario: its mechanisms by effect, then the requirements derived from it
		return p.ScenarioStatus(r), true
	case "pm": // controller: its process-model variables, grouped by the feedback that updates them
		return p.pmCell(r), true
	case "algorithm": // controller: each action with the rule that issues it
		return p.algoCell(r), true
	case "answered-by": // teaming, sec: the requirements derived from it, else its own text
		if rs := p.RequirementsFrom(r); len(rs) > 0 {
			return p.Names(ids(rs)), true
		}
		if r.Kind == "teaming" {
			return r.S("requirement"), true
		}
		return r.S("mitigation"), true
	}
	value = strings.TrimPrefix(value, "field:")
	spec, ok := Kinds[r.Kind][value]
	if !ok {
		if _, common := commonFields[value]; !common {
			return "", false
		}
	}
	if spec.typ == fRefs || spec.typ == fRef || spec.typ == fSources {
		return p.Names(r.L(value)), true
	}
	return r.S(value), true
}

// ScenarioStatus is a loss scenario's status, derived: "Fixed: …", "Guarded: …" or "Measured:
// …" from the mechanisms implementing it, then "needs R-…" for the requirements derived from
// it that no mechanism implements yet, or "met by R-…" for those one does.
func (p *Project) ScenarioStatus(r *Record) string {
	var parts []string
	for _, eff := range []string{"fixed", "guarded", "measured"} {
		var ts []string
		for _, m := range p.Mechanisms(r) {
			if m.S("effect") == eff {
				ts = append(ts, m.S("title"))
			}
		}
		if len(ts) > 0 {
			parts = append(parts, strings.ToUpper(eff[:1])+eff[1:]+": "+strings.Join(ts, "; "))
		}
	}
	var needs, met []string
	for _, q := range p.RequirementsFrom(r) {
		if len(p.Mechanisms(q)) > 0 {
			met = append(met, q.Name())
		} else {
			needs = append(needs, q.Name())
		}
	}
	if len(met) > 0 {
		parts = append(parts, "met by "+strings.Join(met, ", "))
	}
	if len(needs) > 0 {
		parts = append(parts, "needs "+strings.Join(needs, ", "))
	}
	s := strings.Join(parts, "; ")
	if s != "" {
		s = strings.ToUpper(s[:1]) + s[1:]
	}
	return s
}

func knownColumn(kind, value string) bool {
	derived := map[string][]string{
		"controller": {"uca"}, "action-name": {"uca"}, "type": {"uca"}, "mechanisms": {"constraint", "requirement"},
		"unsafe-with-findings": {"sec"}, "title-with-priority": {"requirement"}, "number": {"incident"},
		"scenario-hazards": {"scenario"}, "findings-or-feedback": {"scenario"}, "status": {"scenario"},
		"answered-by": {"teaming", "sec"}, "pm": {"controller"}, "algorithm": {"controller"}, "former": {"scenario", "uca", "requirement", "hazard"},
	}
	if _, ok := Kinds[kind][value]; ok {
		return true
	}
	ks, ok := derived[value]
	return ok && contains(ks, kind)
}

func code(s string) string { return "`" + s + "`" }

func (p *Project) pmCell(c *Record) string {
	type group struct {
		fb   string
		vars []string
	}
	var groups []*group
	idx := map[string]*group{}
	for _, v := range pmOf(c) {
		var fbs []string
		for _, path := range strList(v["updated_by"]) {
			if e := p.Structure.Entries[path]; e != nil {
				fbs = append(fbs, fmt.Sprintf("%q", e.Label))
			}
		}
		key := strings.Join(fbs, ", ")
		if key == "" {
			key = "(no feedback: " + fmt.Sprint(v["source"]) + ")"
		}
		if idx[key] == nil {
			idx[key] = &group{fb: key}
			groups = append(groups, idx[key])
		}
		idx[key].vars = append(idx[key].vars, code(fmt.Sprint(v["name"])))
	}
	var parts []string
	for _, g := range groups {
		if strings.HasPrefix(g.fb, "(") {
			parts = append(parts, strings.Join(g.vars, ", ")+" "+g.fb)
		} else {
			parts = append(parts, strings.Join(g.vars, ", ")+" ← "+g.fb)
		}
	}
	return strings.Join(parts, "; ")
}

func (p *Project) algoCell(c *Record) string {
	var parts []string
	for _, r := range algoOf(c) {
		var acts []string
		for _, path := range strList(r["issues"]) {
			if e := p.Structure.Entries[path]; e != nil {
				acts = append(acts, "**"+e.Label+"**")
			}
		}
		parts = append(parts, strings.Join(acts, ", ")+" when "+fmt.Sprint(r["when"]))
	}
	return strings.Join(parts, "; ")
}

// TableRows returns the records a table shows, in order: natural label order.
func (p *Project) TableRows(t TableSpec) []*Record {
	re := regexp.MustCompile(t.Labels)
	var out []*Record
	for _, r := range p.Of(t.Kind) {
		if t.Labels != "" && !re.MatchString(r.S("label")) {
			continue
		}
		if t.Batch != "" && r.S("batch") != t.Batch {
			continue
		}
		out = append(out, r)
	}
	if t.Kind == "controller" || t.Kind == "controlled_process" {
		pos := map[string]int{}
		for i, n := range p.Structure.NodeOrder {
			pos[n.ID] = i
		}
		sort.SliceStable(out, func(i, j int) bool { return pos[out[i].ID] < pos[out[j].ID] })
		return out
	}
	sort.SliceStable(out, func(i, j int) bool { return NaturalCompare(out[i].S("label"), out[j].S("label")) < 0 })
	return out
}

// TableCells returns the header and the cell strings of a table.
func (p *Project) TableCells(t TableSpec) ([]string, [][]string) {
	var header []string
	for _, c := range t.Columns {
		header = append(header, c.Header)
	}
	var rows [][]string
	type item struct {
		r      *Record
		former string
	}
	var items []item
	for _, r := range p.TableRows(t) {
		if !t.Formerly {
			items = append(items, item{r, ""})
			continue
		}
		for _, f := range r.L("formerly") {
			items = append(items, item{r, f})
		}
	}
	if t.Formerly {
		sort.SliceStable(items, func(i, j int) bool { return NaturalCompare(items[i].former, items[j].former) < 0 })
	}
	for _, it := range items {
		r := it.r
		var row []string
		for _, c := range t.Columns {
			v, _ := p.columnValue(r, c.Value)
			if c.Value == "former" {
				v = it.former
			}
			if v == "" {
				v = c.Empty
			}
			row = append(row, v)
		}
		rows = append(rows, row)
	}
	return header, rows
}

// RenderTable writes a table between generated-section markers, in the repository's table
// style (`| --- |` separators).
func (p *Project) RenderTable(t TableSpec) string {
	return BeginMarker(t.Name) + p.TableBody(t) + EndMarker(t.Name)
}

// BeginMarker and EndMarker delimit a generated section in a document.
func BeginMarker(name string) string {
	return fmt.Sprintf("<!-- stpa:begin %s (generated from otel-chdb/stpa; edit the records, not this section) -->\n", name)
}

func EndMarker(name string) string { return fmt.Sprintf("<!-- stpa:end %s -->\n", name) }

// TableBody is a table without its markers.
func (p *Project) TableBody(t TableSpec) string {
	header, rows := p.TableCells(t)
	var b strings.Builder
	b.WriteString("| " + strings.Join(header, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat(" --- |", len(header)) + "\n")
	for _, r := range rows {
		for i := range r {
			r[i] = strings.ReplaceAll(r[i], "|", "\\|")
		}
		b.WriteString("| " + strings.Join(r, " | ") + " |\n")
	}
	return b.String()
}

// RenderTables renders every table of a tables view as one Markdown file.
func (p *Project) RenderTables(v *View) string {
	var b strings.Builder
	b.WriteString("<!-- Generated by otel-chdb/stpa/tools (stpa render). Do not edit: edit the records. -->\n")
	for _, t := range v.Tables {
		b.WriteString("\n")
		b.WriteString(p.RenderTable(t))
	}
	return b.String()
}
