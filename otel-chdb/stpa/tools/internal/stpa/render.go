package stpa

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Outputs renders every generated file, keyed by its path relative to the project root.
func (p *Project) Outputs() map[string]string {
	out := map[string]string{}
	for _, v := range p.Views {
		switch v.Type {
		case "control-structure":
			L := p.Place(v)
			out["generated/"+v.Name+".svg"] = L.SVG()
			out["generated/"+v.Name+".prd.jsx"] = L.PRD()
			out["generated/"+v.Name+".mmd"] = L.Mermaid()
		case "tables":
			out["generated/"+v.Name+".md"] = p.RenderTables(v)
		case "controller-details":
			for _, n := range p.detailNodes(v) {
				out["generated/controller-"+n.Name+".svg"] = p.DetailSVG(n)
				out["generated/controller-"+n.Name+".prd.jsx"] = p.DetailPRD(n)
				out["generated/controller-"+n.Name+".mmd"] = p.DetailMermaid(n)
			}
		}
	}
	out["generated/labels.json"] = p.LabelsJSON()
	return out
}

// LabelsJSON is the citation catalogue: every citation label with its record, every former
// label (alias_of the label that holds it now), every control-structure entry by its path, and
// the ids that moved (the pilot's action and feedback records, alias_of an entry's path; its
// component records, alias_of the controller or controlled_process with the same hex). Tools
// that resolve the IDs cited in code, tests and commits read this.
func (p *Project) LabelsJSON() string {
	type entry struct {
		ID      string `json:"id,omitempty"`
		Kind    string `json:"kind"`
		Title   string `json:"title,omitempty"`
		Path    string `json:"path"`
		AliasOf string `json:"alias_of,omitempty"`
	}
	all := map[string]entry{}
	for l, r := range p.ByLabel {
		e := entry{ID: r.ID, Kind: r.Kind, Title: r.S("title"), Path: r.Path}
		if r.S("label") != l {
			e.AliasOf = r.S("label")
		}
		all[l] = e
	}
	for _, e := range p.Structure.List {
		all[e.Path()] = entry{Kind: e.Kind, Title: e.Name(), Path: p.Structure.Path}
		if e.Was != "" {
			all[e.Was] = entry{Kind: strings.SplitN(e.Was, "-", 2)[0], Path: p.Structure.Path, AliasOf: e.Path()}
		}
	}
	for _, n := range p.Structure.NodeOrder {
		if n.Rec != nil {
			all["component-"+Hex(n.ID)] = entry{Kind: "component", Title: n.Rec.S("title"), Path: n.Rec.Path, AliasOf: n.ID}
		}
	}
	keys := make([]string, 0, len(all))
	for k := range all {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return NaturalCompare(keys[i], keys[j]) < 0 })
	var b strings.Builder
	b.WriteString("{\n")
	for i, k := range keys {
		e, _ := json.Marshal(all[k])
		kj, _ := json.Marshal(k)
		fmt.Fprintf(&b, "  %s: %s", kj, e)
		if i < len(keys)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString("}\n")
	return b.String()
}

// detailNodes are the controller nodes a controller-details view draws: those it names, or
// every controller of the structure.
func (p *Project) detailNodes(v *View) []*Node {
	var out []*Node
	for _, n := range p.Structure.NodeOrder {
		if n.Controller && n.Rec != nil && (len(v.Controllers) == 0 || contains(v.Controllers, n.Name)) {
			out = append(out, n)
		}
	}
	return out
}

// Sections are the generated sections a document may hold between stpa markers: every table
// of a tables view, and every control-structure view as a Mermaid block.
func (p *Project) Sections() map[string]string {
	out := map[string]string{}
	for _, v := range p.Views {
		switch v.Type {
		case "control-structure":
			out[v.Name] = "```mermaid\n" + p.Place(v).Mermaid() + "```\n"
		case "tables":
			for _, t := range v.Tables {
				out[t.Name] = p.TableBody(t)
			}
		case "controller-details":
			// The detail diagrams as images, relative to the documents' root, each folded.
			dr, _ := filepath.Abs(p.DocRoot())
			pr, _ := filepath.Abs(p.Root)
			rel, _ := filepath.Rel(dr, pr)
			var b strings.Builder
			for _, n := range p.detailNodes(v) {
				fmt.Fprintf(&b, "<details><summary>%s</summary>\n\n![%s: control algorithm and process model](%s)\n\n</details>\n",
					n.Rec.S("title"), n.Rec.S("title"), filepath.ToSlash(filepath.Join(rel, "generated", "controller-"+n.Name+".svg")))
			}
			out[v.Name] = b.String()
		}
	}
	return out
}

var sectionRE = regexp.MustCompile(`(?s)(<!-- stpa:begin (\S+)[^\n]*-->\n)(.*?)(<!-- stpa:end (\S+) -->)`)

// SpliceDocs are the documents (relative to the directory above the project) that hold
// generated sections.
func (p *Project) SpliceDocs() []string {
	var out []string
	for _, v := range p.Views {
		out = append(out, v.Splice...)
	}
	sort.Strings(out)
	return out
}

// Splice returns doc with every generated section replaced by its current content.
func (p *Project) Splice(text string) (string, error) {
	secs := p.Sections()
	var err error
	out := sectionRE.ReplaceAllStringFunc(text, func(m string) string {
		g := sectionRE.FindStringSubmatch(m)
		if g[2] != g[5] {
			err = fmt.Errorf("section %s ends with the marker of %s", g[2], g[5])
			return m
		}
		body, ok := secs[g[2]]
		if !ok {
			err = fmt.Errorf("section %s: no view or table of that name", g[2])
			return m
		}
		return g[1] + body + g[4]
	})
	return out, err
}

func (p *Project) docPath(doc string) string { return filepath.Join(p.DocRoot(), doc) }

// Write writes every output, splices the documents' generated sections, and removes
// generated files no view produces any more.
func (p *Project) Write() ([]string, error) {
	outs := p.Outputs()
	var spliced []string
	for _, doc := range p.SpliceDocs() {
		b, err := os.ReadFile(p.docPath(doc))
		if err != nil {
			return nil, err
		}
		s, err := p.Splice(string(b))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", doc, err)
		}
		if s != string(b) {
			if err := os.WriteFile(p.docPath(doc), []byte(s), 0o644); err != nil {
				return nil, err
			}
			spliced = append(spliced, doc)
		}
	}
	var written []string
	for rel, content := range outs {
		path := filepath.Join(p.Root, rel)
		if old, err := os.ReadFile(path); err == nil && string(old) == content {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return nil, err
		}
		written = append(written, rel)
	}
	existing, _ := filepath.Glob(filepath.Join(p.Root, "generated", "*"))
	for _, f := range existing {
		rel, _ := filepath.Rel(p.Root, f)
		if _, ok := outs[rel]; !ok {
			if err := os.Remove(f); err != nil {
				return nil, err
			}
			written = append(written, rel+" (removed)")
		}
	}
	written = append(written, spliced...)
	sort.Strings(written)
	return written, nil
}

// Stale lists generated files that differ from what the records render: edited by hand, or
// not re-rendered after a record changed.
func (p *Project) Stale() []string {
	outs := p.Outputs()
	var stale []string
	for rel, content := range outs {
		old, err := os.ReadFile(filepath.Join(p.Root, rel))
		if err != nil {
			stale = append(stale, rel+": missing")
		} else if string(old) != content {
			stale = append(stale, rel+": differs from the records (edited by hand, or not re-rendered)")
		}
	}
	existing, _ := filepath.Glob(filepath.Join(p.Root, "generated", "*"))
	for _, f := range existing {
		rel, _ := filepath.Rel(p.Root, f)
		if _, ok := outs[rel]; !ok {
			stale = append(stale, rel+": produced by no view")
		}
	}
	for _, doc := range p.SpliceDocs() {
		b, err := os.ReadFile(p.docPath(doc))
		if err != nil {
			stale = append(stale, doc+": "+err.Error())
			continue
		}
		s, err := p.Splice(string(b))
		if err != nil {
			stale = append(stale, doc+": "+err.Error())
		} else if s != string(b) {
			for _, g := range sectionRE.FindAllStringSubmatch(string(b), -1) {
				if g[3] != p.Sections()[g[2]] {
					stale = append(stale, doc+": section "+g[2]+" differs from the records (edited by hand, or not re-rendered)")
				}
			}
		}
	}
	sort.Strings(stale)
	return stale
}
