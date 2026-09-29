package stpa

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
		}
	}
	out["generated/labels.json"] = p.LabelsJSON()
	return out
}

// LabelsJSON is the citation catalogue: every citation label with its record, for tools that
// resolve the IDs cited in code, tests and commits (the traceability check).
func (p *Project) LabelsJSON() string {
	type entry struct {
		ID    string `json:"id"`
		Kind  string `json:"kind"`
		Title string `json:"title,omitempty"`
		Path  string `json:"path"`
	}
	labels := make([]string, 0, len(p.ByLabel))
	for l := range p.ByLabel {
		labels = append(labels, l)
	}
	sort.Slice(labels, func(i, j int) bool { return NaturalCompare(labels[i], labels[j]) < 0 })
	var b strings.Builder
	b.WriteString("{\n")
	for i, l := range labels {
		r := p.ByLabel[l]
		e, _ := json.Marshal(entry{r.ID, r.Kind, r.S("title"), r.Path})
		k, _ := json.Marshal(l)
		fmt.Fprintf(&b, "  %s: %s", k, e)
		if i < len(labels)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString("}\n")
	return b.String()
}

// Write writes every output and removes generated files no view produces any more.
func (p *Project) Write() ([]string, error) {
	outs := p.Outputs()
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
	sort.Strings(stale)
	return stale
}
