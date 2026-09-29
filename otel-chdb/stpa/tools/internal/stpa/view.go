package stpa

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// View is a generated view: a control-structure diagram or a set of Markdown tables. The view
// file holds only presentation (which components, in which row and column, the title); every
// box, edge and label comes from the records.
type View struct {
	Path    string      `yaml:"-"`
	Name    string      `yaml:"-"` // file base name without .yaml; output files are named after it
	ID      string      `yaml:"id"`
	Type    string      `yaml:"type"` // control-structure | tables
	Title   string      `yaml:"title"`
	Columns int         `yaml:"columns"`
	Rows    [][]Cell    `yaml:"rows"`
	Tables  []TableSpec `yaml:"tables"`
}

// Cell places one component in a diagram row.
type Cell struct {
	Ref    string `yaml:"ref"`
	Col    int    `yaml:"col"`  // 0-based column; default: after the previous cell
	Span   int    `yaml:"span"` // columns covered; default 1
	Detail *bool  `yaml:"detail"`
}

// UnmarshalYAML accepts a bare id as shorthand for {ref: id}.
func (c *Cell) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		c.Ref = n.Value
		c.Col = -1
		return nil
	}
	type plain Cell
	p := plain{Col: -1}
	if err := n.Decode(&p); err != nil {
		return err
	}
	*c = Cell(p)
	return nil
}

// TableSpec is one generated Markdown table.
type TableSpec struct {
	Name    string      `yaml:"name"`
	Kind    string      `yaml:"kind"`
	Labels  string      `yaml:"labels"` // regexp on the label; empty = all of the kind
	Batch   string      `yaml:"batch"`  // incidents only
	Columns []ColumnDef `yaml:"columns"`
	Doc     string      `yaml:"doc"` // the document whose hand-kept copy is compared (relative to otel-chdb/)
}

// ColumnDef names a column and the field (or derived value) it shows.
type ColumnDef struct {
	Header string `yaml:"header"`
	Value  string `yaml:"value"`
	Empty  string `yaml:"empty"` // shown when the value is empty
}

func loadView(path string) (*View, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	v := &View{Path: path, Name: strings.TrimSuffix(filepath.Base(path), ".yaml")}
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if v.Columns == 0 {
		v.Columns = 3
	}
	for _, row := range v.Rows {
		next := 0
		for i := range row {
			if row[i].Span == 0 {
				row[i].Span = 1
			}
			if row[i].Col < 0 {
				row[i].Col = next
			}
			next = row[i].Col + row[i].Span
		}
	}
	return v, nil
}

func (v *View) check(p *Project) []string {
	var errs []string
	switch v.Type {
	case "control-structure":
		seen := map[string]bool{}
		for ri, row := range v.Rows {
			used := make([]bool, v.Columns)
			for _, c := range row {
				r := p.Records[c.Ref]
				if r == nil || r.Kind != "component" {
					errs = append(errs, fmt.Sprintf("row %d: %s is not a component", ri, c.Ref))
				}
				if seen[c.Ref] {
					errs = append(errs, fmt.Sprintf("row %d: %s placed twice", ri, c.Ref))
				}
				seen[c.Ref] = true
				if c.Col+c.Span > v.Columns {
					errs = append(errs, fmt.Sprintf("row %d: %s spans past column %d", ri, c.Ref, v.Columns))
					continue
				}
				for k := c.Col; k < c.Col+c.Span; k++ {
					if used[k] {
						errs = append(errs, fmt.Sprintf("row %d: column %d used twice", ri, k))
					}
					used[k] = true
				}
			}
		}
	case "tables":
		for _, t := range v.Tables {
			if _, ok := Kinds[t.Kind]; !ok {
				errs = append(errs, fmt.Sprintf("table %s: unknown kind %s", t.Name, t.Kind))
			}
			if _, err := regexp.Compile(t.Labels); err != nil {
				errs = append(errs, fmt.Sprintf("table %s: %v", t.Name, err))
			}
			for _, c := range t.Columns {
				if !knownColumn(t.Kind, c.Value) {
					errs = append(errs, fmt.Sprintf("table %s: no value %q for kind %s", t.Name, c.Value, t.Kind))
				}
			}
		}
	default:
		errs = append(errs, "unknown view type "+v.Type)
	}
	return errs
}
