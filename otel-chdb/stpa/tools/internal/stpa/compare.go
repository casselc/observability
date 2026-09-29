package stpa

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Discrepancy is a difference between a generated table row and the hand-kept row with the
// same key in a document.
type Discrepancy struct {
	Doc, Table, Key, Column, Generated, Hand string
}

func (d Discrepancy) String() string {
	if d.Column == "" {
		return fmt.Sprintf("%s [%s] %s: %s", d.Doc, d.Table, d.Key, d.Generated)
	}
	return fmt.Sprintf("%s [%s] %s, column %q:\n    generated: %s\n    hand-kept: %s", d.Doc, d.Table, d.Key, d.Column, d.Generated, d.Hand)
}

type mdTable struct {
	header []string
	rows   map[string][]string
}

// splitRow splits a Markdown table row, respecting backtick spans.
func splitRow(line string) []string {
	s := strings.TrimSpace(line)
	s = strings.TrimSuffix(strings.TrimPrefix(s, "|"), "|")
	var out []string
	var cur strings.Builder
	tick := false
	for _, ch := range s {
		if ch == '`' {
			tick = !tick
		}
		if ch == '|' && !tick {
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		cur.WriteRune(ch)
	}
	return append(out, strings.TrimSpace(cur.String()))
}

var sepRow = regexp.MustCompile(`^\|\s*:?-{3}`)

func parseTables(text string) []mdTable {
	lines := strings.Split(text, "\n")
	var out []mdTable
	for i := 0; i+1 < len(lines); i++ {
		if strings.HasPrefix(lines[i], "|") && sepRow.MatchString(lines[i+1]) {
			t := mdTable{header: splitRow(lines[i]), rows: map[string][]string{}}
			i += 2
			for ; i < len(lines) && strings.HasPrefix(lines[i], "|"); i++ {
				c := splitRow(lines[i])
				t.rows[c[0]] = c
			}
			out = append(out, t)
		}
	}
	return out
}

var spaces = regexp.MustCompile(`\s+`)

func norm(s string) string { return strings.TrimSpace(spaces.ReplaceAllString(s, " ")) }

// Compare checks every table that names a hand-kept document against that document's table
// with the same header (the one holding most of the generated rows). It returns the
// discrepancies and, per table, how many rows matched and how many of the document's rows are
// not yet records.
func (p *Project) Compare(docRoot string) ([]Discrepancy, []string) {
	var ds []Discrepancy
	var summary []string
	for _, v := range p.Views {
		for _, t := range v.Tables {
			if t.Doc == "" {
				continue
			}
			b, err := os.ReadFile(filepath.Join(docRoot, t.Doc))
			if err != nil {
				ds = append(ds, Discrepancy{Doc: t.Doc, Table: t.Name, Generated: err.Error()})
				continue
			}
			header, rows := p.TableCells(t)
			var best *mdTable
			bestHits := 0
			for _, dt := range parseTables(string(b)) {
				if norm(dt.header[0]) != norm(header[0]) {
					continue
				}
				hits := 0
				for _, r := range rows {
					if dt.rows[r[0]] != nil {
						hits++
					}
				}
				if hits > bestHits {
					dt := dt
					best, bestHits = &dt, hits
				}
			}
			if best == nil {
				ds = append(ds, Discrepancy{Doc: t.Doc, Table: t.Name, Generated: "no table in the document holds these rows"})
				continue
			}
			// Columns by header; a header that is a prefix of the other ("UCA" / "UCA or feedback")
			// is the same column renamed.
			col := make([]int, len(header))
			for c := range header {
				col[c] = -1
				for d, h := range best.header {
					g, hh := norm(header[c]), norm(h)
					if g == hh || strings.HasPrefix(g, hh) || strings.HasPrefix(hh, g) {
						col[c] = d
						break
					}
				}
				if col[c] < 0 {
					ds = append(ds, Discrepancy{Doc: t.Doc, Table: t.Name, Key: "(header)", Generated: fmt.Sprintf("column %q is new (not in the document)", header[c])})
				}
			}
			same := 0
			for _, r := range rows {
				h := best.rows[r[0]]
				if h == nil {
					ds = append(ds, Discrepancy{Doc: t.Doc, Table: t.Name, Key: r[0], Generated: "row is a record but not in the document"})
					continue
				}
				ok := true
				for c := range r {
					if col[c] < 0 {
						continue
					}
					hv := ""
					if col[c] < len(h) {
						hv = h[col[c]]
					}
					if norm(r[c]) != norm(hv) {
						ok = false
						ds = append(ds, Discrepancy{Doc: t.Doc, Table: t.Name, Key: r[0], Column: header[c], Generated: r[c], Hand: hv})
					}
				}
				if ok {
					same++
				}
			}
			summary = append(summary, fmt.Sprintf("%s [%s]: %d rows generated, %d identical to the document, %d document rows not yet records",
				t.Doc, t.Name, len(rows), same, len(best.rows)-bestHits))
		}
	}
	return ds, summary
}
