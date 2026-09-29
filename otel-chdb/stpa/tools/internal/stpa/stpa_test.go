package stpa

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var update = flag.Bool("update", false, "rewrite the golden files under testdata/mini/generated")

// The golden test: the mini fixture renders byte-identically to its committed outputs (SVG,
// PRD widget, Mermaid, tables, label catalogue). Run with -update after an intended change and
// review the diff.
func TestGolden(t *testing.T) {
	p, err := Load("testdata/mini")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range p.Check() {
		if !f.Warn {
			t.Errorf("mini fixture does not check: %s", f)
		}
	}
	if *update {
		if _, err := p.Write(); err != nil {
			t.Fatal(err)
		}
	}
	if stale := p.Stale(); len(stale) > 0 {
		t.Errorf("golden files differ (run go test -run TestGolden -update and review):\n%s", strings.Join(stale, "\n"))
	}
}

// The repository's generated files are up to date with its records: a hand edit of a generated
// table or diagram, or a record change without a re-render, fails here (and so in CI).
func TestRepositoryGeneratedUpToDate(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	p, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range p.Check() {
		if !f.Warn {
			t.Errorf("%s", f)
		}
	}
	for _, s := range p.Stale() {
		t.Errorf("otel-chdb/stpa/%s; run `go run ./cmd/stpa render` in otel-chdb/stpa/tools", s)
	}
}

func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestHandEditOfAGeneratedFileIsDetected(t *testing.T) {
	dir := copyTree(t, "testdata/mini")
	f := filepath.Join(dir, "generated", "tables.md")
	b, _ := os.ReadFile(f)
	edited := strings.Replace(string(b), "Before every reader has passed it", "Before any reader has passed it", 1)
	if edited == string(b) {
		t.Fatal("fixture text not found")
	}
	os.WriteFile(f, []byte(edited), 0o644)
	p, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	stale := p.Stale()
	if len(stale) != 1 || !strings.HasPrefix(stale[0], "generated/tables.md") {
		t.Fatalf("want exactly generated/tables.md stale, got %v", stale)
	}
	// A record change without a re-render is detected the same way.
	os.WriteFile(f, b, 0o644)
	rec := filepath.Join(dir, "records", "uca-000e01.md")
	rb, _ := os.ReadFile(rec)
	os.WriteFile(rec, []byte(strings.Replace(string(rb), "category: too-early-too-late-wrong-order", "category: provided", 1)), 0o644)
	p, _ = Load(dir)
	if len(p.Stale()) == 0 {
		t.Fatal("a changed record with stale outputs was not detected")
	}
}

func writeRec(t *testing.T, dir, id, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "records", id+".md"), []byte("---\n"+body+"\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func rules(fs []Finding) map[string]bool {
	m := map[string]bool{}
	for _, f := range fs {
		if !f.Warn {
			m[f.Rule] = true
		}
	}
	return m
}

func TestCheckRejects(t *testing.T) {
	cases := []struct {
		name, id, body, rule string
	}{
		{"unknown field", "hazard-00aa01", "id: hazard-00aa01\nlabel: H-7\ntitle: x\nstate: accepted\ncontroller: someone", "schema"},
		{"dangling reference", "hazard-00aa02", "id: hazard-00aa02\nlabel: H-8\ntitle: x\nlosses: [loss-ffffff]\nstate: accepted", "reference"},
		{"reference of the wrong kind", "hazard-00aa03", "id: hazard-00aa03\nlabel: H-9\ntitle: x\nlosses: [hazard-000a02]\nstate: accepted", "reference"},
		{"duplicate citation label", "hazard-00aa04", "id: hazard-00aa04\nlabel: H-1\ntitle: x\nstate: accepted", "label"},
		{"hex reused across kinds", "loss-000a02", "id: loss-000a02\nlabel: L-2\ntitle: x\nstate: accepted", "id"},
		{"missing required field", "uca-00aa05", "id: uca-00aa05\nlabel: UCA-2\naction: action-000c03\ncontext: y\nhazards: [hazard-000a02]\nstate: accepted", "schema"},
		{"a UCA may not restate its controller", "uca-00aa06", "id: uca-00aa06\nlabel: UCA-3\naction: action-000c03\ncontroller: component-000b04\ncategory: provided\ncontext: y\nhazards: [hazard-000a02]\nstate: accepted", "schema"},
		{"feedback to a non-controller", "feedback-00aa07", "id: feedback-00aa07\nlabel: x\nfrom: component-000b02\nto: component-000b03\nstate: accepted", "feedback-to-controller"},
		{"self edge", "action-00aa08", "id: action-00aa08\nlabel: x\nfrom: component-000b02\nto: component-000b02\nstate: accepted", "self-edge"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := copyTree(t, "testdata/mini")
			writeRec(t, dir, c.id, c.body)
			p, err := Load(dir)
			if err != nil {
				t.Fatal(err)
			}
			if got := rules(p.Check()); !got[c.rule] {
				t.Fatalf("want rule %q, got %v", c.rule, got)
			}
		})
	}
}

func TestMentionOfAMissingLabelWarns(t *testing.T) {
	p, _ := Load("testdata/mini")
	found := false
	for _, f := range p.Check() {
		if f.Rule == "mention" && strings.Contains(f.Msg, "H-9") {
			found = f.Warn
		}
	}
	if !found {
		t.Fatal("H-9 in a CAST hazard cell should be reported as an unresolved mention")
	}
}

func TestDerivedValues(t *testing.T) {
	p, _ := Load("testdata/mini")
	u := p.Records["uca-000e01"]
	if c := p.UCAController(u); c == nil || c.ID != "component-000b04" {
		t.Fatalf("UCA controller must be its action's issuer, got %v", c)
	}
	if p.IsController(p.Records["component-000b03"]) {
		t.Fatal("the store issues no action, so it is a controlled process")
	}
	if ms := p.Mechanisms(p.Records["constraint-000f01"]); len(ms) != 1 {
		t.Fatalf("constraint mechanisms: %v", ms)
	}
}

func TestWrap(t *testing.T) {
	cases := map[string][]string{
		"agents, publishers, buffer; commit protocol per lane": {"agents, publishers, buffer", "commit protocol per lane"},
		"delete old slots; late-copy audit; snapshots":         {"delete old slots", "late-copy audit; snapshots"},
		"leases, time-bound inserts, count check and repair":   {"leases, time-bound inserts", "count check and repair"},
	}
	for in, want := range cases {
		got := wrap(in, 212)
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("wrap(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNaturalCompare(t *testing.T) {
	if NaturalCompare("UCA-2", "UCA-10") >= 0 || NaturalCompare("R-S10", "R-S9") <= 0 || NaturalCompare("H-E1", "H-E1") != 0 {
		t.Fatal("natural order")
	}
}

// The v0 export carries the copies v0 stores redundantly, computed: the UCA's controller is
// its action's issuer, and a component that both acts and is acted on becomes two records.
func TestExportV0(t *testing.T) {
	p, _ := Load("testdata/mini")
	dir := t.TempDir()
	if err := p.ExportV0(dir); err != nil {
		t.Fatal(err)
	}
	read := func(id string) map[string]any {
		b, err := os.ReadFile(filepath.Join(dir, "records", id+".md"))
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		end := strings.Index(s[4:], "\n---\n")
		var m map[string]any
		if err := yaml.Unmarshal([]byte(s[4:4+end+1]), &m); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(s, "\n---\n# "+m["title"].(string)+"\n") {
			t.Errorf("%s: body must start with the title heading", id)
		}
		return m
	}
	if u := read("uca-000e01"); u["controller"] != "controller-000b04" || u["kind"] != "uca" {
		t.Fatalf("uca export: %v", u)
	}
	read("controller-000b02")
	read("process-000b02")
	if _, err := os.Stat(filepath.Join(dir, "records", "controller-000b03.md")); err == nil {
		t.Fatal("the store issues no action and must not become a controller")
	}
	if f := read("feedback-000d01"); f["from"] != "process-000b03" || f["to"] != "controller-000b02" {
		t.Fatalf("feedback export: %v", f)
	}
}
