package stpa

import (
	"flag"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
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
		{"a former label is unique too", "hazard-00aa0a", "id: hazard-00aa0a\nlabel: UCA-2\ntitle: x\nstate: accepted", "label"},
		{"hex reused across kinds", "loss-000a02", "id: loss-000a02\nlabel: L-2\ntitle: x\nstate: accepted", "id"},
		{"hex of a moved action record", "loss-000c03", "id: loss-000c03\nlabel: L-3\ntitle: x\nstate: accepted", "id"},
		{"missing required field", "uca-00aa05", "id: uca-00aa05\nlabel: UCA-2\naction: janitor->store/control/delete\ncontext: y\nhazards: [hazard-000a02]\nstate: accepted", "schema"},
		{"a UCA may not restate its controller", "uca-00aa06", "id: uca-00aa06\nlabel: UCA-3\naction: janitor->store/control/delete\ncontroller: controller-000b04\ncategory: provided\ncontext: y\nhazards: [hazard-000a02]\nstate: accepted", "schema"},
		{"a UCA's action is a control entry", "uca-00aa07", "id: uca-00aa07\nlabel: UCA-4\naction: writer->store/feedback/status\ncategory: provided\ncontext: y\nhazards: [hazard-000a02]\nstate: accepted", "reference"},
		{"a UCA's action exists", "uca-00aa08", "id: uca-00aa08\nlabel: UCA-5\naction: writer->store/control/nothing\ncategory: provided\ncontext: y\nhazards: [hazard-000a02]\nstate: accepted", "reference"},
		{"a UCA's variable is its controller's", "uca-00aa09", "id: uca-00aa09\nlabel: UCA-6\naction: janitor->store/control/delete\ncategory: provided\ncontext: y\nhazards: [hazard-000a02]\nvariables: [committed]\nstate: accepted", "reference"},
		{"an unresolved label in prose", "incident-00aa0b", "id: incident-00aa0b\nlabel: CAST-2\nbatch: first\ntitle: x\nfound_by: x\nhazard: H-9, which does not exist\ncontroller: x\nwhy: x\nfix: x\nlesson: x\nstate: accepted", "mention"},
		{"a qualified variable that does not exist", "incident-00aa0c", "id: incident-00aa0c\nlabel: CAST-3\nbatch: first\ntitle: x\nfound_by: x\nhazard: H-1\ncontroller: x\nwhy: x\nfix: x\nlesson: x\nvariables: [writer/nothing]\nstate: accepted", "reference"},
		{"a scenario with findings stores no hazards", "scenario-00aa0d", "id: scenario-00aa0d\nlabel: LS-2\ntitle: x\nfindings: [uca-000e01]\nfactor: process-model\nhazards: [hazard-000a02]\nvariables: [janitor/positions]\nstate: accepted", "scenario"},
		{"a feedback factor needs feedback", "scenario-00aa0e", "id: scenario-00aa0e\nlabel: LS-3\ntitle: x\nfindings: [uca-000e01]\nfactor: feedback-missing\nstate: accepted", "scenario"},
		{"a requirement cites a record by id, not label", "requirement-00aa0f", "id: requirement-00aa0f\nlabel: R-2\ntitle: x\npriority: P0\nfrom: [LS-1]\nstate: accepted", "reference"},
		{"a requirement cites nothing unknown", "requirement-00aa10", "id: requirement-00aa10\nlabel: R-3\ntitle: x\npriority: P0\nfrom: [D9]\nstate: accepted", "reference"},
		{"a controller cites only its own links", "controller-00aa11", "id: controller-00aa11\ntitle: x\ncomponent_type: software\nprocess_model: [{name: v, meaning: m, updated_by: [writer->store/feedback/status]}]\nstate: accepted", "controller-internals"},
		{"a process-model variable needs a source", "controller-00aa12", "id: controller-00aa12\ntitle: x\ncomponent_type: software\nprocess_model: [{name: v, meaning: m}]\nstate: accepted", "controller-internals"},
		{"a rule uses process-model variables", "controller-00aa13", "id: controller-00aa13\ntitle: x\ncomponent_type: software\ncontrol_algorithm: [{when: always, uses: [nothing], issues: [janitor->store/control/delete]}]\nstate: accepted", "controller-internals"},
		{"a controller record is a node", "controller-00aa14", "id: controller-00aa14\ntitle: x\ncomponent_type: software\nstate: accepted", "structure"},
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

// Rule names head the rules in the detail diagrams, so two rules of one controller may not
// share a name (compared without case and extra spaces), while two controllers may.
func TestRuleNamesUniqueWithinAController(t *testing.T) {
	load := func(t *testing.T, janitor string) []Finding {
		dir := copyTree(t, "testdata/mini")
		f := filepath.Join(dir, "records", "controller-000b04.md")
		b, _ := os.ReadFile(f)
		old := "  - {name: delete an old slot, when: a slot is below every reader's position, uses: [positions], issues: [janitor->store/control/delete]}\n"
		if !strings.Contains(string(b), old) {
			t.Fatal("fixture text not found")
		}
		os.WriteFile(f, []byte(strings.Replace(string(b), old, janitor, 1)), 0o644)
		p, err := Load(dir)
		if err != nil {
			t.Fatal(err)
		}
		return p.Check()
	}
	dup := "  - {name: delete an old slot, when: a slot is below every reader's position, uses: [positions], issues: [janitor->store/control/delete]}\n" +
		"  - {name: 'Delete an old-slot!', when: a slot is quarantined, uses: [positions], issues: [janitor->store/control/delete]}\n"
	found := false
	for _, f := range load(t, dup) {
		found = found || (!f.Warn && f.Rule == "controller-internals" && strings.Contains(f.Msg, "rule names are unique within a controller"))
	}
	if !found {
		t.Fatal("two rules of one controller named alike were not rejected")
	}
	// A rule without a name is rejected too.
	found = false
	for _, f := range load(t, "  - {when: a slot is below every reader's position, uses: [positions], issues: [janitor->store/control/delete]}\n") {
		found = found || (!f.Warn && f.Rule == "controller-internals" && strings.Contains(f.Msg, "name is required"))
	}
	if !found {
		t.Fatal("a rule without a name was not rejected")
	}
	// The same name on another controller's rule is fine.
	same := "  - {name: commit a batch, when: a slot is below every reader's position, uses: [positions], issues: [janitor->store/control/delete]}\n"
	for _, f := range load(t, same) {
		if !f.Warn {
			t.Fatalf("a name shared across controllers must pass: %s", f)
		}
	}
}

// The detail diagram heads each rule with its name and puts the actions it issues on the line
// below; every text of the PRD variant carries a text id, each once.
func TestDetailRuleHeadingAndTextIDs(t *testing.T) {
	p, err := Load("testdata/mini")
	if err != nil {
		t.Fatal(err)
	}
	n := p.Structure.Nodes["janitor"]
	svg := p.DetailSVG(n)
	head := strings.Index(svg, ">delete an old slot</text>")
	sub := strings.Index(svg, ">▸ delete</text>")
	if head < 0 || sub < head {
		t.Fatalf("want the rule name as heading and the issued action below it:\n%s", svg)
	}
	jsx := p.DetailPRD(n)
	texts := strings.Count(jsx, "<text")
	ids := regexp.MustCompile(`data-claude-text-id='([^']+)'`).FindAllStringSubmatch(jsx, -1)
	if texts == 0 || len(ids) != texts {
		t.Fatalf("%d texts, %d text ids", texts, len(ids))
	}
	seen := map[string]bool{}
	for _, m := range ids {
		if seen[m[1]] {
			t.Fatalf("text id %s used twice", m[1])
		}
		seen[m[1]] = true
	}
	if !seen["controller-000b04-rule-delete-an-old-slot"] || !seen["controller-000b04-rule-delete-an-old-slot-s"] {
		t.Fatalf("rule text ids missing: %v", seen)
	}
}

// Every node name in the repository's detail diagrams fits its box: a name wider than the box
// (five nodes under the platform operators) is wrapped, not drawn across the border.
func TestDetailNodeNamesFitTheirBoxes(t *testing.T) {
	tracetag.Covers(t, "G", "CAST-71")
	p, err := Load(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	group := regexp.MustCompile(`(?s)<g>\s*<rect x="\d+" y="\d+" width="(\d+)" height="(?:44|36)"[^>]*/>(.*?)</g>`)
	name := regexp.MustCompile(`<text[^>]*>([^<]*)</text>`)
	for _, n := range p.Structure.NodeOrder {
		if !n.Controller || n.Rec == nil {
			continue
		}
		svg := p.DetailSVG(n)
		boxes := group.FindAllStringSubmatch(svg, -1)
		if len(boxes) == 0 {
			t.Fatalf("%s: no node boxes found", n.Name)
		}
		for _, b := range boxes {
			var w int
			fmt.Sscan(b[1], &w)
			for _, m := range name.FindAllStringSubmatch(b[2], -1) {
				if got := float64(len([]rune(html.UnescapeString(m[1])))) * nameCharW; got > float64(w-24) {
					t.Errorf("%s: %q (~%.0f px) overflows its %d px box", n.Name, m[1], got, w)
				}
			}
		}
	}
}

// A rule's text ids come from its name, not its position: reordering a controller's rules
// keeps every id on the same words (comments in the PRD hang on the ids).
func TestRuleTextIDsSurviveReordering(t *testing.T) {
	p, err := Load(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	idText := regexp.MustCompile(`data-claude-text-id='([^']+)'[^>]*>([^<]*)<`)
	texts := func(jsx string) map[string]string {
		m := map[string]string{}
		for _, g := range idText.FindAllStringSubmatch(jsx, -1) {
			if strings.Contains(g[1], "-rule-") {
				m[g[1]] = g[2]
			}
		}
		return m
	}
	n := p.Structure.Nodes["consumer"]
	before := texts(p.DetailPRD(n))
	rules := n.Rec.F["control_algorithm"].([]any)
	for i, j := 0, len(rules)-1; i < j; i, j = i+1, j-1 {
		rules[i], rules[j] = rules[j], rules[i]
	}
	after := texts(p.DetailPRD(n))
	if len(before) == 0 || len(before) != len(after) {
		t.Fatalf("rule texts: %d before, %d after", len(before), len(after))
	}
	for id, w := range before {
		if after[id] != w {
			t.Errorf("%s: %q before the reordering, %q after", id, w, after[id])
		}
	}
}

// The overview's compartment strips are read in the PRD, whose column is 672 px wide: every
// strip label lands at 10.5 px or more at the widget's displayed scale.
func TestOverviewStripLabelsLegibleInThePRD(t *testing.T) {
	p, err := Load(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	strip := regexp.MustCompile(`data-claude-text-id='[^']*-(?:ca|pm)(?:-n|-[a-z0-9-]+)?'[^>]*fontSize='([0-9.]+)'`)
	for _, v := range p.Views {
		if v.Type != "control-structure" || v.Internals == "" {
			continue
		}
		L := p.Place(v)
		scale := 672.0 / float64(L.W)
		if scale > 1 {
			scale = 1
		}
		found := 0
		for _, m := range strip.FindAllStringSubmatch(L.PRD(), -1) {
			var fs float64
			fmt.Sscan(m[1], &fs)
			found++
			if fs*scale < 10.5 {
				t.Errorf("%s: a strip label at %.1f px authored shows at %.1f px (width %d)", v.Name, fs, fs*scale, L.W)
			}
		}
		if found == 0 {
			t.Errorf("%s: no strip labels found", v.Name)
		}
	}
}

// JSX text is literal text: the PRD refuses an expression such as {'<'} in a text that carries
// a text id (the consumer's "now + budget <= safe_until"), so special characters are entities.
func TestJSXTextIsLiteral(t *testing.T) {
	tracetag.Covers(t, "G", "CAST-70")
	if got, want := jsxText("now + budget <= safe_until {x} & y > z"), "now + budget &lt;= safe_until &#123;x&#125; &amp; y &gt; z"; got != want {
		t.Fatalf("jsxText = %q, want %q", got, want)
	}
}

// The control structure's forbidden forms. The first group cannot be written: the parser
// refuses the document. The second group parses and fails the check.
const miniStructure = `nodes:
  ops: controller-000b01
  writer: controller-000b02
  store: controlled_process-000b03
  janitor: controller-000b04
links:
  ops -> writer:
    control:
      config: {label: config, was: action-000c01}
  ops -> janitor:
    feedback:
      health: {label: "health, cost", was: feedback-000d02}
  writer -> store:
    control:
      commit: {label: commit, title: Commit a batch, was: action-000c02}
    feedback:
      status: {label: commit status, was: feedback-000d01}
  janitor -> store:
    control:
      delete: {label: delete, title: Delete a slot, was: action-000c03}
`

func TestStructureRefusesUnrepresentableForms(t *testing.T) {
	cases := []struct{ name, old, new, want string }{
		{"duplicate pair", "  janitor -> store:\n", "  writer -> store:\n    control: {x: x}\n  janitor -> store:\n", `duplicate key "writer -> store"`},
		{"duplicate action on a pair", "      commit: {label: commit, title: Commit a batch, was: action-000c02}\n",
			"      commit: {label: commit, title: Commit a batch, was: action-000c02}\n      commit: again\n", `duplicate key "commit"`},
		{"duplicate feedback on a pair", "      status: {label: commit status, was: feedback-000d01}\n",
			"      status: {label: commit status, was: feedback-000d01}\n      status: again\n", `duplicate key "status"`},
		{"duplicate node", "  janitor: controller-000b04\n", "  janitor: controller-000b04\n  janitor: controller-000b02\n", `duplicate key "janitor"`},
		{"a pair spelled another way", "  janitor -> store:", "  janitor->store:", "must be spelled"},
		{"a controlled process at the upper end", "  janitor -> store:", "  store -> janitor:", "the upper end store is a controlled process"},
		{"an unknown node", "  janitor -> store:", "  janitor -> disk:", `"disk" is not a node`},
		{"a node that is not a controller or process record", "  janitor: controller-000b04", "  janitor: hazard-000a02", "must be the id of a controller or controlled_process record"},
		{"an unknown key in a link", "    control:\n      delete:", "    controls:\n      delete:", `unknown key "controls"`},
		{"an entry without a label", "      delete: {label: delete, title: Delete a slot, was: action-000c03}", "      delete: {title: Delete a slot}", "label is required"},
		{"an empty link", "  janitor -> store:\n    control:\n      delete: {label: delete, title: Delete a slot, was: action-000c03}\n", "  janitor -> store: {}\n", "has no control and no feedback entry"},
	}
	if _, err := ParseStructure("structure.yaml", []byte(miniStructure), nil); err != nil {
		t.Fatalf("the base structure must parse: %v", err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := strings.Replace(miniStructure, c.old, c.new, 1)
			if src == miniStructure {
				t.Fatal("fixture text not found")
			}
			_, err := ParseStructure("structure.yaml", []byte(src), nil)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want an error containing %q, got %v", c.want, err)
			}
		})
	}
}

func TestStructureCheckedForms(t *testing.T) {
	cases := []struct{ name, old, new, want string }{
		{"a pair keyed in both orders", "  janitor -> store:\n", "  writer -> ops:\n    control: {x: x}\n  janitor -> store:\n", "also keyed the other way round"},
		{"a self link", "  janitor -> store:\n", "  janitor -> janitor:\n    control: {x: x}\n  janitor -> store:\n", "does not control itself"},
		{"a cycle of control", "  janitor -> store:\n", "  writer -> janitor:\n    control: {x: x}\n  janitor -> ops:\n    control: {y: y}\n  janitor -> store:\n", "control runs in a cycle"},
		{"a node naming no record", "  janitor: controller-000b04\n", "  janitor: controller-000b04\n  ghost: controlled_process-00ffff\n", "is not a record"},
		{"two nodes for one record", "  janitor: controller-000b04\n", "  janitor: controller-000b04\n  janitor2: controller-000b04\n", "name the same record"},
		{"a feedback no process-model variable cites", "      status: {label: commit status, was: feedback-000d01}\n",
			"      status: {label: commit status, was: feedback-000d01}\n      latency: {label: latency}\n", "updates no process-model variable"},
		{"a control action no rule issues", "      delete: {label: delete, title: Delete a slot, was: action-000c03}\n",
			"      delete: {label: delete, title: Delete a slot, was: action-000c03}\n      compact: {label: compact}\n", "is issued by no rule"},
		{"a controller that controls nothing", "  ops -> writer:\n    control:\n      config: {label: config, was: action-000c01}\n",
			"  ops -> writer:\n    feedback:\n      config: {label: config}\n", "issues no control action"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := strings.Replace(miniStructure, c.old, c.new, 1)
			if src == miniStructure {
				t.Fatal("fixture text not found")
			}
			dir := copyTree(t, "testdata/mini")
			if err := os.WriteFile(filepath.Join(dir, "structure.yaml"), []byte(src), 0o644); err != nil {
				t.Fatal(err)
			}
			p, err := Load(dir)
			if err != nil {
				t.Fatalf("must parse (it is a checked form, not a refused one): %v", err)
			}
			found := false
			var all []string
			for _, f := range p.Check() {
				all = append(all, f.String())
				found = found || (!f.Warn && strings.Contains(f.Msg, c.want))
			}
			if !found {
				t.Fatalf("want an error containing %q, got:\n%s", c.want, strings.Join(all, "\n"))
			}
		})
	}
}

// A hand edit inside a generated section of a document is detected, and render restores it.
func TestSplicedSectionEditIsDetected(t *testing.T) {
	dir := copyTree(t, "testdata/mini")
	f := filepath.Join(dir, "doc.md")
	b, _ := os.ReadFile(f)
	edited := strings.Replace(string(b), "| UCA-1 | Janitor |", "| UCA-1 | Writer |", 1)
	if edited == string(b) {
		t.Fatal("fixture text not found")
	}
	os.WriteFile(f, []byte(edited), 0o644)
	p, _ := Load(dir)
	stale := p.Stale()
	if len(stale) != 1 || !strings.Contains(stale[0], "doc.md: section ucas differs") {
		t.Fatalf("want doc.md's ucas section stale, got %v", stale)
	}
	if _, err := p.Write(); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(f); string(got) != string(b) {
		t.Fatal("render did not restore the section")
	}
	if !strings.Contains(string(b), "Prose between sections is the document's own.") {
		t.Fatal("prose outside the sections must be kept")
	}
}

func TestDerivedValues(t *testing.T) {
	p, _ := Load("testdata/mini")
	u := p.Records["uca-000e01"]
	if c := p.UCAController(u); c == nil || c.ID != "controller-000b04" {
		t.Fatalf("UCA controller must be the upper end of its action's link, got %v", c)
	}
	if p.IsController(p.Records["controlled_process-000b03"]) {
		t.Fatal("the store is a controlled process")
	}
	if ms := p.Mechanisms(p.Records["constraint-000f01"]); len(ms) != 1 {
		t.Fatalf("constraint mechanisms: %v", ms)
	}
	if st := p.ScenarioStatus(p.Records["scenario-000e02"]); st != "Met by R-1" {
		t.Fatalf("scenario status is derived from the requirements that cite it: %q", st)
	}
	if p.ByLabel["UCA-2"] == nil || p.ByLabel["UCA-2"].ID != "scenario-000e02" {
		t.Fatal("a former label resolves to the record that holds it now")
	}
	if e := p.Structure.Was["action-000c03"]; e == nil || e.Path() != "janitor->store/control/delete" {
		t.Fatal("a moved action record's id resolves to its structure entry")
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
// the upper end of its action's link, a controller that is also controlled becomes two
// records, and the structure's entries become v0 action and feedback records under their old
// ids.
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
	if u := read("uca-000e01"); u["controller"] != "controller-000b04" || u["action"] != "action-000c03" {
		t.Fatalf("uca export: %v", u)
	}
	read("controller-000b02")
	read("process-000b02")
	read("responsibility-000b02")
	if _, err := os.Stat(filepath.Join(dir, "records", "controller-000b03.md")); err == nil {
		t.Fatal("the store issues no action and must not become a controller")
	}
	if f := read("feedback-000d01"); f["from"] != "process-000b03" || f["to"] != "controller-000b02" {
		t.Fatalf("feedback export: %v", f)
	}
	if c := read("controller-000b02"); c["process_model"] == nil {
		t.Fatalf("the process model is exported: %v", c)
	}
}
