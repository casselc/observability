package rule

import (
	"strings"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	rs, err := Parse([]byte(`
rules:
  - name: errors_by_service
    sql: SELECT ServiceName AS service, count() AS value FROM otel_logs WHERE SeverityText = 'ERROR' GROUP BY service
    window: 5m
    every: 1m
    for: 2m
    condition: {op: ">", threshold: 10}
    labels: {team: shop}
`))
	if err != nil {
		t.Fatal(err)
	}
	r := rs[0]
	if r.Window.D() != 5*time.Minute || r.Every.D() != time.Minute || r.Condition.Column != "value" || r.OnNoRows != "ok" || r.Identity != "default" || r.Severity != "page" {
		t.Fatalf("%+v", r)
	}
	if w := r.WindowEnding(600e9); w.FromNs != 300e9 || w.ToNs != 600e9 {
		t.Fatal(w)
	}
	if r.AlignDown(659e9) != 600e9 || r.AlignDown(-1) != -60e9 {
		t.Fatal("align")
	}
	for _, bad := range []string{
		`rules: [{name: "x y", sql: s, window: 1m, condition: {op: ">"}}]`,
		`rules: [{name: x, sql: "", window: 1m, condition: {op: ">"}}]`,
		`rules: [{name: x, sql: s, window: 1m, condition: {op: "~"}}]`,
		`rules: [{name: x, sql: s, window: 1m, every: 1500ms, condition: {op: ">"}}]`,
		`rules: [{name: x, sql: s, window: 1m, condition: {op: ">"}, labels: {alertname: y}}]`,
		`rules: [{name: x, sql: s, window: 1m, condition: {op: ">"}, on_no_rows: maybe}]`,
		`rules: [{name: x, sql: s, window: 1m, condition: {op: ">"}}, {name: x, sql: s, window: 1m, condition: {op: ">"}}]`,
		`rules: [{name: x, sql: s, window: 1m, condition: {op: ">"}, unknown_field: 1}]`,
		`rules: []`,
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestRowsAndGroupKey(t *testing.T) {
	r := &Rule{Name: "x", SQL: "s", Window: Duration(time.Minute), Condition: Condition{Op: ">"}, MaxGroups: 2}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	rows, err := r.Rows([]map[string]any{{"svc": "a", "ns": "shop", "value": "12"}, {"svc": "b", "ns": nil, "value": 3.5}})
	if err != nil || rows[0].Value != 12 || rows[1].Labels["ns"] != "" {
		t.Fatal(rows, err)
	}
	if GroupKey(map[string]string{"a": "1", "b": "2"}) != GroupKey(map[string]string{"b": "2", "a": "1"}) {
		t.Fatal("order-dependent key")
	}
	if GroupKey(map[string]string{"a": "1,b=2"}) == GroupKey(map[string]string{"a": "1", "b": "2"}) {
		t.Fatal("ambiguous key")
	}
	if _, err := r.Rows(make([]map[string]any, 3)); err == nil || !strings.Contains(err.Error(), "max_groups") {
		t.Fatal("max_groups not enforced:", err)
	}
}
