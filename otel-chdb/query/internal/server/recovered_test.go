package server

import (
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
)

// D35 (3): the recovered tables are read only when a request asks for
// them, alone (a statement may then name only recovered tables, and
// without the flag none), labelled source "recovered" and completeness
// "unknown", never with a basis or a sample, and refused where the service
// serves none.
func TestRecoveredTablesOnlyWhenAskedAndLabelled(t *testing.T) {
	f := newFixture(t)
	tok := f.token(teamA)
	window := map[string]any{"from": t0.Add(-time.Hour).Format(time.RFC3339), "to": t0.Format(time.RFC3339)}
	// not configured: refused
	code, out := f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT count() FROM otel_logs_recovered", "recovered": true})
	if code != 400 || out["error"] != "recovered_unavailable" {
		t.Fatalf("%d %v", code, out)
	}
	var tables []*sqlscope.Table
	for _, t := range f.srv.Policy.Tables {
		tables = append(tables, t)
	}
	rec, err := sqlscope.NewPolicy("otel", sqlscope.RecoveredTables(tables), 0)
	if err != nil {
		t.Fatal(err)
	}
	f.srv.Recovered = rec
	// without the flag a recovered table cannot be named
	code, out = f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT count() FROM otel_logs_recovered"})
	if code != 403 || out["error"] != "table_not_allowed" {
		t.Fatalf("a recovered table without the flag: %d %v", code, out)
	}
	// with it, a main table cannot be named: never mixed
	code, out = f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT count() FROM otel_logs", "recovered": true})
	if code != 403 || out["error"] != "table_not_allowed" {
		t.Fatalf("a main table with the flag: %d %v", code, out)
	}
	code, out = f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT count() FROM otel_logs_recovered AS r JOIN otel_logs AS m ON r.Body = m.Body", "recovered": true})
	if code != 403 || out["error"] != "table_not_allowed" {
		t.Fatalf("mixed: %d %v", code, out)
	}
	for _, extra := range []map[string]any{{"basis": "latest"}, {"sample": map[string]any{"rows": 10}}} {
		body := map[string]any{"sql": "SELECT count() FROM otel_logs_recovered", "recovered": true}
		for k, v := range extra {
			body[k] = v
		}
		if code, out = f.post(t, "/v1/query", tok, body); code != 400 || out["error"] != "recovered_alone" {
			t.Fatalf("%v: %d %v", extra, code, out)
		}
	}
	n := len(f.ch.sqls)
	code, out = f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT count() FROM otel_logs_recovered", "recovered": true, "window": window})
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	if out["source"] != "recovered" || out["completeness"] != "unknown" || out["partial"] != true || out["complete_through"] != nil {
		t.Fatalf("label: %v", out)
	}
	if wm := out["watermark"].(map[string]any); wm["status"] != "not_applicable" || !strings.Contains(wm["note"].(string), "consume admit") {
		t.Fatalf("watermark: %v", wm)
	}
	if out["late"].(map[string]any)["status"] != "recovered" {
		t.Fatalf("late: %v", out["late"])
	}
	// scoped like the main table: the caller's clusters and the window
	if f.ch.sqls[n] != "SELECT count() FROM otel.otel_logs_recovered" {
		t.Fatalf("sql %q", f.ch.sqls[n])
	}
	if flt := f.ch.calls[n].Get("additional_table_filters"); !strings.Contains(flt, "otel_logs_recovered") || !strings.Contains(flt, `IN (\'prod-a\')`) || strings.Contains(flt, "prod-b") {
		t.Fatalf("filter %q", flt)
	}
	recs := f.sink.Snapshot()
	if last := recs[len(recs)-2]; last.Decision != "allow" || last.Detail != "recovered" {
		t.Fatalf("the audit says recovered: %+v", last)
	}
}
