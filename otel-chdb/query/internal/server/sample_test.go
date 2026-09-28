package server

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/golang-jwt/jwt/v5"
)

func shippedPerf(t *testing.T) central.SettingsPolicy {
	t.Helper()
	b, err := os.ReadFile("../../queryd.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Central struct {
			PerformanceSettings central.SettingsPolicy `json:"performance_settings"`
		} `json:"central"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return c.Central.PerformanceSettings
}

// TestPerformanceSettings: an allow-listed setting reaches ClickHouse; a
// setting of every forbidden class, an unlisted one, or a bad value is 400
// bad_setting, audited, and nothing runs.
func TestPerformanceSettings(t *testing.T) {
	f := newFixture(t)
	f.srv.Performance = shippedPerf(t)
	tok := f.token(teamA)
	code, out := f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT count() FROM otel_logs",
		"settings": map[string]string{"use_skip_indexes_on_data_read": "true", "query_plan_max_limit_for_lazy_materialization": "100000"}})
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	s := f.ch.calls[0]
	if s.Get("use_skip_indexes_on_data_read") != "1" || s.Get("query_plan_max_limit_for_lazy_materialization") != "100000" {
		t.Fatalf("settings not applied: %v", s)
	}
	for _, m := range central.OverflowModes {
		if s.Get(m) != "throw" {
			t.Fatalf("%s = %q", m, s.Get(m))
		}
	}
	if got := out["settings"].(map[string]any); got["use_skip_indexes_on_data_read"] != "1" {
		t.Fatalf("answer's settings %v", got)
	}
	n := len(f.ch.calls)
	for _, bad := range []map[string]string{
		{"max_threads": "64"}, {"max_rows_to_read": "0"}, {"max_execution_time": "3600"}, // limits
		{"read_overflow_mode": "break"}, {"result_overflow_mode": "break"}, // overflow
		{"readonly": "0"}, {"allow_ddl": "1"}, // access
		{"additional_table_filters": "{'otel.otel_logs':'1'}"}, {"parallel_replicas_custom_key": "x"}, // scope
		{"date_time_output_format": "iso"}, {"output_format_json_quote_64bit_integers": "0"}, // output
		{"use_query_cache": "1"},                // cache
		{"join_use_nulls": "1"}, {"final": "1"}, // semantics
		{"log_comment": "x"},                                                                 // identity
		{"not_a_setting": "1"},                                                               // unlisted
		{"use_skip_indexes": "2"}, {"query_plan_max_limit_for_top_k_optimization": "100001"}, // values
	} {
		code, out := f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT 1", "settings": bad})
		if code != 400 || out["error"] != "bad_setting" {
			t.Errorf("%v: %d %v", bad, code, out)
		}
	}
	if len(f.ch.calls) != n {
		t.Fatalf("a refused setting ran: %d calls", len(f.ch.calls)-n)
	}
	recs := f.sink.Snapshot()
	if last := recs[len(recs)-1]; last.Decision != "deny" || last.Reason != "bad_setting" {
		t.Fatalf("refusal not audited: %+v", last)
	}
	// without an allow-list, every setting is refused
	f.srv.Performance = nil
	if code, out := f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT 1", "settings": map[string]string{"use_skip_indexes": "1"}}); code != 400 {
		t.Fatalf("%d %v", code, out)
	}
}

// TestLabelledSample: a sample stops at its bound (read_overflow_mode break,
// every other mode throw), is labelled completeness "sample" with what it
// read, never "complete", and counts no late rows.
func TestLabelledSample(t *testing.T) {
	f := newFixture(t)
	f.srv.SampleDefaultRows, f.srv.SampleMaxRows = 3_000_000, 10_000_000
	f.srv.Limits.Default.MaxRowsToRead = 5_000_000
	tok := f.token(teamA)
	// a window that is complete by the watermark: the sample is still "sample"
	window := map[string]any{"from": t0.Add(-2 * time.Hour).Format(time.RFC3339), "to": t0.Add(-time.Hour).Format(time.RFC3339)}
	code, out := f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT Body FROM otel_logs LIMIT 10", "window": window, "sample": map[string]any{}})
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	s := f.ch.calls[0]
	if s.Get("read_overflow_mode") != "break" || s.Get("max_rows_to_read") != "3000000" {
		t.Fatalf("sample settings %v", s)
	}
	for _, m := range central.OverflowModes {
		if m != "read_overflow_mode" && s.Get(m) != "throw" {
			t.Fatalf("%s = %q in a sample", m, s.Get(m))
		}
	}
	if out["completeness"] != "sample" || out["partial"] != true {
		t.Fatalf("label %v %v", out["completeness"], out["partial"])
	}
	smp := out["sample"].(map[string]any)
	if smp["sample"] != true || smp["max_rows_to_read"] != 3e6 || smp["rows_read"] != 10.0 || smp["reached_bound"] != false || smp["data_completeness"] != "complete" {
		t.Fatalf("sample %v", smp)
	}
	if late := out["late"].(map[string]any); late["status"] != "sample" {
		t.Fatalf("late %v", late)
	}
	if len(f.ch.calls) != 1 {
		t.Fatalf("a sample ran %d statements (no late count)", len(f.ch.calls))
	}
	// the bound: the request's, at most sample.max_rows and the caller's max_rows_to_read
	for rows, want := range map[int64]string{1000: "1000", 20_000_000: "5000000"} {
		f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT 1", "sample": map[string]any{"rows": rows}})
		if got := f.ch.calls[len(f.ch.calls)-1].Get("max_rows_to_read"); got != want {
			t.Errorf("rows %d: max_rows_to_read %s, want %s", rows, got, want)
		}
	}
	f.post(t, "/v1/query", f.token(jwt.MapClaims{"groups": "sre"}), map[string]any{"sql": "SELECT 1", "sample": map[string]any{"rows": 20_000_000}})
	if got := f.ch.calls[len(f.ch.calls)-1].Get("max_rows_to_read"); got != "5000000" {
		t.Errorf("sre: %s", got)
	}
	// a statement without sample keeps read_overflow_mode throw
	f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT 1"})
	if got := f.ch.calls[len(f.ch.calls)-1].Get("read_overflow_mode"); got != "throw" {
		t.Fatalf("plain statement: read_overflow_mode %s", got)
	}
	// reaching the bound says so
	f.srv.Limits.Default.MaxRowsToRead = 10
	_, out = f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT 1", "sample": map[string]any{"rows": 5}})
	if smp := out["sample"].(map[string]any); smp["reached_bound"] != true {
		t.Fatalf("reached %v", smp)
	}
	// refused: disabled, negative
	f.srv.SampleMaxRows = 0
	if code, out := f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT 1", "sample": map[string]any{}}); code != 400 || out["error"] != "sample_disabled" {
		t.Fatalf("%d %v", code, out)
	}
	f.srv.SampleMaxRows = 10
	if code, out := f.post(t, "/v1/query", tok, map[string]any{"sql": "SELECT 1", "sample": map[string]any{"rows": -1}}); code != 400 || out["error"] != "bad_sample" {
		t.Fatalf("%d %v", code, out)
	}
	if !strings.Contains(metricsText(t, f), `qs_samples_total{reached_bound="true"} 1`) {
		t.Error("qs_samples_total")
	}
}

func metricsText(t *testing.T, f *fixture) string {
	t.Helper()
	var b strings.Builder
	f.srv.Metrics.Write(&b)
	return b.String()
}
