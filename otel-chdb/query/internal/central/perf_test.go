package central

import (
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"testing"
)

// shipped is the allow-list queryd.example.json ships.
func shipped(t *testing.T) SettingsPolicy {
	t.Helper()
	b, err := os.ReadFile("../../queryd.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Central struct {
			PerformanceSettings SettingsPolicy `json:"performance_settings"`
		} `json:"central"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return c.Central.PerformanceSettings
}

// forbiddenByClass: settings of every class no allow-list may hold, with
// the class each must be refused as.
var forbiddenByClass = map[string][]string{
	"limits": {"max_threads", "max_rows_to_read", "max_execution_time", "max_memory_usage", "max_result_rows",
		"max_bytes_to_read", "min_execution_speed", "timeout_before_checking_execution_speed", "priority", "workload",
		"max_rows_to_group_by", "max_concurrent_queries_for_user", "limit", "offset", "Max_Threads"},
	"overflow": {"read_overflow_mode", "result_overflow_mode", "group_by_overflow_mode", "timeout_overflow_mode",
		"read_overflow_mode_leaf", "set_overflow_mode", "sort_overflow_mode", "join_overflow_mode", "distinct_overflow_mode",
		"transfer_overflow_mode", "timeout_overflow_mode_leaf"},
	"access": {"readonly", "allow_ddl", "allow_introspection_functions"},
	"scope": {"additional_table_filters", "additional_result_filter", "parallel_replicas_custom_key",
		"allow_experimental_parallel_reading_from_replicas", "cluster_for_parallel_replicas", "default_database"},
	"output": {"date_time_output_format", "output_format_json_quote_64bit_integers", "default_format", "wait_end_of_query",
		"http_write_exception_in_output_format", "send_progress_in_http_headers", "format_csv_delimiter"},
	"cache":    {"use_query_cache", "query_cache_ttl", "enable_reads_from_query_cache"},
	"identity": {"query_id", "log_comment", "session_id"},
	"semantics": {"final", "join_use_nulls", "transform_null_in", "aggregate_functions_null_for_empty", "enable_analyzer",
		"allow_experimental_analyzer", "optimize_trivial_count_query", "do_not_merge_across_partitions_select_final",
		"force_index_by_date", "cast_keep_nullable", "prefer_column_name_to_alias", "join_algorithm"},
}

// TestForbiddenClassesRefused: no configuration may allow a setting of a
// forbidden class, and a request naming one is refused even if a
// configuration somehow held it.
func TestForbiddenClassesRefused(t *testing.T) {
	for class, names := range forbiddenByClass {
		for _, n := range names {
			got, bad := ForbiddenSetting(n)
			if !bad {
				t.Errorf("%s (%s) is not forbidden", n, class)
				continue
			}
			if got != class && !(class == "semantics" && got != "") {
				t.Errorf("%s: class %q, want %q", n, got, class)
			}
			p := SettingsPolicy{n: {Type: "bool", Why: "x"}}
			if err := p.Validate(); err == nil {
				t.Errorf("an allow-list holding %s validated", n)
			}
			if _, err := p.Check(n, "1"); err == nil {
				t.Errorf("a request setting %s passed", n)
			}
		}
	}
}

// TestShippedAllowList: the example's list validates, and each of its
// settings takes its values and refuses others.
func TestShippedAllowList(t *testing.T) {
	p := shipped(t)
	if len(p) == 0 {
		t.Fatal("queryd.example.json ships no performance settings")
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	for n, r := range p {
		switch r.Type {
		case "bool":
			for in, want := range map[string]string{"1": "1", "0": "0", "true": "1", "false": "0"} {
				if got, err := p.Check(n, in); err != nil || got != want {
					t.Errorf("%s=%s: %q %v", n, in, got, err)
				}
			}
			for _, bad := range []string{"2", "yes", "", "1; SELECT 1", "1'"} {
				if _, err := p.Check(n, bad); err == nil {
					t.Errorf("%s=%q accepted", n, bad)
				}
			}
		case "uint":
			if _, err := p.Check(n, "100000"); err != nil && r.Max >= 100000 {
				t.Errorf("%s=100000: %v", n, err)
			}
			for _, bad := range []string{"-1", "1e3", "0x10", "99999999999999999999", strconv.FormatUint(r.Max+1, 10)} {
				if _, err := p.Check(n, bad); err == nil {
					t.Errorf("%s=%q accepted", n, bad)
				}
			}
		}
	}
	if _, err := p.Check("not_a_listed_setting", "1"); err == nil {
		t.Error("an unlisted setting passed")
	}
	// rules must be complete
	for _, bad := range []SettingsPolicy{
		{"use_skip_indexes": {Type: "bool"}},           // no why
		{"use_skip_indexes": {Type: "int", Why: "x"}},  // no such type
		{"query_plan_x": {Type: "uint", Why: "x"}},     // no max
		{"use_skip_indexes": {Type: "enum", Why: "x"}}, // no values
		{"x; DROP": {Type: "bool", Why: "x"}},          // not a name
		{"use_skip_indexes": {Type: "enum", Why: "x", Values: []string{"a'"}}},
	} {
		if err := bad.Validate(); err == nil {
			t.Errorf("%v validated", bad)
		}
	}
}

func TestSampleSettings(t *testing.T) {
	v := Settings(Limits{MaxRowsToRead: 1e9}, "", "q", "c")
	Sample(v, 3000)
	if v.Get("max_rows_to_read") != "3000" || v.Get("read_overflow_mode") != "break" {
		t.Fatal(v)
	}
	// every other overflow mode stays throw
	for _, m := range OverflowModes {
		if m != "read_overflow_mode" && v.Get(m) != "throw" {
			t.Errorf("%s = %q", m, v.Get(m))
		}
	}
	_ = url.Values{}
}
