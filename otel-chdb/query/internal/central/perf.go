package central

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// SettingRule is one ClickHouse setting a caller may set per statement, and
// the values it may take. The list is configuration
// (central.performance_settings in queryd.example.json), never a code
// constant: what a caller may tune is policy.
type SettingRule struct {
	// Type: "bool" (0, 1, true, false), "uint" (a decimal in [Min, Max]) or
	// "enum" (one of Values).
	Type   string   `json:"type"`
	Min    uint64   `json:"min"`
	Max    uint64   `json:"max"`
	Values []string `json:"values"`
	// Why says what the setting changes (speed, and why not the rows); it is
	// documentation, required so that nothing enters the list unexplained.
	Why string `json:"why"`
}

// SettingsPolicy is the allow-list: setting name -> rule.
type SettingsPolicy map[string]SettingRule

var settingNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}$`)

// Forbidden classes of settings: no configuration can allow one. A setting
// in the allow-list must change how fast a statement runs, never what it
// may read or what it answers; these are the classes that do (DECISIONS.md
// D33). Matched on the lower-cased name.
var forbiddenExact = map[string]string{
	"readonly": "access", "allow_ddl": "access", "allow_introspection_functions": "access",
	"allow_suspicious_low_cardinality_types": "access", "allow_nondeterministic_mutations": "access",
	"limit": "limits", "offset": "limits", "priority": "limits", "workload": "limits",
	"timeout_before_checking_execution_speed": "limits", "use_concurrency_control": "limits",
	"wait_end_of_query": "output", "send_progress_in_http_headers": "output",
	"use_query_cache": "cache", "enable_reads_from_query_cache": "cache", "enable_writes_to_query_cache": "cache",
	"query_id": "identity", "log_comment": "identity", "log_queries": "identity", "user": "identity",
	"session_id": "identity", "default_database": "scope", "database": "scope",
	// result semantics (a different answer from the same rows)
	"final": "semantics", "do_not_merge_across_partitions_select_final": "semantics",
	"join_use_nulls": "semantics", "transform_null_in": "semantics", "aggregate_functions_null_for_empty": "semantics",
	"cast_keep_nullable": "semantics", "data_type_default_nullable": "semantics", "prefer_column_name_to_alias": "semantics",
	"enable_analyzer": "semantics", "allow_experimental_analyzer": "semantics", "select_sequential_consistency": "semantics",
	"optimize_trivial_count_query": "semantics", "empty_result_for_aggregation_by_empty_set": "semantics",
	"group_by_use_nulls": "semantics", "join_default_strictness": "semantics", "any_join_distinct_right_table_keys": "semantics",
	"optimize_skip_unused_shards": "semantics", "use_index_for_in_with_subqueries": "semantics",
	"sample": "semantics",
}

var forbiddenPrefix = []struct{ p, class string }{
	{"max_", "limits"}, {"min_", "limits"}, {"timeout_", "limits"}, {"idle_", "limits"},
	{"additional_", "scope"}, {"parallel_replica", "scope"}, {"allow_experimental_parallel_reading", "scope"},
	{"cluster_for_parallel", "scope"}, {"prefer_localhost", "scope"}, {"distributed_", "scope"},
	{"row_policy", "scope"}, {"ignore_", "scope"}, {"force_", "semantics"},
	{"output_", "output"}, {"input_", "output"}, {"format_", "output"}, {"date_time_", "output"}, {"http_", "output"},
	{"send_", "output"}, {"default_", "output"}, {"query_cache_", "cache"}, {"session_", "identity"},
	{"insert_", "access"}, {"async_insert", "access"}, {"mutations_", "access"}, {"alter_", "access"},
	{"allow_experimental_", "semantics"}, {"allow_suspicious", "access"}, {"enable_global_with", "semantics"},
	{"join_", "semantics"}, {"cross_", "semantics"}, {"union_", "semantics"}, {"except_", "semantics"},
	{"intersect_", "semantics"}, {"count_distinct", "semantics"}, {"normalize_", "semantics"},
	{"aggregate_functions_", "semantics"}, {"function_", "semantics"}, {"sleep", "limits"},
}

var forbiddenContains = []struct{ s, class string }{
	{"overflow_mode", "overflow"}, {"_limit_to_", "limits"}, {"_speed", "limits"}, {"quota", "limits"},
	{"readonly", "access"}, {"_filter", "scope"}, {"password", "identity"}, {"secret", "identity"},
	{"_timeout", "limits"}, {"memory_usage", "limits"}, {"_format", "output"}, {"nulls", "semantics"},
	{"sampl", "semantics"}, {"final", "semantics"}, {"deduplicat", "semantics"},
}

// ForbiddenSetting says whether name is of a class no caller may set, and
// which: limits (max_*, min_*, timeouts, speed), overflow (every
// *_overflow_mode: pinned to throw), access (readonly, allow_ddl, …), scope
// (additional_table_filters, parallel replicas, …), output (formats and
// the answer's framing: the service's), cache (the query result cache
// serves stale answers), identity (query_id, log_comment) and semantics
// (settings that change the answer over the same rows).
func ForbiddenSetting(name string) (string, bool) {
	n := strings.ToLower(name)
	if strings.Contains(n, "overflow_mode") {
		return "overflow", true
	}
	if c, ok := forbiddenExact[n]; ok {
		return c, true
	}
	for _, f := range forbiddenPrefix {
		if strings.HasPrefix(n, f.p) {
			return f.class, true
		}
	}
	for _, f := range forbiddenContains {
		if strings.Contains(n, f.s) {
			return f.class, true
		}
	}
	for _, m := range OverflowModes {
		if n == m {
			return "overflow", true
		}
	}
	return "", false
}

// Validate checks the configured list: every name plain and of no
// forbidden class, every rule complete.
func (p SettingsPolicy) Validate() error {
	names := make([]string, 0, len(p))
	for n := range p {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		r := p[n]
		if !settingNameRE.MatchString(n) {
			return fmt.Errorf("performance setting %q: not a setting name", n)
		}
		if class, bad := ForbiddenSetting(n); bad {
			return fmt.Errorf("performance setting %s: of class %q, which no caller may set", n, class)
		}
		if strings.TrimSpace(r.Why) == "" {
			return fmt.Errorf("performance setting %s: say why it cannot change the rows (why)", n)
		}
		switch r.Type {
		case "bool":
		case "uint":
			if r.Max == 0 || r.Min > r.Max {
				return fmt.Errorf("performance setting %s: uint needs 0 <= min <= max, max > 0", n)
			}
		case "enum":
			if len(r.Values) == 0 {
				return fmt.Errorf("performance setting %s: enum needs values", n)
			}
			for _, v := range r.Values {
				if !settingNameRE.MatchString(v) {
					return fmt.Errorf("performance setting %s: value %q is not a plain word", n, v)
				}
			}
		default:
			return fmt.Errorf("performance setting %s: type must be bool, uint or enum, not %q", n, r.Type)
		}
	}
	return nil
}

// Check returns the value to send for name=value, or why it is refused.
func (p SettingsPolicy) Check(name, value string) (string, error) {
	if class, bad := ForbiddenSetting(name); bad {
		return "", fmt.Errorf("setting %s is of class %q, which no caller may set", name, class)
	}
	r, ok := p[name]
	if !ok {
		return "", fmt.Errorf("setting %s is not in the service's performance-settings allow-list", name)
	}
	v := strings.TrimSpace(value)
	switch r.Type {
	case "bool":
		switch strings.ToLower(v) {
		case "0", "false":
			return "0", nil
		case "1", "true":
			return "1", nil
		}
		return "", fmt.Errorf("setting %s takes 0 or 1, not %q", name, value)
	case "uint":
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n < r.Min || n > r.Max {
			return "", fmt.Errorf("setting %s takes an integer in [%d, %d], not %q", name, r.Min, r.Max, value)
		}
		return strconv.FormatUint(n, 10), nil
	case "enum":
		for _, x := range r.Values {
			if v == x {
				return x, nil
			}
		}
		return "", fmt.Errorf("setting %s takes one of %s, not %q", name, strings.Join(r.Values, ", "), value)
	}
	return "", fmt.Errorf("setting %s: bad rule", name)
}

// Sample turns a statement's settings into a labelled sample's: reading
// stops at bound rows (read_overflow_mode = break, max_rows_to_read =
// bound) and every other overflow mode stays throw. Only the service calls
// it, for a request that asked for a sample; the answer is then labelled
// completeness "sample", never "complete" (DECISIONS.md D33).
func Sample(v url.Values, bound int64) {
	v.Set("max_rows_to_read", strconv.FormatInt(bound, 10))
	v.Set("read_overflow_mode", "break")
}
