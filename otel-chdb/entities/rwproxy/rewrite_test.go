package main

import (
	"strings"
	"testing"
)

func testConfig(mode string) *Config {
	c := &Config{
		Mode:   mode,
		Tables: []string{"rw_c.otel_logs", "rw_c.otel_traces"},
		KV:     "cat.kv",
		Values: map[string]string{
			"k8s.namespace.name": "NS({rid})",
			"k8s.pod.name":       "POD({rid})",
		},
		Covered:      []string{"k8s.namespace.name", "k8s.pod.name", "k8s.node.name"},
		Materialized: map[string]string{"__hdx_materialized_k8s.pod.name": "k8s.pod.name"},
	}
	if err := c.init(); err != nil {
		panic(err)
	}
	return c
}

var hdxParams = map[string]string{"db": "rw_c", "t": "otel_logs", "lo": "1", "hi": "2", "s": "web-1", "n": "10"}

func rw(t *testing.T, c *Config, q string) Result {
	t.Helper()
	return c.Rewrite(q, hdxParams, "")
}

func TestFilterExact(t *testing.T) {
	q := "SELECT count() FROM {db:Identifier}.{t:Identifier} WHERE (Timestamp >= fromUnixTimestamp64Milli({lo:Int64})) AND " +
		"((`ResourceAttributes`['k8s.pod.name'] = 'p-1' AND indexHint(mapContains(`ResourceAttributes`, 'k8s.pod.name'))) AND (SeverityText ILIKE '%ERROR%'))\nFORMAT JSON"
	r := rw(t, testConfig("exact"), q)
	if !r.Rewritten {
		t.Fatalf("not rewritten: %+v", r)
	}
	want := "SELECT count() FROM {db:Identifier}.{t:Identifier} WHERE (Timestamp >= fromUnixTimestamp64Milli({lo:Int64})) AND " +
		"(((if(mapContains(ResourceResidual, 'k8s.pod.name'), ResourceResidual['k8s.pod.name'] = 'p-1', " +
		"resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND Value = 'p-1'))) AND 1) AND (SeverityText ILIKE '%ERROR%'))\nFORMAT JSON"
	if r.SQL != want {
		t.Fatalf("got\n%s\nwant\n%s", r.SQL, want)
	}
}

func TestFilterCatalogMode(t *testing.T) {
	c := testConfig("catalog")
	r := rw(t, c, "SELECT 1 FROM rw_c.otel_logs WHERE ResourceAttributes['k8s.namespace.name'] IN ('a', 'b') AND ResourceAttributes['telemetry.sdk.language'] ILIKE '%java%'")
	want := "SELECT 1 FROM rw_c.otel_logs WHERE (resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.namespace.name' AND Value IN ('a', 'b'))) " +
		"AND (ResourceResidual['telemetry.sdk.language'] ILIKE '%java%')"
	if r.SQL != want {
		t.Fatalf("got\n%s\nwant\n%s", r.SQL, want)
	}
}

// ” matching the operator: rows whose resource is unknown (or lacks the
// key) read ”, so the catalog side is a NOT IN over the complement.
func TestEmptyMatches(t *testing.T) {
	c := testConfig("catalog")
	for q, want := range map[string]string{
		"ResourceAttributes['k8s.pod.name'] != 'x'":         "resource_id NOT IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND NOT (Value != 'x'))",
		"ResourceAttributes['k8s.pod.name'] = ''":           "resource_id NOT IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND NOT (Value = ''))",
		"ResourceAttributes['k8s.pod.name'] NOT ILIKE 'a%'": "resource_id NOT IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND NOT (Value NOT ILIKE 'a%'))",
		"ResourceAttributes['k8s.pod.name'] LIKE '%'":       "resource_id NOT IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND NOT (Value LIKE '%'))",
		"ResourceAttributes['k8s.pod.name'] IN ('', 'a')":   "resource_id NOT IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND NOT (Value IN ('', 'a')))",
		"ResourceAttributes['k8s.pod.name'] NOT IN ('a')":   "resource_id NOT IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND NOT (Value NOT IN ('a')))",
		"ResourceAttributes['k8s.pod.name'] LIKE 'a%'":      "resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND Value LIKE 'a%')",
	} {
		r := rw(t, c, "SELECT 1 FROM rw_c.otel_logs WHERE "+q)
		if got := strings.TrimPrefix(r.SQL, "SELECT 1 FROM rw_c.otel_logs WHERE "); got != "("+want+")" {
			t.Errorf("%s\n got %s\nwant (%s)", q, got, want)
		}
	}
}

func TestValuesAndNames(t *testing.T) {
	c := testConfig("exact")
	r := rw(t, c, "SELECT ResourceAttributes['k8s.namespace.name'] AS g, `ResourceAttributes`['k8s.pod.name'], ResourceAttributes['k8s.node.name'] AS n, "+
		"ResourceAttributes['x.residual'] AS x, count() FROM rw_c.otel_traces GROUP BY g, ResourceAttributes['k8s.pod.name'], n, x")
	want := "SELECT if(mapContains(ResourceResidual, 'k8s.namespace.name'), ResourceResidual['k8s.namespace.name'], NS(resource_id)) AS g, " +
		"if(mapContains(ResourceResidual, 'k8s.pod.name'), ResourceResidual['k8s.pod.name'], POD(resource_id)) AS `arrayElement(ResourceAttributes, 'k8s.pod.name')`, " +
		"ResourceAttributes['k8s.node.name'] AS n, ResourceResidual['x.residual'] AS x, count() FROM rw_c.otel_traces GROUP BY g, " +
		"if(mapContains(ResourceResidual, 'k8s.pod.name'), ResourceResidual['k8s.pod.name'], POD(resource_id)), n, x"
	if r.SQL != want {
		t.Fatalf("got\n%s\nwant\n%s", r.SQL, want)
	}
	if r.Left != 1 { // k8s.node.name: covered, no value expression
		t.Fatalf("left %d", r.Left)
	}
}

func TestScopes(t *testing.T) {
	c := testConfig("catalog")
	// the CTE reads a target table, the outer SELECT reads the CTE
	q := "WITH sampledData AS (SELECT ResourceAttributes['k8s.pod.name'] as param0 FROM {db:Identifier}.{t:Identifier} LIMIT {n:Int32}) " +
		"SELECT groupUniqArray(20)(param0) AS param0 FROM sampledData"
	r := rw(t, c, q)
	if !r.Rewritten || !strings.Contains(r.SQL, "POD(resource_id)) as param0 FROM {db:Identifier}") {
		t.Fatalf("%+v", r)
	}
	// not a target table: untouched
	if r := rw(t, c, "SELECT ResourceAttributes['k8s.pod.name'] FROM other.otel_logs"); r.Rewritten || r.Reason != "no-target" {
		t.Fatalf("%+v", r)
	}
	// default database from the request
	if r := c.Rewrite("SELECT 1 FROM otel_logs WHERE ResourceAttributes['k8s.pod.name'] = 'a'", nil, "rw_c"); !r.Rewritten {
		t.Fatalf("%+v", r)
	}
	// a join is not a single-table scope
	if r := rw(t, c, "SELECT 1 FROM rw_c.otel_logs AS l JOIN rw_c.otel_traces AS t ON l.TraceId = t.TraceId WHERE ResourceAttributes['k8s.pod.name'] = 'a'"); r.Rewritten {
		t.Fatalf("%+v", r)
	}
	// a subquery in a filter is its own scope
	q = "SELECT count() FROM rw_c.otel_logs WHERE TraceId IN (SELECT TraceId FROM rw_c.otel_traces WHERE ResourceAttributes['k8s.pod.name'] = 'a')"
	if r := rw(t, c, q); !r.Rewritten || !strings.Contains(r.SQL, "WHERE (resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND Value = 'a')))") {
		t.Fatalf("%+v", r)
	}
	// an alias shadowing the column: leave the scope alone
	if r := rw(t, c, "SELECT map('a','b') AS ResourceAttributes FROM rw_c.otel_logs WHERE ResourceAttributes['a'] = 'b'"); r.Rewritten {
		t.Fatalf("%+v", r)
	}
}

func TestParamsAndForms(t *testing.T) {
	c := testConfig("catalog")
	r := rw(t, c, "SELECT 1 FROM rw_c.otel_logs WHERE ResourceAttributes['k8s.pod.name'] = {s:String} AND 'b' = ResourceAttributes['k8s.namespace.name'] "+
		"AND NOT mapContains(ResourceAttributes, 'k8s.pod.name') AND __hdx_materialized_k8s.pod.name = 'z'")
	want := "SELECT 1 FROM rw_c.otel_logs WHERE (resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND Value = {s:String})) " +
		"AND (resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.namespace.name' AND Value = 'b')) " +
		"AND NOT (resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name')) " +
		"AND (resource_id IN (SELECT resource_id FROM cat.kv WHERE Key = 'k8s.pod.name' AND Value = 'z'))"
	if r.SQL != want {
		t.Fatalf("got\n%s\nwant\n%s", r.SQL, want)
	}
	// a non-constant right-hand side: the access becomes a value, the comparison stays
	r = rw(t, c, "SELECT 1 FROM rw_c.otel_logs WHERE ResourceAttributes['k8s.pod.name'] = ServiceName")
	if r.SQL != "SELECT 1 FROM rw_c.otel_logs WHERE if(mapContains(ResourceResidual, 'k8s.pod.name'), ResourceResidual['k8s.pod.name'], POD(resource_id)) = ServiceName" {
		t.Fatalf("%s", r.SQL)
	}
	// escapes survive
	r = rw(t, c, `SELECT 1 FROM rw_c.otel_logs WHERE ResourceAttributes['k8s.pod.name'] = 'it\'s'`)
	if !strings.Contains(r.SQL, `Value = 'it\'s'`) {
		t.Fatalf("%s", r.SQL)
	}
}

func TestPassthrough(t *testing.T) {
	c := testConfig("exact")
	for q, reason := range map[string]string{
		"SELECT * FROM system.tables WHERE database = {db:String} FORMAT JSON":                                "no-target",
		"DESCRIBE {db:Identifier}.{t:Identifier} FORMAT JSON":                                                 "no-target",
		"SELECT *, ResourceAttributes AS __hdx_resource_attributes FROM rw_c.otel_logs LIMIT 1":               "unhandled",
		"SELECT Body FROM rw_c.otel_logs WHERE hasAllTokens(lower(Body), lower('card declined')) FORMAT JSON": "no-ref",
		"SELEC nonsense": "parse",
		"INSERT INTO rw_c.otel_logs (Body) VALUES ('a')":                                                             "no-target",
		"EXPLAIN ESTIMATE SELECT Body FROM rw_c.otel_logs WHERE ResourceAttributes['k8s.pod.name'] = 'a'":            "ok",
		"SELECT count() FROM rw_c.otel_logs WHERE ResourceAttributes['k8s.pod.name'] = 'a' SETTINGS max_threads = 1": "ok",
	} {
		if r := rw(t, c, q); r.Reason != reason {
			t.Errorf("%s: %+v, want %s", q, r, reason)
		}
	}
}
