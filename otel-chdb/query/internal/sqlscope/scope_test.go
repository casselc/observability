package sqlscope

import (
	"strings"
	"testing"
)

func testPolicy(t testing.TB) *Policy {
	t.Helper()
	p, err := NewPolicy("otel", []*Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", Scope: "columns",
			Cluster:   "`__hdx_materialized_k8s.cluster.name`",
			Namespace: "`__hdx_materialized_k8s.namespace.name`"},
		{Name: "otel_traces", TimeColumn: "Timestamp", Scope: "columns",
			Cluster:   "ResourceAttributes['k8s.cluster.name']",
			Namespace: "ResourceAttributes['k8s.namespace.name']"},
		{Name: "otel_resources", Scope: "catalog"},
		{Name: "otel_metrics_series", Scope: "fleet"},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var teamA = Scope{Clusters: []string{"prod-a"}, AllNamespaces: true}

func mustFinish(t *testing.T, p *Policy, sql string, s Scope) *Result {
	t.Helper()
	pr, err := p.Prepare(sql)
	if err != nil {
		t.Fatalf("Prepare(%q): %v", sql, err)
	}
	r, err := pr.Finish(s)
	if err != nil {
		t.Fatalf("Finish(%q): %v", sql, err)
	}
	return r
}

func TestAcceptsAndQualifies(t *testing.T) {
	p := testPolicy(t)
	cases := map[string]string{
		"SELECT count() FROM otel_logs":                                  "SELECT count() FROM otel.otel_logs",
		"select ServiceName, count() from otel.otel_logs group by 1":     "SELECT ServiceName, count() FROM otel.otel_logs GROUP BY 1",
		"SELECT * FROM otel_traces AS t WHERE t.TraceId = 'ab' LIMIT 10": "SELECT * FROM otel.otel_traces AS t WHERE t.TraceId = 'ab' LIMIT 10",
		"WITH x AS (SELECT Body FROM otel_logs) SELECT count() FROM x":   "WITH x AS (SELECT Body FROM otel.otel_logs) SELECT count() FROM x",
		"SELECT 1": "SELECT 1",
		"SELECT count() FROM otel_logs WHERE TraceId IN (SELECT TraceId FROM otel_traces)": "SELECT count() FROM otel.otel_logs WHERE TraceId IN (SELECT TraceId FROM otel.otel_traces)",
		// comments are not carried: the statement is rebuilt from the tree
		"SELECT 1 -- FROM system.users": "SELECT 1",
		// a string literal keeps its bytes, escapes included
		`SELECT 'a''b\'c' FROM otel_logs`: `SELECT 'a''b\'c' FROM otel.otel_logs`,
	}
	for in, want := range cases {
		r := mustFinish(t, p, in, teamA)
		if r.SQL != want {
			t.Errorf("%q\n got %q\nwant %q", in, r.SQL, want)
		}
	}
}

func TestRejects(t *testing.T) {
	p := testPolicy(t)
	cases := map[string]string{
		"SELECT * FROM system.users":                           "table_not_allowed",
		"SELECT * FROM otel.secrets":                           "table_not_allowed",
		"SELECT * FROM other.otel_logs":                        "table_not_allowed",
		"SELECT * FROM s3('http://x/y')":                       "table_function",
		"SELECT * FROM merge('otel', '.*')":                    "table_function",
		"SELECT * FROM url('http://x', CSV)":                   "table_function",
		"SELECT * FROM cluster('c', otel.otel_logs)":           "table_function",
		"SELECT * FROM remote('h', otel.otel_logs)":            "table_function",
		"SELECT count() FROM otel_logs SETTINGS max_threads=1": "settings_clause",
		"SELECT count() FROM (SELECT * FROM otel_logs SETTINGS additional_table_filters={'x':'1'})": "settings_clause",
		"SELECT count() FROM otel_logs FORMAT JSON":                                                 "format_clause",
		"INSERT INTO otel.otel_logs (Body) VALUES ('x')":                                            "not_select",
		"DROP TABLE otel.otel_logs":                                                                 "not_select",
		"ALTER TABLE otel.otel_logs DELETE WHERE 1":                                                 "not_select",
		"SET readonly = 0":                                   "not_select",
		"SELECT 1; SELECT 2":                                 "statement_count",
		"SELECT dictGet('entities.d_res', 'attrs', 1)":       "denied_function",
		"SELECT joinGet('db.j', 'v', 1)":                     "denied_function",
		"SELECT getSetting('readonly')":                      "denied_function",
		"SELECT sleep(3)":                                    "denied_function",
		"SELECT file('/etc/passwd')":                         "denied_function",
		"SELECT 1 FROM otel_logs WHERE Body IN secrets":      "in_table",
		"SELECT 1 FROM otel_logs WHERE Body IN otel.secrets": "in_table",
		"SELECT {p:String} FROM otel_logs":                   "query_param",
		"SELECT `a\\` FROM otel_logs":                        "bad_identifier",
		"SELECT FROM WHERE":                                  "parse_error",
		// a CTE of one branch is not visible in the next (fail closed)
		"WITH otel_logs2 AS (SELECT 1) SELECT * FROM otel_logs2 UNION ALL SELECT * FROM otel_logs2": "table_not_allowed",
		// a CTE of a subquery is not visible outside it
		"SELECT * FROM (WITH users AS (SELECT 1) SELECT * FROM users), users": "table_not_allowed",
	}
	for in, reason := range cases {
		_, err := p.Prepare(in)
		r, ok := AsRejection(err)
		if !ok {
			t.Errorf("%q: want rejection %s, got %v", in, reason, err)
			continue
		}
		if r.Reason != reason {
			t.Errorf("%q: want %s, got %s (%s)", in, reason, r.Reason, r.Detail)
		}
	}
}

func TestTooLong(t *testing.T) {
	p := testPolicy(t)
	p.MaxSQLBytes = 16
	_, err := p.Prepare("SELECT count() FROM otel_logs")
	if r, ok := AsRejection(err); !ok || r.Reason != "too_long" {
		t.Fatalf("want too_long, got %v", err)
	}
}

func TestScopeFilters(t *testing.T) {
	p := testPolicy(t)
	r := mustFinish(t, p, "SELECT count() FROM otel_logs l JOIN otel_traces t ON l.TraceId = t.TraceId",
		Scope{Clusters: []string{"prod-b", "prod-a", "prod-a"}, Namespaces: []string{"shop"}})
	want := map[string]string{
		"otel.otel_logs":   "`__hdx_materialized_k8s.cluster.name` IN ('prod-a', 'prod-b') AND `__hdx_materialized_k8s.namespace.name` IN ('shop')",
		"otel.otel_traces": "ResourceAttributes['k8s.cluster.name'] IN ('prod-a', 'prod-b') AND ResourceAttributes['k8s.namespace.name'] IN ('shop')",
	}
	for k, v := range want {
		if r.Filters[k] != v {
			t.Errorf("%s: got %q want %q", k, r.Filters[k], v)
		}
	}
	if got := r.FiltersSetting(); !strings.HasPrefix(got, `{'otel.otel_logs':'`) || !strings.Contains(got, `ResourceAttributes[\'k8s.cluster.name\']`) {
		t.Errorf("setting: %s", got)
	}
}

func TestFleetScopeHasNoFilter(t *testing.T) {
	p := testPolicy(t)
	r := mustFinish(t, p, "SELECT count() FROM otel_logs", Scope{AllClusters: true, AllNamespaces: true})
	if len(r.Filters) != 0 {
		t.Fatalf("fleet scope got filters %v", r.Filters)
	}
	r = mustFinish(t, p, "SELECT count() FROM otel_metrics_series", Scope{AllClusters: true, AllNamespaces: true})
	if len(r.Filters) != 0 || len(r.Tables) != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestUnenforceableScopeIsRefused(t *testing.T) {
	p := testPolicy(t)
	pr, err := p.Prepare("SELECT count() FROM otel_metrics_series")
	if err != nil {
		t.Fatal(err)
	}
	_, err = pr.Finish(teamA)
	if r, ok := AsRejection(err); !ok || r.Reason != "scope_unenforceable" {
		t.Fatalf("want scope_unenforceable, got %v", err)
	}
}

func TestCatalogScope(t *testing.T) {
	p := testPolicy(t)
	pr, err := p.Prepare("SELECT count() FROM otel_resources")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pr.Finish(teamA); err == nil {
		t.Fatal("unresolved catalog must refuse a restricted caller")
	}
	s := teamA
	s.ResourceIDs = []uint64{18446744073709551615, 7, 7}
	r, err := pr.Finish(s)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Filters["otel.otel_resources"]; got != "resource_id IN (7, 18446744073709551615)" {
		t.Fatalf("got %q", got)
	}
	s.ResourceIDs = []uint64{}
	r, err = pr.Finish(s)
	if err != nil || r.Filters["otel.otel_resources"] != "0" {
		t.Fatalf("empty catalog answer must read no row: %v %v", r, err)
	}
}

func TestBadScopeValuesAreRefused(t *testing.T) {
	p := testPolicy(t)
	for _, bad := range []string{"a') OR 1=1 --", "A", "", "-x", "x\\", "a b"} {
		pr, _ := p.Prepare("SELECT count() FROM otel_logs")
		_, err := pr.Finish(Scope{Clusters: []string{bad}, AllNamespaces: true})
		if err == nil {
			t.Errorf("cluster %q accepted", bad)
		}
	}
	pr, _ := p.Prepare("SELECT count() FROM otel_logs")
	if _, err := pr.Finish(Scope{Clusters: nil, AllNamespaces: true}); err == nil {
		t.Error("an empty cluster list must refuse, not read everything")
	}
}

func TestWindow(t *testing.T) {
	p := testPolicy(t)
	r := mustFinish(t, p, "SELECT count() FROM otel_logs", Scope{AllClusters: true, AllNamespaces: true,
		Window: &Window{FromNs: 1_000, ToNs: 2_000}})
	want := "Timestamp >= fromUnixTimestamp64Nano(toInt64(1000), 'UTC') AND Timestamp < fromUnixTimestamp64Nano(toInt64(2000), 'UTC')"
	if r.Filters["otel.otel_logs"] != want {
		t.Fatalf("got %q", r.Filters["otel.otel_logs"])
	}
	pr, _ := p.Prepare("SELECT 1 FROM otel_logs")
	if _, err := pr.Finish(Scope{AllClusters: true, AllNamespaces: true, Window: &Window{FromNs: 5, ToNs: 5}}); err == nil {
		t.Fatal("empty window accepted")
	}
}

func TestHashIsStableAndScopeSensitive(t *testing.T) {
	p := testPolicy(t)
	a := mustFinish(t, p, "SELECT count() FROM otel_logs", teamA)
	b := mustFinish(t, p, "select count()   from otel.otel_logs", teamA)
	c := mustFinish(t, p, "SELECT count() FROM otel_logs", Scope{Clusters: []string{"prod-b"}, AllNamespaces: true})
	if a.Hash != b.Hash {
		t.Error("the same statement and scope must hash alike")
	}
	if a.Hash == c.Hash {
		t.Error("another scope must hash differently")
	}
}

func BenchmarkPrepareFinish(b *testing.B) {
	p := testPolicy(b)
	sql := "SELECT toStartOfMinute(Timestamp) AS t, ServiceName, count() FROM otel_logs WHERE SeverityText = 'ERROR' AND Body ILIKE '%timeout%' GROUP BY t, ServiceName ORDER BY t LIMIT 100"
	for i := 0; i < b.N; i++ {
		pr, err := p.Prepare(sql)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := pr.Finish(teamA); err != nil {
			b.Fatal(err)
		}
	}
}
