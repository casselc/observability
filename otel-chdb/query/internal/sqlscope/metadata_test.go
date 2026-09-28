package sqlscope

import (
	"strings"
	"testing"
	"time"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
	"pgregory.net/rapid"
)

func metaPolicy(t testing.TB) *Policy {
	t.Helper()
	p, err := NewPolicy("otel", []*Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", ReceivedColumn: "received_at", Scope: "columns", Cluster: "c", Namespace: "n"},
		{Name: "otel_traces", TimeColumn: "Timestamp", Scope: "columns", Cluster: "c", Namespace: "n"},
		{Database: "system", Name: "tables", Scope: "metadata"},
		{Database: "system", Name: "columns", Scope: "metadata"},
		{Database: "system", Name: "settings", Scope: "metadata"},
		{Database: "system", Name: "databases", Scope: "metadata", Columns: []string{"name"}},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

const tablesProj = "(SELECT database, name, table, engine, is_temporary, create_table_query, engine_full, as_select, partition_key, sorting_key, primary_key, sampling_key, total_rows, comment FROM system.tables)"

// TestMetadataProjected: every read of a metadata table is replaced by its
// allow-listed projection, so SELECT * cannot reach data_paths or
// metadata_path, and naming them is ClickHouse's unknown identifier.
func TestMetadataProjected(t *testing.T) {
	p := metaPolicy(t)
	all := Scope{AllClusters: true, AllNamespaces: true}
	cases := map[string]string{
		"SELECT * FROM system.tables WHERE database = 'otel' LIMIT 1":  "SELECT * FROM " + tablesProj + " AS `tables` WHERE database = 'otel' LIMIT 1",
		"SELECT t.name FROM system.tables AS t":                        "SELECT t.name FROM " + tablesProj + " AS t",
		"SELECT metadata_path, data_paths FROM system.tables":          "SELECT metadata_path, data_paths FROM " + tablesProj + " AS `tables`",
		"SELECT * FROM system.databases":                               "SELECT * FROM (SELECT name FROM system.databases) AS `databases`",
		"SELECT value FROM system.settings WHERE name = 'max_threads'": "SELECT value FROM (SELECT name, value, changed FROM system.settings) AS `settings` WHERE name = 'max_threads'",
		// in a subquery, a join and an IN: every read
		"SELECT name FROM system.columns WHERE table IN (SELECT name FROM system.tables)": "SELECT name FROM (SELECT database, table, name, type, position, default_kind, default_expression, comment, compression_codec, is_in_partition_key, is_in_sorting_key, is_in_primary_key, is_in_sampling_key FROM system.columns) AS `columns` WHERE table IN (SELECT name FROM " + tablesProj + " AS `tables`)",
		// a caller who writes the projection gets it once, not nested
		"SELECT name FROM (SELECT name FROM system.databases)": "SELECT name FROM (SELECT name FROM system.databases)",
	}
	for in, want := range cases {
		r := mustFinish(t, p, in, all)
		if r.SQL != want {
			t.Errorf("%q\n got %q\nwant %q", in, r.SQL, want)
		}
		for _, bad := range []string{"data_paths FROM system", "metadata_path FROM system", "uuid"} {
			if strings.Contains(r.SQL, bad) {
				t.Errorf("%q: %s reaches system", in, bad)
			}
		}
	}
	// a join of two metadata tables
	r := mustFinish(t, p, "SELECT a.name, c.name FROM system.tables AS a INNER JOIN system.columns AS c ON a.name = c.table", all)
	if strings.Count(r.SQL, "(SELECT database") != 2 {
		t.Errorf("join: %s", r.SQL)
	}
	// data tables are untouched
	r = mustFinish(t, p, "SELECT * FROM otel_logs", all)
	if r.SQL != "SELECT * FROM otel.otel_logs" {
		t.Errorf("data table rewritten: %s", r.SQL)
	}
	// Finish is idempotent on the same Prepared
	pr, _ := p.Prepare("SELECT * FROM system.tables")
	a, _ := pr.Finish(all)
	b, err := pr.Finish(all)
	if err != nil || a.SQL != b.SQL {
		t.Errorf("second Finish: %v\n%s\n%s", err, a.SQL, b.SQL)
	}
}

// TestMetadataConfig: a metadata table needs an allow-list (default or
// configured) of plain names; columns is for metadata tables only.
func TestMetadataConfig(t *testing.T) {
	bad := [][]*Table{
		{{Database: "system", Name: "parts", Scope: "metadata"}},                                     // no default
		{{Database: "system", Name: "tables", Scope: "metadata", Columns: []string{"name", "x y"}}},  // not a name
		{{Database: "system", Name: "tables", Scope: "metadata", Columns: []string{"name", "name"}}}, // twice
		{{Name: "otel_logs", Scope: "fleet", Columns: []string{"a"}}},                                // not metadata
		{{Name: "otel_logs", Scope: "fleet", ReceivedColumn: "received_at"}},                         // no time column
		{{Database: "system", Name: "tables", Scope: "metadata", ReceivedColumn: "received_at"}},     // metadata has no clock
	}
	for _, tb := range bad {
		if _, err := NewPolicy("otel", tb, 0); err == nil {
			t.Errorf("%+v: accepted", tb[0])
		}
	}
	p, err := NewPolicy("otel", []*Table{{Database: "system", Name: "parts", Scope: "metadata", Columns: []string{"name", "active"}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	r := mustFinish(t, p, "SELECT * FROM system.parts", Scope{Clusters: []string{"a"}, AllNamespaces: true})
	if r.SQL != "SELECT * FROM (SELECT name, active FROM system.parts) AS `parts`" {
		t.Error(r.SQL)
	}
}

// TestMetadataProjectedProperty: whatever statement over the served tables
// passes, the text ClickHouse receives names a metadata table only as the
// FROM of its exact projection.
func TestMetadataProjectedProperty(t *testing.T) {
	p := metaPolicy(t)
	rapid.Check(t, func(t *rapid.T) {
		tbl := rapid.SampledFrom([]string{"system.tables", "system.columns", "system.settings", "system.databases", "otel_logs"}).Draw(t, "t")
		col := rapid.SampledFrom([]string{"*", "name", "data_paths", "count()", "x.name"}).Draw(t, "col")
		alias := rapid.SampledFrom([]string{"", " AS x", " x"}).Draw(t, "alias")
		wrap := rapid.IntRange(0, 3).Draw(t, "wrap")
		q := "SELECT " + col + " FROM " + tbl + alias
		switch wrap {
		case 1:
			q = "SELECT * FROM (" + q + ")"
		case 2:
			q = "WITH w AS (" + q + ") SELECT * FROM w"
		case 3:
			q = "SELECT 1 WHERE 'a' IN (" + q + ")"
		}
		pr, err := p.Prepare(q)
		if err != nil {
			return
		}
		r, err := pr.Finish(Scope{AllClusters: true, AllNamespaces: true})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		st, err := chp.NewParser(r.SQL).ParseStmts()
		if err != nil {
			t.Fatalf("%s: %v", r.SQL, err)
		}
		if err := p.checkProjected(st[0]); err != nil {
			t.Fatalf("%s -> %s: %v", q, r.SQL, err)
		}
		if strings.HasPrefix(tbl, "system.") && !strings.Contains(r.SQL, p.Tables[tbl].projection) {
			t.Fatalf("%s -> %s: no projection", q, r.SQL)
		}
	})
}

// TestCheckProjectedRefuses: a statement text that reads a metadata table
// directly (as a rewrite that missed a place would produce) is refused.
func TestCheckProjectedRefuses(t *testing.T) {
	p := metaPolicy(t)
	for _, q := range []string{"SELECT * FROM system.tables", "SELECT name FROM (SELECT name, uuid FROM system.tables)"} {
		st, _ := chp.NewParser(q).ParseStmts()
		if err := p.checkProjected(st[0]); err == nil {
			t.Errorf("%s: passed", q)
		}
	}
}

func TestLateCount(t *testing.T) {
	p := metaPolicy(t)
	r := mustFinish(t, p, "SELECT count() FROM otel_logs WHERE TraceId IN (SELECT TraceId FROM otel_traces) AND 1 IN (SELECT 1 FROM system.tables)",
		Scope{Clusters: []string{"prod-a"}, AllNamespaces: true, Window: &Window{FromNs: 1, ToNs: 2}})
	lc, err := p.LateCount(r, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT 'otel.otel_logs' AS t, count() AS n FROM otel.otel_logs WHERE received_at > (Timestamp + toIntervalNanosecond(90000000000))"
	if lc.SQL != want || strings.Join(lc.Counted, ",") != "otel.otel_logs" || strings.Join(lc.Uncounted, ",") != "otel.otel_traces" {
		t.Fatalf("%+v", lc)
	}
	// no table with both columns: nothing to run
	r = mustFinish(t, p, "SELECT count() FROM otel_traces", Scope{AllClusters: true, AllNamespaces: true})
	if lc, _ := p.LateCount(r, time.Minute); lc.SQL != "" || len(lc.Uncounted) != 1 {
		t.Fatalf("%+v", lc)
	}
}
