package hdxadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/query/internal/rollupmig"
)

// legacyRollup undoes the D33 change on a statement of
// otap-rs/sql/otel_{logs,traces}.sql: ClickStack's rollup, without the
// cluster column (the DDL before 2026-09-28).
func legacyRollup(s string) string {
	s = strings.Replace(s, "    `cluster` LowCardinality(String),\n", "", 1)
	s = strings.Replace(s, "ORDER BY (ColumnIdentifier, Key, Timestamp, Value, cluster)\nPRIMARY KEY (ColumnIdentifier, Key, Timestamp, Value)\n",
		"ORDER BY (ColumnIdentifier, Key, Timestamp, Value)\n", 1)
	s = regexp.MustCompile(`, (CAST\(`+"`__hdx_materialized_k8s.cluster.name`"+` AS String\)|ResourceAttributes\['k8s.cluster.name'\]) AS cluster FROM`).ReplaceAllString(s, " FROM")
	s = strings.Replace(s, "SELECT Timestamp, ColumnIdentifier, Key, Value, cluster, count() AS count FROM elements\nGROUP BY Timestamp, ColumnIdentifier, Key, Value, cluster",
		"SELECT Timestamp, ColumnIdentifier, Key, Value, count() AS count FROM elements\nGROUP BY Timestamp, ColumnIdentifier, Key, Value", 1)
	return s
}

type migConn struct {
	c chc
}

func (m migConn) Exec(ctx context.Context, sql string, settings map[string]string) error {
	v := url.Values{}
	for k, x := range settings {
		v.Set(k, x)
	}
	if code, b := m.c.do(sql, v, ""); code != 200 {
		return fmt.Errorf("%s: %s", firstN(sql, 120), b)
	}
	return nil
}

func (m migConn) Rows(ctx context.Context, sql string) ([]string, error) {
	code, b := m.c.do(sql+" FORMAT JSONCompactColumns", nil, "")
	if code != 200 {
		return nil, fmt.Errorf("%s: %s", firstN(sql, 120), b)
	}
	var cols [][]string
	if err := json.Unmarshal(b, &cols); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, nil
	}
	return cols[0], nil
}

// TestKVRollupMigration: a rollup made with ClickStack's DDL (no cluster
// column), filled by its view, migrated by internal/rollupmig: the fleet's
// sums are unchanged, every rebuilt row names its cluster, each cluster's
// sums are what the table says, rows inserted afterwards are attributed by
// the new view, a second run changes nothing, and a day only the rollup
// holds is kept.
func TestKVRollupMigration(t *testing.T) {
	if os.Getenv("HDXA_IT") == "" {
		t.Skip("HDXA_IT=1 runs the migration against a local ClickHouse")
	}
	root := filepath.Join("..", "..", "..")
	c := chc{t: t, url: env("HDXA_CH", "http://127.0.0.1:18123")}
	const db = "qhd_it_mig"
	c.exec("DROP DATABASE IF EXISTS " + db)
	c.exec("CREATE DATABASE " + db)
	if os.Getenv("HDXA_IT_KEEP") == "" {
		defer c.do("DROP DATABASE IF EXISTS "+db, nil, "")
	}
	q := func(sql string) string {
		code, b := c.do(sql+" FORMAT TSV", nil, "")
		if code != 200 {
			t.Fatalf("%s: %s", sql, b)
		}
		return strings.TrimSpace(string(b))
	}
	insert := func(table string, from, n int) {
		cols := "ResourceAttributes"
		if strings.HasSuffix(table, "logs") {
			cols = "ResourceAttributes, SeverityText, ScopeName"
		}
		vals := "map('k8s.cluster.name', ['qa', 'qb', ''][1 + number % 3], 'k8s.namespace.name', 'shop')"
		if strings.HasSuffix(table, "logs") {
			vals += ", ['INFO', 'ERROR'][1 + number % 2], 'scope'"
		}
		c.exec(fmt.Sprintf(`INSERT INTO %s (Timestamp, ServiceName, %s, received_at)
SELECT toDateTime64('2026-09-26 22:00:00', 9, 'UTC') + toIntervalSecond((number * 37) %% 172800) AS ts, ['frontend', 'cart'][1 + number %% 2], %s, ts
FROM numbers(%d, %d)`, table, cols, vals, from, n))
	}
	for _, sig := range []string{"logs", "traces"} {
		table := db + ".otel_" + sig
		ddlPath := filepath.Join(root, "otap-rs", "sql", "otel_"+sig+".sql")
		st := ddl(t, ddlPath, map[string]string{"table": table})
		c.exec(st[0])
		c.exec(legacyRollup(st[1]))
		c.exec(legacyRollup(st[2]))
		if strings.Contains(q("SHOW CREATE TABLE "+table+"_kv_rollup_15m"), "cluster") {
			t.Fatal("the legacy rollup has a cluster column")
		}
		insert(table, 0, 3000)
		rollup := table + "_kv_rollup_15m"
		// a day only the rollup holds (the table's rows gone by retention)
		c.exec("INSERT INTO " + rollup + " (Timestamp, ColumnIdentifier, Key, Value, count) VALUES ('2026-09-20 00:00:00', 'NativeColumn', 'ServiceName', 'old', 7)")
		sums := "SELECT ColumnIdentifier, Key, Value, sum(count) FROM " + rollup + " WHERE toDate(Timestamp) > '2026-09-20' GROUP BY ALL ORDER BY ALL"
		before := q(sums)
		p, err := rollupmig.NewPlan(ddlPath, table)
		if err != nil {
			t.Fatal(err)
		}
		rep, err := rollupmig.Migrate(context.Background(), migConn{c}, p)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: altered %v, rebuilt %v, kept %v", rollup, rep.Altered, rep.Days, rep.Kept)
		if !rep.Altered || len(rep.Days) != 3 || strings.Join(rep.Kept, ",") != "2026-09-20" {
			t.Fatalf("report %+v", rep)
		}
		if after := q(sums); after != before {
			t.Fatalf("fleet sums changed:\n%s\n---\n%s", before, after)
		}
		// the structure is the new DDL's
		want := ddl(t, ddlPath, map[string]string{"table": db + ".fresh_" + sig})[1]
		c.exec(want)
		norm := func(s, name string) string { return strings.ReplaceAll(s, name, "X") }
		if a, b := norm(q("SELECT create_table_query FROM system.tables WHERE database = '"+db+"' AND name = 'otel_"+sig+"_kv_rollup_15m'"), db+".otel_"+sig+"_kv_rollup_15m"),
			norm(q("SELECT create_table_query FROM system.tables WHERE database = '"+db+"' AND name = 'fresh_"+sig+"_kv_rollup_15m'"), db+".fresh_"+sig+"_kv_rollup_15m"); a != b {
			t.Fatalf("migrated rollup differs from the DDL's:\n%s\n%s", a, b)
		}
		perCluster := "SELECT ColumnIdentifier, Key, Value, cluster, sum(count) FROM " + rollup + " WHERE toDate(Timestamp) > '2026-09-20' GROUP BY ALL ORDER BY ALL"
		fromTable := "SELECT ColumnIdentifier, Key, Value, cluster, sum(count) FROM (" + p.Select + ") GROUP BY ALL ORDER BY ALL"
		if a, b := q(perCluster), q(fromTable); a != b {
			t.Fatalf("per-cluster sums differ from the table's:\n%s\n---\n%s", firstN(a, 400), firstN(b, 400))
		}
		if n := q("SELECT count() FROM " + rollup + " WHERE cluster = '' AND toDate(Timestamp) > '2026-09-20' AND Key = 'ServiceName' AND Value = 'frontend'"); n == "0" {
			t.Fatal("rows without a cluster name lost their '' cluster")
		}
		// the new view attributes new rows
		insert(table, 3000, 300)
		if a, b := q(perCluster), q(fromTable); a != b {
			t.Fatalf("after new rows:\n%s\n---\n%s", firstN(a, 400), firstN(b, 400))
		}
		// a second run changes nothing
		again := q(perCluster)
		rep, err = rollupmig.Migrate(context.Background(), migConn{c}, p)
		if err != nil || rep.Altered || q(perCluster) != again {
			t.Fatalf("second run: %+v %v", rep, err)
		}
		if n := q("SELECT count() FROM system.tables WHERE database = '" + db + "' AND name LIKE '%_migrate'"); n != "0" {
			t.Fatal("the migration left its work table")
		}
	}
}
