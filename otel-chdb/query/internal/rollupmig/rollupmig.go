// Package rollupmig migrates a central otel_logs / otel_traces table's
// key-value rollup (<table>_kv_rollup_15m and its materialized view) to the
// layout with a `cluster` column (otap-rs/sql/otel_{logs,traces}.sql,
// DECISIONS.md D33), so the query service can serve it scoped by cluster.
//
// The rollup is derived data: it is rebuilt from the table, one partition
// (day) at a time, and each day is swapped in atomically (REPLACE
// PARTITION), so a reader sees either a day's old rows (cluster ”) or its
// new ones, never a mix or a gap.
//
// The consumer must be stopped (every replica) while it runs: a row
// inserted into a day between that day's rebuild and its swap would be
// counted in the table and missing from the rollup. The consumer resumes
// from its lanes afterwards; nothing is lost at the edges (D19).
package rollupmig

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Conn runs statements on ClickHouse.
type Conn interface {
	// Exec runs a statement; settings are extra URL settings.
	Exec(ctx context.Context, sql string, settings map[string]string) error
	// Rows runs a query and returns its first column, one value per row.
	Rows(ctx context.Context, sql string) ([]string, error)
}

// Plan is one table's migration.
type Plan struct {
	// Table is the fully qualified table (db.otel_logs).
	Table string
	// Rollup and View are the rollup table and its materialized view.
	Rollup, View string
	// CreateView is the new view's CREATE statement (from the DDL file).
	CreateView string
	// Select is the view's query: what the rollup is rebuilt with.
	Select string
}

var fqRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)

// NewPlan reads the rollup's DDL (otap-rs/sql/otel_logs.sql or
// otel_traces.sql: the table, the rollup table, the view, split at a line
// ending in ';') for table.
func NewPlan(ddlPath, table string) (*Plan, error) {
	if !fqRE.MatchString(table) {
		return nil, fmt.Errorf("table %q: want database.table", table)
	}
	b, err := os.ReadFile(ddlPath)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			lines = append(lines, l)
		}
	}
	var st []string
	for _, s := range strings.Split(strings.Join(lines, "\n"), ";\n") {
		if s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";")); s != "" {
			st = append(st, strings.ReplaceAll(s, "{table}", table))
		}
	}
	if len(st) != 3 {
		return nil, fmt.Errorf("%s: want 3 statements (table, rollup, view), got %d", ddlPath, len(st))
	}
	p := &Plan{Table: table, Rollup: table + "_kv_rollup_15m", CreateView: st[2]}
	m := regexp.MustCompile(`^CREATE MATERIALIZED VIEW IF NOT EXISTS (\S+) TO (\S+)\s+AS\s+`).FindStringSubmatchIndex(st[2])
	if m == nil {
		return nil, fmt.Errorf("%s: the third statement is not the rollup's view", ddlPath)
	}
	p.View = st[2][m[2]:m[3]]
	if to := st[2][m[4]:m[5]]; to != p.Rollup {
		return nil, fmt.Errorf("%s: the view writes %s, not %s", ddlPath, to, p.Rollup)
	}
	p.Select = st[2][m[1]:]
	if !strings.Contains(st[1], "`cluster`") || !strings.Contains(p.Select, " AS cluster ") {
		return nil, fmt.Errorf("%s: the rollup DDL has no cluster column: nothing to migrate to", ddlPath)
	}
	return p, nil
}

// Report says what Migrate did.
type Report struct {
	Altered bool     // the rollup gained its cluster column
	Days    []string // the partitions rebuilt
	Kept    []string // days only the rollup holds: kept as they were (no cluster)
}

// Migrate runs p: add the column (a metadata change), replace the view,
// then rebuild every day of the rollup and of the table from the table.
// It is idempotent: a second run rebuilds the days again and changes
// nothing.
func Migrate(ctx context.Context, c Conn, p *Plan) (*Report, error) {
	r := &Report{}
	db, name, _ := strings.Cut(p.Rollup, ".")
	has, err := c.Rows(ctx, fmt.Sprintf("SELECT name FROM system.columns WHERE database = '%s' AND table = '%s' AND name = 'cluster'", db, name))
	if err != nil {
		return nil, err
	}
	if len(has) == 0 {
		// SummingMergeTree: the new column must end the sort key, or rows
		// of two clusters would be summed into one
		if err := c.Exec(ctx, "ALTER TABLE "+p.Rollup+" ADD COLUMN IF NOT EXISTS `cluster` LowCardinality(String) AFTER Value, "+
			"MODIFY ORDER BY (ColumnIdentifier, Key, Timestamp, Value, cluster)", nil); err != nil {
			return nil, fmt.Errorf("alter %s: %w", p.Rollup, err)
		}
		r.Altered = true
	}
	for _, s := range []string{"DROP VIEW IF EXISTS " + p.View, p.CreateView} {
		if err := c.Exec(ctx, s, nil); err != nil {
			return nil, fmt.Errorf("view %s: %w", p.View, err)
		}
	}
	tmp := p.Rollup + "_migrate"
	if err := c.Exec(ctx, "DROP TABLE IF EXISTS "+tmp, nil); err != nil {
		return nil, err
	}
	if err := c.Exec(ctx, "CREATE TABLE "+tmp+" AS "+p.Rollup, nil); err != nil {
		return nil, err
	}
	// the days the table holds; a day only the rollup still has (the
	// table's rows gone by retention) cannot be rebuilt and keeps its rows
	// without a cluster: fleet callers see them, restricted ones do not
	days, err := c.Rows(ctx, fmt.Sprintf("SELECT DISTINCT toString(toDate(Timestamp)) AS d FROM %s ORDER BY d", p.Table))
	if err != nil {
		return nil, err
	}
	r.Kept, err = c.Rows(ctx, fmt.Sprintf("SELECT DISTINCT toString(toDate(Timestamp)) AS d FROM %s WHERE d NOT IN "+
		"(SELECT DISTINCT toString(toDate(Timestamp)) FROM %s) ORDER BY d", p.Rollup, p.Table))
	if err != nil {
		return nil, err
	}
	for _, d := range days {
		if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(d) {
			return nil, fmt.Errorf("partition %q is not a day", d)
		}
		// the view's own query, cut to the day's rows of the table
		filter := fmt.Sprintf("{'%s': 'toDate(Timestamp) = \\'%s\\''}", p.Table, d)
		if err := c.Exec(ctx, "INSERT INTO "+tmp+" "+p.Select, map[string]string{"additional_table_filters": filter}); err != nil {
			return nil, fmt.Errorf("rebuild %s: %w", d, err)
		}
		if err := c.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION '%s' FROM %s", p.Rollup, d, tmp), nil); err != nil {
			return nil, fmt.Errorf("swap %s: %w", d, err)
		}
		if err := c.Exec(ctx, "TRUNCATE TABLE "+tmp, nil); err != nil {
			return nil, err
		}
		r.Days = append(r.Days, d)
	}
	return r, c.Exec(ctx, "DROP TABLE IF EXISTS "+tmp, nil)
}
