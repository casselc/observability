package sqlscope

import (
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
	"testing"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
	"pgregory.net/rapid"
)

// FuzzRewriteProperties drives the property test's generator with
// coverage-guided bytes.
func FuzzRewriteProperties(f *testing.F) {
	f.Fuzz(rapid.MakeFuzz(rewriteProperty(testPolicy(f))))
}

// FuzzPrepare: any text a user sends as SQL (a security boundary: SEC-7,
// R-S9) never panics the scoper, and what it accepts is safe to run: the
// output parses back to itself, reads only allow-listed tables (every real
// table qualified), has no table function, SETTINGS or FORMAT, and carries
// a scope filter for every table it reads.
func FuzzPrepare(f *testing.F) {
	tracetag.Covers(f, "FZ", "SEC-7", "R-S9")
	for _, s := range []string{
		"SELECT Body FROM otel_logs WHERE ServiceName = 'cart' LIMIT 10",
		"WITH c1 AS (SELECT * FROM otel.otel_traces) SELECT count() FROM c1",
		"SELECT * FROM system.users",
		"SELECT * FROM s3('http://h/b')",
		"SELECT dictGet('d', 'x', 1) FROM otel_logs",
		"SELECT 1 FROM otel_logs AS l JOIN otel_traces AS r ON l.TraceId = r.TraceId SETTINGS max_threads = 1",
		"SELECT Body FROM otel_logs UNION ALL SELECT Body FROM otel.otel_logs",
		"SELECT Body FROM otel_logs WHERE Body IN (SELECT Body FROM otel_logs)",
	} {
		f.Add(s)
	}
	p := testPolicy(f)
	f.Fuzz(func(t *testing.T, sql string) {
		pr, err := p.Prepare(sql)
		if err != nil {
			return
		}
		res, err := pr.Finish(teamA)
		if err != nil {
			return
		}
		st, perr := chp.NewParser(res.SQL).ParseStmts()
		if perr != nil || len(st) != 1 || chp.Format(st[0]) != res.SQL {
			t.Fatalf("output is not a fixed point: %v\nin:  %q\nout: %q", perr, sql, res.SQL)
		}
		ctes := map[string]bool{}
		visit(reflectValue(st[0]), func(n chp.Expr) bool {
			if c, ok := n.(*chp.CTEStmt); ok {
				for _, e := range []chp.Expr{c.Expr, c.Alias} {
					if id, ok := e.(*chp.Ident); ok {
						ctes[id.Name] = true
					}
				}
			}
			return true
		})
		visit(reflectValue(st[0]), func(n chp.Expr) bool {
			switch x := n.(type) {
			case *chp.TableIdentifier:
				if x.Database == nil && !ctes[x.Table.Name] {
					t.Fatalf("unqualified table %s that no WITH defines\nin:  %q\nout: %q", x.Table.Name, sql, res.SQL)
				}
				if x.Database != nil {
					if _, ok := p.Tables[x.Database.Name+"."+x.Table.Name]; !ok {
						t.Fatalf("output reads %s.%s\nin: %q", x.Database.Name, x.Table.Name, sql)
					}
				}
			case *chp.TableFunctionExpr, *chp.SettingsClause, *chp.FormatClause:
				t.Fatalf("output has %T\nin:  %q\nout: %q", n, sql, res.SQL)
			}
			return true
		})
		for _, tb := range res.Tables {
			if res.Filters[tb] == "" {
				t.Fatalf("no scope filter for %s\nin: %q", tb, sql)
			}
		}
	})
}
