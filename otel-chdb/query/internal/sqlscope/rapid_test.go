package sqlscope

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
	"pgregory.net/rapid"
)

// gen builds a random SELECT from allowed and forbidden pieces and records
// whether it used a forbidden one: the oracle for the rewrite's properties.
type gen struct {
	t         *rapid.T
	noWith    bool // the parser refuses WITH at the start of an IN (...) subquery
	forbidden []string
	strings   []string // string literal bodies, as written
	depth     int
}

func (g *gen) pick(label string, xs ...string) string {
	return xs[rapid.IntRange(0, len(xs)-1).Draw(g.t, label)]
}

func (g *gen) literal() string {
	// any runes, escaped the way ClickHouse reads them
	s := rapid.StringMatching(`[a-zA-Z0-9 '\\%_\-\x{e9}\x{1F600}]{0,12}`).Draw(g.t, "lit")
	body := strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s)
	g.strings = append(g.strings, body)
	return "'" + body + "'"
}

func (g *gen) expr() string {
	switch rapid.IntRange(0, 7).Draw(g.t, "expr") {
	case 0:
		return g.pick("col", "Body", "ServiceName", "TraceId", "Timestamp", "ResourceAttributes['k8s.pod.name']")
	case 1:
		return g.literal()
	case 2:
		return fmt.Sprint(rapid.IntRange(0, 1000).Draw(g.t, "n"))
	case 3:
		f := g.pick("fn", "lower", "length", "toString", "coalesce", "dictGet", "sleep", "getSetting", "file")
		if f == "dictGet" || f == "sleep" || f == "getSetting" || f == "file" {
			g.forbidden = append(g.forbidden, "denied_function")
		}
		return f + "(" + g.expr() + ")"
	case 4:
		return g.expr() + " = " + g.expr()
	case 5:
		if g.depth < 3 {
			g.depth++
			defer func() { g.depth-- }()
			g.noWith = true
			return "Body IN (" + g.query(nil) + ")"
		}
		return "1"
	case 6:
		return "Body IN (" + g.literal() + ", " + g.literal() + ")"
	default:
		return "(" + g.expr() + ")"
	}
}

func (g *gen) table(ctes []string) string {
	choice := rapid.IntRange(0, 9).Draw(g.t, "table")
	switch {
	case choice <= 3:
		return g.pick("allowed", "otel_logs", "otel.otel_logs", "otel_traces", "otel.otel_traces")
	case choice == 4 && len(ctes) > 0:
		return g.pick("cte", ctes...)
	case choice == 5:
		g.forbidden = append(g.forbidden, "table_not_allowed")
		return g.pick("denied", "system.users", "secrets", "other.otel_logs", "system.query_log")
	case choice == 6:
		g.forbidden = append(g.forbidden, "table_function")
		return g.pick("tf", "s3('http://h/b')", "numbers(10)", "merge('otel', '.*')", "remote('h', otel.otel_logs)")
	case choice == 7 && g.depth < 3:
		g.depth++
		defer func() { g.depth-- }()
		return "(" + g.query(ctes) + ")"
	default:
		return "otel_logs"
	}
}

func (g *gen) query(ctes []string) string {
	var b strings.Builder
	own := append([]string(nil), ctes...)
	noWith := g.noWith
	g.noWith = false
	if !noWith && g.depth < 3 && rapid.Bool().Draw(g.t, "with") {
		name := g.pick("ctename", "c1", "c2", "recent")
		g.depth++
		body := g.query(own)
		g.depth--
		fmt.Fprintf(&b, "WITH %s AS (%s) ", name, body)
		own = append(own, name)
	}
	fmt.Fprintf(&b, "SELECT %s FROM %s", g.expr(), g.table(own))
	if rapid.Bool().Draw(g.t, "join") {
		fmt.Fprintf(&b, " AS l JOIN %s AS r ON l.TraceId = r.TraceId", g.table(own))
	}
	if rapid.Bool().Draw(g.t, "where") {
		fmt.Fprintf(&b, " WHERE %s", g.expr())
	}
	if rapid.IntRange(0, 9).Draw(g.t, "settings") == 0 {
		g.forbidden = append(g.forbidden, "settings_clause")
		b.WriteString(" SETTINGS max_threads = 1")
	}
	if g.depth < 3 && rapid.IntRange(0, 5).Draw(g.t, "union") == 0 {
		g.depth++
		// a branch does not see this SELECT's own CTE
		fmt.Fprintf(&b, " UNION ALL %s", g.query(ctes))
		g.depth--
	}
	return b.String()
}

func literals(sql string) []string {
	st, err := chp.NewParser(sql).ParseStmts()
	if err != nil {
		return nil
	}
	var out []string
	visit(reflectValue(st[0]), func(n chp.Expr) bool {
		if s, ok := n.(*chp.StringLiteral); ok {
			out = append(out, s.Literal)
		}
		return true
	})
	sort.Strings(out)
	return out
}

// TestRewriteProperties: for any generated statement,
//   - it is accepted exactly when no forbidden piece was used (a query
//     using only allowed pieces must not be refused, one using a forbidden
//     piece must be);
//   - an accepted statement's text parses back to itself, reads only
//     allow-listed tables (every real table qualified), has no table
//     function or SETTINGS, carries a filter for every table it reads, and
//     keeps every string literal byte for byte.
func TestRewriteProperties(t *testing.T) {
	rapid.Check(t, rewriteProperty(testPolicy(t)))
}

// rewriteProperty is TestRewriteProperties' property, shared with
// FuzzRewriteProperties (coverage-guided choices through rapid.MakeFuzz).
func rewriteProperty(p *Policy) func(*rapid.T) {
	return func(rt *rapid.T) {
		g := &gen{t: rt}
		sql := g.query(nil)
		pr, err := p.Prepare(sql)
		var res *Result
		if err == nil {
			res, err = pr.Finish(teamA)
		}
		if len(g.forbidden) > 0 {
			if err == nil {
				rt.Fatalf("accepted a statement with %v:\n%s\n=> %s", g.forbidden, sql, res.SQL)
			}
			return
		}
		if err != nil {
			rt.Fatalf("refused an allowed statement: %v\n%s", err, sql)
		}
		st, perr := chp.NewParser(res.SQL).ParseStmts()
		if perr != nil || len(st) != 1 || chp.Format(st[0]) != res.SQL {
			rt.Fatalf("output is not a fixed point: %v\n%s", perr, res.SQL)
		}
		ctes := map[string]bool{"c1": true, "c2": true, "recent": true}
		visit(reflectValue(st[0]), func(n chp.Expr) bool {
			switch x := n.(type) {
			case *chp.TableIdentifier:
				if x.Database == nil {
					if !ctes[x.Table.Name] {
						rt.Fatalf("unqualified non-CTE table %s in %s", x.Table.Name, res.SQL)
					}
				} else if _, ok := p.Tables[x.Database.Name+"."+x.Table.Name]; !ok {
					rt.Fatalf("output reads %s.%s", x.Database.Name, x.Table.Name)
				}
			case *chp.TableFunctionExpr, *chp.SettingsClause, *chp.FormatClause:
				rt.Fatalf("output has %T: %s", n, res.SQL)
			}
			return true
		})
		for _, tb := range res.Tables {
			if res.Filters[tb] == "" {
				rt.Fatalf("no scope filter for %s", tb)
			}
		}
		in, out := literals(sql), literals(res.SQL)
		if strings.Join(in, "\x00") != strings.Join(out, "\x00") {
			rt.Fatalf("string literals changed:\n in %q\nout %q", in, out)
		}
	}
}

// TestScopeValueProperty: whatever a token carries as a cluster name, the
// filter either refuses it or lists exactly that name.
func TestScopeValueProperty(t *testing.T) {
	p := testPolicy(t)
	rapid.Check(t, func(rt *rapid.T) {
		name := rapid.OneOf(
			rapid.StringMatching(`[a-z0-9][a-z0-9._-]{0,8}[a-z0-9]`),
			rapid.String(),
		).Draw(rt, "cluster")
		pr, err := p.Prepare("SELECT count() FROM otel_traces")
		if err != nil {
			rt.Fatal(err)
		}
		res, err := pr.Finish(Scope{Clusters: []string{name}, AllNamespaces: true})
		if !ClusterRE.MatchString(name) {
			if err == nil {
				rt.Fatalf("accepted cluster %q", name)
			}
			return
		}
		if err != nil {
			rt.Fatalf("refused valid cluster %q: %v", name, err)
		}
		got := literals("SELECT 1 WHERE " + res.Filters["otel.otel_traces"])
		want := []string{"k8s.cluster.name", name}
		sort.Strings(want)
		if strings.Join(got, "|") != strings.Join(want, "|") {
			rt.Fatalf("filter %q: literals %q, want %q", res.Filters["otel.otel_traces"], got, want)
		}
	})
}
