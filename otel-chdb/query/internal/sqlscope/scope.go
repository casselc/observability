// Package sqlscope turns a caller's SQL into the statement the query service
// runs: parsed with a ClickHouse-grammar parser, checked against an
// allow-list, rebuilt from the tree (never spliced), and paired with one
// scope predicate per table read, built as a tree too.
//
// The scope predicate is not added to the statement's WHERE. It is passed to
// ClickHouse as the additional_table_filters setting, which ClickHouse applies
// to every read of that table wherever it occurs (a subquery, a CTE, a join,
// an IN subquery), as a PREWHERE that uses the primary key, and which a
// SELECT alias cannot shadow. A WHERE predicate can be shadowed:
//
//	SELECT 'prod' AS `__hdx_materialized_k8s.cluster.name` ... WHERE <scope>
//
// resolves the scope's column to the alias. See README.md, "SQL".
package sqlscope

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Table is one table callers may read, and how a read of it is scoped.
type Table struct {
	Database string `json:"database"`
	Name     string `json:"name"`
	// TimeColumn is the column a request's window restricts (Timestamp).
	TimeColumn string `json:"time_column"`
	// Scope: "columns" (Cluster / Namespace are expressions over the row),
	// "catalog" (rows are restricted to the resource ids the entity catalog
	// resolves for the caller's clusters and namespaces; ResourceID names the
	// column), or "fleet" (no per-row scope: only callers with every cluster
	// and namespace may read it).
	Scope      string `json:"scope"`
	Cluster    string `json:"cluster_expr"`
	Namespace  string `json:"namespace_expr"`
	ResourceID string `json:"resource_id_column"`

	cluster, namespace, rid, tcol chp.Expr
}

// FQN is the table's canonical database.table name.
func (t *Table) FQN() string { return t.Database + "." + t.Name }

// Policy is the allow-list.
type Policy struct {
	DefaultDatabase string
	Tables          map[string]*Table // by FQN
	MaxSQLBytes     int
}

// NewPolicy parses each table's configured expressions (trusted config).
func NewPolicy(defaultDB string, tables []*Table, maxSQL int) (*Policy, error) {
	p := &Policy{DefaultDatabase: defaultDB, Tables: map[string]*Table{}, MaxSQLBytes: maxSQL}
	if p.MaxSQLBytes <= 0 {
		p.MaxSQLBytes = 64 << 10
	}
	for _, t := range tables {
		if t.Database == "" {
			t.Database = defaultDB
		}
		if !nameRE.MatchString(t.Database) || !nameRE.MatchString(t.Name) {
			return nil, fmt.Errorf("table %q.%q: names must match %s", t.Database, t.Name, nameRE)
		}
		var err error
		parse := func(s string) chp.Expr {
			if s == "" || err != nil {
				return nil
			}
			var e chp.Expr
			e, err = parseExpr(s)
			return e
		}
		switch t.Scope {
		case "columns":
			t.cluster, t.namespace = parse(t.Cluster), parse(t.Namespace)
		case "catalog":
			if t.ResourceID == "" {
				t.ResourceID = "resource_id"
			}
			t.rid = parse(t.ResourceID)
		case "fleet":
		default:
			return nil, fmt.Errorf("table %s: scope must be columns, catalog or fleet, not %q", t.FQN(), t.Scope)
		}
		t.tcol = parse(t.TimeColumn)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", t.FQN(), err)
		}
		p.Tables[t.FQN()] = t
	}
	return p, nil
}

var nameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func parseExpr(s string) (chp.Expr, error) {
	st, err := chp.NewParser("SELECT " + s).ParseStmts()
	if err != nil {
		return nil, fmt.Errorf("expression %q: %w", s, err)
	}
	if len(st) != 1 {
		return nil, fmt.Errorf("expression %q: not one expression", s)
	}
	sq, ok := st[0].(*chp.SelectQuery)
	if !ok || len(sq.SelectItems) != 1 || sq.From != nil || sq.SelectItems[0].Alias != nil {
		return nil, fmt.Errorf("expression %q: not one expression", s)
	}
	return sq.SelectItems[0].Expr, nil
}

// Rejection is why a statement was refused. Reason is a stable token for
// metrics, audit and the API; Detail is for people.
type Rejection struct {
	Reason string
	Detail string
}

func (r *Rejection) Error() string { return r.Reason + ": " + r.Detail }

func reject(reason, format string, a ...any) error {
	return &Rejection{Reason: reason, Detail: fmt.Sprintf(format, a...)}
}

// AsRejection returns the rejection behind err, if any.
func AsRejection(err error) (*Rejection, bool) {
	var r *Rejection
	ok := errors.As(err, &r)
	return r, ok
}

// Prepared is a statement that passed the allow-list, with its tables
// qualified; Finish adds the caller's scope.
type Prepared struct {
	policy *Policy
	root   *chp.SelectQuery
	tables map[string]*Table
}

// Tables are the allow-listed tables the statement reads.
func (p *Prepared) Tables() []*Table {
	out := make([]*Table, 0, len(p.tables))
	for _, t := range p.tables {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FQN() < out[j].FQN() })
	return out
}

// Prepare parses and checks sql. Every rejection is a *Rejection.
func (p *Policy) Prepare(sql string) (*Prepared, error) {
	if len(sql) > p.MaxSQLBytes {
		return nil, reject("too_long", "statement is %d bytes, the limit is %d", len(sql), p.MaxSQLBytes)
	}
	stmts, err := chp.NewParser(sql).ParseStmts()
	if err != nil {
		return nil, reject("parse_error", "%v", err)
	}
	if len(stmts) != 1 {
		return nil, reject("statement_count", "exactly one statement, got %d", len(stmts))
	}
	root, ok := stmts[0].(*chp.SelectQuery)
	if !ok {
		return nil, reject("not_select", "only SELECT is allowed, got %T", stmts[0])
	}
	pr := &Prepared{policy: p, root: root, tables: map[string]*Table{}}
	if err := pr.checkSelect(root, nil); err != nil {
		return nil, err
	}
	return pr, nil
}

// denied scalar functions: they read objects by name (dictionaries, Join
// tables, settings, files), reveal the server, or stall it. Compared
// lower-case; the first list by prefix, the second exactly.
var (
	deniedPrefixes = []string{"dictget", "dicthas", "dictisin", "joinget", "filesystem", "getsetting",
		"catboost", "addressto", "transaction", "generateserial"}
	deniedNames = map[string]bool{
		"file": true, "url": true, "s3": true, "input": true, "sleep": true, "sleepeachrow": true,
		"hascolumnintable": true, "getmacro": true, "getclienthttpheader": true, "modelevaluate": true,
		"evalmlmethod": true, "demangle": true, "getserverport": true, "globalvariable": true,
		"currentprofiles": true, "enabledprofiles": true, "defaultprofiles": true,
		"currentroles": true, "enabledroles": true, "defaultroles": true, "tcpport": true,
		"getoskernelversion": true, "buildid": true,
	}
)

func deniedFunction(name string) bool {
	n := strings.ToLower(name)
	if deniedNames[n] {
		return true
	}
	for _, p := range deniedPrefixes {
		if strings.HasPrefix(n, p) {
			return true
		}
	}
	return false
}

// checkSelect checks one SELECT scope. visible holds the CTE names (of
// subqueries) this SELECT can reference: its ancestors' and its own. A
// set-operation branch (UNION ALL, EXCEPT, ...) does not see the first
// branch's WITH: if ClickHouse does, the reference is refused as an unknown
// table (fail closed), never read as a table it is not.
func (pr *Prepared) checkSelect(sq *chp.SelectQuery, inherited map[string]bool) error {
	if sq.Settings != nil {
		return reject("settings_clause", "SETTINGS is not allowed; limits are the service's")
	}
	if sq.Format != nil {
		return reject("format_clause", "FORMAT is not allowed; the service picks the output format")
	}
	own := map[string]bool{}
	for k := range inherited {
		own[k] = true
	}
	if sq.With != nil {
		for _, c := range sq.With.CTEs {
			if name, ok := c.Expr.(*chp.Ident); ok {
				if _, isQuery := c.Alias.(*chp.SelectQuery); isQuery {
					own[name.Name] = true
				}
			}
		}
	}
	branches := map[chp.Expr]bool{}
	for _, b := range []*chp.SelectQuery{sq.UnionAll, sq.UnionDistinct, sq.Except, sq.Intersect} {
		if b != nil {
			branches[b] = true
		}
	}
	var err error
	children(sq, func(n chp.Expr) bool {
		if err != nil {
			return false
		}
		if inner, ok := n.(*chp.SelectQuery); ok {
			scope := own
			if branches[inner] {
				scope = inherited
			}
			err = pr.checkSelect(inner, scope)
			return false
		}
		err = pr.checkNode(n, own)
		return err == nil
	})
	return err
}

func (pr *Prepared) checkNode(n chp.Expr, visible map[string]bool) error {
	switch x := n.(type) {
	case *chp.SettingsClause:
		return reject("settings_clause", "SETTINGS is not allowed")
	case *chp.FormatClause:
		return reject("format_clause", "FORMAT is not allowed")
	case *chp.TableFunctionExpr:
		return reject("table_function", "table functions are not allowed: %s", chp.Format(x))
	case *chp.QueryParam:
		return reject("query_param", "query parameters are not supported")
	case *chp.PlaceHolder:
		return reject("query_param", "placeholders are not supported")
	case *chp.Ident:
		if strings.ContainsAny(x.Name, "\\\x00\n\r`") {
			return reject("bad_identifier", "identifier %q contains a character the service does not pass", x.Name)
		}
	case *chp.FunctionExpr:
		if x.Name != nil && deniedFunction(x.Name.Name) {
			return reject("denied_function", "function %s is not allowed", x.Name.Name)
		}
	case *chp.BinaryOperation:
		if strings.EqualFold(string(x.Operation), "IN") || strings.EqualFold(string(x.Operation), "NOT IN") {
			switch x.RightExpr.(type) {
			case *chp.Ident, *chp.Path, *chp.NestedIdentifier, *chp.TableIdentifier:
				return reject("in_table", "IN <table> is not allowed; use IN (SELECT ...)")
			}
		}
	case *chp.TableIdentifier:
		return pr.table(x, visible)
	case *chp.StringLiteral:
		if strings.ContainsRune(x.Literal, 0) {
			return reject("bad_literal", "string literal contains NUL")
		}
	}
	return nil
}

func (pr *Prepared) table(ti *chp.TableIdentifier, visible map[string]bool) error {
	if ti.Table == nil {
		return reject("table_not_allowed", "empty table name")
	}
	if ti.Database == nil && visible[ti.Table.Name] {
		return nil // a CTE of this scope
	}
	db := pr.policy.DefaultDatabase
	if ti.Database != nil {
		db = ti.Database.Name
	}
	t, ok := pr.policy.Tables[db+"."+ti.Table.Name]
	if !ok {
		return reject("table_not_allowed", "table %s.%s is not one the service serves", db, ti.Table.Name)
	}
	pr.tables[t.FQN()] = t
	// qualify it, so that ClickHouse's default database never decides
	ti.Database = &chp.Ident{Name: t.Database, QuoteType: chp.Unquoted}
	ti.Table = &chp.Ident{Name: t.Name, QuoteType: chp.Unquoted}
	return nil
}

// Scope is what the caller may see, resolved from the token's attributes.
type Scope struct {
	AllClusters   bool
	Clusters      []string
	AllNamespaces bool
	Namespaces    []string
	// ResourceIDs is the entity catalog's answer for catalog-scoped tables:
	// the resources of the caller's clusters and namespaces. Nil means not
	// resolved, which refuses a catalog-scoped table for a restricted caller.
	ResourceIDs []uint64
	// Window, if set, restricts every table with a time column to
	// [FromNs, ToNs).
	Window *Window
}

// Window is a half-open event-time range in ns since the Unix epoch.
type Window struct{ FromNs, ToNs int64 }

// Result is what runs.
type Result struct {
	SQL     string            `json:"sql"`
	Tables  []string          `json:"tables"`
	Filters map[string]string `json:"filters"`
	Hash    string            `json:"hash"`
}

// FiltersSetting renders Filters as the additional_table_filters setting's
// value: a map literal of quoted strings.
func (r *Result) FiltersSetting() string {
	if len(r.Filters) == 0 {
		return ""
	}
	keys := make([]string, 0, len(r.Filters))
	for k := range r.Filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(Quote(k))
		b.WriteByte(':')
		b.WriteString(Quote(r.Filters[k]))
	}
	b.WriteByte('}')
	return b.String()
}

// Quote renders s as a ClickHouse string literal.
func Quote(s string) string { return "'" + escape(s) + "'" }

func escape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\x00", `\0`, "\n", `\n`, "\r", `\r`, "\t", `\t`)
	return r.Replace(s)
}

// ClusterRE and NamespaceRE bound the scope values a token may carry:
// FORMAT.md §1's cluster names and Kubernetes namespace names.
var (
	ClusterRE   = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)
	NamespaceRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// Finish builds the scope predicates and the statement that runs, and checks
// that the rebuilt text parses back to itself and passes the allow-list
// again (so what ClickHouse receives is what was checked).
func (pr *Prepared) Finish(s Scope) (*Result, error) {
	res := &Result{Filters: map[string]string{}}
	for _, t := range pr.Tables() {
		pred, err := predicate(t, s)
		if err != nil {
			return nil, err
		}
		res.Tables = append(res.Tables, t.FQN())
		if pred != nil {
			text := chp.Format(pred)
			if err := fixedPoint("SELECT 1 WHERE " + text); err != nil {
				return nil, err
			}
			res.Filters[t.FQN()] = text
		}
	}
	res.SQL = chp.Format(pr.root)
	again, err := pr.policy.Prepare(res.SQL)
	if err != nil {
		return nil, reject("roundtrip", "the rebuilt statement does not pass again: %v", err)
	}
	if got := chp.Format(again.root); got != res.SQL {
		return nil, reject("roundtrip", "the rebuilt statement is not a fixed point")
	}
	if !sameTables(again.tables, pr.tables) {
		return nil, reject("roundtrip", "the rebuilt statement reads other tables")
	}
	h := sha256.New()
	h.Write([]byte(res.SQL))
	h.Write([]byte{0})
	h.Write([]byte(res.FiltersSetting()))
	res.Hash = hex.EncodeToString(h.Sum(nil))
	return res, nil
}

func sameTables(a, b map[string]*Table) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func fixedPoint(sql string) error {
	st, err := chp.NewParser(sql).ParseStmts()
	if err != nil || len(st) != 1 {
		return reject("roundtrip", "a built predicate does not parse: %v", err)
	}
	if chp.Format(st[0]) != sql {
		return reject("roundtrip", "a built predicate is not a fixed point")
	}
	return nil
}

func predicate(t *Table, s Scope) (chp.Expr, error) {
	var parts []chp.Expr
	restricted := !s.AllClusters || !s.AllNamespaces
	switch t.Scope {
	case "fleet":
		if restricted {
			return nil, reject("scope_unenforceable", "table %s holds every cluster's rows and has no per-row scope; it needs every cluster and namespace", t.FQN())
		}
	case "columns":
		if !s.AllClusters {
			if t.cluster == nil {
				return nil, reject("scope_unenforceable", "table %s has no cluster column", t.FQN())
			}
			vals, err := strs(s.Clusters, ClusterRE, "cluster")
			if err != nil {
				return nil, err
			}
			parts = append(parts, in(t.cluster, vals))
		}
		if !s.AllNamespaces {
			if t.namespace == nil {
				return nil, reject("scope_unenforceable", "table %s has no namespace column", t.FQN())
			}
			vals, err := strs(s.Namespaces, NamespaceRE, "namespace")
			if err != nil {
				return nil, err
			}
			parts = append(parts, in(t.namespace, vals))
		}
	case "catalog":
		if restricted {
			if s.ResourceIDs == nil {
				return nil, reject("catalog_unavailable", "table %s is scoped by the entity catalog, which did not answer", t.FQN())
			}
			if len(s.ResourceIDs) == 0 {
				return num("0"), nil // no resource of the caller's scope: no row
			}
			ids := append([]uint64(nil), s.ResourceIDs...)
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			items := make([]chp.Expr, 0, len(ids))
			for i, id := range ids {
				if i > 0 && ids[i-1] == id {
					continue
				}
				items = append(items, num(strconv.FormatUint(id, 10)))
			}
			parts = append(parts, &chp.BinaryOperation{LeftExpr: t.rid, Operation: chp.TokenKind(chp.KeywordIn), RightExpr: list(items)})
		}
	}
	if s.Window != nil && t.tcol != nil {
		if s.Window.ToNs <= s.Window.FromNs {
			return nil, reject("bad_window", "the window is empty")
		}
		parts = append(parts,
			&chp.BinaryOperation{LeftExpr: t.tcol, Operation: chp.TokenKindGE, RightExpr: nanos(s.Window.FromNs)},
			&chp.BinaryOperation{LeftExpr: t.tcol, Operation: chp.TokenKindLT, RightExpr: nanos(s.Window.ToNs)})
	}
	if len(parts) == 0 {
		return nil, nil
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out = &chp.BinaryOperation{LeftExpr: out, Operation: chp.TokenKind(chp.KeywordAnd), RightExpr: p}
	}
	return out, nil
}

func strs(vals []string, re *regexp.Regexp, what string) ([]chp.Expr, error) {
	if len(vals) == 0 {
		return nil, reject("empty_scope", "no %s in scope", what)
	}
	seen := map[string]bool{}
	var out []chp.Expr
	sorted := append([]string(nil), vals...)
	sort.Strings(sorted)
	for _, v := range sorted {
		if !re.MatchString(v) {
			return nil, reject("bad_scope_value", "%s %q is not a valid name", what, v)
		}
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, &chp.StringLiteral{Literal: escape(v)})
	}
	return out, nil
}

func in(left chp.Expr, items []chp.Expr) chp.Expr {
	return &chp.BinaryOperation{LeftExpr: left, Operation: chp.TokenKind(chp.KeywordIn), RightExpr: list(items)}
}

func list(items []chp.Expr) chp.Expr {
	cols := make([]chp.Expr, len(items))
	for i, it := range items {
		cols[i] = &chp.ColumnExpr{Expr: it}
	}
	return &chp.ParamExprList{Items: &chp.ColumnExprList{Items: cols}}
}

func num(s string) chp.Expr { return &chp.NumberLiteral{Literal: s, Base: 10} }

func fn(name string, args ...chp.Expr) chp.Expr {
	return &chp.FunctionExpr{Name: &chp.Ident{Name: name, QuoteType: chp.Unquoted}, Params: list(args).(*chp.ParamExprList)}
}

// nanos is fromUnixTimestamp64Nano(toInt64(n), 'UTC'): a DateTime64(9).
func nanos(n int64) chp.Expr {
	return fn("fromUnixTimestamp64Nano", fn("toInt64", num(strconv.FormatInt(n, 10))), &chp.StringLiteral{Literal: "UTC"})
}
