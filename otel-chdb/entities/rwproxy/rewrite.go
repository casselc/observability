package main

// The rewrite: parse a statement with a ClickHouse grammar, find the SELECTs
// that read a catalog table (variant c: ResourceAttributes is an ALIAS
// rebuilt from dictionaries), and replace what they do with
// ResourceAttributes['k'] by catalog lookups. Everything else in the
// statement is left byte for byte: the parser is used to find spans, and the
// output is the original text with those spans replaced.
//
// Semantics being preserved (the ALIAS column, ../sql/generated/c_*.sql):
//
//   ResourceAttributes = mapUpdate(covered(resource_id) if the catalog knows
//                        resource_id else {}, ResourceResidual)
//
// so for a key k, the row's value is ResourceResidual[k] when the residual
// has k (the residual wins), otherwise the catalog's value, otherwise ''.

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Result of one rewrite.
type Result struct {
	SQL       string         `json:"sql,omitempty"`
	Rewritten bool           `json:"rewritten"`
	Reason    string         `json:"reason"`          // why not rewritten, or "ok"
	Rules     map[string]int `json:"rules,omitempty"` // rule -> count
	Left      int            `json:"left"`            // ResourceAttributes references left to the ALIAS
	Err       string         `json:"err,omitempty"`
}

type placeholder struct{ name, typ string }

// {name:Type}: ClickHouse query parameters. HyperDX sends every database,
// table, time bound and limit as one.
var paramRe = regexp.MustCompile(`\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([^{}]+?)\s*\}`)

// mask replaces each {name:Type} with an identifier of the same length, so
// the parser (which rejects placeholders in table position) sees plain
// identifiers and every byte offset stays valid for the original text.
func mask(q string) (string, map[int]placeholder) {
	ph := map[int]placeholder{}
	idx := paramRe.FindAllStringSubmatchIndex(q, -1)
	if len(idx) == 0 {
		return q, ph
	}
	b := []byte(q)
	for _, m := range idx {
		ph[m[0]] = placeholder{name: q[m[2]:m[3]], typ: strings.TrimSpace(q[m[4]:m[5]])}
		b[m[0]] = 'p'
		for i := m[0] + 1; i < m[1]; i++ {
			b[i] = '_'
		}
	}
	return string(b), ph
}

type rewriter struct {
	cfg    *Config
	src    string // original text
	ph     map[int]placeholder
	params map[string]string
	db     string // default database
	edits  []edit
	rules  map[string]int
	left   int
	bad    error
}

type edit struct {
	start, end int
	text       string
}

// Rewrite rewrites one statement. params are the request's query
// parameters (param_<name> without the prefix), db its default database.
func (c *Config) Rewrite(q string, params map[string]string, db string) Result {
	masked, ph := mask(q)
	stmts, err := chp.NewParser(masked).ParseStmts()
	if err != nil {
		return Result{Reason: "parse", Err: firstLine(err.Error())}
	}
	if db == "" {
		db = "default"
	}
	r := &rewriter{cfg: c, src: q, ph: ph, params: params, db: db, rules: map[string]int{}}
	targets := 0
	for _, s := range stmts {
		chp.Walk(s, func(n chp.Expr) bool {
			if sq, ok := n.(*chp.SelectQuery); ok && r.isTarget(sq) {
				targets++
				r.scope(sq)
			}
			return true
		})
	}
	if r.bad != nil {
		return Result{Reason: "span", Err: r.bad.Error()}
	}
	if targets == 0 {
		return Result{Reason: "no-target"}
	}
	if len(r.edits) == 0 {
		reason := "no-ref"
		if r.left > 0 {
			reason = "unhandled"
		}
		return Result{Reason: reason, Left: r.left}
	}
	out, err := apply(q, r.edits)
	if err != nil {
		return Result{Reason: "span", Err: err.Error(), Left: r.left}
	}
	// the result must still parse; otherwise send the original
	m2, _ := mask(out)
	if _, err := chp.NewParser(m2).ParseStmts(); err != nil {
		return Result{Reason: "reparse", Err: firstLine(err.Error()), Left: r.left}
	}
	return Result{SQL: out, Rewritten: true, Reason: "ok", Rules: r.rules, Left: r.left}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func apply(q string, edits []edit) (string, error) {
	sort.Slice(edits, func(i, j int) bool { return edits[i].start < edits[j].start })
	var b strings.Builder
	pos := 0
	for _, e := range edits {
		if e.start < pos || e.end < e.start || e.end > len(q) {
			return "", fmt.Errorf("overlapping or invalid edit [%d,%d)", e.start, e.end)
		}
		b.WriteString(q[pos:e.start])
		b.WriteString(e.text)
		pos = e.end
	}
	b.WriteString(q[pos:])
	return b.String(), nil
}

// identText is an identifier's value: its name, or, for a masked
// {name:Identifier} placeholder, the parameter's value.
func (r *rewriter) identText(id *chp.Ident) (string, bool) {
	if p, ok := r.ph[int(id.NamePos)]; ok {
		v, ok := r.params[p.name]
		return v, ok
	}
	return id.Name, true
}

// isTarget: a SELECT whose FROM is exactly one configured table.
func (r *rewriter) isTarget(sq *chp.SelectQuery) bool {
	if sq.From == nil {
		return false
	}
	jt, ok := sq.From.Expr.(*chp.JoinTableExpr)
	if !ok || jt.Table == nil {
		return false
	}
	ti, ok := jt.Table.Expr.(*chp.TableIdentifier)
	if !ok || ti.Table == nil {
		return false
	}
	t, ok := r.identText(ti.Table)
	if !ok {
		return false
	}
	d := r.db
	if ti.Database != nil {
		if d, ok = r.identText(ti.Database); !ok {
			return false
		}
	}
	if !r.cfg.tables[d+"."+t] {
		return false
	}
	// an alias named like the column would shadow it: leave the scope alone
	shadow := false
	for _, it := range sq.SelectItems {
		if it.Alias != nil && it.Alias.Name == r.cfg.Column {
			shadow = true
		}
	}
	if sq.With != nil {
		for _, cte := range sq.With.CTEs {
			if id, ok := cte.Alias.(*chp.Ident); ok && id.Name == r.cfg.Column {
				shadow = true
			}
		}
	}
	return !shadow
}

// scope collects the edits of one target SELECT: its own clauses, not
// nested SELECTs (they are scopes of their own, handled by the outer walk).
func (r *rewriter) scope(sq *chp.SelectQuery) {
	for _, it := range sq.SelectItems {
		if it.Alias == nil {
			// the result column is named after the expression: rewrite only a
			// bare access, and keep its name
			if k, ok := r.access(it.Expr); ok && k.lit {
				if v, ok := r.value(k); ok {
					name := fmt.Sprintf("arrayElement(%s, %s)", r.cfg.Column, quote(k.key))
					r.add(k.start, k.end, v+" AS "+backquote(name), "select-value-named")
					continue
				}
			}
			r.left += r.countRefs(it.Expr)
			continue
		}
		r.walk(it.Expr)
	}
	for _, n := range []chp.Expr{sq.With, sq.Prewhere, sq.Where, sq.GroupBy, sq.Having, sq.OrderBy, sq.LimitBy, sq.Window} {
		if n != nil && !isNil(n) {
			r.walk(n)
		}
	}
}

func isNil(n chp.Expr) bool {
	switch v := n.(type) {
	case *chp.WithClause:
		return v == nil
	case *chp.PrewhereClause:
		return v == nil
	case *chp.WhereClause:
		return v == nil
	case *chp.GroupByClause:
		return v == nil
	case *chp.HavingClause:
		return v == nil
	case *chp.OrderByClause:
		return v == nil
	case *chp.LimitByClause:
		return v == nil
	case *chp.WindowClause:
		return v == nil
	}
	return false
}

// walk visits root's nodes, skipping nested SELECTs (scopes of their own)
// and nodes inside a span already rewritten. (chp.Walk aborts the whole walk
// when the callback returns false, so it always continues here.)
func (r *rewriter) walk(root chp.Expr) {
	type rng struct{ s, e int }
	var skip []rng
	inSkip := func(p int) bool {
		for _, x := range skip {
			if p >= x.s && p <= x.e {
				return true
			}
		}
		return false
	}
	chp.Walk(root, func(n chp.Expr) bool {
		if inSkip(int(n.Pos())) {
			return true
		}
		claim := func(s, e int) { skip = append(skip, rng{s, e}) }
		switch v := n.(type) {
		case *chp.SelectQuery, *chp.SubQuery:
			claim(int(n.Pos()), int(n.End()))
			return true
		case *chp.BinaryOperation:
			if s, e, ok := r.predicate(v); ok {
				claim(s, e)
				return true
			}
		case *chp.FunctionExpr:
			if s, e, ok := r.function(v); ok {
				claim(s, e)
				return true
			}
		}
		if k, ok := r.access(n); ok {
			if v, ok := r.value(k); ok {
				r.add(k.start, k.end, v, "value")
			} else {
				r.left++
			}
			claim(int(n.Pos()), k.end)
			return true
		}
		if id, ok := n.(*chp.Ident); ok && id.Name == r.cfg.Column {
			r.left++ // the whole map: the ALIAS column builds it
		}
		return true
	})
}

func (r *rewriter) countRefs(root chp.Expr) int {
	n := 0
	chp.Walk(root, func(x chp.Expr) bool {
		if id, ok := x.(*chp.Ident); ok && id.Name == r.cfg.Column {
			n++
		}
		if _, ok := r.materialized(x); ok {
			n++
		}
		return true
	})
	return n
}

func (r *rewriter) add(start, end int, text, rule string) {
	r.edits = append(r.edits, edit{start, end, text})
	r.rules[rule]++
}

// ---- recognising the access ResourceAttributes['k'] -----------------------

type access struct {
	key        string // the key's value
	keyText    string // the key as written ('k' or {p:String})
	lit        bool   // the key is a literal
	start, end int
}

func (r *rewriter) access(n chp.Expr) (access, bool) {
	if ce, ok := n.(*chp.ColumnExpr); ok && ce.Alias == nil {
		n = ce.Expr
	}
	if k, ok := r.materialized(n); ok {
		return k, true
	}
	op, ok := n.(*chp.ObjectParams)
	if !ok {
		return access{}, false
	}
	id, ok := op.Object.(*chp.Ident)
	if !ok || id.Name != r.cfg.Column || r.ph[int(id.NamePos)] != (placeholder{}) {
		return access{}, false
	}
	ap := op.Params
	if ap == nil || ap.Items == nil || len(ap.Items.Items) != 1 {
		return access{}, false
	}
	key, keyText, lit, ok := r.stringConst(ap.Items.Items[0])
	if !ok {
		return access{}, false
	}
	s, ok1 := r.start(id)
	e := int(ap.RightBracketPos)
	if !ok1 || e >= len(r.src) || r.src[e] != ']' {
		r.bad = fmt.Errorf("span of %s[...] at %d", r.cfg.Column, id.NamePos)
		return access{}, false
	}
	return access{key: key, keyText: keyText, lit: lit, start: s, end: e + 1}, true
}

// materialized: a column that stands for a resource key (Config.Materialized).
func (r *rewriter) materialized(n chp.Expr) (access, bool) {
	if len(r.cfg.Materialized) == 0 {
		return access{}, false
	}
	var s, e int
	switch v := n.(type) {
	case *chp.Ident:
		var ok bool
		if s, ok = r.start(v); !ok {
			return access{}, false
		}
		e, _ = r.end(v)
		key, ok := r.cfg.Materialized[v.Name]
		if !ok {
			return access{}, false
		}
		return access{key: key, keyText: quote(key), lit: true, start: s, end: e}, true
	case *chp.Path, *chp.NestedIdentifier, *chp.IndexOperation:
		s, e = int(n.Pos()), int(n.End())
		if s < 0 || e > len(r.src) || s >= e {
			return access{}, false
		}
		key, ok := r.cfg.Materialized[r.src[s:e]]
		if !ok {
			return access{}, false
		}
		return access{key: key, keyText: quote(key), lit: true, start: s, end: e}, true
	}
	return access{}, false
}

// stringConst: a string literal or a {p:String} parameter.
func (r *rewriter) stringConst(n chp.Expr) (val, text string, lit, ok bool) {
	if ce, isCE := n.(*chp.ColumnExpr); isCE && ce.Alias == nil {
		n = ce.Expr
	}
	switch v := n.(type) {
	case *chp.StringLiteral:
		s, e := int(v.LiteralPos)-1, int(v.LiteralEnd)+1
		if s < 0 || e > len(r.src) || r.src[s] != '\'' || r.src[e-1] != '\'' {
			r.bad = fmt.Errorf("span of string literal at %d", v.LiteralPos)
			return "", "", false, false
		}
		return unescape(r.src[s+1 : e-1]), r.src[s:e], true, true
	case *chp.Ident:
		p, isPH := r.ph[int(v.NamePos)]
		if !isPH || !strings.EqualFold(stripLC(p.typ), "String") {
			return "", "", false, false
		}
		pv, has := r.params[p.name]
		if !has {
			return "", "", false, false
		}
		return pv, r.src[v.NamePos:v.NameEnd], false, true
	}
	return "", "", false, false
}

func stripLC(t string) string {
	t = strings.TrimSpace(t)
	if strings.HasPrefix(t, "LowCardinality(") && strings.HasSuffix(t, ")") {
		return strings.TrimSpace(t[len("LowCardinality(") : len(t)-1])
	}
	return t
}

// unescape a ClickHouse string literal body: backslash escapes and ”.
func unescape(s string) string {
	if !strings.ContainsAny(s, `\'`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '0':
				b.WriteByte(0)
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			default:
				b.WriteByte(s[i])
			}
			continue
		}
		if c == '\'' && i+1 < len(s) && s[i+1] == '\'' {
			i++
		}
		b.WriteByte(c)
	}
	return b.String()
}

func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

func backquote(s string) string {
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(s) + "`"
}

// ---- spans ----------------------------------------------------------------

// The parser's End() of a delimited node points at its closing delimiter;
// start/end give the half-open byte range of the node's own text.
func (r *rewriter) start(n chp.Expr) (int, bool) {
	switch v := n.(type) {
	case *chp.Ident:
		if v.QuoteType == chp.BackTicks || v.QuoteType == chp.DoubleQuote {
			return int(v.NamePos) - 1, int(v.NamePos) >= 1
		}
		return int(v.NamePos), true
	case *chp.StringLiteral:
		return int(v.LiteralPos) - 1, int(v.LiteralPos) >= 1
	case *chp.ColumnExpr:
		return r.start(v.Expr)
	case *chp.BinaryOperation:
		return r.start(v.LeftExpr)
	case *chp.ObjectParams:
		return r.start(v.Object)
	case *chp.FunctionExpr:
		return r.start(v.Name)
	case *chp.ParamExprList:
		return int(v.LeftParenPos), true
	case *chp.NumberLiteral:
		return int(v.NumPos), true
	case *chp.Path:
		return int(v.Pos()), true
	}
	return 0, false
}

func (r *rewriter) end(n chp.Expr) (int, bool) {
	closing := func(pos int, c byte) (int, bool) {
		if pos >= 0 && pos < len(r.src) && r.src[pos] == c {
			return pos + 1, true
		}
		return 0, false
	}
	switch v := n.(type) {
	case *chp.Ident:
		switch v.QuoteType {
		case chp.BackTicks:
			return closing(int(v.NameEnd), '`')
		case chp.DoubleQuote:
			return closing(int(v.NameEnd), '"')
		}
		return int(v.NameEnd), true
	case *chp.StringLiteral:
		return closing(int(v.LiteralEnd), '\'')
	case *chp.ColumnExpr:
		return r.end(v.Expr)
	case *chp.BinaryOperation:
		return r.end(v.RightExpr)
	case *chp.ObjectParams:
		if v.Params != nil {
			return closing(int(v.Params.RightBracketPos), ']')
		}
	case *chp.FunctionExpr:
		if v.Params != nil {
			return closing(int(v.Params.RightParenPos), ')')
		}
	case *chp.ParamExprList:
		return closing(int(v.RightParenPos), ')')
	case *chp.NumberLiteral:
		return int(v.NumEnd), true
	case *chp.Path:
		return int(v.End()), true
	}
	return 0, false
}

func (r *rewriter) span(n chp.Expr) (int, int, bool) {
	s, ok1 := r.start(n)
	e, ok2 := r.end(n)
	if !ok1 || !ok2 || s > e {
		return 0, 0, false
	}
	return s, e, true
}

// ---- rules ----------------------------------------------------------------

var cmpOps = map[string]string{
	"=": "=", "==": "=", "!=": "!=", "<>": "!=",
	"LIKE": "LIKE", "ILIKE": "ILIKE", "NOT LIKE": "NOT LIKE", "NOT ILIKE": "NOT ILIKE",
	"IN": "IN", "NOT IN": "NOT IN",
}

// predicate: ResourceAttributes['k'] OP constant.
func (r *rewriter) predicate(b *chp.BinaryOperation) (int, int, bool) {
	op := strings.ToUpper(string(b.Operation))
	if b.HasNot && op == "IN" {
		op = "NOT IN"
	}
	norm, ok := cmpOps[op]
	if !ok {
		return 0, 0, false
	}
	lhs, rhs := b.LeftExpr, b.RightExpr
	k, ok := r.access(lhs)
	if !ok && (norm == "=" || norm == "!=") {
		lhs, rhs = rhs, lhs
		k, ok = r.access(lhs)
	}
	if !ok {
		return 0, 0, false
	}
	vals, ok := r.constList(rhs, norm == "IN" || norm == "NOT IN")
	if !ok {
		return 0, 0, false
	}
	rs, re, ok := r.span(rhs)
	if !ok {
		return 0, 0, false
	}
	s, e, ok := r.span(b)
	if !ok {
		return 0, 0, false
	}
	if lhs != b.LeftExpr { // reversed: the span runs from the constant to the access
		s, _ = r.start(b.LeftExpr)
		e = k.end
	}
	text, rule, ok := r.filter(k, norm, vals, r.src[rs:re])
	if !ok {
		return 0, 0, false
	}
	r.add(s, e, "("+text+")", rule)
	return s, e, true
}

// filter: the rewrite of `ResourceAttributes[k] norm rhsText`, whose
// right-hand side has the string values vals.
func (r *rewriter) filter(k access, norm string, vals []string, rhsText string) (string, string, bool) {
	empty, ok := emptyMatches(norm, vals)
	if !ok {
		return "", "", false
	}
	cond := func(x string) string { return x + " " + norm + " " + rhsText }
	res := r.cfg.Residual
	var cat string
	if !empty {
		cat = fmt.Sprintf("%s IN (SELECT resource_id FROM %s WHERE Key = %s AND %s)", r.cfg.ResourceID, r.cfg.KV, k.keyText, cond("Value"))
	} else {
		cat = fmt.Sprintf("%s NOT IN (SELECT resource_id FROM %s WHERE Key = %s AND NOT (%s))", r.cfg.ResourceID, r.cfg.KV, k.keyText, cond("Value"))
	}
	resid := cond(fmt.Sprintf("%s[%s]", res, k.keyText))
	rule := "filter-" + strings.ToLower(strings.ReplaceAll(norm, " ", "-"))
	switch {
	case r.cfg.Mode == "exact":
		return fmt.Sprintf("if(mapContains(%s, %s), %s, %s)", res, k.keyText, resid, cat), rule, true
	case k.lit && !r.cfg.isCovered(k.key):
		return resid, "filter-residual", true
	}
	return cat, rule, true
}

// constList: the right-hand side's string values (one, or an IN list).
func (r *rewriter) constList(n chp.Expr, list bool) ([]string, bool) {
	if ce, ok := n.(*chp.ColumnExpr); ok && ce.Alias == nil {
		n = ce.Expr
	}
	if !list {
		v, _, _, ok := r.stringConst(n)
		return []string{v}, ok
	}
	pl, ok := n.(*chp.ParamExprList)
	if !ok || pl.Items == nil || len(pl.Items.Items) == 0 || pl.ColumnArgList != nil {
		return nil, false
	}
	var out []string
	for _, it := range pl.Items.Items {
		v, _, _, ok := r.stringConst(it)
		if !ok {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

// emptyMatches: does ” satisfy `” OP vals`? A row whose resource the
// catalog doesn't know, or whose resource lacks the key, reads ”.
func emptyMatches(op string, vals []string) (bool, bool) {
	likeEmpty := func(p string) bool { return strings.Trim(p, "%") == "" }
	switch op {
	case "=":
		return vals[0] == "", true
	case "!=":
		return vals[0] != "", true
	case "IN", "NOT IN":
		has := false
		for _, v := range vals {
			if v == "" {
				has = true
			}
		}
		return has == (op == "IN"), true
	case "LIKE", "ILIKE":
		return likeEmpty(vals[0]), true
	case "NOT LIKE", "NOT ILIKE":
		return !likeEmpty(vals[0]), true
	}
	return false, false
}

// function: mapContains(ResourceAttributes, k) and indexHint(...).
func (r *rewriter) function(f *chp.FunctionExpr) (int, int, bool) {
	name := strings.ToLower(f.Name.Name)
	if name == "notempty" || name == "empty" {
		// HyperDX's `key:*` (exists) renders notEmpty(Map['k']) = 1
		if f.Params == nil || f.Params.Items == nil || len(f.Params.Items.Items) != 1 {
			return 0, 0, false
		}
		k, ok := r.access(f.Params.Items.Items[0])
		if !ok {
			return 0, 0, false
		}
		norm := map[string]string{"notempty": "!=", "empty": "="}[name]
		text, rule, ok := r.filter(k, norm, []string{""}, "''")
		s, e, ok2 := r.span(f)
		if !ok || !ok2 {
			return 0, 0, false
		}
		r.add(s, e, "("+text+")", rule+"-"+name)
		return s, e, true
	}
	if name != "mapcontains" && name != "indexhint" {
		return 0, 0, false
	}
	if name == "indexhint" {
		// indexHint(x) is 1 for every row; x only guides index analysis, and no
		// index reads the ALIAS
		if r.countRefs(f) == 0 {
			return 0, 0, false
		}
		s, e, ok := r.span(f)
		if !ok {
			return 0, 0, false
		}
		r.add(s, e, "1", "indexhint")
		return s, e, true
	}
	if f.Params == nil || f.Params.Items == nil || len(f.Params.Items.Items) != 2 {
		return 0, 0, false
	}
	a0 := f.Params.Items.Items[0]
	if ce, ok := a0.(*chp.ColumnExpr); ok && ce.Alias == nil {
		a0 = ce.Expr
	}
	id, ok := a0.(*chp.Ident)
	if !ok || id.Name != r.cfg.Column {
		return 0, 0, false
	}
	key, keyText, lit, ok := r.stringConst(f.Params.Items.Items[1])
	if !ok {
		return 0, 0, false
	}
	s, e, ok := r.span(f)
	if !ok {
		return 0, 0, false
	}
	cat := fmt.Sprintf("%s IN (SELECT resource_id FROM %s WHERE Key = %s)", r.cfg.ResourceID, r.cfg.KV, keyText)
	resid := fmt.Sprintf("mapContains(%s, %s)", r.cfg.Residual, keyText)
	var text, rule string
	switch {
	case r.cfg.Mode == "exact":
		text, rule = resid+" OR "+cat, "exists"
	case lit && !r.cfg.isCovered(key):
		text, rule = resid, "exists-residual"
	default:
		text, rule = cat, "exists"
	}
	r.add(s, e, "("+text+")", rule)
	return s, e, true
}

// value: the expression for ResourceAttributes['k'] as a value (select,
// group by, order by, a comparison that isn't a constant filter).
func (r *rewriter) value(k access) (string, bool) {
	res := r.cfg.Residual
	if expr, ok := r.cfg.Values[k.key]; ok && k.lit {
		cat := strings.ReplaceAll(expr, "{rid}", r.cfg.ResourceID)
		return fmt.Sprintf("if(mapContains(%s, %s), %s[%s], %s)", res, k.keyText, res, k.keyText, cat), true
	}
	if k.lit && !r.cfg.isCovered(k.key) {
		return fmt.Sprintf("%s[%s]", res, k.keyText), true
	}
	return "", false
}
