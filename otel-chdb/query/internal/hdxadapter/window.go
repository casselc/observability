package hdxadapter

import (
	"math"
	"reflect"
	"strconv"
	"strings"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Window is a half-open event-time range in ns.
type Window struct{ FromNs, ToNs int64 }

// Tables tells the adapter which served tables have a time column: the same
// list as the service's central.tables (db.table -> time column). A table
// the adapter does not know is assumed to have one, so a statement reading
// it gets no window.
type Tables struct {
	DefaultDatabase string
	TimeColumns     map[string]string // "otel.otel_logs" -> "Timestamp"
}

// DeriveWindow returns the window a statement's own time bounds already
// impose, or nil. The service restricts every read of a table with a time
// column to the window it is given, so a window is derived only when that
// changes no row: every SELECT that reads a table directly reads exactly one
// table, that table's time column is bounded in the SELECT's top-level
// WHERE conjuncts (col >= / > / <= / < fromUnixTimestamp64Milli(n)), and
// every such SELECT has the same bounds. Otherwise nil: the label then
// covers everything up to now (partial or unknown), never "complete" for a
// window the rows were not restricted to.
func (t Tables) DeriveWindow(root chp.Expr) *Window {
	ctes := map[string]bool{}
	var selects []*chp.SelectQuery
	walkAll(reflect.ValueOf(root), func(n any) {
		switch x := n.(type) {
		case *chp.SelectQuery:
			selects = append(selects, x)
		case *chp.CTEStmt:
			if id, ok := x.Expr.(*chp.Ident); ok {
				ctes[id.Name] = true
			}
		}
	})
	var w *Window
	for _, sq := range selects {
		var tables []*chp.TableIdentifier
		if sq.From != nil {
			walkNoSubquery(reflect.ValueOf(sq.From), func(ti *chp.TableIdentifier) { tables = append(tables, ti) })
		}
		var data []string
		for _, ti := range tables {
			if ti.Table == nil {
				return nil
			}
			if ti.Database == nil && ctes[ti.Table.Name] {
				continue
			}
			db := t.DefaultDatabase
			if ti.Database != nil {
				db = ti.Database.Name
			}
			if db == "system" {
				continue
			}
			data = append(data, db+"."+ti.Table.Name)
		}
		if len(data) == 0 {
			continue
		}
		if len(data) > 1 {
			return nil
		}
		col, ok := t.TimeColumns[data[0]]
		if !ok || col == "" {
			return nil
		}
		b, ok := bounds(sq, col)
		if !ok {
			return nil
		}
		if w == nil {
			w = &b
		} else if *w != b {
			return nil
		}
	}
	return w
}

// bounds reads [from, to) from sq's top-level WHERE conjuncts on col.
func bounds(sq *chp.SelectQuery, col string) (Window, bool) {
	if sq.Where == nil {
		return Window{}, false
	}
	lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
	var hasLo, hasHi bool
	for _, c := range conjuncts(sq.Where.Expr) {
		b, ok := c.(*chp.BinaryOperation)
		if !ok || b.HasNot || b.HasGlobal {
			continue
		}
		name, ok := columnName(b.LeftExpr)
		if !ok || name != col {
			continue
		}
		ms, ok := millis(b.RightExpr)
		if !ok {
			continue
		}
		if ms > math.MaxInt64/1_000_000-1 || ms < math.MinInt64/1_000_000+1 {
			return Window{}, false
		}
		ns := ms * 1_000_000
		switch b.Operation {
		case chp.TokenKindGE:
			lo, hasLo = max(lo, ns), true
		case chp.TokenKindGT:
			lo, hasLo = max(lo, ns+1), true
		case chp.TokenKindLE:
			hi, hasHi = min(hi, ns+1), true
		case chp.TokenKindLT:
			hi, hasHi = min(hi, ns), true
		}
	}
	if !hasLo || !hasHi || lo >= hi {
		return Window{}, false
	}
	return Window{FromNs: lo, ToNs: hi}, true
}

// conjuncts flattens AND and single-item parentheses.
func conjuncts(e chp.Expr) []chp.Expr {
	switch x := e.(type) {
	case *chp.BinaryOperation:
		if strings.EqualFold(string(x.Operation), chp.KeywordAnd) && !x.HasNot {
			return append(conjuncts(x.LeftExpr), conjuncts(x.RightExpr)...)
		}
	case *chp.ParamExprList:
		if x.Items != nil && len(x.Items.Items) == 1 && x.ColumnArgList == nil {
			return conjuncts(x.Items.Items[0])
		}
	case *chp.ColumnExpr:
		if x.Alias == nil {
			return conjuncts(x.Expr)
		}
	}
	return []chp.Expr{e}
}

func columnName(e chp.Expr) (string, bool) {
	switch x := e.(type) {
	case *chp.Ident:
		return x.Name, true
	case *chp.ColumnExpr:
		if x.Alias == nil {
			return columnName(x.Expr)
		}
	case *chp.ParamExprList:
		if x.Items != nil && len(x.Items.Items) == 1 {
			return columnName(x.Items.Items[0])
		}
	}
	return "", false
}

// millis reads fromUnixTimestamp64Milli(n) or fromUnixTimestamp64Milli(toInt64(n)).
func millis(e chp.Expr) (int64, bool) {
	f, ok := unwrap(e).(*chp.FunctionExpr)
	if !ok || f.Name == nil || !strings.EqualFold(f.Name.Name, "fromUnixTimestamp64Milli") {
		return 0, false
	}
	args := fnArgs(f)
	if len(args) != 1 {
		return 0, false
	}
	a := unwrap(args[0])
	if g, ok := a.(*chp.FunctionExpr); ok && g.Name != nil && strings.EqualFold(g.Name.Name, "toInt64") {
		ga := fnArgs(g)
		if len(ga) != 1 {
			return 0, false
		}
		a = unwrap(ga[0])
	}
	neg := false
	if u, ok := a.(*chp.UnaryExpr); ok && u.Kind == chp.TokenKindMinus {
		neg, a = true, unwrap(u.Expr)
	}
	n, ok := a.(*chp.NumberLiteral)
	if !ok || n.Base != 10 && n.Base != 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(n.Literal, 10, 64)
	if err != nil {
		return 0, false
	}
	if neg {
		v = -v
	}
	return v, true
}

func unwrap(e chp.Expr) chp.Expr {
	for {
		c, ok := e.(*chp.ColumnExpr)
		if !ok || c.Alias != nil {
			return e
		}
		e = c.Expr
	}
}

func fnArgs(f *chp.FunctionExpr) []chp.Expr {
	if f.Params == nil || f.Params.Items == nil || f.Params.ColumnArgList != nil {
		return nil
	}
	return f.Params.Items.Items
}

// walkAll calls fn on every pointer node of the tree.
func walkAll(v reflect.Value, fn func(any)) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		fn(v.Interface())
		walkAll(v.Elem(), fn)
	case reflect.Interface:
		if !v.IsNil() {
			walkAll(v.Elem(), fn)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				walkAll(v.Field(i), fn)
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			walkAll(v.Index(i), fn)
		}
	}
}

// walkNoSubquery calls fn on the table identifiers under v, not descending
// into nested SELECTs (they are scopes of their own).
func walkNoSubquery(v reflect.Value, fn func(*chp.TableIdentifier)) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		switch x := v.Interface().(type) {
		case *chp.SelectQuery:
			return
		case *chp.TableIdentifier:
			fn(x)
			return
		}
		walkNoSubquery(v.Elem(), fn)
	case reflect.Interface:
		if !v.IsNil() {
			walkNoSubquery(v.Elem(), fn)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				walkNoSubquery(v.Field(i), fn)
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			walkNoSubquery(v.Index(i), fn)
		}
	}
}
