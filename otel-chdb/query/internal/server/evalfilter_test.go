package server

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
)

// A fake central that holds rows and applies the additional_table_filters
// it is given, by evaluating the filter's tree (the subset the service
// builds: AND, OR, =, <, >=, IN, parentheses, string and number literals,
// fromUnixTimestamp64Nano(toInt64(n), 'UTC'), column names). So the
// answers depend on the filters exactly, and a basis test sees what
// ClickHouse would read.

type row struct {
	Cluster, Namespace string
	TsNs, RecvNs       int64
}

func (r row) col(name string) any {
	switch name {
	case "__hdx_materialized_k8s.cluster.name":
		return r.Cluster
	case "__hdx_materialized_k8s.namespace.name":
		return r.Namespace
	case "Timestamp":
		return r.TsNs
	case "received_at":
		return r.RecvNs
	}
	panic("unknown column " + name)
}

type rowsCH struct {
	mu    sync.Mutex
	rows  []row
	calls []url.Values
	sqls  []string
}

func (f *rowsCH) add(r ...row) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, r...)
}

var filterRE = regexp.MustCompile(`'((?:[^'\\]|\\.)*)':'((?:[^'\\]|\\.)*)'`)

func unq(s string) string { return strings.NewReplacer(`\\`, `\`, `\'`, `'`).Replace(s) }

var intervalRE = regexp.MustCompile(`toIntervalNanosecond\((\d+)\)`)

func (f *rowsCH) Query(ctx context.Context, sql string, settings url.Values) ([]byte, central.Summary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, settings)
	f.sqls = append(f.sqls, sql)
	var filter chp.Expr
	for _, m := range filterRE.FindAllStringSubmatch(settings.Get("additional_table_filters"), -1) {
		if unq(m[1]) != "otel.otel_logs" {
			continue
		}
		st, err := chp.NewParser("SELECT 1 WHERE " + unq(m[2])).ParseStmts()
		if err != nil {
			return nil, central.Summary{}, fmt.Errorf("fake: filter does not parse: %w", err)
		}
		filter = st[0].(*chp.SelectQuery).Where.Expr
	}
	n := 0
	late := int64(-1)
	if m := intervalRE.FindStringSubmatch(sql); m != nil {
		late, _ = strconv.ParseInt(m[1], 10, 64)
	}
	for _, r := range f.rows {
		if filter != nil && !eval(filter, r).(bool) {
			continue
		}
		if late >= 0 && r.RecvNs <= r.TsNs+late {
			continue
		}
		n++
	}
	if strings.Contains(sql, "AS t, count() AS n") {
		return []byte(fmt.Sprintf(`{"data":[{"t":"otel.otel_logs","n":"%d"}],"rows":1}`, n)), central.Summary{}, nil
	}
	return []byte(fmt.Sprintf(`{"meta":[{"name":"c","type":"UInt64"}],"data":[{"c":"%d"}],"rows":1}`, n)), central.Summary{ReadRows: int64(len(f.rows))}, nil
}

func eval(e chp.Expr, r row) any {
	switch x := e.(type) {
	case *chp.BinaryOperation:
		switch strings.ToUpper(string(x.Operation)) {
		case "AND":
			return eval(x.LeftExpr, r).(bool) && eval(x.RightExpr, r).(bool)
		case "OR":
			return eval(x.LeftExpr, r).(bool) || eval(x.RightExpr, r).(bool)
		case "=":
			return eval(x.LeftExpr, r) == eval(x.RightExpr, r)
		case "<":
			return eval(x.LeftExpr, r).(int64) < eval(x.RightExpr, r).(int64)
		case "<=":
			return eval(x.LeftExpr, r).(int64) <= eval(x.RightExpr, r).(int64)
		case ">=":
			return eval(x.LeftExpr, r).(int64) >= eval(x.RightExpr, r).(int64)
		case "IN":
			l := eval(x.LeftExpr, r)
			for _, v := range eval(x.RightExpr, r).([]any) {
				if v == l {
					return true
				}
			}
			return false
		}
		panic("fake: operator " + string(x.Operation))
	case *chp.ParamExprList:
		var out []any
		for _, it := range x.Items.Items {
			out = append(out, eval(it, r))
		}
		if len(out) == 1 {
			if b, ok := out[0].(bool); ok {
				return b
			}
		}
		return out
	case *chp.ColumnExpr:
		return eval(x.Expr, r)
	case *chp.StringLiteral:
		return unq(x.Literal)
	case *chp.NumberLiteral:
		v, _ := strconv.ParseInt(x.Literal, 10, 64)
		return v
	case *chp.Ident:
		return r.col(x.Name)
	case *chp.FunctionExpr:
		switch x.Name.Name {
		case "fromUnixTimestamp64Nano", "toInt64":
			return eval(x.Params.Items.Items[0], r)
		}
		panic("fake: function " + x.Name.Name)
	}
	panic(fmt.Sprintf("fake: node %T", e))
}
