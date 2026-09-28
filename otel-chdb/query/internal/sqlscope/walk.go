package sqlscope

import (
	"reflect"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

var exprType = reflect.TypeOf((*chp.Expr)(nil)).Elem()

// visit calls fn for every AST node reachable from v, by reflection over
// every exported field, so that no node kind is missed even where the
// parser's own Walk does not descend (fail closed: a node the service does
// not know how to judge is still seen, and rejected by the caller). fn
// returns false to skip a node's children.
func visit(v reflect.Value, fn func(chp.Expr) bool) {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		visit(v.Elem(), fn)
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		if v.Type().Implements(exprType) {
			if !fn(v.Interface().(chp.Expr)) {
				return
			}
		}
		visit(v.Elem(), fn)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !v.Type().Field(i).IsExported() {
				continue
			}
			visit(v.Field(i), fn)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			visit(v.Index(i), fn)
		}
	}
}

// children visits the nodes below n (not n itself).
func children(n chp.Expr, fn func(chp.Expr) bool) {
	v := reflect.ValueOf(n)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		visit(v.Elem(), fn)
	}
}

func reflectValue(n chp.Expr) reflect.Value { return reflect.ValueOf(n) }
