package sqlscope

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
	"pgregory.net/rapid"
)

// D38 / CAST 52: a scope of explicit (cluster, namespace) pairs is their
// union. These properties evaluate what ClickHouse would receive (the
// table filter, the dictionary guard) over every row of a small universe
// and compare it with an oracle computed from the pairs alone.

var (
	uClusters   = []string{"prod-a", "prod-b", "devtools", "stg-a"}
	uNamespaces = []string{"shop", "pay", "dev-alice", "kube-system"}
)

func drawPairs(t *rapid.T) []Pair {
	n := rapid.IntRange(0, 5).Draw(t, "pairs")
	var out []Pair
	for i := 0; i < n; i++ {
		c := rapid.SampledFrom(append([]string{"*"}, uClusters...)).Draw(t, "c")
		ns := rapid.SampledFrom(append([]string{"*"}, uNamespaces...)).Draw(t, "n")
		out = append(out, Pair{c, ns})
	}
	return out
}

// oracle: some pair covers (c, n).
func unionAllows(pairs []Pair, c, n string) bool {
	for _, p := range pairs {
		if (p.Cluster == "*" || p.Cluster == c) && (p.Namespace == "*" || p.Namespace == n) {
			return true
		}
	}
	return false
}

// productAllows is the old rule (the mutant): the clusters of all pairs ×
// the namespaces of all pairs.
func productAllows(pairs []Pair, c, n string) bool {
	cOK, nOK := false, false
	for _, p := range pairs {
		cOK = cOK || p.Cluster == "*" || p.Cluster == c
		nOK = nOK || p.Namespace == "*" || p.Namespace == n
	}
	return cOK && nOK
}

// evalExpr evaluates the subset of expressions the scope builds: AND, OR,
// IN, =, parentheses, string literals; any sub-expression whose text is a
// key of cols reads that value (a column, or a dictionary template).
func evalExpr(e chp.Expr, cols map[string]string) any {
	if v, ok := cols[chp.Format(e)]; ok {
		return v
	}
	switch x := e.(type) {
	case *chp.BinaryOperation:
		switch strings.ToUpper(string(x.Operation)) {
		case "AND":
			return evalExpr(x.LeftExpr, cols).(bool) && evalExpr(x.RightExpr, cols).(bool)
		case "OR":
			return evalExpr(x.LeftExpr, cols).(bool) || evalExpr(x.RightExpr, cols).(bool)
		case "=":
			return evalExpr(x.LeftExpr, cols) == evalExpr(x.RightExpr, cols)
		case "IN":
			l := evalExpr(x.LeftExpr, cols)
			r := evalExpr(x.RightExpr, cols)
			items, ok := r.([]any)
			if !ok {
				items = []any{r}
			}
			for _, v := range items {
				if v == l {
					return true
				}
			}
			return false
		}
	case *chp.ParamExprList:
		var out []any
		for _, it := range x.Items.Items {
			out = append(out, evalExpr(it, cols))
		}
		if len(out) == 1 {
			if b, ok := out[0].(bool); ok {
				return b
			}
		}
		return out
	case *chp.ColumnExpr:
		return evalExpr(x.Expr, cols)
	case *chp.StringLiteral:
		return x.Literal
	}
	panic(fmt.Sprintf("evalExpr: %T %s", e, chp.Format(e)))
}

type fataler interface {
	Fatal(...any)
	Fatalf(string, ...any)
}

func pairsPolicy(t fataler) *Policy {
	p, err := NewPolicy("otel", []*Table{{Name: "t", Scope: "columns", Cluster: "c", Namespace: "n"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// filterAllows runs Finish and evaluates t's filter on (c, n); a refusal
// (empty scope) allows nothing.
func filterAllows(t fataler, p *Policy, s Scope) (func(c, n string) bool, error) {
	pr, err := p.Prepare("SELECT count() FROM t")
	if err != nil {
		t.Fatal(err)
	}
	res, err := pr.Finish(s)
	if err != nil {
		return nil, err
	}
	text, ok := res.Filters["otel.t"]
	if !ok {
		return func(string, string) bool { return true }, nil
	}
	st, err := chp.NewParser("SELECT 1 WHERE " + text).ParseStmts()
	if err != nil {
		t.Fatalf("filter %q: %v", text, err)
	}
	w := st[0].(*chp.SelectQuery).Where.Expr
	return func(c, n string) bool { return evalExpr(w, map[string]string{"c": c, "n": n}).(bool) }, nil
}

// TestPairScopeIsUnionNeverProduct: over random pair sets, the filter
// ClickHouse would receive admits exactly the union's rows.
func TestPairScopeIsUnionNeverProduct(t *testing.T) {
	p := pairsPolicy(t)
	rapid.Check(t, func(t *rapid.T) {
		pairs := drawPairs(t)
		s := PairScope(pairs)
		allows, err := filterAllows(t, p, s)
		if len(pairs) == 0 {
			if r, ok := AsRejection(err); !ok || r.Reason != "empty_scope" {
				t.Fatalf("no pair: want empty_scope, got %v", err)
			}
			return
		}
		if err != nil {
			t.Fatalf("%v: %v", pairs, err)
		}
		for _, c := range append(uClusters, "other") {
			for _, n := range append(uNamespaces, "other") {
				want := unionAllows(pairs, c, n)
				if got := allows(c, n); got != want {
					t.Fatalf("pairs %v: row (%s, %s) admitted=%v, the union says %v", pairs, c, n, got, want)
				}
				if s.Allows(c, n) != want {
					t.Fatalf("Scope.Allows(%s, %s) disagrees", c, n)
				}
			}
		}
		// narrowing to clusters keeps exactly the union's rows of those clusters
		keep := rapid.SliceOfNDistinct(rapid.SampledFrom(uClusters), 1, 3, rapid.ID[string]).Draw(t, "narrow")
		nr, err := filterAllows(t, p, s.NarrowClusters(keep))
		for _, c := range uClusters {
			for _, n := range uNamespaces {
				want := unionAllows(pairs, c, n) && contains(keep, c)
				got := err == nil && nr(c, n)
				if got != want {
					t.Fatalf("pairs %v narrowed to %v: (%s, %s) admitted=%v, want %v (%v)", pairs, keep, c, n, got, want, err)
				}
			}
		}
		// the fleet-table gate: only (*, *) is unrestricted
		if s.Unrestricted() != unionAllows(pairs, "other", "other") {
			t.Fatalf("Unrestricted %v for %v", s.Unrestricted(), pairs)
		}
	})
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// TestProductMutantCaught: the old rule (clusters of all grants × their
// namespaces) is caught by the same oracle: two grants (a, x), (b, y) admit
// (a, y) under it and not under the union, and the drawn scenarios find
// such a case.
func TestProductMutantCaught(t *testing.T) {
	two := []Pair{{"prod-a", "shop"}, {"devtools", "dev-alice"}}
	if !productAllows(two, "prod-a", "dev-alice") || unionAllows(two, "prod-a", "dev-alice") {
		t.Fatal("the fixed counterexample does not separate the rules")
	}
	p := pairsPolicy(t)
	allows, err := filterAllows(t, p, PairScope(two))
	if err != nil {
		t.Fatal(err)
	}
	if allows("prod-a", "dev-alice") || allows("devtools", "shop") || !allows("prod-a", "shop") || !allows("devtools", "dev-alice") {
		t.Fatal("the built filter is the product")
	}
	// the product as a Scope (the old server's projection into one grant)
	old := Scope{Clusters: []string{"devtools", "prod-a"}, Namespaces: []string{"dev-alice", "shop"}}
	oldAllows, err := filterAllows(t, p, old)
	if err != nil || !oldAllows("prod-a", "dev-alice") {
		t.Fatal("the mutant should admit the crossed row")
	}
	gen := rapid.Custom(drawPairs)
	found := false
	for seed := 0; seed < 2000 && !found; seed++ {
		pairs := gen.Example(seed)
		for _, c := range uClusters {
			for _, n := range uNamespaces {
				if productAllows(pairs, c, n) != unionAllows(pairs, c, n) {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("the generator never separates the product from the union: the property would not catch the mutant")
	}
}

// TestPairFilterText pins the shapes: one grant keeps the plain IN terms;
// several are an OR grouped by namespace set.
func TestPairFilterText(t *testing.T) {
	p := pairsPolicy(t)
	for _, tc := range []struct {
		pairs []Pair
		want  string
	}{
		{[]Pair{{"prod-a", "*"}, {"prod-b", "*"}}, "c IN ('prod-a', 'prod-b')"},
		{[]Pair{{"prod-a", "shop"}, {"prod-b", "shop"}}, "c IN ('prod-a', 'prod-b') AND n IN ('shop')"},
		{[]Pair{{"prod-a", "shop"}, {"devtools", "dev-alice"}}, "((c IN ('devtools') AND n IN ('dev-alice')) OR (c IN ('prod-a') AND n IN ('shop')))"},
		{[]Pair{{"*", "dev-alice"}, {"prod-a", "*"}}, "((c IN ('prod-a')) OR (n IN ('dev-alice')))"},
		{[]Pair{{"prod-a", "shop"}, {"prod-a", "*"}}, "c IN ('prod-a')"},
	} {
		pr, _ := p.Prepare("SELECT count() FROM t")
		res, err := pr.Finish(PairScope(tc.pairs))
		if err != nil || res.Filters["otel.t"] != tc.want {
			t.Errorf("%v: %q %v, want %q", tc.pairs, res.Filters["otel.t"], err, tc.want)
		}
	}
	// a scope needing a namespace term on a table without a namespace column
	np, _ := NewPolicy("otel", []*Table{{Name: "t", Scope: "columns", Cluster: "c"}}, 0)
	pr, _ := np.Prepare("SELECT count() FROM t")
	if _, err := pr.Finish(PairScope([]Pair{{"prod-a", "*"}, {"prod-b", "shop"}})); err == nil {
		t.Fatal("a namespace pair on a table without a namespace column must be refused")
	} else if r, _ := AsRejection(err); r.Reason != "scope_unenforceable" {
		t.Fatal(err)
	}
	if _, err := pr.Finish(PairScope([]Pair{{"prod-a", "*"}})); err != nil {
		t.Fatalf("whole-cluster pairs need no namespace column: %v", err)
	}
	if _, err := pr.Finish(PairScope([]Pair{{"Prod_A", "*"}})); err == nil {
		t.Fatal("a bad cluster name must be refused")
	}
}

// TestPairDictionaryGuard (D33): a dictionary lookup's guard admits a key
// exactly when its root's cluster and namespace are in the union.
func TestPairDictionaryGuard(t *testing.T) {
	p := dictPolicy(t)
	var root *Dictionary
	for _, d := range CatalogDictionaries("rw_cat") {
		if d.root() {
			root = d
		}
	}
	tmpl := func(x string) string {
		e, err := parseExpr(strings.ReplaceAll(x, "{key}", "resource_id"))
		if err != nil {
			t.Fatal(err)
		}
		return chp.Format(e)
	}
	cT, nT := tmpl(root.Cluster), tmpl(root.Namespace)
	rapid.Check(t, func(rt *rapid.T) {
		pairs := drawPairs(rt)
		if len(pairs) == 0 {
			return
		}
		s := PairScope(pairs)
		pr, err := p.Prepare("SELECT dictGet('rw_cat.d_res', 'ct', resource_id) AS v FROM otel_logs")
		if err != nil {
			rt.Fatal(err)
		}
		res, err := pr.Finish(s)
		if err != nil {
			rt.Fatalf("%v: %v", pairs, err)
		}
		st, err := chp.NewParser(res.SQL).ParseStmts()
		if err != nil {
			rt.Fatal(err)
		}
		var guard chp.Expr
		visit(reflect.ValueOf(st[0]), func(n chp.Expr) bool {
			if f, ok := n.(*chp.FunctionExpr); ok && f.Name != nil && f.Name.Name == "if" && guard == nil {
				guard = f.Params.Items.Items[0]
			}
			return guard == nil
		})
		if s.Unrestricted() {
			if guard != nil {
				rt.Fatalf("an unrestricted scope got a guard: %s", res.SQL)
			}
			return
		}
		if guard == nil {
			rt.Fatalf("no guard in %s", res.SQL)
		}
		for _, c := range uClusters {
			for _, n := range uNamespaces {
				got := evalExpr(guard, map[string]string{cT: c, nT: n}).(bool)
				if want := unionAllows(pairs, c, n); got != want {
					rt.Fatalf("pairs %v: key of (%s, %s) guarded=%v, want %v\n%s", pairs, c, n, got, want, chp.Format(guard))
				}
			}
		}
	})
}

// TestScopeKeyDistinguishesPairs: two scopes with equal projections but
// different pairs must not share a cache entry (the catalog's resource ids).
func TestScopeKeyDistinguishesPairs(t *testing.T) {
	a := PairScope([]Pair{{"prod-a", "shop"}, {"devtools", "dev-alice"}})
	b := PairScope([]Pair{{"prod-a", "dev-alice"}, {"devtools", "shop"}})
	if !reflect.DeepEqual(a.Clusters, b.Clusters) || !reflect.DeepEqual(a.Namespaces, b.Namespaces) {
		t.Fatal("the example needs equal projections")
	}
	if a.Key() == b.Key() {
		t.Fatal("equal keys for different pairs")
	}
}
