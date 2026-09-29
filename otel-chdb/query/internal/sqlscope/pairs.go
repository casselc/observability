package sqlscope

import (
	"sort"
	"strings"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Pair is one explicit (cluster, namespace) grant (DECISIONS.md D38,
// CAST 52). "*" in either place is every value of that tier. A scope is the
// UNION of its pairs, never the product of their clusters and namespaces:
// a grant of (a, x) and one of (b, y) do not give (a, y).
type Pair struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
}

// All is the pair of every cluster and namespace.
var All = Pair{"*", "*"}

// PairScope is the scope of exactly pairs: normalised (duplicates and
// pairs another covers dropped, sorted), with Clusters / Namespaces (and
// AllClusters / AllNamespaces) set to their projections. The projections
// are upper bounds for labels, bases and audit; rows are cut by the pairs.
func PairScope(pairs []Pair) Scope {
	ps := normalise(pairs)
	s := Scope{Pairs: ps}
	if ps == nil {
		s.Pairs = []Pair{} // empty, not "not set"
	}
	cs, ns := map[string]bool{}, map[string]bool{}
	for _, p := range ps {
		if p.Cluster == "*" {
			s.AllClusters = true
		} else {
			cs[p.Cluster] = true
		}
		if p.Namespace == "*" {
			s.AllNamespaces = true
		} else {
			ns[p.Namespace] = true
		}
	}
	if !s.AllClusters {
		s.Clusters = sortedKeys(cs)
	}
	if !s.AllNamespaces {
		s.Namespaces = sortedKeys(ns)
	}
	return s
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// normalise drops duplicates and pairs covered by a wildcard pair, and
// sorts. (*, *) alone stays when present.
func normalise(pairs []Pair) []Pair {
	set := map[Pair]bool{}
	for _, p := range pairs {
		// values are kept exactly (never trimmed or dropped here): an
		// invalid name is refused when the predicate is built
		set[p] = true
	}
	if set[All] {
		return []Pair{All}
	}
	var out []Pair
	for p := range set {
		if p.Cluster != "*" && p.Namespace != "*" && (set[Pair{p.Cluster, "*"}] || set[Pair{"*", p.Namespace}]) {
			continue
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cluster != out[j].Cluster {
			return out[i].Cluster < out[j].Cluster
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out
}

// pairs is the scope's grant as pairs: Pairs when set; otherwise the scope
// is one grant, the product of Clusters and Namespaces ("*" for All*).
func (s Scope) pairs() []Pair {
	if s.Pairs != nil {
		return normalise(s.Pairs)
	}
	// every cluster or namespace only through All*: a "*" in a list is a
	// name, and not a valid one (bad_scope_value)
	lit := func(xs []string) []string {
		out := make([]string, len(xs))
		for i, x := range xs {
			if x == "*" {
				x = notAName
			}
			out[i] = x
		}
		return out
	}
	cs, ns := lit(s.Clusters), lit(s.Namespaces)
	if s.AllClusters {
		cs = []string{"*"}
	}
	if s.AllNamespaces {
		ns = []string{"*"}
	}
	var out []Pair
	for _, c := range cs {
		for _, n := range ns {
			out = append(out, Pair{c, n})
		}
	}
	return normalise(out)
}

// notAName stands for a literal "*" in a single-grant scope's lists: it
// matches neither name pattern, so the scope is refused.
const notAName = "\x00*"

// Unrestricted reports whether the scope holds every cluster and namespace
// (the pair (*, *)): only such a caller reads fleet tables, unprojected
// metadata and unguarded dictionaries.
func (s Scope) Unrestricted() bool {
	p := s.pairs()
	return len(p) == 1 && p[0] == All
}

// Allows reports whether a row of cluster c and namespace n is in scope.
func (s Scope) Allows(c, n string) bool {
	for _, p := range s.pairs() {
		if (p.Cluster == "*" || p.Cluster == c) && (p.Namespace == "*" || p.Namespace == n) {
			return true
		}
	}
	return false
}

// Key is a canonical text of the scope's pairs, for caches keyed by scope
// (two scopes with the same projections but different pairs differ).
func (s Scope) Key() string {
	var b strings.Builder
	for _, p := range s.pairs() {
		b.WriteString(p.Cluster)
		b.WriteByte('\x00')
		b.WriteString(p.Namespace)
		b.WriteByte('\x01')
	}
	return b.String()
}

// NarrowClusters is s restricted to clusters cs (a request's `clusters`, or
// a basis's): each pair is kept for the named clusters it covers. The other
// fields (window, basis, resource ids) are kept.
func (s Scope) NarrowClusters(cs []string) Scope {
	want := map[string]bool{}
	for _, c := range cs {
		want[c] = true
	}
	var out []Pair
	for _, p := range s.pairs() {
		if p.Cluster == "*" {
			for c := range want {
				out = append(out, Pair{c, p.Namespace})
			}
		} else if want[p.Cluster] {
			out = append(out, p)
		}
	}
	n := PairScope(out)
	n.ResourceIDs, n.Window, n.Received = s.ResourceIDs, s.Window, s.Received
	return n
}

// scopeTerms is the scope as predicate terms over a row's cluster and
// namespace expressions, to be ANDed with the others. One grant shape
// (every cluster with the same namespaces) gives the plain terms
//
//	cluster IN ('a', 'b') AND namespace IN ('x')
//
// and several give one OR of them, grouped by namespace set:
//
//	((cluster IN ('a') AND namespace IN ('x')) OR (cluster IN ('b')) OR (namespace IN ('y')))
//
// (the last: (*, y)). Nil for an unrestricted scope. A term that needs an
// expression the table does not have is refused (scope_unenforceable).
func scopeTerms(what string, cluster, namespace chp.Expr, s Scope) ([]chp.Expr, error) {
	ps := s.pairs()
	if len(ps) == 0 {
		return nil, reject("empty_scope", "no cluster and namespace in scope")
	}
	if len(ps) == 1 && ps[0] == All {
		return nil, nil
	}
	// cluster → its namespaces; then group clusters by namespace set
	byCluster := map[string][]string{}
	for _, p := range ps {
		if p.Cluster != "*" && !ClusterRE.MatchString(p.Cluster) {
			return nil, reject("bad_scope_value", "cluster %q is not a valid name", p.Cluster)
		}
		if p.Namespace != "*" && !NamespaceRE.MatchString(p.Namespace) {
			return nil, reject("bad_scope_value", "namespace %q is not a valid name", p.Namespace)
		}
		byCluster[p.Cluster] = append(byCluster[p.Cluster], p.Namespace)
	}
	type group struct {
		nss      []string
		clusters []string
	}
	groups := map[string]*group{}
	var order []string
	for c, nss := range byCluster {
		sort.Strings(nss)
		k := strings.Join(nss, ",")
		g, ok := groups[k]
		if !ok {
			g = &group{nss: nss}
			groups[k] = g
			order = append(order, k)
		}
		g.clusters = append(g.clusters, c)
	}
	sort.Strings(order)
	var terms [][]chp.Expr
	for _, k := range order {
		g := groups[k]
		sort.Strings(g.clusters)
		var parts []chp.Expr
		if !(len(g.clusters) == 1 && g.clusters[0] == "*") {
			if cluster == nil {
				return nil, reject("scope_unenforceable", "%s has no cluster column", what)
			}
			parts = append(parts, in(cluster, lits(g.clusters)))
		}
		if !(len(g.nss) == 1 && g.nss[0] == "*") {
			if namespace == nil {
				return nil, reject("scope_unenforceable", "%s has no namespace column", what)
			}
			parts = append(parts, in(namespace, lits(g.nss)))
		}
		terms = append(terms, parts)
	}
	if len(terms) == 1 {
		return terms[0], nil
	}
	var out chp.Expr
	for _, parts := range terms {
		t := parts[0]
		for _, p := range parts[1:] {
			t = and(t, p)
		}
		t = paren(t)
		if out == nil {
			out = t
		} else {
			out = &chp.BinaryOperation{LeftExpr: out, Operation: chp.TokenKind(chp.KeywordOr), RightExpr: t}
		}
	}
	return []chp.Expr{paren(out)}, nil
}

func lits(vals []string) []chp.Expr {
	out := make([]chp.Expr, len(vals))
	for i, v := range vals {
		out[i] = &chp.StringLiteral{Literal: escape(v)}
	}
	return out
}
