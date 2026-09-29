package auth

import (
	"fmt"
	"sort"
	"strings"

	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/golang-jwt/jwt/v5"
)

// Grant is what a group (or a token's own claims) gives: each of Roles over
// each (cluster, namespace) of Clusters × Namespaces, and over each of
// Pairs. "*" in Clusters or Namespaces means all of them. The product is
// WITHIN one grant only: grants combine as the union of their tuples, never
// as the product of their combined lists (D38, CAST 52).
type Grant struct {
	Clusters   []string `json:"clusters"`
	Namespaces []string `json:"namespaces"`
	Pairs      []Pair   `json:"pairs,omitempty"`
	Roles      []string `json:"roles"`
}

// Pair is one (cluster, namespace); "*" is every value of that tier.
type Pair = sqlscope.Pair

// Tuple is one explicit grant: a role over one (cluster, namespace).
type Tuple struct {
	Role      string `json:"role"`
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
}

// tuples expands g.
func (g Grant) tuples() []Tuple {
	var out []Tuple
	for _, r := range g.Roles {
		r = strings.TrimSpace(r)
		for _, c := range g.Clusters {
			for _, n := range g.Namespaces {
				out = append(out, Tuple{r, strings.TrimSpace(c), strings.TrimSpace(n)})
			}
		}
		for _, p := range g.Pairs {
			out = append(out, Tuple{r, strings.TrimSpace(p.Cluster), strings.TrimSpace(p.Namespace)})
		}
	}
	return out
}

// Mapping turns claims into a Principal.
type Mapping struct {
	// Claim names. Empty disables reading that attribute from the token
	// directly (groups only).
	SubjectClaim    string `json:"subject"`
	ClustersClaim   string `json:"clusters"`
	NamespacesClaim string `json:"namespaces"`
	RolesClaim      string `json:"roles"`
	GroupsClaim     string `json:"groups"`
	// Groups maps a group name (from GroupsClaim) to what it grants.
	Groups map[string]Grant `json:"group_grants"`
}

// Roles the service knows. A token with none of them can do nothing.
const (
	RoleQuery = "query" // POST /v1/query (central)
	RolePlan  = "plan"  // POST /v1/plan (lake)
)

// Principal is who is asking and what they may see. Grants is the truth;
// Clusters / Namespaces / All* are its projections over the roles of this
// view (For), for labels, bases, audit and cluster checks. Rows are cut by
// Scope(), which is the grants' pairs, never by the projections.
type Principal struct {
	Subject       string   `json:"sub"`
	Grants        []Tuple  `json:"grants,omitempty"`
	Clusters      []string `json:"clusters,omitempty"`
	AllClusters   bool     `json:"all_clusters,omitempty"`
	Namespaces    []string `json:"namespaces,omitempty"`
	AllNamespaces bool     `json:"all_namespaces,omitempty"`
	Roles         []string `json:"roles"`
	Groups        []string `json:"groups,omitempty"`
}

// Has reports whether the principal holds role r.
func (p *Principal) Has(r string) bool {
	for _, x := range p.Roles {
		if x == r {
			return true
		}
	}
	return false
}

// MayCluster reports whether cluster c is in scope.
func (p *Principal) MayCluster(c string) bool {
	if p.AllClusters {
		return true
	}
	for _, x := range p.Clusters {
		if x == c {
			return true
		}
	}
	return false
}

// Principal maps verified claims. Only names in the known role set count;
// anything else in a roles claim is dropped.
func (m *Mapping) Principal(c jwt.MapClaims) (*Principal, error) {
	subClaim := m.SubjectClaim
	if subClaim == "" {
		subClaim = "sub"
	}
	sub, _ := c[subClaim].(string)
	if sub == "" {
		return nil, fmt.Errorf("%w: no %q claim", ErrUnauthenticated, subClaim)
	}
	var ts []Tuple
	roles := map[string]bool{}
	add := func(g Grant) {
		for _, r := range g.Roles {
			if r = strings.TrimSpace(r); r == RoleQuery || r == RolePlan {
				roles[r] = true
			}
		}
		ts = append(ts, g.tuples()...)
	}
	if m.ClustersClaim != "" || m.NamespacesClaim != "" || m.RolesClaim != "" {
		add(Grant{Clusters: strs(c, m.ClustersClaim), Namespaces: strs(c, m.NamespacesClaim), Roles: strs(c, m.RolesClaim)})
	}
	groups := strs(c, m.GroupsClaim)
	for _, name := range groups {
		if gr, ok := m.Groups[name]; ok {
			add(gr)
		}
	}
	p := &Principal{Subject: sub, Groups: uniq(groups)}
	for _, t := range ts {
		if (t.Role == RoleQuery || t.Role == RolePlan) && t.Cluster != "" && t.Namespace != "" {
			p.Grants = append(p.Grants, t)
		}
	}
	for _, r := range []string{RolePlan, RoleQuery} { // sorted
		if roles[r] {
			p.Roles = append(p.Roles, r)
		}
	}
	p.Grants = normaliseTuples(p.Grants)
	if p.Grants == nil {
		p.Grants = []Tuple{} // built from claims: no grant, not "not set"
	}
	p.project()
	return p, nil
}

// For is the principal's view for roles: the grants of those roles only,
// and their projections. A handler serving role r works on p.For(r), so a
// grant of another role never widens it (a plan grant is not a query scope).
func (p *Principal) For(roles ...string) *Principal {
	v := &Principal{Subject: p.Subject, Groups: p.Groups}
	want := map[string]bool{}
	for _, r := range roles {
		want[r] = true
	}
	for _, r := range p.Roles {
		if want[r] {
			v.Roles = append(v.Roles, r)
		}
	}
	for _, t := range p.grants() {
		if want[t.Role] {
			v.Grants = append(v.Grants, t)
		}
	}
	if v.Grants == nil {
		v.Grants = []Tuple{}
	}
	v.project()
	return v
}

// Pairs is the view's (cluster, namespace) scope: the union of its grants.
func (p *Principal) Pairs() []Pair {
	var out []Pair
	for _, t := range p.grants() {
		out = append(out, Pair{Cluster: t.Cluster, Namespace: t.Namespace})
	}
	return sqlscope.PairScope(out).Pairs
}

// Scope is the view's scope for the SQL rewrite (sqlscope.PairScope).
func (p *Principal) Scope() sqlscope.Scope { return sqlscope.PairScope(p.Pairs()) }

// WholeClusters is the view restricted to its grants of whole clusters
// (namespace "*"): what may be read from raw lane objects, which hold every
// namespace of their cluster (the lake plan; presigning).
func (p *Principal) WholeClusters() *Principal {
	v := &Principal{Subject: p.Subject, Groups: p.Groups, Roles: p.Roles}
	for _, t := range p.grants() {
		if t.Namespace == "*" {
			v.Grants = append(v.Grants, t)
		}
	}
	if v.Grants == nil {
		v.Grants = []Tuple{}
	}
	v.project()
	return v
}

// grants is Grants; for a Principal built as a literal with projections
// only (no Grants), the one grant those describe: each role over the
// product of Clusters and Namespaces.
func (p *Principal) grants() []Tuple {
	if p.Grants != nil {
		return p.Grants
	}
	cs, ns := p.Clusters, p.Namespaces
	if p.AllClusters {
		cs = []string{"*"}
	}
	if p.AllNamespaces {
		ns = []string{"*"}
	}
	return Grant{Clusters: cs, Namespaces: ns, Roles: p.Roles}.tuples()
}

// project sets the projections from Grants.
func (p *Principal) project() {
	sc := sqlscope.PairScope(p.Pairs())
	p.AllClusters, p.Clusters = sc.AllClusters, sc.Clusters
	p.AllNamespaces, p.Namespaces = sc.AllNamespaces, sc.Namespaces
	if len(p.Clusters) == 0 {
		p.Clusters = nil
	}
	if len(p.Namespaces) == 0 {
		p.Namespaces = nil
	}
}

// normaliseTuples drops duplicates and tuples a wildcard tuple of the same
// role covers, and sorts.
func normaliseTuples(ts []Tuple) []Tuple {
	byRole := map[string][]Pair{}
	for _, t := range ts {
		byRole[t.Role] = append(byRole[t.Role], Pair{Cluster: t.Cluster, Namespace: t.Namespace})
	}
	roles := make([]string, 0, len(byRole))
	for r := range byRole {
		roles = append(roles, r)
	}
	sort.Strings(roles)
	var out []Tuple
	for _, r := range roles {
		for _, pr := range sqlscope.PairScope(byRole[r]).Pairs {
			out = append(out, Tuple{Role: r, Cluster: pr.Cluster, Namespace: pr.Namespace})
		}
	}
	return out
}

// strs reads a claim that is a string list, or one string of names
// separated by commas or spaces.
func strs(c jwt.MapClaims, name string) []string {
	if name == "" {
		return nil
	}
	switch v := c[name].(type) {
	case string:
		return strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' })
	case []any:
		var out []string
		for _, x := range v {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func uniq(xs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range xs {
		x = strings.TrimSpace(x)
		if x == "" || seen[x] {
			continue
		}
		seen[x] = true
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}
