package auth_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/golang-jwt/jwt/v5"
	"pgregory.net/rapid"
)

// D38 / CAST 52: a principal's effective scope for a role is the union of
// the (role, cluster, namespace) tuples its grants give, never the product
// of their combined cluster, namespace and role lists.

var (
	tClusters   = []string{"prod-a", "prod-b", "devtools"}
	tNamespaces = []string{"shop", "dev-alice", "kube-system"}
	tRoles      = []string{auth.RoleQuery, auth.RolePlan}
)

type scenario struct {
	groups map[string]auth.Grant
	claims jwt.MapClaims // groups, and maybe a direct grant
	direct *auth.Grant   // the token's own claims as a grant
	held   []string      // the groups the token names
}

func drawList(t *rapid.T, label string, from []string) []string {
	xs := rapid.SliceOfNDistinct(rapid.SampledFrom(append([]string{"*"}, from...)), 0, 2, rapid.ID[string]).Draw(t, label)
	return xs
}

func drawScenario(t *rapid.T) scenario {
	sc := scenario{groups: map[string]auth.Grant{}, claims: jwt.MapClaims{"sub": "u"}}
	n := rapid.IntRange(1, 4).Draw(t, "groups")
	for i := 0; i < n; i++ {
		g := auth.Grant{Clusters: drawList(t, "gc", tClusters), Namespaces: drawList(t, "gn", tNamespaces),
			Roles: rapid.SliceOfNDistinct(rapid.SampledFrom(tRoles), 0, 2, rapid.ID[string]).Draw(t, "gr")}
		if rapid.Bool().Draw(t, "haspair") {
			g.Pairs = []auth.Pair{{Cluster: rapid.SampledFrom(tClusters).Draw(t, "pc"), Namespace: rapid.SampledFrom(append([]string{"*"}, tNamespaces...)).Draw(t, "pn")}}
		}
		name := fmt.Sprintf("g%d", i)
		sc.groups[name] = g
		if rapid.Bool().Draw(t, "held") {
			sc.held = append(sc.held, name)
		}
	}
	var gs []any
	for _, h := range append(sc.held, "unknown") {
		gs = append(gs, h)
	}
	sc.claims["groups"] = gs
	if rapid.Bool().Draw(t, "direct") {
		d := auth.Grant{Clusters: drawList(t, "dc", tClusters), Namespaces: drawList(t, "dn", tNamespaces),
			Roles: rapid.SliceOfNDistinct(rapid.SampledFrom(tRoles), 0, 2, rapid.ID[string]).Draw(t, "dr")}
		sc.direct = &d
		sc.claims["clusters"], sc.claims["namespaces"], sc.claims["roles"] = strings.Join(d.Clusters, ","), strings.Join(d.Namespaces, ","), strings.Join(d.Roles, ",")
	}
	return sc
}

func grantAllows(g auth.Grant, role, c, n string) bool {
	has := func(xs []string, x string) bool {
		for _, y := range xs {
			if y == x || y == "*" {
				return true
			}
		}
		return false
	}
	roleOK := false
	for _, r := range g.Roles {
		roleOK = roleOK || r == role
	}
	if !roleOK {
		return false
	}
	if has(g.Clusters, c) && has(g.Namespaces, n) {
		return true
	}
	for _, p := range g.Pairs {
		if (p.Cluster == "*" || p.Cluster == c) && (p.Namespace == "*" || p.Namespace == n) {
			return true
		}
	}
	return false
}

// oracle: some one grant the principal holds gives role over (c, n).
func (sc scenario) allows(role, c, n string) bool {
	if sc.direct != nil && grantAllows(*sc.direct, role, c, n) {
		return true
	}
	for _, h := range sc.held {
		if grantAllows(sc.groups[h], role, c, n) {
			return true
		}
	}
	return false
}

// productAllows is the old Mapping.Principal (the mutant): every held
// grant's clusters, namespaces and roles appended into one list each.
func (sc scenario) productAllows(role, c, n string) bool {
	var all auth.Grant
	add := func(g auth.Grant) {
		all.Clusters = append(all.Clusters, g.Clusters...)
		all.Namespaces = append(all.Namespaces, g.Namespaces...)
		all.Roles = append(all.Roles, g.Roles...)
		for _, p := range g.Pairs {
			all.Clusters, all.Namespaces = append(all.Clusters, p.Cluster), append(all.Namespaces, p.Namespace)
		}
	}
	if sc.direct != nil {
		add(*sc.direct)
	}
	for _, h := range sc.held {
		add(sc.groups[h])
	}
	return grantAllows(all, role, c, n)
}

func mapping(sc scenario) auth.Mapping {
	return auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups", Groups: sc.groups}
}

// TestEffectiveScopeIsUnionOfTuples: for every role, cluster and namespace
// of a small universe, the principal's view for the role admits exactly
// what one of its grants gives; the lake's whole-cluster view admits a
// cluster only through a grant of every namespace in it.
func TestEffectiveScopeIsUnionOfTuples(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		sc := drawScenario(t)
		m := mapping(sc)
		p, err := m.Principal(sc.claims)
		if err != nil {
			t.Fatal(err)
		}
		for _, role := range tRoles {
			v := p.For(role)
			s := v.Scope()
			wholeAll := v.WholeClusters()
			for _, c := range append(tClusters, "other") {
				anyNs := false
				for _, n := range append(tNamespaces, "other") {
					want := sc.allows(role, c, n)
					anyNs = anyNs || want
					if got := s.Allows(c, n); got != want {
						t.Fatalf("role %s (%s, %s): scope admits %v, the grants say %v; tuples %v", role, c, n, got, want, p.Grants)
					}
				}
				if v.MayCluster(c) != anyNs {
					t.Fatalf("role %s: MayCluster(%s) = %v, want %v", role, c, v.MayCluster(c), anyNs)
				}
				whole := sc.allows(role, c, "a-namespace-no-grant-names")
				if wholeAll.MayCluster(c) != whole {
					t.Fatalf("role %s: whole-cluster view of %s = %v, want %v; tuples %v", role, c, wholeAll.MayCluster(c), whole, p.Grants)
				}
			}
		}
	})
}

// TestProductMutantCaughtByOracle: the old combination (clusters ×
// namespaces × roles across grants) is separated from the union by the
// oracle on the generated scenarios, and on the D37 example: a devtools
// grant and a Kubernetes grant held together.
func TestProductMutantCaughtByOracle(t *testing.T) {
	sc := scenario{groups: map[string]auth.Grant{
		"devtools-alice": {Clusters: []string{"devtools"}, Namespaces: []string{"dev-alice"}, Roles: []string{"query"}},
		"shop-oncall":    {Clusters: []string{"prod-a"}, Namespaces: []string{"*"}, Roles: []string{"plan"}},
	}, held: []string{"devtools-alice", "shop-oncall"}}
	sc.claims = jwt.MapClaims{"sub": "alice", "groups": []any{"devtools-alice", "shop-oncall"}}
	m := mapping(sc)
	p, _ := m.Principal(sc.claims)
	q, pl := p.For(auth.RoleQuery), p.For(auth.RolePlan)
	if q.Scope().Allows("prod-a", "dev-alice") || q.Scope().Allows("devtools", "shop") || q.MayCluster("prod-a") {
		t.Fatalf("query view crosses grants: %+v", q)
	}
	if !q.Scope().Allows("devtools", "dev-alice") {
		t.Fatal("query view lost its own grant")
	}
	if pl.MayCluster("devtools") || !pl.WholeClusters().MayCluster("prod-a") {
		t.Fatalf("plan view: %+v", pl)
	}
	// the old rule gave query on prod-a (every namespace) and plan on devtools
	if !sc.productAllows(auth.RoleQuery, "prod-a", "shop") || !sc.productAllows(auth.RolePlan, "devtools", "shop") {
		t.Fatal("the fixed example does not show the mutant's over-reach")
	}
	gen := rapid.Custom(drawScenario)
	found := 0
	for seed := 0; seed < 1000; seed++ {
		s := gen.Example(seed)
	scan:
		for _, role := range tRoles {
			for _, c := range tClusters {
				for _, n := range tNamespaces {
					if s.productAllows(role, c, n) != s.allows(role, c, n) {
						found++
						break scan
					}
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("no generated scenario separates the product from the union")
	}
	t.Logf("%d of 1000 generated scenarios separate the old product from the union", found)
}

// TestCompiledCedarGrantsLoad: the Cedar compiler's queryd output
// (otel-chdb/grants/examples/out, D38) is a Mapping's group_grants: a group
// holding tuples of two roles gets each role over its own scope only.
func TestCompiledCedarGrantsLoad(t *testing.T) {
	for env, check := range map[string]func(*auth.Principal){
		"prd": func(p *auth.Principal) {
			q, pl := p.For(auth.RoleQuery), p.For(auth.RolePlan)
			if !q.Scope().Allows("prod-eu-1", "shop") || !q.Scope().Allows("prod-us-1", "any") {
				t.Fatalf("prd query: %+v", q)
			}
			if !pl.WholeClusters().MayCluster("prod-us-1") {
				t.Fatalf("prd plan: %+v", pl)
			}
		},
		"dev": func(p *auth.Principal) {
			q, pl := p.For(auth.RoleQuery), p.For(auth.RolePlan)
			if !q.Scope().Allows("devtools", "dev-payments") || q.Scope().Allows("devtools", "dev-search") || !q.Scope().Allows("dev-eu-1", "anything") {
				t.Fatalf("dev query: %+v", q)
			}
			if pl.MayCluster("devtools") || !pl.WholeClusters().MayCluster("dev-eu-1") {
				t.Fatalf("dev plan: %+v", pl)
			}
		},
	} {
		b, err := os.ReadFile("../../../grants/examples/out/queryd-grants-" + env + ".json")
		if err != nil {
			t.Fatal(err)
		}
		var cfg struct {
			Groups map[string]auth.Grant `json:"group_grants"`
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatal(err)
		}
		m := auth.Mapping{GroupsClaim: "groups", Groups: cfg.Groups}
		var gs []any
		for g := range cfg.Groups {
			gs = append(gs, g)
		}
		p, err := m.Principal(jwt.MapClaims{"sub": "u", "groups": gs})
		if err != nil {
			t.Fatal(err)
		}
		check(p)
	}
	// one group alone: team-search in prd reads its namespace, plans nothing
	b, _ := os.ReadFile("../../../grants/examples/out/queryd-grants-prd.json")
	var cfg struct {
		Groups map[string]auth.Grant `json:"group_grants"`
	}
	_ = json.Unmarshal(b, &cfg)
	m := auth.Mapping{GroupsClaim: "groups", Groups: cfg.Groups}
	p, _ := m.Principal(jwt.MapClaims{"sub": "u", "groups": "team-search"})
	if q := p.For(auth.RoleQuery).Scope(); !q.Allows("prod-eu-1", "search") || q.Allows("prod-eu-1", "shop") || q.Allows("prod-us-1", "search") {
		t.Fatalf("team-search: %+v", p)
	}
	if p.Has(auth.RolePlan) {
		t.Fatalf("team-search plans: %+v", p)
	}
}
