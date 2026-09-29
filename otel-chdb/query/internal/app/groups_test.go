package app

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"pgregory.net/rapid"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// CAST row 36 (near miss): one group name keys two policies, claims.
// group_grants (scope, unioned over a principal's groups) and limits.groups
// (budgets). Adding an identity to a group for its budget must never widen
// what it may read. The property, over the shipped example config and over
// random configs: for any claims, adding any group that is only a limits
// tier changes neither the grants, nor the roles, nor the projections of
// the principal; only its limits.
func TestLimitsOnlyGroupsNeverChangeScope(t *testing.T) {
	tracetag.Covers(t, "P", "CAST-36", "H-6", "R-S8")
	t.Setenv("QS_CH_PASSWORD", "x")
	c, err := Load("../../queryd.example.json")
	if err != nil {
		t.Fatal(err)
	}
	limitsOnly := limitsOnlyGroups(c.Claims, c.Limits.Groups)
	// the shipped example has one: the evaluators' budget tier (1414bb4);
	// if that ever grants scope, this is the check that says so
	if !reflect.DeepEqual(limitsOnly, []string{"alert-evaluator"}) {
		t.Fatalf("limits-only groups of the example: %v, want [alert-evaluator]", limitsOnly)
	}
	if _, grants := c.Claims.Groups["alert-evaluator"]; grants {
		t.Fatal("alert-evaluator grants scope")
	}
	shipped := func(t *rapid.T) (auth.Mapping, []string, []string) {
		var all []string
		for g := range c.Claims.Groups {
			all = append(all, g)
		}
		sort.Strings(all)
		return c.Claims, all, limitsOnly
	}
	random := func(t *rapid.T) (auth.Mapping, []string, []string) {
		m := auth.Mapping{SubjectClaim: "sub", ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles",
			GroupsClaim: "groups", Groups: map[string]auth.Grant{}}
		var all []string
		for i, n := 0, rapid.IntRange(0, 4).Draw(t, "grantGroups"); i < n; i++ {
			g := fmt.Sprintf("g%d", i)
			m.Groups[g] = auth.Grant{
				Clusters:   rapid.SliceOfN(rapid.SampledFrom([]string{"*", "c1", "c2"}), 0, 2).Draw(t, "clusters"),
				Namespaces: rapid.SliceOfN(rapid.SampledFrom([]string{"*", "n1", "n2"}), 0, 2).Draw(t, "namespaces"),
				Roles:      rapid.SliceOfN(rapid.SampledFrom([]string{"query", "plan", "admin"}), 0, 2).Draw(t, "roles"),
			}
			all = append(all, g)
		}
		return m, all, []string{"limits-a", "limits-b"}
	}
	for name, cfg := range map[string]func(*rapid.T) (auth.Mapping, []string, []string){"shipped": shipped, "random": random} {
		t.Run(name, rapid.MakeCheck(func(t *rapid.T) {
			m, grantGroups, extra := cfg(t)
			claims := jwt.MapClaims{"sub": "svc"}
			if rapid.Bool().Draw(t, "directClaims") {
				claims["clusters"] = toAny(rapid.SliceOfN(rapid.SampledFrom([]string{"*", "c1", "prod-eu-1"}), 0, 2).Draw(t, "c"))
				claims["namespaces"] = toAny(rapid.SliceOfN(rapid.SampledFrom([]string{"*", "n1", "shop"}), 0, 2).Draw(t, "n"))
				claims["roles"] = toAny(rapid.SliceOfN(rapid.SampledFrom([]string{"query", "plan"}), 0, 2).Draw(t, "r"))
			}
			var groups []string
			if len(grantGroups) > 0 {
				groups = rapid.SliceOfN(rapid.SampledFrom(grantGroups), 0, 3).Draw(t, "groups")
			}
			claims["groups"] = toAny(groups)
			before, err1 := m.Principal(claims)
			with := append(append([]string(nil), groups...), rapid.SliceOfN(rapid.SampledFrom(extra), 1, 2).Draw(t, "limitsGroups")...)
			claims["groups"] = toAny(with)
			after, err2 := m.Principal(claims)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("errors differ: %v / %v", err1, err2)
			}
			if err1 != nil {
				return
			}
			if !reflect.DeepEqual(before.Pairs(), after.Pairs()) || !reflect.DeepEqual(before.Grants, after.Grants) ||
				!reflect.DeepEqual(before.Roles, after.Roles) || before.AllClusters != after.AllClusters ||
				!reflect.DeepEqual(before.Clusters, after.Clusters) || before.AllNamespaces != after.AllNamespaces ||
				!reflect.DeepEqual(before.Namespaces, after.Namespaces) {
				t.Fatalf("a limits-only group changed the scope:\nbefore %+v\nafter  %+v", before, after)
			}
		}))
	}
	// and the budget is what the group is for: it raises the evaluators' limit
	p, err := c.Claims.Principal(jwt.MapClaims{"sub": "alertd", "groups": []any{"alertd-fleet", "alert-evaluator"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Limits.For(p).MaxConcurrent; got != c.Limits.Groups["alert-evaluator"].MaxConcurrent || got <= c.Limits.Default.MaxConcurrent {
		t.Fatalf("alert-evaluator's max_concurrent %d (default %d)", got, c.Limits.Default.MaxConcurrent)
	}
}

// limitsOnlyGroups are the limits tiers that grant nothing, sorted.
func limitsOnlyGroups(m auth.Mapping, limits map[string]central.Limits) []string {
	var out []string
	for g := range limits {
		if _, ok := m.Groups[g]; !ok {
			out = append(out, g)
		}
	}
	sort.Strings(out)
	return out
}

func toAny(xs []string) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
