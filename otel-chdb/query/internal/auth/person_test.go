package auth_test

import (
	"fmt"
	"testing"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/golang-jwt/jwt/v5"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// resolve_person (research/grants.md §9.3) is a role of its own: a
// resolve_person grant never widens a query or plan view, and a query grant
// never gives names.
func TestResolvePersonIsItsOwnRole(t *testing.T) {
	tracetag.Covers(t, "P", "H-G1", "H-G8", "R-G1", "R-G9")
	m := &auth.Mapping{GroupsClaim: "groups", Groups: map[string]auth.Grant{
		"names": {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{auth.RolePerson}},
		"team":  {Clusters: []string{"devtools"}, Namespaces: []string{"dev-a"}, Roles: []string{auth.RoleQuery}},
	}}
	p, err := m.Principal(jwt.MapClaims{"sub": "x", "groups": []any{"names", "team"}})
	if err != nil {
		t.Fatal(err)
	}
	if !p.Has(auth.RolePerson) || !p.Has(auth.RoleQuery) || p.Has(auth.RolePlan) {
		t.Fatalf("roles %v", p.Roles)
	}
	if got := fmt.Sprint(p.For(auth.RoleQuery).Pairs()); got != "[{devtools dev-a}]" {
		t.Fatalf("query view %s", got)
	}
	if got := fmt.Sprint(p.For(auth.RoleQuery, auth.RolePlan).Pairs()); got != "[{devtools dev-a}]" {
		t.Fatalf("basis view %s", got)
	}
	q, _ := m.Principal(jwt.MapClaims{"sub": "y", "groups": []any{"team"}})
	if len(q.For(auth.RolePerson).Pairs()) != 0 {
		t.Fatalf("a query grant gives names: %v", q.For(auth.RolePerson).Pairs())
	}
}
