package auth

import (
	"fmt"
	"sort"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Grant is a set of attributes: what a group (or a token's own claims)
// gives. "*" in Clusters or Namespaces means all of them.
type Grant struct {
	Clusters   []string `json:"clusters"`
	Namespaces []string `json:"namespaces"`
	Roles      []string `json:"roles"`
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

// Principal is who is asking and what they may see.
type Principal struct {
	Subject       string   `json:"sub"`
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
	var g Grant
	add := func(x Grant) {
		g.Clusters = append(g.Clusters, x.Clusters...)
		g.Namespaces = append(g.Namespaces, x.Namespaces...)
		g.Roles = append(g.Roles, x.Roles...)
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
	for _, x := range uniq(g.Clusters) {
		if x == "*" {
			p.AllClusters = true
		} else {
			p.Clusters = append(p.Clusters, x)
		}
	}
	for _, x := range uniq(g.Namespaces) {
		if x == "*" {
			p.AllNamespaces = true
		} else {
			p.Namespaces = append(p.Namespaces, x)
		}
	}
	if p.AllClusters {
		p.Clusters = nil
	}
	if p.AllNamespaces {
		p.Namespaces = nil
	}
	for _, r := range uniq(g.Roles) {
		if r == RoleQuery || r == RolePlan {
			p.Roles = append(p.Roles, r)
		}
	}
	return p, nil
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
