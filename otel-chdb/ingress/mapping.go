package ingress

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
)

// Policy maps verified identities to the tenant scope the pipeline keys
// everything by: (cluster, namespace). It is the operator's, validated at
// start, and it grants WRITE into a namespace only. It is deliberately not
// the query service's grant table and not the limits table (CAST 36: one
// name keying two policies): a group that lets a person send telemetry
// says nothing about what they may read, and limits are per principal.
type Policy struct {
	// Cluster is the first key segment and k8s.cluster.name of everything
	// this ingress writes (e.g. "devtools"). It must not be the name of a
	// Kubernetes cluster: the query service scopes by cluster, and a
	// namespace named like a team in a real cluster would otherwise meet
	// it in a grant (research/entra-ingress.md §4.4).
	Cluster string `json:"cluster"`
	// NamespacePrefix, if set, every mapped namespace must start with
	// (e.g. "dev-"), so a devtools namespace never equals a Kubernetes one.
	NamespacePrefix string `json:"namespace_prefix"`
	// Rules: an app role value or a group object id, in one Entra tenant,
	// grants writing into one namespace.
	Rules []Rule `json:"rules"`
}

// Rule is one grant.
type Rule struct {
	Tenant    string `json:"tenant"`    // Entra tid (GUID)
	Role      string `json:"role"`      // an app role value, e.g. "Telemetry.Team.Payments"
	Group     string `json:"group"`     // or a group object id (GUID); roles are preferred (no overage)
	Namespace string `json:"namespace"` // the tenant namespace, e.g. "dev-payments"
}

// Validate checks the policy once, at start (CAST 25: parameters that
// combine are validated together).
func (p *Policy) Validate() error {
	if !commit.ValidName(p.Cluster) {
		return fmt.Errorf("policy: cluster %q is not a key segment", p.Cluster)
	}
	if len(p.Rules) == 0 {
		return errors.New("policy: no rules (nobody could write)")
	}
	for i, r := range p.Rules {
		if !IsGUID(r.Tenant) {
			return fmt.Errorf("policy rule %d: tenant %q is not a GUID", i, r.Tenant)
		}
		if (r.Role == "") == (r.Group == "") {
			return fmt.Errorf("policy rule %d: exactly one of role and group", i)
		}
		if r.Group != "" && !IsGUID(r.Group) {
			return fmt.Errorf("policy rule %d: group %q is not an object id (display names are not unique)", i, r.Group)
		}
		if !commit.ValidName(r.Namespace) || !strings.HasPrefix(r.Namespace, p.NamespacePrefix) {
			return fmt.Errorf("policy rule %d: namespace %q (want a key segment starting %q)", i, r.Namespace, p.NamespacePrefix)
		}
	}
	return nil
}

// ErrForbidden: authenticated, but not allowed to write here (HTTP 403).
var ErrForbidden = errors.New("forbidden")

// Namespaces are the namespaces id may write, sorted.
func (p *Policy) Namespaces(id *Identity) []string {
	seen := map[string]bool{}
	for _, r := range p.Rules {
		if r.Tenant != id.TenantID {
			continue
		}
		if (r.Role != "" && slices.Contains(id.Roles, r.Role)) || (r.Group != "" && slices.Contains(id.Groups, r.Group)) {
			seen[r.Namespace] = true
		}
	}
	out := make([]string, 0, len(seen))
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// Resolve picks the namespace a request writes into. A principal granted
// exactly one namespace gets it; one granted several must name one of them
// (the requested value is a choice among grants, never a grant); anything
// else is refused. A groups overage with no role-based grant is refused
// with its own reason: the ingress does not call Graph on the hot path.
func (p *Policy) Resolve(id *Identity, requested string) (string, error) {
	nss := p.Namespaces(id)
	switch {
	case len(nss) == 0 && id.Overage:
		return "", fmt.Errorf("%w: groups claim overage and no app role grants a namespace (assign an app role)", ErrForbidden)
	case len(nss) == 0:
		return "", fmt.Errorf("%w: no namespace is granted to this principal", ErrForbidden)
	case requested != "":
		if slices.Contains(nss, requested) {
			return requested, nil
		}
		return "", fmt.Errorf("%w: namespace %q is not granted to this principal", ErrForbidden, requested)
	case len(nss) == 1:
		return nss[0], nil
	default:
		return "", fmt.Errorf("%w: several namespaces granted (%s); name one", ErrForbidden, strings.Join(nss, ", "))
	}
}
