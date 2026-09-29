// Package grants compiles Cedar grant policies (the source of truth,
// DECISIONS.md D38) into what the pipeline enforces: the query service's
// explicit (role, cluster, namespace) tuples, per environment, and IAM
// policies for the plan (presign) and write paths, expressed only with S3
// prefixes and principal tags (D18). A policy outside that fragment is
// rejected at authoring time with a reason, never compiled to something
// wider or narrower than Cedar says. Check verifies the result against
// Cedar's own authorizer over the registry's whole finite universe.
package grants

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	cedar "github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"
	xast "github.com/cedar-policy/cedar-go/x/exp/ast"
	"github.com/cedar-policy/cedar-go/x/exp/schema"
	"github.com/cedar-policy/cedar-go/x/exp/schema/validate"
)

// Entity types and actions of schema.cedarschema.
const (
	NS          = "Oscope"
	TEnv        = NS + "::Env"
	TCluster    = NS + "::Cluster"
	TNamespace  = NS + "::Namespace"
	TGroup      = NS + "::Group"
	TUser       = NS + "::User"
	TWorkload   = NS + "::Workload"
	TAction     = NS + "::Action"
	ActQuery    = "query"
	ActPlan     = "plan"
	ActContent  = "llm_content"
	ActWrite    = "write"
	ActAdmin    = "admin"
	defaultTag  = "cluster"
	unnamedNs   = "zz-unnamed"
	unlistedCl  = "zz-unregistered"
	probePrefix = "probe:"
)

// ReadActions are the actions compiled to query-service tuples.
var ReadActions = []string{ActQuery, ActPlan, ActContent}

var (
	clusterRE   = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)
	namespaceRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	envRE       = regexp.MustCompile(`^[a-z][a-z0-9-]{0,15}$`)
)

// Registry is the environments and their clusters: which bucket and root
// an environment's lanes live in, and which clusters it holds. A cluster
// belongs to exactly one environment.
type Registry struct {
	Envs map[string]Env `json:"envs"`
}

// Env is one environment.
type Env struct {
	Bucket string `json:"bucket"`
	Root   string `json:"root"`
	// Clusters: each cluster's known namespaces (may be empty; used only
	// to widen the equivalence check's universe).
	Clusters map[string][]string `json:"clusters"`
	// ClusterTag is the session tag that carries a workload's cluster
	// (default "cluster"; "eks-cluster-name" under EKS Pod Identity).
	ClusterTag string `json:"cluster_tag,omitempty"`
}

// LoadRegistry reads and checks a registry file.
func LoadRegistry(path string) (*Registry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Registry
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &r, r.Check()
}

// Check validates names and that no cluster is in two environments.
func (r *Registry) Check() error {
	seen := map[string]string{}
	for e, env := range r.Envs {
		if !envRE.MatchString(e) {
			return fmt.Errorf("environment %q: not a valid name", e)
		}
		if env.Bucket == "" || env.Root == "" || strings.ContainsAny(env.Root, "*?$") || strings.HasSuffix(env.Root, "/") {
			return fmt.Errorf("environment %s: bucket and root are required (root without wildcards or a trailing /)", e)
		}
		for c, nss := range env.Clusters {
			if !clusterRE.MatchString(c) {
				return fmt.Errorf("environment %s: cluster %q is not a valid name", e, c)
			}
			if o, ok := seen[c]; ok {
				return fmt.Errorf("cluster %s is in environments %s and %s: a cluster belongs to one", c, o, e)
			}
			seen[c] = e
			for _, n := range nss {
				if !namespaceRE.MatchString(n) {
					return fmt.Errorf("cluster %s: namespace %q is not a valid name", c, n)
				}
			}
		}
	}
	return nil
}

func (r *Registry) envOf(cluster string) (string, bool) {
	for e, env := range r.Envs {
		if _, ok := env.Clusters[cluster]; ok {
			return e, true
		}
	}
	return "", false
}

func (r *Registry) envNames() []string {
	out := make([]string, 0, len(r.Envs))
	for e := range r.Envs {
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

func (e Env) clusterNames() []string {
	out := make([]string, 0, len(e.Clusters))
	for c := range e.Clusters {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func (e Env) tag() string {
	if e.ClusterTag != "" {
		return e.ClusterTag
	}
	return defaultTag
}

// Tuple is the query service's grant (query/internal/auth.Tuple).
type Tuple struct {
	Role      string `json:"role"`
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
}

// Output is what a policy set compiles to.
type Output struct {
	// Query: environment → group → tuples, for that environment's query
	// service (one per environment). An environment-wide grant is its
	// registered clusters, never "*".
	Query map[string]map[string][]Tuple `json:"query"`
	// TagWrites: environment → groups whose workloads write their own
	// cluster's prefix (the session's cluster tag).
	TagWrites map[string][]string `json:"tag_writes,omitempty"`
	// LiteralWrites: environment → group → clusters it writes.
	LiteralWrites map[string]map[string][]string `json:"literal_writes,omitempty"`
	// Accepted policy ids, in input order.
	Accepted []string `json:"accepted"`
}

// Rejection is why a policy was not compiled.
type Rejection struct {
	Policy string `json:"policy"`
	At     string `json:"at"`
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

func (r Rejection) Error() string {
	return fmt.Sprintf("%s (%s): %s: %s", r.Policy, r.At, r.Reason, r.Detail)
}

// LoadSchema parses and resolves a Cedar schema.
func LoadSchema(text []byte) (*validate.Validator, error) {
	var s schema.Schema
	if err := s.UnmarshalCedar(text); err != nil {
		return nil, err
	}
	rs, err := s.Resolve()
	if err != nil {
		return nil, err
	}
	return validate.New(rs, validate.WithStrict()), nil
}

// Parse reads a policy file.
func Parse(name string, text []byte) (cedar.PolicyList, error) {
	return cedar.NewPolicyListFromBytes(name, text)
}

// PolicyID is a policy's @id annotation, or its position.
func PolicyID(p *cedar.Policy, i int) string {
	if id, ok := p.Annotations()["id"]; ok && id != "" {
		return string(id)
	}
	return fmt.Sprintf("policy%d", i)
}

// Compile compiles ps against reg. Rejected policies are left out whole
// (a policy is never compiled in part) and listed with their reasons; the
// caller refuses the set when any is rejected (grantc does).
func Compile(v *validate.Validator, ps cedar.PolicyList, reg *Registry) (*Output, []Rejection) {
	out := &Output{Query: map[string]map[string][]Tuple{}, TagWrites: map[string][]string{}, LiteralWrites: map[string]map[string][]string{}}
	var rej []Rejection
	for i, p := range ps {
		id := PolicyID(p, i)
		pos := p.Position()
		at := fmt.Sprintf("%s:%d:%d", pos.Filename, pos.Line, pos.Column)
		c, r := compileOne(v, id, p, reg)
		if r != nil {
			r.Policy, r.At = id, at
			rej = append(rej, *r)
			continue
		}
		out.Accepted = append(out.Accepted, id)
		c.apply(out, reg)
	}
	out.normalise()
	return out, rej
}

// compiled is one accepted policy.
type compiled struct {
	group    string
	actions  []string
	env      string // the environment of the resource
	cluster  string // "*" for the whole environment
	ns       string // "*" for the whole cluster
	tagWrite bool   // write: resource in principal.cluster
}

// apply adds c to out. An environment-wide grant is expanded to the
// registry's clusters of that environment: the registry decides which
// clusters an environment holds for reads as for writes (an unregistered
// cluster's edge is denied by the write policy, so it has no data to miss).
func (c compiled) apply(out *Output, reg *Registry) {
	clusters := []string{c.cluster}
	if c.cluster == "*" {
		clusters = reg.Envs[c.env].clusterNames()
	}
	for _, a := range c.actions {
		switch a {
		case ActQuery, ActPlan, ActContent:
			g := out.Query[c.env]
			if g == nil {
				g = map[string][]Tuple{}
				out.Query[c.env] = g
			}
			for _, cl := range clusters {
				g[c.group] = append(g[c.group], Tuple{a, cl, c.ns})
			}
		case ActWrite:
			if c.tagWrite {
				out.TagWrites[c.env] = append(out.TagWrites[c.env], c.group)
				continue
			}
			g := out.LiteralWrites[c.env]
			if g == nil {
				g = map[string][]string{}
				out.LiteralWrites[c.env] = g
			}
			g[c.group] = append(g[c.group], c.cluster)
		}
	}
}

func (o *Output) normalise() {
	for _, groups := range o.Query {
		for g, ts := range groups {
			groups[g] = normTuples(ts)
		}
	}
	for e, gs := range o.TagWrites {
		o.TagWrites[e] = uniq(gs)
	}
	for _, groups := range o.LiteralWrites {
		for g, cs := range groups {
			groups[g] = uniq(cs)
		}
	}
}

// normTuples drops duplicates and tuples a wildcard of the same role
// covers, and sorts.
func normTuples(ts []Tuple) []Tuple {
	set := map[Tuple]bool{}
	for _, t := range ts {
		set[t] = true
	}
	var out []Tuple
	for t := range set {
		if set[Tuple{t.Role, "*", "*"}] && t != (Tuple{t.Role, "*", "*"}) {
			continue
		}
		if t.Namespace != "*" && (set[Tuple{t.Role, t.Cluster, "*"}] || set[Tuple{t.Role, "*", t.Namespace}]) {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Role != b.Role {
			return a.Role < b.Role
		}
		if a.Cluster != b.Cluster {
			return a.Cluster < b.Cluster
		}
		return a.Namespace < b.Namespace
	})
	return out
}

func uniq(xs []string) []string {
	sort.Strings(xs)
	out := xs[:0]
	for i, x := range xs {
		if i == 0 || xs[i-1] != x {
			out = append(out, x)
		}
	}
	return out
}

func reject(reason, format string, a ...any) *Rejection {
	return &Rejection{Reason: reason, Detail: fmt.Sprintf(format, a...)}
}

func compileOne(v *validate.Validator, id string, p *cedar.Policy, reg *Registry) (compiled, *Rejection) {
	var c compiled
	a := (*xast.Policy)(p.AST())
	if v != nil {
		if err := v.Policy(id, a); err != nil {
			return c, reject("schema", "%v", err)
		}
	}
	if a.Effect == xast.EffectForbid {
		return c, reject("forbid_not_compiled", "grants are a union of permits: a forbid cannot be a query-service tuple, and an IAM Deny alone would make the two layers disagree; remove the permit it would cancel")
	}
	// principal: in a group (or: is Workload in a group)
	workloadOnly := false
	switch s := a.Principal.(type) {
	case xast.ScopeTypeIn:
		if string(s.Entity.Type) != TGroup {
			if string(s.Entity.Type) == TUser {
				return c, reject("principal_person", "grant a person through a group or app role (assigned deliberately, reviewable; TM-E3), not by name")
			}
			return c, reject("principal_not_group", "principal must be `in %s::\"…\"`", TGroup)
		}
		c.group = string(s.Entity.ID)
	case xast.ScopeTypeIsIn:
		if string(s.Entity.Type) != TGroup || (string(s.Type) != TWorkload && string(s.Type) != TUser) {
			return c, reject("principal_not_group", "principal must be `in %s::\"…\"` (optionally `is %s`)", TGroup, TWorkload)
		}
		c.group, workloadOnly = string(s.Entity.ID), string(s.Type) == TWorkload
	case xast.ScopeTypeEq:
		if string(s.Entity.Type) == TUser {
			return c, reject("principal_person", "grant a person through a group or app role (assigned deliberately, reviewable; TM-E3), not by name")
		}
		return c, reject("principal_not_group", "principal must be `in %s::\"…\"`", TGroup)
	case xast.ScopeTypeAll:
		return c, reject("principal_unconstrained", "a permit for every principal grants every signed-in identity")
	default:
		return c, reject("principal_not_group", "principal must be `in %s::\"…\"`", TGroup)
	}
	// actions: named
	var acts []types.EntityUID
	switch s := a.Action.(type) {
	case xast.ScopeTypeEq:
		acts = []types.EntityUID{s.Entity}
	case xast.ScopeTypeInSet:
		acts = s.Entities
	case xast.ScopeTypeAll:
		return c, reject("action_unconstrained", "name the actions (query, plan, llm_content, write); a new action must not be granted by an old policy")
	default:
		return c, reject("action_unknown", "actions are named with == or in [...]")
	}
	for _, u := range acts {
		if string(u.Type) != TAction {
			return c, reject("action_unknown", "%s is not an %s", u, TAction)
		}
		c.actions = append(c.actions, string(u.ID))
	}
	sort.Strings(c.actions)
	// resource: one environment, cluster or namespace, by `in`
	var kind string
	switch s := a.Resource.(type) {
	case xast.ScopeTypeIn:
		kind = string(s.Entity.Type)
		if r := c.resource(kind, string(s.Entity.ID), reg); r != nil {
			return c, r
		}
	case xast.ScopeTypeEq:
		if string(s.Entity.Type) != TNamespace {
			return c, reject("resource_eq_container", "`resource == %s` names the entity alone, not the data it holds; use `in`", s.Entity)
		}
		kind = TNamespace
		if r := c.resource(kind, string(s.Entity.ID), reg); r != nil {
			return c, r
		}
	case xast.ScopeTypeAll:
		kind = "all"
	case xast.ScopeTypeIs, xast.ScopeTypeIsIn:
		return c, reject("resource_type_only", "`resource is …` grants every entity of a type; name an environment, cluster or namespace")
	default:
		return c, reject("resource_unsupported", "resource must be `in` an environment, cluster or namespace")
	}
	// conditions: none, except `resource in principal.cluster` for write
	if len(a.Conditions) > 0 {
		if !(len(a.Conditions) == 1 && a.Conditions[0].Condition == xast.ConditionWhen && inPrincipalCluster(a.Conditions[0].Body)) {
			return c, reject("condition_not_expressible", "conditions over attributes or context cannot be enforced by an S3 prefix or a principal tag; only `when { resource in principal.cluster }` (write) is compiled")
		}
		if len(c.actions) != 1 || c.actions[0] != ActWrite || !workloadOnly {
			return c, reject("condition_not_expressible", "`resource in principal.cluster` compiles to the workload's cluster session tag: only for `principal is %s in …` and `action == %s::\"write\"`", TWorkload, TAction)
		}
		if kind != TEnv {
			return c, reject("tag_write_needs_env", "a tag-bound write names its environment (`resource in %s::\"…\"`): the tag chooses the cluster, the environment chooses the bucket", TEnv)
		}
		c.tagWrite = true
	}
	if kind == "all" {
		return c, reject("resource_unconstrained", "a grant across every environment (a dev principal reading prd); name the environment (break-glass: research/grants.md §8, not compiled)")
	}
	for _, act := range c.actions {
		switch act {
		case ActQuery, ActContent, ActPlan:
			if workloadOnly {
				return c, reject("principal_type_not_compiled", "the query service grants a group to every token that names it, person or workload: `principal is %s` on a read cannot be enforced; use a group only workloads hold", TWorkload)
			}
		}
		switch act {
		case ActQuery, ActContent:
		case ActPlan:
			if c.ns != "*" {
				return c, reject("plan_namespace", "namespace is not a prefix tier: raw lane objects hold every namespace of their cluster, so a namespace grant may give query, never plan (grant plan on the cluster, or query only)")
			}
		case ActWrite:
			if c.ns != "*" {
				return c, reject("write_namespace", "namespace is not a prefix tier: an edge writes its whole cluster's lanes; grant write on a cluster or by the cluster tag")
			}
			if !workloadOnly {
				return c, reject("write_person", "write is for workloads: `principal is %s in %s::\"…\"`", TWorkload, TGroup)
			}
			if !c.tagWrite && kind == TEnv {
				return c, reject("write_env_literal", "a write to every cluster of an environment would let one cluster's edge forge another's lanes (SEC-1); bind it to the workload's cluster: `when { resource in principal.cluster }`")
			}
		case ActAdmin:
			return c, reject("admin_not_compiled", "operating roles (consumer, GC, retirement) are infrastructure policies in deploy/iam, reviewed there")
		default:
			return c, reject("action_unknown", "action %q is not in the schema", act)
		}
	}
	return c, nil
}

// resource sets c's env, cluster and namespace from an entity.
func (c *compiled) resource(kind, id string, reg *Registry) *Rejection {
	switch kind {
	case TEnv:
		if _, ok := reg.Envs[id]; !ok {
			return reject("resource_unregistered", "environment %q is not in the registry", id)
		}
		c.env, c.cluster, c.ns = id, "*", "*"
	case TCluster:
		e, ok := reg.envOf(id)
		if !ok {
			return reject("resource_unregistered", "cluster %q is in no environment of the registry: its environment (and so its bucket) is unknown", id)
		}
		c.env, c.cluster, c.ns = e, id, "*"
	case TNamespace:
		cl, ns, ok := strings.Cut(id, "/")
		if !ok || !namespaceRE.MatchString(ns) {
			return reject("resource_bad_namespace", "a namespace id is \"{cluster}/{namespace}\", got %q", id)
		}
		e, found := reg.envOf(cl)
		if !found {
			return reject("resource_unregistered", "cluster %q (of namespace %q) is in no environment of the registry", cl, id)
		}
		c.env, c.cluster, c.ns = e, cl, ns
	default:
		return reject("resource_unsupported", "resource must be `in` an environment, cluster or namespace, not %s", kind)
	}
	return nil
}

// inPrincipalCluster matches `resource in principal.cluster`.
func inPrincipalCluster(n xast.IsNode) bool {
	in, ok := n.(xast.NodeTypeIn)
	if !ok {
		return false
	}
	l, ok := in.Left.(xast.NodeTypeVariable)
	if !ok || l.Name != "resource" {
		return false
	}
	acc, ok := in.Right.(xast.NodeTypeAccess)
	if !ok || acc.Value != "cluster" {
		return false
	}
	pv, ok := acc.Arg.(xast.NodeTypeVariable)
	return ok && pv.Name == "principal"
}

// ErrRejected is returned by callers that refuse a set with rejections.
var ErrRejected = errors.New("policies rejected")
