package grants

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	cedar "github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/x/exp/schema/validate"
	"pgregory.net/rapid"
)

func load(t testing.TB) (*validate.Validator, *Registry) {
	t.Helper()
	st, err := os.ReadFile("schema.cedarschema")
	if err != nil {
		t.Fatal(err)
	}
	v, err := LoadSchema(st)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry("examples/registry.json")
	if err != nil {
		t.Fatal(err)
	}
	return v, reg
}

func parseFile(t testing.TB, p string) cedar.PolicyList {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := Parse(p, b)
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

// TestExamplesCompileAgreeAndAreCommitted: the accepted examples compile,
// agree with Cedar over the whole universe, and the committed outputs
// (examples/out, linted by ci/iam-lint.sh) are what they compile to.
func TestExamplesCompileAgreeAndAreCommitted(t *testing.T) {
	v, reg := load(t)
	ps := parseFile(t, "examples/policies.cedar")
	out, rej := Compile(v, ps, reg)
	if len(rej) > 0 {
		t.Fatalf("rejected: %v", rej)
	}
	if err := Check(ps, out, reg); err != nil {
		t.Fatal(err)
	}
	docs, err := IAM(out, reg)
	if err != nil {
		t.Fatal(err)
	}
	for e := range reg.Envs {
		docs["queryd-grants-"+e+".json"] = QueryConfig(out, e)
	}
	for n, d := range docs {
		p := filepath.Join("examples/out", n)
		if !strings.HasPrefix(n, "queryd") {
			p = filepath.Join("examples/out/iam", n)
		}
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, Marshal(d)) {
			t.Errorf("%s is not what the examples compile to (go run ./cmd/grantc -schema schema.cedarschema -registry examples/registry.json -out examples/out examples/policies.cedar): %v", p, err)
		}
	}
	// the fragment: plan only on whole clusters; nothing across environments
	for e, groups := range out.Query {
		for g, ts := range groups {
			for _, tp := range ts {
				if tp.Role == ActPlan && tp.Namespace != "*" {
					t.Errorf("%s/%s: plan on a namespace %v", e, g, tp)
				}
				if env, ok := reg.envOf(tp.Cluster); !ok || env != e {
					t.Errorf("%s/%s: tuple %v names a cluster of %q", e, g, tp, env)
				}
			}
		}
	}
}

// TestRejectedExamples: every policy of rejected.cedar is refused, with the
// reason its @expect names.
func TestRejectedExamples(t *testing.T) {
	v, reg := load(t)
	ps := parseFile(t, "examples/rejected.cedar")
	out, rej := Compile(v, ps, reg)
	if len(out.Accepted) != 0 || len(rej) != len(ps) {
		t.Fatalf("accepted %v; %d rejections of %d", out.Accepted, len(rej), len(ps))
	}
	for i, p := range ps {
		want := string(p.Annotations()["expect"])
		if rej[i].Reason != want {
			t.Errorf("%s: %s (%s), want %s", rej[i].Policy, rej[i].Reason, rej[i].Detail, want)
		}
	}
}

// TestCheckCatchesCompilerMutants: Check is the equivalence oracle; a
// compiler bug that widens, narrows or moves a grant is refused.
func TestCheckCatchesCompilerMutants(t *testing.T) {
	v, reg := load(t)
	ps := parseFile(t, "examples/policies.cedar")
	fresh := func() *Output {
		out, _ := Compile(v, ps, reg)
		return out
	}
	mutants := map[string]func(o *Output){
		"namespace widened to its cluster": func(o *Output) { o.Query["prd"]["team-search"][0].Namespace = "*" },
		"a tuple dropped":                  func(o *Output) { o.Query["prd"]["sre"] = o.Query["prd"]["sre"][1:] },
		"a grant moved to another environment": func(o *Output) {
			o.Query["dev"]["sre"] = o.Query["prd"]["sre"]
		},
		"plan given with query": func(o *Output) {
			o.Query["prd"]["team-search"] = append(o.Query["prd"]["team-search"], Tuple{ActPlan, "prod-eu-1", "*"})
		},
		"a role crossed to group": func(o *Output) { o.Query["dev"]["approle:Team.Payments"][0].Role = ActPlan },
		"tag write in another env": func(o *Output) {
			o.TagWrites["dev"] = nil
			o.TagWrites["prd"] = append(o.TagWrites["prd"], "entra-ingress")
		},
		"literal write widened": func(o *Output) { o.LiteralWrites["dev"]["entra-ingress"] = []string{"devtools", "dev-eu-1"} },
	}
	for name, m := range mutants {
		o := fresh()
		m(o)
		if err := Check(ps, o, reg); err == nil {
			t.Errorf("mutant %q passed the check", name)
		}
	}
}

// --- generated policies -----------------------------------------------

func drawPolicy(t *rapid.T, reg *Registry, i int) string {
	groups := []string{"sre", "team-a", "team-b", "edges"}
	g := rapid.SampledFrom(groups).Draw(t, "group")
	principal := fmt.Sprintf(`principal in Oscope::Group::%q`, g)
	switch rapid.IntRange(0, 9).Draw(t, "principal") {
	case 0:
		principal = "principal"
	case 1:
		principal = `principal == Oscope::User::"u1"`
	case 2, 3:
		principal = fmt.Sprintf(`principal is Oscope::Workload in Oscope::Group::%q`, g)
	}
	acts := rapid.SliceOfNDistinct(rapid.SampledFrom([]string{ActQuery, ActPlan, ActContent, ActWrite}), 1, 3, rapid.ID[string]).Draw(t, "acts")
	var qa []string
	for _, a := range acts {
		qa = append(qa, fmt.Sprintf(`Oscope::Action::%q`, a))
	}
	action := "action in [" + strings.Join(qa, ", ") + "]"
	var res []string
	for _, e := range reg.envNames() {
		res = append(res, fmt.Sprintf(`resource in Oscope::Env::%q`, e))
		for _, c := range reg.Envs[e].clusterNames() {
			res = append(res, fmt.Sprintf(`resource in Oscope::Cluster::%q`, c))
			for _, n := range reg.Envs[e].Clusters[c] {
				res = append(res, fmt.Sprintf(`resource in Oscope::Namespace::"%s/%s"`, c, n))
			}
		}
	}
	res = append(res, "resource", `resource in Oscope::Cluster::"nowhere"`, `resource == Oscope::Env::"dev"`)
	resource := rapid.SampledFrom(res).Draw(t, "resource")
	cond := ""
	if rapid.IntRange(0, 3).Draw(t, "cond") == 0 {
		cond = " when { resource in principal.cluster }"
	}
	effect := "permit"
	if rapid.IntRange(0, 9).Draw(t, "effect") == 0 {
		effect = "forbid"
	}
	return fmt.Sprintf("@id(\"p%d\")\n%s (%s, %s, %s)%s;\n", i, effect, principal, action, resource, cond)
}

// TestGeneratedPoliciesAgreeWithCedar: over random policy sets, the
// accepted policies compile to output that Cedar's authorizer agrees with
// on every request of the universe, every rejection has a reason, and
// the compiled output stays inside the fragment.
func TestGeneratedPoliciesAgreeWithCedar(t *testing.T) {
	v, reg := load(t)
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 6).Draw(t, "n")
		var src strings.Builder
		for i := 0; i < n; i++ {
			src.WriteString(drawPolicy(t, reg, i))
		}
		ps, err := Parse("gen.cedar", []byte(src.String()))
		if err != nil {
			t.Fatalf("%v\n%s", err, src.String())
		}
		out, rej := Compile(v, ps, reg)
		rejected := map[string]bool{}
		for _, r := range rej {
			if r.Reason == "" || r.Detail == "" {
				t.Fatalf("a rejection without a reason: %+v", r)
			}
			rejected[r.Policy] = true
		}
		var acc cedar.PolicyList
		for i, p := range ps {
			if !rejected[PolicyID(p, i)] {
				acc = append(acc, p)
			}
		}
		if err := Check(acc, out, reg); err != nil {
			t.Fatalf("%v\n%s", err, src.String())
		}
		for _, groups := range out.Query {
			for _, ts := range groups {
				for _, tp := range ts {
					if tp.Role == ActPlan && tp.Namespace != "*" {
						t.Fatalf("plan on a namespace compiled: %v", tp)
					}
				}
			}
		}
	})
}

// --- the compiled IAM, evaluated ---------------------------------------

// allowedBy is a small model of IAM evaluation for the statements these
// documents use: an explicit Deny wins, then any Allow. Resource and
// s3:prefix patterns glob with *; policy variables are substituted from
// tags; conditions Null, StringEquals, StringNotEquals, StringLike and
// StringLikeIfExists over the given context. Not AWS: a check that the
// documents say what Check assumes.
func allowedBy(t *testing.T, d any, action, resource string, ctx map[string]string) bool {
	b, _ := json.Marshal(d)
	var doc struct{ Statement []map[string]any }
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	sub := func(s string) string {
		for k, v := range ctx {
			s = strings.ReplaceAll(s, "${"+k+"}", v)
		}
		return s
	}
	list := func(x any) []string {
		switch v := x.(type) {
		case string:
			return []string{v}
		case []any:
			var out []string
			for _, y := range v {
				out = append(out, y.(string))
			}
			return out
		}
		return nil
	}
	match := func(pats []string, s string) bool {
		for _, p := range pats {
			if ok, _ := path.Match(strings.ReplaceAll(sub(p), "/", "\x00"), strings.ReplaceAll(s, "/", "\x00")); ok {
				return true
			}
			// '*' in IAM crosses '/': fall back to a prefix/suffix split
			if i := strings.Index(sub(p), "*"); i >= 0 && strings.HasPrefix(s, sub(p)[:i]) && strings.HasSuffix(s, sub(p)[i+1:]) && !strings.Contains(sub(p)[i+1:], "*") {
				return true
			}
			if sub(p) == s {
				return true
			}
		}
		return false
	}
	cond := func(c map[string]any) bool {
		for op, kv := range c {
			for k, vals := range kv.(map[string]any) {
				have, ok := ctx[k]
				want := list(vals)
				switch op {
				case "Null":
					if (want[0] == "true") == ok {
						return false
					}
				case "StringEquals":
					if !ok || !match(want, have) && !contains(want, have) {
						return false
					}
				case "StringNotEquals":
					if ok && contains(want, have) {
						return false
					}
				case "StringLike":
					if !ok || !match(want, have) {
						return false
					}
				case "StringLikeIfExists":
					if ok && !match(want, have) {
						return false
					}
				case "ForAllValues:StringEquals":
					// tag keys: modelled by the caller passing only allowed keys
				default:
					t.Fatalf("condition %s not modelled", op)
				}
			}
		}
		return true
	}
	allow := false
	for _, st := range doc.Statement {
		acts := list(st["Action"])
		if !contains(acts, action) && !contains(acts, "s3:*") {
			continue
		}
		if r, ok := st["Resource"]; ok && !match(list(r), resource) && !contains(list(r), "*") {
			continue
		}
		if c, ok := st["Condition"].(map[string]any); ok && !cond(c) {
			continue
		}
		if st["Effect"] == "Deny" {
			return false
		}
		allow = true
	}
	return allow
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// TestCompiledIAMMatchesTheCheck: the committed IAM documents allow a
// workload's PutObject, and a presign session's GetObject, exactly where
// Check's model of them does, over every cluster of every environment and
// one unregistered cluster; the trust policy admits exactly the clusters
// some plan grant reaches.
func TestCompiledIAMMatchesTheCheck(t *testing.T) {
	v, reg := load(t)
	out, _ := Compile(v, parseFile(t, "examples/policies.cedar"), reg)
	docs, _ := IAM(out, reg)
	var all []string
	for _, e := range reg.envNames() {
		all = append(all, reg.Envs[e].clusterNames()...)
	}
	all = append(all, "prod-ap-9")
	for _, e := range reg.envNames() {
		env := reg.Envs[e]
		for _, own := range all {
			for _, target := range all {
				key := fmt.Sprintf("arn:aws:s3:::%s/%s/%s/pub-0/logs/20260929T000000.000Z-00000000/%020d.parquet", env.Bucket, env.Root, target, 1)
				ctx := map[string]string{"aws:PrincipalTag/" + env.tag(): own}
				got := docs["edge-"+e+".json"] != nil && allowedBy(t, docs["edge-"+e+".json"], "s3:PutObject", key, ctx)
				want := allowsWrite(out, []string{"edges"}, own, e, target, reg)
				if got != want {
					t.Errorf("%s: edge of %s writing %s: IAM %v, check %v", e, own, target, got, want)
				}
				// the ingress's literal write
				lit := docs["write-entra-ingress-"+e+".json"] != nil && allowedBy(t, docs["write-entra-ingress-"+e+".json"], "s3:PutObject", key, ctx)
				if lw := allowsWrite(out, []string{"entra-ingress"}, own, e, target, reg); lit != lw {
					t.Errorf("%s: ingress writing %s: IAM %v, check %v", e, target, lit, lw)
				}
				// the control prefix is never writable
				ctl := fmt.Sprintf("arn:aws:s3:::%s/%s/_consumer/watermark.json", env.Bucket, env.Root)
				if docs["edge-"+e+".json"] != nil && allowedBy(t, docs["edge-"+e+".json"], "s3:PutObject", ctl, map[string]string{"aws:PrincipalTag/" + env.tag(): "_consumer"}) {
					t.Errorf("%s: a session tagged _consumer writes the watermark", e)
				}
			}
			// presign: a session tagged (e, own) reads own only
			if d := docs["presign-"+e+".json"]; d != nil {
				ctx := map[string]string{"aws:PrincipalTag/cluster": own, "aws:PrincipalTag/env": e}
				for _, target := range all {
					key := fmt.Sprintf("arn:aws:s3:::%s/%s/%s/pub-0/logs/e/1.parquet", env.Bucket, env.Root, target)
					if got := allowedBy(t, d, "s3:GetObject", key, ctx); got != (target == own) {
						t.Errorf("%s: presign session of %s reading %s: %v", e, own, target, got)
					}
				}
				other := map[string]string{"aws:PrincipalTag/cluster": own, "aws:PrincipalTag/env": "not-" + e}
				if allowedBy(t, d, "s3:GetObject", fmt.Sprintf("arn:aws:s3:::%s/%s/%s/x", env.Bucket, env.Root, own), other) {
					t.Errorf("%s: a session of another environment reads", e)
				}
				trust := docs["presign-"+e+"-trust.json"]
				req := map[string]string{"aws:RequestTag/env": e, "aws:RequestTag/cluster": own}
				planned := false
				for _, ts := range out.Query[e] {
					for _, tp := range ts {
						planned = planned || tp.Role == ActPlan && tp.Cluster == own
					}
				}
				if got := allowedBy(t, trust, "sts:AssumeRole", "", req); got != planned {
					t.Errorf("%s: trust admits a %s session: %v, plan grants reach it: %v", e, own, got, planned)
				}
			}
		}
	}
}
