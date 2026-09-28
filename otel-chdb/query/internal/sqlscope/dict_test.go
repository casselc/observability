package sqlscope

import (
	"fmt"
	"strings"
	"testing"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
	"pgregory.net/rapid"
)

// CatalogDictionaries is the entity catalog's dictionaries
// (entities/sql/dictionaries.sql) in database db, as queryd.example.json
// configures them: d_res is the root, scoped through d_pod to d_cluster's
// and d_ns's names; the other five are reached only through a key
// attribute.
func CatalogDictionaries(db string) []*Dictionary {
	q := func(n string) string { return db + "." + n }
	attrs := map[string]DictAttribute{"attrs": {Type: "String", Default: "'{}'"}}
	return []*Dictionary{
		{Name: q("d_res"), Attributes: map[string]DictAttribute{
			"pod_key": {Type: "UInt64", Default: "0", Ref: q("d_pod")},
			"ct":      {Type: "UInt8", Default: "0"},
		},
			Cluster:   fmt.Sprintf("JSONExtractString(dictGet('%s', 'attrs', dictGet('%s', 'cluster_key', dictGet('%s', 'pod_key', {key}))), 'k8s.cluster.name')", q("d_cluster"), q("d_pod"), q("d_res")),
			Namespace: fmt.Sprintf("JSONExtractString(dictGet('%s', 'attrs', dictGet('%s', 'ns_key', dictGet('%s', 'pod_key', {key}))), 'k8s.namespace.name')", q("d_ns"), q("d_pod"), q("d_res")),
		},
		{Name: q("d_pod"), Attributes: map[string]DictAttribute{
			"name":        {Type: "String", Default: "''"},
			"uid":         {Type: "UUID", Default: "'00000000-0000-0000-0000-000000000000'"},
			"start":       {Type: "DateTime", Default: "0"},
			"extra":       {Type: "String", Default: "'{}'"},
			"wl_key":      {Type: "UInt64", Default: "0", Ref: q("d_wl")},
			"node_key":    {Type: "UInt64", Default: "0", Ref: q("d_node")},
			"ns_key":      {Type: "UInt64", Default: "0", Ref: q("d_ns")},
			"cluster_key": {Type: "UInt64", Default: "0", Ref: q("d_cluster")},
		}},
		{Name: q("d_wl"), Attributes: map[string]DictAttribute{"attrs": {Type: "String", Default: "'{}'"}, "containers": {Type: "String", Default: "'[]'"}}},
		{Name: q("d_node"), Attributes: attrs},
		{Name: q("d_ns"), Attributes: attrs},
		{Name: q("d_cluster"), Attributes: attrs},
	}
}

func dictPolicy(t testing.TB) *Policy {
	t.Helper()
	p, err := NewPolicy("otel", []*Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", Scope: "columns",
			Cluster: "`__hdx_materialized_k8s.cluster.name`", Namespace: "`__hdx_materialized_k8s.namespace.name`"},
		{Database: "rw_cat", Name: "resource_kv", Scope: "columns", Cluster: "cluster", Namespace: "namespace"},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetDictionaries(CatalogDictionaries("rw_cat")); err != nil {
		t.Fatal(err)
	}
	return p
}

// the value expression rwproxy writes for k8s.namespace.name (rwproxy
// README §2), and its filter on a covered key
const (
	rwNsValue  = "if(dictHas('rw_cat.d_res', resource_id), JSONExtractString(dictGet('rw_cat.d_ns', 'attrs', dictGet('rw_cat.d_pod', 'ns_key', dictGet('rw_cat.d_res', 'pod_key', resource_id))), 'k8s.namespace.name'), '')"
	rwPodValue = "dictGet('rw_cat.d_pod', 'name', dictGet('rw_cat.d_res', 'pod_key', resource_id))"
)

func TestDictionariesFleetUnchanged(t *testing.T) {
	p := dictPolicy(t)
	fleet := Scope{AllClusters: true, AllNamespaces: true}
	for _, e := range []string{rwNsValue, rwPodValue, "dictGet('rw_cat.d_res', 'ct', resource_id)"} {
		sql := "SELECT " + e + " AS v, count() FROM otel_logs GROUP BY v"
		r := mustFinish(t, p, sql, fleet)
		if !strings.Contains(r.SQL, e) {
			t.Errorf("fleet: %q\nrebuilt as %q", e, r.SQL)
		}
		if strings.Contains(r.SQL, "if(((") || strings.Contains(r.SQL, "k8s.cluster.name') IN") {
			t.Errorf("fleet caller got a guard: %s", r.SQL)
		}
	}
}

func TestDictionariesGuardedForRestricted(t *testing.T) {
	p := dictPolicy(t)
	qa := Scope{Clusters: []string{"qa"}, AllNamespaces: true}
	r := mustFinish(t, p, "SELECT "+rwPodValue+" AS pod FROM otel_logs", qa)
	guard := "JSONExtractString(dictGet('rw_cat.d_cluster', 'attrs', dictGet('rw_cat.d_pod', 'cluster_key', dictGet('rw_cat.d_res', 'pod_key', resource_id))), 'k8s.cluster.name') IN ('qa')"
	want := "SELECT if((" + guard + "), " + rwPodValue + ", CAST('', 'String')) AS pod FROM otel.otel_logs"
	if r.SQL != want {
		t.Errorf("got  %s\nwant %s", r.SQL, want)
	}
	// dictHas: and(guard, dictHas(...)); a namespace-restricted caller gets
	// both expressions
	team := Scope{Clusters: []string{"qa", "qb"}, Namespaces: []string{"shop"}}
	r = mustFinish(t, p, "SELECT dictHas('rw_cat.d_res', 42)", team)
	if !strings.HasPrefix(r.SQL, "SELECT and((JSONExtractString(dictGet('rw_cat.d_cluster'") ||
		!strings.Contains(r.SQL, "IN ('qa', 'qb') AND JSONExtractString(dictGet('rw_cat.d_ns', 'attrs', dictGet('rw_cat.d_pod', 'ns_key', dictGet('rw_cat.d_res', 'pod_key', 42))), 'k8s.namespace.name') IN ('shop')), dictHas('rw_cat.d_res', 42))") {
		t.Errorf("dictHas: %s", r.SQL)
	}
	// rwproxy's namespace value: the outer dictHas and the chain's top
	// (d_ns) are guarded; the chain's inner lookups only make d_ns's key
	r = mustFinish(t, p, "SELECT "+rwNsValue+" AS ns FROM otel_logs", qa)
	if n := strings.Count(r.SQL, "IN ('qa')"); n != 2 {
		t.Errorf("want 2 guards (dictHas and d_ns), got %d: %s", n, r.SQL)
	}
	if !strings.Contains(r.SQL, "CAST('{}', 'String')") {
		t.Errorf("d_ns's default: %s", r.SQL)
	}
}

func TestDictionaryRefusals(t *testing.T) {
	p := dictPolicy(t)
	cases := map[string]string{
		"SELECT dictGet('rw_cat.secrets', 'x', 1)":                                                                         "denied_function",
		"SELECT dictGet('rw_cat.d_res', 'valid_to', 1)":                                                                    "denied_function", // attribute not served
		"SELECT dictGet(concat('rw_cat.', 'd_res'), 'pod_key', 1)":                                                         "denied_function", // name not a literal
		"SELECT dictGet('rw_cat.d_res', lower('POD_KEY'), 1)":                                                              "denied_function",
		"SELECT dictGet('rw_cat.d_r\\x65s', 'pod_key', 1)":                                                                 "denied_function", // escapes
		"SELECT dictGet('d_res', 'pod_key', 1)":                                                                            "denied_function", // unqualified
		"SELECT dictGet('rw_cat.d_res', 'pod_key')":                                                                        "denied_function",
		"SELECT dictGet('rw_cat.d_res', 'pod_key', 1, 2)":                                                                  "denied_function",
		"SELECT dictHas('rw_cat.d_res')":                                                                                   "denied_function",
		"SELECT dictGetOrDefault('rw_cat.d_res', 'pod_key', 1, 0)":                                                         "denied_function",
		"SELECT dictGetString('rw_cat.d_pod', 'name', 1)":                                                                  "denied_function",
		"SELECT dictGetOrNull('rw_cat.d_res', 'pod_key', 1)":                                                               "denied_function",
		"SELECT dictGetAll('rw_cat.d_res', 'pod_key', 1)":                                                                  "denied_function",
		"SELECT dictGetHierarchy('rw_cat.d_res', 1)":                                                                       "denied_function",
		"SELECT joinGet('rw_cat.j', 'x', 1)":                                                                               "denied_function",
		"SELECT dictGet('rw_cat.d_res', ('pod_key', 'ct'), 1)":                                                             "denied_function",
		"SELECT dictGet('rw_cat.d_res' AS n, 'pod_key', 1)":                                                                "denied_function",
		"SELECT * FROM dictionary('rw_cat.d_res')":                                                                         "table_function",
		"SELECT * FROM rw_cat.d_res":                                                                                       "table_not_allowed",
		"SELECT dictGet('rw_cat.d_pod', 'name', 17)":                                                                       "dict_key", // a probe of a derived dictionary
		"SELECT dictHas('rw_cat.d_cluster', 17)":                                                                           "dict_key",
		"SELECT dictGet('rw_cat.d_pod', 'name', resource_id) FROM otel_logs":                                               "dict_key",
		"SELECT dictGet('rw_cat.d_pod', 'name', dictGet('rw_cat.d_res', 'ct', 1))":                                         "dict_key", // not a key attribute
		"SELECT dictGet('rw_cat.d_ns', 'attrs', dictGet('rw_cat.d_pod', 'wl_key', dictGet('rw_cat.d_res', 'pod_key', 1)))": "dict_key", // another level's key
		"SELECT dictGet('rw_cat.d_pod', 'name', dictGetOrDefault('rw_cat.d_res', 'pod_key', 1, 5))":                        "dict_key",
	}
	for sql, want := range cases {
		_, err := p.Prepare(sql)
		rj, ok := AsRejection(err)
		if !ok || rj.Reason != want {
			t.Errorf("%s: got %v, want %s", sql, err, want)
		}
	}
	// without dictionaries every dictionary function is refused, as before
	plain := testPolicy(t)
	if _, err := plain.Prepare("SELECT " + rwPodValue + " FROM otel_logs"); err == nil {
		t.Error("a policy without dictionaries accepted dictGet")
	}
}

func TestDictionaryConfig(t *testing.T) {
	good := CatalogDictionaries("rw_cat")
	bad := map[string]func(ds []*Dictionary){
		"bad name":    func(ds []*Dictionary) { ds[0].Name = "d_res" },
		"bad attr":    func(ds []*Dictionary) { ds[0].Attributes["a b"] = DictAttribute{Type: "UInt8", Default: "0"} },
		"bad type":    func(ds []*Dictionary) { ds[0].Attributes["x"] = DictAttribute{Type: "UInt8; DROP", Default: "0"} },
		"bad default": func(ds []*Dictionary) { ds[0].Attributes["x"] = DictAttribute{Type: "UInt8", Default: "(("} },
		"unknown ref": func(ds []*Dictionary) {
			ds[0].Attributes["x"] = DictAttribute{Type: "UInt8", Default: "0", Ref: "rw_cat.nope"}
		},
		"no namespace":  func(ds []*Dictionary) { ds[0].Namespace = "" },
		"no key":        func(ds []*Dictionary) { ds[0].Cluster = "'qa'" },
		"unreachable":   func(ds []*Dictionary) { ds[2].Name = "rw_cat.d_orphan" },
		"guard refused": func(ds []*Dictionary) { ds[0].Cluster = "file({key})" },
		"guard literal": func(ds []*Dictionary) { ds[0].Cluster = "dictGet('rw_cat.d_pod', 'name', {key})" },
		"no attributes": func(ds []*Dictionary) { ds[5].Attributes = nil },
		"listed twice":  func(ds []*Dictionary) { ds[5].Name = ds[4].Name },
	}
	for name, mut := range bad {
		ds := CatalogDictionaries("rw_cat")
		mut(ds)
		p, _ := NewPolicy("otel", nil, 0)
		if err := p.SetDictionaries(ds); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	p, _ := NewPolicy("otel", nil, 0)
	if err := p.SetDictionaries(good); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(p.DictionaryNames(), ","); got != "rw_cat.d_cluster,rw_cat.d_node,rw_cat.d_ns,rw_cat.d_pod,rw_cat.d_res,rw_cat.d_wl" {
		t.Error(got)
	}
}

// TestDictionaryGuardProperty: for any composition of lookups, a
// restricted caller's rebuilt statement reads no dictionary value outside a
// guard: every dictGet / dictHas in it is either the key of a derived
// lookup, inside a guard's own expression, or the protected branch of an
// if(guard, …, default) / and(guard, …); and a fleet caller's statement is
// the input, rebuilt.
func TestDictionaryGuardProperty(t *testing.T) {
	p := dictPolicy(t)
	rapid.Check(t, func(t *rapid.T) {
		var gen func(depth int) string
		key := func(depth int) string {
			return rapid.SampledFrom([]string{"resource_id", "42", "toUInt64(length(Body))", "(resource_id + 1)"}).Draw(t, "key")
		}
		chain := func(k string) string {
			pod := "dictGet('rw_cat.d_res', 'pod_key', " + k + ")"
			switch rapid.IntRange(0, 6).Draw(t, "level") {
			case 0:
				return pod
			case 1:
				return "dictGet('rw_cat.d_pod', 'name', " + pod + ")"
			case 2:
				return "dictGet('rw_cat.d_ns', 'attrs', dictGet('rw_cat.d_pod', 'ns_key', " + pod + "))"
			case 3:
				return "dictGet('rw_cat.d_wl', 'containers', dictGet('rw_cat.d_pod', 'wl_key', " + pod + "))"
			case 4:
				return "dictHas('rw_cat.d_res', " + k + ")"
			case 5:
				return "dictHas('rw_cat.d_pod', " + pod + ")"
			default:
				return "toString(dictGet('rw_cat.d_res', 'ct', " + k + "))"
			}
		}
		gen = func(depth int) string {
			if depth > 2 || rapid.Bool().Draw(t, "leaf") {
				return "toString(" + chain(key(depth)) + ")"
			}
			switch rapid.IntRange(0, 2).Draw(t, "op") {
			case 0:
				return "concat(" + gen(depth+1) + ", " + gen(depth+1) + ")"
			case 1:
				// a lookup keyed by another lookup's value (root key: any expression)
				return "toString(dictGet('rw_cat.d_res', 'ct', cityHash64(" + gen(depth+1) + ")))"
			default:
				return "if(" + gen(depth+1) + " = '', 'a', " + gen(depth+1) + ")"
			}
		}
		sql := "SELECT " + gen(0) + " AS v FROM otel_logs"
		pr, err := p.Prepare(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		fleetSQL := chp.Format(pr.root)
		fr, err := pr.Finish(Scope{AllClusters: true, AllNamespaces: true})
		if err != nil || fr.SQL != fleetSQL {
			t.Fatalf("fleet: %v\n%s\n%s", err, fr, fleetSQL)
		}
		pr, _ = p.Prepare(sql)
		r, err := pr.Finish(Scope{Clusters: []string{"qa"}, AllNamespaces: true})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		st, err := chp.NewParser(r.SQL).ParseStmts()
		if err != nil {
			t.Fatal(err)
		}
		if bad := unguarded(p, st[0]); bad != "" {
			t.Fatalf("unguarded lookup %s in %s", bad, r.SQL)
		}
	})
}

// unguarded finds a dictionary lookup whose value is not behind a guard.
func unguarded(p *Policy, root chp.Expr) string {
	var bad string
	isGuard := func(e chp.Expr) bool {
		s := chp.Format(e)
		return strings.HasPrefix(s, "(JSONExtractString(dictGet('rw_cat.d_cluster'") && strings.HasSuffix(s, "IN ('qa'))")
	}
	var walk func(n chp.Expr)
	walk = func(n chp.Expr) {
		if bad != "" || n == nil {
			return
		}
		if f, ok := n.(*chp.FunctionExpr); ok && f.Name != nil {
			args, _ := funcArgs(f)
			if (f.Name.Name == "if" && len(args) == 3 || f.Name.Name == "and" && len(args) == 2) && isGuard(args[0]) {
				if inner, ok := args[1].(*chp.FunctionExpr); ok && inner.Name != nil {
					if _, dict := isDictFunc(inner.Name.Name); dict {
						// the protected lookup: down its key chain to the
						// root lookup's key, the caller's expression
						dc, err := p.asDictCall(inner)
						for err == nil && !dc.d.root() {
							dc, err = p.parentOf(dc)
						}
						if err != nil {
							bad = chp.Format(inner)
							return
						}
						walk(dc.key)
						for _, a := range args[2:] {
							walk(a)
						}
						return
					}
				}
			}
			if _, dict := isDictFunc(f.Name.Name); dict {
				bad = chp.Format(f)
				return
			}
		}
		children(n, func(c chp.Expr) bool {
			walk(c)
			return false
		})
	}
	walk(root)
	return bad
}
