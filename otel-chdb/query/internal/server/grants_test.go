package server

import (
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/golang-jwt/jwt/v5"
)

// D38 / CAST 52, end to end: grants held together are the union of their
// (role, cluster, namespace) tuples. The fake central applies the filters it
// is given, so the counts are what ClickHouse would read.

func TestQueryGrantsAreUnionNotProduct(t *testing.T) {
	f := newBasisFixture(t)
	f.srv.Mapping.Groups["devtools-alice"] = auth.Grant{Clusters: []string{"devtools"}, Namespaces: []string{"dev-alice"}, Roles: []string{"query"}}
	f.srv.Mapping.Groups["shop"] = auth.Grant{Clusters: []string{"prod-a"}, Namespaces: []string{"shop"}, Roles: []string{"query"}}
	f.srv.Mapping.Groups["b-plan"] = auth.Grant{Clusters: []string{"prod-b"}, Namespaces: []string{"*"}, Roles: []string{"plan"}}
	ts := t0.Add(-30 * time.Minute).UnixNano()
	for _, c := range []string{"prod-a", "prod-b", "devtools"} {
		for _, n := range []string{"shop", "dev-alice", "pay"} {
			f.rows.add(row{Cluster: c, Namespace: n, TsNs: ts, RecvNs: ts})
		}
	}
	tok := f.token(jwt.MapClaims{"groups": []any{"devtools-alice", "shop", "b-plan"}})
	code, out, n := f.count(t, tok, map[string]any{})
	if code != 200 || n != 2 {
		// the old product: {devtools, prod-a} × {dev-alice, shop} = 4 rows;
		// with b-plan's cluster and "*" it would have been every prod-b row too
		t.Fatalf("want the 2 rows of the two query grants, got %d (%d %v)", n, code, out)
	}
	flt := f.rows.calls[len(f.rows.calls)-1].Get("additional_table_filters")
	if !strings.Contains(flt, " OR ") || strings.Contains(flt, "prod-b") {
		t.Fatalf("filter %q", flt)
	}
	recs := f.sink.Snapshot()
	var dec *struct{ pairs []string }
	for i := len(recs) - 1; i >= 0; i-- {
		if recs[i].Event == "decision" && recs[i].Action == "query" {
			dec = &struct{ pairs []string }{recs[i].Pairs}
			break
		}
	}
	if dec == nil || strings.Join(dec.pairs, ",") != "devtools/dev-alice,prod-a/shop" {
		t.Fatalf("audit pairs %v", dec)
	}
	// narrowed to one cluster: that cluster's grant only
	if _, _, n := f.count(t, tok, map[string]any{"clusters": []string{"prod-a"}}); n != 1 {
		t.Fatalf("narrowed to prod-a: %d", n)
	}
	// a cluster only the plan grant names is not in the query scope
	if code, out, _ := f.count(t, tok, map[string]any{"clusters": []string{"prod-b"}}); code != 403 || out["error"] != "cluster_not_in_scope" {
		t.Fatalf("prod-b through a plan grant: %d %v", code, out)
	}
}

func TestPlanUsesOnlyWholeClusterPlanGrants(t *testing.T) {
	f := newFixture(t)
	f.srv.Mapping.Groups["a-plan"] = auth.Grant{Clusters: []string{"prod-a"}, Namespaces: []string{"*"}, Roles: []string{"plan"}}
	f.srv.Mapping.Groups["b-query"] = auth.Grant{Clusters: []string{"prod-b"}, Namespaces: []string{"*"}, Roles: []string{"query"}}
	f.srv.Mapping.Groups["b-shop-plan"] = auth.Grant{Clusters: []string{"prod-b"}, Namespaces: []string{"shop"}, Roles: []string{"plan"}}
	req := map[string]any{"signal": "logs", "from": t0.Add(-time.Hour).UnixNano(), "to": t0.UnixNano()}
	tok := f.token(jwt.MapClaims{"groups": []any{"a-plan", "b-query", "b-shop-plan"}})
	code, out := f.post(t, "/v1/plan", tok, req)
	if code != 200 || strings.Join(toStrings(out["clusters"]), ",") != "prod-a" {
		// the old product: roles {plan, query} × clusters {prod-a, prod-b} ×
		// namespaces {*, shop} planned (presigned) prod-b's objects too
		t.Fatalf("want prod-a only: %d %v", code, out["clusters"])
	}
	for _, o := range out["objects"].([]any) {
		if k := o.(map[string]any)["key"].(string); !strings.HasPrefix(k, root+"/prod-a/") {
			t.Fatalf("presigned %s", k)
		}
	}
	req["clusters"] = []string{"prod-b"}
	if code, out := f.post(t, "/v1/plan", tok, req); code != 403 || out["error"] != "namespace_scope_needs_filtering_reader" {
		t.Fatalf("prod-b through a namespace plan grant: %d %v", code, out)
	}
	// only a namespace grant for plan: refused, not planned over the cluster
	only := f.token(jwt.MapClaims{"groups": []any{"b-query", "b-shop-plan"}})
	delete(req, "clusters")
	if code, out := f.post(t, "/v1/plan", only, req); code != 403 || out["error"] != "namespace_scope_needs_filtering_reader" {
		t.Fatalf("namespace-only plan grant: %d %v", code, out)
	}
}
