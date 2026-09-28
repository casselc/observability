package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/golang-jwt/jwt/v5"
)

// perClusterDocs: prod-b's stalled logs lane holds the fleet value an hour
// back; prod-a's logs are 20 s behind, its traces 3 min.
func perClusterDocs(t *testing.T, f *fixture) {
	t.Helper()
	policy, err := sqlscope.NewPolicy("otel", []*sqlscope.Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", Scope: "columns", Cluster: "`__hdx_materialized_k8s.cluster.name`", Namespace: "`__hdx_materialized_k8s.namespace.name`", Signals: []string{"logs"}},
		{Name: "otel_traces", TimeColumn: "Timestamp", Scope: "columns", Cluster: "ResourceAttributes['k8s.cluster.name']", Namespace: "ResourceAttributes['k8s.namespace.name']", Signals: []string{"traces"}},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.srv.Policy = policy
	ns := func(d time.Duration) uint64 { return uint64(t0.Add(d).UnixNano()) }
	wall := uint64(t0.Add(-10 * time.Second).UnixMilli())
	put := func(k string, v any) {
		b, _ := json.Marshal(v)
		f.mem.Put(root+"/_consumer/"+k, b, nil, t0)
	}
	put("watermark.json", completeness.Doc{Format: 2, CompleteThroughNs: ns(-time.Hour), WallMs: wall, ListCapNs: ns(-15 * time.Second),
		Holding:  []completeness.LaneWm{{Lane: "prod-b/pub-0/logs", WmNs: ns(-time.Hour)}, {Lane: "prod-a/pub-0/traces", WmNs: ns(-3 * time.Minute)}},
		Clusters: map[string]uint64{"prod-a": ns(-3 * time.Minute), "prod-b": ns(-time.Hour)},
		Signals:  map[string]uint64{"logs": ns(-time.Hour), "traces": ns(-3 * time.Minute)}, UnlistedSignalsNs: ns(-15 * time.Second)})
	put("watermark/prod-a.json", completeness.ClusterDoc{Format: 2, Cluster: "prod-a", CompleteThroughNs: ns(-3 * time.Minute), WallMs: wall,
		Signals: map[string]uint64{"logs": ns(-20 * time.Second), "traces": ns(-3 * time.Minute)},
		Holding: []completeness.LaneWm{{Lane: "prod-a/pub-0/traces", WmNs: ns(-3 * time.Minute)}, {Lane: "prod-a/pub-0/logs", WmNs: ns(-20 * time.Second)}}})
	put("watermark/prod-b.json", completeness.ClusterDoc{Format: 2, Cluster: "prod-b", CompleteThroughNs: ns(-time.Hour), WallMs: wall,
		Signals: map[string]uint64{"logs": ns(-time.Hour)}, Holding: []completeness.LaneWm{{Lane: "prod-b/pub-0/logs", WmNs: ns(-time.Hour)}}})
}

// TestQueryPerClusterWatermark (D29): a statement scoped to prod-a (by its
// token, or narrowed with `clusters`) and reading logs is labelled with
// prod-a's logs value and is complete, and the label says which scope it
// used; anything including prod-b is not. A cluster outside the token is
// refused; the lake plan follows the same rule.
func TestQueryPerClusterWatermark(t *testing.T) {
	f := newFixture(t)
	perClusterDocs(t, f)
	ns := func(d time.Duration) uint64 { return uint64(t0.Add(d).UnixNano()) }
	sre := f.token(jwt.MapClaims{"groups": []any{"sre"}})
	// settled through complete_through − 60 s: a window ending 100 s ago
	window := map[string]any{"from": t0.Add(-time.Hour).UnixNano(), "to": t0.Add(-100 * time.Second).UnixNano()}
	q := func(tok, sql string, clusters []string) (int, map[string]any) {
		body := map[string]any{"sql": sql, "window": window}
		if clusters != nil {
			body["clusters"] = clusters
		}
		return f.post(t, "/v1/query", tok, body)
	}
	for _, tc := range []struct {
		name, tok, sql string
		clusters       []string
		want           string
		ct             uint64
		fleet          bool
	}{
		{"prod-a's token, logs", f.token(teamA), "SELECT count() FROM otel_logs", nil, "complete", ns(-20 * time.Second), false},
		{"prod-a's token, traces", f.token(teamA), "SELECT count() FROM otel_traces", nil, "partial", ns(-3 * time.Minute), false},
		{"prod-a's token, both tables", f.token(teamA), "SELECT count() FROM otel_logs UNION ALL SELECT count() FROM otel_traces", nil, "partial", ns(-3 * time.Minute), false},
		{"fleet token narrowed to prod-a", sre, "SELECT count() FROM otel_logs", []string{"prod-a"}, "complete", ns(-20 * time.Second), false},
		{"fleet token", sre, "SELECT count() FROM otel_logs", nil, "partial", ns(-time.Hour), true},
		{"fleet token, both clusters", sre, "SELECT count() FROM otel_logs", []string{"prod-a", "prod-b"}, "partial", ns(-time.Hour), false},
	} {
		code, out := q(tc.tok, tc.sql, tc.clusters)
		if code != 200 || out["completeness"] != tc.want || uint64(out["complete_through_ns"].(float64)) != tc.ct {
			t.Errorf("%s: %d %v %v, want %s at %d", tc.name, code, out["completeness"], out["complete_through_ns"], tc.want, tc.ct)
			continue
		}
		sc, _ := out["watermark"].(map[string]any)["scope"].(map[string]any)
		if sc == nil || (sc["clusters"].([]any)[0] == "*") != tc.fleet {
			t.Errorf("%s: scope %v", tc.name, sc)
		}
	}
	// narrowed: rows are filtered to the narrowed cluster, and the audit says so
	n := len(f.ch.sqls)
	if _, out := q(sre, "SELECT count() FROM otel_logs", []string{"prod-a"}); out["completeness"] != "complete" {
		t.Fatalf("%v", out)
	}
	if flt := f.ch.calls[n].Get("additional_table_filters"); !strings.Contains(flt, `IN (\'prod-a\')`) || strings.Contains(flt, "prod-b") {
		t.Fatalf("filter %q", flt)
	}
	recs := f.sink.Snapshot()
	if d := recs[len(recs)-2]; d.Event != "decision" || strings.Join(d.Clusters, ",") != "prod-a" {
		t.Fatalf("audit %+v", d)
	}
	// the holding lanes are the scope's
	_, out := q(f.token(teamA), "SELECT count() FROM otel_traces", nil)
	if h := out["watermark"].(map[string]any)["holding"].([]any); len(h) != 1 || h[0].(map[string]any)["lane"] != "prod-a/pub-0/traces" {
		t.Fatalf("holding %v", h)
	}
	// refusals
	if code, out := q(f.token(teamA), "SELECT count() FROM otel_logs", []string{"prod-b"}); code != 403 || out["error"] != "cluster_not_in_scope" {
		t.Fatalf("%d %v", code, out)
	}
	if code, out := q(sre, "SELECT count() FROM otel_logs", []string{"Prod_B"}); code != 400 || out["error"] != "bad_cluster" {
		t.Fatalf("%d %v", code, out)
	}
	// the lake plan: prod-a's logs are settled, the fleet's are not
	req := map[string]any{"signal": "logs", "from": t0.Add(-time.Hour).UnixNano(), "to": t0.Add(-100 * time.Second).UnixNano()}
	if code, out := f.post(t, "/v1/plan", f.token(teamA), req); code != 200 || out["completeness"] != "complete" {
		t.Fatalf("plan prod-a: %d %v", code, out)
	}
	if code, out := f.post(t, "/v1/plan", sre, req); code != 200 || out["completeness"] != "partial" {
		t.Fatalf("plan fleet: %d %v", code, out)
	}
}
