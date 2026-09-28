package sqlscope

import (
	"strings"
	"testing"
)

func basisPolicy(t *testing.T) *Policy {
	t.Helper()
	p, err := NewPolicy("otel", []*Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", ReceivedColumn: "received_at", Scope: "columns",
			Cluster: "`__hdx_materialized_k8s.cluster.name`", Namespace: "`__hdx_materialized_k8s.namespace.name`"},
		{Name: "otel_traces", TimeColumn: "Timestamp", Scope: "columns", Cluster: "ResourceAttributes['k8s.cluster.name']",
			Namespace: "ResourceAttributes['k8s.namespace.name']"}, // no received column
		{Name: "otel_spans", TimeColumn: "Timestamp", ReceivedColumn: "received_at", Scope: "catalog"}, // no cluster expression
		{Name: "otel_metrics_series", Scope: "fleet", TimeColumn: "Timestamp", ReceivedColumn: "received_at"},
		{Database: "system", Name: "tables", Scope: "metadata"},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// A basis becomes one more conjunct of the table filter, per cluster, with
// a strict upper bound (and an inclusive lower one for a delta).
func TestBasisFilter(t *testing.T) {
	p := basisPolicy(t)
	s := Scope{Clusters: []string{"prod-a", "prod-b"}, AllNamespaces: true,
		Received: &Received{Before: map[string]uint64{"prod-b": 20, "prod-a": 10}}}
	r := mustFinish(t, p, "SELECT count() FROM otel_logs", s)
	want := "`__hdx_materialized_k8s.cluster.name` IN ('prod-a', 'prod-b') AND ((`__hdx_materialized_k8s.cluster.name` = 'prod-a' AND received_at < fromUnixTimestamp64Nano(toInt64(10), 'UTC')) OR (`__hdx_materialized_k8s.cluster.name` = 'prod-b' AND received_at < fromUnixTimestamp64Nano(toInt64(20), 'UTC')))"
	if got := r.Filters["otel.otel_logs"]; got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	s.Received.From = map[string]uint64{"prod-a": 5, "prod-b": 20}
	r = mustFinish(t, p, "SELECT count() FROM otel_logs", s)
	if got := r.Filters["otel.otel_logs"]; !strings.Contains(got, "received_at < fromUnixTimestamp64Nano(toInt64(10), 'UTC') AND received_at >= fromUnixTimestamp64Nano(toInt64(5), 'UTC')") {
		t.Fatal(got)
	}
	// a fleet bound needs no cluster column (a fleet caller)
	fleet := Scope{AllClusters: true, AllNamespaces: true, Received: &Received{Before: map[string]uint64{"*": 7}}}
	r = mustFinish(t, p, "SELECT count() FROM otel_metrics_series", fleet)
	if got := r.Filters["otel.otel_metrics_series"]; got != "(received_at < fromUnixTimestamp64Nano(toInt64(7), 'UTC'))" {
		t.Fatal(got)
	}
	// metadata tables are schema: no bound
	r = mustFinish(t, p, "SELECT name FROM system.tables", s)
	if _, ok := r.Filters["system.tables"]; ok {
		t.Fatal("a metadata table filtered")
	}
	// a table that cannot be served at a basis is refused, not read unbounded
	for sql, sc := range map[string]Scope{
		"SELECT count() FROM otel_traces":                          s,                                                              // no received column
		"SELECT count() FROM otel_spans":                           {AllClusters: true, AllNamespaces: true, Received: s.Received}, // per-cluster bound, no cluster expression
		"SELECT * FROM otel_logs JOIN otel_traces USING (TraceId)": s,
	} {
		pr, err := p.Prepare(sql)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pr.Finish(sc)
		if rj, ok := AsRejection(err); !ok || rj.Reason != "basis_unservable" {
			t.Errorf("%s: %v", sql, err)
		}
	}
	// the delta count counts only what the filters admit, per table
	r = mustFinish(t, p, "SELECT count() FROM otel_logs", s)
	dc, err := p.DeltaCount(r)
	if err != nil || dc.SQL != "SELECT 'otel.otel_logs' AS t, count() AS n FROM otel.otel_logs" {
		t.Fatal(dc, err)
	}
}
