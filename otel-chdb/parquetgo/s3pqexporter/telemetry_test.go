package s3pqexporter

import (
	"context"
	"testing"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestCommitOutcomesMetric(t *testing.T) {
	st := &commit.Stats{}
	st.Committed.Add(3)
	st.ResolvedOwn.Add(1)
	st.Halted.Add(1)
	st.Unresolved.Add(2)
	rd := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rd))
	reg, err := registerOutcomes(mp, st)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()
	var rm metricdata.ResourceMetrics
	if err := rd.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "s3pq_commit_outcomes" || m.Unit != "{event}" {
				t.Fatalf("metric %q unit %q", m.Name, m.Unit)
			}
			sum := m.Data.(metricdata.Sum[int64])
			if !sum.IsMonotonic {
				t.Fatal("not a counter")
			}
			for _, dp := range sum.DataPoints {
				o, _ := dp.Attributes.Value("outcome")
				got[o.AsString()] = dp.Value
			}
		}
	}
	// Every outcome is a series from the start (0 included), with the Rust
	// edge's label values (otap-rs/src/commit_metrics.rs).
	want := map[string]int64{"committed": 3, "resolved_own": 1, "known": 0, "learned_other": 0, "resent": 0,
		"tombstoned": 1, "unresolved": 2, "inconsistent": 0}
	if len(got) != len(want) {
		t.Fatal(got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: %d, want %d (%v)", k, got[k], v, got)
		}
	}
}
