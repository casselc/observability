package s3pqexporter

import (
	"context"
	"testing"

	"github.com/casselc/observability/otel-chdb/parquetgo"
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
	off := parquetgo.OffloadStats{Offloaded: 4, OffloadedBytes: 312, Split: 4, Refused: 1, Carried: 6, Dedup: 2}
	reg, err := registerOutcomes(mp, st, func() parquetgo.OffloadStats { return off })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()
	var rm metricdata.ResourceMetrics
	if err := rd.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got, gotOff := map[string]int64{}, map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Unit != "{event}" {
				t.Fatalf("metric %q unit %q", m.Name, m.Unit)
			}
			into := got
			switch m.Name {
			case "s3pq_commit_outcomes":
			case "s3pq_offload":
				into = gotOff
			default:
				t.Fatalf("metric %q", m.Name)
			}
			sum := m.Data.(metricdata.Sum[int64])
			if !sum.IsMonotonic {
				t.Fatal("not a counter")
			}
			for _, dp := range sum.DataPoints {
				o, _ := dp.Attributes.Value("outcome")
				into[o.AsString()] = dp.Value
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
	// s3pq_offload: the Rust edge's OFFLOAD_OUTCOMES, every one a series.
	wantOff := map[string]int64{"offloaded": 4, "offloaded_bytes": 312, "split": 4, "truncated": 0, "redacted": 0,
		"refused": 1, "carried": 6, "dedup": 2}
	if len(gotOff) != len(wantOff) {
		t.Fatal(gotOff)
	}
	for k, v := range wantOff {
		if gotOff[k] != v {
			t.Fatalf("offload %s: %d, want %d (%v)", k, gotOff[k], v, gotOff)
		}
	}
}
