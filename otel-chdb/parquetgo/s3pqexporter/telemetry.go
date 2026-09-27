package s3pqexporter

import (
	"context"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// s3pq_commit_outcomes_total{outcome}: the commit protocol's outcomes
// (AMBIGUITY.md S1, E1), with the Rust edge's name and label values
// (../../otap-rs/src/commit_metrics.rs, which has the table of what each
// means). committed, resolved_own, known, unresolved and inconsistent end an
// append; learned_other, resent and tombstoned are steps on the way.
var outcomes = [...]string{
	"committed", "resolved_own", "known", "learned_other", "resent", "tombstoned", "unresolved", "inconsistent",
}

const meterScope = "github.com/casselc/observability/otel-chdb/parquetgo/s3pqexporter"

// outcomeCounts reads the lanes' counters, in `outcomes` order.
func outcomeCounts(st *commit.Stats) [len(outcomes)]int64 {
	return [...]int64{
		st.Committed.Load(), st.ResolvedOwn.Load(), st.KnownSkipped.Load(), st.LearnedOther.Load(),
		st.Resent.Load(), st.Halted.Load(), st.Unresolved.Load(), st.Inconsistent.Load(),
	}
}

// registerOutcomes observes st as the counter. One registration per edge
// (the edge's traces, logs and metrics pipelines share its lanes).
func registerOutcomes(mp metric.MeterProvider, st *commit.Stats) (metric.Registration, error) {
	m := mp.Meter(meterScope)
	c, err := m.Int64ObservableCounter("s3pq_commit_outcomes",
		metric.WithDescription("Commit protocol events (create-only PUT and HEAD resolution), by outcome."),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, err
	}
	var opts [len(outcomes)]metric.ObserveOption
	for i, o := range outcomes {
		opts[i] = metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", o)))
	}
	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		n := outcomeCounts(st)
		for i := range outcomes {
			o.ObserveInt64(c, n[i], opts[i])
		}
		return nil
	}, c)
}
