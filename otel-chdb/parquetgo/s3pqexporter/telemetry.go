package s3pqexporter

import (
	"context"

	"github.com/casselc/observability/otel-chdb/parquetgo"
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

// s3pq_offload_total{outcome}: what the payload offloader did
// (../offload.go, DECISIONS.md D36), with the Rust edge's names
// (../../otap-rs/src/commit_metrics.rs OFFLOAD_OUTCOMES): values offloaded
// (and their bytes), split, truncated, redacted; requests refused over the
// cap; payloads carried in committed objects, and referenced but not
// carried (already sent in the lane's epoch).
var offloadOutcomes = [...]string{
	"offloaded", "offloaded_bytes", "split", "truncated", "redacted", "refused", "carried", "dedup",
}

// offloadCounts is s in `offloadOutcomes` order.
func offloadCounts(s parquetgo.OffloadStats) [len(offloadOutcomes)]int64 {
	return [...]int64{int64(s.Offloaded), int64(s.OffloadedBytes), int64(s.Split), int64(s.Truncated),
		int64(s.Redacted), int64(s.Refused), int64(s.Carried), int64(s.Dedup)}
}

// registerOutcomes observes st as s3pq_commit_outcomes and off (nil: none)
// as s3pq_offload. One registration per edge (the edge's traces, logs and
// metrics pipelines share its lanes).
func registerOutcomes(mp metric.MeterProvider, st *commit.Stats, off func() parquetgo.OffloadStats) (metric.Registration, error) {
	m := mp.Meter(meterScope)
	c, err := m.Int64ObservableCounter("s3pq_commit_outcomes",
		metric.WithDescription("Commit protocol events (create-only PUT and HEAD resolution), by outcome."),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, err
	}
	oc, err := m.Int64ObservableCounter("s3pq_offload",
		metric.WithDescription("Payload offloader events (values offloaded, split, truncated or redacted, requests refused, payloads carried or deduplicated, offloaded bytes), by outcome."),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, err
	}
	var opts [len(outcomes)]metric.ObserveOption
	for i, o := range outcomes {
		opts[i] = metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", o)))
	}
	var oopts [len(offloadOutcomes)]metric.ObserveOption
	for i, o := range offloadOutcomes {
		oopts[i] = metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", o)))
	}
	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		n := outcomeCounts(st)
		for i := range outcomes {
			o.ObserveInt64(c, n[i], opts[i])
		}
		if off != nil {
			n := offloadCounts(off())
			for i := range offloadOutcomes {
				o.ObserveInt64(oc, n[i], oopts[i])
			}
		}
		return nil
	}, c, oc)
}
