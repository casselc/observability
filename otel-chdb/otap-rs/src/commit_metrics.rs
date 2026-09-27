//! `s3pq_commit_outcomes_total{outcome}`: the commit protocol's outcomes as
//! an engine metric (AMBIGUITY.md S1, E1), with the same name and label
//! values as the Go edge (`../parquetgo/s3pqexporter/telemetry.go`).
//!
//! The counts are `runner::Stats`, which the lanes already keep; this module
//! only turns them into deltas for the engine's telemetry, which clears a
//! measurement bucket once it has reported it.
//!
//! Outcomes, one per protocol event (`runner::append`):
//!
//! | outcome | event | terminal for the append? |
//! |---|---|---|
//! | `committed` | 200 on the create-only PUT | yes: ours |
//! | `resolved_own` | 412 or no answer, then the HEAD found our content key and epoch | yes: ours |
//! | `known` | the lane already found this content committed (a retry of a request whose answer was lost) | yes: ours |
//! | `learned_other` | the HEAD found another batch in the slot: ours moves to the next slot | no |
//! | `resent` | no answer, then the HEAD found the slot free: the PUT is sent again to the same slot | no |
//! | `tombstoned` | the HEAD found a tombstone: the consumer closed the log, a new epoch | no |
//! | `unresolved` | the HEAD failed or timed out: the slot is kept, the request NACKed and retried | yes: unknown |
//! | `inconsistent` | 412, then the HEAD found nothing (store not read-after-write): kept, NACKed | yes: unknown |
//!
//! An append ends in exactly one terminal outcome, so their sum is the
//! appends; the others count steps on the way.

use crate::runner::Stats;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_telemetry::error::Error;
use otel_arrow_dfe_telemetry::instrument::Counter;
use otel_arrow_dfe_telemetry::metrics::MeasurementMetricSet;
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use otel_arrow_dfe_telemetry_macros::{AttributeEnum, attribute_set, metric_set};

#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub enum CommitOutcome {
    Committed,
    ResolvedOwn,
    Known,
    LearnedOther,
    Resent,
    Tombstoned,
    Unresolved,
    Inconsistent,
}

pub const OUTCOMES: [CommitOutcome; 8] = [
    CommitOutcome::Committed,
    CommitOutcome::ResolvedOwn,
    CommitOutcome::Known,
    CommitOutcome::LearnedOther,
    CommitOutcome::Resent,
    CommitOutcome::Tombstoned,
    CommitOutcome::Unresolved,
    CommitOutcome::Inconsistent,
];

#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub struct CommitOutcomeAttributes {
    pub outcome: CommitOutcome,
}

/// Commit protocol outcomes, by outcome.
#[metric_set(name = "exporter.s3pq", measurement_attributes = CommitOutcomeAttributes)]
#[derive(Debug, Default, Clone)]
pub struct CommitOutcomeMetrics {
    /// Commit protocol events (create-only PUT and HEAD resolution), by outcome.
    #[metric(unit = "{event}")]
    pub s3pq_commit_outcomes: Counter<u64>,
}

/// The counters' current values, in `OUTCOMES` order.
pub fn totals(s: &Stats) -> [u64; 8] {
    [
        s.committed.get(),
        s.resolved_own.get(),
        s.known_skipped.get(),
        s.learned_other.get(),
        s.resent.get(),
        s.halted.get(),
        s.unresolved.get(),
        s.inconsistent.get(),
    ]
}

/// The engine's metric set plus what it has been given so far.
pub struct CommitOutcomes {
    set: MeasurementMetricSet<CommitOutcomeMetrics>,
    last: [u64; 8],
}

impl CommitOutcomes {
    pub fn register(ctx: &PipelineContext) -> Self {
        CommitOutcomes { set: CommitOutcomeMetrics::register(ctx), last: [0; 8] }
    }

    /// Adds what `stats` counted since the last call, and reports. Every
    /// outcome is touched, at 0 too, so each series exists from the first
    /// report (an alert on `increase()` needs it).
    pub fn report(&mut self, stats: &Stats, reporter: &mut MetricsReporter) -> Result<(), Error> {
        let now = totals(stats);
        for (i, o) in OUTCOMES.iter().enumerate() {
            let d = now[i].saturating_sub(self.last[i]);
            self.set.with(CommitOutcomeAttributes { outcome: *o }).s3pq_commit_outcomes.add(d);
        }
        self.last = now;
        reporter.report_measurement(&mut self.set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_telemetry::attributes::AttributeEnum as _;

    #[test]
    fn label_values_match_the_go_edge() {
        // ../parquetgo/s3pqexporter/telemetry.go `outcomes`: same names, same order.
        assert_eq!(
            CommitOutcome::VARIANTS,
            &["committed", "resolved_own", "known", "learned_other", "resent", "tombstoned", "unresolved", "inconsistent"]
        );
    }

    #[test]
    fn totals_follow_the_stats() {
        let s = Stats::default();
        s.committed.set(3);
        s.halted.set(1);
        s.unresolved.set(2);
        assert_eq!(totals(&s), [3, 0, 0, 0, 0, 1, 2, 0]);
    }
}
