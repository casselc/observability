//! The late split (DECISIONS.md D31, `../FORMAT.md` §2.2), as the Go edge
//! does it (`../parquetgo/edge/late.go`): an object's `oscope-min-time` /
//! `oscope-max-time` span all its rows, so one old row (a late batch, a
//! replay, a skewed clock) would stretch its range over every window in
//! between. With a bound B > 0, a traces or logs request whose rows reach
//! back more than B before its NEWEST row becomes two objects in its lane:
//! the bulk (event time >= max − B) and the late rows (the rest), each with
//! its own range, content key, `oscope-part` and `oscope-late-after`.
//!
//! The cutoff is relative to the request's newest row, not to received_at:
//! a function of the request's rows and B alone, so a retry or a replay
//! splits the same way and finds its parts by their keys. Row times are the
//! walk's (`Stats::see`): a span's start, a log record's time (its observed
//! time when 0), compared as u64 as Go does.

use crate::Signal;
use crate::batch::{EncodeError, Encoder, Flat};
use crate::flatten::Stats;
use crate::proto;
use arrow::array::{Array, ArrayRef, BooleanArray, TimestampNanosecondArray, UInt64Array};
use std::collections::HashMap;

/// Which rows of a split request an object holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    Bulk,
    Late,
}

impl Part {
    pub fn name(self) -> &'static str {
        match self {
            Part::Bulk => proto::PART_BULK,
            Part::Late => proto::PART_LATE,
        }
    }
}

/// The split's cutoff (max − after) when at least one row is below it;
/// `None` for after 0, no rows, or rows all within `after` of the newest
/// (parquetgo `edge.LateCut`).
pub fn late_cut(ts: impl Iterator<Item = u64> + Clone, after_ns: u64) -> Option<u64> {
    if after_ns == 0 {
        return None;
    }
    let hi = ts.clone().max()?;
    let cut = hi.checked_sub(after_ns)?;
    ts.into_iter().any(|t| t < cut).then_some(cut)
}

/// The namespace name a split part's content key is taken under:
/// "{signal}/{part}/{bound ns}" (parquetgo `edge.SplitContent`); signals
/// hold no '/', so it never equals an unsplit request's.
pub fn part_namespace(signal: Signal, part: Part, after_ns: u64) -> String {
    format!("{}/{}/{}", signal.name(), part.name(), after_ns)
}

impl Encoder {
    /// Splits a traces or logs object at max − `after_ns` into its bulk and
    /// late parts (in that order); anything else, or nothing to split, is
    /// returned whole. `key` gives each part's content key from its
    /// namespace name (`part_namespace`) and columns.
    pub fn split_late(&self, f: Flat, after_ns: u64, key: impl Fn(&str, &[ArrayRef]) -> String) -> Result<Vec<Flat>, EncodeError> {
        if after_ns == 0 || f.stats.rows == 0 || !matches!(f.signal, Signal::Traces | Signal::Logs) {
            return Ok(vec![f]);
        }
        let sc = self.schemas(f.signal);
        let col = |name: &str| sc.arrow.index_of(name).map_err(|e| EncodeError(format!("late split: {e}")));
        let ts = f.cols[col("Timestamp")?]
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| EncodeError("late split: Timestamp is not ns".into()))?;
        let ts: Vec<u64> = ts.values().iter().map(|&v| v as u64).collect();
        let Some(cut) = late_cut(ts.iter().copied(), after_ns) else {
            return Ok(vec![f]);
        };
        let ids = f.cols[col("resource_id")?]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| EncodeError("late split: resource_id is not UInt64".into()))?;
        let mut out = Vec::with_capacity(2);
        for part in [Part::Bulk, Part::Late] {
            let keep: Vec<bool> = ts.iter().map(|&t| (t < cut) == (part == Part::Late)).collect();
            let mask = BooleanArray::from(keep.clone());
            let cols = f
                .cols
                .iter()
                .map(|c| arrow::compute::filter(c.as_ref(), &mask))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| EncodeError(format!("late split: {e}")))?;
            // The part's stats and each resource's first row, in walk
            // order, as the Go walk skipping the other part's rows sees them.
            let mut stats = Stats::default();
            let mut first: HashMap<u64, u32> = HashMap::new();
            for (i, &t) in ts.iter().enumerate().filter(|(i, _)| keep[*i]) {
                let _ = first.entry(ids.value(i)).or_insert(stats.rows as u32);
                stats.see(t);
                stats.rows += 1;
            }
            let mut resources: Vec<_> =
                f.resources.iter().filter_map(|(c, _)| first.get(&c.id).map(|&r| (c.clone(), r))).collect();
            resources.sort_by_key(|r| r.1);
            let content = key(&part_namespace(f.signal, part, after_ns), &cols);
            out.push(Flat { signal: f.signal, content, cols, stats, announce: Vec::new(), resources, split: Some((part, after_ns)) });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{Format, Input, content_hash_named, content_hash_otlp};
    use crate::encode::ParquetOptions;
    use crate::flatten::Envelope;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value::Value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};
    use prost::Message;

    const T: u64 = 1_790_000_000_000_000_000;
    const MIN: u64 = 60_000_000_000;

    #[test]
    fn late_cut_matches_go() {
        let b = 15 * MIN;
        let cases: &[(&str, &[u64], u64, Option<u64>)] = &[
            ("off", &[T, T - 10 * b], 0, None),
            ("no rows", &[], b, None),
            ("one row", &[T], b, None),
            ("exactly at the bound is bulk", &[T, T - b], b, None),
            ("one ns past the bound is late", &[T, T - b - 1], b, Some(T - b)),
            ("order does not matter", &[T - b - 1, T, T - 1], b, Some(T - b)),
            ("all old but close together", &[T - 100 * b, T - 100 * b + 5], b, None),
            ("newest below the bound", &[b - 1, 0], b, None),
            ("a zero timestamp is late", &[T, 0], b, Some(T - b)),
        ];
        for (name, ts, after, want) in cases {
            assert_eq!(late_cut(ts.iter().copied(), *after), *want, "{name}");
        }
    }

    fn res(svc: &str) -> Option<Resource> {
        Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue { value: Some(Value::StringValue(svc.into())) }),
            }],
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        })
    }

    /// One span per time, two resources: the first holds the even spans.
    fn traces(ts: &[u64]) -> Vec<u8> {
        let rs = |r: usize| ResourceSpans {
            resource: res(&format!("svc-{r}")),
            schema_url: String::new(),
            scope_spans: vec![ScopeSpans {
                scope: None,
                schema_url: String::new(),
                spans: ts
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| i % 2 == r)
                    .map(|(i, &t)| Span {
                        trace_id: vec![i as u8; 16],
                        span_id: vec![2; 8],
                        name: format!("op-{i}"),
                        start_time_unix_nano: t,
                        end_time_unix_nano: t + 1000,
                        ..Default::default()
                    })
                    .collect(),
            }],
        };
        TracesData { resource_spans: vec![rs(0), rs(1)] }.encode_to_vec()
    }

    fn split(enc: &mut Encoder, sig: Signal, bytes: &[u8], after: u64) -> Vec<Flat> {
        let f = enc.flatten(&Input::Otlp(sig, bytes)).expect("flatten");
        enc.split_late(f, after, |ns, _| content_hash_named(ns, bytes)).expect("split")
    }

    fn times(enc: &Encoder, f: &Flat) -> Vec<u64> {
        let i = enc.schemas(f.signal).arrow.index_of("Timestamp").unwrap();
        let a = f.cols[i].as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        a.values().iter().map(|&v| v as u64).collect()
    }

    /// Two parts, each with its rows, honest stats, the Go edge's content
    /// keys and metadata; resources re-anchored on each part's first rows.
    #[test]
    fn splits_traces_like_the_go_edge() {
        let b = 15 * MIN;
        let hr = 60 * MIN;
        let ts = [T, T - 1000, T - 24 * hr, T - b, T - 5, T - 24 * hr + 7, T - 2 * hr];
        let bytes = traces(&ts);
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let parts = split(&mut enc, Signal::Traces, &bytes, b);
        assert_eq!(parts.len(), 2);
        let (bulk, late) = (&parts[0], &parts[1]);
        assert_eq!(bulk.split, Some((Part::Bulk, b)));
        assert_eq!(late.split, Some((Part::Late, b)));
        // walk order: resource 0's spans (0, 2, 4, 6), then resource 1's
        assert_eq!(times(&enc, bulk), vec![T, T - 5, T - 1000, T - b]);
        assert_eq!(times(&enc, late), vec![T - 24 * hr, T - 2 * hr, T - 24 * hr + 7]);
        assert_eq!((bulk.stats.rows, bulk.stats.min_ts, bulk.stats.max_ts), (4, T - b, T));
        assert_eq!((late.stats.rows, late.stats.min_ts, late.stats.max_ts), (3, T - 24 * hr, T - 2 * hr));
        assert_eq!(bulk.content, content_hash_named(&format!("traces/bulk/{b}"), &bytes));
        assert_eq!(late.content, content_hash_named(&format!("traces/late/{b}"), &bytes));
        assert_ne!(bulk.content, content_hash_otlp(Signal::Traces, &bytes));
        // each part's resources at their first row
        for p in &parts {
            let ids = p.cols[enc.schemas(p.signal).arrow.index_of("resource_id").unwrap()]
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec();
            assert_eq!(p.resources.len(), 2);
            for (c, row) in &p.resources {
                assert_eq!(ids.iter().position(|&x| x == c.id), Some(*row as usize));
            }
        }
        // the encoded objects carry the part and the bound, in the footer too
        let env = Envelope { producer: "p".into(), epoch: "e".into(), batch: 0, received_ns: T };
        let o = enc.encode(late, &env).expect("encode");
        assert_eq!(o.meta.get(proto::META_PART).map(String::as_str), Some("late"));
        assert_eq!(o.meta.get(proto::META_LATE_AFTER), Some(&b.to_string()));
        assert_eq!(o.meta.get(proto::META_ROWS).map(String::as_str), Some("3"));
        assert_eq!(o.meta.get(proto::META_MIN_TIME), Some(&(T - 24 * hr).to_string()));
        // nothing to split: the request whole, its own key, no part
        let whole = split(&mut enc, Signal::Traces, &traces(&[T, T - b]), b);
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0].split, None);
        let o = enc.encode(&whole[0], &env).expect("encode");
        assert!(!o.meta.contains_key(proto::META_PART));
        // off
        assert_eq!(split(&mut enc, Signal::Traces, &bytes, 0).len(), 1);
    }

    /// Logs: the observed time stands in for a zero time; an all-late
    /// request whose rows are close together stays whole.
    #[test]
    fn logs_boundary_and_all_late() {
        let b = MIN;
        let logs = |ts: &[u64]| {
            LogsData {
                resource_logs: vec![ResourceLogs {
                    resource: res("svc"),
                    schema_url: String::new(),
                    scope_logs: vec![ScopeLogs {
                        scope: None,
                        schema_url: String::new(),
                        log_records: ts
                            .iter()
                            .enumerate()
                            .map(|(i, &t)| {
                                if i == 0 {
                                    LogRecord { observed_time_unix_nano: t, ..Default::default() }
                                } else {
                                    LogRecord { time_unix_nano: t, ..Default::default() }
                                }
                            })
                            .collect(),
                    }],
                }],
            }
            .encode_to_vec()
        };
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        assert_eq!(split(&mut enc, Signal::Logs, &logs(&[T - b, T]), b).len(), 1);
        let p = split(&mut enc, Signal::Logs, &logs(&[T - b - 1, T]), b);
        assert_eq!((p.len(), times(&enc, &p[1])), (2, vec![T - b - 1]));
        assert_eq!(p[1].resources.len(), 1);
        assert_eq!(p[1].resources[0].1, 0);
        let p = split(&mut enc, Signal::Logs, &logs(&[T - 3 * b, T]), b);
        assert_eq!(p.len(), 2, "the observed time decides");
        let old = 48 * 60 * MIN;
        assert_eq!(split(&mut enc, Signal::Logs, &logs(&[T - old, T - old + 30]), b).len(), 1);
    }
}
