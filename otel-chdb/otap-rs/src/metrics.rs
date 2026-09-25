//! OTLP/OTAP metrics → the contrib clickhouseexporter's five metrics tables,
//! one row set per metric type, written straight into column buffers.
//!
//! One walk over the request fills every type's buffers; each non-empty type
//! becomes its own object. The walk is generic over otap-dataflow's metrics
//! view traits, so the same code reads OTLP protobuf bytes (`RawMetricsData`,
//! zero-copy) and OTAP record batches (`OtapMetricsView`).
//!
//! The object layout is ../parquetgo/METRICS_SCHEMA.md. Row shape and
//! rendering are those of contrib v0.161.0
//! (`internal/metrics/*_metrics.go`), which this file follows line by line:
//! - rows are data points, in request order: resource, scope, metric, point;
//! - maps (`AttributesToMap`) are `Value.AsString` per value, **sorted by key**
//!   with Go's unstable `slices.SortFunc` (clickhouse-go `orderedmap.CollectN`),
//!   duplicates kept (see `gosort.rs`);
//! - `ServiceName` is `AsString` of the resource's first `service.name`;
//! - a number point's or exemplar's value is the double, the int as float64,
//!   or 0 when unset (`getValue`);
//! - exemplar span and trace ids are hex of the fixed-size id, so an absent
//!   or zero id is all zeros (not empty, unlike spans and logs);
//! - histogram `Sum`, `Min`, `Max` are 0 when unset; an absent exponential
//!   bucket range is offset 0 with no counts;
//! - a metric with no data (type Empty) fails the whole request, as contrib's
//!   `pushMetricsData` does ("metrics type is unset").

use crate::Signal;
use crate::columns::{Bin, ListOff, Map, prim};
use crate::flatten::{Stats, opt, push_value, service_name};
use crate::gosort::sort_like_go;
use crate::schema::{Schemas, dt_type};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{
    DataType, Float64Type, Int32Type, TimestampMillisecondType, UInt32Type, UInt64Type,
};
use otel_arrow_dfe_pdata_views::views::common::{AttributeView, InstrumentationScopeView};
use otel_arrow_dfe_pdata_views::views::metrics::{
    BucketsView, DataType as MType, DataView, ExemplarView, ExponentialHistogramDataPointView,
    ExponentialHistogramView, GaugeView, HistogramDataPointView, HistogramView, MetricView,
    MetricsView, NumberDataPointView, ResourceMetricsView, ScopeMetricsView, SumView,
    SummaryDataPointView, SummaryView, Value, ValueAtQuantileView,
};
use otel_arrow_dfe_pdata_views::views::resource::ResourceView;
use std::sync::Arc;

const ZERO_SPAN: &[u8] = b"0000000000000000";
const ZERO_TRACE: &[u8] = b"00000000000000000000000000000000";

/// A timestamp as the exporter's `DateTime` column stores it, in the
/// milliseconds of the Parquet `dt` type: `pcommon.Timestamp.AsTime()` is
/// `time.Unix(0, int64(ns))`, and ch-go stores `uint32(t.Unix())`, so the
/// nanoseconds are read as signed, floor-divided to seconds, and wrapped
/// mod 2^32.
#[inline]
pub fn dt_ms(ns: u64) -> i64 {
    ((ns as i64).div_euclid(1_000_000_000) as u32 as i64) * 1000
}

/// `getValue`: the double, the int as float64, 0 when the oneof is unset.
#[inline]
fn value_f64(v: Option<Value>) -> f64 {
    match v {
        Some(Value::Double(f)) => f,
        Some(Value::Integer(i)) => i as f64,
        None => 0.0,
    }
}

/// Scratch space for sorting one map's entries by key, Go's way.
#[derive(Default)]
struct Sorter {
    keys: Bin,
    vals: Bin,
    idx: Vec<u32>,
}

impl Sorter {
    /// Renders `it` and leaves `idx` in the order contrib writes the entries.
    fn load<A: AttributeView>(&mut self, it: impl Iterator<Item = A>) {
        self.keys.clear();
        self.vals.clear();
        self.idx.clear();
        for kv in it {
            self.idx.push(self.keys.len() as u32);
            self.keys.push(kv.key());
            let v = kv.value();
            push_value(&mut self.vals, v.as_ref());
        }
        let keys = &self.keys;
        sort_like_go(&mut self.idx, |i| {
            let i = i as usize;
            &keys.data[keys.off[i] as usize..keys.off[i + 1] as usize]
        });
    }

    fn entry(&self, i: u32) -> (&[u8], &[u8]) {
        let i = i as usize;
        (
            &self.keys.data[self.keys.off[i] as usize..self.keys.off[i + 1] as usize],
            &self.vals.data[self.vals.off[i] as usize..self.vals.off[i + 1] as usize],
        )
    }

    /// Appends the loaded map, sorted, as one map value of `m`.
    fn push_into(&self, m: &mut Map) {
        for &i in &self.idx {
            let (k, v) = self.entry(i);
            m.keys.push(k);
            m.vals.push(v);
        }
        m.commit();
    }

    fn push_attrs<A: AttributeView>(&mut self, m: &mut Map, it: impl Iterator<Item = A>) {
        self.load(it);
        self.push_into(m);
    }
}

/// A map rendered and sorted once (resource, scope), copied into every row.
#[derive(Default)]
struct SortedMap {
    keys: Bin,
    vals: Bin,
}

impl SortedMap {
    fn fill<A: AttributeView>(&mut self, s: &mut Sorter, it: impl Iterator<Item = A>) {
        s.load(it);
        self.keys.clear();
        self.vals.clear();
        for &i in &s.idx {
            let (k, v) = s.entry(i);
            self.keys.push(k);
            self.vals.push(v);
        }
    }
    fn clear(&mut self) {
        self.keys.clear();
        self.vals.clear();
    }
    fn copy_into(&self, m: &mut Map) {
        m.keys.off.extend(self.keys.off[1..].iter().map(|o| o + m.keys.data.len() as i32));
        m.keys.data.extend_from_slice(&self.keys.data);
        m.vals.off.extend(self.vals.off[1..].iter().map(|o| o + m.vals.data.len() as i32));
        m.vals.data.extend_from_slice(&self.vals.data);
        m.commit();
    }
}

/// What every row of a metric shares: resource, scope and metric fields.
#[derive(Default)]
struct Ctx {
    res: SortedMap,
    res_url: Vec<u8>,
    svc: Vec<u8>,
    scope_name: Vec<u8>,
    scope_version: Vec<u8>,
    scope: SortedMap,
    scope_dropped: u32,
    scope_url: Vec<u8>,
    name: Vec<u8>,
    desc: Vec<u8>,
    unit: Vec<u8>,
}

/// The columns every metrics table starts with.
#[derive(Default)]
struct Common {
    res_attrs: Map,
    res_url: Bin,
    scope_name: Bin,
    scope_version: Bin,
    scope_attrs: Map,
    scope_dropped: Vec<u32>,
    scope_url: Bin,
    svc: Bin,
    name: Bin,
    desc: Bin,
    unit: Bin,
    attrs: Map,
    start: Vec<i64>,
    time: Vec<i64>,
    stats: Stats,
}

impl Common {
    fn clear(&mut self) {
        for b in [
            &mut self.res_url,
            &mut self.scope_name,
            &mut self.scope_version,
            &mut self.scope_url,
            &mut self.svc,
            &mut self.name,
            &mut self.desc,
            &mut self.unit,
        ] {
            b.clear();
        }
        for m in [&mut self.res_attrs, &mut self.scope_attrs, &mut self.attrs] {
            m.clear();
        }
        self.scope_dropped.clear();
        self.start.clear();
        self.time.clear();
        self.stats = Stats::default();
    }

    /// One row's shared and point-level common fields.
    fn row<A: AttributeView>(
        &mut self,
        c: &Ctx,
        s: &mut Sorter,
        attrs: impl Iterator<Item = A>,
        start: u64,
        time: u64,
    ) {
        c.res.copy_into(&mut self.res_attrs);
        self.res_url.push(&c.res_url);
        self.scope_name.push(&c.scope_name);
        self.scope_version.push(&c.scope_version);
        c.scope.copy_into(&mut self.scope_attrs);
        self.scope_dropped.push(c.scope_dropped);
        self.scope_url.push(&c.scope_url);
        self.svc.push(&c.svc);
        self.name.push(&c.name);
        self.desc.push(&c.desc);
        self.unit.push(&c.unit);
        s.push_attrs(&mut self.attrs, attrs);
        self.start.push(dt_ms(start));
        self.time.push(dt_ms(time));
        self.stats.rows += 1;
        self.stats.see(time);
    }

    fn arrays(&mut self, sc: &Schemas) -> Vec<ArrayRef> {
        vec![
            self.res_attrs.take(&sc.entries),
            self.res_url.take(),
            self.scope_name.take(),
            self.scope_version.take(),
            self.scope_attrs.take(&sc.entries),
            prim::<UInt32Type>(std::mem::take(&mut self.scope_dropped), DataType::UInt32),
            self.scope_url.take(),
            self.svc.take(),
            self.name.take(),
            self.desc.take(),
            self.unit.take(),
            self.attrs.take(&sc.entries),
            prim::<TimestampMillisecondType>(std::mem::take(&mut self.start), dt_type()),
            prim::<TimestampMillisecondType>(std::mem::take(&mut self.time), dt_type()),
        ]
    }
}

/// The Exemplars Nested columns.
#[derive(Default)]
struct Exemplars {
    off: ListOff,
    attrs: Map,
    time: Vec<i64>,
    value: Vec<f64>,
    span: Bin,
    trace: Bin,
}

impl Exemplars {
    fn clear(&mut self) {
        self.off.clear();
        self.attrs.clear();
        self.time.clear();
        self.value.clear();
        self.span.clear();
        self.trace.clear();
    }

    fn push<E: ExemplarView>(&mut self, s: &mut Sorter, it: impl Iterator<Item = E>) {
        for e in it {
            s.push_attrs(&mut self.attrs, e.filtered_attributes());
            self.time.push(dt_ms(e.time_unix_nano()));
            self.value.push(value_f64(e.value()));
            match e.span_id() {
                Some(id) => hex_into(&mut self.span, &id[..]),
                None => self.span.push(ZERO_SPAN),
            }
            match e.trace_id() {
                Some(id) => hex_into(&mut self.trace, &id[..]),
                None => self.trace.push(ZERO_TRACE),
            }
        }
        self.off.commit(self.time.len());
    }

    fn arrays(&mut self, sc: &Schemas) -> Vec<ArrayRef> {
        let off = std::mem::take(&mut self.off);
        vec![
            lst(&off, &sc.map_elem, self.attrs.take(&sc.entries)),
            lst(&off, &sc.dt_elem, prim::<TimestampMillisecondType>(std::mem::take(&mut self.time), dt_type())),
            lst(&off, &sc.f64_elem, prim::<Float64Type>(std::mem::take(&mut self.value), DataType::Float64)),
            lst(&off, &sc.str_elem, self.span.take()),
            lst(&off, &sc.str_elem, self.trace.take()),
        ]
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// `hex.EncodeToString` of a fixed-size id (zeros included).
fn hex_into(b: &mut Bin, id: &[u8]) {
    for &x in id {
        b.data.push(HEX[(x >> 4) as usize]);
        b.data.push(HEX[(x & 0xf) as usize]);
    }
    b.commit();
}

fn lst(o: &ListOff, elem: &arrow::datatypes::FieldRef, vals: ArrayRef) -> ArrayRef {
    let mut c = ListOff { off: o.off.clone() };
    c.take(elem, vals)
}

/// A list of primitives: offsets plus values.
#[derive(Default)]
struct PList<T> {
    off: ListOff,
    v: Vec<T>,
}

impl<T> PList<T> {
    fn clear(&mut self) {
        self.off.clear();
        self.v.clear();
    }
    fn commit(&mut self) {
        self.off.commit(self.v.len());
    }
}

fn f64_list(l: &mut PList<f64>, sc: &Schemas) -> ArrayRef {
    let v = prim::<Float64Type>(std::mem::take(&mut l.v), DataType::Float64);
    std::mem::take(&mut l.off).take(&sc.f64_elem, v)
}

fn u64_list(l: &mut PList<u64>, sc: &Schemas) -> ArrayRef {
    let v = prim::<UInt64Type>(std::mem::take(&mut l.v), DataType::UInt64);
    std::mem::take(&mut l.off).take(&sc.u64_elem, v)
}

fn f64s(v: &mut Vec<f64>) -> ArrayRef {
    prim::<Float64Type>(std::mem::take(v), DataType::Float64)
}
fn u64s(v: &mut Vec<u64>) -> ArrayRef {
    prim::<UInt64Type>(std::mem::take(v), DataType::UInt64)
}
fn u32s(v: &mut Vec<u32>) -> ArrayRef {
    prim::<UInt32Type>(std::mem::take(v), DataType::UInt32)
}
fn i32s(v: &mut Vec<i32>) -> ArrayRef {
    prim::<Int32Type>(std::mem::take(v), DataType::Int32)
}

/// Gauge and Sum (number data points).
#[derive(Default)]
struct NumberBuf {
    c: Common,
    value: Vec<f64>,
    flags: Vec<u32>,
    ex: Exemplars,
    temporality: Vec<i32>,
    monotonic: Vec<bool>,
}

#[derive(Default)]
struct HistBuf {
    c: Common,
    count: Vec<u64>,
    sum: Vec<f64>,
    buckets: PList<u64>,
    bounds: PList<f64>,
    ex: Exemplars,
    flags: Vec<u32>,
    min: Vec<f64>,
    max: Vec<f64>,
    temporality: Vec<i32>,
}

#[derive(Default)]
struct ExpBuf {
    c: Common,
    count: Vec<u64>,
    sum: Vec<f64>,
    scale: Vec<i32>,
    zero: Vec<u64>,
    pos_off: Vec<i32>,
    pos: PList<u64>,
    neg_off: Vec<i32>,
    neg: PList<u64>,
    ex: Exemplars,
    flags: Vec<u32>,
    min: Vec<f64>,
    max: Vec<f64>,
    temporality: Vec<i32>,
}

#[derive(Default)]
struct SummaryBuf {
    c: Common,
    count: Vec<u64>,
    sum: Vec<f64>,
    quantiles: PList<f64>,
    qvalues: Vec<f64>,
    flags: Vec<u32>,
}

/// Reusable buffers for all five metric types.
#[derive(Default)]
pub struct MetricsBuf {
    gauge: NumberBuf,
    sum: NumberBuf,
    hist: HistBuf,
    exp: ExpBuf,
    summary: SummaryBuf,
    ctx: Ctx,
    sorter: Sorter,
}

/// A metric with no data: contrib rejects the whole request.
#[derive(Debug)]
pub struct EmptyMetric(pub String);

impl MetricsBuf {
    fn clear(&mut self) {
        for n in [&mut self.gauge, &mut self.sum] {
            n.c.clear();
            n.value.clear();
            n.flags.clear();
            n.ex.clear();
            n.temporality.clear();
            n.monotonic.clear();
        }
        let h = &mut self.hist;
        h.c.clear();
        h.count.clear();
        h.sum.clear();
        h.buckets.clear();
        h.bounds.clear();
        h.ex.clear();
        h.flags.clear();
        h.min.clear();
        h.max.clear();
        h.temporality.clear();
        let e = &mut self.exp;
        e.c.clear();
        e.count.clear();
        e.sum.clear();
        e.scale.clear();
        e.zero.clear();
        e.pos_off.clear();
        e.pos.clear();
        e.neg_off.clear();
        e.neg.clear();
        e.ex.clear();
        e.flags.clear();
        e.min.clear();
        e.max.clear();
        e.temporality.clear();
        let s = &mut self.summary;
        s.c.clear();
        s.count.clear();
        s.sum.clear();
        s.quantiles.clear();
        s.qvalues.clear();
        s.flags.clear();
    }

    /// Walks the request into the five types' buffers.
    pub fn fill<M: MetricsView>(&mut self, md: &M) -> Result<(), EmptyMetric> {
        self.clear();
        let (ctx, s) = (&mut self.ctx, &mut self.sorter);
        for rm in md.resources() {
            match rm.resource() {
                Some(r) => {
                    ctx.res.fill(s, r.attributes());
                    service_name(&mut ctx.svc, r.attributes());
                }
                None => {
                    ctx.res.clear();
                    ctx.svc.clear();
                }
            }
            ctx.res_url.clear();
            ctx.res_url.extend_from_slice(opt(rm.schema_url()));
            for sm in rm.scopes() {
                match sm.scope() {
                    Some(sc) => {
                        ctx.scope.fill(s, sc.attributes());
                        ctx.scope_name.clear();
                        ctx.scope_name.extend_from_slice(opt(sc.name()));
                        ctx.scope_version.clear();
                        ctx.scope_version.extend_from_slice(opt(sc.version()));
                        ctx.scope_dropped = sc.dropped_attributes_count();
                    }
                    None => {
                        ctx.scope.clear();
                        ctx.scope_name.clear();
                        ctx.scope_version.clear();
                        ctx.scope_dropped = 0;
                    }
                }
                ctx.scope_url.clear();
                ctx.scope_url.extend_from_slice(sm.schema_url());
                for m in sm.metrics() {
                    ctx.name.clear();
                    ctx.name.extend_from_slice(m.name());
                    ctx.desc.clear();
                    ctx.desc.extend_from_slice(m.description());
                    ctx.unit.clear();
                    ctx.unit.extend_from_slice(m.unit());
                    let Some(d) = m.data() else {
                        return Err(EmptyMetric(format!(
                            "metrics type is unset (metric {:?})",
                            String::from_utf8_lossy(m.name())
                        )));
                    };
                    match d.value_type() {
                        MType::Gauge => {
                            if let Some(g) = d.as_gauge() {
                                for dp in g.data_points() {
                                    number_row(&mut self.gauge, ctx, s, &dp);
                                }
                            }
                        }
                        MType::Sum => {
                            if let Some(g) = d.as_sum() {
                                let (t, mono) = (g.aggregation_temporality() as i32, g.is_monotonic());
                                for dp in g.data_points() {
                                    number_row(&mut self.sum, ctx, s, &dp);
                                    self.sum.temporality.push(t);
                                    self.sum.monotonic.push(mono);
                                }
                            }
                        }
                        MType::Histogram => {
                            if let Some(h) = d.as_histogram() {
                                let t = h.aggregation_temporality() as i32;
                                let b = &mut self.hist;
                                for dp in h.data_points() {
                                    b.c.row(ctx, s, dp.attributes(), dp.start_time_unix_nano(), dp.time_unix_nano());
                                    b.count.push(dp.count());
                                    b.sum.push(dp.sum().unwrap_or(0.0));
                                    b.buckets.v.extend(dp.bucket_counts());
                                    b.buckets.commit();
                                    b.bounds.v.extend(dp.explicit_bounds());
                                    b.bounds.commit();
                                    b.ex.push(s, dp.exemplars());
                                    b.flags.push(dp.flags().into_inner());
                                    b.min.push(dp.min().unwrap_or(0.0));
                                    b.max.push(dp.max().unwrap_or(0.0));
                                    b.temporality.push(t);
                                }
                            }
                        }
                        MType::ExponentialHistogram => {
                            if let Some(h) = d.as_exponential_histogram() {
                                let t = h.aggregation_temporality() as i32;
                                let b = &mut self.exp;
                                for dp in h.data_points() {
                                    b.c.row(ctx, s, dp.attributes(), dp.start_time_unix_nano(), dp.time_unix_nano());
                                    b.count.push(dp.count());
                                    b.sum.push(dp.sum().unwrap_or(0.0));
                                    b.scale.push(dp.scale());
                                    b.zero.push(dp.zero_count());
                                    match dp.positive() {
                                        Some(p) => {
                                            b.pos_off.push(p.offset());
                                            b.pos.v.extend(p.bucket_counts());
                                        }
                                        None => b.pos_off.push(0),
                                    }
                                    b.pos.commit();
                                    match dp.negative() {
                                        Some(n) => {
                                            b.neg_off.push(n.offset());
                                            b.neg.v.extend(n.bucket_counts());
                                        }
                                        None => b.neg_off.push(0),
                                    }
                                    b.neg.commit();
                                    b.ex.push(s, dp.exemplars());
                                    b.flags.push(dp.flags().into_inner());
                                    b.min.push(dp.min().unwrap_or(0.0));
                                    b.max.push(dp.max().unwrap_or(0.0));
                                    b.temporality.push(t);
                                }
                            }
                        }
                        MType::Summary => {
                            if let Some(h) = d.as_summary() {
                                let b = &mut self.summary;
                                for dp in h.data_points() {
                                    b.c.row(ctx, s, dp.attributes(), dp.start_time_unix_nano(), dp.time_unix_nano());
                                    b.count.push(dp.count());
                                    b.sum.push(dp.sum());
                                    for q in dp.quantile_values() {
                                        b.quantiles.v.push(q.quantile());
                                        b.qvalues.push(q.value());
                                    }
                                    b.quantiles.commit();
                                    b.flags.push(dp.flags().into_inner());
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Row stats of one type.
    pub fn stats(&self, t: Signal) -> Stats {
        match t {
            Signal::MetricsGauge => self.gauge.c.stats,
            Signal::MetricsSum => self.sum.c.stats,
            Signal::MetricsHistogram => self.hist.c.stats,
            Signal::MetricsExpHistogram => self.exp.c.stats,
            Signal::MetricsSummary => self.summary.c.stats,
            _ => Stats::default(),
        }
    }

    /// One type's content columns (no envelope), in the table's order.
    /// Consumes that type's buffers.
    pub fn content_arrays(&mut self, t: Signal, sc: &Schemas) -> Vec<ArrayRef> {
        match t {
            Signal::MetricsGauge | Signal::MetricsSum => {
                let b = if t == Signal::MetricsGauge { &mut self.gauge } else { &mut self.sum };
                let mut v = b.c.arrays(sc);
                v.push(f64s(&mut b.value));
                v.push(u32s(&mut b.flags));
                v.extend(b.ex.arrays(sc));
                if t == Signal::MetricsSum {
                    v.push(i32s(&mut b.temporality));
                    v.push(Arc::new(BooleanArray::from(std::mem::take(&mut b.monotonic))));
                }
                v
            }
            Signal::MetricsHistogram => {
                let b = &mut self.hist;
                let mut v = b.c.arrays(sc);
                v.push(u64s(&mut b.count));
                v.push(f64s(&mut b.sum));
                v.push(u64_list(&mut b.buckets, sc));
                v.push(f64_list(&mut b.bounds, sc));
                v.extend(b.ex.arrays(sc));
                v.push(u32s(&mut b.flags));
                v.push(f64s(&mut b.min));
                v.push(f64s(&mut b.max));
                v.push(i32s(&mut b.temporality));
                v
            }
            Signal::MetricsExpHistogram => {
                let b = &mut self.exp;
                let mut v = b.c.arrays(sc);
                v.push(u64s(&mut b.count));
                v.push(f64s(&mut b.sum));
                v.push(i32s(&mut b.scale));
                v.push(u64s(&mut b.zero));
                v.push(i32s(&mut b.pos_off));
                v.push(u64_list(&mut b.pos, sc));
                v.push(i32s(&mut b.neg_off));
                v.push(u64_list(&mut b.neg, sc));
                v.extend(b.ex.arrays(sc));
                v.push(u32s(&mut b.flags));
                v.push(f64s(&mut b.min));
                v.push(f64s(&mut b.max));
                v.push(i32s(&mut b.temporality));
                v
            }
            Signal::MetricsSummary => {
                let b = &mut self.summary;
                let mut v = b.c.arrays(sc);
                v.push(u64s(&mut b.count));
                v.push(f64s(&mut b.sum));
                let off = std::mem::take(&mut b.quantiles.off);
                let q = prim::<Float64Type>(std::mem::take(&mut b.quantiles.v), DataType::Float64);
                v.push(lst(&off, &sc.f64_elem, q));
                v.push(lst(&off, &sc.f64_elem, f64s(&mut b.qvalues)));
                v.push(u32s(&mut b.flags));
                v
            }
            _ => unreachable!("not a metrics signal"),
        }
    }
}

fn number_row<P: NumberDataPointView>(b: &mut NumberBuf, ctx: &Ctx, s: &mut Sorter, dp: &P) {
    b.c.row(ctx, s, dp.attributes(), dp.start_time_unix_nano(), dp.time_unix_nano());
    b.value.push(value_f64(dp.value()));
    b.flags.push(dp.flags().into_inner());
    b.ex.push(s, dp.exemplars());
}
