//! Metrics layout B (`series_table`): narrow per-type **points** objects keyed
//! by a series id computed here, at the edge, plus a **series** object that
//! carries everything else of the contrib row, for the series this edge has
//! not announced yet in the current cache window.
//!
//! This is a port of ../metrics-layout/seriesenc (the spike's Go prototype),
//! which specifies the layout; `tests/series.rs` checks that both produce the
//! same Parquet schema, rows and series ids for the same requests.
//!
//! **Series id (v1).** xxh3-64 over a length-prefixed canonical encoding, in
//! two levels so that the resource and scope part is hashed once per
//! ScopeMetrics:
//!
//! ```text
//! h_rs      = xxh3_64( res_kvs, ResourceSchemaUrl, ScopeName, ScopeVersion,
//!                      scope_kvs, le32(ScopeDroppedAttrCount), ScopeSchemaUrl )
//! series_id = xxh3_64( le64(h_rs), MetricType, MetricName, MetricDescription,
//!                      MetricUnit, le32(AggregationTemporality), IsMonotonic,
//!                      point_kvs, uvarint(len) le64(ExplicitBounds...) )
//! ```
//!
//! A string is `uvarint(len) || bytes`; kvs are `uvarint(n)` then each entry,
//! sorted by key bytes (stable, so duplicates keep their wire order), as the
//! key then the value tagged with its type: `s` string, `i`/`d` 8
//! little-endian bytes, `b` one byte, else `x` and `AsString()`.
//!
//! **Maps in the series object** are key and value arrays (central builds
//! the Map with `mapFromArrays`) in the order the contrib exporter writes
//! them (Go's unstable sort, `gosort.rs`). That only differs from the Go
//! prototype's stable order for maps with duplicate keys above 12 entries,
//! where the prototype disagrees with contrib and this matches contrib.
//!
//! **Series cache.** A series is announced the first time it is seen in a
//! window (the hour of the point's `TimeUnix`); within a request, once. The
//! caller marks a request's new series announced (`announced`) only after
//! its series object has **committed**: until then the next request
//! announces them again. A cache belongs to the series lane's epoch: a
//! commit in an epoch the cache hasn't seen clears it, so a new epoch
//! re-announces everything. Entries older than the previous window are
//! pruned; a point more than a window late may be re-announced (harmless:
//! the series table is idempotent).
//!
//! **Beyond the prototype**, both on by default (`SeriesOptions`):
//! - `merge_number_points`: gauge and sum points share one object and table
//!   (`metrics_number_points`), with a `MetricType` column after `series_id`,
//!   so a request has one object fewer (the spike's recommendation; the
//!   views filter on it);
//! - `exemplar_attributes`: `Exemplars.FilteredAttributes` (a list of maps,
//!   contrib's position) in the points objects, which the prototype drops.
//!
//! With both off the objects are the prototype's, column for column.

use crate::Signal;
use crate::columns::{Bin, ListOff, Map, prim};
use crate::flatten::{NoAttr, Stats, opt, push_value, service_name};
use crate::gosort::sort_like_go;
use crate::metrics::{EmptyMetric, Sorter, ZERO_SPAN, ZERO_TRACE, hex_into, value_f64};
use crate::render;
use crate::schema::{Schemas, map_type, ts_type};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{
    DataType, Field, Float64Type, Int32Type, Schema, UInt8Type, UInt32Type, UInt64Type,
};
use otel_arrow_dfe_pdata_views::views::common::{
    AnyValueView, AttributeView, InstrumentationScopeView, ValueType,
};
use otel_arrow_dfe_pdata_views::views::metrics::{
    BucketsView, DataType as MType, DataView, ExemplarView, ExponentialHistogramDataPointView,
    ExponentialHistogramView, GaugeView, HistogramDataPointView, HistogramView, MetricView,
    MetricsView, NumberDataPointView, ResourceMetricsView, ScopeMetricsView, SumView,
    SummaryDataPointView, SummaryView, ValueAtQuantileView,
};
use otel_arrow_dfe_pdata_views::views::resource::ResourceView;
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as Physical};
use parquet::schema::types::{SchemaDescriptor, Type};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;
use std::time::Duration;
use xxhash_rust::xxh3::xxh3_64;

/// Which tables the exporter writes metrics for.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricsLayout {
    /// The contrib exporter's five `otel_metrics_*` tables (layout A).
    ClickstackTables,
    /// Layout B: points tables + `otel_metrics_series`.
    #[default]
    SeriesTable,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct SeriesOptions {
    /// The cache window: a series is re-announced once per window of its
    /// points' `TimeUnix`.
    #[serde(with = "humantime_serde")]
    pub window: Duration,
    /// One points object and table for gauge and sum.
    pub merge_number_points: bool,
    /// Carry `Exemplars.FilteredAttributes` in the points objects.
    pub exemplar_attributes: bool,
}

impl Default for SeriesOptions {
    fn default() -> Self {
        Self { window: Duration::from_secs(3600), merge_number_points: true, exemplar_attributes: true }
    }
}

impl SeriesOptions {
    /// The Go prototype's layout exactly (for the comparison test).
    pub fn prototype() -> Self {
        Self { merge_number_points: false, exemplar_attributes: false, ..Self::default() }
    }

    /// The points namespaces a request can produce, in object order.
    pub fn point_signals(&self) -> Vec<Signal> {
        let mut v = if self.merge_number_points {
            vec![Signal::MetricsNumberPoints]
        } else {
            vec![Signal::MetricsGaugePoints, Signal::MetricsSumPoints]
        };
        v.extend([Signal::MetricsHistogramPoints, Signal::MetricsExpHistogramPoints, Signal::MetricsSummaryPoints]);
        v
    }
}

/// `MetricType`, as the series table's Enum8 numbers it.
pub const GAUGE: u8 = 0;
pub const SUM: u8 = 1;
pub const HISTOGRAM: u8 = 2;
pub const EXP_HISTOGRAM: u8 = 3;
pub const SUMMARY: u8 = 4;

// ---- schema ----

#[derive(Clone, Copy)]
enum K {
    Str,
    U64,
    U32,
    U16,
    U8,
    I32,
    F64,
    Bool,
    TsNs,
    LStr,
    LU32,
    LU64,
    LF64,
    LMap,
}

/// The columns of a layout-B object, envelope included, in object order.
fn fields(signal: Signal, o: &SeriesOptions) -> (&'static str, Vec<(&'static str, K)>) {
    use K::*;
    use Signal as S;
    let mut f = if signal == S::MetricsSeries {
        vec![
            ("series_id", U64),
            ("MetricType", U8),
            ("MetricName", Str),
            ("MetricDescription", Str),
            ("MetricUnit", Str),
            ("ServiceName", Str),
            ("ResourceAttributesKeys", LStr),
            ("ResourceAttributesValues", LStr),
            ("ResourceSchemaUrl", Str),
            ("ScopeName", Str),
            ("ScopeVersion", Str),
            ("ScopeAttributesKeys", LStr),
            ("ScopeAttributesValues", LStr),
            ("ScopeDroppedAttrCount", U32),
            ("ScopeSchemaUrl", Str),
            ("AttributesKeys", LStr),
            ("AttributesValues", LStr),
            ("AggregationTemporality", I32),
            ("IsMonotonic", Bool),
            ("ExplicitBounds", LF64),
            ("FirstSeen", U32),
        ]
    } else {
        let mut h = vec![("MetricName", Str), ("ServiceName", Str), ("series_id", U64)];
        if signal == S::MetricsNumberPoints {
            h.push(("MetricType", U8));
        }
        h.extend([("StartTimeUnix", U32), ("TimeUnix", U32)]);
        h
    };
    let mut ex = Vec::new();
    if o.exemplar_attributes {
        ex.push(("Exemplars.FilteredAttributes", LMap));
    }
    ex.extend([
        ("Exemplars.TimeUnix", LU32),
        ("Exemplars.Value", LF64),
        ("Exemplars.SpanId", LStr),
        ("Exemplars.TraceId", LStr),
    ]);
    let root = match signal {
        S::MetricsNumberPoints | S::MetricsGaugePoints | S::MetricsSumPoints => {
            f.extend([("Value", F64), ("Flags", U32)]);
            f.extend(ex);
            "NumberRow"
        }
        S::MetricsHistogramPoints => {
            f.extend([("Count", U64), ("Sum", F64), ("BucketCounts", LU64), ("Min", F64), ("Max", F64), ("Flags", U32)]);
            f.extend(ex);
            "HistRow"
        }
        S::MetricsExpHistogramPoints => {
            f.extend([
                ("Count", U64),
                ("Sum", F64),
                ("Scale", I32),
                ("ZeroCount", U64),
                ("PositiveOffset", I32),
                ("PositiveBucketCounts", LU64),
                ("NegativeOffset", I32),
                ("NegativeBucketCounts", LU64),
                ("Min", F64),
                ("Max", F64),
                ("Flags", U32),
            ]);
            f.extend(ex);
            "ExpRow"
        }
        S::MetricsSummaryPoints => {
            f.extend([
                ("Count", U64),
                ("Sum", F64),
                ("ValueAtQuantiles.Quantile", LF64),
                ("ValueAtQuantiles.Value", LF64),
                ("Flags", U32),
            ]);
            "SummaryRow"
        }
        S::MetricsSeries => "SeriesRow",
        other => unreachable!("{other:?} is not a layout-B signal"),
    };
    f.extend([
        ("producer_id", Str),
        ("producer_epoch", Str),
        ("batch_id", U64),
        ("row_ordinal", U32),
        ("received_at", TsNs),
        ("schema_version", U16),
    ]);
    (root, f)
}

fn arrow_type(k: K, s: &DataType) -> DataType {
    let list = |t: DataType| DataType::List(Arc::new(Field::new("element", t, false)));
    match k {
        K::Str => s.clone(),
        K::U64 => DataType::UInt64,
        K::U32 => DataType::UInt32,
        K::U16 => DataType::UInt16,
        K::U8 => DataType::UInt8,
        K::I32 => DataType::Int32,
        K::F64 => DataType::Float64,
        K::Bool => DataType::Boolean,
        K::TsNs => ts_type(),
        K::LStr => list(s.clone()),
        K::LU32 => list(DataType::UInt32),
        K::LU64 => list(DataType::UInt64),
        K::LF64 => list(DataType::Float64),
        K::LMap => list(map_type(s)),
    }
}

fn leaf(name: &str, p: Physical, lt: Option<LogicalType>) -> Type {
    Type::primitive_type_builder(name, p)
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(lt)
        .build()
        .expect("valid leaf")
}

fn group(name: &str, rep: Repetition, lt: Option<LogicalType>, fields: Vec<Type>) -> Type {
    Type::group_type_builder(name)
        .with_repetition(rep)
        .with_logical_type(lt)
        .with_fields(fields.into_iter().map(Arc::new).collect())
        .build()
        .expect("valid group")
}

fn int(bits: i8, signed: bool) -> Option<LogicalType> {
    Some(LogicalType::Integer { bit_width: bits, is_signed: signed })
}

/// The Parquet type parquet-go gives the prototype's field (`list` tags are
/// 3-level LISTs of `element`, every field required).
fn parquet_type(name: &str, k: K) -> Type {
    let list = |e: Type| {
        group(name, Repetition::REQUIRED, Some(LogicalType::List), vec![group("list", Repetition::REPEATED, None, vec![e])])
    };
    match k {
        K::Str => leaf(name, Physical::BYTE_ARRAY, Some(LogicalType::String)),
        K::U64 => leaf(name, Physical::INT64, int(64, false)),
        K::U32 => leaf(name, Physical::INT32, int(32, false)),
        K::U16 => leaf(name, Physical::INT32, int(16, false)),
        K::U8 => leaf(name, Physical::INT32, int(8, false)),
        K::I32 => leaf(name, Physical::INT32, int(32, true)),
        K::F64 => leaf(name, Physical::DOUBLE, None),
        K::Bool => leaf(name, Physical::BOOLEAN, None),
        K::TsNs => leaf(
            name,
            Physical::INT64,
            Some(LogicalType::Timestamp { is_adjusted_to_u_t_c: true, unit: TimeUnit::NANOS }),
        ),
        K::LStr => list(parquet_type("element", K::Str)),
        K::LU32 => list(parquet_type("element", K::U32)),
        K::LU64 => list(parquet_type("element", K::U64)),
        K::LF64 => list(parquet_type("element", K::F64)),
        K::LMap => list(group(
            "element",
            Repetition::REQUIRED,
            Some(LogicalType::Map),
            vec![group(
                "key_value",
                Repetition::REPEATED,
                None,
                vec![parquet_type("key", K::Str), parquet_type("value", K::Str)],
            )],
        )),
    }
}

/// Leaves written without a dictionary (beyond `schema::HIGH_CARDINALITY`):
/// the near-unique numbers.
const PLAIN: &[&[&str]] = &[
    &["series_id"],
    &["Count"],
    &["Min"],
    &["Max"],
    &["ZeroCount"],
    &["FirstSeen"],
    &["BucketCounts", "list", "element"],
    &["PositiveBucketCounts", "list", "element"],
    &["NegativeBucketCounts", "list", "element"],
    &["ValueAtQuantiles.Value", "list", "element"],
];

/// The Arrow schema (strings as Binary) and the published Parquet schema.
pub fn schemas(signal: Signal, o: &SeriesOptions) -> Schemas {
    let (root, f) = fields(signal, o);
    let arrow = Schema::new(f.iter().map(|(n, k)| Field::new(*n, arrow_type(*k, &DataType::Binary), false)).collect::<Vec<_>>());
    let pq = group(root, Repetition::REQUIRED, None, f.iter().map(|(n, k)| parquet_type(n, *k)).collect());
    let plain = PLAIN.iter().map(|p| p.iter().map(|s| s.to_string()).collect()).collect();
    let mut sc = Schemas::with(Arc::new(arrow), SchemaDescriptor::new(Arc::new(pq)), plain);
    // 0, 1, 2, ...: 2.3 B/point plain + zstd, next to nothing as deltas (the
    // prototype's `delta` tag).
    sc.delta = vec![vec!["row_ordinal".to_string()]];
    sc
}

/// The object's `s3()` structure (ClickHouse types), envelope included.
pub fn structure(signal: Signal, o: &SeriesOptions) -> String {
    fn ch(k: K) -> &'static str {
        match k {
            K::Str => "String",
            K::U64 => "UInt64",
            K::U32 => "UInt32",
            K::U16 => "UInt16",
            K::U8 => "UInt8",
            K::I32 => "Int32",
            K::F64 => "Float64",
            K::Bool => "Bool",
            K::TsNs => "DateTime64(9)",
            K::LStr => "Array(String)",
            K::LU32 => "Array(UInt32)",
            K::LU64 => "Array(UInt64)",
            K::LF64 => "Array(Float64)",
            K::LMap => "Array(Map(String, String))",
        }
    }
    let (_, f) = fields(signal, o);
    f.iter()
        .map(|(n, k)| {
            // DateTime columns travel as uint32 seconds.
            let t = match (*n, *k) {
                ("StartTimeUnix" | "TimeUnix" | "FirstSeen", _) => "DateTime",
                ("Exemplars.TimeUnix", _) => "Array(DateTime)",
                (_, k) => ch(k),
            };
            format!("`{n}` {t}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The importer's statement for one object: `INSERT INTO db.<table> SELECT
/// ... FROM <src>`, where `src` is an `s3(...)` call with `structure`. The
/// series object's maps are built with `mapFromArrays`, its `LastSeen`
/// starts as `FirstSeen`, and its envelope is not stored (the series table
/// is idempotent: no count check, no dedup token needed).
pub fn insert_select(signal: Signal, o: &SeriesOptions, db: &str, src: &str) -> String {
    let (_, f) = fields(signal, o);
    let names: Vec<&str> = f.iter().map(|(n, _)| *n).collect();
    if signal == Signal::MetricsSeries {
        let mut cols = Vec::new();
        let mut sel = Vec::new();
        let mut i = 0;
        while i < names.len() {
            let n = names[i];
            if let Some(base) = n.strip_suffix("Keys") {
                cols.push(format!("`{base}`"));
                sel.push(format!("mapFromArrays(`{n}`, `{}`)", names[i + 1]));
                i += 2;
                continue;
            }
            cols.push(format!("`{n}`"));
            sel.push(format!("`{n}`"));
            if n == "FirstSeen" {
                // The envelope that follows isn't stored in the series table.
                cols.push("`LastSeen`".into());
                sel.push("`FirstSeen`".into());
                break;
            }
            i += 1;
        }
        return format!("INSERT INTO {db}.{} ({}) SELECT {} FROM {src}", signal.table(), cols.join(", "), sel.join(", "));
    }
    let cols = names.iter().map(|n| format!("`{n}`")).collect::<Vec<_>>().join(", ");
    format!("INSERT INTO {db}.{} ({cols}) SELECT {cols} FROM {src}", signal.table())
}

// ---- the id encoding ----

#[inline]
fn uvarint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.push(v as u8 | 0x80);
        v >>= 7;
    }
    b.push(v as u8);
}

#[inline]
fn put_str(b: &mut Vec<u8>, s: &[u8]) {
    uvarint(b, s.len() as u64);
    b.extend_from_slice(s);
}

/// The exporter's DateTime conversion: signed floor seconds, mod 2^32.
#[inline]
fn dt(ns: u64) -> u32 {
    (ns as i64).div_euclid(1_000_000_000) as u32
}

/// One attribute map, loaded once: keys, typed values, the canonical (stable)
/// order for the id, and the contrib order for the series row.
#[derive(Default)]
struct Kv {
    keys: Bin,
    /// Strings and `x` values (AsString); empty for i/d/b, rendered on demand.
    vals: Bin,
    tag: Vec<u8>,
    raw: Vec<u64>,
    stable: Vec<u32>,
    go: Vec<u32>,
    dup: bool,
}

impl Kv {
    fn key(&self, i: u32) -> &[u8] {
        let i = i as usize;
        &self.keys.data[self.keys.off[i] as usize..self.keys.off[i + 1] as usize]
    }
    fn val(&self, i: u32) -> &[u8] {
        let i = i as usize;
        &self.vals.data[self.vals.off[i] as usize..self.vals.off[i + 1] as usize]
    }

    fn load<A: AttributeView>(&mut self, it: impl Iterator<Item = A>) {
        self.keys.clear();
        self.vals.clear();
        self.tag.clear();
        self.raw.clear();
        for kv in it {
            self.keys.push(kv.key());
            let v = kv.value();
            let (tag, raw) = match v.as_ref().map(|x| x.value_type()) {
                Some(ValueType::String) => {
                    self.vals.push(v.as_ref().and_then(|x| x.as_string()).unwrap_or_default());
                    (b's', 0)
                }
                Some(ValueType::Int64) => {
                    self.vals.commit();
                    (b'i', v.as_ref().and_then(|x| x.as_int64()).unwrap_or_default() as u64)
                }
                Some(ValueType::Double) => {
                    self.vals.commit();
                    (b'd', v.as_ref().and_then(|x| x.as_double()).unwrap_or_default().to_bits())
                }
                Some(ValueType::Bool) => {
                    self.vals.commit();
                    (b'b', v.as_ref().and_then(|x| x.as_bool()).unwrap_or_default() as u64)
                }
                _ => {
                    push_value(&mut self.vals, v.as_ref());
                    (b'x', 0)
                }
            };
            self.tag.push(tag);
            self.raw.push(raw);
        }
        let n = self.tag.len() as u32;
        self.stable.clear();
        self.stable.extend(0..n);
        self.go.clear();
        let mut sorted = true;
        for i in 1..n {
            if self.key(i - 1) > self.key(i) {
                sorted = false;
                break;
            }
        }
        if !sorted {
            let mut st = std::mem::take(&mut self.stable);
            st.sort_by(|&a, &b| self.key(a).cmp(self.key(b))); // stable
            self.stable = st;
        }
        self.dup = self.stable.windows(2).any(|w| self.key(w[0]) == self.key(w[1]));
    }

    /// `uvarint(n)` then each entry in key order: key, tag, value.
    fn hash_into(&self, b: &mut Vec<u8>) {
        uvarint(b, self.stable.len() as u64);
        for &i in &self.stable {
            put_str(b, self.key(i));
            let t = self.tag[i as usize];
            b.push(t);
            match t {
                b'i' | b'd' => b.extend_from_slice(&self.raw[i as usize].to_le_bytes()),
                b'b' => b.push(self.raw[i as usize] as u8),
                _ => put_str(b, self.val(i)),
            }
        }
    }

    /// Appends the map as key and value lists (one list element each), in
    /// the contrib exporter's order, values rendered as `AsString`.
    fn render(&mut self, keys: &mut Bin, vals: &mut Bin) {
        if self.dup && self.go.is_empty() {
            let mut go: Vec<u32> = (0..self.tag.len() as u32).collect();
            let kb = &self.keys;
            sort_like_go(&mut go, |i| {
                let i = i as usize;
                &kb.data[kb.off[i] as usize..kb.off[i + 1] as usize]
            });
            self.go = go;
        }
        let order = if self.dup { &self.go } else { &self.stable };
        for &i in order {
            keys.push(self.key(i));
            let raw = self.raw[i as usize];
            match self.tag[i as usize] {
                b'i' => render::int(&mut vals.data, raw as i64),
                b'd' => render::double(&mut vals.data, f64::from_bits(raw)),
                b'b' => vals.data.extend_from_slice(if raw != 0 { b"true" } else { b"false" }),
                _ => vals.data.extend_from_slice(self.val(i)),
            }
            vals.commit();
        }
    }
}

/// A list of strings: element offsets plus the strings.
#[derive(Default)]
struct StrList {
    off: ListOff,
    s: Bin,
}

impl StrList {
    fn clear(&mut self) {
        self.off.clear();
        self.s.clear();
    }
    fn commit(&mut self) {
        self.off.commit(self.s.len());
    }
    fn take(&mut self, sc: &Schemas) -> ArrayRef {
        let v = self.s.take();
        self.off.take(&sc.str_elem, v)
    }
}

/// A list of primitives.
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
    l.off.take(&sc.f64_elem, v)
}
fn u64_list(l: &mut PList<u64>, sc: &Schemas) -> ArrayRef {
    let v = prim::<UInt64Type>(std::mem::take(&mut l.v), DataType::UInt64);
    l.off.take(&sc.u64_elem, v)
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

/// The columns every points object starts with.
#[derive(Default)]
struct Head {
    name: Bin,
    svc: Bin,
    id: Vec<u64>,
    mtype: Vec<u8>,
    start: Vec<u32>,
    time: Vec<u32>,
    stats: Stats,
}

impl Head {
    fn clear(&mut self) {
        self.name.clear();
        self.svc.clear();
        self.id.clear();
        self.mtype.clear();
        self.start.clear();
        self.time.clear();
        self.stats = Stats::default();
    }
    fn arrays(&mut self, merged: bool) -> Vec<ArrayRef> {
        let mut v = vec![self.name.take(), self.svc.take(), u64s(&mut self.id)];
        if merged {
            v.push(prim::<UInt8Type>(std::mem::take(&mut self.mtype), DataType::UInt8));
        } else {
            self.mtype.clear();
        }
        v.push(u32s(&mut self.start));
        v.push(u32s(&mut self.time));
        v
    }
}

#[derive(Default)]
struct Exemplars {
    off: ListOff,
    attrs: Map,
    time: Vec<u32>,
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
    fn push<E: ExemplarView>(&mut self, s: &mut Sorter, with_attrs: bool, it: impl Iterator<Item = E>) {
        for e in it {
            if with_attrs {
                s.push_attrs(&mut self.attrs, e.filtered_attributes());
            }
            self.time.push(dt(e.time_unix_nano()));
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
    fn arrays(&mut self, sc: &Schemas, with_attrs: bool) -> Vec<ArrayRef> {
        let off = std::mem::take(&mut self.off);
        let lst = |elem: &arrow::datatypes::FieldRef, vals: ArrayRef| ListOff { off: off.off.clone() }.take(elem, vals);
        let mut v = Vec::with_capacity(5);
        if with_attrs {
            v.push(lst(&sc.map_elem, self.attrs.take(&sc.entries)));
        } else {
            self.attrs.clear();
        }
        v.push(lst(&sc.u32_elem, u32s(&mut self.time)));
        v.push(lst(&sc.f64_elem, f64s(&mut self.value)));
        v.push(lst(&sc.str_elem, self.span.take()));
        v.push(lst(&sc.str_elem, self.trace.take()));
        v
    }
}

#[derive(Default)]
struct NumBuf {
    h: Head,
    value: Vec<f64>,
    flags: Vec<u32>,
    ex: Exemplars,
}

#[derive(Default)]
struct HistBuf {
    h: Head,
    count: Vec<u64>,
    sum: Vec<f64>,
    buckets: PList<u64>,
    min: Vec<f64>,
    max: Vec<f64>,
    flags: Vec<u32>,
    ex: Exemplars,
}

#[derive(Default)]
struct ExpBuf {
    h: Head,
    count: Vec<u64>,
    sum: Vec<f64>,
    scale: Vec<i32>,
    zero: Vec<u64>,
    pos_off: Vec<i32>,
    pos: PList<u64>,
    neg_off: Vec<i32>,
    neg: PList<u64>,
    min: Vec<f64>,
    max: Vec<f64>,
    flags: Vec<u32>,
    ex: Exemplars,
}

#[derive(Default)]
struct SummaryBuf {
    h: Head,
    count: Vec<u64>,
    sum: Vec<f64>,
    q: PList<f64>,
    qv: Vec<f64>,
    flags: Vec<u32>,
}

#[derive(Default)]
struct SeriesRows {
    id: Vec<u64>,
    mtype: Vec<u8>,
    name: Bin,
    desc: Bin,
    unit: Bin,
    svc: Bin,
    rk: StrList,
    rv: StrList,
    rurl: Bin,
    sname: Bin,
    sver: Bin,
    sk: StrList,
    sv: StrList,
    dropped: Vec<u32>,
    surl: Bin,
    ak: StrList,
    av: StrList,
    temp: Vec<i32>,
    mono: Vec<bool>,
    bounds: PList<f64>,
    first: Vec<u32>,
    stats: Stats,
}

/// What every row of a metric shares.
#[derive(Default)]
struct Ctx {
    svc: Vec<u8>,
    res_url: Vec<u8>,
    scope_name: Vec<u8>,
    scope_version: Vec<u8>,
    scope_url: Vec<u8>,
    dropped: u32,
    name: Vec<u8>,
    desc: Vec<u8>,
    unit: Vec<u8>,
    typ: u8,
    temp: i32,
    mono: bool,
    /// The rendered resource / scope lists, built once per resource / scope
    /// the first time one of its series is new.
    res_k: Bin,
    res_v: Bin,
    res_ready: bool,
    scope_k: Bin,
    scope_v: Bin,
    scope_ready: bool,
    /// Length of the id buffer up to the end of the metric part.
    moff: usize,
}

/// Identity hasher for keys that are already hashes.
#[derive(Default)]
pub struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = self.0.rotate_left(8) ^ x as u64;
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
type IdBuild = BuildHasherDefault<IdHasher>;

/// Layout B's encoder: the per-request walk and the series cache.
pub struct SeriesBuf {
    pub opts: SeriesOptions,
    window: i64,
    cache: HashMap<u64, i32, IdBuild>,
    cache_epochs: HashSet<String>,
    max_win: i32,
    seen: HashSet<u64, IdBuild>,
    new: Vec<(u64, i32)>,
    /// gauge, sum (with `merge_number_points`, both go to `num[0]`).
    num: [NumBuf; 2],
    hist: HistBuf,
    exp: ExpBuf,
    summary: SummaryBuf,
    ser: SeriesRows,
    ctx: Ctx,
    rkv: Kv,
    skv: Kv,
    kv: Kv,
    sorter: Sorter,
    buf: Vec<u8>,
    bounds: Vec<f64>,
}

impl SeriesBuf {
    pub fn new(opts: SeriesOptions) -> Self {
        let window = opts.window.as_secs().max(1) as i64;
        Self {
            opts,
            window,
            cache: HashMap::default(),
            cache_epochs: HashSet::new(),
            max_win: i32::MIN,
            seen: HashSet::default(),
            new: Vec::new(),
            num: Default::default(),
            hist: Default::default(),
            exp: Default::default(),
            summary: Default::default(),
            ser: Default::default(),
            ctx: Default::default(),
            rkv: Default::default(),
            skv: Default::default(),
            kv: Default::default(),
            sorter: Default::default(),
            buf: Vec::with_capacity(1024),
            bounds: Vec::new(),
        }
    }

    /// Series in the cache.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Marks series as announced: call only once the series object that
    /// carried them has committed, in `epoch` (its lane's). A commit in an
    /// epoch the cache hasn't seen yet empties the cache first.
    pub fn announced(&mut self, ids: &[(u64, i32)], epoch: &str) {
        if !self.cache_epochs.contains(epoch) {
            if !self.cache_epochs.is_empty() {
                self.cache.clear();
            }
            let _ = self.cache_epochs.insert(epoch.to_string());
        }
        for &(id, w) in ids {
            let _ = self.cache.insert(id, w);
            if w > self.max_win {
                self.max_win = w;
                self.cache.retain(|_, v| *v >= w - 1);
            }
        }
    }

    /// The series the last `fill` announced, with their windows.
    pub fn take_new(&mut self) -> Vec<(u64, i32)> {
        std::mem::take(&mut self.new)
    }

    fn clear(&mut self) {
        for n in &mut self.num {
            n.h.clear();
            n.value.clear();
            n.flags.clear();
            n.ex.clear();
        }
        let h = &mut self.hist;
        h.h.clear();
        h.count.clear();
        h.sum.clear();
        h.buckets.clear();
        h.min.clear();
        h.max.clear();
        h.flags.clear();
        h.ex.clear();
        let e = &mut self.exp;
        e.h.clear();
        e.count.clear();
        e.sum.clear();
        e.scale.clear();
        e.zero.clear();
        e.pos_off.clear();
        e.pos.clear();
        e.neg_off.clear();
        e.neg.clear();
        e.min.clear();
        e.max.clear();
        e.flags.clear();
        e.ex.clear();
        let s = &mut self.summary;
        s.h.clear();
        s.count.clear();
        s.sum.clear();
        s.q.clear();
        s.qv.clear();
        s.flags.clear();
        let r = &mut self.ser;
        r.id.clear();
        r.mtype.clear();
        for b in [&mut r.name, &mut r.desc, &mut r.unit, &mut r.svc, &mut r.rurl, &mut r.sname, &mut r.sver, &mut r.surl] {
            b.clear();
        }
        for l in [&mut r.rk, &mut r.rv, &mut r.sk, &mut r.sv, &mut r.ak, &mut r.av] {
            l.clear();
        }
        r.dropped.clear();
        r.temp.clear();
        r.mono.clear();
        r.bounds.clear();
        r.first.clear();
        r.stats = Stats::default();
        self.seen.clear();
        self.new.clear();
    }

    /// The series id of a point (its attributes and bounds on top of the
    /// metric prefix in `buf`); a series row if it is new in this window.
    fn point_id<A: AttributeView>(&mut self, attrs: impl Iterator<Item = A>, ts_ns: u64) -> u64 {
        self.kv.load(attrs);
        let b = &mut self.buf;
        b.truncate(self.ctx.moff);
        self.kv.hash_into(b);
        uvarint(b, self.bounds.len() as u64);
        for x in &self.bounds {
            b.extend_from_slice(&x.to_bits().to_le_bytes());
        }
        let id = xxh3_64(b);
        let ts = dt(ts_ns);
        let w = (ts as i64 / self.window) as i32;
        if self.cache.get(&id) == Some(&w) || !self.seen.insert(id) {
            return id;
        }
        self.new.push((id, w));
        self.series_row(id, ts, ts_ns);
        id
    }

    fn series_row(&mut self, id: u64, ts: u32, ts_ns: u64) {
        let (c, r) = (&mut self.ctx, &mut self.ser);
        r.id.push(id);
        r.mtype.push(c.typ);
        r.name.push(&c.name);
        r.desc.push(&c.desc);
        r.unit.push(&c.unit);
        r.svc.push(&c.svc);
        if !c.res_ready {
            c.res_k.clear();
            c.res_v.clear();
            self.rkv.render(&mut c.res_k, &mut c.res_v);
            c.res_ready = true;
        }
        copy_list(&c.res_k, &mut r.rk);
        copy_list(&c.res_v, &mut r.rv);
        r.rurl.push(&c.res_url);
        r.sname.push(&c.scope_name);
        r.sver.push(&c.scope_version);
        if !c.scope_ready {
            c.scope_k.clear();
            c.scope_v.clear();
            self.skv.render(&mut c.scope_k, &mut c.scope_v);
            c.scope_ready = true;
        }
        copy_list(&c.scope_k, &mut r.sk);
        copy_list(&c.scope_v, &mut r.sv);
        r.dropped.push(c.dropped);
        r.surl.push(&c.scope_url);
        self.kv.render(&mut r.ak.s, &mut r.av.s);
        r.ak.commit();
        r.av.commit();
        r.temp.push(c.temp);
        r.mono.push(c.mono);
        r.bounds.v.extend_from_slice(&self.bounds);
        r.bounds.commit();
        r.first.push(ts);
        r.stats.rows += 1;
        r.stats.see(ts_ns);
    }

    fn head(&mut self, which: Which, id: u64, start: u64, time: u64) {
        let c = &self.ctx;
        let h = match which {
            Which::Num(i) => &mut self.num[i].h,
            Which::Hist => &mut self.hist.h,
            Which::Exp => &mut self.exp.h,
            Which::Summary => &mut self.summary.h,
        };
        h.name.push(&c.name);
        h.svc.push(&c.svc);
        h.id.push(id);
        h.mtype.push(c.typ);
        h.start.push(dt(start));
        h.time.push(dt(time));
        h.stats.rows += 1;
        h.stats.see(time);
    }

    /// Walks one request: points into the per-type buffers, new series into
    /// the series rows. A metric with no data rejects the whole request, as
    /// the contrib exporter does.
    pub fn fill<M: MetricsView>(&mut self, md: &M) -> Result<(), EmptyMetric> {
        self.clear();
        let merged = self.opts.merge_number_points;
        let ex_attrs = self.opts.exemplar_attributes;
        for rm in md.resources() {
            match rm.resource() {
                Some(r) => {
                    self.rkv.load(r.attributes());
                    service_name(&mut self.ctx.svc, r.attributes());
                }
                None => {
                    self.rkv.load(std::iter::empty::<NoAttr>());
                    self.ctx.svc.clear();
                }
            }
            self.ctx.res_url.clear();
            self.ctx.res_url.extend_from_slice(opt(rm.schema_url()));
            self.ctx.res_ready = false;
            for sm in rm.scopes() {
                let c = &mut self.ctx;
                c.scope_name.clear();
                c.scope_version.clear();
                match sm.scope() {
                    Some(sc) => {
                        self.skv.load(sc.attributes());
                        c.scope_name.extend_from_slice(opt(sc.name()));
                        c.scope_version.extend_from_slice(opt(sc.version()));
                        c.dropped = sc.dropped_attributes_count();
                    }
                    None => {
                        self.skv.load(std::iter::empty::<NoAttr>());
                        c.dropped = 0;
                    }
                }
                c.scope_url.clear();
                c.scope_url.extend_from_slice(sm.schema_url());
                c.scope_ready = false;
                let b = &mut self.buf;
                b.clear();
                self.rkv.hash_into(b);
                put_str(b, &c.res_url);
                put_str(b, &c.scope_name);
                put_str(b, &c.scope_version);
                self.skv.hash_into(b);
                b.extend_from_slice(&c.dropped.to_le_bytes());
                put_str(b, &c.scope_url);
                let h_rs = xxh3_64(b);
                for m in sm.metrics() {
                    let c = &mut self.ctx;
                    c.name.clear();
                    c.name.extend_from_slice(m.name());
                    c.desc.clear();
                    c.desc.extend_from_slice(m.description());
                    c.unit.clear();
                    c.unit.extend_from_slice(m.unit());
                    let Some(d) = m.data() else {
                        return Err(EmptyMetric(format!(
                            "metrics type is unset (metric {:?})",
                            String::from_utf8_lossy(m.name())
                        )));
                    };
                    let (typ, temp, mono) = match d.value_type() {
                        MType::Gauge => (GAUGE, 0, false),
                        MType::Sum => match d.as_sum() {
                            Some(s) => (SUM, s.aggregation_temporality() as i32, s.is_monotonic()),
                            None => (SUM, 0, false),
                        },
                        MType::Histogram => {
                            (HISTOGRAM, d.as_histogram().map_or(0, |h| h.aggregation_temporality() as i32), false)
                        }
                        MType::ExponentialHistogram => (
                            EXP_HISTOGRAM,
                            d.as_exponential_histogram().map_or(0, |h| h.aggregation_temporality() as i32),
                            false,
                        ),
                        MType::Summary => (SUMMARY, 0, false),
                    };
                    c.typ = typ;
                    c.temp = temp;
                    c.mono = mono;
                    let b = &mut self.buf;
                    b.clear();
                    b.extend_from_slice(&h_rs.to_le_bytes());
                    b.push(typ);
                    put_str(b, &c.name);
                    put_str(b, &c.desc);
                    put_str(b, &c.unit);
                    b.extend_from_slice(&(temp as u32).to_le_bytes());
                    b.push(mono as u8);
                    c.moff = b.len();
                    self.bounds.clear();
                    match typ {
                        GAUGE | SUM => {
                            let i = if merged || typ == GAUGE { 0 } else { 1 };
                            if typ == GAUGE {
                                if let Some(g) = d.as_gauge() {
                                    for dp in g.data_points() {
                                        self.number(i, &dp, ex_attrs);
                                    }
                                }
                            } else if let Some(g) = d.as_sum() {
                                for dp in g.data_points() {
                                    self.number(i, &dp, ex_attrs);
                                }
                            }
                        }
                        HISTOGRAM => {
                            if let Some(h) = d.as_histogram() {
                                for dp in h.data_points() {
                                    self.bounds.clear();
                                    self.bounds.extend(dp.explicit_bounds());
                                    let id = self.point_id(dp.attributes(), dp.time_unix_nano());
                                    self.head(Which::Hist, id, dp.start_time_unix_nano(), dp.time_unix_nano());
                                    let b = &mut self.hist;
                                    b.count.push(dp.count());
                                    b.sum.push(dp.sum().unwrap_or(0.0));
                                    b.buckets.v.extend(dp.bucket_counts());
                                    b.buckets.commit();
                                    b.min.push(dp.min().unwrap_or(0.0));
                                    b.max.push(dp.max().unwrap_or(0.0));
                                    b.flags.push(dp.flags().into_inner());
                                    b.ex.push(&mut self.sorter, ex_attrs, dp.exemplars());
                                }
                                self.bounds.clear();
                            }
                        }
                        EXP_HISTOGRAM => {
                            if let Some(h) = d.as_exponential_histogram() {
                                for dp in h.data_points() {
                                    let id = self.point_id(dp.attributes(), dp.time_unix_nano());
                                    self.head(Which::Exp, id, dp.start_time_unix_nano(), dp.time_unix_nano());
                                    let b = &mut self.exp;
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
                                    b.min.push(dp.min().unwrap_or(0.0));
                                    b.max.push(dp.max().unwrap_or(0.0));
                                    b.flags.push(dp.flags().into_inner());
                                    b.ex.push(&mut self.sorter, ex_attrs, dp.exemplars());
                                }
                            }
                        }
                        _ => {
                            if let Some(h) = d.as_summary() {
                                for dp in h.data_points() {
                                    let id = self.point_id(dp.attributes(), dp.time_unix_nano());
                                    self.head(Which::Summary, id, dp.start_time_unix_nano(), dp.time_unix_nano());
                                    let b = &mut self.summary;
                                    b.count.push(dp.count());
                                    b.sum.push(dp.sum());
                                    for q in dp.quantile_values() {
                                        b.q.v.push(q.quantile());
                                        b.qv.push(q.value());
                                    }
                                    b.q.commit();
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

    fn number<P: NumberDataPointView>(&mut self, i: usize, dp: &P, ex_attrs: bool) {
        let id = self.point_id(dp.attributes(), dp.time_unix_nano());
        self.head(Which::Num(i), id, dp.start_time_unix_nano(), dp.time_unix_nano());
        let b = &mut self.num[i];
        b.value.push(value_f64(dp.value()));
        b.flags.push(dp.flags().into_inner());
        b.ex.push(&mut self.sorter, ex_attrs, dp.exemplars());
    }

    /// Row stats of one of layout B's objects.
    pub fn stats(&self, s: Signal) -> Stats {
        match s {
            Signal::MetricsNumberPoints | Signal::MetricsGaugePoints => self.num[0].h.stats,
            Signal::MetricsSumPoints => self.num[1].h.stats,
            Signal::MetricsHistogramPoints => self.hist.h.stats,
            Signal::MetricsExpHistogramPoints => self.exp.h.stats,
            Signal::MetricsSummaryPoints => self.summary.h.stats,
            Signal::MetricsSeries => self.ser.stats,
            _ => Stats::default(),
        }
    }

    /// One object's content columns (no envelope), in object order.
    /// Consumes that object's buffers.
    pub fn content_arrays(&mut self, s: Signal, sc: &Schemas) -> Vec<ArrayRef> {
        let merged = s == Signal::MetricsNumberPoints;
        let xa = self.opts.exemplar_attributes;
        match s {
            Signal::MetricsNumberPoints | Signal::MetricsGaugePoints | Signal::MetricsSumPoints => {
                let b = &mut self.num[if s == Signal::MetricsSumPoints { 1 } else { 0 }];
                let mut v = b.h.arrays(merged);
                v.push(f64s(&mut b.value));
                v.push(u32s(&mut b.flags));
                v.extend(b.ex.arrays(sc, xa));
                v
            }
            Signal::MetricsHistogramPoints => {
                let b = &mut self.hist;
                let mut v = b.h.arrays(false);
                v.push(u64s(&mut b.count));
                v.push(f64s(&mut b.sum));
                v.push(u64_list(&mut b.buckets, sc));
                v.push(f64s(&mut b.min));
                v.push(f64s(&mut b.max));
                v.push(u32s(&mut b.flags));
                v.extend(b.ex.arrays(sc, xa));
                v
            }
            Signal::MetricsExpHistogramPoints => {
                let b = &mut self.exp;
                let mut v = b.h.arrays(false);
                v.push(u64s(&mut b.count));
                v.push(f64s(&mut b.sum));
                v.push(i32s(&mut b.scale));
                v.push(u64s(&mut b.zero));
                v.push(i32s(&mut b.pos_off));
                v.push(u64_list(&mut b.pos, sc));
                v.push(i32s(&mut b.neg_off));
                v.push(u64_list(&mut b.neg, sc));
                v.push(f64s(&mut b.min));
                v.push(f64s(&mut b.max));
                v.push(u32s(&mut b.flags));
                v.extend(b.ex.arrays(sc, xa));
                v
            }
            Signal::MetricsSummaryPoints => {
                let b = &mut self.summary;
                let mut v = b.h.arrays(false);
                v.push(u64s(&mut b.count));
                v.push(f64s(&mut b.sum));
                let off = std::mem::take(&mut b.q.off);
                let q = prim::<Float64Type>(std::mem::take(&mut b.q.v), DataType::Float64);
                v.push(ListOff { off: off.off.clone() }.take(&sc.f64_elem, q));
                v.push(ListOff { off: off.off }.take(&sc.f64_elem, f64s(&mut b.qv)));
                v.push(u32s(&mut b.flags));
                v
            }
            Signal::MetricsSeries => {
                let r = &mut self.ser;
                vec![
                    u64s(&mut r.id),
                    prim::<UInt8Type>(std::mem::take(&mut r.mtype), DataType::UInt8),
                    r.name.take(),
                    r.desc.take(),
                    r.unit.take(),
                    r.svc.take(),
                    r.rk.take(sc),
                    r.rv.take(sc),
                    r.rurl.take(),
                    r.sname.take(),
                    r.sver.take(),
                    r.sk.take(sc),
                    r.sv.take(sc),
                    u32s(&mut r.dropped),
                    r.surl.take(),
                    r.ak.take(sc),
                    r.av.take(sc),
                    i32s(&mut r.temp),
                    Arc::new(BooleanArray::from(std::mem::take(&mut r.mono))),
                    f64_list(&mut r.bounds, sc),
                    u32s(&mut r.first),
                ]
            }
            other => unreachable!("{other:?} is not a layout-B signal"),
        }
    }
}

/// Copies a rendered map (one list element per entry) into a list column.
fn copy_list(src: &Bin, dst: &mut StrList) {
    let base = dst.s.data.len() as i32;
    dst.s.off.extend(src.off[1..].iter().map(|o| o + base));
    dst.s.data.extend_from_slice(&src.data);
    dst.commit();
}

#[derive(Clone, Copy)]
enum Which {
    Num(usize),
    Hist,
    Exp,
    Summary,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_matches_go() {
        let mut b = Vec::new();
        for v in [0u64, 1, 127, 128, 300, u64::MAX] {
            uvarint(&mut b, v);
        }
        assert_eq!(
            b,
            [0, 1, 127, 0x80, 1, 0xac, 2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1]
        );
    }

    #[test]
    fn dt_floors_and_wraps() {
        assert_eq!(dt(1_500_000_000), 1);
        assert_eq!(dt((-1i64) as u64), u32::MAX);
        assert_eq!(dt(u64::MAX), u32::MAX);
    }

    #[test]
    fn announced_epochs_and_pruning() {
        let mut s = SeriesBuf::new(SeriesOptions::default());
        s.announced(&[(1, 10), (2, 10)], "e1");
        assert_eq!(s.cache_len(), 2);
        s.announced(&[(3, 11)], "e1");
        assert_eq!(s.cache_len(), 3, "the previous window stays");
        s.announced(&[(4, 12)], "e1");
        assert_eq!(s.cache_len(), 2, "windows before the previous one are pruned");
        s.announced(&[(5, 12)], "e2");
        assert_eq!(s.cache_len(), 1, "a new epoch empties the cache");
    }

    #[test]
    fn insert_select_series() {
        let o = SeriesOptions::default();
        let q = insert_select(Signal::MetricsSeries, &o, "db", "SRC");
        assert!(q.contains("mapFromArrays(`ResourceAttributesKeys`, `ResourceAttributesValues`)"), "{q}");
        assert!(q.contains("`FirstSeen`, `LastSeen`) SELECT"), "{q}");
        assert!(!q.contains("producer_id"), "{q}");
        assert!(q.starts_with("INSERT INTO db.otel_metrics_series (`series_id`, `MetricType`"), "{q}");
    }
}
