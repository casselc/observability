//! OTLP/OTAP → ClickStack rows, written straight into column buffers.
//!
//! The walk is generic over otap-dataflow's backend-agnostic view traits, so
//! one implementation reads OTLP protobuf bytes (`RawTraceData`, zero-copy,
//! no pdata decode) and OTAP Arrow record batches (`OtapTracesView`). It
//! visits rows in the same order, and renders every field the same way, as
//! `parquetgo/walk.go` (itself a copy of the chdb exporter's walker), which is
//! the contrib clickhouse exporter's row shape.

use crate::columns::{Bin, ListOff, Map, prim, repeat_bin};
use crate::render;
use crate::schema::{SCHEMA_VERSION, Schemas, ts_type};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{
    DataType, TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use otel_arrow_dfe_pdata_views::views::common::{
    AnyValueView, AttributeView, InstrumentationScopeView, ValueType,
};
use otel_arrow_dfe_pdata_views::views::logs::{
    LogRecordView, LogsDataView, ResourceLogsView, ScopeLogsView,
};
use otel_arrow_dfe_pdata_views::views::resource::ResourceView;
use otel_arrow_dfe_pdata_views::views::trace::{
    EventView, LinkView, ResourceSpansView, ScopeSpansView, SpanView, StatusView, TracesView,
};

/// The batch identity stamped on every row.
#[derive(Clone, Debug)]
pub struct Envelope {
    pub producer: String,
    pub epoch: String,
    pub batch: u64,
    pub received_ns: u64,
}

/// What the walk learned, for the object's metadata.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub rows: usize,
    pub min_ts: u64,
    pub max_ts: u64,
}

impl Stats {
    #[inline]
    fn see(&mut self, ts: u64) {
        // parquetgo Envelope.write: 0 means "unset" for the minimum.
        if self.min_ts == 0 || ts < self.min_ts {
            self.min_ts = ts;
        }
        if ts > self.max_ts {
            self.max_ts = ts;
        }
    }
}

fn push_value<'a, V: AnyValueView<'a>>(dst: &mut Bin, v: Option<&V>) {
    match v {
        Some(val) if val.value_type() == ValueType::String => {
            dst.push(val.as_string().unwrap_or_default())
        }
        _ => {
            render::value(&mut dst.data, v);
            dst.commit();
        }
    }
}

/// Appends one map (a row's, or a list element's) of attributes.
fn push_attrs<A: AttributeView>(m: &mut Map, it: impl Iterator<Item = A>) {
    for kv in it {
        m.keys.push(kv.key());
        let v = kv.value();
        push_value(&mut m.vals, v.as_ref());
    }
    m.commit();
}

/// A map rendered once and copied into every row that shares it (resource
/// and scope attributes).
#[derive(Default)]
struct PreMap {
    keys: Bin,
    vals: Bin,
}

impl PreMap {
    fn fill<A: AttributeView>(&mut self, it: impl Iterator<Item = A>) {
        self.keys.clear();
        self.vals.clear();
        for kv in it {
            self.keys.push(kv.key());
            let v = kv.value();
            push_value(&mut self.vals, v.as_ref());
        }
    }
    fn copy_into(&self, m: &mut Map) {
        for i in 0..self.keys.len() {
            let (k0, k1) = (self.keys.off[i] as usize, self.keys.off[i + 1] as usize);
            let (v0, v1) = (self.vals.off[i] as usize, self.vals.off[i + 1] as usize);
            m.keys.push(&self.keys.data[k0..k1]);
            m.vals.push(&self.vals.data[v0..v1]);
        }
        m.commit();
    }
}

/// pcommon `Map.Get("service.name")`: the first such key; a string as is,
/// anything else as `AsString`.
fn service_name<A: AttributeView>(dst: &mut Vec<u8>, it: impl Iterator<Item = A>) {
    dst.clear();
    for kv in it {
        if kv.key() == b"service.name" {
            let v = kv.value();
            match &v {
                Some(val) if val.value_type() == ValueType::String => {
                    dst.extend_from_slice(val.as_string().unwrap_or_default())
                }
                _ => render::value(dst, v.as_ref()),
            }
            return;
        }
    }
}

fn opt(b: Option<&[u8]>) -> &[u8] {
    b.unwrap_or_default()
}

fn envelope_cols(env: &Envelope, n: usize) -> Vec<ArrayRef> {
    vec![
        repeat_bin(env.producer.as_bytes(), n),
        repeat_bin(env.epoch.as_bytes(), n),
        prim::<UInt64Type>(vec![env.batch; n], DataType::UInt64),
        prim::<UInt32Type>((0..n as u32).collect(), DataType::UInt32),
        prim::<TimestampNanosecondType>(vec![env.received_ns as i64; n], ts_type()),
        prim::<UInt16Type>(vec![SCHEMA_VERSION; n], DataType::UInt16),
    ]
}

/// Reusable buffers for one signal (traces).
#[derive(Default)]
pub struct TracesBuf {
    ts: Vec<i64>,
    trace_id: Bin,
    span_id: Bin,
    parent: Bin,
    trace_state: Bin,
    name: Bin,
    kind: Bin,
    svc: Bin,
    res_attrs: Map,
    scope_name: Bin,
    scope_version: Bin,
    attrs: Map,
    duration: Vec<u64>,
    status_code: Bin,
    status_msg: Bin,
    ev_off: ListOff,
    ev_ts: Vec<i64>,
    ev_name: Bin,
    ev_attrs: Map,
    ln_off: ListOff,
    ln_tid: Bin,
    ln_sid: Bin,
    ln_state: Bin,
    ln_attrs: Map,
    res_pre: PreMap,
    svc_scratch: Vec<u8>,
}

impl TracesBuf {
    fn clear(&mut self) {
        self.ts.clear();
        self.duration.clear();
        self.ev_ts.clear();
        for b in [
            &mut self.trace_id,
            &mut self.span_id,
            &mut self.parent,
            &mut self.trace_state,
            &mut self.name,
            &mut self.kind,
            &mut self.svc,
            &mut self.scope_name,
            &mut self.scope_version,
            &mut self.status_code,
            &mut self.status_msg,
            &mut self.ev_name,
            &mut self.ln_tid,
            &mut self.ln_sid,
            &mut self.ln_state,
        ] {
            b.clear();
        }
        for m in [
            &mut self.res_attrs,
            &mut self.attrs,
            &mut self.ev_attrs,
            &mut self.ln_attrs,
        ] {
            m.clear();
        }
        self.ev_off.clear();
        self.ln_off.clear();
    }

    /// Walks the traces into the buffers; returns the row stats.
    pub fn fill<T: TracesView>(&mut self, t: &T) -> Stats {
        self.clear();
        let mut st = Stats::default();
        for rs in t.resources() {
            let res = rs.resource();
            match &res {
                Some(r) => {
                    self.res_pre.fill(r.attributes());
                    service_name(&mut self.svc_scratch, r.attributes());
                }
                None => {
                    self.res_pre.fill(std::iter::empty::<NoAttr>());
                    self.svc_scratch.clear();
                }
            }
            for ss in rs.scopes() {
                let scope = ss.scope();
                let (sname, sver) = match &scope {
                    Some(s) => (opt(s.name()).to_vec(), opt(s.version()).to_vec()),
                    None => (Vec::new(), Vec::new()),
                };
                for s in ss.spans() {
                    let start = s.start_time_unix_nano().unwrap_or(0);
                    let end = s.end_time_unix_nano().unwrap_or(0);
                    st.see(start);
                    st.rows += 1;
                    self.ts.push(start as i64);
                    render::hex_id(&mut self.trace_id.data, s.trace_id().map(|x| &x[..]));
                    self.trace_id.commit();
                    render::hex_id(&mut self.span_id.data, s.span_id().map(|x| &x[..]));
                    self.span_id.commit();
                    render::hex_id(&mut self.parent.data, s.parent_span_id().map(|x| &x[..]));
                    self.parent.commit();
                    self.trace_state.push(opt(s.trace_state()));
                    self.name.push(opt(s.name()));
                    self.kind.push(render::span_kind(s.kind()));
                    self.svc.push(&self.svc_scratch);
                    self.res_pre.copy_into(&mut self.res_attrs);
                    self.scope_name.push(&sname);
                    self.scope_version.push(&sver);
                    push_attrs(&mut self.attrs, s.attributes());
                    self.duration.push(end.wrapping_sub(start));
                    match s.status() {
                        Some(stt) => {
                            self.status_code.push(render::status_code(stt.status_code()));
                            self.status_msg.push(opt(stt.message()));
                        }
                        None => {
                            self.status_code.push(render::status_code(0));
                            self.status_msg.push(b"");
                        }
                    }
                    for ev in s.events() {
                        self.ev_ts.push(ev.time_unix_nano().unwrap_or(0) as i64);
                        self.ev_name.push(opt(ev.name()));
                        push_attrs(&mut self.ev_attrs, ev.attributes());
                    }
                    self.ev_off.commit(self.ev_ts.len());
                    for l in s.links() {
                        render::hex_id(&mut self.ln_tid.data, l.trace_id().map(|x| &x[..]));
                        self.ln_tid.commit();
                        render::hex_id(&mut self.ln_sid.data, l.span_id().map(|x| &x[..]));
                        self.ln_sid.commit();
                        self.ln_state.push(opt(l.trace_state()));
                        push_attrs(&mut self.ln_attrs, l.attributes());
                    }
                    self.ln_off.commit(self.ln_tid.len());
                }
            }
        }
        st
    }

    /// Everything but the envelope, as arrays (consumes the buffers).
    pub fn content_arrays(&mut self, sc: &Schemas) -> Vec<ArrayRef> {
        let ev_off = std::mem::take(&mut self.ev_off);
        let ln_off = std::mem::take(&mut self.ln_off);
        let lst = |o: &ListOff, elem, vals| {
            let mut c = ListOff { off: o.off.clone() };
            c.take(elem, vals)
        };
        vec![
            prim::<TimestampNanosecondType>(std::mem::take(&mut self.ts), ts_type()),
            self.trace_id.take(),
            self.span_id.take(),
            self.parent.take(),
            self.trace_state.take(),
            self.name.take(),
            self.kind.take(),
            self.svc.take(),
            self.res_attrs.take(&sc.entries),
            self.scope_name.take(),
            self.scope_version.take(),
            self.attrs.take(&sc.entries),
            prim::<UInt64Type>(std::mem::take(&mut self.duration), DataType::UInt64),
            self.status_code.take(),
            self.status_msg.take(),
            lst(
                &ev_off,
                &sc.ts_elem,
                prim::<TimestampNanosecondType>(std::mem::take(&mut self.ev_ts), ts_type()),
            ),
            lst(&ev_off, &sc.str_elem, self.ev_name.take()),
            lst(&ev_off, &sc.map_elem, self.ev_attrs.take(&sc.entries)),
            lst(&ln_off, &sc.str_elem, self.ln_tid.take()),
            lst(&ln_off, &sc.str_elem, self.ln_sid.take()),
            lst(&ln_off, &sc.str_elem, self.ln_state.take()),
            lst(&ln_off, &sc.map_elem, self.ln_attrs.take(&sc.entries)),
        ]
    }
}

/// Reusable buffers for logs.
#[derive(Default)]
pub struct LogsBuf {
    ts: Vec<i64>,
    trace_id: Bin,
    span_id: Bin,
    flags: Vec<u8>,
    sev_text: Bin,
    sev_num: Vec<u8>,
    svc: Bin,
    body: Bin,
    res_url: Bin,
    res_attrs: Map,
    scope_url: Bin,
    scope_name: Bin,
    scope_version: Bin,
    scope_attrs: Map,
    attrs: Map,
    event_name: Bin,
    res_pre: PreMap,
    scope_pre: PreMap,
    svc_scratch: Vec<u8>,
}

impl LogsBuf {
    fn clear(&mut self) {
        self.ts.clear();
        self.flags.clear();
        self.sev_num.clear();
        for b in [
            &mut self.trace_id,
            &mut self.span_id,
            &mut self.sev_text,
            &mut self.svc,
            &mut self.body,
            &mut self.res_url,
            &mut self.scope_url,
            &mut self.scope_name,
            &mut self.scope_version,
            &mut self.event_name,
        ] {
            b.clear();
        }
        for m in [&mut self.res_attrs, &mut self.scope_attrs, &mut self.attrs] {
            m.clear();
        }
    }

    pub fn fill<L: LogsDataView>(&mut self, l: &L) -> Stats {
        self.clear();
        let mut st = Stats::default();
        for rl in l.resources() {
            let res = rl.resource();
            match &res {
                Some(r) => {
                    self.res_pre.fill(r.attributes());
                    service_name(&mut self.svc_scratch, r.attributes());
                }
                None => {
                    self.res_pre.fill(std::iter::empty::<NoAttr>());
                    self.svc_scratch.clear();
                }
            }
            let res_url = opt(rl.schema_url()).to_vec();
            for sl in rl.scopes() {
                let scope = sl.scope();
                let (sname, sver) = match &scope {
                    Some(s) => {
                        self.scope_pre.fill(s.attributes());
                        (opt(s.name()).to_vec(), opt(s.version()).to_vec())
                    }
                    None => {
                        self.scope_pre.fill(std::iter::empty::<NoAttr>());
                        (Vec::new(), Vec::new())
                    }
                };
                let scope_url = opt(sl.schema_url()).to_vec();
                for r in sl.log_records() {
                    let mut ts = r.time_unix_nano().unwrap_or(0);
                    if ts == 0 {
                        ts = r.observed_time_unix_nano().unwrap_or(0);
                    }
                    st.see(ts);
                    st.rows += 1;
                    self.ts.push(ts as i64);
                    render::hex_id(&mut self.trace_id.data, r.trace_id().map(|x| &x[..]));
                    self.trace_id.commit();
                    render::hex_id(&mut self.span_id.data, r.span_id().map(|x| &x[..]));
                    self.span_id.commit();
                    self.flags.push(r.flags().unwrap_or(0) as u8);
                    self.sev_text.push(opt(r.severity_text()));
                    self.sev_num.push(r.severity_number().unwrap_or(0) as u8);
                    self.svc.push(&self.svc_scratch);
                    let body = r.body();
                    push_value(&mut self.body, body.as_ref());
                    self.res_url.push(&res_url);
                    self.res_pre.copy_into(&mut self.res_attrs);
                    self.scope_url.push(&scope_url);
                    self.scope_name.push(&sname);
                    self.scope_version.push(&sver);
                    self.scope_pre.copy_into(&mut self.scope_attrs);
                    push_attrs(&mut self.attrs, r.attributes());
                    self.event_name.push(opt(r.event_name()));
                }
            }
        }
        st
    }

    pub fn content_arrays(&mut self, sc: &Schemas) -> Vec<ArrayRef> {
        vec![
            prim::<TimestampNanosecondType>(std::mem::take(&mut self.ts), ts_type()),
            self.trace_id.take(),
            self.span_id.take(),
            prim::<UInt8Type>(std::mem::take(&mut self.flags), DataType::UInt8),
            self.sev_text.take(),
            prim::<UInt8Type>(std::mem::take(&mut self.sev_num), DataType::UInt8),
            self.svc.take(),
            self.body.take(),
            self.res_url.take(),
            self.res_attrs.take(&sc.entries),
            self.scope_url.take(),
            self.scope_name.take(),
            self.scope_version.take(),
            self.scope_attrs.take(&sc.entries),
            self.attrs.take(&sc.entries),
            self.event_name.take(),
        ]
    }
}

/// Builds the record batch: content columns plus the envelope.
pub fn record_batch(sc: &Schemas, mut cols: Vec<ArrayRef>, rows: usize, env: &Envelope) -> RecordBatch {
    cols.extend(envelope_cols(env, rows));
    RecordBatch::try_new(sc.arrow.clone(), cols).expect("columns match the published schema")
}

/// An attribute type for "no attributes" (a missing resource or scope).
pub enum NoAttr {}
impl AttributeView for NoAttr {
    type Val<'val>
        = NoVal
    where
        Self: 'val;
    fn key(&self) -> &[u8] {
        match *self {}
    }
    fn value(&self) -> Option<NoVal> {
        match *self {}
    }
}
pub enum NoVal {}
impl<'a> AnyValueView<'a> for NoVal {
    type KeyValue = NoAttr;
    type ArrayIter<'arr>
        = std::iter::Empty<NoVal>
    where
        Self: 'arr;
    type KeyValueIter<'kv>
        = std::iter::Empty<NoAttr>
    where
        Self: 'kv;
    fn value_type(&self) -> ValueType {
        match *self {}
    }
    fn as_string(&self) -> Option<&[u8]> {
        match *self {}
    }
    fn as_bool(&self) -> Option<bool> {
        match *self {}
    }
    fn as_int64(&self) -> Option<i64> {
        match *self {}
    }
    fn as_double(&self) -> Option<f64> {
        match *self {}
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        match *self {}
    }
    fn as_array(&self) -> Option<Self::ArrayIter<'_>> {
        match *self {}
    }
    fn as_kvlist(&self) -> Option<Self::KeyValueIter<'_>> {
        match *self {}
    }
}
