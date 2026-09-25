//! Model-based test of a request split into several objects, against
//! ../model/s3InlineMetrics.qnt (instance `s3InlineMetricsDesign`), with
//! quint-connect: two metric types' logs (G and S, each s3Inline.qnt
//! unchanged, each driven by the same driver as `mbt_s3inline.rs`), plus the
//! request level: the sender keeps a request until it gets a 2xx
//! (`ackRequest`), and an edge crash (`crash`) restarts every lane in a new
//! epoch with the unacknowledged requests resent to all of them.
//!
//! The request-level decision under test is `proto::request_verdict`, which
//! the exporter applies to a request's objects. After every step, besides
//! both logs' variables, the test compares `ackable`: the requests the
//! implementation would ACK now (the verdict over what each type's current
//! lane has found committed) against those the model lets `ackRequest` take
//! (every type's `acked` holds it). A verdict that ACKs early shows up there
//! at the first half-committed request, before any sender drops it.
//!
//!   cargo test --release --test mbt_s3inline_metrics -- --nocapture
//!   OTAPRS_MUTANT=ack_on_any cargo test --release --test mbt_s3inline_metrics   # must fail
//!   QUINT_SEED=0x5eed cargo test ...

mod common;

use common::s3inline::*;
use otap_s3pq::proto::{self, PartOutcome, PutOutcome, Verdict};
use quint_connect::*;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

const REQUESTS: [i64; 2] = [1, 2];

#[derive(Deserialize)]
struct Raw {
    #[serde(rename = "s3InlineMetrics::G::log")]
    g_log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    #[serde(rename = "s3InlineMetrics::G::writers")]
    g_writers: BTreeMap<i64, Writer>,
    #[serde(rename = "s3InlineMetrics::G::lease")]
    g_lease: i64,
    #[serde(rename = "s3InlineMetrics::G::queue")]
    g_queue: BTreeSet<i64>,
    #[serde(rename = "s3InlineMetrics::G::acked")]
    g_acked: BTreeSet<i64>,
    #[serde(rename = "s3InlineMetrics::G::inflight")]
    g_inflight: BTreeSet<Req>,
    #[serde(rename = "s3InlineMetrics::G::responses")]
    g_responses: BTreeSet<Resp>,
    #[serde(rename = "s3InlineMetrics::G::ckpt")]
    g_ckpt: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineMetrics::G::closed")]
    g_closed: BTreeMap<i64, bool>,
    #[serde(rename = "s3InlineMetrics::G::consumer")]
    g_consumer: ConsumerM,
    #[serde(rename = "s3InlineMetrics::G::central")]
    g_central: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineMetrics::S::log")]
    s_log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    #[serde(rename = "s3InlineMetrics::S::writers")]
    s_writers: BTreeMap<i64, Writer>,
    #[serde(rename = "s3InlineMetrics::S::lease")]
    s_lease: i64,
    #[serde(rename = "s3InlineMetrics::S::queue")]
    s_queue: BTreeSet<i64>,
    #[serde(rename = "s3InlineMetrics::S::acked")]
    s_acked: BTreeSet<i64>,
    #[serde(rename = "s3InlineMetrics::S::inflight")]
    s_inflight: BTreeSet<Req>,
    #[serde(rename = "s3InlineMetrics::S::responses")]
    s_responses: BTreeSet<Resp>,
    #[serde(rename = "s3InlineMetrics::S::ckpt")]
    s_ckpt: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineMetrics::S::closed")]
    s_closed: BTreeMap<i64, bool>,
    #[serde(rename = "s3InlineMetrics::S::consumer")]
    s_consumer: ConsumerM,
    #[serde(rename = "s3InlineMetrics::S::central")]
    s_central: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineMetricsDesign::s3InlineMetrics::reqAcked")]
    req_acked: BTreeSet<i64>,
}

/// Both logs' variables, the requests acked, and the requests ACKable now.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(from = "Raw")]
pub struct MetricsSpec {
    g: LogState,
    s: LogState,
    req_acked: BTreeSet<i64>,
    ackable: BTreeSet<i64>,
}

impl From<Raw> for MetricsSpec {
    fn from(r: Raw) -> Self {
        let g = LogState { log: r.g_log, writers: r.g_writers, lease: r.g_lease, queue: r.g_queue, acked: r.g_acked, inflight: r.g_inflight, responses: r.g_responses, ckpt: r.g_ckpt, closed: r.g_closed, consumer: r.g_consumer, central: r.g_central };
        let s = LogState { log: r.s_log, writers: r.s_writers, lease: r.s_lease, queue: r.s_queue, acked: r.s_acked, inflight: r.s_inflight, responses: r.s_responses, ckpt: r.s_ckpt, closed: r.s_closed, consumer: r.s_consumer, central: r.s_central };
        // `ackRequest(p)` is enabled: not acked yet, and every type's lane
        // has found p committed.
        let ackable = REQUESTS
            .into_iter()
            .filter(|p| !r.req_acked.contains(p) && g.acked.contains(p) && s.acked.contains(p))
            .collect();
        MetricsSpec { g, s, req_acked: r.req_acked, ackable }
    }
}

#[derive(Default)]
pub struct MetricsDriver {
    g: S3InlineDriver,
    s: S3InlineDriver,
    req_acked: BTreeSet<i64>,
}

/// What the exporter's `commit_one` reports for request p's object of one
/// type on this attempt: the type's current lane knows p committed, or it is
/// not resolved yet.
fn part(d: &S3InlineDriver, p: i64) -> PartOutcome {
    match d.lanes.get(&d.lease).and_then(|l| l.known(&content(p))) {
        Some(r) => PartOutcome::Committed(r.clone()),
        None => PartOutcome::Unresolved("not committed yet".into()),
    }
}

impl MetricsDriver {
    fn verdict(&self, p: i64) -> Verdict {
        proto::request_verdict_with(&[part(&self.g, p), part(&self.s, p)], mutation())
    }

    fn init(&mut self) {
        self.g.prefix = "mbt/metrics_gauge".into();
        self.s.prefix = "mbt/metrics_sum".into();
        self.g.init();
        self.s.init();
        self.req_acked.clear();
    }

    fn ack_request(&mut self, p: i64) {
        assert_eq!(self.verdict(p), Verdict::Ack, "the model ACKs request {p}, the exporter wouldn't");
        self.req_acked.insert(p);
    }

    /// The edge restarts: new lanes in new epochs for every type, nothing
    /// remembered, the sender resends what it has no 2xx for.
    fn crash(&mut self) {
        let pending: BTreeSet<i64> = REQUESTS.into_iter().filter(|p| !self.req_acked.contains(p)).collect();
        for d in [&mut self.g, &mut self.s] {
            d.new_incarnation(false);
            d.queue = pending.clone();
            d.acked.clear();
        }
    }
}

fn tomb_timeout(d: &mut S3InlineDriver) {
    let _ = d.consumer.on_tomb_put(PutOutcome::Unknown);
}

impl Driver for MetricsDriver {
    type State = MetricsSpec;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init(),
            step => self.init(), // see mbt_s3inline.rs: the Rust evaluator's state-0 label
            ackRequest(p: i64) => self.ack_request(p),
            crash => self.crash(),
            gStartPush(e: i64, p: i64) => self.g.start_push(e, p),
            gSend(e: i64) => self.g.send(e),
            gTimeout(e: i64) => self.g.timeout(e),
            gResolve(e: i64) => self.g.resolve(e),
            gSwitchPayload(e: i64, p: i64) => self.g.switch_payload(e, p),
            gReceive(e: i64, r: Resp) => self.g.receive(e, r),
            gApply(q: Req) => self.g.apply(q),
            gLose(q: Req) => self.g.lose(q),
            gCCheck(e: i64) => self.g.c_check(e),
            gCInsert => self.g.c_insert(),
            gCAdvance => self.g.consumer.advance(),
            gCSeeTomb(e: i64) => self.g.consumer.see_tomb(&epoch_name(e)),
            gCTomb(e: i64) => self.g.c_tomb(e),
            gCTombReceive(r: Resp) => self.g.c_tomb_receive(r),
            gCTombTimeout => tomb_timeout(&mut self.g),
            gCTombResolve => self.g.c_tomb_resolve(),
            gCCrash => self.g.consumer.crash(),
            sStartPush(e: i64, p: i64) => self.s.start_push(e, p),
            sSend(e: i64) => self.s.send(e),
            sTimeout(e: i64) => self.s.timeout(e),
            sResolve(e: i64) => self.s.resolve(e),
            sSwitchPayload(e: i64, p: i64) => self.s.switch_payload(e, p),
            sReceive(e: i64, r: Resp) => self.s.receive(e, r),
            sApply(q: Req) => self.s.apply(q),
            sLose(q: Req) => self.s.lose(q),
            sCCheck(e: i64) => self.s.c_check(e),
            sCInsert => self.s.c_insert(),
            sCAdvance => self.s.consumer.advance(),
            sCSeeTomb(e: i64) => self.s.consumer.see_tomb(&epoch_name(e)),
            sCTomb(e: i64) => self.s.c_tomb(e),
            sCTombReceive(r: Resp) => self.s.c_tomb_receive(r),
            sCTombTimeout => tomb_timeout(&mut self.s),
            sCTombResolve => self.s.c_tomb_resolve(),
            sCCrash => self.s.consumer.crash(),
        })
    }
}

impl State<MetricsDriver> for MetricsSpec {
    fn from_driver(d: &MetricsDriver) -> Result<Self> {
        let ackable = REQUESTS
            .into_iter()
            .filter(|p| !d.req_acked.contains(p) && d.verdict(*p) == Verdict::Ack)
            .collect();
        Ok(MetricsSpec { g: d.g.project(), s: d.s.project(), req_acked: d.req_acked.clone(), ackable })
    }
}

#[quint_run(spec = "../model/s3InlineMetrics.qnt", main = "s3InlineMetricsDesign", max_samples = 300, max_steps = 80)]
fn s3inline_metrics_design_simulation() -> impl Driver {
    MetricsDriver::default()
}
