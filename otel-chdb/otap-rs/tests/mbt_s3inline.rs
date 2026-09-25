//! Model-based test of the commit protocol against ../model/s3Inline.qnt
//! (instance `s3InlineDesign`: ambiguous and late S3 writes, zombie writers,
//! consumer crashes), with quint-connect.
//!
//! quint-connect runs `quint run --mbt` on the model, and replays every
//! trace step by step: each model action becomes a call into this crate's
//! `proto::Lane` (the writer) or `proto::Consumer`, with the outcome the
//! model chose injected (200, 412, no answer, a late or lost request, a
//! crash, a zombie). S3 is an in-memory bucket with create-only puts, holding
//! the same user metadata `runner::append` writes and read back through
//! `Slot::from_meta`; central is a count per content key. After every step
//! the implementation's state is projected onto the model's variables (what
//! each slot holds, each writer's phase, slot and batch, the queue and its
//! acks, the consumer's checkpoints, closed epochs and phase, what central
//! holds, the requests in flight) and compared.
//!
//!   cargo test --release --test mbt_s3inline -- --nocapture          # quint on PATH
//!   OTAPRS_MUTANT=retry_new_key|no_halt|no_check_central cargo test ... # must fail
//!   QUINT_SEED=0x5eed cargo test ...                                   # reproduce

mod common;

use common::s3inline::*;
use otap_s3pq::proto::PutOutcome;
use quint_connect::*;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// The model variables the implementation is checked against (all but the
/// history variables `everLog` and `events`).
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct SpecState {
    #[serde(rename = "s3InlineDesign::s3Inline::log")]
    log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    #[serde(rename = "s3InlineDesign::s3Inline::writers")]
    writers: BTreeMap<i64, Writer>,
    #[serde(rename = "s3InlineDesign::s3Inline::lease")]
    lease: i64,
    #[serde(rename = "s3InlineDesign::s3Inline::queue")]
    queue: BTreeSet<i64>,
    #[serde(rename = "s3InlineDesign::s3Inline::acked")]
    acked: BTreeSet<i64>,
    #[serde(rename = "s3InlineDesign::s3Inline::inflight")]
    inflight: BTreeSet<Req>,
    #[serde(rename = "s3InlineDesign::s3Inline::responses")]
    responses: BTreeSet<Resp>,
    #[serde(rename = "s3InlineDesign::s3Inline::ckpt")]
    ckpt: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineDesign::s3Inline::closed")]
    closed: BTreeMap<i64, bool>,
    #[serde(rename = "s3InlineDesign::s3Inline::consumer")]
    consumer: ConsumerM,
    #[serde(rename = "s3InlineDesign::s3Inline::central")]
    central: BTreeMap<i64, i64>,
}

impl Driver for S3InlineDriver {
    type State = SpecState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init(),
            // quint's Rust evaluator (v0.6.0) labels the initial state of some
            // traces `step` instead of `init` (only ever state 0; the
            // TypeScript backend labels them all `init`). A `step` anywhere
            // else would reset the driver, and the state check would fail.
            step => self.init(),
            newIncarnation(z: bool) => self.new_incarnation(z),
            startPush(e: i64, p: i64) => self.start_push(e, p),
            send(e: i64) => self.send(e),
            apply(q: Req) => self.apply(q),
            lose(q: Req) => self.lose(q),
            receive(e: i64, r: Resp) => self.receive(e, r),
            timeout(e: i64) => self.timeout(e),
            resolve(e: i64) => self.resolve(e),
            switchPayload(e: i64, p: i64) => self.switch_payload(e, p),
            cCheck(e: i64) => self.c_check(e),
            cInsert => self.c_insert(),
            cAdvance => self.consumer.advance(),
            cSeeTomb(e: i64) => self.consumer.see_tomb(&epoch_name(e)),
            cTomb(e: i64) => self.c_tomb(e),
            cTombReceive(r: Resp) => self.c_tomb_receive(r),
            cTombTimeout => { let _ = self.consumer.on_tomb_put(PutOutcome::Unknown); },
            cTombResolve => self.c_tomb_resolve(),
            cCrash => self.consumer.crash(),
        })
    }
}

impl State<S3InlineDriver> for SpecState {
    fn from_driver(d: &S3InlineDriver) -> Result<Self> {
        let l = d.project();
        Ok(SpecState {
            log: l.log,
            writers: l.writers,
            lease: l.lease,
            queue: l.queue,
            acked: l.acked,
            inflight: l.inflight,
            responses: l.responses,
            ckpt: l.ckpt,
            closed: l.closed,
            consumer: l.consumer,
            central: l.central,
        })
    }
}

#[quint_run(spec = "../model/s3Inline.qnt", main = "s3InlineDesign", max_samples = 300, max_steps = 60)]
fn s3inline_design_simulation() -> impl Driver {
    S3InlineDriver { prefix: "mbt/traces".into(), ..Default::default() }
}
