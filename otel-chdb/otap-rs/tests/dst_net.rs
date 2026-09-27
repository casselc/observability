//! Deterministic simulation of the consumer fleet over a simulated network
//! (DST.md, level 2): turmoil hosts for an S3 emulator and a ClickHouse
//! emulator (`tests/dst/{s3emu,chemu}.rs`), the edges, N workers and GC,
//! each using the REAL clients: object_store's AmazonS3 (its signing, XML,
//! retries and error classification) through an `HttpConnector` onto
//! turmoil's TCP, and `ClickHouseCentral` (the real SQL, settings and error
//! handling) through the `central::Transport` seam. Faults: the emulators'
//! (AMBIGUITY.md S1, S3, S5, S6, C1, C2) and the network's (partitions,
//! held links, restarts of worker hosts). Invariants as `dst_consumer.rs`.
//!
//! Also the emulators' fidelity: `s3_emulator_matches_seaweedfs` and
//! `ch_emulator_matches_clickhouse` run one scripted sequence through the
//! real clients against the emulator (on a real socket) and against the
//! local SeaweedFS / ClickHouse, and compare (skipped when those aren't up).
//!
//!   cargo test --release --test dst_net -- net_seeds                 # 8 seeds
//!   DST_SEEDS=100 cargo test --release --test dst_net -- net_seeds
//!   DST_SEED=5 DST_TRACE=1 cargo test --release --test dst_net -- net_seeds --nocapture

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
mod consumer;
mod dst;

use bytes::Bytes;
use consumer::bucket::{Bucket, Cond, Put, S3Bucket};
use consumer::coord::{CkptDoc, LeaseDoc, Mutation, Timing, join};
use consumer::discovery::Backoff;
use consumer::gc::{GcConfig, gc_step};
use consumer::plan::Obj;
use consumer::sql::{Central, ClickHouseCentral, Fence, LaneKind};
use consumer::worker::{BalanceMode, Clock, Config, Worker};
use dst::chemu::{ChEmu, ChFaults, InsertFault, ObjInfo};
use dst::net::{Pause, TurmoilS3, ch_transport, serve_ch, serve_s3};
use dst::s3emu::{Fault, Faults, S3Emu};
use dst::sim::{self, Sim, now_ms, sleep_ms, trace};
use object_store::aws::AmazonS3Builder;
use object_store::{ClientOptions, RetryConfig};
use otap_s3pq::proto;
use otap_s3pq::store::S3Store;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;

const BUCKET: &str = "b";
const ROOT: &str = "r/edges";
const CTL: &str = "r/ctl";
const DB: &str = "db";
const SIGNALS: [&str; 3] = ["traces", "logs", "metrics_gauge"];
const T: Timing = Timing { ttl_ms: 9000, margin_ms: 1000, budget_ms: 3000, slack_ms: 1000, mutation: Mutation::None };
/// The S3 client's whole-request timeout (put_timeout, scaled like T).
const S3_TIMEOUT: Duration = Duration::from_millis(1_200);

#[derive(Clone, Debug)]
struct NetProfile {
    workers: usize,
    producers: usize,
    run_ms: u64,
    lat_ms: u64,
    /// S3: a conditional PUT applied, then answered 500 (object_store retries: 412 for our own write).
    s3_err_after: f64,
    /// S3: a PUT applied, then the connection drops (object_store retries it as unsent).
    s3_drop_after: f64,
    /// S3: a request refused with 503 (nothing applied).
    s3_refuse: f64,
    /// S3: the connection drops before the request is applied.
    s3_drop_before: f64,
    list_lag_ms: u64,
    ch_settled: f64,
    ch_lost: f64,
    ch_late: f64,
    ch_timeout_commit: f64,
    ch_check_err: f64,
    /// C2: the server profile breaks limits instead of throwing.
    ch_break_profile: f64,
    server_skew_ms: i64,
    /// Per second of simulated time, per worker.
    partition_rate: f64,
    hold_rate: f64,
    bounce_rate: f64,
    pause_rate: f64,
}

impl NetProfile {
    fn draw(sim: &Sim) -> NetProfile {
        let pf = |xs: &[f64]| *sim.rng.borrow_mut().pick(xs);
        let pu = |xs: &[u64]| *sim.rng.borrow_mut().pick(xs);
        NetProfile {
            workers: sim.range(1, 3) as usize,
            producers: sim.range(1, 2) as usize,
            run_ms: sim.range(20_000, 45_000),
            lat_ms: pu(&[1, 5, 20]),
            s3_err_after: pf(&[0.0, 0.02, 0.05]),
            s3_drop_after: pf(&[0.0, 0.02, 0.05]),
            s3_refuse: pf(&[0.0, 0.01, 0.03]),
            s3_drop_before: pf(&[0.0, 0.01]),
            list_lag_ms: if std::env::var_os("DST_LIST_LAG").is_some() { pu(&[0, 500, 2_000]) } else { 0 },
            ch_settled: pf(&[0.0, 0.03]),
            ch_lost: pf(&[0.0, 0.05, 0.1]),
            ch_late: pf(&[0.0, 0.05]),
            ch_timeout_commit: pf(&[0.0, 0.05]),
            ch_check_err: pf(&[0.0, 0.05]),
            ch_break_profile: pf(&[0.0, 0.05]),
            server_skew_ms: sim.range(0, T.margin_ms) as i64 - T.margin_ms as i64 / 2,
            partition_rate: pf(&[0.0, 0.01, 0.03]),
            hold_rate: pf(&[0.0, 0.01, 0.03]),
            bounce_rate: pf(&[0.0, 0.01, 0.02]),
            pause_rate: pf(&[0.0, 0.01, 0.03]),
        }
    }
}

struct World {
    sim: Rc<Sim>,
    p: NetProfile,
    s3: Rc<S3Emu>,
    ch: Rc<ChEmu>,
    ever: RefCell<BTreeMap<(String, String, u64), String>>,
    rows_of: RefCell<BTreeMap<(String, String), u64>>,
    leases: RefCell<BTreeMap<String, (LeaseDoc, u64)>>,
    /// Statement number -> the lease epochs of its lanes when it arrived.
    arrivals: RefCell<BTreeMap<u64, Vec<(String, u64)>>>,
    violations: RefCell<Vec<String>>,
    stop: Cell<bool>,
    healed: Cell<bool>,
    incarnations: RefCell<Vec<u32>>,
    /// Per worker host: its process pause.
    pauses: Vec<Pause>,
    n_content: Cell<u64>,
}

fn table_of(signal: &str) -> String {
    otap_s3pq::Signal::from_name(signal).expect("signal").table().to_string()
}

fn parse_slot(key: &str) -> Option<(String, String, u64)> {
    let rest = key.strip_prefix(ROOT)?.strip_prefix('/')?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 4 {
        return None;
    }
    Some((format!("{}/{}", parts[0], parts[1]), parts[2].to_string(), parts[3].strip_suffix(".parquet")?.parse().ok()?))
}

fn url_key(url: &str) -> Option<&str> {
    url.strip_prefix("http://s3:9000/")?.strip_prefix(BUCKET)?.strip_prefix('/')
}

impl World {
    fn violation(&self, v: String) {
        trace(format!("VIOLATION {v}"));
        self.violations.borrow_mut().push(format!("t={} {v}", now_ms()));
    }

    fn fault(&self, p: f64) -> bool {
        !self.healed.get() && self.sim.chance(p)
    }

    fn doc<D: serde::de::DeserializeOwned>(&self, key: &str) -> Option<D> {
        self.s3.objs.borrow().get(key).and_then(|o| serde_json::from_slice(&o.body).ok())
    }

    fn lease_epoch(&self, lane: &str) -> Option<u64> {
        self.doc::<LeaseDoc>(&format!("{}.json", join(&join(CTL, "lease"), lane))).map(|d| d.epoch)
    }

    fn count(&self, table: &str, content: &str) -> u64 {
        self.ch.count(&format!("{DB}.{table}"), content)
    }

    /// Every applied S3 change: record data, check checkpoints and leases.
    fn changed(&self, key: &str, obj: Option<&dst::s3emu::Obj>) {
        let Some(o) = obj else { return };
        if let Some((lane, epoch, seq)) = parse_slot(key) {
            if o.meta.get(proto::META_KIND).map(String::as_str) == Some(proto::KIND_DATA) {
                let content = o.meta.get(proto::META_CONTENT).cloned().unwrap_or_default();
                let _ = self.ever.borrow_mut().insert((lane, epoch, seq), content);
            }
            return;
        }
        let lprefix = format!("{}/", join(CTL, "lease"));
        let cprefix = format!("{}/", join(CTL, "ckpt"));
        if let Some(lane) = key.strip_prefix(&lprefix).and_then(|r| r.strip_suffix(".json")) {
            let Ok(doc) = serde_json::from_slice::<LeaseDoc>(&o.body) else { return self.violation(format!("lease {key} does not parse")) };
            if let Some((prev, at)) = self.leases.borrow().get(lane) {
                let age = now_ms() - at;
                if !prev.owner.is_empty() && !doc.owner.is_empty() && doc.owner != prev.owner && age < T.ttl_ms + T.margin_ms {
                    self.violation(format!("{} took {lane} from {} whose lease was written {age} ms ago", doc.owner, prev.owner));
                }
            }
            let _ = self.leases.borrow_mut().insert(lane.to_string(), (doc, now_ms()));
        } else if let Some(lane) = key.strip_prefix(&cprefix).and_then(|r| r.strip_suffix(".json")) {
            let Ok(doc) = serde_json::from_slice::<CkptDoc>(&o.body) else { return self.violation(format!("checkpoint {key} does not parse")) };
            let table = table_of(lane.rsplit('/').next().unwrap_or(lane));
            let rows_of = self.rows_of.borrow();
            for (e, pos) in &doc.epochs {
                for ((_, _, s), c) in self.ever.borrow().range((lane.to_string(), e.clone(), 0)..(lane.to_string(), e.clone(), pos.next)) {
                    let need = rows_of.get(&(table.clone(), c.clone())).copied().unwrap_or(0);
                    let have = self.count(&table, c);
                    if have < need {
                        self.violation(format!("neverSkipsCommitted: checkpoint v{} of {lane} moves {e} to {} past slot {s} ({c}): {have}/{need}", doc.version, pos.next));
                    }
                }
            }
        }
    }
}

struct S3F(Weak<World>);
impl Faults for S3F {
    fn decide(&self, op: &str, _key: &str) -> Fault {
        let Some(w) = self.0.upgrade() else { return Fault::None };
        let p = &w.p;
        if op.starts_with("PUT") {
            if op != "PUT" && w.fault(p.s3_err_after) {
                return Fault::ErrorAfter;
            }
            if w.fault(p.s3_drop_after) {
                return Fault::DropAfter;
            }
        }
        if w.fault(p.s3_drop_before) {
            return Fault::DropBefore;
        }
        if w.fault(p.s3_refuse) {
            return Fault::Refuse;
        }
        Fault::None
    }
    fn list_lag_ms(&self) -> u64 {
        self.0.upgrade().map_or(0, |w| w.p.list_lag_ms)
    }
}

struct ChF(Weak<World>);
impl ChFaults for ChF {
    fn insert(&self) -> InsertFault {
        let Some(w) = self.0.upgrade() else { return InsertFault::None };
        let p = &w.p;
        if w.fault(p.ch_settled) {
            InsertFault::SettledErr
        } else if w.fault(p.ch_lost) {
            InsertFault::Lost
        } else if w.fault(p.ch_late) {
            InsertFault::Late
        } else if w.fault(p.ch_timeout_commit) {
            InsertFault::TimeoutCommit
        } else {
            InsertFault::None
        }
    }
    fn check_err(&self) -> bool {
        self.0.upgrade().is_some_and(|w| w.fault(w.p.ch_check_err))
    }
    fn break_profile(&self) -> bool {
        self.0.upgrade().is_some_and(|w| w.fault(w.p.ch_break_profile))
    }
    fn exec_ms(&self) -> u64 {
        self.0.upgrade().map_or(5, |w| if w.sim.chance(0.05) { w.sim.range(300, 2_000) } else { w.sim.range(5, 200) })
    }
    fn between(&self, lo: u64, hi: u64) -> u64 {
        self.0.upgrade().map_or(hi, |w| w.sim.range(lo, hi.max(lo)))
    }
}

#[derive(Clone)]
struct NetClock {
    wall0: u64,
}
impl Clock for NetClock {
    fn mono(&self) -> u64 {
        1_000_000 + now_ms()
    }
    fn wall(&self) -> u64 {
        self.wall0 + now_ms()
    }
}

/// object_store's S3 client as `S3Config::build` configures it, onto turmoil.
fn s3_bucket(pause: Pause) -> S3Bucket {
    let s3 = AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_endpoint("http://s3:9000")
        .with_virtual_hosted_style_request(false)
        .with_access_key_id("otel")
        .with_secret_access_key("otelsecret")
        .with_client_options(ClientOptions::new().with_allow_http(true).with_timeout(S3_TIMEOUT))
        .with_retry(RetryConfig { max_retries: 2, retry_timeout: S3_TIMEOUT, ..Default::default() })
        .with_http_connector(TurmoilS3 { host: "s3".into(), port: 9000, timeout: S3_TIMEOUT, pause })
        .build()
        .expect("s3 client");
    S3Bucket::new(S3Store { store: Arc::new(s3), prefix: String::new(), endpoint: format!("http://s3:9000/{BUCKET}"), bucket: BUCKET.into() })
}

thread_local! {
    /// A deliberate bug in the workers (`coord::Mutation`), for `net_catches_mutants`.
    static MUTANT: Cell<Mutation> = const { Cell::new(Mutation::None) };
}

fn worker_cfg(name: &str) -> Config {
    let mut c = Config::new(ROOT, CTL, name);
    c.timing = T;
    c.timing.mutation = MUTANT.with(|m| m.get());
    c.discover_ms = 500;
    c.lanes_every_ms = 2_000;
    c.full_list_ms = 5_000;
    c.quiet_ms = 2_000;
    c.limits.max_objects = 4;
    c.backoff = Backoff::off();
    c.poll_ms = 200;
    c.balance.mode = BalanceMode::Count;
    c.horizon_ms = Some(86_400_000);
    c
}

async fn worker_host(w: Rc<World>, i: usize) {
    let inc = {
        let mut v = w.incarnations.borrow_mut();
        v[i] += 1;
        v[i]
    };
    let name = format!("w{i}-{inc}");
    trace(format!("START {name}"));
    let bucket = Rc::new(s3_bucket(w.pauses[i].clone()));
    let mut central = ClickHouseCentral::new("http://ch:8123", DB, bucket.clone(), "otel", "otelsecret", T.budget_ms + 2_000);
    let t = ch_transport("ch", 8123, Duration::from_millis(T.budget_ms + 2_000), w.pauses[i].clone());
    central.ch.transport = Some(t.clone());
    for r in central.replicas.iter_mut() {
        r.transport = Some(t.clone());
    }
    let mut wk = Worker::new(worker_cfg(&name), bucket, Rc::new(central), NetClock { wall0: w.sim.wall0_ms });
    loop {
        w.pauses[i].gate().await;
        let _ = wk.step().await;
        sleep_ms(200 + w.sim.range(0, 100)).await;
    }
}

async fn edge_host(w: Rc<World>, producer: usize) {
    let b = Rc::new(s3_bucket(Pause::default()));
    let mut lanes = Vec::new();
    for s in SIGNALS {
        let (w, b) = (w.clone(), b.clone());
        lanes.push(tokio::task::spawn_local(async move {
            let prefix = format!("{ROOT}/p{producer}/{s}");
            let table = table_of(s);
            let (mut n_ep, mut next) = (1u32, 0u64);
            let gap = w.sim.range(100, 1_500);
            while !w.stop.get() {
                sleep_ms(w.sim.range(gap / 2, gap * 3 / 2)).await;
                if w.stop.get() {
                    break;
                }
                w.n_content.set(w.n_content.get() + 1);
                let c = format!("c{}", w.n_content.get());
                let rows = w.sim.range(1, 9);
                let _ = w.rows_of.borrow_mut().insert((table.clone(), c.clone()), rows);
                let recv = (w.sim.wall0_ms + now_ms()) * 1_000_000;
                loop {
                    let epoch = format!("E{n_ep:04}");
                    let key = proto::slot_key(&prefix, &epoch, next);
                    let mut m = BTreeMap::new();
                    for (k, v) in [
                        (proto::META_KIND, proto::KIND_DATA.to_string()),
                        (proto::META_EPOCH, epoch.clone()),
                        (proto::META_SEQ, next.to_string()),
                        (proto::META_CONTENT, c.clone()),
                        (proto::META_ROWS, rows.to_string()),
                        (proto::META_RECEIVED, recv.to_string()),
                    ] {
                        let _ = m.insert(k.to_string(), v);
                    }
                    match b.put(&key, Bytes::from_static(&[0u8; 64]), Cond::Create, &m).await {
                        Put::Ok(_) => {
                            next += 1;
                            break;
                        }
                        _ => match b.head(&key).await {
                            Ok(Some(h)) => match proto::Slot::from_meta(&h) {
                                proto::Slot::Tomb => {
                                    n_ep += 1;
                                    next = 0;
                                }
                                proto::Slot::Data { epoch: e, content } if e == epoch && content == c => {
                                    next += 1;
                                    break;
                                }
                                _ => next += 1,
                            },
                            Ok(None) => {}
                            Err(_) => sleep_ms(200).await,
                        },
                    }
                }
            }
        }));
    }
    for l in lanes {
        let _ = l.await;
    }
}

async fn gc_host(w: Rc<World>) {
    let b = s3_bucket(Pause::default());
    let gcc = GcConfig { root: ROOT.into(), ctl: CTL.into(), delay_ms: T.ttl_ms + T.margin_ms + 4 * S3_TIMEOUT.as_millis() as u64, zombie_ms: 60_000, dry_run: false };
    loop {
        sleep_ms(w.sim.range(1_000, 4_000)).await;
        match gc_step(&b, &gcc, w.sim.wall0_ms + now_ms()).await {
            Ok(r) => trace(format!("GC deleted {} data, {} tombstones", r.deleted_data, r.deleted_tombstones)),
            Err(e) => trace(format!("GC error {e}")),
        }
    }
}

fn net_fleet(sim: Rc<Sim>) -> String {
    let p = NetProfile::draw(&sim);
    trace(format!("PROFILE {p:?}"));
    let wall0 = sim.wall0_ms;
    let skew = p.server_skew_ms;
    let world = Rc::new_cyclic(|weak: &Weak<World>| {
        let s3 = Rc::new(S3Emu::new(BUCKET, Box::new(move || wall0 + now_ms())));
        let s3w = Rc::downgrade(&s3);
        let objects = move |url: &str| -> Option<ObjInfo> {
            let s3 = s3w.upgrade()?;
            let objs = s3.objs.borrow();
            let o = objs.get(url_key(url)?)?;
            Some(ObjInfo {
                rows: o.meta.get(proto::META_ROWS)?.parse().ok()?,
                received_ns: o.meta.get(proto::META_RECEIVED).and_then(|v| v.parse().ok()).unwrap_or(0),
            })
        };
        let ch = Rc::new(ChEmu::new(Box::new(move || (wall0 as i64 + now_ms() as i64 + skew) as u64), Box::new(objects), T.slack_ms));
        *s3.faults.borrow_mut() = Rc::new(S3F(weak.clone()));
        *ch.faults.borrow_mut() = Rc::new(ChF(weak.clone()));
        *s3.log.borrow_mut() = Some(Box::new(|l: &str| trace(l)));
        *ch.log.borrow_mut() = Some(Box::new(|l: &str| trace(l)));
        let w1 = weak.clone();
        *s3.on_change.borrow_mut() = Some(Box::new(move |k: &str, o| {
            if let Some(w) = w1.upgrade() {
                w.changed(k, o)
            }
        }));
        let w2 = weak.clone();
        *ch.on_arrive.borrow_mut() = Some(Box::new(move |n: u64, urls: &[String], peer: &str| {
            let Some(w) = w2.upgrade() else { return };
            let mut lanes: Vec<(String, u64)> = Vec::new();
            for u in urls {
                if let Some((lane, _, _)) = url_key(u).and_then(parse_slot) {
                    if !lanes.iter().any(|(l, _)| *l == lane) {
                        // Inside its fence, a statement runs only under its sender's lease.
                        let doc = w.doc::<LeaseDoc>(&format!("{}.json", join(&join(CTL, "lease"), &lane)));
                        if !doc.as_ref().is_some_and(|d| d.owner.starts_with(&format!("{peer}-"))) {
                            w.violation(format!("statement #{n} from {peer} starts on {lane} under lease {:?}", doc.map(|d| (d.owner, d.epoch))));
                        }
                        let e = w.lease_epoch(&lane).unwrap_or(0);
                        lanes.push((lane, e));
                    }
                }
            }
            let _ = w.arrivals.borrow_mut().insert(n, lanes);
        }));
        let w3 = weak.clone();
        *ch.on_land.borrow_mut() = Some(Box::new(move |n: u64, table: &str, landed: &[(String, String, u64)]| {
            let Some(w) = w3.upgrade() else { return };
            let lanes = w.arrivals.borrow().get(&n).cloned().unwrap_or_default();
            for (lane, e) in lanes {
                let now = w.lease_epoch(&lane).unwrap_or(0);
                if now != e {
                    w.violation(format!("statement #{n} lands on {lane} after its lease epoch {e} changed hands (now {now})"));
                }
            }
            for (_, c, add) in landed {
                trace(format!("CH #{n} lands {table} {c} +{add}"));
            }
        }));
        World {
            sim: sim.clone(),
            p: p.clone(),
            s3,
            ch,
            ever: RefCell::new(BTreeMap::new()),
            rows_of: RefCell::new(BTreeMap::new()),
            leases: RefCell::new(BTreeMap::new()),
            arrivals: RefCell::new(BTreeMap::new()),
            violations: RefCell::new(Vec::new()),
            stop: Cell::new(false),
            healed: Cell::new(false),
            incarnations: RefCell::new(vec![0; p.workers]),
            pauses: (0..p.workers).map(|_| Pause::default()).collect(),
            n_content: Cell::new(0),
        }
    });
    let mut t = turmoil::Builder::new();
    t.simulation_duration(Duration::from_secs(3_600))
        .tick_duration(Duration::from_millis(1))
        .min_message_latency(Duration::from_millis(0))
        .max_message_latency(Duration::from_millis(p.lat_ms))
        .rng_seed(sim.seed)
        .enable_random_order();
    let mut ts = t.build();
    {
        let w = world.clone();
        ts.host("s3", move || {
            let w = w.clone();
            async move {
                let l = turmoil::net::TcpListener::bind(("0.0.0.0", 9000)).await?;
                loop {
                    let (s, _) = l.accept().await?;
                    std::mem::drop(tokio::task::spawn_local(serve_s3(w.s3.clone(), s)));
                }
            }
        });
    }
    {
        let w = world.clone();
        ts.host("ch", move || {
            let w = w.clone();
            async move {
                let l = turmoil::net::TcpListener::bind(("0.0.0.0", 8123)).await?;
                loop {
                    let (s, addr) = l.accept().await?;
                    let peer = turmoil::reverse_lookup(addr.ip()).unwrap_or_default();
                    std::mem::drop(tokio::task::spawn_local(serve_ch(w.ch.clone(), s, peer)));
                }
            }
        });
    }
    for pi in 0..p.producers {
        let w = world.clone();
        ts.host(format!("edge{pi}"), move || {
            let w = w.clone();
            async move {
                edge_host(w, pi).await;
                futures::future::pending::<()>().await;
                Ok(())
            }
        });
    }
    for i in 0..p.workers {
        let w = world.clone();
        ts.host(format!("w{i}"), move || {
            let w = w.clone();
            async move {
                worker_host(w, i).await;
                Ok(())
            }
        });
    }
    {
        let w = world.clone();
        ts.host("gc", move || {
            let w = w.clone();
            async move {
                gc_host(w).await;
                Ok(())
            }
        });
    }
    // The driver: chaos while the edges write, then heal and wait.
    let mut next_chaos = 1_000;
    let mut undo: Vec<(u64, String, &'static str)> = Vec::new();
    let mut quiesce_at: Option<u64> = None;
    let mut progress = (0u64, 0u64);
    let complete = |w: &World| w.rows_of.borrow().iter().all(|((t, c), r)| w.count(t, c) >= *r);
    loop {
        if let Err(e) = ts.step() {
            panic!("turmoil: {e}");
        }
        let now = ts.elapsed().as_millis() as u64;
        sim::set_turmoil_now(now);
        undo.retain(|(at, host, what)| {
            if now < *at {
                return true;
            }
            match *what {
                "repair" => {
                    ts.repair(host.as_str(), "s3");
                    ts.repair(host.as_str(), "ch");
                }
                "release" => {
                    ts.release(host.as_str(), "s3");
                    ts.release(host.as_str(), "ch");
                }
                _ => ts.bounce(host.as_str()),
            }
            trace(format!("HEAL {what} {host}"));
            false
        });
        if now >= next_chaos && now < p.run_ms {
            next_chaos += 1_000;
            for i in 0..p.workers {
                let h = format!("w{i}");
                if undo.iter().any(|(_, x, _)| *x == h) {
                    continue;
                }
                if sim.chance(p.partition_rate) {
                    ts.partition(h.as_str(), "s3");
                    ts.partition(h.as_str(), "ch");
                    let d = sim.range(500, T.ttl_ms + 2_000);
                    trace(format!("PARTITION {h} for {d} ms"));
                    undo.push((now + d, h, "repair"));
                } else if sim.chance(p.hold_rate) {
                    ts.hold(h.as_str(), "s3");
                    ts.hold(h.as_str(), "ch");
                    let d = sim.range(500, T.ttl_ms + 2_000);
                    trace(format!("HOLD {h} for {d} ms"));
                    undo.push((now + d, h, "release"));
                } else if sim.chance(p.pause_rate) {
                    let d = sim.range(500, T.ttl_ms + 3 * T.margin_ms + 2_000);
                    world.pauses[i].until(now + d);
                    trace(format!("PAUSE {h} for {d} ms"));
                } else if sim.chance(p.bounce_rate) {
                    ts.crash(h.as_str());
                    let d = sim.range(0, 3_000);
                    trace(format!("CRASH {h}, restart in {d} ms"));
                    undo.push((now + d, h, "bounce"));
                }
            }
        }
        if now >= p.run_ms && !world.stop.get() {
            world.stop.set(true);
            trace("STOP edges");
        }
        if now >= p.run_ms + 5_000 && quiesce_at.is_none() && undo.is_empty() {
            world.healed.set(true);
            for pz in &world.pauses {
                pz.until(0);
            }
            quiesce_at = Some(now);
            trace("QUIESCE (healed)");
        }
        if let Some(q) = quiesce_at {
            if now % 1_000 == 0 {
                // Liveness as in dst_consumer: done, or a minute without progress.
                let total: u64 = world.ch.tables.borrow().values().map(|t| t.rows.values().sum::<u64>()).sum();
                if total != progress.0 {
                    progress = (total, now);
                }
                // Late commits can still land: run on past the settle bound.
                let settled = now > q + T.ttl_ms + T.slack_ms + T.margin_ms;
                if (complete(&world) && settled) || now > progress.1 + 60_000 || now > q + 3_600_000 {
                    break;
                }
            }
        }
    }
    let quiesce_ms = now_ms() - quiesce_at.unwrap_or(0);
    drop(ts);
    let rows_of = world.rows_of.borrow().clone();
    let (mut missing, mut dup) = (Vec::new(), Vec::new());
    for ((t, c), r) in &rows_of {
        let have = world.count(t, c);
        if have < *r {
            missing.push(format!("{t}/{c} {have}/{r}"));
        } else if have > *r {
            dup.push(format!("{t}/{c} {have}/{r}"));
        }
    }
    let extra: Vec<String> = {
        let tables = world.ch.tables.borrow();
        tables
            .iter()
            .flat_map(|(t, tb)| tb.rows.keys().map(move |c| (t.clone(), c.clone())))
            .filter(|(t, c)| !rows_of.contains_key(&(t.trim_start_matches(&format!("{DB}.")).to_string(), c.clone())))
            .map(|(t, c)| format!("{t}/{c}"))
            .collect()
    };
    let v = world.violations.borrow().clone();
    let summary = format!(
        "workers {} producers {} objects {} contents {} statements fenced {} deduplicated {} quiesce {quiesce_ms} ms",
        p.workers,
        p.producers,
        world.ever.borrow().len(),
        rows_of.len(),
        world.ch.fenced.get(),
        world.ch.deduplicated.get()
    );
    trace(format!("END {summary}"));
    assert!(v.is_empty(), "invariant violations: {v:#?}\n{summary}");
    assert!(dup.is_empty(), "atMostOnce: {dup:?}\n{summary}");
    assert!(extra.is_empty(), "onlyCommittedIngested: {extra:?}\n{summary}");
    assert!(missing.is_empty(), "not ingested, no progress for 60 s after {quiesce_ms} ms healed: {missing:?}\n{summary}");
    summary
}

fn wall0(seed: u64) -> u64 {
    (20_000 + seed % 7) * 86_400_000 + 3_600_000
}

#[test]
fn net_seeds() {
    let seeds = sim::seeds(8);
    let t0 = std::time::Instant::now();
    let out = sim::sweep("net_seeds", "dst_net", &seeds, |seed, keep| sim::run_turmoil(seed, wall0(seed), keep, net_fleet));
    eprintln!("DST net: {} seeds passed, {:.1} s", out.len(), t0.elapsed().as_secs_f64());
}

/// The network-level harness finds the model's mutants too.
#[test]
fn net_catches_mutants() {
    let n = std::env::var("DST_MUTANT_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(40u64);
    // (no_time_bound needs a pause to hit an insert between its window check
    // and its send; level 1 finds it, this level didn't in 250 seeds.)
    for (name, m) in [("no_verify", Mutation::NoVerify), ("release_in_flight", Mutation::ReleaseInFlight), ("error_settles", Mutation::ErrorSettles)] {
        let caught = (1..=n).find_map(|seed| {
            let o = sim::run_turmoil(seed, wall0(seed), false, move |sim| {
                MUTANT.with(|c| c.set(m));
                net_fleet(sim)
            });
            o.failure.map(|f| (seed, f))
        });
        match caught {
            Some((seed, f)) => {
                let first = f.lines().find(|l| l.contains("t=") || l.contains(':')).unwrap_or(&f).trim().to_string();
                eprintln!("net mutant {name}: caught by seed {seed}: {}", &first[..first.len().min(200)]);
            }
            None => panic!("net mutant {name} survived {n} seeds"),
        }
    }
}

#[test]
fn net_is_deterministic() {
    let n = std::env::var("DST_META_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(2u64);
    for seed in 500..500 + n {
        let a = sim::run_turmoil(seed, wall0(seed), true, net_fleet);
        let b = sim::run_turmoil(seed, wall0(seed), true, net_fleet);
        assert!(a.lines > 500, "seed {seed}: {} lines ({:?})", a.lines, a.failure);
        if a.trace != b.trace {
            let dir = sim::trace_dir();
            let _ = std::fs::write(dir.join(format!("dst-net-meta-{seed}-a.trace")), &a.trace);
            let _ = std::fs::write(dir.join(format!("dst-net-meta-{seed}-b.trace")), &b.trace);
            let first = a.trace.lines().zip(b.trace.lines()).position(|(x, y)| x != y);
            panic!("seed {seed}: two runs differ (first at line {first:?}), traces in {}", dir.display());
        }
        eprintln!("DST net meta seed {seed}: identical, {} lines ({})", a.lines, a.failure.as_deref().unwrap_or("passed"));
    }
}

// ---- the emulators against the real services ------------------------------------------

fn seaweed() -> String {
    std::env::var("DST_S3").unwrap_or_else(|_| "http://127.0.0.1:18333/otel".into())
}

fn real_bucket(url: &str) -> Rc<S3Bucket> {
    let store = otap_s3pq::store::S3Config {
        url: url.into(),
        access_key_id: Some("otel".into()),
        secret_access_key: Some("otelsecret".into()),
        ..Default::default()
    }
    .build()
    .unwrap();
    Rc::new(S3Bucket::new(store))
}

/// Serves an emulator on a real localhost socket; returns its port.
async fn serve_local<F, Fut>(serve: F) -> u16
where
    F: Fn(tokio::net::TcpStream) -> Fut + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    std::mem::drop(tokio::task::spawn_local(async move {
        while let Ok((s, _)) = l.accept().await {
            std::mem::drop(tokio::task::spawn_local(serve(s)));
        }
    }));
    port
}

/// The consumer's S3 operations, one scripted sequence, normalized
/// (ETag values replaced by whether they changed).
async fn s3_script(b: &S3Bucket, root: &str) -> Vec<String> {
    let mut out = Vec::new();
    let k = |s: &str| format!("{root}/{s}");
    let meta: BTreeMap<String, String> = [("oscope-kind", "data"), ("oscope-content", "c1"), ("oscope-rows", "3")].iter().map(|(a, b)| (a.to_string(), b.to_string())).collect();
    let strip = |v: Vec<String>| v.into_iter().map(|x| x.trim_start_matches(&format!("{root}/")).to_string()).collect::<Vec<_>>();
    let put = |r: Put| match r {
        Put::Ok(e) => format!("ok etag:{}", !e.is_empty()),
        other => format!("{other:?}"),
    };
    out.push(format!("create a: {}", put(b.put(&k("l/E1/a"), Bytes::from("x"), Cond::Create, &meta).await)));
    out.push(format!("create a again: {}", put(b.put(&k("l/E1/a"), Bytes::from("y"), Cond::Create, &meta).await)));
    let h = b.head(&k("l/E1/a")).await.unwrap().map(|m| m.into_iter().collect::<BTreeMap<_, _>>());
    out.push(format!("head a: {h:?}"));
    let (body, e1) = b.get(&k("l/E1/a")).await.unwrap().unwrap();
    out.push(format!("get a: {body:?}"));
    let r = b.put(&k("l/E1/a"), Bytes::from("z"), Cond::IfMatch(&e1), &BTreeMap::new()).await;
    let e2 = match &r {
        Put::Ok(e) => e.clone(),
        _ => String::new(),
    };
    out.push(format!("cas a: {} new etag {}", put(r), e2 != e1));
    out.push(format!("cas a stale: {}", put(b.put(&k("l/E1/a"), Bytes::from("w"), Cond::IfMatch(&e1), &BTreeMap::new()).await)));
    out.push(format!("cas missing: {}", put(b.put(&k("l/E1/missing"), Bytes::from("w"), Cond::IfMatch(&e1), &BTreeMap::new()).await)));
    out.push(format!("get a after cas: {:?}", b.get(&k("l/E1/a")).await.unwrap().map(|x| x.0)));
    out.push(format!("head a after cas: {:?}", b.head(&k("l/E1/a")).await.unwrap().map(|m| m.len())));
    out.push(format!("put plain c: {}", put(b.put(&k("l/c"), Bytes::from("c"), Cond::None, &BTreeMap::new()).await)));
    for (e, s) in [("E1", 0), ("E1", 1), ("E1", 2), ("E2", 0), ("E10", 0)] {
        let _ = b.put(&proto::slot_key(&k("m/traces"), e, s), Bytes::from("s"), Cond::Create, &meta).await;
    }
    out.push(format!("list all: {:?}", strip(b.list(root, None).await.unwrap().into_iter().map(|i| i.key).collect())));
    out.push(format!("list sizes: {:?}", b.list(root, None).await.unwrap().into_iter().map(|i| (i.size, i.etag.is_some())).collect::<Vec<_>>()));
    let sa = proto::slot_key(&k("m/traces"), "E1", 0);
    out.push(format!("list after slot: {:?}", strip(b.list(&k("m/traces"), Some(&sa)).await.unwrap().into_iter().map(|i| i.key).collect())));
    let sa0 = format!("{}/0", k("m/traces/E1"));
    out.push(format!("list after epoch/0: {:?}", strip(b.list(&k("m/traces"), Some(&sa0)).await.unwrap().into_iter().map(|i| i.key).collect())));
    out.push(format!("list after floor/~: {:?}", strip(b.list(&k("m/traces"), Some(&format!("{}/~", k("m/traces/E1")))).await.unwrap().into_iter().map(|i| i.key).collect())));
    out.push(format!("dirs root: {:?}", b.list_dirs(root).await.unwrap()));
    out.push(format!("dirs m: {:?}", b.list_dirs(&k("m")).await.unwrap()));
    out.push(format!("head missing: {:?}", b.head(&k("nope")).await.unwrap()));
    out.push(format!("get missing: {:?}", b.get(&k("nope")).await.unwrap()));
    let all: Vec<String> = b.list(root, None).await.unwrap().into_iter().map(|i| i.key).collect();
    let mut del = all.clone();
    del.push(k("never-existed"));
    out.push(format!("delete: {:?}", b.delete(&del).await));
    out.push(format!("list after delete: {:?}", b.list(root, None).await.unwrap().len()));
    out
}

/// The S3 emulator against SeaweedFS, through object_store and reqwest.
#[test]
fn s3_emulator_matches_seaweedfs() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    tokio::task::LocalSet::new().block_on(&rt, async {
        let base = seaweed();
        let real = real_bucket(&format!("{base}/dst-diff"));
        let nonce = format!("{:08x}", rand::random::<u32>());
        let root = format!("dst-diff/{nonce}");
        if real.list("dst-diff", None).await.is_err() {
            eprintln!("no SeaweedFS at {base}: skipped");
            return;
        }
        let want = s3_script(&real, &root).await;
        let bucket = base.rsplit('/').next().unwrap().to_string();
        let emu = Rc::new(S3Emu::new(&bucket, Box::new(|| 1_790_000_000_000)));
        let port = serve_local(move |s| serve_s3(emu.clone(), s)).await;
        let fake = real_bucket(&format!("http://127.0.0.1:{port}/{bucket}/dst-diff"));
        let got = s3_script(&fake, &root).await;
        for (w, g) in want.iter().zip(got.iter()) {
            eprintln!("seaweedfs: {w}\nemulator:  {g}");
        }
        assert_eq!(want, got, "the S3 emulator differs from SeaweedFS");
    });
}

/// The consumer's ClickHouse statements, one scripted sequence.
async fn ch_script(c: &ClickHouseCentral<S3Bucket>, objs: &[Obj]) -> Vec<String> {
    let mut out = Vec::new();
    let lk = LaneKind::for_signal("logs").unwrap();
    let wall = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let fence = |d: i64| Fence { wall_ms: (wall() as i64 + d) as u64, budget_ms: 10_000 };
    let refs: Vec<&Obj> = objs.iter().collect();
    let keys: Vec<&str> = objs.iter().map(|o| o.content.as_str()).collect();
    let counts = |r: Result<std::collections::HashMap<String, u64>, String>| format!("{:?}", r.map(|m| m.into_iter().collect::<BTreeMap<_, _>>()));
    out.push(format!("ensure: {:?}", c.ensure(&lk).await));
    out.push(format!("ensure again: {:?}", c.ensure(&lk).await));
    out.push(format!("ranged: {}", c.ranged(&lk)));
    out.push(format!("counts empty: {}", counts(c.counts(&lk, &keys, None).await)));
    out.push(format!("insert: {:?}", c.insert(&lk, &refs, fence(60_000), "dst-t1", true).await.map_err(|e| e.msg)));
    out.push(format!("counts: {}", counts(c.counts(&lk, &keys, None).await)));
    out.push(format!("retry same token: {:?}", c.insert(&lk, &refs, fence(60_000), "dst-t1", true).await.map_err(|e| e.msg)));
    out.push(format!("counts after retry: {}", counts(c.counts(&lk, &keys, None).await)));
    out.push(format!("insert fenced: {:?}", c.insert(&lk, &refs[..1], fence(-60_000), "dst-t2", true).await.map_err(|e| e.msg)));
    out.push(format!("counts after fenced: {}", counts(c.counts(&lk, &keys, None).await)));
    let day = objs[0].received_ns;
    let r = consumer::plan::CheckRange { lo_ns: day, hi_ns: day };
    out.push(format!("counts in range: {}", counts(c.counts(&lk, &keys, Some(r)).await)));
    let r2 = consumer::plan::CheckRange { lo_ns: day + 3 * 86_400_000_000_000, hi_ns: day + 4 * 86_400_000_000_000 };
    out.push(format!("counts other days: {}", counts(c.counts(&lk, &keys, Some(r2)).await)));
    out.push(format!("repair present: {:?}", c.repair(&lk, &objs[1], fence(60_000), "dst-r1").await.map_err(|e| e.msg)));
    out.push(format!("counts after repair: {}", counts(c.counts(&lk, &keys, None).await)));
    out.push(format!("insert new token: {:?}", c.insert(&lk, &refs[..1], fence(60_000), "dst-t3", true).await.map_err(|e| e.msg)));
    out.push(format!("counts after new token: {}", counts(c.counts(&lk, &keys, None).await)));
    out
}

/// The ClickHouse emulator against a real ClickHouse (reading Parquet
/// objects the test writes to SeaweedFS), both through `ClickHouseCentral`.
#[test]
fn ch_emulator_matches_clickhouse() {
    use consumer::audit::tests::otlp_logs;
    use otap_s3pq::batch::{Encoder, Format, Input};
    use otap_s3pq::encode::ParquetOptions;
    use otap_s3pq::flatten::Envelope;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    tokio::task::LocalSet::new().block_on(&rt, async {
        let ch_url = std::env::var("DST_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
        let ch = otap_s3pq::central::ClickHouse::new(&ch_url);
        let base = seaweed();
        let nonce = format!("{:08x}", rand::random::<u32>());
        let bucket = real_bucket(&format!("{base}/dst-diff"));
        if ch.query("SELECT 1", &[]).await.is_err() || bucket.list("dst-diff", None).await.is_err() {
            eprintln!("no ClickHouse at {ch_url} or no SeaweedFS at {base}: skipped");
            return;
        }
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        let lane = format!("dst-diff/{nonce}/p1/logs");
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let mut objs = Vec::new();
        let mut infos = BTreeMap::new();
        for (seq, (n, tag)) in [(7usize, "a"), (5, "b")].into_iter().enumerate() {
            let f = enc.flatten(&Input::Otlp(otap_s3pq::Signal::Logs, &otlp_logs(n, tag))).unwrap();
            let o = enc.encode(&f, &Envelope { producer: "p1".into(), epoch: "E1".into(), batch: seq as u64, received_ns: now }).unwrap();
            let key = proto::slot_key(&lane, "E1", seq as u64);
            assert!(matches!(bucket.put(&key, o.body, Cond::Create, &o.meta).await, Put::Ok(_)));
            let _ = infos.insert(bucket.object_url(&key), ObjInfo { rows: n as u64, received_ns: now });
            objs.push(Obj { lane: lane.clone(), epoch: "E1".into(), seq: seq as u64, key, size: 1, content: f.content.clone(), rows: n as u64, received_ns: now, seen_ms: 0 });
        }
        let db = format!("dst_diff_{nonce}");
        let real = ClickHouseCentral::new(&ch_url, &db, bucket.clone(), "otel", "otelsecret", 20_000);
        let want = ch_script(&real, &objs).await;
        let _ = ch.query(&format!("DROP DATABASE {db} SYNC"), &[]).await;
        let emu = Rc::new(ChEmu::new(
            Box::new(|| std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64),
            Box::new(move |u: &str| infos.get(u).copied()),
            0,
        ));
        let port = serve_local(move |s| serve_ch(emu.clone(), s, "local".into())).await;
        let fake = ClickHouseCentral::new(&format!("http://127.0.0.1:{port}"), &db, bucket.clone(), "otel", "otelsecret", 20_000);
        let got = ch_script(&fake, &objs).await;
        let keys: Vec<String> = bucket.list(&format!("dst-diff/{nonce}"), None).await.unwrap().into_iter().map(|i| i.key).collect();
        let _ = bucket.delete(&keys).await;
        for (w, g) in want.iter().zip(got.iter()) {
            eprintln!("clickhouse: {w}\nemulator:   {g}");
        }
        assert_eq!(want, got, "the ClickHouse emulator differs from ClickHouse");
    });
}
