//! Concurrent stateful testing with Hegel (HEGEL.md): real threads racing
//! the conditional writes the protocol rests on, through the production S3
//! client (`consumer::bucket::S3Bucket` over object_store) against the S3
//! emulator served on a real socket (or SeaweedFS: `HEGEL_RACE_S3`).
//!
//! - Create-only slots (`If-None-Match: *`, the exporter's slot writes and
//!   the consumer's first lease / checkpoint): of the writers racing for a
//!   key exactly one wins; every loser gets 412, and reading the key back
//!   learns the winner's body, never its own (CAST #15: a 412 is not proof
//!   that someone else wrote).
//! - Compare-and-swap (`If-Match`, lease renewal and takeover): of the
//!   writers holding the same ETag at most one wins; losers read back
//!   someone else's body.
//! - A created object never changes: every read of it sees the winner's body.
//!
//! Hegel's `#[hegel::concurrent_state_machine]` runs rounds of rules on up
//! to four worker threads and checks the invariants at the join points.
//! Unlike tests/hegel_dst.rs this is NOT deterministic (the OS schedules the
//! threads), so a failure may not replay; it runs with a small budget and
//! not on every PR (HEGEL.md, ci/README.md).
//!
//!   cargo test --release --test hegel_race -- race_conditional_writes
//!   HEGEL_RACE_S3=http://127.0.0.1:18333/otel cargo test --release --test hegel_race -- race_conditional_writes  # SeaweedFS
//!   HEGEL_RACE_MUTANTS=1 cargo test --release --test hegel_race -- race_finds_a_racy_store --nocapture
//!
//! The mutant (`racy_conditional`, test-only, in this file's server): the
//! emulator evaluates a precondition, yields, then writes unconditionally,
//! as a store without atomic conditional writes would. The machine must
//! find the two winners or the lost update.

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
mod consumer;
mod dst;

use bytes::Bytes;
use consumer::bucket::{Bucket, Cond, Put, S3Bucket};
use dst::s3emu::{Answer, S3Emu};
use hegel::generators as gs;
use hegel::{HealthCheck, Settings, TestCase};
use http::{Method, Request};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

const BUCKET: &str = "otel";
const SLOTS: usize = 3;
const LEASES: usize = 2;

// ---- the store under test ------------------------------------------------------------------

/// An emulator on a real socket, on a thread of its own, for the whole
/// process; returns its base URL. `racy`: the mutant's (conditional PUTs
/// checked, then applied later).
fn emulator(racy: bool) -> &'static str {
    static URL: OnceLock<String> = OnceLock::new();
    static RACY_URL: OnceLock<String> = OnceLock::new();
    (if racy { &RACY_URL } else { &URL }).get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("s3emu".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    let clock = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                    let emu = Rc::new(S3Emu::new(BUCKET, Box::new(clock)));
                    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
                    tx.send(l.local_addr().expect("addr").port()).expect("port");
                    while let Ok((s, _)) = l.accept().await {
                        std::mem::drop(tokio::task::spawn_local(serve(emu.clone(), s, racy)));
                    }
                });
            })
            .expect("spawn");
        format!("http://127.0.0.1:{}/{BUCKET}", rx.recv().expect("emulator port"))
    })
}

/// `net::serve_s3`, plus the racy mutant: a conditional PUT's precondition
/// is evaluated (by a HEAD), then, after a yield in which other requests
/// run, the PUT is applied without it.
async fn serve(emu: Rc<S3Emu>, io: tokio::net::TcpStream, racy: bool) {
    let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
        let emu = emu.clone();
        async move {
            let (mut parts, body) = req.into_parts();
            let body = body.collect().await.map(|c| c.to_bytes()).unwrap_or_default();
            let conditional = parts.headers.contains_key("if-none-match") || parts.headers.contains_key("if-match");
            if racy && parts.method == Method::PUT && conditional {
                let head = Request::builder().method(Method::HEAD).uri(parts.uri.clone()).body(Bytes::new()).expect("head");
                let (exists, etag) = match emu.handle(head) {
                    Answer::Http(r) => (r.status().is_success(), r.headers().get("etag").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()),
                    Answer::Drop => (false, String::new()),
                };
                let holds = match (parts.headers.get("if-none-match"), parts.headers.get("if-match")) {
                    (Some(_), _) => !exists,
                    (_, Some(m)) => exists && m.to_str().unwrap_or("") == etag,
                    _ => true,
                };
                if holds {
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    parts.headers.remove("if-none-match");
                    parts.headers.remove("if-match");
                }
            }
            match emu.handle(Request::from_parts(parts, body)) {
                Answer::Http(r) => Ok::<_, std::io::Error>(r.map(Full::new)),
                Answer::Drop => Err(std::io::Error::other("connection dropped")),
            }
        }
    });
    let _ = hyper::server::conn::http1::Builder::new().timer(TokioTimer::new()).serve_connection(TokioIo::new(io), svc).await;
}

fn base_url(racy: bool) -> String {
    if racy {
        return emulator(true).to_string();
    }
    std::env::var("HEGEL_RACE_S3").unwrap_or_else(|_| emulator(false).to_string())
}

type Client = Rc<(tokio::runtime::Runtime, S3Bucket)>;

thread_local! {
    /// Each thread's own clients and runtime (a real process per writer).
    static CLIENTS: std::cell::RefCell<HashMap<String, Client>> = std::cell::RefCell::new(HashMap::new());
}

fn client(url: &str) -> Client {
    CLIENTS.with(|c| {
        c.borrow_mut()
            .entry(url.to_string())
            .or_insert_with(|| {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
                let store = otap_s3pq::store::S3Config {
                    url: url.into(),
                    access_key_id: Some("otel".into()),
                    secret_access_key: Some("otelsecret".into()),
                    ..Default::default()
                }
                .build()
                .expect("s3 config");
                Rc::new((rt, S3Bucket::new(store)))
            })
            .clone()
    })
}

fn put(url: &str, key: &str, body: &str, cond: Cond<'_>) -> Put {
    let c = client(url);
    c.0.block_on(c.1.put(key, Bytes::from(body.to_string()), cond, &BTreeMap::new()))
}

fn get(url: &str, key: &str) -> Option<(String, String)> {
    let c = client(url);
    c.0.block_on(c.1.get(key)).expect("GET").map(|(b, e)| (String::from_utf8_lossy(&b).into_owned(), e))
}

// ---- the machine ----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Won,
    Lost,
    NoAnswer,
}

fn outcome(p: &Put) -> Outcome {
    match p {
        Put::Ok(_) => Outcome::Won,
        Put::Conflict => Outcome::Lost,
        Put::Unknown(_) => Outcome::NoAnswer,
    }
}

/// One conditional write: who, on what (and for a CAS, which ETag it held),
/// how it ended, and what the writer read back after losing.
#[derive(Debug, Clone)]
struct Attempt {
    key: String,
    held: Option<String>,
    token: String,
    outcome: Outcome,
    read_back: Option<String>,
}

struct Race {
    url: String,
    prefix: String,
    creates: Mutex<Vec<Attempt>>,
    cases: Mutex<Vec<Attempt>>,
    reads: Mutex<Vec<(String, String)>>,
    /// Each lease's first body (written before the rounds).
    initial: HashMap<String, String>,
}

static TOKENS: AtomicU64 = AtomicU64::new(0);

fn token(tag: &str) -> String {
    format!("{tag}-{:?}-{}", std::thread::current().id(), TOKENS.fetch_add(1, Ordering::Relaxed))
}

impl Race {
    fn slot(&self, tc: &TestCase) -> String {
        format!("{}/slot-{}", self.prefix, tc.draw(gs::integers::<usize>().max_value(SLOTS - 1)))
    }
    fn lease(&self, tc: &TestCase) -> String {
        format!("{}/lease-{}", self.prefix, tc.draw(gs::integers::<usize>().max_value(LEASES - 1)))
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[hegel::concurrent_state_machine]
impl Race {
    /// A writer creates a slot (`If-None-Match: *`); on 412 it reads the
    /// slot back to learn who holds it.
    #[rule(group = "s3")]
    fn create(&self, tc: TestCase) {
        let key = self.slot(&tc);
        let t = token("c");
        let p = put(&self.url, &key, &t, Cond::Create);
        let o = outcome(&p);
        let read_back = (o == Outcome::Lost).then(|| get(&self.url, &key).map(|(b, _)| b).unwrap_or_default());
        tc.note(&format!("create {key} as {t}: {p:?}, read back {read_back:?}"));
        lock(&self.creates).push(Attempt { key, held: None, token: t, outcome: o, read_back });
    }

    /// A writer reads a lease and replaces it (`If-Match`: renewal or
    /// takeover); on 412 it reads back who wrote it.
    #[rule(group = "s3", weight = 2.0)]
    fn cas(&self, tc: TestCase) {
        let key = self.lease(&tc);
        let Some((_, etag)) = get(&self.url, &key) else { panic!("lease {key} is gone") };
        let t = token("l");
        let p = put(&self.url, &key, &t, Cond::IfMatch(&etag));
        let o = outcome(&p);
        let read_back = (o == Outcome::Lost).then(|| get(&self.url, &key).map(|(b, _)| b).unwrap_or_default());
        tc.note(&format!("cas {key} held {etag} as {t}: {p:?}, read back {read_back:?}"));
        lock(&self.cases).push(Attempt { key, held: Some(etag), token: t, outcome: o, read_back });
    }

    /// A reader reads a slot.
    #[rule(group = "s3")]
    fn read(&self, tc: TestCase) {
        let key = self.slot(&tc);
        if let Some((b, _)) = get(&self.url, &key) {
            lock(&self.reads).push((key, b));
        }
    }

    /// Exactly one winner per slot; losers learn it; readers see only it.
    #[invariant(always_run)]
    fn one_winner_per_slot(&self, _tc: TestCase) {
        let creates = lock(&self.creates).clone();
        let reads = lock(&self.reads).clone();
        let mut keys: Vec<&String> = creates.iter().map(|a| &a.key).collect();
        keys.sort();
        keys.dedup();
        for key in keys {
            let tries: Vec<&Attempt> = creates.iter().filter(|a| &a.key == key).collect();
            let won: Vec<&Attempt> = tries.iter().copied().filter(|a| a.outcome == Outcome::Won).collect();
            assert!(won.len() <= 1, "{key}: {} writers won a create-only PUT: {won:?}", won.len());
            let stored = get(&self.url, key).map(|(b, _)| b);
            if let Some(w) = won.first() {
                assert_eq!(stored.as_deref(), Some(w.token.as_str()), "{key}: the winner's body is not what is stored");
            }
            for a in tries.iter().filter(|a| a.outcome == Outcome::Lost) {
                let seen = a.read_back.as_deref().unwrap_or("");
                assert_ne!(seen, a.token, "{key}: {} got 412 but its own body is stored (a 412 that was ours)", a.token);
                assert_eq!(Some(seen), stored.as_deref(), "{key}: loser {} read back {seen:?}, not the winner", a.token);
            }
            for (_, b) in reads.iter().filter(|(k, _)| k == key) {
                assert_eq!(Some(b.as_str()), stored.as_deref(), "{key}: a create-only object changed");
            }
        }
    }

    /// At most one winner per (lease, ETag held); the lease is the last
    /// winner's (or still the initial body).
    #[invariant(always_run)]
    fn one_winner_per_etag(&self, _tc: TestCase) {
        let cases = lock(&self.cases).clone();
        let mut won: BTreeMap<(String, String), Vec<&Attempt>> = BTreeMap::new();
        for a in cases.iter().filter(|a| a.outcome == Outcome::Won) {
            won.entry((a.key.clone(), a.held.clone().unwrap_or_default())).or_default().push(a);
        }
        for ((key, etag), w) in &won {
            assert!(w.len() == 1, "{key}: {} writers won If-Match {etag}: {w:?} (a lost update)", w.len());
        }
        for a in cases.iter().filter(|a| a.outcome == Outcome::Lost) {
            assert_ne!(a.read_back.as_deref(), Some(a.token.as_str()), "{}: {} got 412 but its own body is stored", a.key, a.token);
        }
        for (key, first) in &self.initial {
            let stored = get(&self.url, key).map(|(b, _)| b).unwrap_or_default();
            let known = &stored == first || cases.iter().any(|a| &a.key == key && a.outcome != Outcome::Lost && a.token == stored);
            assert!(known, "{key}: stores {stored:?}, which no successful writer wrote");
        }
    }
}

static CASE: AtomicU64 = AtomicU64::new(0);

fn run_case(tc: TestCase, racy: bool) {
    let url = base_url(racy);
    let run = std::env::var("HEGEL_RACE_RUN").unwrap_or_else(|_| format!("{}", std::process::id()));
    let prefix = format!("hegel-race/{run}/{}", CASE.fetch_add(1, Ordering::Relaxed));
    let mut initial = HashMap::new();
    for i in 0..LEASES {
        let key = format!("{prefix}/lease-{i}");
        let t = token("init");
        assert!(matches!(put(&url, &key, &t, Cond::Create), Put::Ok(_)), "{key}: the initial lease");
        let _ = initial.insert(key, t);
    }
    let m = Race { url, prefix, creates: Mutex::new(Vec::new()), cases: Mutex::new(Vec::new()), reads: Mutex::new(Vec::new()), initial };
    hegel::stateful::machine(m).steps(8).min_concurrency(2).max_concurrency(4).run_concurrent(tc);
}

fn race_settings() -> Settings {
    Settings::new().suppress_health_check([HealthCheck::TooSlow])
}

/// The store's conditional writes under real thread races.
#[hegel::test(race_settings())]
fn race_conditional_writes(tc: TestCase) {
    run_case(tc, false);
}

/// The racy store (check, then write) must be caught. Opt-in:
/// `HEGEL_RACE_MUTANTS=1`; `HEGEL_RACE_MUTANT_CASES` bounds the search
/// (default 300). Emulator only.
#[test]
fn race_finds_a_racy_store() {
    if std::env::var_os("HEGEL_RACE_MUTANTS").is_none() || std::env::var_os("HEGEL_RACE_S3").is_some() {
        eprintln!("HEGEL_RACE_MUTANTS unset (or HEGEL_RACE_S3 set): skipped");
        return;
    }
    let budget: u64 = std::env::var("HEGEL_RACE_MUTANT_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
    let t0 = std::time::Instant::now();
    let n0 = CASE.load(Ordering::Relaxed);
    let settings = race_settings().test_cases(budget).database(None).print_blob(false).verbosity(hegel::Verbosity::Quiet);
    let r = std::panic::catch_unwind(|| hegel::Hegel::new(|tc| run_case(tc, true)).settings(settings).run());
    let msg = r.as_ref().err().map(|p| p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default()).unwrap_or_default();
    eprintln!(
        "racy_conditional: {} after {} executions in {:.1} s: {}",
        if r.is_err() { "FOUND" } else { "SURVIVED" },
        CASE.load(Ordering::Relaxed) - n0,
        t0.elapsed().as_secs_f64(),
        msg.lines().next().unwrap_or("")
    );
    assert!(r.is_err(), "the racy store survived {budget} cases");
}
