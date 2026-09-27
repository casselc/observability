//! The deterministic core of the simulation tests: one seed, one thread, one
//! trace.
//!
//! - **OS randomness.** `getrandom(2)` / `getentropy(3)` are defined in this
//!   test binary (the technique of S2's mad-turmoil, which is published as
//!   `mad-turmoil` 0.2.1 but keeps one process-global RNG that can be set
//!   once, so one seed per process). Here the stream is per thread and
//!   re-seedable: each run gets a fresh thread whose OS randomness comes from
//!   its seed, so std's `RandomState` (every `HashMap`'s iteration order),
//!   `rand::random` (worker nonces) and tokio's own RNG (`select!` order)
//!   are functions of the seed. Threads that aren't simulations get the real
//!   syscall.
//! - **Time.** A current-thread tokio runtime with the clock paused: time
//!   moves only when every task waits, straight to the next timer.
//!   `clock_gettime(2)` is overridden too, so `std::time::Instant` and
//!   `SystemTime` inside a simulation read simulated time (object_store's
//!   retry deadline, hyper's pool); outside one, the real clock.
//! - **Decisions.** Every choice the harness makes (fault injection,
//!   latencies, which worker pauses) comes from `Sim::rng`, seeded from the
//!   seed and separate from the OS stream, so a new `HashMap` in the code
//!   under test doesn't shift the faults.
//! - **The trace.** Every request, answer, fault and log line, stamped with
//!   simulated time. A seed must give the same trace byte for byte; the
//!   meta test runs seeds twice and compares.

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

// ---- OS randomness -------------------------------------------------------------------

thread_local! {
    /// The seeded OS stream of a simulation thread (splitmix64 state).
    static OS_RNG: Cell<Option<u64>> = const { Cell::new(None) };
    /// Simulated clocks: (the runtime's start instant, the monotonic ns and
    /// the wall ns it maps to). Set only while inside the paused runtime.
    static SIM_CLOCK: Cell<Option<(tokio::time::Instant, i64, i64)>> = const { Cell::new(None) };
    static IN_CLOCK: Cell<bool> = const { Cell::new(false) };
    /// A turmoil simulation: (monotonic ns, wall ns) at its start; time is
    /// `turmoil::sim_elapsed()` inside a host, else the driver's last step.
    static TURMOIL: Cell<Option<(i64, i64)>> = const { Cell::new(None) };
    static TURMOIL_NOW: Cell<u64> = const { Cell::new(0) };
    static TRACE: RefCell<Trace> = RefCell::new(Trace::default());
}

fn splitmix(s: &mut u64) -> u64 {
    *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Fills `dest` from the thread's seeded stream; false outside a simulation.
fn seeded_fill(dest: &mut [u8]) -> bool {
    OS_RNG
        .try_with(|c| match c.get() {
            Some(mut s) => {
                for chunk in dest.chunks_mut(8) {
                    let v = splitmix(&mut s).to_le_bytes();
                    chunk.copy_from_slice(&v[..chunk.len()]);
                }
                c.set(Some(s));
                true
            }
            None => false,
        })
        .unwrap_or(false)
}

/// The count of OS-randomness requests served from a seed (the self-test
/// checks the override is really in the path).
pub static SEEDED_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn getrandom(buf: *mut u8, buflen: usize, flags: u32) -> isize {
    if buf.is_null() {
        return -1;
    }
    if buflen == 0 {
        return 0;
    }
    let dest = unsafe { std::slice::from_raw_parts_mut(buf, buflen) };
    if seeded_fill(dest) {
        SEEDED_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return buflen as isize;
    }
    unsafe { libc::syscall(libc::SYS_getrandom, buf, buflen, flags) as isize }
}

#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn getentropy(buf: *mut u8, buflen: usize) -> i32 {
    if buflen > 256 {
        unsafe { *libc::__errno_location() = libc::EIO };
        return -1;
    }
    let mut done = 0;
    while done < buflen {
        let n = unsafe { getrandom(buf.add(done), buflen - done, 0) };
        if n < 0 {
            return -1;
        }
        done += n as usize;
    }
    0
}

#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn clock_gettime(clockid: libc::clockid_t, tp: *mut libc::timespec) -> libc::c_int {
    if let Some(ns) = sim_clock_ns(clockid) {
        unsafe {
            (*tp).tv_sec = ns.div_euclid(1_000_000_000) as libc::time_t;
            (*tp).tv_nsec = ns.rem_euclid(1_000_000_000) as libc::c_long;
        }
        return 0;
    }
    unsafe { libc::syscall(libc::SYS_clock_gettime, clockid as libc::c_long, tp) as libc::c_int }
}

fn sim_clock_ns(clockid: libc::clockid_t) -> Option<i64> {
    if let Some((mono0, wall0)) = TURMOIL.try_with(|c| c.get()).ok().flatten() {
        if IN_CLOCK.try_with(|f| f.replace(true)).unwrap_or(true) {
            return None;
        }
        let el = turmoil_elapsed_ns();
        IN_CLOCK.with(|f| f.set(false));
        return match clockid {
            libc::CLOCK_REALTIME | libc::CLOCK_REALTIME_COARSE => Some(wall0 + el),
            libc::CLOCK_MONOTONIC | libc::CLOCK_MONOTONIC_RAW | libc::CLOCK_MONOTONIC_COARSE | libc::CLOCK_BOOTTIME => Some(mono0 + el),
            _ => None,
        };
    }
    let (start, mono0, wall0) = SIM_CLOCK.try_with(|c| c.get()).ok().flatten()?;
    // tokio::time::Instant::now() reads the paused clock without calling back
    // here; the flag makes sure a fallback to std can't recurse.
    if IN_CLOCK.try_with(|f| f.replace(true)).unwrap_or(true) {
        return None;
    }
    let el = tokio::time::Instant::now().saturating_duration_since(start).as_nanos() as i64;
    IN_CLOCK.with(|f| f.set(false));
    match clockid {
        libc::CLOCK_REALTIME | libc::CLOCK_REALTIME_COARSE => Some(wall0 + el),
        libc::CLOCK_MONOTONIC | libc::CLOCK_MONOTONIC_RAW | libc::CLOCK_MONOTONIC_COARSE | libc::CLOCK_BOOTTIME => Some(mono0 + el),
        _ => None,
    }
}

// ---- the decision RNG ----------------------------------------------------------------

#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed ^ 0x5DEE_CE66_D1CE_5EED)
    }
    pub fn next(&mut self) -> u64 {
        splitmix(&mut self.0)
    }
    /// Uniform in [0, n) (0 for n = 0).
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    /// Uniform in [lo, hi].
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi.saturating_sub(lo) + 1)
    }
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }
}

// ---- the trace -------------------------------------------------------------------------

#[derive(Default)]
struct Trace {
    text: String,
    lines: u64,
    keep: bool,
    hasher: Option<blake3::Hasher>,
}

/// Appends one line, stamped with simulated time (ms since the start).
pub fn trace(line: impl AsRef<str>) {
    let t = now_ms();
    TRACE.with(|tr| {
        let mut tr = tr.borrow_mut();
        let l = format!("{:>9}.{:03} {}\n", t / 1000, t % 1000, line.as_ref());
        if let Some(h) = tr.hasher.as_mut() {
            let _ = h.update(l.as_bytes());
        }
        tr.lines += 1;
        if tr.keep {
            tr.text.push_str(&l);
        }
    });
}

/// Simulated ms since the run started (0 outside a run).
pub fn now_ms() -> u64 {
    if TURMOIL.with(|c| c.get()).is_some() {
        return (turmoil_elapsed_ns() / 1_000_000) as u64;
    }
    SIM_CLOCK
        .with(|c| c.get())
        .map_or(0, |(start, _, _)| tokio::time::Instant::now().saturating_duration_since(start).as_millis() as u64)
}

fn turmoil_elapsed_ns() -> i64 {
    match turmoil::sim_elapsed() {
        Some(d) => d.as_nanos() as i64,
        None => TURMOIL_NOW.with(|c| c.get()) as i64 * 1_000_000,
    }
}

/// The turmoil driver's clock, for trace lines written between steps.
pub fn set_turmoil_now(ms: u64) {
    TURMOIL_NOW.with(|c| c.set(ms));
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Sleeps until `at` (simulated ms since the start).
pub async fn sleep_until_ms(at: u64) {
    let now = now_ms();
    if at > now {
        sleep_ms(at - now).await;
    }
}

// ---- a run -------------------------------------------------------------------------------

/// What a scenario gets: the decision RNG and the wall clock's origin.
pub struct Sim {
    pub seed: u64,
    pub rng: RefCell<Rng>,
    /// The wall clock (ms since the Unix epoch) at the start.
    pub wall0_ms: u64,
}

impl Sim {
    pub fn below(&self, n: u64) -> u64 {
        self.rng.borrow_mut().below(n)
    }
    pub fn range(&self, lo: u64, hi: u64) -> u64 {
        self.rng.borrow_mut().range(lo, hi)
    }
    pub fn chance(&self, p: f64) -> bool {
        self.rng.borrow_mut().chance(p)
    }
    /// Wall ms now (simulated).
    pub fn wall(&self) -> u64 {
        self.wall0_ms + now_ms()
    }
}

pub struct Outcome {
    pub seed: u64,
    /// The trace, when kept.
    pub trace: String,
    pub trace_hash: String,
    pub lines: u64,
    /// The panic message, when the run failed.
    pub failure: Option<String>,
    /// The scenario's own summary line.
    pub summary: String,
    pub real_ms: u64,
}

/// Runs `scenario` for `seed` on a fresh thread: seeded OS randomness,
/// a paused current-thread runtime with a LocalSet, simulated clocks, the
/// crate's log lines in the trace. `keep` keeps the whole trace text.
pub fn run<F, Fut>(seed: u64, wall0_ms: u64, keep: bool, scenario: F) -> Outcome
where
    F: FnOnce(Rc<Sim>) -> Fut + Send + 'static,
    Fut: Future<Output = String> + 'static,
{
    on_thread(seed, keep, move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime");
        let local = tokio::task::LocalSet::new();
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            local.block_on(&rt, async move {
                let start = tokio::time::Instant::now();
                SIM_CLOCK.with(|c| c.set(Some((start, 1_000_000_000_000, wall0_ms as i64 * 1_000_000))));
                let sim = Rc::new(Sim { seed, rng: RefCell::new(Rng::new(seed)), wall0_ms });
                let s = scenario(sim).await;
                SIM_CLOCK.with(|c| c.set(None));
                s
            })
        }));
        SIM_CLOCK.with(|c| c.set(None));
        drop(local);
        drop(rt);
        match res {
            Ok(s) => s,
            Err(p) => std::panic::resume_unwind(p),
        }
    })
}

/// As `run`, for a turmoil simulation: `scenario` builds and drives the
/// `turmoil::Sim` itself (seeded with `seed`); std's clocks read turmoil's
/// simulated time inside its hosts.
pub fn run_turmoil<F>(seed: u64, wall0_ms: u64, keep: bool, scenario: F) -> Outcome
where
    F: FnOnce(Rc<Sim>) -> String + Send + 'static,
{
    on_thread(seed, keep, move || {
        TURMOIL.with(|c| c.set(Some((1_000_000_000_000, wall0_ms as i64 * 1_000_000))));
        TURMOIL_NOW.with(|c| c.set(0));
        let sim = Rc::new(Sim { seed, rng: RefCell::new(Rng::new(seed)), wall0_ms });
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scenario(sim)));
        TURMOIL.with(|c| c.set(None));
        match res {
            Ok(s) => s,
            Err(p) => std::panic::resume_unwind(p),
        }
    })
}

/// A fresh thread with seeded OS randomness and the trace; `body`'s panic
/// is the failure.
fn on_thread(seed: u64, keep: bool, body: impl FnOnce() -> String + Send + 'static) -> Outcome {
    // A failing seed's panic is reported by `sweep` (with its repro line), not
    // by the default hook's message and backtrace on every run.
    static QUIET: std::sync::Once = std::sync::Once::new();
    QUIET.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !std::thread::current().name().is_some_and(|n| n.starts_with("dst-")) {
                prev(info);
            }
        }));
    });
    let t0 = std::time::Instant::now();
    let h = std::thread::Builder::new()
        .name(format!("dst-{seed}"))
        .stack_size(64 << 20)
        .spawn(move || {
            let mut s = seed ^ 0x0DD5_EED0_0000_0000;
            OS_RNG.with(|c| c.set(Some(splitmix(&mut s))));
            TRACE.with(|t| *t.borrow_mut() = Trace { keep, hasher: Some(blake3::Hasher::new()), ..Default::default() });
            otap_s3pq::set_log_sink(Some(Box::new(|m: &str| trace(format!("LOG {m}")))));
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
            otap_s3pq::set_log_sink(None);
            OS_RNG.with(|c| c.set(None));
            let tr = TRACE.with(|t| std::mem::take(&mut *t.borrow_mut()));
            let hash = tr.hasher.map(|h| h.finalize().to_hex().to_string()).unwrap_or_default();
            match res {
                Ok(summary) => (tr.text, hash, tr.lines, None, summary),
                Err(p) => {
                    let msg = p
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "panic".into());
                    (tr.text, hash, tr.lines, Some(msg), String::new())
                }
            }
        })
        .expect("spawn");
    let (trace, trace_hash, lines, failure, summary) = h.join().expect("sim thread");
    Outcome { seed, trace, trace_hash, lines, failure, summary, real_ms: t0.elapsed().as_millis() as u64 }
}

/// Seeds from the environment: `DST_SEED=n` (one), else `DST_SEEDS=n`
/// seeds from `DST_SEED_BASE` (default 1), else `default`.
pub fn seeds(default: u64) -> Vec<u64> {
    if let Some(s) = std::env::var("DST_SEED").ok().and_then(|v| v.parse().ok()) {
        return vec![s];
    }
    let n = std::env::var("DST_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(default);
    let base = std::env::var("DST_SEED_BASE").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
    (base..base + n).collect()
}

/// Where a failing seed's trace is written.
pub fn trace_dir() -> std::path::PathBuf {
    std::env::var_os("DST_TRACE_DIR").map(Into::into).unwrap_or_else(std::env::temp_dir)
}

/// Runs every seed (`run(seed, keep_trace)`); on a failure, reruns it
/// keeping the trace, writes the trace, and panics with the seed and a
/// one-line repro.
pub fn sweep(name: &str, test: &str, seeds: &[u64], run: impl Fn(u64, bool) -> Outcome) -> Vec<Outcome> {
    let keep = std::env::var_os("DST_TRACE").is_some();
    let mut out = Vec::new();
    let mut failed = Vec::new();
    for &seed in seeds {
        let o = run(seed, keep);
        if let Some(f) = &o.failure {
            let o2 = if keep { o.trace.clone() } else { run(seed, true).trace };
            let path = trace_dir().join(format!("dst-{name}-{seed}.trace"));
            let _ = std::fs::write(&path, &o2);
            eprintln!(
                "DST {name} seed {seed} FAILED: {f}\n  trace: {}\n  repro: DST_SEED={seed} DST_TRACE=1 cargo test --release --test {test} -- {name} --nocapture",
                path.display()
            );
            failed.push(seed);
        } else if keep {
            let path = trace_dir().join(format!("dst-{name}-{seed}.trace"));
            let _ = std::fs::write(&path, &o.trace);
        }
        if std::env::var_os("DST_VERBOSE").is_some() || seeds.len() == 1 {
            eprintln!("DST {name} seed {seed}: {} lines, {} ms real, {} {}", o.lines, o.real_ms, &o.trace_hash[..16], o.summary);
        }
        out.push(o);
    }
    assert!(failed.is_empty(), "DST {name}: {} of {} seeds failed: {failed:?} (see the repro lines above)", failed.len(), seeds.len());
    out
}
