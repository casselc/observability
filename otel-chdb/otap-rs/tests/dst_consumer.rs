//! Deterministic simulation of the consumer fleet (DST.md, level 1): the
//! real `Worker`, `gc_step` and edge writers as concurrent tasks on a paused
//! current-thread runtime, against a simulated bucket and central that add
//! latency, pauses, partitions and the ambiguous outcomes of AMBIGUITY.md,
//! all drawn from one seed. A seed gives one trace, byte for byte
//! (`fleet_is_deterministic`).
//!
//! Checked while it runs (every checkpoint write, every statement landing)
//! and at the end:
//! - **neverSkipsCommitted**: a checkpoint written with `next = n` for an
//!   epoch has every committed slot below `n` fully in central;
//! - **noCommitAfterClose**: no data at or after a closed epoch's tombstone;
//! - **statements stay inside their lease**: a statement is issued only while
//!   its worker's lease is the lane's current one, and lands (commits) before
//!   that lease version changes hands (a takeover, or a release);
//! - **exactly once** at the end: every committed content key in central
//!   with its committed rows (atMostOnce + neverSkipsCommitted + liveness),
//!   nothing uncommitted.
//!
//!   cargo test --release --test dst_consumer -- fleet                  # 40 seeds
//!   DST_SEEDS=2000 cargo test --release --test dst_consumer -- fleet_seeds
//!   DST_SEED=17 DST_TRACE=1 cargo test --release --test dst_consumer -- fleet_seeds --nocapture

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
mod consumer;
mod dst;
#[path = "dst/fleet.rs"]
mod fleet;

use consumer::coord::Mutation;
use dst::sim::{self, sleep_ms, trace};
use fleet::*;
use std::collections::HashMap;

// ---- the tests ---------------------------------------------------------------------------

/// The overrides are really in the path: in a simulation, OS randomness
/// (HashMap order, rand::random) is a function of the seed, and std's
/// clocks read simulated time.
#[test]
fn sim_self_test() {
    let order = |seed: u64| {
        sim::run(seed, 1_700_000_000_000, true, |_sim| async move {
            let m: HashMap<u32, ()> = (0..64).map(|i| (i, ())).collect();
            let order: Vec<u32> = m.keys().copied().collect();
            let r: u64 = rand::random();
            let i0 = std::time::Instant::now();
            let s0 = std::time::SystemTime::now();
            sleep_ms(90_000).await;
            let di = i0.elapsed().as_millis();
            let ds = s0.elapsed().unwrap().as_millis();
            let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
            trace(format!("{order:?} {r} {di} {ds} {wall}"));
            format!("{order:?} {r} {di} {ds} {wall}")
        })
    };
    let before = sim::SEEDED_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let (a, b, c) = (order(1), order(1), order(2));
    assert!(a.failure.is_none(), "{:?}", a.failure);
    assert_eq!(a.summary, b.summary, "same seed, same HashMap order and randomness");
    assert_eq!(a.trace, b.trace);
    assert_ne!(a.summary, c.summary, "another seed, another order");
    assert!(a.summary.ends_with(&format!(" 90000 90000 {}", 1_700_000_000_000u64 + 90_000)), "std clocks read simulated time: {}", a.summary);
    assert!(sim::SEEDED_CALLS.load(std::sync::atomic::Ordering::Relaxed) > before, "getrandom served from the seed");
}

/// The fleet over many seeds (`DST_SEEDS`, `DST_SEED_BASE`, `DST_SEED`).
#[test]
fn fleet_seeds() {
    let seeds = sim::seeds(40);
    let t0 = std::time::Instant::now();
    let out = sim::sweep("fleet_seeds", "dst_consumer", &seeds, |seed, keep| sim::run(seed, wall0(seed), keep, fleet));
    let lines: u64 = out.iter().map(|o| o.lines).sum();
    eprintln!("DST fleet: {} seeds passed, {lines} trace lines, {:.1} s", out.len(), t0.elapsed().as_secs_f64());
}

/// Regression (found by seeds 20 and 34): a worker whose discovery round
/// is slow (here: its LIST of the heartbeats answers 15 s late; a process
/// pause does the same) used to record the lease ETags it then listed as
/// first seen when the round started. A renewal made during the slow round
/// looked unchanged for longer than it was, and the worker took a live lease
/// from its holder (`worker.rs` `heartbeat_and_leases`).
#[test]
fn a_slow_discovery_round_does_not_backdate_lease_observations() {
    let o = sim::run(7, wall0(7), true, |sim| async move {
        let mut p = Profile::calm(2, 1, 40_000);
        p.slow_list = Some((1, 20_000, 15_000));
        fleet_with(sim, p).await
    });
    if let Some(f) = &o.failure {
        let path = sim::trace_dir().join("dst-slow-discovery.trace");
        let _ = std::fs::write(&path, &o.trace);
        panic!("{f}\n  trace: {}", path.display());
    }
    assert!(o.trace.contains("its answer is 15000 ms late"), "the slow LIST happened");
}

/// Regression (found by ~10% of the first 200 seeds): a worker with a
/// backlog on several lanes HEADs slot after slot, lane after lane, with no
/// renewal in between; when that takes longer than the lease window (slow
/// S3, many lanes: in production 75 s of HEADs, e.g. 40 lanes × 256 slots
/// at 20 ms), every lease lapses before the insert, the HEAD cache goes with
/// them, and the next step starts over: nothing is ever ingested. Now the
/// step renews between lanes and a lane stops HEADing once its renewal is
/// due (`worker.rs` `step`, `scan_inner`).
#[test]
fn a_backlog_longer_than_the_lease_window_is_still_ingested() {
    let o = sim::run(11, wall0(11), true, |sim| async move {
        let mut p = Profile::calm(1, 3, 40_000);
        p.lat_ms = 60;
        p.pause_at = Some((0, 10_000, 20_000));
        fleet_with(sim, p).await
    });
    if let Some(f) = &o.failure {
        let path = sim::trace_dir().join("dst-backlog.trace");
        let _ = std::fs::write(&path, &o.trace);
        panic!("{}\n  trace: {}", &f[..f.len().min(600)], path.display());
    }
}

/// Regression (found by seed 1950 on the fleet without late parts): the
/// server's clock 359 ms ahead of the worker's, an announcement statement
/// sent just past its fence was a silent no-op on the server, answered
/// with an empty OK the worker took for landed; the lane's rows followed in
/// a statement with a renewed fence, before their resources' announcements
/// (`sameLane`). Announcements now carry a loud fence (sql.rs `FENCED`).
#[test]
fn a_server_fenced_announcement_is_not_taken_for_landed() {
    let o = sim::run(1950, wall0(1950), false, |sim| async move {
        NO_LATE.with(|c| c.set(true));
        fleet(sim).await
    });
    assert!(o.failure.is_none(), "{:?}", o.failure);
}

/// The harness finds the model's mutants (`coord::Mutation`, the bugs the
/// Quint models were checked against): each must fail some seed.
#[test]
fn fleet_catches_mutants() {
    let n = std::env::var("DST_MUTANT_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(150u64);
    for (name, m) in [
        ("no_time_bound", Mutation::NoTimeBound),
        ("no_verify", Mutation::NoVerify),
        ("release_in_flight", Mutation::ReleaseInFlight),
        ("error_settles", Mutation::ErrorSettles),
        ("early_compact", Mutation::EarlyCompact),
        ("mix_late_parts", Mutation::MixLateParts),
    ] {
        let t0 = std::time::Instant::now();
        let caught = (1..=n).find_map(|seed| {
            let o = sim::run(seed, wall0(seed), false, move |sim| async move {
                MUTANT.with(|c| c.set(m));
                fleet(sim).await
            });
            o.failure.map(|f| (seed, f))
        });
        match caught {
            Some((seed, f)) => {
                let first = f.lines().find(|l| l.contains("t=") || l.contains("not ingested") || l.contains("once")).unwrap_or(&f);
                eprintln!("mutant {name}: caught by seed {seed} in {:.1} s: {}", t0.elapsed().as_secs_f64(), &first.trim()[..first.trim().len().min(200)]);
            }
            None => panic!("mutant {name} survived {n} seeds"),
        }
    }
}

/// Determinism: each seed twice, traces compared byte for byte.
#[test]
fn fleet_is_deterministic() {
    let n = std::env::var("DST_META_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(4u64);
    let base = std::env::var("DST_SEED_BASE").ok().and_then(|v| v.parse().ok()).unwrap_or(1000u64);
    for seed in base..base + n {
        let a = sim::run(seed, wall0(seed), true, fleet);
        let b = sim::run(seed, wall0(seed), true, fleet);
        assert!(a.lines > 1000, "seed {seed}: a trace worth comparing ({} lines)", a.lines);
        if a.trace != b.trace || a.failure != b.failure {
            let dir = sim::trace_dir();
            let (pa, pb) = (dir.join(format!("dst-meta-{seed}-a.trace")), dir.join(format!("dst-meta-{seed}-b.trace")));
            let _ = std::fs::write(&pa, &a.trace);
            let _ = std::fs::write(&pb, &b.trace);
            let first = a.trace.lines().zip(b.trace.lines()).position(|(x, y)| x != y);
            panic!(
                "seed {seed}: two runs differ (first at line {first:?}): diff {} {}\n  repro: DST_SEED_BASE={seed} DST_META_SEEDS=1 cargo test --release --test dst_consumer -- fleet_is_deterministic",
                pa.display(),
                pb.display()
            );
        }
        eprintln!("DST meta seed {seed}: identical, {} lines, {} ({})", a.lines, &a.trace_hash[..16], a.failure.as_deref().unwrap_or("passed"));
    }
}

// ---- dead-lane retirement (DECISIONS.md D35, ../model/retirement.qnt) ------------------------

#[path = "dst/retire.rs"]
mod retire;

fn retire_run(seed: u64, k: retire::Knobs) -> retire::Outcome {
    tokio::runtime::Builder::new_current_thread().build().expect("runtime").block_on(retire::run(seed, k))
}

/// The design over many seeds (`DST_RETIRE_SEEDS`, default 300): no
/// property of the model breaks, and its witnesses are reached (a lane
/// retired by its close and passed, a retired lane reborn and ingested, a
/// request lost with its volume, a zombie PUT landing, an adopted volume).
#[test]
fn retirement_seeds() {
    let n = std::env::var("DST_RETIRE_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(300u64);
    let t0 = std::time::Instant::now();
    let mut seen = std::collections::BTreeMap::new();
    for seed in 1..=n {
        let o = retire_run(seed, retire::Knobs::design());
        assert!(o.violations.is_empty(), "seed {seed}: {:#?}\n{}", o.violations, o.summary);
        for w in o.witnesses {
            *seen.entry(w).or_insert(0u64) += 1;
        }
    }
    eprintln!("DST retirement: {n} seeds passed in {:.1} s; witnesses {seen:?}", t0.elapsed().as_secs_f64());
    for w in ["a lane retired", "published past a retired lane", "reborn", "lost with its volume", "zombie landed", "adopted"] {
        assert!(seen.contains_key(w), "witness {w:?} never reached in {n} seeds: {seen:?}");
    }
    assert!(!seen.contains_key("quarantined"), "the design never quarantines");
}

/// Each of the model's mutants breaks a property on some seed.
#[test]
fn retirement_catches_mutants() {
    let n = std::env::var("DST_MUTANT_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(300u64);
    let d = retire::Knobs::design();
    for (name, k, want) in [
        ("retireStale", retire::Knobs { mutation: Mutation::RetireStale, ..d }, "completeSound"),
        ("closeUndrained", retire::Knobs { close_undrained: true, ..d }, "completeSound"),
        ("staysRetired", retire::Knobs { mutation: Mutation::StaysRetired, ..d }, "completeSound"),
    ] {
        let caught = (1..=n).find_map(|seed| {
            let o = retire_run(seed, k);
            o.violations.iter().find(|v| v.starts_with(want)).map(|v| (seed, v.clone()))
        });
        match caught {
            Some((seed, v)) => eprintln!("mutant {name}: caught by seed {seed}: {v}"),
            None => panic!("mutant {name} survived {n} seeds (no {want} violation)"),
        }
    }
}
