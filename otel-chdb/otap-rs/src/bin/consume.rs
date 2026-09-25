//! The central consumer: a fleet of workers sharing producer lanes through
//! leases and checkpoints on S3 (conditional writes), ingesting committed
//! objects into ClickHouse several per statement, and a separate GC step.
//! See `src/consumer/mod.rs` and the README's "Consumer" section.
//!
//!   consume --s3 http://127.0.0.1:18333/otel/prefix/edges --ch http://127.0.0.1:18123 --db central
//!           [--depth 2] [--ctl PREFIX] [--signals traces,logs,...] [--worker NAME]
//!           [--ttl 30s --margin 2s --budget 10s] [--poll 1s] [--discover 2s] [--quiet 30s]
//!           [--max-batch 32] [--max-mb 16] [--max-rows 200000] [--no-squash] [--stats FILE --stats-every 5s]
//!           [--once | --exit-after-idle 5s | --run-for 10m] [--key K --secret S] [--ch-s3 URL] [--verbose]
//!   consume gc --s3 ... [--ctl PREFIX] --delay 40s --zombie 10m [--dry-run] [--every 5s --run-for 10m]
//!
//! Lanes are `{root}/{producer}/{signal}` (`--depth 2`, the default) or
//! `{root}/{signal}` (`--depth 1`). The control prefix defaults to
//! `{root}/_consumer`.
//!
//! The prototype's flags still work: `--signal S --table db.t [--state F]`
//! is `--depth 1 --signals S` with that table (the state file is ignored:
//! progress is on S3 now), and the leases are released on exit so the next
//! run can take them at once.

#[path = "../consumer/mod.rs"]
mod consumer;

use consumer::bucket::{Bucket, S3Bucket};
use consumer::coord::{self, Timing};
use consumer::gc::{GcConfig, gc_step};
use consumer::sql::ClickHouseCentral;
use consumer::worker::{Config, RealClock, Worker};
use otap_s3pq::store::S3Config;
use std::rc::Rc;
use std::time::Duration;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn dur_ms(s: &str) -> u64 {
    if let Some(ms) = s.strip_suffix("ms") {
        ms.parse().expect("duration")
    } else if let Some(m) = s.strip_suffix('m') {
        (m.parse::<f64>().expect("duration") * 60_000.0) as u64
    } else {
        (s.trim_end_matches('s').parse::<f64>().expect("duration") * 1000.0) as u64
    }
}

fn opt_ms(args: &[String], name: &str, default: &str) -> u64 {
    dur_ms(&arg(args, name).unwrap_or_else(|| default.to_string()))
}

fn write_atomic(path: &str, s: &str) {
    let tmp = format!("{path}.tmp");
    if std::fs::write(&tmp, s).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

/// `consume audit --s3 … --out FILE [--every 2s --run-for 30m]`: records
/// every object under the root (LIST, then HEAD each new key) as one JSON
/// line: the ground truth of what was committed, kept even after GC deletes
/// the objects. A test tool for the soak.
async fn audit(args: &[String], b: &S3Bucket, root: &str) {
    use std::io::Write;
    let out = arg(args, "--out").expect("--out");
    let every = opt_ms(args, "--every", "2s");
    let run_for = opt_ms(args, "--run-for", "0s");
    let mut seen = std::collections::HashSet::new();
    if let Ok(s) = std::fs::read_to_string(&out) {
        for l in s.lines() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(l) {
                let _ = seen.insert(v["key"].as_str().unwrap_or("").to_string());
            }
        }
    }
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&out).expect("--out");
    let t0 = consumer::mono_ms();
    loop {
        match b.list(root, None).await {
            Ok(items) => {
                for it in items {
                    if seen.contains(&it.key) || it.key.contains("/_consumer/") {
                        continue;
                    }
                    let Ok(Some(meta)) = b.head(&it.key).await else { continue };
                    let rest = it.key.strip_prefix(root).unwrap_or(&it.key).trim_start_matches('/');
                    let parts: Vec<&str> = rest.split('/').collect();
                    if parts.len() < 3 {
                        continue;
                    }
                    let n = parts.len();
                    let v = serde_json::json!({
                        "key": it.key, "size": it.size, "signal": parts[n - 3], "epoch": parts[n - 2],
                        "lane": parts[..n - 2].join("/"),
                        "kind": meta.get(otap_s3pq::proto::META_KIND), "content": meta.get(otap_s3pq::proto::META_CONTENT),
                        "rows": meta.get(otap_s3pq::proto::META_ROWS).and_then(|r| r.parse::<u64>().ok()),
                        "seen_wall_ms": consumer::wall_ms(),
                    });
                    let _ = writeln!(f, "{v}");
                    let _ = seen.insert(it.key);
                }
            }
            Err(e) => eprintln!("audit: {e}"),
        }
        if consumer::mono_ms() - t0 >= run_for {
            break;
        }
        tokio::time::sleep(Duration::from_millis(every)).await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    otel_arrow_dfe_otap::crypto::install_crypto_provider().expect("crypto provider");
    let args: Vec<String> = std::env::args().collect();
    // For scripts: a lane kind's s3() structure / target columns / DDL.
    for (f, what) in [("--print-structure", 0), ("--print-cols", 1), ("--print-ddl", 2)] {
        if let Some(s) = arg(&args, f) {
            let k = consumer::sql::LaneKind::for_signal(&s).expect("a known signal");
            println!("{}", [k.structure.clone(), k.cols.clone(), k.create_table(&format!("db.{}", k.table))][what]);
            return;
        }
    }
    let legacy_signal = arg(&args, "--signal");
    let (key, secret) = (arg(&args, "--key").unwrap_or("otel".into()), arg(&args, "--secret").unwrap_or("otelsecret".into()));
    let s3cfg = S3Config {
        url: arg(&args, "--s3").expect("--s3"),
        access_key_id: Some(key.clone()),
        secret_access_key: Some(secret.clone()),
        put_timeout: Duration::from_millis(opt_ms(&args, "--s3-timeout", "5s")),
        ..Default::default()
    };
    let store = s3cfg.build().expect("s3");
    let root = store.prefix.clone();
    let mut bucket = S3Bucket::new(store);
    bucket.ch_endpoint = arg(&args, "--ch-s3");
    let bucket = Rc::new(bucket);
    let ctl = arg(&args, "--ctl").unwrap_or_else(|| coord::join(&root, "_consumer"));

    if args.get(1).map(String::as_str) == Some("gc") {
        let cfg = GcConfig {
            root,
            ctl,
            delay_ms: opt_ms(&args, "--delay", "40s"),
            zombie_ms: opt_ms(&args, "--zombie", "10m"),
            dry_run: flag(&args, "--dry-run"),
        };
        let every = arg(&args, "--every").map(|s| dur_ms(&s));
        let run_for = opt_ms(&args, "--run-for", "0s");
        let t0 = consumer::mono_ms();
        loop {
            let before = bucket.counts().snap();
            match gc_step(&*bucket, &cfg, consumer::wall_ms()).await {
                Ok(r) => {
                    let after = bucket.counts().snap();
                    println!(
                        "{}",
                        serde_json::json!({"gc": r, "s3": {"list": after.list - before.list, "get": after.get - before.get,
                            "delete": after.delete - before.delete, "put_cas": after.put_cas - before.put_cas + after.put_create - before.put_create}})
                    )
                }
                Err(e) => eprintln!("gc: {e}"),
            }
            match every {
                Some(d) if consumer::mono_ms() - t0 < run_for => tokio::time::sleep(Duration::from_millis(d)).await,
                _ => break,
            }
        }
        return;
    }

    if args.get(1).map(String::as_str) == Some("purge") {
        // Deletes every object under the root (test cleanup).
        let keys: Vec<String> = bucket.list(&root, None).await.expect("list").into_iter().map(|i| i.key).collect();
        let n = bucket.delete(&keys).await.expect("delete");
        println!("{}", serde_json::json!({"purged": n, "root": root}));
        return;
    }
    if args.get(1).map(String::as_str) == Some("audit") {
        audit(&args, &*bucket, &root).await;
        return;
    }

    let worker = arg(&args, "--worker").unwrap_or_else(|| "w".into());
    let worker = format!("{worker}-{}", coord::nonce());
    let mut cfg = Config::new(&root, &ctl, &worker);
    cfg.depth = arg(&args, "--depth").map_or(2, |d| d.parse().expect("--depth"));
    cfg.signals = arg(&args, "--signals").map(|s| s.split(',').map(str::to_string).collect()).unwrap_or_default();
    let mut table_override = None;
    if let Some(s) = &legacy_signal {
        cfg.depth = 1;
        cfg.signals = vec![s.clone()];
        table_override = arg(&args, "--table");
    }
    cfg.timing = Timing {
        ttl_ms: opt_ms(&args, "--ttl", "30s"),
        margin_ms: opt_ms(&args, "--margin", "2s"),
        budget_ms: opt_ms(&args, "--budget", "10s"),
        mutation: coord::Mutation::None,
    };
    cfg.timing.check().expect("lease timing");
    cfg.discover_ms = opt_ms(&args, "--discover", "2s");
    cfg.full_list_ms = opt_ms(&args, "--full-list", "30s");
    cfg.quiet_ms = opt_ms(&args, "--quiet", "30s");
    cfg.limits.max_objects = arg(&args, "--max-batch").map_or(32, |s| s.parse().expect("--max-batch"));
    cfg.limits.max_bytes = arg(&args, "--max-mb").map_or(16, |s| s.parse::<u64>().expect("--max-mb")) << 20;
    cfg.limits.max_rows = arg(&args, "--max-rows").map_or(200_000, |s| s.parse().expect("--max-rows"));
    cfg.verbose = flag(&args, "--verbose") || flag(&args, "-v");
    let once = flag(&args, "--once");
    if once {
        cfg.quiet_ms = 0; // as the prototype: close superseded free heads at once
        cfg.discover_ms = 0;
        cfg.solo = true;
    }
    let poll = opt_ms(&args, "--poll", "1s");
    let idle_exit = arg(&args, "--exit-after-idle").map(|s| dur_ms(&s));
    let run_for = arg(&args, "--run-for").map(|s| dur_ms(&s));
    let stats_path = arg(&args, "--stats");
    let stats_every = opt_ms(&args, "--stats-every", "5s");

    let db = match (&table_override, arg(&args, "--db")) {
        (Some(t), _) => t.split_once('.').map(|(d, _)| d.to_string()).unwrap_or("default".into()),
        (None, Some(d)) => d,
        (None, None) => panic!("--db"),
    };
    let ch_url = arg(&args, "--ch").unwrap_or("http://127.0.0.1:18123".into());
    let mut central = ClickHouseCentral::new(&ch_url, &db, bucket.clone(), &key, &secret, cfg.timing.budget_ms + 5000);
    central.squash = !flag(&args, "--no-squash");
    let central = Rc::new(central);
    if let (Some(t), Some(s)) = (&table_override, &legacy_signal) {
        central.table_override.borrow_mut().insert(s.clone(), t.split_once('.').map_or(t.clone(), |(_, n)| n.to_string()));
    }
    let mut w = Worker::new(cfg, bucket.clone(), central.clone(), RealClock);
    let (cpu0, t0) = (consumer::cpu_ms(), consumer::mono_ms());
    let mut last_progress = consumer::mono_ms();
    let mut last_stats = 0;
    let dump = |w: &Worker<S3Bucket, ClickHouseCentral<S3Bucket>, RealClock>, final_: bool| {
        let mut v = w.stats_json();
        let m = v.as_object_mut().expect("object");
        let _ = m.insert("cpu_ms".into(), (consumer::cpu_ms() - cpu0).into());
        let _ = m.insert("elapsed_ms".into(), (consumer::mono_ms() - t0).into());
        let _ = m.insert("ch_statements".into(), central.statements.get().into());
        let _ = m.insert("ch_checks".into(), central.checks.get().into());
        let _ = m.insert("summary".into(), final_.into());
        v.to_string()
    };
    loop {
        let t = consumer::mono_ms();
        let progressed = w.step().await;
        let now = consumer::mono_ms();
        if progressed {
            last_progress = now;
        }
        if let Some(p) = &stats_path {
            if now >= last_stats + stats_every {
                write_atomic(p, &dump(&w, false));
                last_stats = now;
            }
        }
        let done = (once && !progressed)
            || idle_exit.is_some_and(|d| now - last_progress >= d)
            || run_for.is_some_and(|d| now - t0 >= d);
        if done {
            break;
        }
        if !(once && progressed) {
            let spent = now - t;
            tokio::time::sleep(Duration::from_millis(poll.saturating_sub(spent))).await;
        }
    }
    w.release_all().await;
    let s = dump(&w, true);
    if let Some(p) = &stats_path {
        write_atomic(p, &s);
    }
    println!("{s}");
}
