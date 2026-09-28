//! The central consumer: a fleet of workers sharing producer lanes through
//! leases and checkpoints on S3 (conditional writes), ingesting committed
//! objects into ClickHouse several per statement, and a separate GC step.
//! See `src/consumer/mod.rs` and the README's "Consumer" section.
//!
//!   consume --s3 http://127.0.0.1:18333/otel/prefix/edges --ch http://127.0.0.1:18123 --db central
//!           [--depth 3] [--ctl PREFIX] [--signals traces,logs,...] [--worker NAME]
//!           [--ttl 75s --margin 20s --budget 10s --keeper-slack 20s [--allow-short-margin]]
//!           [--poll 1s] [--discover 2s] [--lanes-every 30s] [--quiet 30s]
//!           [--idle-backoff 1s..30s | off] [--idle-after 10s] [--linger 0ms]
//!           [--balance load|count] [--hysteresis 0.2] [--lane-weight 50] [--load-window 60s] [--min-hold 30s] [--loads-every 10s]
//!           [--check-horizon 3d | all] [--no-check-range]
//!           [--max-batch 32] [--max-mb 16] [--max-rows 200000] [--no-squash] [--stats FILE --stats-every 5s]
//!           [--once | --exit-after-idle 5s | --run-for 10m] [--ch-s3 URL] [--verbose]
//!           credentials (every subcommand): [--key K --secret S [--session-token T]] | [--profile P] [--role-arn ARN]
//!           [--credential-process CMD]; else the AWS chain (env, profile, IRSA, Pod Identity, IMDS);
//!           [--region R] (else AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
//!           [--ch-s3-auth pass | server] (ClickHouse's s3(): the consumer's credentials per statement, or the server's own)
//!           replicated central: [--ch URL1,URL2] [--sync-replica [--sync-timeout 5s] [--switch-hold <budget+slack+2s>]]
//!           [--no-ddl] [--insert-setting k=v ...] [--metrics-addr HOST:PORT]
//!   consume gc --s3 ... [--ctl PREFIX] --delay 115s --zombie 10m [--dry-run] [--every 5s --run-for 10m]
//!           [--depth 3 --wm-skew 5s --wm-stale 5m | --no-watermark]
//!   consume watermark --s3 ... [--ctl PREFIX] [--every 5s --run-for 10m] [--depth 3 --wm-skew 5s --wm-stale 5m]
//!           (complete_through alone: {ctl}/watermark.json, ../../FORMAT.md §3)
//!           [--ch URL --db DB [--audit-every 24h | off] <audit flags>] [--metrics-addr HOST:PORT]
//!   consume --print-ddl SIGNAL | --print-rollups SIGNAL | --print-structure SIGNAL | --print-cols SIGNAL
//!   consume horizon-audit --s3 ... --ch URL --db DB [--every 1h [--run-for D]] [--metrics-addr HOST:PORT]
//!           audit flags: [--check-horizon 3d | all] [--audit-lookback 2d] [--audit-sample-hex 0]
//!           [--audit-max-candidates 1000] [--audit-tables t1,t2] [--audit-max-threads 2] [--audit-dup-sample 16]
//!           [--audit-timeout 30m] [--audit-no-state]
//!           replicated central: [--ch URL1,URL2 (a run reads the first that answers)]
//!           [--sync-replica [--audit-sync-timeout 60s] (SYNC REPLICA … LIGHTWEIGHT per table first)]
//!
//! The horizon audit (`consumer/audit.rs`) reports copies of a request that
//! were ingested twice because the copy was received more than the check's
//! horizon after its original (a WARN line each, `consumer_late_copies_total`).
//! It runs beside GC or alone, never in a worker; its failures are reported
//! (`consumer_audit_runs_total{result="error"}`), never fatal. It remembers
//! what it reported in `{ctl}/audit/{db}.json`. `--metrics-addr` serves
//! Prometheus text on `/metrics` (off by default; e.g. `:9464`).
//!
//! The lease timing is validated at start: a margin below 20 s is refused
//! (on a replicated central a commit can land up to the Keeper session
//! timeout, 30 s, after its statement started: 20 s past a 10 s budget)
//! unless `--allow-short-margin` (tests with compressed timing), and the
//! margin must cover `--keeper-slack` (default 20 s; with
//! `--allow-short-margin`, the margin itself). On a replicated central
//! (`--sync-replica` or several `--ch`) the worker also reads the replicas'
//! Keeper session timeout and refuses a slack below it minus the budget.
//!
//! Checkpoints are compacted: once `consume gc` has retired a closed epoch
//! (after `--zombie`), the lane's holder drops it at its next full listing
//! (`--full-list`), so a checkpoint holds only the epochs not retired yet.
//! Without GC running, checkpoints keep every epoch.
//!
//! Lanes are `{root}/{cluster}/{producer}/{signal}` (format v2,
//! `../../FORMAT.md`; `--depth 3`, the default; 2 and 1 drop the leading
//! segments, for tests). The control prefix defaults to `{root}/_consumer`.
//! The workers and GC check `{ctl}/format.json` at start, creating it when
//! absent, and refuse to run on a bucket of another format.
//!
//! The prototype's flags still work: `--signal S --table db.t [--state F]`
//! is `--signals S` with that table (the state file is ignored:
//! progress is on S3 now), and the leases are released on exit so the next
//! run can take them at once.

#[path = "../consumer/mod.rs"]
mod consumer;

use consumer::audit::{self, AuditConfig, AuditState};
use consumer::bucket::{Bucket, Cond, Put, S3Bucket};
use consumer::coord::{self, Timing};
use consumer::gc::{GcConfig, gc_step};
use consumer::sql::ClickHouseCentral;
use consumer::discovery::Backoff;
use consumer::worker::{BalanceMode, Config, RealClock, Worker};
use otap_s3pq::store::S3Config;
use consumer::metrics::{self, AuditMetrics};
use std::cell::RefCell;
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
    } else if let Some(d) = s.strip_suffix('d') {
        (d.parse::<f64>().expect("duration") * 86_400_000.0) as u64
    } else if let Some(h) = s.strip_suffix('h') {
        (h.parse::<f64>().expect("duration") * 3_600_000.0) as u64
    } else if let Some(m) = s.strip_suffix('m') {
        (m.parse::<f64>().expect("duration") * 60_000.0) as u64
    } else {
        (s.trim_end_matches('s').parse::<f64>().expect("duration") * 1000.0) as u64
    }
}

fn opt_ms(args: &[String], name: &str, default: &str) -> u64 {
    dur_ms(&arg(args, name).unwrap_or_else(|| default.to_string()))
}

/// Where the S3 credentials come from.
#[derive(Debug, PartialEq)]
enum Creds {
    /// `--key`/`--secret` (and `--session-token`).
    Keys { key: String, secret: String, token: Option<String> },
    /// Nothing given or named, and the store is local (http on loopback):
    /// the local stack's otel/otelsecret, as before the chain existed.
    DevDefault,
    /// The AWS chain: environment keys, shared profile (`--profile`,
    /// `AWS_PROFILE`), web identity (IRSA), container credentials (EKS Pod
    /// Identity), IMDS; `--role-arn` on top (store.rs, creds.rs).
    Chain,
}

/// Picks the credential source from the flags and the environment.
fn choose_creds(args: &[String], env: &dyn Fn(&str) -> Option<String>, s3_url: &str) -> Result<Creds, String> {
    let set = |k: &str| env(k).is_some_and(|v| !v.is_empty());
    match (arg(args, "--key"), arg(args, "--secret")) {
        (Some(key), Some(secret)) => return Ok(Creds::Keys { key, secret, token: arg(args, "--session-token").filter(|t| !t.is_empty()) }),
        (Some(_), None) | (None, Some(_)) => return Err("--key and --secret go together".into()),
        (None, None) => {}
    }
    if arg(args, "--session-token").is_some() {
        return Err("--session-token needs --key and --secret".into());
    }
    let named = ["--profile", "--role-arn", "--credential-process"].iter().any(|f| arg(args, f).is_some())
        || [
            "AWS_ACCESS_KEY_ID",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_PROFILE",
        ]
        .iter()
        .any(|v| set(v));
    let local = url::Url::parse(s3_url).is_ok_and(|u| {
        u.scheme() == "http" && matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]" | "::1"))
    });
    Ok(if !named && local { Creds::DevDefault } else { Creds::Chain })
}

/// What the GC / audit process exports.
#[derive(Default)]
struct Metrics {
    gc: Option<GcMetrics>,
    audit_on: bool,
    audit: AuditState,
    audit_m: AuditMetrics,
    horizon_ms: Option<u64>,
    wm: Option<consumer::watermark::WmDoc>,
    wm_err: u64,
}

/// `complete_through` as metrics (../../FORMAT.md §3).
fn wm_families(p: &mut metrics::Prom, d: &consumer::watermark::WmDoc, errors: u64) {
    p.gauge("consumer_complete_through_seconds", "The published complete_through (Unix s): every request received before it is ingested.", &[], d.complete_through_ns as f64 / 1e9);
    p.gauge("consumer_complete_through_lag_seconds", "Wall clock minus complete_through at the last run.", &[], (d.wall_ms as f64 / 1e3 - d.complete_through_ns as f64 / 1e9).max(0.0));
    p.gauge("consumer_watermark_lanes", "Lanes the last run saw.", &[], d.lanes as f64);
    p.gauge("consumer_watermark_stale_lanes", "Lanes whose watermark lags the wall clock by more than --wm-stale.", &[], d.stale.len() as f64);
    p.counter("consumer_watermark_errors_total", "Watermark runs that failed.", &[], errors as f64);
    p.declare("consumer_lane_watermark_lag_seconds", metrics::Kind::Gauge, "A stale lane's watermark lag (only stale lanes are exported).");
    for l in &d.stale {
        p.gauge("consumer_lane_watermark_lag_seconds", "", &[("lane", l.lane.as_str())], l.lag_s);
    }
}

#[derive(Default)]
struct GcMetrics {
    ok: u64,
    err: u64,
    deleted: u64,
    last_success_s: Option<f64>,
}

impl GcMetrics {
    fn families(&self, p: &mut metrics::Prom) {
        p.counter("consumer_gc_runs_total", "GC runs, by result.", &[("result", "ok")], self.ok as f64);
        p.counter("consumer_gc_runs_total", "", &[("result", "error")], self.err as f64);
        p.counter("consumer_gc_deleted_objects_total", "Objects GC deleted (data slots and retired epochs' objects).", &[], self.deleted as f64);
        p.declare("consumer_gc_last_success_timestamp_seconds", metrics::Kind::Gauge, "Unix time of the last GC run with no error.");
        if let Some(t) = self.last_success_s {
            p.gauge("consumer_gc_last_success_timestamp_seconds", "", &[], t);
        }
    }
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
    // For scripts: a lane kind's s3() structure / target columns / table DDL
    // (one statement, no ';') / the statements `ensure` runs after the table
    // (the key-value rollup table and its materialized view for traces and
    // logs, each ending in ';'; nothing for metrics). Table names are `db.<table>…`.
    for (f, what) in [("--print-structure", 0), ("--print-cols", 1), ("--print-ddl", 2), ("--print-rollups", 3)] {
        if let Some(s) = arg(&args, f) {
            let k = consumer::sql::LaneKind::for_signal(&s).expect("a known signal");
            let fq = format!("db.{}", k.table);
            let rollups = k.create_rollups(&fq).iter().map(|st| format!("{st};\n")).collect::<Vec<_>>().join("\n");
            print!("{}", [k.structure.clone() + "\n", k.cols.clone() + "\n", k.create_table(&fq) + "\n", rollups][what]);
            return;
        }
    }
    // For scripts: the horizon audit's statements (`consumer_check_range.sh`
    // measures exactly these): the candidates of table FQ from --since-ns,
    // or with --keys k1,k2 --days d1,d2 the two confirmation queries.
    if let Some(fq) = arg(&args, "--print-audit-sql") {
        let sample = arg(&args, "--audit-sample-hex").map_or(0, |s| s.parse().expect("--audit-sample-hex"));
        match arg(&args, "--keys") {
            None => println!("{}", audit::candidates_sql(&fq, arg(&args, "--since-ns").expect("--since-ns").parse().expect("ns"), sample, 1000)),
            Some(k) => {
                let keys: Vec<String> = k.split(',').map(str::to_string).collect();
                let days = arg(&args, "--days").expect("--days").split(',').map(|d| d.parse().expect("day")).collect();
                println!("{}\n{}", audit::dups_sql(&fq, &keys, &days), audit::groups_sql(&fq, &keys, &days));
            }
        }
        return;
    }
    let legacy_signal = arg(&args, "--signal");
    let s3_url = arg(&args, "--s3").expect("--s3");
    let env = |k: &str| std::env::var(k).ok();
    let creds = match choose_creds(&args, &env, &s3_url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("consume: {e}");
            std::process::exit(2);
        }
    };
    let (access_key_id, secret_access_key, session_token) = match &creds {
        Creds::Keys { key, secret, token } => (Some(key.clone()), Some(secret.clone()), token.clone()),
        Creds::DevDefault => {
            eprintln!("consume: no credentials given or named in the environment and {s3_url} is local: using the local stack's otel/otelsecret");
            (Some("otel".to_string()), Some("otelsecret".to_string()), None)
        }
        Creds::Chain => (None, None, None),
    };
    let profile = arg(&args, "--profile");
    let region_env = |k: &str| if k == "AWS_PROFILE" && profile.is_some() { profile.clone() } else { env(k) };
    let region = otap_s3pq::store::resolve_region(arg(&args, "--region").as_deref(), &region_env, &otap_s3pq::creds::load_profiles());
    let s3cfg = S3Config {
        url: s3_url.clone(),
        region,
        access_key_id,
        secret_access_key,
        session_token,
        profile: profile.clone(),
        role_arn: arg(&args, "--role-arn"),
        credential_process: arg(&args, "--credential-process"),
        put_timeout: Duration::from_millis(opt_ms(&args, "--s3-timeout", "5s")),
        ..Default::default()
    };
    let store = match s3cfg.build() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("consume: {e}");
            std::process::exit(2);
        }
    };
    let ch_s3_auth = match (arg(&args, "--ch-s3-auth").as_deref(), &store.credentials) {
        (None | Some("pass"), Some(p)) => consumer::sql::S3Auth::Chain(p.clone()),
        (Some("server"), _) => consumer::sql::S3Auth::Server,
        (a, _) => {
            eprintln!("consume: --ch-s3-auth {a:?}: pass or server");
            std::process::exit(2);
        }
    };
    let root = store.prefix.clone();
    let mut bucket = S3Bucket::new(store);
    bucket.ch_endpoint = arg(&args, "--ch-s3");
    let bucket = Rc::new(bucket);
    let ctl = arg(&args, "--ctl").unwrap_or_else(|| coord::join(&root, "_consumer"));

    let metrics_addr = arg(&args, "--metrics-addr");
    let prom = metrics::shared();
    if let Some(a) = &metrics_addr {
        match metrics::serve(a, prom.clone()).await {
            Ok(at) => eprintln!("consume: metrics on http://{at}/metrics"),
            // Metrics are not worth stopping ingest (or GC) for.
            Err(e) => eprintln!("consume: --metrics-addr {a}: {e}; running without metrics"),
        }
    }
    let horizon_ms = match arg(&args, "--check-horizon").as_deref() {
        None => Some(consumer::worker::DEFAULT_HORIZON_MS),
        Some("all") => None,
        Some(h) => Some(dur_ms(h)),
    };

    let sub = args.get(1).map(String::as_str);
    if !matches!(sub, Some("horizon-audit" | "purge" | "audit")) {
        if let Err(e) = consumer::ensure_format(&*bucket, &ctl).await {
            eprintln!("consume: refusing to start: {e}");
            std::process::exit(2);
        }
    }
    if matches!(sub, Some("gc" | "horizon-audit" | "watermark")) {
        // GC (`gc`), and the horizon audit beside it (`gc --audit-every`) or
        // alone (`horizon-audit`). One task: GC and the audit interleave at
        // their awaits, and neither is on any worker's path.
        let run_for = opt_ms(&args, "--run-for", "0s");
        let every = arg(&args, "--every").map(|s| dur_ms(&s));
        let t0 = consumer::mono_ms();
        let state = Rc::new(RefCell::new(Metrics { horizon_ms, ..Default::default() }));
        let render = |st: &Metrics| {
            let mut p = metrics::Prom::default();
            if let Some(g) = &st.gc {
                g.families(&mut p);
            }
            if st.audit_on {
                metrics::audit_families(&mut p, &st.audit, &st.audit_m, st.horizon_ms);
            }
            if let Some(d) = &st.wm {
                wm_families(&mut p, d, st.wm_err);
            }
            metrics::publish(&prom, p.render());
        };
        let gc_loop = async {
            if !matches!(sub, Some("gc" | "watermark")) {
                return;
            }
            // complete_through, after each GC run (`--no-watermark`: off), or alone (`watermark`).
            let wm_cfg = (!flag(&args, "--no-watermark")).then(|| consumer::watermark::WmConfig {
                depth: arg(&args, "--depth").map_or(3, |d| d.parse().expect("--depth")),
                skew_ms: opt_ms(&args, "--wm-skew", "5s"),
                stale_ms: opt_ms(&args, "--wm-stale", "5m"),
                ..consumer::watermark::WmConfig::new(&root, &ctl)
            });
            let cfg = GcConfig {
                root: root.clone(),
                ctl: ctl.clone(),
                delay_ms: opt_ms(&args, "--delay", "115s"),
                zombie_ms: opt_ms(&args, "--zombie", "10m"),
                dry_run: flag(&args, "--dry-run"),
            };
            loop {
                if let Some(wc) = &wm_cfg {
                    let r = consumer::watermark::watermark_step(&*bucket, wc, consumer::wall_ms()).await;
                    let mut st = state.borrow_mut();
                    match r {
                        Ok(d) => {
                            println!("{}", serde_json::json!({"watermark": d}));
                            st.wm = Some(d);
                        }
                        Err(e) => {
                            st.wm_err += 1;
                            eprintln!("watermark: {e}");
                        }
                    }
                    render(&st);
                }
                if sub == Some("watermark") {
                    match every {
                        Some(d) if consumer::mono_ms() - t0 < run_for => {
                            tokio::time::sleep(Duration::from_millis(d)).await;
                            continue;
                        }
                        _ => break,
                    }
                }
                let before = bucket.counts().snap();
                let r = gc_step(&*bucket, &cfg, consumer::wall_ms()).await;
                {
                    let mut st = state.borrow_mut();
                    let g = st.gc.get_or_insert_with(Default::default);
                    match &r {
                        Ok(r) => {
                            g.ok += 1;
                            g.deleted += (r.deleted_data + r.deleted_tombstones) as u64;
                            g.last_success_s = Some(consumer::wall_ms() as f64 / 1e3);
                        }
                        Err(_) => g.err += 1,
                    }
                    render(&st);
                }
                match r {
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
        };
        // `gc` with `--db` audits every `--audit-every` (24 h; `off`: never).
        let audit_every = match (sub, arg(&args, "--audit-every").as_deref()) {
            (Some("horizon-audit"), _) => Some(every.unwrap_or(0)),
            (_, Some("off")) => None,
            (_, a) if arg(&args, "--db").is_some() => Some(dur_ms(a.unwrap_or("24h"))),
            _ => None,
        };
        let audit_loop = async {
            let Some(audit_every) = audit_every else { return };
            let db = arg(&args, "--db").expect("the horizon audit needs --db (and --ch)");
            let ch_url = arg(&args, "--ch").unwrap_or("http://127.0.0.1:18123".into());
            // `--ch a,b`: a replicated central; a run reads the first replica
            // that answers (and, with --sync-replica, syncs each table first).
            let replicas: Vec<otap_s3pq::central::ClickHouse> = ch_url
                .split(',')
                .filter(|u| !u.is_empty())
                .map(|u| {
                    let mut ch = otap_s3pq::central::ClickHouse::new(u);
                    ch.http = reqwest::Client::builder()
                        .timeout(Duration::from_millis(opt_ms(&args, "--audit-timeout", "30m")))
                        .build()
                        .expect("http client");
                    ch
                })
                .collect();
            let mut cur = 0usize;
            let cfg = AuditConfig {
                horizon_ms,
                lookback_ms: opt_ms(&args, "--audit-lookback", "2d"),
                sample_hex: arg(&args, "--audit-sample-hex").map_or(0, |s| s.parse().expect("--audit-sample-hex")),
                max_candidates: arg(&args, "--audit-max-candidates").map_or(1000, |s| s.parse().expect("--audit-max-candidates")),
                tables: arg(&args, "--audit-tables").map(|s| s.split(',').map(str::to_string).collect()).unwrap_or_default(),
                max_threads: arg(&args, "--audit-max-threads").map_or(2, |s| s.parse().expect("--audit-max-threads")),
                sync_replica: flag(&args, "--sync-replica"),
                sync_timeout_ms: opt_ms(&args, "--audit-sync-timeout", "60s"),
                dup_sample: arg(&args, "--audit-dup-sample").map_or(16, |s| s.parse().expect("--audit-dup-sample")),
            };
            // The copies already reported survive a restart: {ctl}/audit/{db}.json.
            let state_key = coord::join(&ctl, &format!("audit/{db}.json"));
            let persist = !flag(&args, "--audit-no-state");
            // Read before borrowing: `state` is shared with the GC loop, which
            // borrows it mutably at its own awaits, so no borrow may be held
            // across one here.
            let saved = if persist {
                match bucket.get(&state_key).await {
                    Ok(Some((b, _))) => match serde_json::from_slice(&b) {
                        Ok(a) => Some(a),
                        Err(e) => {
                            eprintln!("horizon-audit: {state_key}: {e} (starting afresh)");
                            None
                        }
                    },
                    Ok(None) => None,
                    Err(e) => {
                        eprintln!("horizon-audit: reading {state_key}: {e} (starting afresh)");
                        None
                    }
                }
            } else {
                None
            };
            {
                let mut st = state.borrow_mut();
                st.audit_on = true;
                if let Some(a) = saved {
                    st.audit = a;
                }
                render(&st);
            }
            loop {
                let mut a = state.borrow().audit.clone();
                let rep = audit::run_replicas(&replicas, &mut cur, &db, &cfg, &mut a, consumer::wall_ms() * 1_000_000).await;
                for f in rep.late.iter().chain(rep.unexplained.iter()) {
                    eprintln!("{}", f.log_line(cfg.horizon_ms));
                }
                for e in &rep.errors {
                    eprintln!("horizon-audit: error: {e}");
                }
                if persist {
                    let body = bytes::Bytes::from(serde_json::to_vec(&a).expect("state"));
                    if let Put::Unknown(e) = bucket.put(&state_key, body, Cond::None, &Default::default()).await {
                        eprintln!("horizon-audit: saving {state_key}: {e}");
                    }
                }
                {
                    let mut st = state.borrow_mut();
                    st.audit = a;
                    let m = &mut st.audit_m;
                    m.last_duration_s = Some(rep.duration_ms as f64 / 1e3);
                    m.last_candidates = rep.candidates as u64;
                    m.tables = rep.tables as u64;
                    if rep.errors.is_empty() {
                        m.runs_ok += 1;
                        m.last_success_s = Some(consumer::wall_ms() as f64 / 1e3);
                    } else {
                        m.runs_err += 1;
                    }
                    render(&st);
                }
                println!("{}", serde_json::json!({"horizon_audit": rep}));
                // `gc`: as long as GC runs (`--every`, `--run-for`); `horizon-audit`:
                // once, or every `--every` until `--run-for` (0: for ever).
                let stop = |now: u64| match sub {
                    Some("gc") => every.is_none() || now - t0 >= run_for,
                    _ => every.is_none() || (run_for > 0 && now - t0 >= run_for),
                };
                if stop(consumer::mono_ms()) {
                    break;
                }
                let left = (t0 + run_for).saturating_sub(consumer::mono_ms());
                tokio::time::sleep(Duration::from_millis(if run_for > 0 { audit_every.min(left) } else { audit_every })).await;
                if stop(consumer::mono_ms()) {
                    break;
                }
            }
        };
        tokio::join!(gc_loop, audit_loop);
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
    cfg.depth = arg(&args, "--depth").map_or(3, |d| d.parse().expect("--depth"));
    cfg.signals = arg(&args, "--signals").map(|s| s.split(',').map(str::to_string).collect()).unwrap_or_default();
    let mut table_override = None;
    if let Some(s) = &legacy_signal {
        cfg.signals = vec![s.clone()];
        table_override = arg(&args, "--table");
    }
    let allow_short = flag(&args, "--allow-short-margin");
    let d = Timing::production();
    let margin_ms = arg(&args, "--margin").map_or(d.margin_ms, |s| dur_ms(&s));
    cfg.timing = Timing {
        ttl_ms: arg(&args, "--ttl").map_or(d.ttl_ms, |s| dur_ms(&s)),
        margin_ms,
        budget_ms: arg(&args, "--budget").map_or(d.budget_ms, |s| dur_ms(&s)),
        slack_ms: arg(&args, "--keeper-slack").map_or(if allow_short { d.slack_ms.min(margin_ms) } else { d.slack_ms }, |s| dur_ms(&s)),
        mutation: coord::Mutation::None,
    };
    if let Err(e) = cfg.timing.check_production(allow_short) {
        eprintln!("consume: refusing to start: {e}");
        std::process::exit(2);
    }
    cfg.discover_ms = opt_ms(&args, "--discover", "2s");
    cfg.lanes_every_ms = opt_ms(&args, "--lanes-every", "30s");
    cfg.backoff = match arg(&args, "--idle-backoff").as_deref() {
        None => Backoff::default(),
        Some("off") => Backoff::off(),
        Some(r) => {
            let (lo, hi) = r.split_once("..").expect("--idle-backoff MIN..MAX");
            Backoff { min_ms: dur_ms(lo), max_ms: dur_ms(hi), ..Backoff::default() }
        }
    };
    cfg.backoff.after_ms = arg(&args, "--idle-after").map_or(cfg.backoff.after_ms, |s| dur_ms(&s));
    cfg.linger_ms = opt_ms(&args, "--linger", "0ms");
    cfg.balance.mode = match arg(&args, "--balance").as_deref() {
        None | Some("load") => BalanceMode::Load,
        Some("count") => BalanceMode::Count,
        Some(x) => panic!("--balance {x}: load or count"),
    };
    if let Some(h) = arg(&args, "--hysteresis") {
        cfg.balance.hysteresis = h.parse().expect("--hysteresis");
    }
    if let Some(w) = arg(&args, "--lane-weight") {
        cfg.balance.base_weight = w.parse().expect("--lane-weight");
    }
    cfg.balance.window_ms = opt_ms(&args, "--load-window", "60s");
    cfg.balance.min_hold_ms = opt_ms(&args, "--min-hold", "30s");
    cfg.balance.loads_every_ms = opt_ms(&args, "--loads-every", "10s");
    cfg.horizon_ms = horizon_ms;
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
    cfg.poll_ms = poll;
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
    let mut central = ClickHouseCentral::new(&ch_url, &db, bucket.clone(), "", "", cfg.timing.budget_ms + 5000);
    // Resolved before every statement: a temporary credential is passed
    // with its session token and is refreshed by the provider before it
    // expires (consumer::sql::ClickHouseCentral::s3_creds).
    central.s3_auth = ch_s3_auth;
    if cfg.timing.budget_ms >= 240_000 {
        eprintln!("consume: --budget of 4 min or more: a temporary credential handed to s3() may expire mid-statement (it is refreshed 5 min before expiry)");
    }
    central.squash = !flag(&args, "--no-squash");
    central.use_ranges = !flag(&args, "--no-check-range") && cfg.horizon_ms.is_some();
    // A replicated central (central-replicated/README.md): `--ch r1,r2` fails
    // over between replicas, `--sync-replica` syncs before a check that may
    // follow statements committed on another replica, `--no-ddl` leaves the
    // tables to the operator's replicated DDL, `--insert-setting k=v` adds
    // settings to every insert (e.g. insert_quorum=2).
    central.sync_replica = flag(&args, "--sync-replica");
    central.sync_timeout_ms = opt_ms(&args, "--sync-timeout", "5s");
    central.switch_hold_ms = arg(&args, "--switch-hold").map_or(cfg.timing.budget_ms + cfg.timing.slack_ms + 2_000, |s| dur_ms(&s));
    central.no_ddl = flag(&args, "--no-ddl");
    central.insert_settings = args
        .iter()
        .enumerate()
        .filter(|(_, a)| *a == "--insert-setting")
        .filter_map(|(i, _)| args.get(i + 1)?.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
        .collect();
    // A replicated central: a commit can land up to the Keeper session
    // timeout after its statement started (central-replicated/README.md,
    // "Keeper overrun"), so the slack must cover that minus the budget.
    if central.sync_replica || central.replicas.len() > 1 {
        match central.keeper_session_timeout_ms().await {
            Some(st) if cfg.timing.slack_ms + cfg.timing.budget_ms < st => {
                let msg = format!(
                    "--keeper-slack {} ms + --budget {} ms is below the replicas' Keeper session timeout {st} ms: \
                     a commit whose Keeper request hangs can land up to the session timeout after its statement \
                     started. Raise --keeper-slack (and --margin, --ttl), or lower zookeeper.session_timeout_ms",
                    cfg.timing.slack_ms, cfg.timing.budget_ms
                );
                if allow_short {
                    eprintln!("consume: warning (--allow-short-margin): {msg}");
                } else {
                    eprintln!("consume: {msg}");
                    std::process::exit(2);
                }
            }
            Some(_) => {}
            None => eprintln!("consume: warning: no replica told its Keeper session timeout; the slack is not checked against it"),
        }
    }
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
        let _ = m.insert("ch_syncs".into(), central.syncs.get().into());
        let _ = m.insert("ch_sync_errors".into(), central.sync_errors.get().into());
        let _ = m.insert("ch_switches".into(), central.switches.get().into());
        let _ = m.insert("ch_replica".into(), central.cur.get().into());
        let _ = m.insert("summary".into(), final_.into());
        v
    };
    let mut last_metrics = 0;
    let horizon = w.cfg.horizon_ms;
    let export = |v: &serde_json::Value| {
        let mut p = metrics::Prom::default();
        metrics::worker_families(&mut p, v, horizon);
        metrics::publish(&prom, p.render());
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
                write_atomic(p, &dump(&w, false).to_string());
                last_stats = now;
            }
        }
        if metrics_addr.is_some() && now >= last_metrics + 1000 {
            export(&dump(&w, false));
            last_metrics = now;
        }
        let done = (once && !progressed)
            || idle_exit.is_some_and(|d| now - last_progress >= d)
            || run_for.is_some_and(|d| now - t0 >= d);
        if done {
            break;
        }
        if !(once && progressed) {
            // Sleep out the poll, or less if a linger ends (or a lane's backoff) sooner.
            let spent = now - t;
            let mut sleep = poll.saturating_sub(spent);
            if let Some(w) = w.next_wake() {
                sleep = sleep.min(w.saturating_sub(now).max(5));
            }
            tokio::time::sleep(Duration::from_millis(sleep)).await;
        }
    }
    w.release_all().await;
    let v = dump(&w, true);
    if metrics_addr.is_some() {
        export(&v);
    }
    let s = v.to_string();
    if let Some(p) = &stats_path {
        write_atomic(p, &s);
    }
    println!("{s}");
}

#[cfg(test)]
mod creds_tests {
    use super::*;

    fn a(v: &[&str]) -> Vec<String> {
        std::iter::once("consume").chain(v.iter().copied()).map(str::to_string).collect()
    }

    #[test]
    fn credential_source_choice() {
        let none = |_: &str| None;
        let irsa = |k: &str| (k == "AWS_WEB_IDENTITY_TOKEN_FILE").then(|| "/var/run/token".to_string());
        let podid = |k: &str| (k == "AWS_CONTAINER_CREDENTIALS_FULL_URI").then(|| "http://169.254.170.23/v1/credentials".to_string());
        let local = "http://127.0.0.1:18333/otel/x";
        let aws = "s3://bucket/x";
        assert_eq!(
            choose_creds(&a(&["--key", "k", "--secret", "s", "--session-token", "t"]), &none, aws),
            Ok(Creds::Keys { key: "k".into(), secret: "s".into(), token: Some("t".into()) })
        );
        assert_eq!(choose_creds(&a(&["--key", "k", "--secret", "s"]), &irsa, aws), Ok(Creds::Keys { key: "k".into(), secret: "s".into(), token: None }));
        assert!(choose_creds(&a(&["--key", "k"]), &none, aws).is_err());
        assert!(choose_creds(&a(&["--session-token", "t"]), &none, aws).is_err());
        // Nothing named: the chain (IMDS at the end of it) off-host, the local keys on a local store.
        assert_eq!(choose_creds(&a(&[]), &none, aws), Ok(Creds::Chain));
        assert_eq!(choose_creds(&a(&[]), &none, "https://objects.example.com/b/x"), Ok(Creds::Chain));
        assert_eq!(choose_creds(&a(&[]), &none, local), Ok(Creds::DevDefault));
        assert_eq!(choose_creds(&a(&[]), &none, "http://localhost:8333/b/x"), Ok(Creds::DevDefault));
        // Named in the environment or by a flag: the chain, even on a local store.
        assert_eq!(choose_creds(&a(&[]), &irsa, local), Ok(Creds::Chain));
        assert_eq!(choose_creds(&a(&[]), &podid, aws), Ok(Creds::Chain));
        assert_eq!(choose_creds(&a(&["--profile", "p"]), &none, local), Ok(Creds::Chain));
        assert_eq!(choose_creds(&a(&["--role-arn", "arn:aws:iam::1:role/r"]), &none, local), Ok(Creds::Chain));
    }
}
