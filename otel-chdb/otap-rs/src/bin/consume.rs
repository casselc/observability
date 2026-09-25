//! The central consumer for the manifest-less layout: a port of
//! ../awss3/cmd/inlineconsume onto `proto::Consumer`, with the importer
//! rules of ../model/FASTPATH.md: a full count check against an aggregating
//! projection before insert, single-block inserts with the content key as
//! dedup token, and a batch-constant partition key.
//!
//! It follows every epoch's log in slot order, HEADs each slot (content key,
//! rows), checks central, inserts with s3(), and closes superseded epochs
//! whose head slot stays free for `--quiet` by racing a create-only
//! tombstone into it. Progress (checkpoints, closed epochs) goes to
//! `--state` after every step, standing in for S3NATIVE.md's CAS'd object.
//!
//!   consume --s3 http://127.0.0.1:18333/otel/otap-rs/edge --signal traces
//!           --ch http://127.0.0.1:18123 --table db.otel_traces [--once] [--poll 200ms] [--quiet 5s]
//!           [--state FILE] [--key K --secret S]

use otap_s3pq::Signal;
use otap_s3pq::central::{self, ClickHouse, ONE_BLOCK, sq};
use otap_s3pq::proto::{self, Consumer, Insert, Slot};
use otap_s3pq::runner::read_slot;
use otap_s3pq::store::{S3Config, SlotStore};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant, SystemTime};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn dur(s: &str) -> Duration {
    if let Some(ms) = s.strip_suffix("ms") {
        Duration::from_millis(ms.parse().unwrap())
    } else {
        Duration::from_secs_f64(s.trim_end_matches('s').parse().unwrap())
    }
}

fn save(path: &Option<String>, c: &Consumer) {
    if let Some(p) = path {
        let j = serde_json::json!({"ckpt": c.ckpt, "closed": c.closed});
        let tmp = format!("{p}.tmp");
        std::fs::write(&tmp, j.to_string()).unwrap();
        std::fs::rename(&tmp, p).unwrap();
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    otel_arrow_dfe_otap::crypto::install_crypto_provider().expect("crypto provider");
    let args: Vec<String> = std::env::args().collect();
    let signal = match arg(&args, "--signal").as_deref() {
        Some("logs") => Signal::Logs,
        _ => Signal::Traces,
    };
    let cfg = S3Config {
        url: arg(&args, "--s3").expect("--s3"),
        access_key_id: arg(&args, "--key").or(Some("otel".into())),
        secret_access_key: arg(&args, "--secret").or(Some("otelsecret".into())),
        ..Default::default()
    };
    let store = cfg.build().expect("s3");
    let prefix = format!("{}/{}", store.prefix, signal.name());
    let ch = ClickHouse::new(&arg(&args, "--ch").unwrap_or("http://127.0.0.1:18123".into()));
    let table = arg(&args, "--table").expect("--table");
    let once = args.iter().any(|a| a == "--once");
    let poll = dur(&arg(&args, "--poll").unwrap_or("1s".into()));
    let quiet = dur(&arg(&args, "--quiet").unwrap_or("5s".into()));
    let max_idle = arg(&args, "--exit-after-idle").map(|s| dur(&s));
    let state = arg(&args, "--state");
    let (key, secret) = (cfg.access_key_id.clone().unwrap(), cfg.secret_access_key.clone().unwrap());

    ch.query(&central::create_table(&table, signal), &[]).await.expect("create table");
    let mut c = Consumer::default();
    if let Some(p) = &state {
        if let Ok(s) = std::fs::read_to_string(p) {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            c.ckpt = serde_json::from_value(v["ckpt"].clone()).unwrap();
            c.closed = serde_json::from_value(v["closed"].clone()).unwrap();
        }
    }
    let mut last_seen: HashMap<String, Instant> = HashMap::new();
    let (mut inserted, mut dedup, mut repaired, mut rows_total, mut tombs, mut tomb_lost) = (0, 0, 0, 0u64, 0, 0);
    let mut lat_ms: Vec<f64> = Vec::new();
    let mut idle_since = Instant::now();
    let cols = central::cols(signal);
    let structure = central::structure(signal);
    loop {
        let epochs = store.list_dirs(&prefix).await.expect("list epochs");
        let mut progressed = false;
        for (i, e) in epochs.iter().enumerate() {
            let _ = last_seen.entry(e.clone()).or_insert_with(Instant::now);
            while !c.is_closed(e) {
                let s = c.next_slot(e);
                let k = proto::slot_key(&prefix, e, s);
                let head = store.head(&k).await.expect("head");
                let found = head.as_ref().map(Slot::from_meta).unwrap_or(Slot::Free);
                match &found {
                    Slot::Tomb => {
                        c.see_tomb(e);
                        save(&state, &c);
                        println!("closed {e} at slot {s} (tombstone)");
                    }
                    Slot::Data { content, .. } => {
                        let meta = head.as_ref().unwrap();
                        let rows: u64 = meta.get(proto::META_ROWS).and_then(|r| r.parse().ok()).unwrap_or(0);
                        let n: u64 = ch
                            .query(&format!("SELECT count() FROM {table} WHERE content_key = {}", sq(content)), &[])
                            .await
                            .expect("check")
                            .parse()
                            .unwrap();
                        let src = format!(
                            "s3({}, {}, {}, 'Parquet', {})",
                            sq(&store.object_url(&k)),
                            sq(&key),
                            sq(&secret),
                            sq(&structure)
                        );
                        if n > 0 && n < rows {
                            // Only possible if the batch spanned partitions (not with
                            // toDate(received_at)): insert the missing rows only.
                            let token = format!("{content}/repair");
                            let mut st: Vec<(&str, &str)> = ONE_BLOCK.to_vec();
                            st.push(("insert_deduplication_token", &token));
                            ch.query(&format!(
                                "INSERT INTO {table} ({cols}, content_key) SELECT {cols}, {} FROM {src} WHERE row_ordinal NOT IN (SELECT row_ordinal FROM {table} WHERE content_key = {})",
                                sq(content), sq(content)), &st).await.expect("repair");
                            repaired += 1;
                        }
                        c.check(e, content, n > 0);
                        match c.insert(&found) {
                            Insert::Rows { content } => {
                                let mut st: Vec<(&str, &str)> = ONE_BLOCK.to_vec();
                                st.push(("insert_deduplication_token", &content));
                                ch.query(
                                    &format!("INSERT INTO {table} ({cols}, content_key) SELECT {cols}, {} FROM {src}", sq(&content)),
                                    &st,
                                )
                                .await
                                .expect("insert");
                                inserted += 1;
                                rows_total += rows;
                                let recv: u64 = meta.get(proto::META_RECEIVED).and_then(|r| r.parse().ok()).unwrap_or(0);
                                let l = (now_ns().saturating_sub(recv)) as f64 / 1e6;
                                lat_ms.push(l);
                                println!("ingest {e}/{s} content={content} rows={rows} visible_after_ms={l:.1}");
                            }
                            Insert::DedupHit => {
                                dedup += 1;
                                println!("skip   {e}/{s} content={content}: already in central (a copy from another epoch)");
                            }
                            Insert::Replaced => println!("slot {e}/{s} no longer holds data"),
                        }
                        c.advance();
                        save(&state, &c);
                        let _ = last_seen.insert(e.clone(), Instant::now());
                        progressed = true;
                        continue;
                    }
                    Slot::Free => {
                        let superseded = i + 1 < epochs.len();
                        if !superseded || (!once && last_seen[e].elapsed() < quiet) {
                            break;
                        }
                        // Race a create-only tombstone into the free head slot.
                        let _ = c.tomb(e);
                        let mut meta = BTreeMap::new();
                        let _ = meta.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
                        let _ = meta.insert(proto::META_EPOCH.to_string(), e.clone());
                        loop {
                            let o = store.put_create(&k, bytes::Bytes::new(), "application/octet-stream", &meta).await;
                            match c.on_tomb_put(o) {
                                Some(_) => break,
                                None => {
                                    let now = read_slot(&store, &k).await.expect("head tombstone slot");
                                    if !c.on_tomb_head(&now) {
                                        break;
                                    }
                                }
                            }
                        }
                        if c.is_closed(e) {
                            tombs += 1;
                            println!("closed {e} at slot {s} (our tombstone)");
                        } else {
                            tomb_lost += 1;
                            println!("tombstone {e}/{s} lost to a late batch: ingesting it");
                        }
                        save(&state, &c);
                    }
                }
            }
        }
        if progressed {
            idle_since = Instant::now();
        }
        if once || max_idle.is_some_and(|d| idle_since.elapsed() >= d) {
            break;
        }
        tokio::time::sleep(poll).await;
    }
    let total = ch
        .query(&format!("SELECT count(), uniqExact(content_key) FROM {table} FORMAT TSV"), &[])
        .await
        .unwrap();
    lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| lat_ms.get(((lat_ms.len() as f64 - 1.0) * p).round() as usize).copied().unwrap_or(0.0);
    println!(
        "{}",
        serde_json::json!({"summary": true, "inserted": inserted, "rows": rows_total, "dedup_skipped": dedup,
            "repaired": repaired, "epochs_closed_by_tombstone": tombs, "tombstones_lost_to_data": tomb_lost,
            "central_count_distinct_content": total.replace('\t', " "),
            "visible_after_ms_p50": pct(0.5), "visible_after_ms_p90": pct(0.9), "visible_after_ms_max": pct(1.0)})
    );
}
