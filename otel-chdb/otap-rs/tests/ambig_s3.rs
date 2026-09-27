//! A store that applies a PUT and answers an error anyway (AMBIGUITY.md,
//! audit a). `tools/cmd/faultproxy2 -mode commit-error` forwards the first
//! matching PUT to SeaweedFS and answers 500, 503 or 409 after it applied.
//!
//! - create-only (the exporter's slot PUT): object_store retries a 5xx on its
//!   own, and its retry meets our own object: 412. The lane must HEAD the
//!   slot and find its own content key and epoch, not "another batch".
//! - If-Match (the consumer's leases and checkpoints): the retry carries the
//!   old ETag and meets our own new one: 412, reported as `Precondition`.
//!   What that means is up to the caller (`consumer/worker.rs`
//!   `write_lease` / `write_ckpt` read the object back).
//!
//! Needs SeaweedFS (`AMBIG_S3`, default `http://127.0.0.1:18333/otel`) and
//! the faultproxy2 binary (`FAULTPROXY2`); skipped without `FAULTPROXY2`.
//! `cargo test --release --test ambig_s3 -- --nocapture --test-threads 1`

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use otap_s3pq::proto::Lane;
use otap_s3pq::runner::{self, EncodedCache, Encoded, Stats, Timeouts};
use otap_s3pq::store::{S3Config, SlotStore};
use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct Proxy {
    child: Child,
    addr: String,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn target() -> (String, String) {
    let u = std::env::var("AMBIG_S3").unwrap_or_else(|_| "http://127.0.0.1:18333/otel".into());
    let (ep, b) = u.rsplit_once('/').expect("AMBIG_S3: http://host:port/bucket");
    (ep.to_string(), b.to_string())
}

fn proxy(matching: &str, status: u16) -> Option<Proxy> {
    let bin = std::env::var("FAULTPROXY2").ok()?;
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    drop(l);
    let (ep, _) = target();
    let p = Proxy {
        child: Command::new(bin)
            .args(["-listen", &addr, "-target", &ep, "-limit", "1", "-mode", "commit-error"])
            .args(["-status", &status.to_string(), "-match", matching])
            .stderr(Stdio::inherit())
            .spawn()
            .expect("faultproxy2"),
        addr,
    };
    for _ in 0..50 {
        if std::net::TcpStream::connect(&p.addr).is_ok() {
            return Some(p);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("faultproxy2 did not start"); // (Drop kills and waits)
}

fn cfg(endpoint: &str, bucket: &str, prefix: &str) -> S3Config {
    S3Config {
        url: format!("{endpoint}/{bucket}/{prefix}"),
        access_key_id: Some("otel".into()),
        secret_access_key: Some("otelsecret".into()),
        ..Default::default()
    }
}

fn run_id(what: &str) -> String {
    format!("am-rs-{what}-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
}

#[tokio::test(flavor = "current_thread")]
async fn create_only_put_answered_an_error_after_it_applied_is_resolved_as_ours() {
    let (ep, bucket) = target();
    for status in [500u16, 503, 409] {
        let run = run_id(&format!("create-{status}"));
        let Some(p) = proxy(&run, status) else {
            eprintln!("FAULTPROXY2 not set: skipped");
            return;
        };
        let store = cfg(&format!("http://{}", p.addr), &bucket, &run).build().unwrap();
        let direct = cfg(&ep, &bucket, &run).build().unwrap();
        let mut lane = Lane::new(String::new());
        let mut cache = EncodedCache::default();
        let stats = Stats::default();
        let t = Timeouts { put: Duration::from_secs(5), head: Duration::from_secs(2) };
        let mut enc = |_: &otap_s3pq::proto::Ref| -> Result<Encoded, String> {
            Ok(Encoded { body: Bytes::from_static(b"batch-1"), content_type: "application/octet-stream", meta: BTreeMap::new() })
        };
        let prefix = format!("{run}/traces");
        let at = runner::append(&mut lane, &mut cache, &store, &prefix, "am", "content-1", &mut enc, &t, &stats)
            .await
            .unwrap_or_else(|e| panic!("status {status}: {e}"));
        eprintln!(
            "status {status}: committed at {at:?}; puts {} heads {} resolved_own {} learned_other {} resent {}",
            stats.puts.get(),
            stats.heads.get(),
            stats.resolved_own.get(),
            stats.learned_other.get(),
            stats.resent.get()
        );
        assert_eq!(at.seq, 0, "status {status}: our own write was taken for another writer's");
        assert_eq!(stats.learned_other.get(), 0);
        assert_eq!(stats.resolved_own.get(), 1, "status {status}: resolved by HEAD as ours");
        let keys = direct.list_after(&format!("{run}/"), None).await.unwrap();
        assert_eq!(keys.len(), 1, "status {status}: {keys:?}");
        for k in keys {
            direct.store.delete(&Path::from(k)).await.unwrap();
        }
    }
}

/// What object_store hands the consumer when an If-Match PUT applied and the
/// answer was an error: its retry sends the old ETag and gets 412 for our own
/// write (500/503 are retried; 409 too, since object_store retries a 409 on
/// If-Match). Records the behaviour the consumer's read-back must cover.
#[tokio::test(flavor = "current_thread")]
async fn if_match_put_answered_an_error_after_it_applied_reports_a_precondition_failure() {
    let (ep, bucket) = target();
    for status in [500u16, 503, 409] {
        let run = run_id(&format!("cas-{status}"));
        let direct = cfg(&ep, &bucket, &run).build().unwrap();
        let key = Path::from(format!("{run}/lease.json"));
        let r0 = direct.store.put(&key, PutPayload::from_static(b"v0")).await.unwrap();
        let Some(p) = proxy(&run, status) else {
            eprintln!("FAULTPROXY2 not set: skipped");
            return;
        };
        let store = cfg(&format!("http://{}", p.addr), &bucket, &run).build().unwrap();
        let opts = PutOptions {
            mode: PutMode::Update(UpdateVersion { e_tag: r0.e_tag.clone(), version: None }),
            ..Default::default()
        };
        let r = store.store.put_opts(&key, PutPayload::from_static(b"v1"), opts).await;
        let now = direct.store.get(&key).await.unwrap().bytes().await.unwrap();
        eprintln!("status {status}: put_opts(If-Match) -> {r:?}; the object now holds {now:?}");
        assert_eq!(now, Bytes::from_static(b"v1"), "the first attempt applied");
        assert!(
            matches!(r, Err(object_store::Error::Precondition { .. })),
            "status {status}: expected a 412 for our own write, got {r:?}"
        );
        direct.store.delete(&key).await.unwrap();
    }
}
