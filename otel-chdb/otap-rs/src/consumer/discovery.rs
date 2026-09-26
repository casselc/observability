//! Discovery cost at fleet scale, sans-IO: when a held lane is LISTed, and
//! the seam for event-driven hints.
//!
//! **Idle-lane backoff.** A lane whose LISTs have found nothing to do for
//! `after_ms` is LISTed again after a wait that doubles from `min_ms` up to
//! `max_ms` (jittered, so a fleet of idle lanes doesn't LIST in step), and
//! drops back to every poll as soon as a LIST finds work. (The grace keeps a
//! lane with an object every few seconds on every poll: backing it off
//! would cost it latency and statements their batching.) The periodic full listing from the
//! floor (`--full-list`) happens at the lane's next LIST once due, so an
//! idle lane costs one LIST per backoff period, whatever the poll.
//!
//! **Hints** (event-driven discovery). An S3 event notification
//! (`s3:ObjectCreated:*` to SQS, or EventBridge) names a new key; the
//! worker maps it to a lane and LISTs that lane at its next poll instead of
//! waiting out the backoff. A hint is never trusted for what it says about
//! the slot: the LIST from the checkpoint and the HEAD of each slot stay the
//! only source of truth, and the backoff's `max_ms` is the reconciliation
//! period. So a hint that is lost, late, duplicated, reordered or spurious
//! changes only *when* a lane is listed, never *what* is ingested:
//! correctness does not depend on notifications arriving (the tests run
//! with hints dropped, and with only spurious ones).
//!
//! What is built: the `Hints` trait, the key → lane mapping, the parsing of
//! S3 and EventBridge event bodies, and an in-memory source for tests. What
//! is not: an SQS client (see the README's "Event-driven discovery").

use async_trait::async_trait;
use std::cell::RefCell;

/// Per-lane LIST backoff after LISTs that found nothing to do.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Backoff {
    /// How long a lane must have been idle before it backs off.
    pub after_ms: u64,
    /// The first wait after an idle LIST (0 with `max_ms` 0: off, LIST every poll).
    pub min_ms: u64,
    /// The longest wait: an idle lane is LISTed at least this often (the
    /// reconciliation period when hints are on).
    pub max_ms: u64,
    /// Spread each wait over ±jitter/2 of it (0..1).
    pub jitter: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff { after_ms: 10_000, min_ms: 1_000, max_ms: 30_000, jitter: 0.2 }
    }
}

impl Backoff {
    pub fn off() -> Self {
        Backoff { after_ms: 0, min_ms: 0, max_ms: 0, jitter: 0.0 }
    }

    pub fn enabled(&self) -> bool {
        self.max_ms > 0
    }

    /// The next (unjittered) wait after an idle LIST, given the previous one
    /// (0: the lane was busy).
    pub fn next_base(&self, prev_ms: u64) -> u64 {
        if !self.enabled() {
            return 0;
        }
        if prev_ms == 0 { self.min_ms.min(self.max_ms) } else { prev_ms.saturating_mul(2).clamp(self.min_ms, self.max_ms) }
    }

    /// A wait jittered by `r` in [0, 1): base × (1 − jitter/2 + jitter × r).
    pub fn jittered(&self, base_ms: u64, r: f64) -> u64 {
        let f = 1.0 - self.jitter / 2.0 + self.jitter * r.clamp(0.0, 1.0);
        (base_ms as f64 * f).round() as u64
    }
}

/// A small deterministic PRNG for jitter (xorshift64*), seeded per worker.
#[derive(Clone, Debug)]
pub struct Jitter(u64);

impl Jitter {
    pub fn new(seed: &str) -> Self {
        let h = blake3::hash(seed.as_bytes());
        let mut b = [0u8; 8];
        b.copy_from_slice(&h.as_bytes()[..8]);
        Jitter(u64::from_le_bytes(b) | 1)
    }
    /// Uniform in [0, 1).
    pub fn next(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Where a worker learns that a lane may have new objects.
#[async_trait(?Send)]
pub trait Hints {
    /// Lane ids (`producer/signal`, or `signal` at depth 1) that may have new
    /// objects since the last call. Best effort in every way.
    async fn poll(&self) -> Vec<String>;
}

/// Hints pushed by hand (tests; or a bridge that feeds decoded events in).
#[derive(Default)]
pub struct MemHints {
    pub queue: RefCell<Vec<String>>,
}

impl MemHints {
    pub fn push(&self, lane: &str) {
        self.queue.borrow_mut().push(lane.to_string());
    }
}

#[async_trait(?Send)]
impl Hints for MemHints {
    async fn poll(&self) -> Vec<String> {
        std::mem::take(&mut *self.queue.borrow_mut())
    }
}

/// The lane of an object key under `root`, if the key is a slot key
/// (`{root}/{producer}/{signal}/{epoch}/{seq:020d}.parquet`, or without the
/// producer at depth 1). Control keys (`_consumer/…`) are not lanes.
pub fn lane_of_key(root: &str, depth: usize, key: &str) -> Option<String> {
    let root = root.trim_matches('/');
    let rest = if root.is_empty() { key } else { key.strip_prefix(root)?.strip_prefix('/')? };
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != depth + 2 || parts.iter().any(|p| p.is_empty() || p.starts_with('_')) {
        return None;
    }
    let file = parts[depth + 1];
    let seq = file.strip_suffix(".parquet").unwrap_or(file);
    if seq.is_empty() || !seq.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(parts[..depth].join("/"))
}

/// Object keys named by an event body: an S3 notification (`Records[].s3.object.key`,
/// URL-encoded, as SQS delivers it, possibly wrapped in an SNS envelope's
/// `Message`) or an EventBridge event (`detail.object.key`, not encoded).
pub fn keys_of_event(body: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else { return Vec::new() };
    if let Some(msg) = v.get("Message").and_then(|m| m.as_str()) {
        return keys_of_event(msg);
    }
    let mut out = Vec::new();
    if let Some(recs) = v.get("Records").and_then(|r| r.as_array()) {
        for r in recs {
            if let Some(k) = r.pointer("/s3/object/key").and_then(|k| k.as_str()) {
                out.push(url_decode(k));
            }
        }
    }
    if let Some(k) = v.pointer("/detail/object/key").and_then(|k| k.as_str()) {
        out.push(k.to_string());
    }
    out
}

/// S3 event keys are form-encoded: `+` is a space, `%XX` a byte.
fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                match u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("zz"), 16) {
                    Ok(x) => {
                        out.push(x);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_the_cap_and_jitters() {
        let b = Backoff { after_ms: 0, min_ms: 1000, max_ms: 30_000, jitter: 0.2 };
        let mut w = 0;
        let mut seen = Vec::new();
        for _ in 0..8 {
            w = b.next_base(w);
            seen.push(w);
        }
        assert_eq!(seen, vec![1000, 2000, 4000, 8000, 16_000, 30_000, 30_000, 30_000]);
        assert_eq!(b.jittered(10_000, 0.0), 9000);
        assert_eq!(b.jittered(10_000, 0.5), 10_000);
        assert!(b.jittered(10_000, 0.999) <= 11_000);
        assert_eq!(Backoff::off().next_base(5000), 0);
        let mut j = Jitter::new("w1");
        let xs: Vec<f64> = (0..1000).map(|_| j.next()).collect();
        assert!(xs.iter().all(|x| (0.0..1.0).contains(x)));
        let mean = xs.iter().sum::<f64>() / xs.len() as f64;
        assert!((0.45..0.55).contains(&mean), "{mean}");
    }

    #[test]
    fn keys_map_to_lanes() {
        let k = "r/edges/edge-1/traces/1727000000000Z-ab12/00000000000000000042.parquet";
        assert_eq!(lane_of_key("r/edges", 2, k).as_deref(), Some("edge-1/traces"));
        assert_eq!(lane_of_key("r/edges/", 2, k).as_deref(), Some("edge-1/traces"));
        assert_eq!(lane_of_key("r/edges/edge-1", 1, k).as_deref(), Some("traces"));
        assert_eq!(lane_of_key("r/edges", 2, "r/edges/_consumer/ckpt/edge-1/traces.json"), None);
        assert_eq!(lane_of_key("r/edges", 2, "r/other/edge-1/traces/E/00000000000000000001.parquet"), None);
        assert_eq!(lane_of_key("r/edges", 2, "r/edges/edge-1/traces/E"), None);
        // S3 notification (SQS body), SNS-wrapped, and EventBridge
        let s3 = r#"{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"r/edges/edge%2D1/logs/E1/00000000000000000000.parquet","size":10}}}]}"#;
        assert_eq!(keys_of_event(s3), vec!["r/edges/edge-1/logs/E1/00000000000000000000.parquet"]);
        let sns = serde_json::json!({"Type": "Notification", "Message": s3}).to_string();
        assert_eq!(keys_of_event(&sns).len(), 1);
        let eb = r#"{"detail-type":"Object Created","detail":{"bucket":{"name":"b"},"object":{"key":"r/edges/e/traces/E/00000000000000000001.parquet"}}}"#;
        assert_eq!(lane_of_key("r/edges", 2, &keys_of_event(eb)[0]).as_deref(), Some("e/traces"));
        assert!(keys_of_event("not json").is_empty());
        assert_eq!(url_decode("a+b%2Fc%zz"), "a b/c%zz");
    }
}
