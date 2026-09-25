//! The I/O around the protocol cores in `proto.rs`: appending a batch to a
//! lane's log, and (for the consumer) reading slots. Timeouts turn into
//! `PutOutcome::Unknown`, which the lane resolves with a HEAD.

use crate::proto::{self, Lane, PutOutcome, Ref, Slot, Step};
use crate::store::SlotStore;
use bytes::Bytes;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::Duration;

/// One encoded batch, for one slot.
#[derive(Clone, Debug)]
pub struct Encoded {
    pub body: Bytes,
    pub content_type: &'static str,
    /// Metadata the log doesn't set itself (rows, times, ...).
    pub meta: BTreeMap<String, String>,
}

/// Counters, one per model event.
#[derive(Default, Debug)]
pub struct Stats {
    pub committed: Cell<u64>,
    pub resolved_own: Cell<u64>,
    pub resent: Cell<u64>,
    pub learned_other: Cell<u64>,
    pub halted: Cell<u64>,
    pub known_skipped: Cell<u64>,
    pub encodes: Cell<u64>,
    pub puts: Cell<u64>,
    pub heads: Cell<u64>,
}

fn inc(c: &Cell<u64>) {
    c.set(c.get() + 1);
}

#[derive(Debug)]
pub enum AppendError {
    /// The outcome is not known yet; the lane holds the slot, and the next
    /// append (a retry, or another batch) resolves it first.
    Unresolved(String),
    /// The batch could not be encoded (permanent).
    Encode(String),
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::Unresolved(s) => write!(f, "commit unresolved: {s}"),
            AppendError::Encode(s) => write!(f, "encode: {s}"),
        }
    }
}

/// The last encoding of a batch, kept so that a resend, or a retry of the
/// same batch into the same slot, sends identical bytes.
#[derive(Default)]
pub struct EncodedCache {
    key: Option<(String, Ref)>,
    obj: Option<Encoded>,
}

pub struct Timeouts {
    pub put: Duration,
    pub head: Duration,
}

/// Commits the batch with content hash `content` at the next free slot of
/// the lane's log: the loop of `../awss3/inline/log.go`'s `Log.Append`,
/// driven by `Lane`. `encode(epoch, seq)` is called only when the batch
/// needs bytes for a slot it has no bytes for.
#[allow(clippy::too_many_arguments)]
pub async fn append<S: SlotStore>(
    lane: &mut Lane,
    cache: &mut EncodedCache,
    store: &S,
    prefix: &str,
    producer: &str,
    content: &str,
    encode: &mut dyn FnMut(&Ref) -> Result<Encoded, String>,
    t: &Timeouts,
    stats: &Stats,
) -> Result<Ref, AppendError> {
    if let Some(r) = lane.start(content) {
        inc(&stats.known_skipped);
        return Ok(r);
    }
    loop {
        let here = lane.slot();
        let want = (content.to_string(), here.clone());
        if cache.key.as_ref() != Some(&want) {
            let obj = encode(&here).map_err(AppendError::Encode)?;
            inc(&stats.encodes);
            cache.key = Some(want);
            cache.obj = Some(obj);
        }
        let obj = cache.obj.as_ref().expect("encoded above");
        let mut meta = obj.meta.clone();
        for (k, v) in [
            (proto::META_KIND, proto::KIND_DATA.to_string()),
            (proto::META_EPOCH, here.epoch.clone()),
            (proto::META_SEQ, here.seq.to_string()),
            (proto::META_CONTENT, content.to_string()),
            (proto::META_PRODUCER, producer.to_string()),
        ] {
            let _ = meta.insert(k.to_string(), v);
        }
        let key = proto::slot_key(prefix, &here.epoch, here.seq);
        lane.sent();
        inc(&stats.puts);
        let o = match tokio::time::timeout(
            t.put,
            store.put_create(&key, obj.body.clone(), obj.content_type, &meta),
        )
        .await
        {
            Ok(o) => o,
            Err(_) => PutOutcome::Unknown,
        };
        match lane.on_put(o) {
            Step::Committed { at, .. } => {
                inc(&stats.committed);
                return Ok(at);
            }
            Step::Put => continue,
            Step::Head => {}
            s => unreachable!("on_put gave {s:?}"),
        }
        // 412, or no answer: read the slot. With no answer the request may
        // still be in flight; If-None-Match lets at most one copy land.
        inc(&stats.heads);
        let found = match tokio::time::timeout(t.head, store.head(&key)).await {
            Ok(Ok(Some(m))) => Slot::from_meta(&m),
            Ok(Ok(None)) => Slot::Free,
            Ok(Err(e)) => {
                lane.phase = proto::Phase::Unresolved;
                return Err(AppendError::Unresolved(format!("put {key}: {o:?}; head: {e}")));
            }
            Err(_) => {
                lane.phase = proto::Phase::Unresolved;
                return Err(AppendError::Unresolved(format!("put {key}: {o:?}; head timed out")));
            }
        };
        match lane.on_head(&found) {
            Step::Committed { at, .. } => {
                inc(&stats.resolved_own);
                return Ok(at);
            }
            Step::Put => inc(&stats.resent),
            Step::LearnedOther { .. } => inc(&stats.learned_other),
            Step::Halted => {
                // The consumer closed this log. A new epoch, at slot 0.
                inc(&stats.halted);
                lane.reincarnate(proto::new_epoch());
            }
            Step::Inconsistent => {
                return Err(AppendError::Unresolved(format!(
                    "put {key}: 412 but HEAD finds no object (store not read-after-write consistent?)"
                )));
            }
            Step::Head => unreachable!(),
        }
    }
}

/// Reads a slot the way the consumer and the lane do.
pub async fn read_slot<S: SlotStore>(store: &S, key: &str) -> Result<Slot, crate::store::StoreError> {
    Ok(match store.head(key).await? {
        Some(m) => Slot::from_meta(&m),
        None => Slot::Free,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Fault, MemStore};

    fn enc(r: &Ref) -> Result<Encoded, String> {
        Ok(Encoded {
            body: Bytes::from(format!("{}/{}", r.epoch, r.seq)),
            content_type: "application/octet-stream",
            meta: BTreeMap::new(),
        })
    }

    fn t() -> Timeouts {
        Timeouts { put: Duration::from_secs(1), head: Duration::from_secs(1) }
    }

    async fn push(l: &mut Lane, c: &mut EncodedCache, s: &MemStore, content: &str, st: &Stats) -> Result<Ref, AppendError> {
        append(l, c, s, "p", "prod", content, &mut enc, &t(), st).await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn faults_commit_once_without_gaps() {
        let s = MemStore::default();
        let st = Stats::default();
        let mut l = Lane::new("E".into());
        let mut c = EncodedCache::default();
        // 1. clean
        assert_eq!(push(&mut l, &mut c, &s, "a", &st).await.unwrap().seq, 0);
        // 2. applied, answer lost: resolved as ours by HEAD
        s.inject(Fault::ApplyLoseAnswer);
        assert_eq!(push(&mut l, &mut c, &s, "b", &st).await.unwrap().seq, 1);
        // 3. dropped: HEAD finds it free, resend
        s.inject(Fault::Drop);
        assert_eq!(push(&mut l, &mut c, &s, "c", &st).await.unwrap().seq, 2);
        // 4. held (late): the resend wins, the late copy gets 412 when released
        s.inject(Fault::Hold);
        assert_eq!(push(&mut l, &mut c, &s, "d", &st).await.unwrap().seq, 3);
        assert_eq!(s.release_held(), 0);
        // 5. a retry of a committed batch: no request at all
        let puts = s.0.borrow().puts;
        assert_eq!(push(&mut l, &mut c, &s, "b", &st).await.unwrap().seq, 1);
        assert_eq!(s.0.borrow().puts, puts);
        assert_eq!(s.0.borrow().objects.len(), 4);
        assert_eq!((st.resolved_own.get(), st.resent.get(), st.known_skipped.get()), (1, 2, 1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tombstone_halts_into_new_epoch() {
        let s = MemStore::default();
        let st = Stats::default();
        let mut l = Lane::new("E".into());
        let mut c = EncodedCache::default();
        push(&mut l, &mut c, &s, "a", &st).await.unwrap();
        // the consumer closes E at slot 1
        let mut tomb = BTreeMap::new();
        let _ = tomb.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
        assert_eq!(s.put_create(&proto::slot_key("p", "E", 1), Bytes::new(), "", &tomb).await, PutOutcome::Ok);
        let r = push(&mut l, &mut c, &s, "b", &st).await.unwrap();
        assert_ne!(r.epoch, "E");
        assert_eq!(r.seq, 0);
        assert_eq!(st.halted.get(), 1);
    }
}
