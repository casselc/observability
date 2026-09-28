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
    /// The HEAD that should resolve a slot failed or timed out.
    pub unresolved: Cell<u64>,
    /// 412, then the HEAD found the slot free.
    pub inconsistent: Cell<u64>,
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

/// Resends one `append` makes (a PUT without an answer, then a HEAD that
/// finds the slot free) before it returns `Unresolved`, the lane staying at
/// the slot: without it a store that fails every PUT spins on the slot
/// forever, holding the lane and its heartbeats. parquetgo `commit.MaxResends`.
pub const MAX_RESENDS: u32 = 8;

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
    let mut resends = 0u32;
    loop {
        // A lane made without an epoch names it now, at its first write, not
        // when the lane was made: an idle lane's first slot must not land
        // under a name older than the epochs the consumer has since closed
        // and compacted past (src/consumer/coord.rs, `CkptDoc::compact`).
        if lane.epoch.is_empty() {
            lane.epoch = proto::new_epoch();
        }
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
        // A heartbeat's encoder says so (`KIND_BEAT`); anything else is data.
        let _ = meta.entry(proto::META_KIND.to_string()).or_insert_with(|| proto::KIND_DATA.to_string());
        for (k, v) in [
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
                inc(&stats.unresolved);
                lane.phase = proto::Phase::Unresolved;
                return Err(AppendError::Unresolved(format!("put {key}: {o:?}; head: {e}")));
            }
            Err(_) => {
                inc(&stats.unresolved);
                lane.phase = proto::Phase::Unresolved;
                return Err(AppendError::Unresolved(format!("put {key}: {o:?}; head timed out")));
            }
        };
        match lane.on_head(&found) {
            Step::Committed { at, .. } => {
                inc(&stats.resolved_own);
                return Ok(at);
            }
            Step::Put if resends >= MAX_RESENDS => {
                // Not sent again by this call: the PUT may still land, so
                // the slot stays unresolved (the next append starts there).
                inc(&stats.unresolved);
                lane.phase = proto::Phase::Unresolved;
                return Err(AppendError::Unresolved(format!("put {key}: {o:?}, and {resends} resends after HEADs found the slot free")));
            }
            Step::Put => {
                resends += 1;
                inc(&stats.resent);
            }
            Step::LearnedOther { .. } => inc(&stats.learned_other),
            Step::Halted => {
                // The consumer closed this log. A new epoch, at slot 0.
                inc(&stats.halted);
                lane.reincarnate(proto::new_epoch());
            }
            Step::Inconsistent => {
                inc(&stats.inconsistent);
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
    async fn an_unnamed_lane_names_its_epoch_at_its_first_write() {
        let s = MemStore::default();
        let st = Stats::default();
        let mut l = Lane::new(String::new());
        let mut c = EncodedCache::default();
        let before = proto::new_epoch();
        let r = push(&mut l, &mut c, &s, "a", &st).await.unwrap();
        assert_eq!(r.epoch.len(), before.len(), "a minted name: {}", r.epoch);
        assert!(r.epoch[..21] >= before[..21], "named at the write, not before");
        assert_eq!(push(&mut l, &mut c, &s, "b", &st).await.unwrap(), Ref { epoch: r.epoch.clone(), seq: 1 });
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

    /// A store whose PUT always answers `put` and whose HEAD answers `head`.
    struct Fixed {
        put: PutOutcome,
        head_fails: bool,
    }

    #[async_trait::async_trait(?Send)]
    impl SlotStore for Fixed {
        async fn put_create(&self, _: &str, _: Bytes, _: &str, _: &BTreeMap<String, String>) -> PutOutcome {
            self.put
        }
        async fn head(&self, _: &str) -> Result<Option<crate::store::Meta>, crate::store::StoreError> {
            if self.head_fails { Err(crate::store::StoreError("no answer".into())) } else { Ok(None) }
        }
        async fn list_dirs(&self, _: &str) -> Result<Vec<String>, crate::store::StoreError> {
            Ok(Vec::new())
        }
        async fn list_after(&self, _: &str, _: Option<&str>) -> Result<Vec<String>, crate::store::StoreError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unresolved_and_inconsistent_are_counted() {
        let st = Stats::default();
        let mut c = EncodedCache::default();
        // no answer, then the HEAD fails: unresolved
        let mut l = Lane::new("E".into());
        let s = Fixed { put: PutOutcome::Unknown, head_fails: true };
        assert!(matches!(append(&mut l, &mut c, &s, "p", "prod", "a", &mut enc, &t(), &st).await, Err(AppendError::Unresolved(_))));
        // 412, then the HEAD finds the slot free: inconsistent
        let mut l = Lane::new("E".into());
        let s = Fixed { put: PutOutcome::Exists, head_fails: false };
        assert!(matches!(append(&mut l, &mut c, &s, "p", "prod", "a", &mut enc, &t(), &st).await, Err(AppendError::Unresolved(_))));
        assert_eq!((st.unresolved.get(), st.inconsistent.get(), st.committed.get()), (1, 1, 0));
        assert_eq!(crate::commit_metrics::totals(&st), [0, 0, 0, 0, 0, 0, 1, 1]);
    }

    /// A store that fails every PUT, whose HEADs find the slot free: the
    /// append gives up after `MAX_RESENDS` resends instead of spinning on
    /// the slot while holding the lane, and the next append commits in that
    /// slot. parquetgo `commit.TestAppendResendLimit` (found in the D31
    /// integration run).
    #[tokio::test(flavor = "current_thread")]
    async fn a_store_that_fails_every_put_is_not_resent_forever() {
        let st = Stats::default();
        let mut c = EncodedCache::default();
        let mut l = Lane::new("E".into());
        let s = Fixed { put: PutOutcome::Unknown, head_fails: false };
        assert!(matches!(append(&mut l, &mut c, &s, "p", "prod", "a", &mut enc, &t(), &st).await, Err(AppendError::Unresolved(_))));
        assert_eq!((st.puts.get(), st.resent.get(), st.unresolved.get()), (u64::from(MAX_RESENDS) + 1, u64::from(MAX_RESENDS), 1));
        let m = MemStore::default();
        assert_eq!(append(&mut l, &mut c, &m, "p", "prod", "a", &mut enc, &t(), &st).await.unwrap(), Ref { epoch: "E".into(), seq: 0 });
    }
}
