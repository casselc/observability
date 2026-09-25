//! The manifest-less commit protocol (`../awss3/README.md`, `../model/s3Inline.qnt`),
//! as sans-IO state machines: a writer lane and the central consumer. They
//! take outcomes (a PUT's answer, what a HEAD found, whether central already
//! holds a content key) and say what to do next; `runner.rs` does the I/O.
//! Keeping the I/O out is what lets the model-based test drive every step of
//! the model, including the ones a real network makes hard to hit.
//!
//! Layout: `{prefix}/{epoch}/{seq:020d}.parquet`. A slot holds a data object
//! (the batch; its description in `x-amz-meta-oscope-*` and in the Parquet
//! footer) or a zero-byte tombstone the consumer wrote. Every write is
//! `PUT If-None-Match: *`.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

// Metadata keys: S3 user metadata `x-amz-meta-<key>`, and the same keys in
// the Parquet footer. Identical to ../awss3/inline/format.go.
pub const META_KIND: &str = "oscope-kind";
pub const META_PRODUCER: &str = "oscope-producer";
pub const META_EPOCH: &str = "oscope-epoch";
pub const META_SEQ: &str = "oscope-seq";
pub const META_CONTENT: &str = "oscope-content";
pub const META_SIGNAL: &str = "oscope-signal";
pub const META_SCHEMA: &str = "oscope-schema";
pub const META_ROWS: &str = "oscope-rows";
pub const META_MIN_TIME: &str = "oscope-min-time";
pub const META_MAX_TIME: &str = "oscope-max-time";
pub const META_RECEIVED: &str = "oscope-received";
pub const KIND_DATA: &str = "data";
pub const KIND_TOMB: &str = "tomb";

/// The key of slot `seq` in `epoch`'s log. Zero padding makes LIST order slot order.
pub fn slot_key(prefix: &str, epoch: &str, seq: u64) -> String {
    format!("{}/{}/{:020}.parquet", prefix.trim_end_matches('/'), epoch, seq)
}

/// Parses a key made by `slot_key`.
pub fn parse_slot_key(prefix: &str, key: &str) -> Option<(String, u64)> {
    let rest = key.strip_prefix(prefix.trim_end_matches('/'))?.strip_prefix('/')?;
    let (epoch, name) = rest.split_once('/')?;
    let seq = name.strip_suffix(".parquet")?.parse().ok()?;
    Some((epoch.to_string(), seq))
}

/// A new log's name: sortable by start time, unique by a random suffix
/// (the same format as the Go appender's `NewEpoch`).
pub fn new_epoch() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let (secs, ms) = (now.as_secs() as i64, now.subsec_millis());
    let (days, sod) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}.{ms:03}Z-{:08x}",
        sod / 3600,
        (sod / 60) % 60,
        sod % 60,
        rand::random::<u32>()
    )
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// What a slot holds, as a HEAD reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Slot {
    Free,
    Data { epoch: String, content: String },
    Tomb,
}

impl Slot {
    /// From S3 user metadata (lower-case keys without `x-amz-meta-`).
    pub fn from_meta(meta: &HashMap<String, String>) -> Slot {
        if meta.get(META_KIND).map(String::as_str) == Some(KIND_TOMB) {
            return Slot::Tomb;
        }
        Slot::Data {
            epoch: meta.get(META_EPOCH).cloned().unwrap_or_default(),
            content: meta.get(META_CONTENT).cloned().unwrap_or_default(),
        }
    }
}

/// Where a batch is committed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Ref {
    pub epoch: String,
    pub seq: u64,
}

/// A PUT's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// 200: created.
    Ok,
    /// 412: the key is taken.
    Exists,
    /// No answer (timeout, reset, 5xx): it may have applied, may still apply, or never will.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// No batch in hand.
    Idle,
    /// A batch is assigned to slot `next`; the PUT is to be sent.
    Ready,
    /// The PUT is out.
    Waiting,
    /// The PUT got no answer: the slot must be read before anything else.
    Unresolved,
    /// The consumer closed this log: this incarnation writes no more here.
    Halted,
}

/// What the lane wants next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Our batch (`content`) is committed at `Ref` (on the first try, or found by HEAD).
    Committed { at: Ref, content: String, resolved: bool },
    /// Send (or resend) the PUT for slot `next`.
    Put,
    /// Read slot `next` and call `on_head`.
    Head,
    /// Another batch holds slot `next - 1`: it is committed; ours moves on.
    LearnedOther { content: String, at: Ref },
    /// A tombstone holds our slot: this log is closed. Start a new epoch.
    Halted,
    /// 412, then the HEAD found nothing: the store isn't read-after-write
    /// consistent, or the slot was deleted. Never guess: stay unresolved.
    Inconsistent,
}

/// A deliberate protocol bug, for showing that the model-based test catches it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mutation {
    #[default]
    None,
    /// A 412 or a timeout moves the batch to the next slot, without reading
    /// the slot (stock awss3exporter + exporterhelper retry).
    RetryNewKey,
    /// A tombstone in our slot is skipped: the batch moves to the next slot.
    NoHalt,
    /// The consumer inserts without checking central for the content key.
    NoCheckCentral,
    /// A request split into several objects is ACKed once any one of them
    /// has committed (`../model/s3InlineMetrics.qnt`'s `ackOnAny`).
    AckOnAny,
}

const RECENT_CAP: usize = 4096;

/// One writer lane: one epoch's log, appended one batch at a time.
#[derive(Debug)]
pub struct Lane {
    pub epoch: String,
    pub next: u64,
    pub phase: Phase,
    /// The content hash of the batch in hand.
    pub entry: Option<String>,
    /// Content hashes this lane found committed (its own or others'), bounded.
    recent: HashMap<String, Ref>,
    order: VecDeque<String>,
    /// Set by `on_put(Exists)`: a HEAD that then finds the slot free is an inconsistency.
    after_412: bool,
    pub mutation: Mutation,
}

impl Lane {
    pub fn new(epoch: String) -> Self {
        Self {
            epoch,
            next: 0,
            phase: Phase::Idle,
            entry: None,
            recent: HashMap::new(),
            order: VecDeque::new(),
            after_412: false,
            mutation: Mutation::None,
        }
    }

    /// The slot the batch in hand is for.
    pub fn slot(&self) -> Ref {
        Ref { epoch: self.epoch.clone(), seq: self.next }
    }

    pub fn known(&self, content: &str) -> Option<&Ref> {
        self.recent.get(content)
    }

    fn remember(&mut self, content: String, at: Ref) {
        if self.recent.contains_key(&content) {
            return;
        }
        self.order.push_back(content.clone());
        let _ = self.recent.insert(content, at);
        if self.order.len() > RECENT_CAP {
            if let Some(old) = self.order.pop_front() {
                let _ = self.recent.remove(&old);
            }
        }
    }

    /// Takes a batch. A batch this lane already found committed is done at
    /// once. From `Unresolved` the new (or the same) batch goes to the same
    /// slot: the earlier request may still land, and whichever lands first
    /// wins (the model's `switchPayload`).
    pub fn start(&mut self, content: &str) -> Option<Ref> {
        if let Some(r) = self.recent.get(content) {
            return Some(r.clone());
        }
        debug_assert!(matches!(self.phase, Phase::Idle | Phase::Unresolved | Phase::Ready));
        self.entry = Some(content.to_string());
        self.phase = Phase::Ready;
        self.after_412 = false;
        None
    }

    /// The PUT is being sent for `slot()`.
    pub fn sent(&mut self) {
        debug_assert_eq!(self.phase, Phase::Ready);
        self.phase = Phase::Waiting;
    }

    pub fn on_put(&mut self, o: PutOutcome) -> Step {
        match o {
            PutOutcome::Ok => self.own(false),
            PutOutcome::Exists | PutOutcome::Unknown if self.mutation == Mutation::RetryNewKey => {
                self.next += 1;
                self.phase = Phase::Ready;
                Step::Put
            }
            PutOutcome::Exists => {
                self.after_412 = true;
                Step::Head
            }
            PutOutcome::Unknown => {
                self.phase = Phase::Unresolved;
                Step::Head
            }
        }
    }

    fn own(&mut self, resolved: bool) -> Step {
        let at = self.slot();
        let content = self.entry.take().unwrap_or_default();
        self.remember(content.clone(), at.clone());
        self.next += 1;
        self.phase = Phase::Idle;
        self.after_412 = false;
        Step::Committed { at, content, resolved }
    }

    /// What a HEAD of `slot()` found (after a 412, or to resolve a timeout).
    pub fn on_head(&mut self, found: &Slot) -> Step {
        match found {
            Slot::Data { epoch, content }
                if Some(content) == self.entry.as_ref() && *epoch == self.epoch =>
            {
                self.own(true)
            }
            Slot::Free if self.after_412 => {
                self.phase = Phase::Unresolved;
                Step::Inconsistent
            }
            Slot::Free => {
                self.phase = Phase::Ready;
                Step::Put
            }
            Slot::Tomb if self.mutation != Mutation::NoHalt => {
                self.phase = Phase::Halted;
                self.after_412 = false;
                Step::Halted
            }
            Slot::Tomb => {
                self.next += 1;
                self.phase = Phase::Ready;
                self.after_412 = false;
                Step::Put
            }
            Slot::Data { content, .. } => {
                let at = self.slot();
                self.remember(content.clone(), at.clone());
                self.next += 1;
                self.phase = Phase::Ready;
                self.after_412 = false;
                Step::LearnedOther { content: content.clone(), at }
            }
        }
    }

    /// After a halt: a new log, starting at slot 0, with the same batch in hand.
    pub fn reincarnate(&mut self, epoch: String) {
        self.epoch = epoch;
        self.next = 0;
        self.after_412 = false;
        self.phase = if self.entry.is_some() { Phase::Ready } else { Phase::Idle };
    }
}

// ---- requests with several objects ------------------------------------------------

/// How one object of a request ended up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartOutcome {
    /// Committed at a slot (first try, resolved by HEAD, or found known).
    Committed(Ref),
    /// Not resolved yet: the lane holds the slot; a retry resolves it.
    Unresolved(String),
    /// The part can't be encoded: retrying can't help.
    Rejected(String),
}

/// What the request's sender is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Every object is committed (or there were none).
    Ack,
    /// At least one object is unresolved: retry the whole request. The parts
    /// already committed are found in their lanes' known set on the retry,
    /// or, after a restart, committed again in the new epoch, where the
    /// consumer's content-key check skips the copy.
    Retry(String),
    /// A part can't be encoded: permanent.
    Reject(String),
}

/// The request-level decision for a request split into several objects (a
/// metrics request: one object per metric type, each in its own log): ACK
/// only when every object has committed. Never ACK on a subset: the sender
/// would drop the request, and a part still unresolved in a lane is lost if
/// the edge then restarts (`../model/s3InlineMetrics.qnt`, `ackOnFirst`).
pub fn request_verdict<'a>(parts: impl IntoIterator<Item = &'a PartOutcome>) -> Verdict {
    request_verdict_with(parts, Mutation::None)
}

/// `request_verdict`, or with `Mutation::AckOnAny` the broken rule, for the
/// model-based test.
pub fn request_verdict_with<'a>(parts: impl IntoIterator<Item = &'a PartOutcome>, m: Mutation) -> Verdict {
    let mut retry = None;
    let mut any = false;
    for p in parts {
        match p {
            PartOutcome::Committed(_) => any = true,
            PartOutcome::Unresolved(e) => retry = retry.or_else(|| Some(e.clone())),
            PartOutcome::Rejected(e) => return Verdict::Reject(e.clone()),
        }
    }
    match retry {
        Some(_) if any && m == Mutation::AckOnAny => Verdict::Ack,
        Some(e) => Verdict::Retry(e),
        None => Verdict::Ack,
    }
}

// ---- consumer -----------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CPhase {
    Idle,
    Checked,
    Inserted,
    TombWait,
    TombUnresolved,
}

/// What `insert` decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Insert {
    /// Insert the object's rows under this content key (and dedup token).
    Rows { content: String },
    /// Central already holds the content (a copy from another epoch, or a
    /// retry after a crash between insert and checkpoint).
    DedupHit,
    /// The slot no longer holds data (only possible without create-only PUTs).
    Replaced,
}

/// The consumer: per epoch a checkpoint (the next slot to ingest) and a
/// closed flag. In production these are one CAS'd object (S3NATIVE.md);
/// here they are this struct, persisted by the runner.
#[derive(Clone, Debug)]
pub struct Consumer {
    pub ckpt: BTreeMap<String, u64>,
    pub closed: BTreeSet<String>,
    pub phase: CPhase,
    pub cur: Option<(String, u64)>,
    cur_content: Option<String>,
    pub saw_present: bool,
    pub mutation: Mutation,
}

impl Default for Consumer {
    fn default() -> Self {
        Self {
            ckpt: BTreeMap::new(),
            closed: BTreeSet::new(),
            phase: CPhase::Idle,
            cur: None,
            cur_content: None,
            saw_present: false,
            mutation: Mutation::None,
        }
    }
}

impl Consumer {
    pub fn next_slot(&self, epoch: &str) -> u64 {
        self.ckpt.get(epoch).copied().unwrap_or(0)
    }

    pub fn is_closed(&self, epoch: &str) -> bool {
        self.closed.contains(epoch)
    }

    /// The slot at the checkpoint holds data with `content`; `in_central` is
    /// whether central already has rows with that content key.
    pub fn check(&mut self, epoch: &str, content: &str, in_central: bool) {
        debug_assert_eq!(self.phase, CPhase::Idle);
        self.cur = Some((epoch.to_string(), self.next_slot(epoch)));
        self.cur_content = Some(content.to_string());
        self.saw_present = in_central && self.mutation != Mutation::NoCheckCentral;
        self.phase = CPhase::Checked;
    }

    /// Decides the insert, given what the slot holds now.
    pub fn insert(&mut self, now: &Slot) -> Insert {
        debug_assert_eq!(self.phase, CPhase::Checked);
        self.phase = CPhase::Inserted;
        match now {
            Slot::Data { content, .. } if !self.saw_present => {
                Insert::Rows { content: content.clone() }
            }
            Slot::Data { .. } => Insert::DedupHit,
            _ => Insert::Replaced,
        }
    }

    pub fn advance(&mut self) {
        debug_assert_eq!(self.phase, CPhase::Inserted);
        if let Some((e, s)) = self.cur.take() {
            let _ = self.ckpt.insert(e, s + 1);
        }
        self.reset();
    }

    fn reset(&mut self) {
        self.phase = CPhase::Idle;
        self.cur = None;
        self.cur_content = None;
        self.saw_present = false;
    }

    /// A tombstone is at the checkpoint: the epoch is closed there.
    pub fn see_tomb(&mut self, epoch: &str) {
        let _ = self.closed.insert(epoch.to_string());
    }

    /// Races a tombstone into the free head slot of a superseded epoch.
    /// Returns the slot to PUT the tombstone into.
    pub fn tomb(&mut self, epoch: &str) -> u64 {
        let s = self.next_slot(epoch);
        self.cur = Some((epoch.to_string(), s));
        self.phase = CPhase::TombWait;
        s
    }

    /// The tombstone PUT's answer. `Some(true)`: closed; `Some(false)`: data
    /// won the race (ingest it next); `None`: read the slot (`on_tomb_head`).
    pub fn on_tomb_put(&mut self, o: PutOutcome) -> Option<bool> {
        match o {
            PutOutcome::Ok => {
                if let Some((e, _)) = self.cur.clone() {
                    self.see_tomb(&e);
                }
                self.reset();
                Some(true)
            }
            PutOutcome::Exists => None,
            PutOutcome::Unknown => {
                self.phase = CPhase::TombUnresolved;
                None
            }
        }
    }

    /// What the slot holds after a 412 or a timeout of the tombstone PUT.
    /// Returns true if the tombstone has to be sent again (the slot is free).
    pub fn on_tomb_head(&mut self, found: &Slot) -> bool {
        match found {
            Slot::Tomb => {
                if let Some((e, _)) = self.cur.clone() {
                    self.see_tomb(&e);
                }
                self.reset();
                false
            }
            Slot::Data { .. } => {
                self.reset();
                false
            }
            Slot::Free => {
                self.phase = CPhase::TombWait;
                true
            }
        }
    }

    /// The consumer process died: in-memory progress is lost, the durable
    /// checkpoint and closed set stay.
    pub fn crash(&mut self) {
        self.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        let k = slot_key("p/traces/", "E1", 7);
        assert_eq!(k, "p/traces/E1/00000000000000000007.parquet");
        assert_eq!(parse_slot_key("p/traces", &k), Some(("E1".into(), 7)));
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20721), (2026, 9, 25));
        assert_eq!(new_epoch().len(), "20260925T031500.123Z-".len() + 8);
    }

    #[test]
    fn ambiguous_then_own() {
        let mut l = Lane::new("E".into());
        assert!(l.start("h1").is_none());
        l.sent();
        assert_eq!(l.on_put(PutOutcome::Unknown), Step::Head);
        let found = Slot::Data { epoch: "E".into(), content: "h1".into() };
        assert!(matches!(l.on_head(&found), Step::Committed { resolved: true, .. }));
        assert_eq!(l.next, 1);
        assert_eq!(l.start("h1"), Some(Ref { epoch: "E".into(), seq: 0 }));
    }

    #[test]
    fn switched_slot_learns_other() {
        let mut l = Lane::new("E".into());
        l.start("h1");
        l.sent();
        l.on_put(PutOutcome::Unknown);
        l.start("h2"); // the next request goes to the same slot
        l.sent();
        assert_eq!(l.on_put(PutOutcome::Exists), Step::Head);
        let found = Slot::Data { epoch: "E".into(), content: "h1".into() };
        assert!(matches!(l.on_head(&found), Step::LearnedOther { .. }));
        assert_eq!((l.next, l.phase), (1, Phase::Ready));
        assert!(l.known("h1").is_some());
    }

    #[test]
    fn tomb_halts() {
        let mut l = Lane::new("E".into());
        l.start("h1");
        l.sent();
        l.on_put(PutOutcome::Exists);
        assert_eq!(l.on_head(&Slot::Tomb), Step::Halted);
        l.reincarnate("F".into());
        assert_eq!((l.next, l.phase, l.epoch.as_str()), (0, Phase::Ready, "F"));
    }

    #[test]
    fn verdict_needs_every_part() {
        let ok = PartOutcome::Committed(Ref { epoch: "E".into(), seq: 0 });
        let un = PartOutcome::Unresolved("t".into());
        assert_eq!(request_verdict(&[]), Verdict::Ack);
        assert_eq!(request_verdict(&[ok.clone(), ok.clone()]), Verdict::Ack);
        assert_eq!(request_verdict(&[ok.clone(), un.clone()]), Verdict::Retry("t".into()));
        assert_eq!(request_verdict(&[un, PartOutcome::Rejected("x".into()), ok]), Verdict::Reject("x".into()));
    }

    #[test]
    fn inconsistent_store() {
        let mut l = Lane::new("E".into());
        l.start("h1");
        l.sent();
        l.on_put(PutOutcome::Exists);
        assert_eq!(l.on_head(&Slot::Free), Step::Inconsistent);
        assert_eq!(l.phase, Phase::Unresolved);
    }
}
