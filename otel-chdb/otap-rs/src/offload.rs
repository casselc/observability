//! Content by reference: the edge's payload offloader (DECISIONS.md D36,
//! `../../FORMAT.md` §2.3, `../../research/langfuse.md` §6.2).
//!
//! Generic, not GenAI-specific: every span attribute, span event attribute,
//! log attribute and log body goes through `Offloader::value`, which
//!
//! 1. **redacts** a value whose key is in `redact_keys` (the hook runs first,
//!    so what is offloaded, and hashed, is the redacted value);
//! 2. **offloads** a value longer than `threshold`, or non-empty under a key
//!    in `keys`: the value becomes a *reference document*, a JSON array of
//!    `"h:<32 hex>"`, and its content goes to the object's payload part
//!    (`payloads`, on the first row that references it);
//! 3. **splits** a value under a key in `split_keys` that is a JSON array
//!    (a bounded, iterative scan: `split_max_depth`, `split_max_elements`)
//!    into one payload per element, so a conversation resent call after call
//!    is stored once per message;
//! 4. **caps** a value at `max_value` bytes (truncated, marked with its
//!    original size, counted).
//!
//! Markers are appended to the same map after its entries, in the order the
//! values were met: `otel.payload.<key>.bytes` (the offloaded value's size),
//! `.truncated_from` (the size before the cap), `.elements` (a split's element
//! count), `.redacted_from` (a redacted value's size). A log body's key is
//! `@body` and its markers go in the log's attributes.
//!
//! **Hash** (R-L1, R-L2, SEC-L5): BLAKE3-128 of the content under a key
//! derived from the tenant and the day, so equal content in two tenants (or
//! two days) has unrelated hashes, and within a tenant and day it
//! deduplicates:
//!
//! ```text
//! key  = BLAKE3.derive_key("otel-chdb payload v1", le64(len cluster) ‖ cluster ‖ le64(len namespace) ‖ namespace ‖ decimal(received_ns / 86400e9))
//! hash = BLAKE3.keyed(key, content)[..16]
//! ```
//!
//! `cluster` is the edge's own `cluster` (the key segment), `namespace` the
//! resource's covered `k8s.namespace.name` (the edge's resource detection,
//! never a span attribute or an SDK key), `received_ns` the request's
//! custody time. The Go edge (`../../parquetgo/offload.go`) is the same,
//! byte for byte; `../../langfuse/testdata/offload_vectors.json` holds the
//! vectors both are tested against.
//!
//! **Which payloads an object carries** is decided when it is encoded for
//! its slot, from `PayloadCache` (one per writer lane, per epoch, bounded),
//! exactly as resource announcements are (`resource.rs`): a payload counts as
//! sent only once an object carrying it has committed.

use std::collections::{HashMap, HashSet};

/// The derive-key context of the payload hash (never change it: every
/// stored reference was made under it).
pub const HASH_CONTEXT: &str = "otel-chdb payload v1";
/// A log body's key, for its markers.
pub const BODY_KEY: &[u8] = b"@body";
/// The marker prefix.
pub const MARKER_PREFIX: &[u8] = b"otel.payload.";
const NS_PER_DAY: u64 = 86_400_000_000_000;

/// The GenAI content keys, Langfuse's input and output keys and
/// OpenInference's `input.value` / `output.value`: offloaded whatever
/// their size.
pub const DEFAULT_KEYS: &[&str] = &[
    "gen_ai.input.messages",
    "gen_ai.output.messages",
    "gen_ai.system_instructions",
    "gen_ai.tool.definitions",
    "gen_ai.tool.call.arguments",
    "gen_ai.tool.call.result",
    "gen_ai.prompt",
    "gen_ai.completion",
    "langfuse.observation.input",
    "langfuse.observation.output",
    "langfuse.trace.input",
    "langfuse.trace.output",
    "input.value",
    "output.value",
];

/// The keys whose JSON arrays are split per element (message lists).
pub const DEFAULT_SPLIT_KEYS: &[&str] = &[
    "gen_ai.input.messages",
    "gen_ai.output.messages",
    "gen_ai.system_instructions",
    "langfuse.observation.input",
    "langfuse.observation.output",
];

/// `offload:` in the exporter's config (policy, DECISIONS.md D36's owner
/// decisions). Validated together at start (`validate`, CAST 25).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct OffloadOptions {
    /// Off: values stay inline, `payload_refs` and `payloads` stay empty.
    pub enabled: bool,
    /// Values longer than this many bytes are offloaded (2 KiB).
    pub threshold: usize,
    /// Values longer than this are truncated first, with a marker (8 MiB).
    pub max_value: usize,
    /// An OTLP request larger than this (its protobuf bytes) is refused as a
    /// client error, counted (128 MiB: the receivers' default body limit); OTAP input is bounded by its
    /// receiver's message size.
    pub max_request_bytes: usize,
    /// Keys offloaded whatever the value's size (when non-empty).
    pub keys: Vec<String>,
    /// Keys whose JSON-array values are split into one payload per element.
    pub split_keys: Vec<String>,
    /// Keys whose values are redacted (replaced by the empty string, with a
    /// `redacted_from` marker) before anything else.
    pub redact_keys: Vec<String>,
    /// The split's scan gives up (one payload, not split) past this nesting
    /// depth (containers, the array itself included).
    pub split_max_depth: usize,
    /// ... or past this many elements.
    pub split_max_elements: usize,
    /// Payloads remembered as sent, per writer lane (the least recently
    /// sent beyond it are forgotten and sent again).
    pub cache_size: usize,
}

impl Default for OffloadOptions {
    fn default() -> Self {
        OffloadOptions {
            enabled: true,
            threshold: 2 << 10,
            max_value: 8 << 20,
            max_request_bytes: 128 << 20,
            keys: DEFAULT_KEYS.iter().map(|s| s.to_string()).collect(),
            split_keys: DEFAULT_SPLIT_KEYS.iter().map(|s| s.to_string()).collect(),
            redact_keys: Vec::new(),
            split_max_depth: 64,
            split_max_elements: 4096,
            cache_size: 65536,
        }
    }
}

/// The largest `max_request_bytes`: every column's values of one object sit
/// behind 32-bit offsets (Arrow `Binary`, 2 GiB), payloads included.
pub const MAX_REQUEST_CAP: usize = 1 << 30;

impl OffloadOptions {
    /// Off (the library default: `Encoder::new` offloads nothing).
    pub fn off() -> Self {
        OffloadOptions { enabled: false, ..Default::default() }
    }

    /// Threshold, caps and bounds validated together (CAST 25): each must
    /// make sense against the others, or the edge refuses to start.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        let mut errs = Vec::new();
        if self.threshold == 0 {
            errs.push("threshold must be > 0".to_string());
        }
        if self.threshold >= self.max_value {
            errs.push(format!("threshold {} must be below max_value {}", self.threshold, self.max_value));
        }
        if self.max_value > self.max_request_bytes {
            errs.push(format!("max_value {} must not exceed max_request_bytes {}", self.max_value, self.max_request_bytes));
        }
        if self.max_request_bytes > MAX_REQUEST_CAP {
            errs.push(format!("max_request_bytes {} exceeds {MAX_REQUEST_CAP} (32-bit column offsets)", self.max_request_bytes));
        }
        if self.split_max_depth == 0 || self.split_max_depth > 1024 {
            errs.push(format!("split_max_depth {} must be in 1..=1024", self.split_max_depth));
        }
        if self.split_max_elements == 0 {
            errs.push("split_max_elements must be > 0".to_string());
        }
        if self.cache_size == 0 {
            errs.push("cache_size must be > 0".to_string());
        }
        for k in &self.split_keys {
            if !self.keys.contains(k) {
                errs.push(format!("split key {k:?} is not in keys (a split applies to offloaded values only)"));
            }
        }
        for k in self.keys.iter().chain(&self.split_keys).chain(&self.redact_keys) {
            if k.is_empty() || k.as_bytes().starts_with(MARKER_PREFIX) {
                errs.push(format!("key {k:?}: empty or a marker key"));
            }
        }
        if errs.is_empty() { Ok(()) } else { Err(format!("offload: {}", errs.join("; "))) }
    }
}

/// The per-tenant, per-day hash key.
pub fn tenant_key(cluster: &str, namespace: &[u8], received_ns: u64) -> [u8; 32] {
    let day = (received_ns / NS_PER_DAY).to_string();
    let mut m = Vec::with_capacity(cluster.len() + namespace.len() + day.len() + 16);
    m.extend_from_slice(&(cluster.len() as u64).to_le_bytes());
    m.extend_from_slice(cluster.as_bytes());
    m.extend_from_slice(&(namespace.len() as u64).to_le_bytes());
    m.extend_from_slice(namespace);
    m.extend_from_slice(day.as_bytes());
    blake3::derive_key(HASH_CONTEXT, &m)
}

/// A payload's hash under a tenant key.
pub fn payload_hash(key: &[u8; 32], content: &[u8]) -> [u8; 16] {
    let h = blake3::keyed_hash(key, content);
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.as_bytes()[..16]);
    out
}

/// Where a value over `n` bytes is cut: at `n`, backed off (at most three
/// bytes) to the start of a UTF-8 sequence, so a valid string stays valid.
pub fn truncate_at(v: &[u8], n: usize) -> usize {
    if v.len() <= n {
        return v.len();
    }
    let mut cut = n;
    while cut > 0 && cut + 3 > n && (v[cut] & 0xC0) == 0x80 {
        cut -= 1;
    }
    cut
}

#[inline]
fn ws(v: &[u8], mut i: usize) -> usize {
    while i < v.len() && matches!(v[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

fn scan_string(v: &[u8], mut i: usize) -> Option<usize> {
    // v[i] == '"'
    i += 1;
    while i < v.len() {
        match v[i] {
            b'"' => return Some(i + 1),
            b'\\' => {
                let c = *v.get(i + 1)?;
                match c {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => i += 2,
                    b'u' => {
                        let h = v.get(i + 2..i + 6)?;
                        if !h.iter().all(u8::is_ascii_hexdigit) {
                            return None;
                        }
                        i += 6;
                    }
                    _ => return None,
                }
            }
            c if c < 0x20 => return None,
            _ => i += 1,
        }
    }
    None
}

fn scan_number(v: &[u8], mut i: usize) -> Option<usize> {
    let digits = |v: &[u8], mut i: usize| {
        let s = i;
        while i < v.len() && v[i].is_ascii_digit() {
            i += 1;
        }
        (i > s).then_some(i)
    };
    if v.get(i) == Some(&b'-') {
        i += 1;
    }
    match v.get(i) {
        Some(b'0') => i += 1,
        Some(b'1'..=b'9') => i = digits(v, i)?,
        _ => return None,
    }
    if v.get(i) == Some(&b'.') {
        i = digits(v, i + 1)?;
    }
    if matches!(v.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(v.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        i = digits(v, i)?;
    }
    Some(i)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum St {
    Value,
    ValueOrClose,
    KeyOrClose,
    Key,
    Colon,
    After,
}

/// The top-level elements of `v` if it is one JSON array (RFC 8259's
/// grammar over bytes; string contents are not checked for UTF-8), as byte
/// ranges without surrounding whitespace. `None` when it is not an array,
/// not valid, nested deeper than `max_depth` containers, or has more than
/// `max_elements` elements. Iterative: no recursion, whatever the input.
pub fn json_array_elements(v: &[u8], max_depth: usize, max_elements: usize) -> Option<Vec<(usize, usize)>> {
    let mut i = ws(v, 0);
    if v.get(i) != Some(&b'[') {
        return None;
    }
    let mut stack: Vec<u8> = Vec::new();
    let mut elems: Vec<(usize, usize)> = Vec::new();
    let mut elem_start = 0usize;
    let mut s = St::Value;
    loop {
        i = ws(v, i);
        match s {
            St::Value | St::ValueOrClose => {
                let c = *v.get(i)?;
                if s == St::ValueOrClose && c == b']' {
                    let _ = stack.pop();
                    i += 1;
                    if stack.len() == 1 {
                        if elems.len() >= max_elements {
                            return None;
                        }
                        elems.push((elem_start, i));
                    }
                    s = St::After;
                    continue;
                }
                if stack.len() == 1 {
                    elem_start = i;
                }
                match c {
                    b'[' | b'{' => {
                        if stack.len() >= max_depth {
                            return None;
                        }
                        stack.push(c);
                        i += 1;
                        s = if c == b'[' { St::ValueOrClose } else { St::KeyOrClose };
                        continue;
                    }
                    b'"' => i = scan_string(v, i)?,
                    b'-' | b'0'..=b'9' => i = scan_number(v, i)?,
                    b't' | b'f' | b'n' => {
                        let lit: &[u8] = match c {
                            b't' => b"true",
                            b'f' => b"false",
                            _ => b"null",
                        };
                        if v.get(i..i + lit.len())? != lit {
                            return None;
                        }
                        i += lit.len();
                    }
                    _ => return None,
                }
                if stack.len() == 1 {
                    if elems.len() >= max_elements {
                        return None;
                    }
                    elems.push((elem_start, i));
                }
                s = St::After;
            }
            St::KeyOrClose | St::Key => {
                let c = *v.get(i)?;
                if s == St::KeyOrClose && c == b'}' {
                    let _ = stack.pop();
                    i += 1;
                    if stack.len() == 1 {
                        if elems.len() >= max_elements {
                            return None;
                        }
                        elems.push((elem_start, i));
                    }
                    s = St::After;
                    continue;
                }
                if c != b'"' {
                    return None;
                }
                i = scan_string(v, i)?;
                s = St::Colon;
            }
            St::Colon => {
                if v.get(i) != Some(&b':') {
                    return None;
                }
                i += 1;
                s = St::Value;
            }
            St::After => {
                let Some(&top) = stack.last() else {
                    return (i == v.len()).then_some(elems);
                };
                let c = *v.get(i)?;
                i += 1;
                match (c, top) {
                    (b',', b'[') => s = St::Value,
                    (b',', b'{') => s = St::Key,
                    (b']', b'[') | (b'}', b'{') => {
                        let _ = stack.pop();
                        if stack.len() == 1 {
                            if elems.len() >= max_elements {
                                return None;
                            }
                            elems.push((elem_start, i));
                        }
                    }
                    _ => return None,
                }
            }
        }
    }
}

/// Lower-case hex of a hash.
pub fn hex16(h: &[u8; 16]) -> String {
    hex::encode(h)
}

/// The counters of one offloader (summed into the exporter's).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OffloadStats {
    /// Values replaced by references.
    pub offloaded: u64,
    /// Their bytes (after truncation).
    pub offloaded_bytes: u64,
    /// Values split per element.
    pub split: u64,
    /// Values truncated at `max_value`.
    pub truncated: u64,
    /// Values redacted.
    pub redacted: u64,
    /// Requests refused (`max_request_bytes`).
    pub refused: u64,
    /// Payloads carried in committed objects' payload parts.
    pub carried: u64,
    /// Payloads referenced by a committed object but not carried: already
    /// sent in the lane's epoch (the cache's dedup hits).
    pub dedup: u64,
}

impl OffloadStats {
    pub fn add(&mut self, o: &OffloadStats) {
        self.offloaded += o.offloaded;
        self.offloaded_bytes += o.offloaded_bytes;
        self.split += o.split;
        self.truncated += o.truncated;
        self.redacted += o.redacted;
        self.refused += o.refused;
        self.carried += o.carried;
        self.dedup += o.dedup;
    }

    /// In `commit_metrics::OFFLOAD_OUTCOMES` order.
    pub fn totals(&self) -> [u64; 8] {
        [self.offloaded, self.offloaded_bytes, self.split, self.truncated, self.redacted, self.refused, self.carried, self.dedup]
    }
}

/// One payload of a request: its hash, content and the first row (walk
/// order) that references it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payload {
    pub hash: [u8; 16],
    pub content: Vec<u8>,
    pub first_row: u32,
}

/// What `value` did to one value.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Unchanged: store the value as it is.
    Inline,
    /// Replaced: store `out` instead.
    Replaced,
}

/// The offloader's policy, compiled (sets of keys).
#[derive(Clone, Debug)]
pub struct Policy {
    pub opts: OffloadOptions,
    keys: HashSet<Vec<u8>>,
    split: HashSet<Vec<u8>>,
    redact: HashSet<Vec<u8>>,
}

impl Policy {
    pub fn new(opts: OffloadOptions) -> Self {
        let set = |v: &[String]| v.iter().map(|s| s.as_bytes().to_vec()).collect::<HashSet<_>>();
        Policy { keys: set(&opts.keys), split: set(&opts.split_keys), redact: set(&opts.redact_keys), opts }
    }
}

/// A request's offloading state: the tenant key of the resource being
/// walked, the request's payloads and each row's references.
pub struct Offloader {
    pub policy: std::sync::Arc<Policy>,
    cluster: String,
    received_ns: u64,
    key: [u8; 32],
    keys_by_ns: HashMap<Vec<u8>, [u8; 32]>,
    /// The request's distinct payloads, in the order first referenced.
    pub payloads: Vec<Payload>,
    index: HashMap<[u8; 16], usize>,
    /// The current row's distinct references, in order.
    row_refs: Vec<[u8; 16]>,
    row_seen: HashSet<[u8; 16]>,
    /// Markers of the map being written.
    pub markers: Vec<(Vec<u8>, Vec<u8>)>,
    /// Total payload bytes (distinct) of the request.
    pub payload_bytes: usize,
    pub stats: OffloadStats,
    scratch: Vec<u8>,
}

impl Offloader {
    pub fn new(policy: std::sync::Arc<Policy>, cluster: &str, received_ns: u64) -> Self {
        let key = tenant_key(cluster, b"", received_ns);
        Offloader {
            policy,
            cluster: cluster.to_string(),
            received_ns,
            key,
            keys_by_ns: HashMap::new(),
            payloads: Vec::new(),
            index: HashMap::new(),
            row_refs: Vec::new(),
            row_seen: HashSet::new(),
            markers: Vec::new(),
            payload_bytes: 0,
            stats: OffloadStats::default(),
            scratch: Vec::new(),
        }
    }

    /// The resource being walked: its covered `k8s.namespace.name` (empty
    /// when it has none).
    pub fn tenant(&mut self, namespace: &[u8]) {
        if let Some(k) = self.keys_by_ns.get(namespace) {
            self.key = *k;
            return;
        }
        let k = tenant_key(&self.cluster, namespace, self.received_ns);
        let _ = self.keys_by_ns.insert(namespace.to_vec(), k);
        self.key = k;
    }

    /// Whether a value needs `value`'s slow path (the fast path stores it
    /// as is): its key is listed, or it is over the threshold.
    #[inline]
    pub fn candidate(&self, key: &[u8], len: usize) -> bool {
        len > self.policy.opts.threshold || self.policy.keys.contains(key) || self.policy.redact.contains(key)
    }

    fn reference(&mut self, content: &[u8], row: u32) -> [u8; 16] {
        let h = payload_hash(&self.key, content);
        if !self.index.contains_key(&h) {
            let _ = self.index.insert(h, self.payloads.len());
            self.payloads.push(Payload { hash: h, content: content.to_vec(), first_row: row });
            self.payload_bytes += content.len();
        }
        if self.row_seen.insert(h) {
            self.row_refs.push(h);
        }
        h
    }

    fn marker(&mut self, key: &[u8], what: &str, n: usize) {
        let mut k = Vec::with_capacity(MARKER_PREFIX.len() + key.len() + what.len() + 1);
        k.extend_from_slice(MARKER_PREFIX);
        k.extend_from_slice(key);
        k.push(b'.');
        k.extend_from_slice(what.as_bytes());
        self.markers.push((k, n.to_string().into_bytes()));
    }

    /// One value of row `row` under `key`: `Inline` (store `v`), or
    /// `Replaced` with the value to store in `out`. Markers accumulate in
    /// `markers` for the caller to append after the map's entries.
    pub fn value(&mut self, key: &[u8], v: &[u8], row: u32, out: &mut Vec<u8>) -> Outcome {
        out.clear();
        if self.policy.redact.contains(key) {
            if v.is_empty() {
                return Outcome::Inline;
            }
            self.stats.redacted += 1;
            self.marker(key, "redacted_from", v.len());
            return Outcome::Replaced; // out is empty
        }
        let listed = self.policy.keys.contains(key);
        if !(v.len() > self.policy.opts.threshold || (listed && !v.is_empty())) {
            return Outcome::Inline;
        }
        let o = &self.policy.opts;
        let cut = truncate_at(v, o.max_value);
        let val = &v[..cut];
        let split = if self.policy.split.contains(key) {
            json_array_elements(val, o.split_max_depth, o.split_max_elements)
        } else {
            None
        };
        let mut sc = std::mem::take(&mut self.scratch);
        sc.clear();
        sc.push(b'[');
        let mut first = true;
        let mut push = |this: &mut Self, sc: &mut Vec<u8>, part: &[u8]| {
            let h = this.reference(part, row);
            if !first {
                sc.push(b',');
            }
            first = false;
            sc.extend_from_slice(b"\"h:");
            let mut hx = [0u8; 32];
            hex::encode_to_slice(h, &mut hx).expect("32 hex digits");
            sc.extend_from_slice(&hx);
            sc.push(b'"');
        };
        match &split {
            Some(elems) => {
                for &(a, b) in elems {
                    push(self, &mut sc, &val[a..b]);
                }
            }
            None => push(self, &mut sc, val),
        }
        sc.push(b']');
        out.extend_from_slice(&sc);
        self.scratch = sc;
        self.stats.offloaded += 1;
        self.stats.offloaded_bytes += cut as u64;
        self.marker(key, "bytes", cut);
        if cut < v.len() {
            self.stats.truncated += 1;
            self.marker(key, "truncated_from", v.len());
        }
        if let Some(e) = &split {
            self.stats.split += 1;
            self.marker(key, "elements", e.len());
        }
        Outcome::Replaced
    }

    /// Ends row `row`'s references: appended to `refs` (hex strings, the
    /// `payload_refs` column's list), resetting the row.
    pub fn end_row(&mut self, refs: &mut crate::columns::Bin) -> usize {
        let n = self.row_refs.len();
        for h in self.row_refs.drain(..) {
            let mut hx = [0u8; 32];
            hex::encode_to_slice(h, &mut hx).expect("32 hex digits");
            refs.push(&hx);
        }
        self.row_seen.clear();
        n
    }

    /// Takes the request's payloads (resetting them).
    pub fn take_payloads(&mut self) -> Vec<Payload> {
        self.index.clear();
        self.payload_bytes = 0;
        std::mem::take(&mut self.payloads)
    }
}

/// One writer lane's sent payloads, for its current epoch (the payload
/// part's `AnnounceCache`): a hash counts as sent once an object carrying it
/// has committed; a new epoch starts empty; bounded (the least recently
/// sent eighth goes first). The hash names its tenant and day, so no window
/// is needed: tomorrow's references are other hashes.
#[derive(Clone, Debug, Default)]
pub struct PayloadCache {
    epoch: String,
    map: HashMap<[u8; 16], u64>,
    tick: u64,
    pub evicted: u64,
}

impl PayloadCache {
    /// Whether an object in `epoch` must carry payload `h`.
    pub fn wants(&self, epoch: &str, h: &[u8; 16]) -> bool {
        self.epoch != epoch || !self.map.contains_key(h)
    }

    /// Marks payloads sent: only once an object carrying them has committed
    /// in `epoch`.
    pub fn sent(&mut self, epoch: &str, hs: &[[u8; 16]], cap: usize) {
        if self.epoch != epoch {
            self.map.clear();
            self.epoch = epoch.to_string();
        }
        for h in hs {
            self.tick += 1;
            let _ = self.map.insert(*h, self.tick);
        }
        let cap = cap.max(1);
        if self.map.len() > cap {
            let keep = cap - cap / 8;
            let mut v: Vec<(u64, [u8; 16])> = self.map.iter().map(|(h, t)| (*t, *h)).collect();
            v.sort_unstable();
            for (_, h) in &v[..v.len() - keep] {
                let _ = self.map.remove(h);
                self.evicted += 1;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn off(opts: OffloadOptions) -> Offloader {
        Offloader::new(Arc::new(Policy::new(opts)), "c1", 1_790_000_000_000_000_000)
    }

    fn elems(s: &str) -> Option<Vec<&str>> {
        json_array_elements(s.as_bytes(), 64, 4096).map(|v| v.into_iter().map(|(a, b)| &s[a..b]).collect())
    }

    #[test]
    fn json_arrays_split_into_raw_elements() {
        assert_eq!(elems("[]"), Some(vec![]));
        assert_eq!(elems(" [ 1 , \"a\" ,{\"k\":[1,{}]} , [] ,null,true,false,-0.5e+3 ] \n"), Some(vec!["1", "\"a\"", "{\"k\":[1,{}]}", "[]", "null", "true", "false", "-0.5e+3"]));
        assert_eq!(elems("[\"\\u00e9\\n\",\"\u{1F680}\"]"), Some(vec!["\"\\u00e9\\n\"", "\"\u{1F680}\""]));
        for bad in ["", "{}", "1", "[", "[1,]", "[,1]", "[1 2]", "[01]", "[1.]", "[.1]", "[1e]", "[tru]", "[\"a]", "[\"\\x\"]", "[\"\\u12g4\"]",
            "[\"a\u{1}\"]", "[1]]", "[1] x", "[{\"a\"}]", "[{\"a\":}]", "[{1:2}]", "[{\"a\":1,}]", "[+1]", "[-]", "[nul]"] {
            assert_eq!(elems(bad), None, "{bad:?}");
        }
        // invalid UTF-8 inside a string is data
        assert_eq!(json_array_elements(b"[\"\xff\xfe\"]", 4, 4), Some(vec![(1, 5)]));
    }

    #[test]
    fn json_scan_is_bounded() {
        let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
        assert_eq!(json_array_elements(deep.as_bytes(), 64, 4096), None, "a bomb deeper than the bound is not split");
        let ok = format!("{}{}", "[".repeat(64), "]".repeat(64));
        assert_eq!(json_array_elements(ok.as_bytes(), 64, 4096).map(|v| v.len()), Some(1));
        assert_eq!(json_array_elements(ok.as_bytes(), 63, 4096), None);
        let many = format!("[{}]", vec!["1"; 10_000].join(","));
        assert_eq!(json_array_elements(many.as_bytes(), 64, 4096), None);
        assert_eq!(json_array_elements(many.as_bytes(), 64, 10_000).map(|v| v.len()), Some(10_000));
    }

    #[test]
    fn truncation_keeps_utf8_sequences_whole() {
        let s = "aé🚀".as_bytes(); // 1 + 2 + 4
        assert_eq!(truncate_at(s, 7), 7);
        assert_eq!(truncate_at(s, 3), 3);
        assert_eq!(truncate_at(s, 2), 1, "é is not cut");
        assert_eq!(truncate_at(s, 5), 3, "the rocket is not cut");
        assert_eq!(truncate_at(&[0x80u8; 10], 5), 2, "backs off at most three bytes");
    }

    #[test]
    fn hashes_are_keyed_by_tenant_and_day() {
        let day = 1_790_000_000_000_000_000u64;
        let a = payload_hash(&tenant_key("c1", b"ns-a", day), b"the same prompt");
        let b = payload_hash(&tenant_key("c1", b"ns-b", day), b"the same prompt");
        let c = payload_hash(&tenant_key("c2", b"ns-a", day), b"the same prompt");
        let d = payload_hash(&tenant_key("c1", b"ns-a", day + NS_PER_DAY), b"the same prompt");
        let e = payload_hash(&tenant_key("c1", b"ns-a", day + 1), b"the same prompt");
        assert!(a != b && a != c && a != d && b != c);
        assert_eq!(a, e, "same day");
        // length-prefixed: ("c1\0", "x") is not ("c1", "\0x")
        assert_ne!(tenant_key("c1", b"\0x", day), tenant_key("c1\0", b"x", day));
    }

    #[test]
    fn values_are_offloaded_split_truncated_and_redacted() {
        let mut o = off(OffloadOptions { max_value: 4096, redact_keys: vec!["secret".into()], ..Default::default() });
        let mut out = Vec::new();
        assert_eq!(o.value(b"k", b"small", 0, &mut out), Outcome::Inline);
        assert_eq!(o.value(b"gen_ai.input.messages", b"", 0, &mut out), Outcome::Inline, "empty is never offloaded");
        // listed and small: offloaded whole (not an array)
        assert_eq!(o.value(b"input.value", b"hi", 0, &mut out), Outcome::Replaced);
        let h = hex16(&payload_hash(&tenant_key("c1", b"", 1_790_000_000_000_000_000), b"hi"));
        assert_eq!(out, format!("[\"h:{h}\"]").into_bytes());
        // split
        let msgs = br#"[{"role":"system","content":"x"}, {"role":"user","content":"y"}]"#;
        assert_eq!(o.value(b"gen_ai.input.messages", msgs, 1, &mut out), Outcome::Replaced);
        let doc: Vec<String> = serde_json::from_slice(&out).unwrap();
        assert_eq!(doc.len(), 2);
        assert!(doc.iter().all(|r| r.len() == 34 && r.starts_with("h:")));
        // over the threshold, unlisted: offloaded whole, and truncated
        let big = vec![b'z'; 5000];
        assert_eq!(o.value(b"blob", &big, 2, &mut out), Outcome::Replaced);
        // redaction
        assert_eq!(o.value(b"secret", b"hunter2", 2, &mut out), Outcome::Replaced);
        assert!(out.is_empty());
        let m: Vec<(String, String)> = o.markers.iter().map(|(k, v)| (String::from_utf8(k.clone()).unwrap(), String::from_utf8(v.clone()).unwrap())).collect();
        assert_eq!(
            m,
            vec![
                ("otel.payload.input.value.bytes".into(), "2".into()),
                ("otel.payload.gen_ai.input.messages.bytes".into(), msgs.len().to_string()),
                ("otel.payload.gen_ai.input.messages.elements".into(), "2".into()),
                ("otel.payload.blob.bytes".into(), "4096".into()),
                ("otel.payload.blob.truncated_from".into(), "5000".into()),
                ("otel.payload.secret.redacted_from".into(), "7".into()),
            ]
        );
        assert_eq!(o.payloads.len(), 4);
        assert_eq!(o.payloads.iter().map(|p| p.first_row).collect::<Vec<_>>(), vec![0, 1, 1, 2]);
        assert_eq!(o.stats, OffloadStats { offloaded: 3, offloaded_bytes: (2 + msgs.len() + 4096) as u64, split: 1, truncated: 1, redacted: 1, ..Default::default() });
    }

    #[test]
    fn a_resent_conversation_shares_its_messages() {
        let mut o = off(OffloadOptions::default());
        let mut out = Vec::new();
        let _ = o.value(b"gen_ai.input.messages", br#"[{"role":"system","content":"s"},{"role":"user","content":"q1"}]"#, 0, &mut out);
        let _ = o.value(b"gen_ai.input.messages", br#"[{"role":"system","content":"s"},{"role":"user","content":"q1"},{"role":"assistant","content":"a1"},{"role":"user","content":"q2"}]"#, 1, &mut out);
        assert_eq!(o.payloads.len(), 4, "two of the second call's four messages were already there");
        let mut refs = crate::columns::Bin::default();
        refs.clear();
        assert_eq!(o.end_row(&mut refs), 4, "row references, distinct");
    }

    #[test]
    fn options_are_validated_together() {
        assert!(OffloadOptions::default().validate().is_ok());
        assert!(OffloadOptions { enabled: false, threshold: 0, ..Default::default() }.validate().is_ok());
        for bad in [
            OffloadOptions { threshold: 8 << 20, ..Default::default() },
            OffloadOptions { max_value: 256 << 20, ..Default::default() },
            OffloadOptions { max_request_bytes: 2 << 30, max_value: 2 << 30, ..Default::default() },
            OffloadOptions { split_max_depth: 0, ..Default::default() },
            OffloadOptions { split_max_elements: 0, ..Default::default() },
            OffloadOptions { cache_size: 0, ..Default::default() },
            OffloadOptions { split_keys: vec!["not.listed".into()], ..Default::default() },
            OffloadOptions { keys: vec!["otel.payload.x".into()], split_keys: vec![], ..Default::default() },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_cache_is_per_epoch_and_bounded() {
        let mut c = PayloadCache::default();
        let h = |i: u8| [i; 16];
        assert!(c.wants("e1", &h(1)));
        c.sent("e1", &[h(1), h(2)], 100);
        assert!(!c.wants("e1", &h(1)));
        assert!(c.wants("e2", &h(1)));
        c.sent("e2", &[h(3)], 100);
        assert_eq!(c.len(), 1);
        c.sent("e2", &(10..30).map(h).collect::<Vec<_>>(), 16);
        assert!(c.len() <= 16 && c.evicted > 0);
        assert!(c.wants("e2", &h(3)));
    }
}
