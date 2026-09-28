//! `resource_id` at the edge, and the announcement lane's cache
//! (`../../entities/README.md` §3.1, §6.3; `../../FORMAT.md` §2).
//!
//! **resource_id** is a content address of a resource's *covered*
//! attributes, the ones the entity catalog can reproduce:
//!
//! ```text
//! resource_id = xxh3_64( "res.v1\0" ‖ for (k, v) in covered, sorted by k bytewise: k ‖ "\0" ‖ v ‖ "\0" )
//! ```
//!
//! byte for byte the entity controller's `internal/rid.ID(rid.Split(...))`
//! and parquetgo's `resource.go`; `../../entities/testdata/resource_id_vectors.json`
//! holds the vectors all three are tested against. The covered set is the
//! FIRST occurrence of each key, kept when the key is in `COVERED_KEYS` or is
//! `k8s.pod.label.` plus a non-empty rest, and the value is a non-empty
//! string without NUL (the hash's separator). Everything else is residual.
//!
//! **Announcements** ride in the data object itself (`resource_announce`,
//! the covered set, on the first row of each resource the object announces):
//! an announcement commits exactly when the rows that need it do, in the
//! data's own lane and epoch, and the consumer inserts it before those rows
//! (`../../model/entityCatalog.qnt`, `sameLane`/`sameObject`). Which
//! resources an object announces is decided when it is encoded for its slot,
//! from `AnnounceCache`: a resource is announced once per window per lane
//! epoch, and counts as announced only once an object carrying it has
//! committed (`announced`). A new epoch starts empty; the cache is bounded
//! and whatever it evicts is simply announced again.

use std::collections::HashMap;
use xxhash_rust::xxh3::xxh3_64;

/// The covered keys, sorted (the controller's `rid.CoveredKeys`).
pub const COVERED_KEYS: [&str; 27] = [
    "cloud.account.id",
    "cloud.availability.zone",
    "cloud.platform",
    "cloud.provider",
    "cloud.region",
    "container.image.name",
    "container.image.tag",
    "deployment.environment.name",
    "host.id",
    "host.name",
    "host.type",
    "k8s.cluster.name",
    "k8s.cluster.uid",
    "k8s.container.name",
    "k8s.cronjob.name",
    "k8s.daemonset.name",
    "k8s.deployment.name",
    "k8s.job.name",
    "k8s.namespace.name",
    "k8s.node.name",
    "k8s.node.uid",
    "k8s.pod.name",
    "k8s.pod.start_time",
    "k8s.pod.uid",
    "k8s.replicaset.name",
    "k8s.statefulset.name",
    "service.name",
];

/// Every `k8s.pod.label.<l>` is covered (the agent extracts the same label
/// list the controller derives).
pub const LABEL_PREFIX: &[u8] = b"k8s.pod.label.";

const DOMAIN: &[u8] = b"res.v1\0";

/// Whether a key is covered, by name.
pub fn is_covered_key(k: &[u8]) -> bool {
    if k.len() > LABEL_PREFIX.len() && k.starts_with(LABEL_PREFIX) {
        return !k.contains(&0);
    }
    COVERED_KEYS.binary_search_by(|c| c.as_bytes().cmp(k)).is_ok()
}

/// A resource's covered set (sorted by key bytes) and its id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Covered {
    pub id: u64,
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The id of a covered set whose pairs are sorted by key and have no empty
/// value (what `CoveredBuilder::finish` returns).
pub fn resource_id(pairs: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    let n = DOMAIN.len() + pairs.iter().map(|(k, v)| k.len() + v.len() + 2).sum::<usize>();
    let mut b = Vec::with_capacity(n);
    b.extend_from_slice(DOMAIN);
    for (k, v) in pairs {
        b.extend_from_slice(k);
        b.push(0);
        b.extend_from_slice(v);
        b.push(0);
    }
    xxh3_64(&b)
}

/// The id of the empty covered set: rows of a resource without any covered
/// attribute (such a resource is never announced).
pub fn empty_id() -> u64 {
    xxh3_64(DOMAIN)
}

/// Collects a resource's attributes, in their order, into its covered set.
#[derive(Default)]
pub struct CoveredBuilder {
    seen: Vec<Vec<u8>>,
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl CoveredBuilder {
    /// One attribute: `v` is the value's bytes if it is a string, `None`
    /// for any other type.
    pub fn push(&mut self, k: &[u8], v: Option<&[u8]>) {
        if !is_covered_key(k) {
            return; // never covered, so its duplicates can't matter
        }
        if self.seen.iter().any(|s| s == k) {
            return; // the first occurrence decides (pcommon Map.Get)
        }
        self.seen.push(k.to_vec());
        if let Some(v) = v
            && !v.is_empty()
            && !v.contains(&0)
        {
            self.pairs.push((k.to_vec(), v.to_vec()));
        }
    }

    pub fn finish(mut self) -> Covered {
        self.pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Covered { id: resource_id(&self.pairs), pairs: self.pairs }
    }
}

/// The distinct resources of one object, in the order the walk first met
/// them, and each row's resource.
#[derive(Clone, Debug, Default)]
pub struct Resources {
    /// Per row.
    pub ids: Vec<u64>,
    /// Per distinct non-empty resource: its covered set and the first row
    /// that uses it (the row an announcement rides on).
    pub entries: Vec<(Covered, u32)>,
    index: HashMap<u64, usize>,
}

impl Resources {
    pub fn clear(&mut self) {
        self.ids.clear();
        self.entries.clear();
        self.index.clear();
    }

    /// A new resource (a `ResourceSpans` / `ResourceLogs`): its id, which
    /// `row` then stamps on each of its rows.
    pub fn resource(&mut self, c: Covered) -> u64 {
        let id = c.id;
        if !c.pairs.is_empty() && !self.index.contains_key(&id) {
            let _ = self.index.insert(id, self.entries.len());
            // first_row is set at the resource's first row
            self.entries.push((c, u32::MAX));
        }
        id
    }

    #[inline]
    pub fn row(&mut self, id: u64) {
        if let Some(&i) = self.index.get(&id)
            && self.entries[i].1 == u32::MAX
        {
            self.entries[i].1 = self.ids.len() as u32;
        }
        self.ids.push(id);
    }

    /// Takes the per-row ids and the entries that have rows (a resource with
    /// no row announces nothing), resetting the collector.
    pub fn take(&mut self) -> (Vec<u64>, Vec<(Covered, u32)>) {
        self.index.clear();
        let entries = std::mem::take(&mut self.entries).into_iter().filter(|e| e.1 != u32::MAX).collect();
        (std::mem::take(&mut self.ids), entries)
    }
}

/// Announcement options (`resources:` in the exporter's config).
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceOptions {
    /// Write `resource_announce` (the covered set of resources not yet
    /// announced in the lane's epoch and window). Off: every object's
    /// `resource_announce` is empty; `resource_id` is always written.
    pub announce: bool,
    /// A resource is announced again once per window (by the request's
    /// `received_at`): central keeps first- and last-seen evidence.
    #[serde(with = "humantime_serde")]
    pub window: std::time::Duration,
    /// Resources remembered per lane; beyond it the least recently
    /// announced are forgotten (and announced again when next seen).
    pub cache_size: usize,
}

impl Default for ResourceOptions {
    fn default() -> Self {
        ResourceOptions { announce: true, window: std::time::Duration::from_secs(3600), cache_size: 65536 }
    }
}

impl ResourceOptions {
    /// The window a request received at `received_ns` falls in.
    pub fn window_of(&self, received_ns: u64) -> i64 {
        (received_ns / (self.window.as_nanos().max(1) as u64)) as i64
    }
}

/// One lane's announced resources, for the lane's current epoch.
#[derive(Clone, Debug, Default)]
pub struct AnnounceCache {
    epoch: String,
    /// id -> (window it was last announced in, last use)
    map: HashMap<u64, (i64, u64)>,
    tick: u64,
    pub evicted: u64,
}

impl AnnounceCache {
    /// Whether an object in `epoch`, window `w`, must announce `id`.
    pub fn wants(&self, epoch: &str, id: u64, w: i64) -> bool {
        self.epoch != epoch || self.map.get(&id).map(|e| e.0) != Some(w)
    }

    /// Marks resources announced: call only once an object carrying them
    /// has committed in `epoch`. A commit in another epoch empties the
    /// cache first. Over `cap`, the least recently announced eighth goes.
    pub fn announced(&mut self, epoch: &str, ids: &[u64], w: i64, cap: usize) {
        if self.epoch != epoch {
            self.map.clear();
            self.epoch = epoch.to_string();
        }
        for &id in ids {
            self.tick += 1;
            let _ = self.map.insert(id, (w, self.tick));
        }
        let cap = cap.max(1);
        if self.map.len() > cap {
            let keep = cap - cap / 8;
            let mut v: Vec<(u64, u64)> = self.map.iter().map(|(id, e)| (e.1, *id)).collect();
            v.sort_unstable();
            for (_, id) in &v[..v.len() - keep] {
                let _ = self.map.remove(id);
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

    fn cov(kv: &[(&str, Option<&str>)]) -> Covered {
        let mut b = CoveredBuilder::default();
        for (k, v) in kv {
            b.push(k.as_bytes(), v.map(str::as_bytes));
        }
        b.finish()
    }

    #[test]
    fn covered_keys_are_sorted_and_match_the_prefix_rule() {
        assert!(COVERED_KEYS.windows(2).all(|w| w[0] < w[1]));
        assert!(is_covered_key(b"k8s.pod.name") && is_covered_key(b"k8s.pod.label.team"));
        assert!(!is_covered_key(b"k8s.pod.label.") && !is_covered_key(b"k8s.pod.labels.x") && !is_covered_key(b"service.version"));
    }

    #[test]
    fn the_clickhouse_reference_vector() {
        let c = cov(&[("k8s.pod.name", Some("a-1")), ("k8s.namespace.name", Some("ns")), ("empty", Some(""))]);
        assert_eq!(c.id, 18114781823887046220);
        assert_eq!(cov(&[]).id, empty_id());
    }

    #[test]
    fn first_occurrence_decides_and_non_strings_are_residual() {
        let a = cov(&[("k8s.pod.name", Some("a")), ("k8s.pod.name", Some("b")), ("k8s.node.name", None), ("k8s.node.name", Some("n"))]);
        assert_eq!(a.pairs, vec![(b"k8s.pod.name".to_vec(), b"a".to_vec())]);
    }

    #[test]
    fn resources_keep_first_rows_and_skip_the_empty_set() {
        let mut r = Resources::default();
        let a = r.resource(cov(&[("k8s.pod.name", Some("a"))]));
        r.row(a);
        r.row(a);
        let e = r.resource(cov(&[("x", Some("y"))]));
        r.row(e);
        let a2 = r.resource(cov(&[("k8s.pod.name", Some("a"))]));
        r.row(a2);
        let _unused = r.resource(cov(&[("k8s.pod.name", Some("z"))]));
        let (ids, entries) = r.take();
        assert_eq!(ids, vec![a, a, empty_id(), a]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, 0);
    }

    #[test]
    fn the_cache_is_per_epoch_windowed_and_bounded() {
        let mut c = AnnounceCache::default();
        assert!(c.wants("e1", 1, 10));
        c.announced("e1", &[1, 2], 10, 100);
        assert!(!c.wants("e1", 1, 10));
        assert!(c.wants("e1", 1, 11), "a new window announces again");
        assert!(c.wants("e2", 1, 10), "another epoch announces again");
        c.announced("e2", &[3], 10, 100);
        assert_eq!(c.len(), 1, "a commit in a new epoch empties the cache");
        c.announced("e2", &(10..30).collect::<Vec<_>>(), 10, 16);
        assert!(c.len() <= 16 && c.evicted > 0);
        assert!(c.wants("e2", 3, 10), "the oldest went first");
        assert!(!c.wants("e2", 29, 10));
    }
}
