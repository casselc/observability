//! What the consumer needs from the bucket, with a request count per kind:
//! object_store's S3 client (the same `S3Config` as the exporter), and an
//! in-memory bucket with conditional-write semantics and fault injection.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{Attribute, AttributeValue, Attributes, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use otap_s3pq::store::{S3Store, chain};
use serde::Serialize;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};

pub type Meta = HashMap<String, String>;

/// A PUT's precondition.
#[derive(Clone, Copy, Debug)]
pub enum Cond<'a> {
    None,
    /// `If-None-Match: *`
    Create,
    /// `If-Match: <etag>`
    IfMatch(&'a str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Put {
    /// Applied; the new ETag.
    Ok(String),
    /// 412 (or 404 on If-Match): the precondition failed.
    Conflict,
    /// No answer: it may or may not have applied. Resolve by reading.
    Unknown(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    /// LastModified (the store's clock), ms.
    pub modified_ms: u64,
}

/// Requests sent, by kind.
#[derive(Default, Debug)]
pub struct Counts {
    pub list: Cell<u64>,
    pub get: Cell<u64>,
    pub head: Cell<u64>,
    pub put_create: Cell<u64>,
    pub put_cas: Cell<u64>,
    pub put_plain: Cell<u64>,
    pub delete: Cell<u64>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub struct CountSnap {
    pub list: u64,
    pub get: u64,
    pub head: u64,
    pub put_create: u64,
    pub put_cas: u64,
    pub put_plain: u64,
    pub delete: u64,
}

impl Counts {
    pub fn snap(&self) -> CountSnap {
        CountSnap {
            list: self.list.get(),
            get: self.get.get(),
            head: self.head.get(),
            put_create: self.put_create.get(),
            put_cas: self.put_cas.get(),
            put_plain: self.put_plain.get(),
            delete: self.delete.get(),
        }
    }
}

fn inc(c: &Cell<u64>) {
    c.set(c.get() + 1);
}

#[async_trait(?Send)]
pub trait Bucket {
    /// GET: the body and its ETag, `None` for 404.
    async fn get(&self, key: &str) -> Result<Option<(Bytes, String)>, String>;
    async fn put(&self, key: &str, body: Bytes, cond: Cond<'_>, meta: &BTreeMap<String, String>) -> Put;
    /// HEAD: the user metadata (lower-case keys without `x-amz-meta-`), `None` for 404.
    async fn head(&self, key: &str) -> Result<Option<Meta>, String>;
    /// Every key under `prefix/` after `start_after`, in order (LIST v2, paged).
    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<Item>, String>;
    /// The immediate "directories" under `prefix/`, sorted (LIST with delimiter).
    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, String>;
    /// Deletes keys (DeleteObjects, 1,000 per request); returns how many.
    async fn delete(&self, keys: &[String]) -> Result<usize, String>;
    fn counts(&self) -> &Counts;
    /// The URL ClickHouse's s3() reads `key` at, and `_path` for it.
    fn object_url(&self, key: &str) -> String;
    fn path_of(&self, key: &str) -> String;
}

// ---- S3 ------------------------------------------------------------------------

pub struct S3Bucket {
    pub s3: S3Store,
    pub counts: Counts,
    /// The endpoint as ClickHouse sees it, if it differs ("http://host:port/bucket").
    pub ch_endpoint: Option<String>,
}

impl S3Bucket {
    pub fn new(s3: S3Store) -> Self {
        Self { s3, counts: Counts::default(), ch_endpoint: None }
    }
}

#[async_trait(?Send)]
impl Bucket for S3Bucket {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, String)>, String> {
        inc(&self.counts.get);
        match self.s3.store.get_opts(&Path::from(key), GetOptions::default()).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone().unwrap_or_default();
                let b = r.bytes().await.map_err(|e| format!("get {key}: {}", chain(&e)))?;
                Ok(Some((b, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(format!("get {key}: {}", chain(&e))),
        }
    }

    async fn put(&self, key: &str, body: Bytes, cond: Cond<'_>, meta: &BTreeMap<String, String>) -> Put {
        let mut attrs = Attributes::new();
        let _ = attrs.insert(Attribute::ContentType, AttributeValue::from("application/json"));
        for (k, v) in meta {
            let _ = attrs.insert(Attribute::Metadata(k.clone().into()), AttributeValue::from(v.clone()));
        }
        let mode = match cond {
            Cond::None => {
                inc(&self.counts.put_plain);
                PutMode::Overwrite
            }
            Cond::Create => {
                inc(&self.counts.put_create);
                PutMode::Create
            }
            Cond::IfMatch(e) => {
                inc(&self.counts.put_cas);
                PutMode::Update(UpdateVersion { e_tag: Some(e.to_string()), version: None })
            }
        };
        let opts = PutOptions { mode, attributes: attrs, ..Default::default() };
        match self.s3.store.put_opts(&Path::from(key), PutPayload::from_bytes(body), opts).await {
            Ok(r) => Put::Ok(r.e_tag.unwrap_or_default()),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotFound { .. },
            ) => Put::Conflict,
            Err(e) => Put::Unknown(format!("put {key}: {}", chain(&e))),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<Meta>, String> {
        inc(&self.counts.head);
        let opts = GetOptions { head: true, ..Default::default() };
        match self.s3.store.get_opts(&Path::from(key), opts).await {
            Ok(r) => {
                let mut m = Meta::new();
                for (k, v) in r.attributes.iter() {
                    if let Attribute::Metadata(name) = k {
                        let _ = m.insert(name.to_ascii_lowercase(), v.as_ref().to_string());
                    }
                }
                Ok(Some(m))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(format!("head {key}: {}", chain(&e))),
        }
    }

    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<Item>, String> {
        use futures::StreamExt;
        let p = Path::from(prefix.trim_end_matches('/'));
        let mut stream = match start_after {
            Some(s) => self.s3.store.list_with_offset(Some(&p), &Path::from(s)),
            None => self.s3.store.list(Some(&p)),
        };
        // One request per page of 1,000 (object_store pages internally).
        let mut out = Vec::new();
        inc(&self.counts.list);
        while let Some(m) = stream.next().await {
            let m = m.map_err(|e| format!("list {prefix}: {}", chain(&e)))?;
            if !out.is_empty() && out.len() % 1000 == 0 {
                inc(&self.counts.list);
            }
            out.push(Item { key: m.location.to_string(), size: m.size, etag: m.e_tag, modified_ms: m.last_modified.timestamp_millis().max(0) as u64 });
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, String> {
        inc(&self.counts.list);
        let p = Path::from(prefix.trim_end_matches('/'));
        let r = self.s3.store.list_with_delimiter(Some(&p)).await.map_err(|e| format!("list {prefix}: {}", chain(&e)))?;
        let mut out: Vec<String> = r.common_prefixes.iter().filter_map(|c| c.filename().map(str::to_string)).collect();
        out.sort();
        Ok(out)
    }

    async fn delete(&self, keys: &[String]) -> Result<usize, String> {
        use futures::StreamExt;
        if keys.is_empty() {
            return Ok(0);
        }
        for _ in 0..keys.len().div_ceil(1000) {
            inc(&self.counts.delete);
        }
        let paths: Vec<object_store::Result<Path>> = keys.iter().map(|k| Ok(Path::from(k.as_str()))).collect();
        let mut s = self.s3.store.delete_stream(futures::stream::iter(paths).boxed());
        let mut n = 0;
        while let Some(r) = s.next().await {
            match r {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => n += 1,
                Err(e) => return Err(format!("delete: {}", chain(&e))),
            }
        }
        Ok(n)
    }

    fn counts(&self) -> &Counts {
        &self.counts
    }

    fn object_url(&self, key: &str) -> String {
        match &self.ch_endpoint {
            Some(e) => format!("{}/{key}", e.trim_end_matches('/')),
            None => self.s3.object_url(key),
        }
    }

    fn path_of(&self, key: &str) -> String {
        format!("{}/{key}", self.s3.bucket)
    }
}

// ---- in memory -------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct MemObj {
    pub body: Bytes,
    pub meta: BTreeMap<String, String>,
    pub etag: String,
    pub modified_ms: u64,
}

/// Faults on PUTs to keys containing `matching`.
#[derive(Clone, Debug, Default)]
pub struct MemFaults {
    pub matching: String,
    /// Every n-th matching PUT is applied, but answered as Unknown.
    pub ambiguous_every: u64,
    /// Every n-th matching PUT is dropped, and answered as Unknown.
    pub drop_every: u64,
}

#[derive(Default)]
pub struct MemBucket {
    pub objs: RefCell<BTreeMap<String, MemObj>>,
    pub counts: Counts,
    pub faults: RefCell<MemFaults>,
    /// The store's clock for LastModified (a shared test clock).
    pub clock: std::rc::Rc<Cell<u64>>,
    pub n: Cell<u64>,
    pub puts: Cell<u64>,
}

impl MemBucket {
    pub fn insert(&self, key: &str, body: Bytes, meta: BTreeMap<String, String>) {
        let e = self.next_etag();
        let _ = self.objs.borrow_mut().insert(key.to_string(), MemObj { body, meta, etag: e, modified_ms: self.clock.get() });
    }
    fn next_etag(&self) -> String {
        self.n.set(self.n.get() + 1);
        format!("\"m{}\"", self.n.get())
    }
    pub fn keys(&self) -> Vec<String> {
        self.objs.borrow().keys().cloned().collect()
    }
}

#[async_trait(?Send)]
impl Bucket for MemBucket {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, String)>, String> {
        inc(&self.counts.get);
        Ok(self.objs.borrow().get(key).map(|o| (o.body.clone(), o.etag.clone())))
    }

    async fn put(&self, key: &str, body: Bytes, cond: Cond<'_>, meta: &BTreeMap<String, String>) -> Put {
        match cond {
            Cond::None => inc(&self.counts.put_plain),
            Cond::Create => inc(&self.counts.put_create),
            Cond::IfMatch(_) => inc(&self.counts.put_cas),
        }
        let f = self.faults.borrow().clone();
        let faulty = !f.matching.is_empty() && key.contains(&f.matching);
        let n = if faulty {
            self.puts.set(self.puts.get() + 1);
            self.puts.get()
        } else {
            0
        };
        if faulty && f.drop_every > 0 && n % f.drop_every == 0 {
            return Put::Unknown("dropped (injected)".into());
        }
        let cur = self.objs.borrow().get(key).map(|o| o.etag.clone());
        let ok = match cond {
            Cond::None => true,
            Cond::Create => cur.is_none(),
            Cond::IfMatch(e) => cur.as_deref() == Some(e),
        };
        if !ok {
            return Put::Conflict;
        }
        let e = self.next_etag();
        let _ = self.objs.borrow_mut().insert(key.to_string(), MemObj { body, meta: meta.clone(), etag: e.clone(), modified_ms: self.clock.get() });
        if faulty && f.ambiguous_every > 0 && n % f.ambiguous_every == 0 {
            return Put::Unknown("answer lost (injected)".into());
        }
        Put::Ok(e)
    }

    async fn head(&self, key: &str) -> Result<Option<Meta>, String> {
        inc(&self.counts.head);
        Ok(self.objs.borrow().get(key).map(|o| o.meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
    }

    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<Item>, String> {
        inc(&self.counts.list);
        let p = format!("{}/", prefix.trim_end_matches('/'));
        Ok(self
            .objs
            .borrow()
            .iter()
            .filter(|(k, _)| k.starts_with(&p) && start_after.is_none_or(|s| k.as_str() > s))
            .map(|(k, o)| Item { key: k.clone(), size: o.body.len() as u64, etag: Some(o.etag.clone()), modified_ms: o.modified_ms })
            .collect())
    }

    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, String> {
        inc(&self.counts.list);
        let p = format!("{}/", prefix.trim_end_matches('/'));
        let mut out: Vec<String> = self
            .objs
            .borrow()
            .keys()
            .filter_map(|k| k.strip_prefix(&p).and_then(|r| r.split_once('/')).map(|(d, _)| d.to_string()))
            .collect();
        out.dedup();
        Ok(out)
    }

    async fn delete(&self, keys: &[String]) -> Result<usize, String> {
        for _ in 0..keys.len().div_ceil(1000) {
            inc(&self.counts.delete);
        }
        let mut o = self.objs.borrow_mut();
        for k in keys {
            let _ = o.remove(k);
        }
        Ok(keys.len())
    }

    fn counts(&self) -> &Counts {
        &self.counts
    }
    fn object_url(&self, key: &str) -> String {
        format!("mem://{key}")
    }
    fn path_of(&self, key: &str) -> String {
        format!("mem/{key}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn mem_cas() {
        let b = MemBucket::default();
        let m = BTreeMap::new();
        let Put::Ok(e1) = b.put("c/x", Bytes::from("1"), Cond::Create, &m).await else { panic!() };
        assert_eq!(b.put("c/x", Bytes::from("2"), Cond::Create, &m).await, Put::Conflict);
        let Put::Ok(e2) = b.put("c/x", Bytes::from("2"), Cond::IfMatch(&e1), &m).await else { panic!() };
        assert_eq!(b.put("c/x", Bytes::from("3"), Cond::IfMatch(&e1), &m).await, Put::Conflict);
        assert_eq!(b.get("c/x").await.unwrap().unwrap(), (Bytes::from("2"), e2));
        b.insert("c/y/a/1", Bytes::new(), m.clone());
        b.insert("c/y/b/1", Bytes::new(), m.clone());
        assert_eq!(b.list_dirs("c/y").await.unwrap(), vec!["a", "b"]);
        assert_eq!(b.list("c/y", Some("c/y/a/1")).await.unwrap().len(), 1);
        *b.faults.borrow_mut() = MemFaults { matching: "z".into(), ambiguous_every: 1, drop_every: 0 };
        assert!(matches!(b.put("z", Bytes::new(), Cond::Create, &m).await, Put::Unknown(_)));
        assert!(b.get("z").await.unwrap().is_some(), "applied, answer lost");
        assert_eq!(b.counts.snap().put_create, 3);
    }
}
