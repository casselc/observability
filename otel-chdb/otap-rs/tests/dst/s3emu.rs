//! An in-memory S3 that speaks enough of the REST API for object_store's
//! AmazonS3 client (path-style, SigV4 ignored): PUT with `If-None-Match: *`
//! / `If-Match`, GET, HEAD, LIST v2 (prefix, start-after, delimiter,
//! continuation), DeleteObjects. Strongly consistent like AWS S3 and
//! SeaweedFS, except where a fault says otherwise.
//!
//! Faults (AMBIGUITY.md's S3 rows) are decided per request by `Faults::decide`:
//! a conditional PUT applied then answered 500 (the client's retry meets its
//! own write: 412), a PUT applied and the connection dropped before the
//! answer, a request refused with 503, and (a switch) LIST not showing
//! objects younger than a lag. `tests/dst_net.rs` checks the emulator
//! against SeaweedFS (`s3_emulator_matches_seaweedfs`).

#![allow(dead_code)]

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct Obj {
    pub body: Bytes,
    pub meta: BTreeMap<String, String>,
    pub etag: String,
    pub modified_ms: u64,
    pub content_type: String,
}

/// What happens to one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    None,
    /// 503 SlowDown, nothing applied.
    Refuse,
    /// Applied, then answered 500 InternalError.
    ErrorAfter,
    /// Applied, then the connection closes without an answer.
    DropAfter,
    /// Never applied, the connection closes.
    DropBefore,
}

pub trait Faults {
    /// `op`: GET, HEAD, PUT, PUT-CREATE, PUT-IFMATCH, LIST, DELETE.
    fn decide(&self, op: &str, key: &str) -> Fault;
    /// LIST hides objects younger than this (ms; 0: consistent).
    fn list_lag_ms(&self) -> u64 {
        0
    }
}

pub struct NoFaults;
impl Faults for NoFaults {
    fn decide(&self, _: &str, _: &str) -> Fault {
        Fault::None
    }
}

pub struct S3Emu {
    pub bucket: String,
    pub objs: RefCell<BTreeMap<String, Obj>>,
    n: Cell<u64>,
    /// Wall clock, ms.
    pub clock: Box<dyn Fn() -> u64>,
    pub faults: RefCell<Rc<dyn Faults>>,
    /// Called after every applied PUT / DELETE (key, the object or None).
    pub on_change: RefCell<Option<Box<dyn Fn(&str, Option<&Obj>)>>>,
    pub log: RefCell<Option<Box<dyn Fn(&str)>>>,
}

/// What the handler decided: an answer, or a dropped connection.
pub enum Answer {
    Http(Response<Bytes>),
    Drop,
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&apos;")
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

pub fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Days since 1970-01-01 -> (y, m, d) (Howard Hinnant's civil_from_days).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn iso8601(ms: u64) -> String {
    let s = ms / 1000;
    let (y, mo, d) = civil((s / 86_400) as i64);
    let r = s % 86_400;
    format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z", r / 3600, r / 60 % 60, r % 60, ms % 1000)
}

pub fn http_date(ms: u64) -> String {
    let s = ms / 1000;
    let days = (s / 86_400) as i64;
    let (y, mo, d) = civil(days);
    let wd = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"][days.rem_euclid(7) as usize];
    let mon = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][mo as usize - 1];
    let r = s % 86_400;
    format!("{wd}, {d:02} {mon} {y:04} {:02}:{:02}:{:02} GMT", r / 3600, r / 60 % 60, r % 60)
}

fn resp(status: StatusCode, body: impl Into<Bytes>) -> Response<Bytes> {
    let mut r = Response::new(body.into());
    *r.status_mut() = status;
    r
}

fn error(status: StatusCode, code: &str, msg: &str, resource: &str) -> Response<Bytes> {
    let mut r = resp(
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{}</Message><Resource>{}</Resource><RequestId>sim</RequestId></Error>",
            xml_escape(msg),
            xml_escape(resource)
        ),
    );
    let _ = r.headers_mut().insert("content-type", "application/xml".parse().unwrap());
    r
}

impl S3Emu {
    pub fn new(bucket: &str, clock: Box<dyn Fn() -> u64>) -> Self {
        S3Emu {
            bucket: bucket.into(),
            objs: RefCell::new(BTreeMap::new()),
            n: Cell::new(0),
            clock,
            faults: RefCell::new(Rc::new(NoFaults)),
            on_change: RefCell::new(None),
            log: RefCell::new(None),
        }
    }

    fn log(&self, line: &str) {
        if let Some(f) = self.log.borrow().as_ref() {
            f(line);
        }
    }

    fn etag(&self, body: &[u8]) -> String {
        self.n.set(self.n.get() + 1);
        // Distinct per write even for equal bodies (as S3's MD5 is not: the
        // consumer relies on no ETag property but change detection).
        format!("\"{:016x}{:016x}\"", self.n.get(), body.len())
    }

    /// Handles one request. `DropAfter` / `ErrorAfter` apply first.
    pub fn handle(&self, req: Request<Bytes>) -> Answer {
        let path = req.uri().path().to_string();
        let query: BTreeMap<String, String> =
            url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes()).map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
        let rest = path.trim_start_matches('/');
        let (bucket, key) = match rest.split_once('/') {
            Some((b, k)) => (b.to_string(), pct_decode(k)),
            None => (rest.to_string(), String::new()),
        };
        if bucket != self.bucket {
            return Answer::Http(error(StatusCode::NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist", &bucket));
        }
        let m = req.method().clone();
        let op = match (&m, key.is_empty()) {
            (&Method::PUT, false) => {
                if req.headers().contains_key("if-none-match") {
                    "PUT-CREATE"
                } else if req.headers().contains_key("if-match") {
                    "PUT-IFMATCH"
                } else {
                    "PUT"
                }
            }
            (&Method::GET, false) => "GET",
            (&Method::HEAD, false) => "HEAD",
            (&Method::GET, true) => "LIST",
            (&Method::POST, true) if query.contains_key("delete") => "DELETE",
            _ => return Answer::Http(error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", "not emulated", &path)),
        };
        let fault = self.faults.borrow().decide(op, &key);
        match fault {
            Fault::Refuse => {
                self.log(&format!("S3 {op} {key} -> 503 (injected)"));
                return Answer::Http(error(StatusCode::SERVICE_UNAVAILABLE, "SlowDown", "Please reduce your request rate.", &key));
            }
            Fault::DropBefore => {
                self.log(&format!("S3 {op} {key} -> connection dropped before (injected)"));
                return Answer::Drop;
            }
            _ => {}
        }
        let r = match op {
            "PUT" | "PUT-CREATE" | "PUT-IFMATCH" => self.put(&key, &req),
            "GET" | "HEAD" => self.get(&key, op == "HEAD", &req),
            "LIST" => self.list(&query),
            "DELETE" => self.delete(req.body()),
            _ => unreachable!(),
        };
        let st = r.status();
        match fault {
            Fault::ErrorAfter => {
                self.log(&format!("S3 {op} {key} -> {st}, answered 500 (injected)"));
                Answer::Http(error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", "We encountered an internal error. Please try again.", &key))
            }
            Fault::DropAfter => {
                self.log(&format!("S3 {op} {key} -> {st}, connection dropped (injected)"));
                Answer::Drop
            }
            _ => {
                self.log(&format!("S3 {op} {key} -> {st}{}", r.headers().get("etag").map(|e| format!(" {}", e.to_str().unwrap_or(""))).unwrap_or_default()));
                Answer::Http(r)
            }
        }
    }

    fn put(&self, key: &str, req: &Request<Bytes>) -> Response<Bytes> {
        let h = req.headers();
        let cur = self.objs.borrow().get(key).map(|o| o.etag.clone());
        if let Some(v) = h.get("if-none-match") {
            if v.to_str().unwrap_or("") == "*" && cur.is_some() {
                return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", key);
            }
        }
        if let Some(v) = h.get("if-match") {
            let want = v.to_str().unwrap_or("");
            match &cur {
                None => return error(StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.", key),
                Some(e) if e != want && e.trim_matches('"') != want.trim_matches('"') => {
                    return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", key);
                }
                _ => {}
            }
        }
        let mut meta = BTreeMap::new();
        for (k, v) in h.iter() {
            if let Some(name) = k.as_str().strip_prefix("x-amz-meta-") {
                let _ = meta.insert(name.to_string(), v.to_str().unwrap_or("").to_string());
            }
        }
        let body = req.body().clone();
        let etag = self.etag(&body);
        let obj = Obj {
            body,
            meta,
            etag: etag.clone(),
            modified_ms: (self.clock)(),
            content_type: h.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("binary/octet-stream").to_string(),
        };
        let _ = self.objs.borrow_mut().insert(key.to_string(), obj.clone());
        if let Some(f) = self.on_change.borrow().as_ref() {
            f(key, Some(&obj));
        }
        let mut r = resp(StatusCode::OK, Bytes::new());
        let _ = r.headers_mut().insert("etag", etag.parse().unwrap());
        r
    }

    fn get(&self, key: &str, head: bool, req: &Request<Bytes>) -> Response<Bytes> {
        let Some(o) = self.objs.borrow().get(key).cloned() else {
            return if head { resp(StatusCode::NOT_FOUND, Bytes::new()) } else { error(StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.", key) };
        };
        if let Some(v) = req.headers().get("if-match") {
            if v.to_str().unwrap_or("") != o.etag {
                return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", key);
            }
        }
        let mut r = resp(StatusCode::OK, if head { Bytes::new() } else { o.body.clone() });
        let hs = r.headers_mut();
        let _ = hs.insert("etag", o.etag.parse().unwrap());
        let _ = hs.insert("last-modified", http_date(o.modified_ms).parse().unwrap());
        let _ = hs.insert("content-length", o.body.len().to_string().parse().unwrap());
        let _ = hs.insert("content-type", o.content_type.parse().unwrap_or_else(|_| "binary/octet-stream".parse().unwrap()));
        let _ = hs.insert("accept-ranges", "bytes".parse().unwrap());
        for (k, v) in &o.meta {
            if let (Ok(n), Ok(v)) = (http::HeaderName::from_bytes(format!("x-amz-meta-{k}").as_bytes()), http::HeaderValue::from_str(v)) {
                let _ = hs.insert(n, v);
            }
        }
        r
    }

    fn list(&self, q: &BTreeMap<String, String>) -> Response<Bytes> {
        let prefix = q.get("prefix").cloned().unwrap_or_default();
        let delim = q.get("delimiter").cloned().filter(|d| !d.is_empty());
        let after = q.get("continuation-token").cloned().or_else(|| q.get("start-after").cloned()).unwrap_or_default();
        let max: usize = q.get("max-keys").and_then(|v| v.parse().ok()).unwrap_or(1000).min(1000);
        let lag = self.faults.borrow().list_lag_ms();
        let now = (self.clock)();
        let objs = self.objs.borrow();
        let mut contents = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut last = String::new();
        let mut truncated = false;
        for (k, o) in objs.range(after.clone()..) {
            if !k.starts_with(&prefix) || *k <= after {
                if k.as_str() > prefix.as_str() && !k.starts_with(&prefix) {
                    break;
                }
                continue;
            }
            if lag > 0 && o.modified_ms + lag > now {
                continue;
            }
            if contents.len() + prefixes.len() >= max {
                truncated = true;
                break;
            }
            if let Some(d) = &delim {
                if let Some(i) = k[prefix.len()..].find(d.as_str()) {
                    let p = k[..prefix.len() + i + d.len()].to_string();
                    if prefixes.last() != Some(&p) {
                        prefixes.push(p);
                    }
                    last = k.clone();
                    continue;
                }
            }
            contents.push((k.clone(), o.clone()));
            last = k.clone();
        }
        let mut x = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        x.push_str(&format!("<Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{max}</MaxKeys>", self.bucket, xml_escape(&prefix)));
        if let Some(d) = &delim {
            x.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(d)));
        }
        x.push_str(&format!("<KeyCount>{}</KeyCount><IsTruncated>{truncated}</IsTruncated>", contents.len() + prefixes.len()));
        if truncated {
            x.push_str(&format!("<NextContinuationToken>{}</NextContinuationToken>", xml_escape(&last)));
        }
        for (k, o) in &contents {
            x.push_str(&format!(
                "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                xml_escape(k),
                iso8601(o.modified_ms),
                xml_escape(&o.etag),
                o.body.len()
            ));
        }
        for p in &prefixes {
            x.push_str(&format!("<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>", xml_escape(p)));
        }
        x.push_str("</ListBucketResult>");
        let mut r = resp(StatusCode::OK, x);
        let _ = r.headers_mut().insert("content-type", "application/xml".parse().unwrap());
        r
    }

    fn delete(&self, body: &Bytes) -> Response<Bytes> {
        let s = String::from_utf8_lossy(body);
        let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        let mut rest = s.as_ref();
        while let Some(i) = rest.find("<Key>") {
            let after = &rest[i + 5..];
            let Some(j) = after.find("</Key>") else { break };
            let key = xml_unescape(&after[..j]);
            rest = &after[j + 6..];
            let _ = self.objs.borrow_mut().remove(&key);
            if let Some(f) = self.on_change.borrow().as_ref() {
                f(&key, None);
            }
            out.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(&key)));
        }
        out.push_str("</DeleteResult>");
        let mut r = resp(StatusCode::OK, out);
        let _ = r.headers_mut().insert("content-type", "application/xml".parse().unwrap());
        r
    }
}
