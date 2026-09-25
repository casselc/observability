//! The S3 side: an async slot store over `object_store` (AWS S3 and
//! S3-compatible stores), its configuration and credentials, and an
//! in-memory store with conditional-put semantics and fault injection for
//! tests.
//!
//! Credentials, in order:
//! 1. `access_key_id` / `secret_access_key` (+ `session_token`) in config:
//!    static keys (Nutanix Objects, SeaweedFS, MinIO).
//! 2. `credential_process` in config: runs the command and reads its JSON,
//!    as the AWS SDKs do; cached until 5 minutes before `Expiration`. This
//!    is IAM Roles Anywhere's `aws_signing_helper credential-process`.
//!    object_store has no process provider of its own.
//! 3. otherwise object_store's own chain from the environment:
//!    `AWS_ACCESS_KEY_ID`..., web identity (`AWS_ROLE_ARN` +
//!    `AWS_WEB_IDENTITY_TOKEN_FILE`: EKS IRSA; `AWS_ENDPOINT_URL_STS`
//!    honoured), container credentials (`AWS_CONTAINER_CREDENTIALS_FULL_URI`
//!    + `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`: EKS Pod Identity), then
//!    IMDSv2 at `AWS_METADATA_ENDPOINT`. The SDKs' name for that endpoint,
//!    `AWS_EC2_METADATA_SERVICE_ENDPOINT`, is mapped onto it here, so
//!    `aws_signing_helper serve` (Roles Anywhere as a local IMDS) works with
//!    the same environment as for the Go publisher and ClickHouse.
//!
//!
//! Before 3, the SDKs' shared files and role chaining (`creds.rs`): a named
//! profile (`profile`, or `AWS_PROFILE`) wins over the environment, as in
//! aws-sdk-go-v2; the `default` profile comes after environment keys and
//! web identity. `role_arn` (config) assumes a role on top of whichever base
//! credentials were resolved (STS `AssumeRole`, signed here).
//!
//! A private CA: `ca_bundle` (PEM, one or more certificates), else
//! `AWS_CA_BUNDLE`, else the profile's `ca_bundle`, is added to the roots;
//! `SSL_CERT_FILE` / `SSL_CERT_DIR` are honoured by the native-roots loader
//! as well. Proxies: `proxy_url` / `AWS_PROXY_URL` with `proxy_excludes`;
//! otherwise reqwest's system proxy applies, which is the SDKs' contract:
//! `HTTPS_PROXY` for https, `HTTP_PROXY` for http, `NO_PROXY` (both object
//! store and STS requests; checked in tests/creds.rs).

use crate::proto::PutOutcome;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::{AmazonS3Builder, AwsCredential, AwsCredentialProvider};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, ClientOptions, CredentialProvider, GetOptions,
    ObjectStore, PutMode, PutOptions, PutPayload, RetryConfig,
};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

pub type Meta = HashMap<String, String>;

/// An error with its sources, one line.
pub fn chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        s.push_str(": ");
        s.push_str(&c.to_string());
        cur = c.source();
    }
    s
}

#[derive(Debug)]
pub struct StoreError(pub String);
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for StoreError {}

/// What the protocol needs from a bucket.
#[async_trait(?Send)]
pub trait SlotStore {
    /// `PUT If-None-Match: *` with user metadata.
    async fn put_create(&self, key: &str, body: Bytes, content_type: &str, meta: &BTreeMap<String, String>) -> PutOutcome;
    /// HEAD: `Ok(None)` for 404.
    async fn head(&self, key: &str) -> Result<Option<Meta>, StoreError>;
    /// The immediate "directories" under `prefix/`.
    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
    /// Keys under `prefix/` after `start_after`, in order.
    async fn list_after(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<String>, StoreError>;
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Config {
    /// `s3://bucket/prefix` (AWS, virtual-hosted) or `http(s)://host[:port]/bucket/prefix`
    /// (custom endpoint, path-style).
    pub url: String,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// e.g. `aws_signing_helper credential-process --certificate ... --role-arn ...`
    pub credential_process: Option<String>,
    /// PEM file with extra root certificates (a private CA).
    pub ca_bundle: Option<String>,
    /// Force path-style (default: true for a custom endpoint, false for s3://).
    pub path_style: Option<bool>,
    /// An HTTP(S) proxy for S3 and STS requests (default: $AWS_PROXY_URL).
    /// object_store does not read HTTPS_PROXY.
    pub proxy_url: Option<String>,
    /// Hosts that bypass the proxy (default: $AWS_PROXY_EXCLUDES).
    pub proxy_excludes: Option<String>,
    /// A shared config / credentials profile (default: $AWS_PROFILE).
    pub profile: Option<String>,
    /// Assume this role on top of the base credentials (role chaining).
    pub role_arn: Option<String>,
    pub role_session_name: Option<String>,
    pub external_id: Option<String>,
    /// STS endpoint (default: $AWS_ENDPOINT_URL_STS, else the regional one).
    pub sts_endpoint: Option<String>,
    #[serde(with = "humantime_serde")]
    pub put_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub head_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            url: String::new(),
            region: "us-east-1".into(),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            credential_process: None,
            ca_bundle: None,
            path_style: None,
            proxy_url: None,
            proxy_excludes: None,
            profile: None,
            role_arn: None,
            role_session_name: None,
            external_id: None,
            sts_endpoint: None,
            put_timeout: Duration::from_secs(10),
            head_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// An object_store-backed bucket plus the key prefix inside it.
pub struct S3Store {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: String,
    /// "bucket/prefix" for building s3() URLs.
    pub endpoint: String,
    pub bucket: String,
}

impl S3Config {
    pub fn build(&self) -> Result<S3Store, StoreError> {
        let err = |e: &dyn std::fmt::Display| StoreError(format!("s3 config: {e}"));
        let (endpoint, bucket, prefix, custom) = if let Some(rest) = self.url.strip_prefix("s3://") {
            let (b, p) = rest.split_once('/').unwrap_or((rest, ""));
            (format!("https://{b}.s3.{}.amazonaws.com", self.region), b.to_string(), p.to_string(), false)
        } else {
            let u = url::Url::parse(&self.url).map_err(|e| err(&e))?;
            let mut segs = u.path().trim_start_matches('/').splitn(2, '/');
            let b = segs.next().unwrap_or("").to_string();
            let p = segs.next().unwrap_or("").to_string();
            let host = u.host_str().ok_or_else(|| err(&"url has no host"))?;
            let ep = match u.port() {
                Some(port) => format!("{}://{host}:{port}", u.scheme()),
                None => format!("{}://{host}", u.scheme()),
            };
            (ep, b, p, true)
        };
        if bucket.is_empty() {
            return Err(err(&"url has no bucket"));
        }
        let profiles = crate::creds::load_profiles();
        let named = self.profile.clone().or_else(|| std::env::var("AWS_PROFILE").ok()).filter(|p| !p.is_empty());
        let prof = profiles.get(named.as_deref().unwrap_or("default"));
        let ca_bundle = self
            .ca_bundle
            .clone()
            .or_else(|| std::env::var("AWS_CA_BUNDLE").ok().filter(|v| !v.is_empty()))
            .or_else(|| prof.and_then(|p| p.get("ca_bundle").cloned()));
        let mut roots = Vec::new();
        if let Some(path) = &ca_bundle {
            let pem = std::fs::read(path).map_err(|e| err(&format!("{path}: {e}")))?;
            roots = object_store::Certificate::from_pem_bundle(&pem).map_err(|e| err(&e))?;
        }
        // (allow_http lives in ClientOptions: with_client_options below
        // replaces anything set through the builder's own with_allow_http.)
        let mut co = ClientOptions::new()
            .with_allow_http(custom && endpoint.starts_with("http://"))
            .with_timeout(self.put_timeout)
            .with_connect_timeout(self.connect_timeout);
        // The builder's client options (from AWS_* env) are replaced below,
        // so carry the proxy settings over explicitly.
        let proxy = self.proxy_url.clone().or_else(|| std::env::var("AWS_PROXY_URL").ok());
        let excludes = self.proxy_excludes.clone().or_else(|| std::env::var("AWS_PROXY_EXCLUDES").ok());
        if let Some(p) = &proxy {
            co = co.with_proxy_url(p);
        }
        if let Some(e) = &excludes {
            co = co.with_proxy_excludes(e);
        }
        for c in &roots {
            co = co.with_root_certificate(c.clone());
        }
        let base = || {
            let mut b = AmazonS3Builder::from_env();
            // The SDKs' IMDS endpoint variable (aws_signing_helper serve, IMDS stand-ins).
            if std::env::var_os("AWS_METADATA_ENDPOINT").is_none() {
                if let Ok(v) = std::env::var("AWS_EC2_METADATA_SERVICE_ENDPOINT") {
                    b = b.with_metadata_endpoint(v.trim_end_matches('/'));
                }
            }
            b = b.with_bucket_name(&bucket).with_region(&self.region);
            if custom {
                b = b.with_endpoint(&endpoint).with_virtual_hosted_style_request(!self.path_style.unwrap_or(true));
            } else if self.path_style == Some(true) {
                b = b.with_virtual_hosted_style_request(false);
            }
            b.with_client_options(co.clone()).with_retry(RetryConfig {
                // The protocol resolves ambiguity itself (HEAD the slot); keep
                // the client's own retries short so a PUT's outcome is decided
                // within put_timeout.
                max_retries: 2,
                retry_timeout: self.put_timeout,
                ..Default::default()
            })
        };
        // Where the credentials come from, in aws-sdk-go-v2's order.
        use crate::creds::Source;
        let env_has = |v: &str| std::env::var_os(v).is_some_and(|x| !x.is_empty());
        let source: Option<Source> = if let (Some(k), Some(s)) = (&self.access_key_id, &self.secret_access_key) {
            Some(Source::Static { key: k.clone(), secret: s.clone(), token: self.session_token.clone() })
        } else if let Some(cmd) = &self.credential_process {
            Some(Source::Process(cmd.clone()))
        } else if let Some(n) = &named {
            Some(crate::creds::resolve_profile(&profiles, n).map_err(|e| err(&e))?)
        } else if env_has("AWS_ACCESS_KEY_ID") || env_has("AWS_WEB_IDENTITY_TOKEN_FILE") {
            None // object_store's chain: environment keys, then IRSA
        } else {
            crate::creds::resolve_profile(&profiles, "default").ok()
        };
        let mut b = base();
        if source.is_some() || self.role_arn.is_some() {
            let sts_region = prof.and_then(|p| p.get("region").cloned()).unwrap_or_else(|| self.region.clone());
            let mut hc = reqwest::Client::builder().connect_timeout(self.connect_timeout).timeout(self.put_timeout);
            if let Some(p) = &proxy {
                let mut px = reqwest::Proxy::all(p).map_err(|e| err(&e))?;
                if let Some(e) = &excludes {
                    px = px.no_proxy(reqwest::NoProxy::from_string(e));
                }
                hc = hc.proxy(px);
            }
            if let Some(path) = &ca_bundle {
                let pem = std::fs::read(path).map_err(|e| err(&format!("{path}: {e}")))?;
                for c in reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| err(&e))? {
                    hc = hc.add_root_certificate(c);
                }
            }
            let sts = crate::creds::Sts::new(hc.build().map_err(|e| err(&e))?, &sts_region, self.sts_endpoint.as_deref());
            let env_chain = || -> Result<AwsCredentialProvider, String> {
                base().build().map(|s| s.credentials().clone()).map_err(|e| e.to_string())
            };
            let mut p = match &source {
                Some(src) => crate::creds::provider(src, &sts, &env_chain).map_err(|e| err(&e))?,
                None => env_chain().map_err(|e| err(&e))?,
            };
            if let Some(role) = &self.role_arn {
                p = Arc::new(crate::creds::AssumeRole::new(
                    sts,
                    role.clone(),
                    self.role_session_name.clone().unwrap_or_else(|| format!("otap-s3pq-{}", std::process::id())),
                    self.external_id.clone(),
                    p,
                ));
            }
            b = b.with_credentials(p);
        }
        let store = b.build().map_err(|e| err(&e))?;
        Ok(S3Store {
            store: Arc::new(store),
            prefix: prefix.trim_matches('/').to_string(),
            endpoint: if custom { format!("{endpoint}/{bucket}") } else { endpoint },
            bucket,
        })
    }
}

impl S3Store {
    fn path(&self, key: &str) -> Path {
        Path::from(key)
    }

    /// The URL ClickHouse's s3() reads a key at.
    pub fn object_url(&self, key: &str) -> String {
        format!("{}/{}", self.endpoint, key)
    }
}

#[async_trait(?Send)]
impl SlotStore for S3Store {
    async fn put_create(&self, key: &str, body: Bytes, content_type: &str, meta: &BTreeMap<String, String>) -> PutOutcome {
        let mut attrs = Attributes::new();
        let _ = attrs.insert(Attribute::ContentType, AttributeValue::from(content_type.to_string()));
        for (k, v) in meta {
            let _ = attrs.insert(Attribute::Metadata(k.clone().into()), AttributeValue::from(v.clone()));
        }
        let opts = PutOptions { mode: PutMode::Create, attributes: attrs, ..Default::default() };
        match self.store.put_opts(&self.path(key), PutPayload::from_bytes(body), opts).await {
            Ok(_) => PutOutcome::Ok,
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                PutOutcome::Exists
            }
            Err(e) => {
                crate::log(&format!("put {key}: {}", chain(&e)));
                PutOutcome::Unknown
            }
        }
    }

    async fn head(&self, key: &str) -> Result<Option<Meta>, StoreError> {
        let opts = GetOptions { head: true, ..Default::default() };
        match self.store.get_opts(&self.path(key), opts).await {
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
            Err(e) => Err(StoreError(format!("head {key}: {}", chain(&e)))),
        }
    }

    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let p = Path::from(prefix.trim_end_matches('/'));
        let r = self
            .store
            .list_with_delimiter(Some(&p))
            .await
            .map_err(|e| StoreError(format!("list {prefix}: {e}")))?;
        let mut out: Vec<String> = r
            .common_prefixes
            .iter()
            .filter_map(|c| c.filename().map(str::to_string))
            .collect();
        out.sort();
        Ok(out)
    }

    async fn list_after(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<String>, StoreError> {
        use futures::TryStreamExt;
        let p = Path::from(prefix.trim_end_matches('/'));
        let stream = match start_after {
            Some(s) => self.store.list_with_offset(Some(&p), &Path::from(s)),
            None => self.store.list(Some(&p)),
        };
        let mut out: Vec<String> = stream
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .map_err(|e| StoreError(format!("list {prefix}: {e}")))?;
        out.sort();
        Ok(out)
    }
}

// ---- credential_process ---------------------------------------------------------

/// Runs a `credential_process` command (the AWS SDKs' contract: JSON with
/// Version 1, AccessKeyId, SecretAccessKey, SessionToken, Expiration) and
/// caches the result until 5 minutes before it expires.
#[derive(Debug)]
pub struct ProcessCredentials {
    cmd: String,
    cache: tokio::sync::Mutex<Option<(Arc<AwsCredential>, Option<std::time::SystemTime>)>>,
}

impl ProcessCredentials {
    pub fn new(cmd: String) -> Self {
        Self { cmd, cache: tokio::sync::Mutex::new(None) }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ProcessOutput {
    version: u32,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    expiration: Option<String>,
}

pub(crate) fn parse_rfc3339(s: &str) -> Option<std::time::SystemTime> {
    // YYYY-MM-DDTHH:MM:SS[.frac](Z|+HH:MM)
    let b = s.as_bytes();
    if b.len() < 20 {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d, h, mi, se) = (n(0..4)?, n(5..7)?, n(8..10)?, n(11..13)?, n(14..16)?, n(17..19)?);
    let mut i = 19;
    if b.get(i) == Some(&b'.') {
        i += 1;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
    }
    let off = match b.get(i) {
        Some(b'Z') | Some(b'z') => 0,
        Some(&c @ (b'+' | b'-')) => {
            let o = n(i + 1..i + 3)? * 3600 + n(i + 4..i + 6)? * 60;
            if c == b'+' { o } else { -o }
        }
        _ => return None,
    };
    // days from civil
    let (yy, mm) = if mo <= 2 { (y - 1, mo + 9) } else { (y, mo - 3) };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let doy = (153 * mm + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let secs = days * 86400 + h * 3600 + mi * 60 + se - off;
    Some(std::time::UNIX_EPOCH + Duration::from_secs(u64::try_from(secs).ok()?))
}

#[async_trait]
impl CredentialProvider for ProcessCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let mut c = self.cache.lock().await;
        if let Some((cred, exp)) = c.as_ref() {
            let fresh = exp.is_none_or(|e| {
                e.duration_since(std::time::SystemTime::now()).unwrap_or_default() > Duration::from_secs(300)
            });
            if fresh {
                return Ok(cred.clone());
            }
        }
        let gen_err = |m: String| object_store::Error::Generic { store: "credential_process", source: m.into() };
        let out = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&self.cmd)
            .output()
            .await
            .map_err(|e| gen_err(format!("run: {e}")))?;
        if !out.status.success() {
            return Err(gen_err(format!(
                "exit {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let p: ProcessOutput = serde_json::from_slice(&out.stdout).map_err(|e| gen_err(format!("output: {e}")))?;
        if p.version != 1 {
            return Err(gen_err(format!("unsupported Version {}", p.version)));
        }
        let exp = match &p.expiration {
            Some(s) => Some(parse_rfc3339(s).ok_or_else(|| gen_err(format!("bad Expiration {s}")))?),
            None => None,
        };
        let cred = Arc::new(AwsCredential {
            key_id: p.access_key_id,
            secret_key: p.secret_access_key,
            token: p.session_token.filter(|t| !t.is_empty()),
        });
        *c = Some((cred.clone(), exp));
        Ok(cred)
    }
}

// ---- in-memory store -----------------------------------------------------------------

/// What the next PUT does, beyond the conditional create.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Applied, and the answer is lost: the client sees Unknown.
    ApplyLoseAnswer,
    /// Never applied; the client sees Unknown.
    Drop,
    /// Held: applied only when `release_held` is called (a late request);
    /// the client sees Unknown.
    Hold,
}

#[derive(Default)]
pub struct MemInner {
    pub objects: BTreeMap<String, (Bytes, Meta)>,
    pub faults: std::collections::VecDeque<Fault>,
    pub held: Vec<(String, Bytes, Meta)>,
    pub puts: usize,
    pub heads: usize,
    pub lists: usize,
}

/// An in-memory bucket: atomic conditional creates, strong consistency.
#[derive(Clone, Default)]
pub struct MemStore(pub Rc<RefCell<MemInner>>);

impl MemStore {
    fn create(&self, key: &str, body: Bytes, meta: Meta) -> bool {
        let mut s = self.0.borrow_mut();
        if s.objects.contains_key(key) {
            return false;
        }
        let _ = s.objects.insert(key.to_string(), (body, meta));
        true
    }
    /// Applies every held PUT, create-only; returns how many won.
    pub fn release_held(&self) -> usize {
        let held = std::mem::take(&mut self.0.borrow_mut().held);
        held.into_iter().filter(|(k, b, m)| self.create(k, b.clone(), m.clone())).count()
    }
    pub fn inject(&self, f: Fault) {
        self.0.borrow_mut().faults.push_back(f);
    }
}

#[async_trait(?Send)]
impl SlotStore for MemStore {
    async fn put_create(&self, key: &str, body: Bytes, _ct: &str, meta: &BTreeMap<String, String>) -> PutOutcome {
        let meta: Meta = meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let fault = {
            let mut s = self.0.borrow_mut();
            s.puts += 1;
            s.faults.pop_front()
        };
        match fault {
            None => {
                if self.create(key, body, meta) { PutOutcome::Ok } else { PutOutcome::Exists }
            }
            Some(Fault::ApplyLoseAnswer) => {
                let _ = self.create(key, body, meta);
                PutOutcome::Unknown
            }
            Some(Fault::Drop) => PutOutcome::Unknown,
            Some(Fault::Hold) => {
                self.0.borrow_mut().held.push((key.to_string(), body, meta));
                PutOutcome::Unknown
            }
        }
    }

    async fn head(&self, key: &str) -> Result<Option<Meta>, StoreError> {
        let mut s = self.0.borrow_mut();
        s.heads += 1;
        Ok(s.objects.get(key).map(|(_, m)| m.clone()))
    }

    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut s = self.0.borrow_mut();
        s.lists += 1;
        let p = format!("{}/", prefix.trim_end_matches('/'));
        let mut out: Vec<String> = s
            .objects
            .keys()
            .filter_map(|k| k.strip_prefix(&p)?.split_once('/').map(|(d, _)| d.to_string()))
            .collect();
        out.dedup();
        Ok(out)
    }

    async fn list_after(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<String>, StoreError> {
        let mut s = self.0.borrow_mut();
        s.lists += 1;
        let p = format!("{}/", prefix.trim_end_matches('/'));
        Ok(s
            .objects
            .keys()
            .filter(|k| k.starts_with(&p) && start_after.is_none_or(|a| k.as_str() > a))
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339() {
        let t = parse_rfc3339("2026-09-25T03:00:00Z").unwrap();
        assert_eq!(t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(), 1790305200);
        let t2 = parse_rfc3339("2026-09-25T05:00:00.123+02:00").unwrap();
        assert_eq!(t, t2);
    }
}
