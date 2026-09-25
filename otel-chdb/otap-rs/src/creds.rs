//! Credentials the AWS SDKs resolve and object_store does not, so that the
//! Rust edge takes the same environment as the Go publisher:
//!
//! - **Shared config and credentials files** (`AWS_PROFILE`, or the config's
//!   `profile`; `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`, default
//!   `~/.aws/config` and `~/.aws/credentials`): static keys,
//!   `credential_process`, `role_arn` with `source_profile` (chained, as deep
//!   as the SDKs go) or `credential_source`, and `role_arn` with
//!   `web_identity_token_file`. The profile's `ca_bundle` and `region` (for
//!   STS) are honoured. SSO profiles are refused with a message.
//! - **AssumeRole chaining**: `role_arn` on top of whatever the base
//!   credentials are (config keys, `credential_process`, a profile, or
//!   object_store's own chain: env keys, IRSA, Pod Identity, IMDS). STS
//!   `AssumeRole` is signed here (SigV4, `sign`), and its result cached until
//!   5 minutes before it expires, as the SDKs do.
//!
//! The order is aws-sdk-go-v2's (`resolveCredentialChain`): an explicitly
//! named profile wins; then environment keys; then the environment's web
//! identity; then the default profile if it holds credentials; then
//! container credentials and IMDS (object_store's chain covers those).
//!
//! STS is `https://sts.<region>.amazonaws.com` unless `sts_endpoint` or
//! `AWS_ENDPOINT_URL_STS` says otherwise, and here an `http://` endpoint is
//! allowed when named explicitly (object_store refuses one for IRSA).

use object_store::aws::{AwsCredential, AwsCredentialProvider};
use object_store::{CredentialProvider, StaticCredentialProvider};
use ring::{digest, hmac};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---- shared config / credentials files ----

/// Every profile of both files: name -> key -> value. The credentials file
/// wins over the config file for a key both set (as the SDKs merge them).
pub type Profiles = HashMap<String, HashMap<String, String>>;

fn home() -> Option<String> {
    std::env::var("HOME").ok()
}

/// Parses one INI file. In the config file, sections are `[default]` or
/// `[profile NAME]` (`[sso-session ...]` and `[services ...]` are skipped);
/// in the credentials file, `[NAME]`. Indented lines continue a nested
/// value (`s3 =` blocks) and are ignored.
pub fn parse_ini(text: &str, config_file: bool, into: &mut Profiles) {
    let mut cur: Option<String> = None;
    for line in text.lines() {
        if line.starts_with([' ', '\t']) {
            continue;
        }
        let l = line.trim();
        if l.is_empty() || l.starts_with('#') || l.starts_with(';') {
            continue;
        }
        if let Some(sec) = l.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            let sec = sec.trim();
            cur = if !config_file {
                Some(sec.to_string())
            } else if sec == "default" {
                Some("default".into())
            } else {
                sec.strip_prefix("profile").filter(|r| r.starts_with([' ', '\t'])).map(|r| r.trim().to_string())
            };
            continue;
        }
        let (Some(p), Some((k, v))) = (&cur, l.split_once('=')) else { continue };
        let (k, v) = (k.trim().to_ascii_lowercase(), v.trim().to_string());
        let e = into.entry(p.clone()).or_default();
        if config_file {
            let _ = e.entry(k).or_insert(v);
        } else {
            let _ = e.insert(k, v);
        }
    }
}

/// Both files, from their environment-named or default locations. Missing
/// files are not an error.
pub fn load_profiles() -> Profiles {
    let mut p = Profiles::new();
    let cred = std::env::var("AWS_SHARED_CREDENTIALS_FILE").ok().or_else(|| home().map(|h| format!("{h}/.aws/credentials")));
    let conf = std::env::var("AWS_CONFIG_FILE").ok().or_else(|| home().map(|h| format!("{h}/.aws/config")));
    if let Some(t) = cred.and_then(|f| std::fs::read_to_string(f).ok()) {
        parse_ini(&t, false, &mut p);
    }
    if let Some(t) = conf.and_then(|f| std::fs::read_to_string(f).ok()) {
        parse_ini(&t, true, &mut p);
    }
    p
}

/// Where a profile's credentials come from.
#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    Static { key: String, secret: String, token: Option<String> },
    Process(String),
    /// object_store's own chain (a profile's `credential_source`).
    Environment,
    AssumeRole { role_arn: String, session: String, external_id: Option<String>, base: Box<Source> },
    WebIdentity { role_arn: String, token_file: String, session: String },
}

fn session_name(p: &HashMap<String, String>) -> String {
    p.get("role_session_name").cloned().unwrap_or_else(|| format!("otap-s3pq-{}", std::process::id()))
}

/// Resolves a profile to its credential source, following `source_profile`.
pub fn resolve_profile(profiles: &Profiles, name: &str) -> Result<Source, String> {
    resolve_depth(profiles, name, 0)
}

fn static_of(p: &HashMap<String, String>) -> Option<Source> {
    match (p.get("aws_access_key_id"), p.get("aws_secret_access_key")) {
        (Some(k), Some(s)) => Some(Source::Static {
            key: k.clone(),
            secret: s.clone(),
            token: p.get("aws_session_token").filter(|t| !t.is_empty()).cloned(),
        }),
        _ => None,
    }
}

fn resolve_depth(profiles: &Profiles, name: &str, depth: usize) -> Result<Source, String> {
    if depth > 8 {
        return Err(format!("profile {name}: source_profile chain too deep (a loop?)"));
    }
    let p = profiles.get(name).ok_or_else(|| format!("profile {name} not found in the shared config or credentials file"))?;
    if let Some(role) = p.get("role_arn") {
        if let Some(tf) = p.get("web_identity_token_file") {
            return Ok(Source::WebIdentity { role_arn: role.clone(), token_file: tf.clone(), session: session_name(p) });
        }
        let base = match (p.get("source_profile"), p.get("credential_source")) {
            // A profile that names itself uses its own static keys.
            (Some(sp), _) if sp == name => {
                static_of(p).ok_or_else(|| format!("profile {name}: source_profile is itself but it has no keys"))?
            }
            (Some(sp), _) => resolve_depth(profiles, sp, depth + 1)?,
            (None, Some(_)) => Source::Environment,
            (None, None) => return Err(format!("profile {name}: role_arn needs source_profile or credential_source")),
        };
        return Ok(Source::AssumeRole {
            role_arn: role.clone(),
            session: session_name(p),
            external_id: p.get("external_id").cloned(),
            base: Box::new(base),
        });
    }
    if let Some(cmd) = p.get("credential_process") {
        return Ok(Source::Process(cmd.clone()));
    }
    if let Some(s) = static_of(p) {
        return Ok(s);
    }
    if p.keys().any(|k| k.starts_with("sso_")) {
        return Err(format!("profile {name}: SSO profiles are not supported; use credential_process (aws configure export-credentials)"));
    }
    Err(format!("profile {name} has no credentials"))
}

// ---- SigV4 ----

fn hex(b: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push(H[(x >> 4) as usize] as char);
        s.push(H[(x & 15) as usize] as char);
    }
    s
}

pub fn sha256_hex(b: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, b).as_ref())
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), msg).as_ref().to_vec()
}

/// `YYYYMMDDTHHMMSSZ` for a time.
pub fn amz_date(t: SystemTime) -> String {
    let s = t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let (days, rem) = (s.div_euclid(86400), s.rem_euclid(86400));
    // civil from days (Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + (m <= 2) as i64;
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", rem / 3600, rem / 60 % 60, rem % 60)
}

/// Signs a request with SigV4: `headers` must hold every header to sign
/// (lowercase names; `host` and `x-amz-date` included); returns the
/// `Authorization` value. `query` is the canonical query string (sorted,
/// encoded), `payload_hash` the hex SHA-256 of the body (or
/// `UNSIGNED-PAYLOAD`).
#[allow(clippy::too_many_arguments)]
pub fn sign(
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
    cred: &AwsCredential,
    region: &str,
    service: &str,
) -> String {
    let mut h: Vec<(String, String)> = headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string())).collect();
    h.sort();
    let date = h.iter().find(|(k, _)| k == "x-amz-date").map(|(_, v)| v.clone()).unwrap_or_default();
    let canon_headers: String = h.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed: String = h.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let creq = format!("{method}\n{path}\n{query}\n{canon_headers}\n{signed}\n{payload_hash}");
    let scope = format!("{}/{region}/{service}/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{date}\n{scope}\n{}", sha256_hex(creq.as_bytes()));
    let k = hmac(format!("AWS4{}", cred.secret_key).as_bytes(), date[..8].as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, service.as_bytes());
    let k = hmac(&k, b"aws4_request");
    let sig = hex(&hmac(&k, sts.as_bytes()));
    format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={sig}", cred.key_id)
}

// ---- STS ----

/// An STS endpoint and the HTTP client to reach it (with the store's CA
/// bundle and proxy settings).
#[derive(Clone, Debug)]
pub struct Sts {
    pub client: reqwest::Client,
    pub endpoint: String,
    pub region: String,
}

impl Sts {
    /// `explicit` (config `sts_endpoint`), else `AWS_ENDPOINT_URL_STS`, else
    /// the regional endpoint.
    pub fn new(client: reqwest::Client, region: &str, explicit: Option<&str>) -> Self {
        let endpoint = explicit
            .map(str::to_string)
            .or_else(|| std::env::var("AWS_ENDPOINT_URL_STS").ok())
            .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"));
        Self { client, endpoint: endpoint.trim_end_matches('/').to_string(), region: region.to_string() }
    }

    async fn call(&self, form: &[(&str, &str)], signer: Option<&AwsCredential>) -> Result<(AwsCredential, Option<SystemTime>), String> {
        let body: String = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(form).finish();
        let u = url::Url::parse(&self.endpoint).map_err(|e| format!("sts endpoint {}: {e}", self.endpoint))?;
        let host = match u.port() {
            Some(p) => format!("{}:{p}", u.host_str().unwrap_or_default()),
            None => u.host_str().unwrap_or_default().to_string(),
        };
        let ct = "application/x-www-form-urlencoded; charset=utf-8";
        let mut req = self.client.post(u.as_str()).header("content-type", ct);
        if let Some(c) = signer {
            let date = amz_date(SystemTime::now());
            let mut h = vec![("content-type".to_string(), ct.to_string()), ("host".into(), host), ("x-amz-date".into(), date.clone())];
            if let Some(t) = &c.token {
                h.push(("x-amz-security-token".into(), t.clone()));
                req = req.header("x-amz-security-token", t);
            }
            let path = if u.path().is_empty() { "/" } else { u.path() };
            let auth = sign("POST", path, "", &h, &sha256_hex(body.as_bytes()), c, &self.region, "sts");
            req = req.header("x-amz-date", date).header("authorization", auth);
        }
        let r = req.body(body).send().await.map_err(|e| format!("sts {}: {}", self.endpoint, crate::store::chain(&e)))?;
        let status = r.status();
        let text = r.text().await.map_err(|e| format!("sts body: {e}"))?;
        if !status.is_success() {
            return Err(format!("sts {}: {status}: {}", form[0].1, text.chars().take(300).collect::<String>()));
        }
        let tag = |t: &str| {
            let (o, c) = (format!("<{t}>"), format!("</{t}>"));
            let s = text.find(&o)? + o.len();
            let e = text[s..].find(&c)? + s;
            Some(text[s..e].to_string())
        };
        let cred = AwsCredential {
            key_id: tag("AccessKeyId").ok_or("sts: no AccessKeyId in the answer")?,
            secret_key: tag("SecretAccessKey").ok_or("sts: no SecretAccessKey in the answer")?,
            token: tag("SessionToken").filter(|t| !t.is_empty()),
        };
        Ok((cred, tag("Expiration").and_then(|e| crate::store::parse_rfc3339(&e))))
    }
}

type Cache = tokio::sync::Mutex<Option<(Arc<AwsCredential>, Option<SystemTime>)>>;

async fn cached<F, Fut>(cache: &Cache, fetch: F) -> object_store::Result<Arc<AwsCredential>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(AwsCredential, Option<SystemTime>), String>>,
{
    let mut c = cache.lock().await;
    if let Some((cred, exp)) = c.as_ref() {
        if exp.is_none_or(|e| e.duration_since(SystemTime::now()).unwrap_or_default() > Duration::from_secs(300)) {
            return Ok(cred.clone());
        }
    }
    let (cred, exp) = fetch().await.map_err(|m| object_store::Error::Generic { store: "S3", source: m.into() })?;
    let cred = Arc::new(cred);
    *c = Some((cred.clone(), exp));
    Ok(cred)
}

/// `sts:AssumeRole` on top of base credentials (role chaining).
#[derive(Debug)]
pub struct AssumeRole {
    pub sts: Sts,
    pub role_arn: String,
    pub session: String,
    pub external_id: Option<String>,
    pub base: AwsCredentialProvider,
    cache: Cache,
}

impl AssumeRole {
    pub fn new(sts: Sts, role_arn: String, session: String, external_id: Option<String>, base: AwsCredentialProvider) -> Self {
        Self { sts, role_arn, session, external_id, base, cache: Default::default() }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for AssumeRole {
    type Credential = AwsCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        cached(&self.cache, || async {
            let base = self.base.get_credential().await.map_err(|e| format!("base credentials for AssumeRole: {e}"))?;
            let mut form = vec![
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", self.role_arn.as_str()),
                ("RoleSessionName", self.session.as_str()),
                ("DurationSeconds", "3600"),
            ];
            if let Some(x) = &self.external_id {
                form.push(("ExternalId", x.as_str()));
            }
            self.sts.call(&form, Some(&base)).await
        })
        .await
    }
}

/// `sts:AssumeRoleWithWebIdentity` from a profile's `web_identity_token_file`.
#[derive(Debug)]
pub struct WebIdentity {
    pub sts: Sts,
    pub role_arn: String,
    pub token_file: String,
    pub session: String,
    cache: Cache,
}

#[async_trait::async_trait]
impl CredentialProvider for WebIdentity {
    type Credential = AwsCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        cached(&self.cache, || async {
            let token = std::fs::read_to_string(&self.token_file).map_err(|e| format!("{}: {e}", self.token_file))?;
            let form = [
                ("Action", "AssumeRoleWithWebIdentity"),
                ("Version", "2011-06-15"),
                ("RoleArn", self.role_arn.as_str()),
                ("RoleSessionName", self.session.as_str()),
                ("WebIdentityToken", token.trim()),
            ];
            self.sts.call(&form, None).await
        })
        .await
    }
}

/// A provider for a resolved source. `env` supplies object_store's own chain
/// when a profile says `credential_source`.
pub fn provider(src: &Source, sts: &Sts, env: &dyn Fn() -> Result<AwsCredentialProvider, String>) -> Result<AwsCredentialProvider, String> {
    Ok(match src {
        Source::Static { key, secret, token } => Arc::new(StaticCredentialProvider::new(AwsCredential {
            key_id: key.clone(),
            secret_key: secret.clone(),
            token: token.clone(),
        })),
        Source::Process(cmd) => Arc::new(crate::store::ProcessCredentials::new(cmd.clone())),
        Source::Environment => env()?,
        Source::AssumeRole { role_arn, session, external_id, base } => Arc::new(AssumeRole::new(
            sts.clone(),
            role_arn.clone(),
            session.clone(),
            external_id.clone(),
            provider(base, sts, env)?,
        )),
        Source::WebIdentity { role_arn, token_file, session } => Arc::new(WebIdentity {
            sts: sts.clone(),
            role_arn: role_arn.clone(),
            token_file: token_file.clone(),
            session: session.clone(),
            cache: Default::default(),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profiles() -> Profiles {
        let mut p = Profiles::new();
        parse_ini(
            "[default]\naws_access_key_id = AK\naws_secret_access_key = SK\n\n[edge]\naws_access_key_id=E\naws_secret_access_key=ES\naws_session_token=T\n",
            false,
            &mut p,
        );
        parse_ini(
            "[default]\nregion = eu-west-1\n[profile chained]\nrole_arn = arn:aws:iam::1:role/a\nsource_profile = edge\nexternal_id = x\n\
             [profile twice]\nrole_arn = arn:aws:iam::1:role/b\nsource_profile = chained\nrole_session_name = s2\n\
             [profile proc]\ncredential_process = helper --x\n  nested = ignored\n[profile env]\nrole_arn = r\ncredential_source = Ec2InstanceMetadata\n\
             [profile wi]\nrole_arn = r\nweb_identity_token_file = /t\n[profile sso]\nsso_start_url = u\n[sso-session x]\nsso_region = r\n\
             [profile self]\nrole_arn = r\nsource_profile = self\naws_access_key_id = A\naws_secret_access_key = B\n",
            true,
            &mut p,
        );
        p
    }

    #[test]
    fn profiles_resolve_like_the_sdks() {
        let p = profiles();
        assert_eq!(p["default"]["region"], "eu-west-1");
        assert_eq!(resolve_profile(&p, "default").unwrap(), Source::Static { key: "AK".into(), secret: "SK".into(), token: None });
        let Source::AssumeRole { role_arn, external_id, base, .. } = resolve_profile(&p, "chained").unwrap() else { panic!() };
        assert_eq!((role_arn.as_str(), external_id.as_deref()), ("arn:aws:iam::1:role/a", Some("x")));
        assert_eq!(*base, Source::Static { key: "E".into(), secret: "ES".into(), token: Some("T".into()) });
        let Source::AssumeRole { session, base, .. } = resolve_profile(&p, "twice").unwrap() else { panic!() };
        assert_eq!(session, "s2");
        assert!(matches!(*base, Source::AssumeRole { .. }), "two hops");
        assert_eq!(resolve_profile(&p, "proc").unwrap(), Source::Process("helper --x".into()));
        let Source::AssumeRole { base, .. } = resolve_profile(&p, "env").unwrap() else { panic!() };
        assert_eq!(*base, Source::Environment);
        assert!(matches!(resolve_profile(&p, "wi").unwrap(), Source::WebIdentity { .. }));
        assert!(resolve_profile(&p, "sso").unwrap_err().contains("SSO"));
        assert!(resolve_profile(&p, "nope").is_err());
        let Source::AssumeRole { base, .. } = resolve_profile(&p, "self").unwrap() else { panic!() };
        assert_eq!(*base, Source::Static { key: "A".into(), secret: "B".into(), token: None });
    }

    /// The worked example of the AWS SigV4 documentation (IAM ListUsers,
    /// 2015-08-30), whose signature is published.
    #[test]
    fn sigv4_documented_example() {
        let c = AwsCredential { key_id: "AKIDEXAMPLE".into(), secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(), token: None };
        let h = vec![
            ("Content-Type".to_string(), "application/x-www-form-urlencoded; charset=utf-8".to_string()),
            ("Host".into(), "iam.amazonaws.com".into()),
            ("X-Amz-Date".into(), "20150830T123600Z".into()),
        ];
        let a = sign("GET", "/", "Action=ListUsers&Version=2010-05-08", &h, &sha256_hex(b""), &c, "us-east-1", "iam");
        assert_eq!(
            a,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    #[test]
    fn amz_date_formats() {
        assert_eq!(amz_date(UNIX_EPOCH + Duration::from_secs(1440938160)), "20150830T123600Z");
        assert_eq!(amz_date(UNIX_EPOCH), "19700101T000000Z");
    }
}
