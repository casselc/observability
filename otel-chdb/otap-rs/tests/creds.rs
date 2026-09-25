//! Credential modes of the S3 client, against local stand-ins for the AWS
//! side (../parquetgo/compare/cmd/credstubs: Pod Identity agent and STS on
//! :18901, IMDSv2 as `aws_signing_helper serve` provides it on :18904, a TLS
//! proxy to SeaweedFS with a private CA on :18903). Each mode builds the
//! store with no keys in config, then does the protocol's own requests (a
//! create-only PUT with metadata, and a HEAD), and checks the stand-in saw
//! the credential flow.
//!
//! Then the modes `creds.rs` adds, which object_store alone doesn't have:
//! `AWS_CA_BUNDLE`, `HTTPS_PROXY` / `NO_PROXY` (through a CONNECT proxy
//! started here that logs what it tunnels), `AWS_PROFILE` and the default
//! profile of the shared config / credentials files (static keys,
//! `credential_process`, `role_arn` + `source_profile`), and AssumeRole
//! chaining (`role_arn` on top of `credential_process`, IMDS, a profile, and
//! via the https STS behind `HTTPS_PROXY`). The stand-in STS answers any
//! action and doesn't check signatures, so the SigV4 signer is checked
//! against SeaweedFS, which does (and against AWS's documented example in
//! `creds::tests`).
//!
//!   (cd DIR && credstubs) &   # writes DIR/ca.pem and DIR/stub.log
//!   OTAPRS_CREDSTUBS=DIR cargo test --release --test creds -- --nocapture --test-threads 1

use otap_s3pq::proto::PutOutcome;
use otap_s3pq::store::{S3Config, SlotStore};
use std::collections::BTreeMap;

const VARS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ENDPOINT_URL_STS",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_METADATA_ENDPOINT",
    "AWS_EC2_METADATA_SERVICE_ENDPOINT",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "SSL_CERT_FILE",
    "AWS_CA_BUNDLE",
    "AWS_PROFILE",
    "AWS_PROXY_URL",
    "AWS_PROXY_EXCLUDES",
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

/// No credentials, proxies, CA or profiles from the environment; the shared
/// files point at nothing (so a real ~/.aws can't leak in).
fn clear_env() {
    for v in VARS {
        unsafe { std::env::remove_var(v) };
    }
    set("AWS_CONFIG_FILE", "/nonexistent/aws-config");
    set("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials");
}

/// A CONNECT proxy on an ephemeral port that tunnels to whatever the client
/// asks for and logs each CONNECT target.
fn tunnel_proxy() -> (u16, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use std::io::{Read, Write};
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let lg = log.clone();
    std::thread::spawn(move || {
        for c in l.incoming() {
            let Ok(mut c) = c else { continue };
            let lg = lg.clone();
            std::thread::spawn(move || {
                let mut head = Vec::new();
                let mut b = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if c.read(&mut b).unwrap_or(0) == 0 {
                        return;
                    }
                    head.push(b[0]);
                }
                let line = String::from_utf8_lossy(&head).lines().next().unwrap_or_default().to_string();
                let target = line.split_whitespace().nth(1).unwrap_or_default().to_string();
                lg.lock().unwrap().push(line);
                let Ok(u) = std::net::TcpStream::connect(&target) else { return };
                let _ = c.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n");
                let (mut c2, mut u2) = (c.try_clone().unwrap(), u.try_clone().unwrap());
                let t = std::thread::spawn(move || {
                    let _ = std::io::copy(&mut c2, &mut u2);
                    let _ = u2.shutdown(std::net::Shutdown::Write);
                });
                let (mut u3, mut c3) = (u, c);
                let _ = std::io::copy(&mut u3, &mut c3);
                let _ = c3.shutdown(std::net::Shutdown::Write);
                let _ = t.join();
            });
        }
    });
    (port, log)
}

fn set(k: &str, v: &str) {
    unsafe { std::env::set_var(k, v) };
}

fn stub_lines(dir: &str) -> usize {
    std::fs::read_to_string(format!("{dir}/stub.log")).map(|s| s.lines().count()).unwrap_or(0)
}

fn stub_tail(dir: &str, from: usize) -> Vec<String> {
    std::fs::read_to_string(format!("{dir}/stub.log"))
        .unwrap_or_default()
        .lines()
        .skip(from)
        .map(str::to_string)
        .collect()
}

async fn roundtrip(name: &str, cfg: S3Config) -> Result<(), String> {
    let store = cfg.build().map_err(|e| e.to_string())?;
    let key = format!("{}/creds/{name}-{}.parquet", store.prefix, std::process::id());
    let mut meta = BTreeMap::new();
    let _ = meta.insert("oscope-kind".to_string(), "data".to_string());
    match store.put_create(&key, bytes::Bytes::from_static(b"x"), "application/octet-stream", &meta).await {
        PutOutcome::Ok => {}
        o => return Err(format!("put: {o:?}")),
    }
    match store.put_create(&key, bytes::Bytes::from_static(b"y"), "application/octet-stream", &meta).await {
        PutOutcome::Exists => {}
        o => return Err(format!("second create-only put: {o:?}, want Exists")),
    }
    let h = store.head(&key).await.map_err(|e| e.to_string())?;
    match h {
        Some(m) if m.get("oscope-kind").map(String::as_str) == Some("data") => Ok(()),
        other => Err(format!("head: {other:?}")),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn credential_modes() {
    let Ok(dir) = std::env::var("OTAPRS_CREDSTUBS") else {
        eprintln!("skipped: set OTAPRS_CREDSTUBS to the credstubs directory");
        return;
    };
    let _ = otel_arrow_dfe_otap::crypto::install_crypto_provider(); // once per process
    let plain = "http://127.0.0.1:18333/otel/otap-rs-edge".to_string();
    let tls = "https://127.0.0.1:18903/otel/otap-rs-edge".to_string();
    let ca = format!("{dir}/ca.pem");
    let token_file = format!("{dir}/token");
    std::fs::write(&token_file, "stub-service-account-token").unwrap();
    let mut results = Vec::new();
    let mut run = |name: &str, r: Result<(), String>, saw: Vec<String>, expect_ok: bool| {
        let ok = r.is_ok() == expect_ok;
        let line = format!(
            "{} {name}: {} | stub saw: {}",
            if ok { "PASS" } else { "FAIL" },
            match &r {
                Ok(()) => "PUT create + 412 + HEAD ok".to_string(),
                Err(e) => format!("error (expected: {}): {}", !expect_ok, e.chars().take(160).collect::<String>()),
            },
            saw.iter().map(|l| l.chars().take(110).collect::<String>()).collect::<Vec<_>>().join(" ; ")
        );
        println!("{line}");
        results.push(ok);
    };

    // 1. Static keys, custom endpoint, path-style (Nutanix Objects, SeaweedFS).
    clear_env();
    let r = roundtrip("static", S3Config {
        url: plain.clone(),
        access_key_id: Some("otel".into()),
        secret_access_key: Some("otelsecret".into()),
        ..Default::default()
    })
    .await;
    run("static keys, http endpoint, path-style", r, vec![], true);

    // 2. Private CA: https endpoint whose certificate is signed by ca.pem.
    let keys = |url: &str| S3Config {
        url: url.to_string(),
        access_key_id: Some("otel".into()),
        secret_access_key: Some("otelsecret".into()),
        ..Default::default()
    };
    clear_env();
    let n = stub_lines(&dir);
    let r = roundtrip("ca-missing", keys(&tls)).await;
    run("private CA, not configured", r, stub_tail(&dir, n), false);
    let n = stub_lines(&dir);
    let r = roundtrip("ca-bundle", S3Config { ca_bundle: Some(ca.clone()), ..keys(&tls) }).await;
    run("private CA via ca_bundle", r, stub_tail(&dir, n), true);
    set("SSL_CERT_FILE", &ca);
    let n = stub_lines(&dir);
    let r = roundtrip("ca-sslcertfile", keys(&tls)).await;
    run("private CA via SSL_CERT_FILE", r, stub_tail(&dir, n), true);

    // 3. EKS IRSA: web identity token file + role ARN. object_store only
    //    talks https to STS (it refuses AWS_ENDPOINT_URL_STS=http://...), so
    //    the default https://sts.us-east-1.amazonaws.com is reached through
    //    the stand-in's CONNECT proxy, which terminates TLS with a
    //    certificate from ca.pem and answers as STS; S3 bypasses the proxy.
    clear_env();
    set("AWS_ROLE_ARN", "arn:aws:iam::111122223333:role/otel-edge");
    set("AWS_WEB_IDENTITY_TOKEN_FILE", &token_file);
    set("AWS_REGION", "us-east-1");
    set("AWS_PROXY_URL", "http://127.0.0.1:18902");
    set("AWS_PROXY_EXCLUDES", "127.0.0.1");
    let n = stub_lines(&dir);
    let r = roundtrip("irsa", S3Config { url: plain.clone(), ca_bundle: Some(ca.clone()), ..Default::default() }).await;
    run("EKS IRSA (AssumeRoleWithWebIdentity via https STS)", r, stub_tail(&dir, n), true);
    unsafe {
        std::env::remove_var("AWS_PROXY_URL");
        std::env::remove_var("AWS_PROXY_EXCLUDES");
    }

    // 4. EKS Pod Identity: container credentials, full URI + token file.
    clear_env();
    set("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:18901/v1/credentials");
    set("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", &token_file);
    let n = stub_lines(&dir);
    let r = roundtrip("podidentity", S3Config { url: plain.clone(), ..Default::default() }).await;
    run("EKS Pod Identity (container credentials)", r, stub_tail(&dir, n), true);

    // 5. IAM Roles Anywhere, `aws_signing_helper serve`: IMDSv2 on localhost,
    //    named the SDKs' way.
    clear_env();
    set("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:18904");
    let n = stub_lines(&dir);
    let r = roundtrip("imds", S3Config { url: plain.clone(), ..Default::default() }).await;
    run("Roles Anywhere via aws_signing_helper serve (IMDSv2)", r, stub_tail(&dir, n), true);

    // 6. IAM Roles Anywhere, credential_process: a script standing in for
    //    `aws_signing_helper credential-process`, counting its runs.
    clear_env();
    let helper = format!("{dir}/aws_signing_helper");
    let calls = format!("{dir}/helper-calls");
    let calls = calls.as_str();
    let _ = std::fs::remove_file(&calls);
    std::fs::write(
        &helper,
        format!(
            "#!/bin/sh\necho run >> {calls}\necho '{{\"Version\":1,\"AccessKeyId\":\"otel\",\"SecretAccessKey\":\"otelsecret\",\"SessionToken\":\"\",\"Expiration\":\"2099-01-01T00:00:00Z\"}}'\n"
        ),
    )
    .unwrap();
    std::process::Command::new("chmod").args(["+x", &helper]).status().unwrap();
    let r = roundtrip(
        "process",
        S3Config {
            url: plain.clone(),
            credential_process: Some(format!("{helper} credential-process --certificate c.pem --private-key k.pem")),
            ..Default::default()
        },
    )
    .await;
    let n_calls = std::fs::read_to_string(&calls).map(|s| s.lines().count()).unwrap_or(0);
    run(
        "Roles Anywhere via credential_process",
        r,
        vec![format!("helper ran {n_calls}x for 3 requests")],
        true,
    );

    // ---- what creds.rs adds ----

    // 7. AWS_CA_BUNDLE: the private CA from the SDKs' variable.
    clear_env();
    set("AWS_CA_BUNDLE", &ca);
    let n = stub_lines(&dir);
    let r = roundtrip("ca-aws-bundle", keys(&tls)).await;
    run("private CA via AWS_CA_BUNDLE", r, stub_tail(&dir, n), true);

    // 8. HTTPS_PROXY / NO_PROXY: the https endpoint through a CONNECT proxy,
    //    then bypassing it.
    let (pport, plog) = tunnel_proxy();
    clear_env();
    set("HTTPS_PROXY", &format!("http://127.0.0.1:{pport}"));
    let r = roundtrip("https-proxy", S3Config { ca_bundle: Some(ca.clone()), ..keys(&tls) }).await;
    let saw: Vec<String> = std::mem::take(&mut *plog.lock().unwrap());
    let tunneled = saw.iter().any(|l| l.contains("127.0.0.1:18903"));
    run("HTTPS_PROXY (S3 requests tunneled)", r.and_then(|_| if tunneled { Ok(()) } else { Err("proxy saw nothing".into()) }), saw, true);
    set("NO_PROXY", "127.0.0.1");
    let r = roundtrip("no-proxy", S3Config { ca_bundle: Some(ca.clone()), ..keys(&tls) }).await;
    let saw: Vec<String> = std::mem::take(&mut *plog.lock().unwrap());
    let ok = saw.is_empty();
    run("NO_PROXY=127.0.0.1 (bypassed)", r.and_then(|_| if ok { Ok(()) } else { Err("proxy was used".into()) }), saw, true);

    // 9-11. Shared config and credentials files.
    let conf = format!("{dir}/aws-config");
    let credf = format!("{dir}/aws-credentials");
    std::fs::write(
        &credf,
        "[default]\naws_access_key_id = otel\naws_secret_access_key = otelsecret\n\n[edge]\naws_access_key_id = otel\naws_secret_access_key = otelsecret\n",
    )
    .unwrap();
    std::fs::write(
        &conf,
        format!(
            "[default]\nregion = us-east-1\n\n[profile helper]\ncredential_process = {helper} credential-process --profile-arn p\n\n\
             [profile chained]\nrole_arn = arn:aws:iam::111122223333:role/otel-chained\nsource_profile = edge\nrole_session_name = edge-chain\nexternal_id = ext-42\n\n\
             [profile sso]\nsso_start_url = https://example.awsapps.com/start\n"
        ),
    )
    .unwrap();
    let files = || {
        clear_env();
        set("AWS_CONFIG_FILE", &conf);
        set("AWS_SHARED_CREDENTIALS_FILE", &credf);
    };
    files();
    set("AWS_PROFILE", "edge");
    let r = roundtrip("profile-static", S3Config { url: plain.clone(), ..Default::default() }).await;
    run("AWS_PROFILE=edge (credentials file keys)", r, vec![], true);
    files();
    let r = roundtrip("profile-default", S3Config { url: plain.clone(), ..Default::default() }).await;
    run("default profile, no AWS_PROFILE", r, vec![], true);
    files();
    set("AWS_PROFILE", "helper");
    let _ = std::fs::remove_file(&calls);
    let r = roundtrip("profile-process", S3Config { url: plain.clone(), ..Default::default() }).await;
    let n_calls = std::fs::read_to_string(&calls).map(|s| s.lines().count()).unwrap_or(0);
    run("AWS_PROFILE=helper (credential_process in the config file)", r, vec![format!("helper ran {n_calls}x")], true);
    files();
    set("AWS_PROFILE", "sso");
    let r = roundtrip("profile-sso", S3Config { url: plain.clone(), ..Default::default() }).await;
    run("AWS_PROFILE=sso (refused with a message)", r, vec![], false);

    // 12. A profile that assumes a role from another profile's keys.
    files();
    set("AWS_PROFILE", "chained");
    set("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:18901");
    let n = stub_lines(&dir);
    let r = roundtrip("profile-role", S3Config { url: plain.clone(), ..Default::default() }).await;
    let saw = stub_tail(&dir, n);
    let ok = saw.iter().any(|l| l.contains("\"Action\":\"AssumeRole\"") && l.contains("otel-chained") && l.contains("ext-42"));
    run("AWS_PROFILE=chained (role_arn + source_profile -> AssumeRole)", r.and_then(|_| if ok { Ok(()) } else { Err("no AssumeRole".into()) }), saw, true);

    // 13. role_arn in config on top of credential_process (Roles Anywhere,
    //     then a second role).
    clear_env();
    set("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:18901");
    let _ = std::fs::remove_file(&calls);
    let n = stub_lines(&dir);
    let r = roundtrip(
        "process-role",
        S3Config {
            url: plain.clone(),
            credential_process: Some(format!("{helper} credential-process")),
            role_arn: Some("arn:aws:iam::111122223333:role/otel-writer".into()),
            ..Default::default()
        },
    )
    .await;
    let mut saw = stub_tail(&dir, n);
    let ok = saw.iter().any(|l| l.contains("AssumeRole") && l.contains("otel-writer"));
    let n_calls = std::fs::read_to_string(&calls).map(|s| s.lines().count()).unwrap_or(0);
    saw.push(format!("helper ran {n_calls}x"));
    run("role_arn on credential_process (AssumeRole, cached)", r.and_then(|_| if ok { Ok(()) } else { Err("no AssumeRole".into()) }), saw, true);

    // 14. role_arn on top of IMDS (aws_signing_helper serve).
    clear_env();
    set("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:18904");
    set("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:18901");
    let n = stub_lines(&dir);
    let r = roundtrip(
        "imds-role",
        S3Config { url: plain.clone(), role_arn: Some("arn:aws:iam::111122223333:role/otel-imds-chain".into()), ..Default::default() },
    )
    .await;
    let saw = stub_tail(&dir, n);
    let ok = saw.iter().any(|l| l.contains("IMDS")) && saw.iter().any(|l| l.contains("otel-imds-chain"));
    run("role_arn on IMDS base (object_store's chain, then AssumeRole)", r.and_then(|_| if ok { Ok(()) } else { Err("chain not seen".into()) }), saw, true);

    // 15. The default https STS reached through HTTPS_PROXY (the stand-in's
    //     CONNECT proxy answers as STS with a leaf from ca.pem, trusted via
    //     AWS_CA_BUNDLE); S3 on 127.0.0.1 bypasses it via NO_PROXY.
    clear_env();
    set("HTTPS_PROXY", "http://127.0.0.1:18902");
    set("NO_PROXY", "127.0.0.1");
    set("AWS_CA_BUNDLE", &ca);
    let n = stub_lines(&dir);
    let r = roundtrip(
        "https-sts-role",
        S3Config {
            url: plain.clone(),
            access_key_id: Some("otel".into()),
            secret_access_key: Some("otelsecret".into()),
            role_arn: Some("arn:aws:iam::111122223333:role/otel-https".into()),
            ..Default::default()
        },
    )
    .await;
    let saw = stub_tail(&dir, n);
    let ok = saw.iter().any(|l| l.contains("CONNECT") && l.contains("sts.us-east-1.amazonaws.com"))
        && saw.iter().any(|l| l.contains("otel-https"));
    run("role_arn via https STS behind HTTPS_PROXY", r.and_then(|_| if ok { Ok(()) } else { Err("STS not reached".into()) }), saw, true);

    clear_env();
    assert!(results.iter().all(|&b| b), "some credential modes failed");
}

/// The SigV4 signer (used for STS AssumeRole) against a server that checks
/// signatures: SeaweedFS accepts a signed ListObjectsV2 and refuses the same
/// request signed with a wrong secret.
#[tokio::test(flavor = "current_thread")]
async fn sigv4_accepted_by_seaweedfs() {
    if std::env::var("OTAPRS_CREDSTUBS").is_err() {
        return;
    }
    let _ = otel_arrow_dfe_otap::crypto::install_crypto_provider(); // once per process
    use otap_s3pq::creds::{amz_date, sha256_hex, sign};
    let c = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut codes = Vec::new();
    for secret in ["otelsecret", "wrong"] {
        let cred = object_store::aws::AwsCredential { key_id: "otel".into(), secret_key: secret.into(), token: None };
        let date = amz_date(std::time::SystemTime::now());
        let ph = sha256_hex(b"");
        let h = vec![
            ("host".to_string(), "127.0.0.1:18333".to_string()),
            ("x-amz-content-sha256".into(), ph.clone()),
            ("x-amz-date".into(), date.clone()),
        ];
        let q = "list-type=2&max-keys=1&prefix=otap-rs";
        let auth = sign("GET", "/otel", q, &h, &ph, &cred, "us-east-1", "s3");
        let r = c
            .get(format!("http://127.0.0.1:18333/otel?{q}"))
            .header("x-amz-content-sha256", ph)
            .header("x-amz-date", date)
            .header("authorization", auth)
            .send()
            .await
            .unwrap();
        codes.push(r.status().as_u16());
    }
    println!("SigV4 ListObjectsV2 at SeaweedFS: right secret {}, wrong secret {}", codes[0], codes[1]);
    assert_eq!(codes, vec![200, 403]);
}
