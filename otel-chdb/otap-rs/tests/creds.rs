//! Credential modes of the S3 client, against local stand-ins for the AWS
//! side (../parquetgo/compare/cmd/credstubs: Pod Identity agent and STS on
//! :18901, IMDSv2 as `aws_signing_helper serve` provides it on :18904, a TLS
//! proxy to SeaweedFS with a private CA on :18903). Each mode builds the
//! store with no keys in config, then does the protocol's own requests (a
//! create-only PUT with metadata, and a HEAD), and checks the stand-in saw
//! the credential flow.
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
];

fn clear_env() {
    for v in VARS {
        unsafe { std::env::remove_var(v) };
    }
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
    otel_arrow_dfe_otap::crypto::install_crypto_provider().expect("crypto provider");
    let plain = "http://127.0.0.1:18333/otel/otap-rs".to_string();
    let tls = "https://127.0.0.1:18903/otel/otap-rs".to_string();
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

    // 3. EKS IRSA: web identity token file + role ARN, STS at the stand-in.
    clear_env();
    set("AWS_ROLE_ARN", "arn:aws:iam::111122223333:role/otel-edge");
    set("AWS_WEB_IDENTITY_TOKEN_FILE", &token_file);
    set("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:18901");
    set("AWS_REGION", "us-east-1");
    let n = stub_lines(&dir);
    let r = roundtrip("irsa", S3Config { url: plain.clone(), ..Default::default() }).await;
    run("EKS IRSA (AssumeRoleWithWebIdentity)", r, stub_tail(&dir, n), true);

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
    clear_env();
    assert!(results.iter().all(|&b| b), "some credential modes failed");
}
