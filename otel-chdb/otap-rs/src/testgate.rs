//! The one gate for tests that need a service (ClickHouse, S3, ...): skip
//! where the service is absent, FAIL where it is supposed to be present.
//!
//! A test that skips without its service keeps the suite runnable on a
//! laptop, but a skip reads as a pass: two tests skipped silently for lack
//! of a bucket and were broken underneath (STPA.md CAST row 43). So a job
//! that provides services says so, `OSCOPE_REQUIRE_SERVICES=clickhouse,s3`
//! (`ci/services.sh start` writes it to `$GITHUB_ENV`), and in that job the
//! gate panics instead of skipping. The Go twin is `otel-chdb/testgate`.
//!
//! `OSCOPE_REQUIRE_SERVICES`: unset, empty or `0`: nothing required; `1`,
//! `all` or `*`: every service; otherwise service names separated by commas
//! or spaces (case-insensitive). Names in use: `clickhouse`, `s3`,
//! `clickhouse-replicated` (central-replicated/), `credstubs`.
//!
//! ```ignore
//! if ch.query("SELECT 1", &[]).await.is_err() {
//!     return testgate::skip("clickhouse", format!("no ClickHouse at {url}"));
//! }
//! ```
//!
//! Opt-in gates (a slow test behind `HEGEL_DST_MUTANTS`, a dataset behind
//! `OTAPRS_DATA`, a binary behind `FAULTPROXY2`) are not service gates and
//! do not use this.

use std::fmt::Display;

/// The variable that names the services a run requires.
pub const ENV: &str = "OSCOPE_REQUIRE_SERVICES";

/// Whether `spec` (the variable's value) requires `service`.
pub fn required_in(spec: Option<&str>, service: &str) -> bool {
    let Some(spec) = spec.map(str::trim) else { return false };
    match spec {
        "" | "0" => false,
        "1" | "all" | "*" => true,
        _ => spec
            .split(|c: char| c == ',' || c.is_whitespace())
            .any(|s| !s.is_empty() && s.eq_ignore_ascii_case(service)),
    }
}

/// Whether this run requires `service` (`OSCOPE_REQUIRE_SERVICES`).
pub fn required(service: &str) -> bool {
    required_in(std::env::var(ENV).ok().as_deref(), service)
}

/// Skip the calling test for lack of `service` (the caller then returns),
/// or panic if this run requires it. `why` says what was missing.
#[track_caller]
pub fn skip(service: &str, why: impl Display) {
    gate(std::env::var(ENV).ok().as_deref(), service, &why);
    // Not required here, so skipped: a traceability tag records it as such, not as a pass.
    crate::oscope_trace::skipped();
}

#[track_caller]
fn gate(spec: Option<&str>, service: &str, why: &dyn Display) {
    if required_in(spec, service) {
        panic!("{why}: service {service} is required here ({ENV}={}), so this test fails instead of skipping", spec.unwrap_or(""));
    }
    eprintln!("{why}: skipped (service {service}; {ENV} does not require it)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_spec_names_what_is_required() {
        for (spec, svc, want) in [
            (None, "clickhouse", false),
            (Some(""), "clickhouse", false),
            (Some("0"), "s3", false),
            (Some("1"), "clickhouse-replicated", true),
            (Some("all"), "s3", true),
            (Some(" * "), "credstubs", true),
            (Some("clickhouse,s3"), "s3", true),
            (Some("clickhouse, s3"), "s3", true),
            (Some("ClickHouse s3"), "clickhouse", true),
            (Some("clickhouse,s3"), "clickhouse-replicated", false),
            (Some("clickhouse,s3"), "credstubs", false),
            (Some("s3x"), "s3", false),
            (Some(",,"), "s3", false),
        ] {
            assert_eq!(required_in(spec, svc), want, "{spec:?} {svc}");
        }
    }

    #[test]
    fn a_required_service_fails_and_an_optional_one_skips() {
        let _trace = crate::oscope_trace::covers("G", &["CAST-43", "CAST-46"]);
        let r = std::panic::catch_unwind(|| gate(Some("clickhouse,s3"), "s3", &"no S3 at http://x/b"));
        let m = *r.expect_err("required: a failure").downcast::<String>().unwrap();
        assert!(m.starts_with("no S3 at http://x/b: service s3 is required here (OSCOPE_REQUIRE_SERVICES=clickhouse,s3)"), "{m}");
        assert!(std::panic::catch_unwind(|| gate(Some("clickhouse"), "s3", &"no S3")).is_ok());
        assert!(std::panic::catch_unwind(|| gate(None, "clickhouse", &"no ClickHouse")).is_ok());
    }
}
