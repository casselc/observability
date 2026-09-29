//! Runtime traceability tags: which STPA hazards, requirements and CAST rows
//! a test verifies, recorded with how the test ended. The Go twin is
//! `otel-chdb/testgate/tracetag`; ci/trace/trace.py joins the records
//! (ci/README.md, "Traceability").
//!
//! A static tag only says someone meant a test to cover an item; a skipped or
//! unrun test is an unknown, not evidence (STPA.md CAST rows 41, 43, 45, 46,
//! 53). So the tag is a value that lives as long as the test:
//!
//! ```ignore
//! #[test]
//! fn a_server_fenced_announcement_is_not_taken_for_landed() {
//!     let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-42", "LS-5", "H-3"]);
//! ```
//!
//! With `OSCOPE_TRACE_OUT` set, dropping the guard appends one JSON line to
//! that file: the IDs, the technique (VERIFICATION.md §1), the test (the
//! libtest thread's name), the source file, the commit (`GITHUB_SHA`, else
//! `git rev-parse HEAD`), the CI job and the outcome: `failed` when the guard
//! drops while the thread panics, `skipped` when the test went through
//! [`crate::testgate::skip`] (or called [`skipped`]) and returned early,
//! `passed` otherwise. Unset, nothing is written.
//!
//! Bind it to a named variable (`_trace`, not `_`), first in the test. A
//! property test whose body runs once per case records once per outcome.

use std::cell::Cell;
use std::collections::HashSet;
use std::io::Write;
use std::panic::Location;
use std::sync::{Mutex, OnceLock};

/// The variable naming the JSONL file records are appended to.
pub const ENV: &str = "OSCOPE_TRACE_OUT";

thread_local! {
    static SKIPPED: Cell<bool> = const { Cell::new(false) };
}

/// Marks the current test as skipped: its guard records `skipped`, not
/// `passed`. `testgate::skip` calls it; an opt-in gate (an env variable a
/// slow test waits for) calls it before its early return.
pub fn skipped() {
    SKIPPED.with(|s| s.set(true));
}

/// The guard [`covers`] returns; it records on drop.
#[must_use = "bind the guard to `_trace`: dropped at once, it records before the test has run"]
pub struct Covers {
    rec: Option<Record>,
}

#[derive(Clone)]
struct Record {
    ids: Vec<String>,
    technique: String,
    test: String,
    file: String,
    out: String,
}

/// Tags the calling test as verifying `ids` with `technique`.
#[track_caller]
pub fn covers(technique: &str, ids: &[&str]) -> Covers {
    SKIPPED.with(|s| s.set(false));
    let Some(out) = std::env::var_os(ENV).filter(|v| !v.is_empty()) else { return Covers { rec: None } };
    let test = std::thread::current().name().unwrap_or("unknown").to_string();
    Covers {
        rec: Some(Record {
            ids: ids.iter().map(|s| s.to_string()).collect(),
            technique: technique.to_string(),
            test,
            file: repo_path(Location::caller().file()),
            out: out.to_string_lossy().into_owned(),
        }),
    }
}

impl Drop for Covers {
    fn drop(&mut self) {
        let Some(r) = self.rec.take() else { return };
        let outcome = if std::thread::panicking() {
            "failed"
        } else if SKIPPED.with(|s| s.replace(false)) {
            "skipped"
        } else {
            "passed"
        };
        // One record per (test, outcome) per process: a property body runs once per case.
        static SEEN: OnceLock<Mutex<HashSet<(String, &'static str)>>> = OnceLock::new();
        let seen = SEEN.get_or_init(Default::default);
        if !seen.lock().map(|mut s| s.insert((r.test.clone(), outcome))).unwrap_or(true) {
            return;
        }
        let func = r.test.rsplit("::").next().unwrap_or(&r.test).to_string();
        let line = serde_json::json!({
            "ids": r.ids,
            "technique": r.technique,
            "test": r.test,
            "func": func,
            "package": format!("otap-s3pq/{}", target()),
            "file": r.file,
            "lang": "rust",
            "commit": commit(),
            "job": std::env::var("GITHUB_JOB").unwrap_or_default(),
            "outcome": outcome,
        });
        if let Err(e) = append(&r.out, &line.to_string()) {
            // Not a panic in a drop that may already be unwinding: a lost record
            // is caught by the traceability job as a tagged test that did not run.
            eprintln!("oscope_trace: {ENV}={}: {e}", r.out);
        }
    }
}

/// The test binary's name without cargo's hash (`dst_consumer`, `consume`, `otap_s3pq`).
fn target() -> String {
    let exe = std::env::current_exe().ok();
    let stem = exe.as_ref().and_then(|p| p.file_stem()).and_then(|s| s.to_str()).unwrap_or("");
    match stem.rsplit_once('-') {
        Some((name, hash)) if hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit()) => name.to_string(),
        _ => stem.to_string(),
    }
}

/// A source path as the static listing names it: from the repository root.
fn repo_path(file: &str) -> String {
    let file = file.replace('\\', "/");
    if let Some(i) = file.rfind("/otel-chdb/") {
        return normalize(&file[i + 1..]);
    }
    normalize(&format!("otel-chdb/otap-rs/{file}"))
}

fn normalize(p: &str) -> String {
    let mut out: Vec<&str> = Vec::new();
    for part in p.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                out.pop();
            }
            _ => out.push(part),
        }
    }
    out.join("/")
}

fn commit() -> String {
    static C: OnceLock<String> = OnceLock::new();
    C.get_or_init(|| {
        if let Some(s) = std::env::var("GITHUB_SHA").ok().filter(|s| !s.is_empty()) {
            return s;
        }
        std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    })
    .clone()
}

/// One line, one `write` on an `O_APPEND` file: records from parallel test
/// binaries do not interleave.
fn append(path: &str, line: &str) -> std::io::Result<()> {
    if let Some(dir) = std::path::Path::new(path).parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    f.write_all(format!("{line}\n").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_from_the_repository_root() {
        assert_eq!(repo_path("src/consumer/tests.rs"), "otel-chdb/otap-rs/src/consumer/tests.rs");
        assert_eq!(repo_path("tests/../src/consumer/tests.rs"), "otel-chdb/otap-rs/src/consumer/tests.rs");
        assert_eq!(repo_path("/w/x/otel-chdb/otap-rs/tests/dst_consumer.rs"), "otel-chdb/otap-rs/tests/dst_consumer.rs");
    }

    /// Every outcome, written to a file of its own. The env variable is read
    /// in `covers`, so each case sets it on a thread of its own name, the
    /// way libtest runs tests.
    #[test]
    fn records_passed_failed_and_skipped() {
        let dir = std::env::temp_dir().join(format!("oscope-trace-{}", std::process::id()));
        let out = dir.join("t.jsonl");
        let _ = std::fs::remove_file(&out);
        let run = |name: &str, f: fn()| {
            let out = out.clone();
            std::thread::Builder::new()
                .name(format!("oscope_trace::fixture::{name}"))
                .spawn(move || {
                    let g = Covers {
                        rec: Some(Record {
                            ids: vec!["H-2".into(), "CAST-1".into()],
                            technique: "P".into(),
                            test: std::thread::current().name().unwrap().to_string(),
                            file: "otel-chdb/otap-rs/src/oscope_trace.rs".into(),
                            out: out.to_string_lossy().into_owned(),
                        }),
                    };
                    SKIPPED.with(|s| s.set(false));
                    f();
                    drop(g);
                })
                .unwrap()
                .join()
        };
        assert!(run("passes", || {}).is_ok());
        // Through the gate where this run does not require the service (the hook under test);
        // a run with OSCOPE_REQUIRE_SERVICES=all would fail there, so mark it directly.
        assert!(run("skips", || if crate::testgate::required("oscope-trace-fixture") { skipped() } else { crate::testgate::skip("oscope-trace-fixture", "fixture") }).is_ok());
        assert!(run("fails", || panic!("planned")).is_err());
        let text = std::fs::read_to_string(&out).unwrap();
        let mut got: Vec<(String, String, String)> = text
            .lines()
            .map(|l| {
                let v: serde_json::Value = serde_json::from_str(l).unwrap();
                (v["func"].as_str().unwrap().to_string(), v["outcome"].as_str().unwrap().to_string(), v["lang"].as_str().unwrap().to_string())
            })
            .collect();
        got.sort();
        let want = [("fails", "failed"), ("passes", "passed"), ("skips", "skipped")];
        assert_eq!(got.len(), 3, "{text}");
        for ((f, o, l), (wf, wo)) in got.iter().zip(want) {
            assert_eq!((f.as_str(), o.as_str(), l.as_str()), (wf, wo, "rust"), "{text}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
