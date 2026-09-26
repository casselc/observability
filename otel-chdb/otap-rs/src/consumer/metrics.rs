//! Prometheus text exposition (format 0.0.4) for `consume`, and a minimal
//! HTTP endpoint for it (`--metrics-addr`), on tokio's TCP listener alone:
//! no HTTP framework. The process renders its text into a shared buffer
//! whenever its numbers change; the endpoint only copies that buffer out,
//! so a scrape never touches the worker's state and never waits on it.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
}

/// Samples grouped by family (one HELP and TYPE each, in first-use order).
#[derive(Default)]
pub struct Prom {
    order: Vec<String>,
    families: BTreeMap<String, (Kind, String, Vec<String>)>,
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

fn fmt_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf".into() } else { "-Inf".into() }
    } else if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

impl Prom {
    pub fn sample(&mut self, name: &str, kind: Kind, help: &str, labels: &[(&str, &str)], v: f64) -> &mut Self {
        if !self.families.contains_key(name) {
            self.order.push(name.to_string());
        }
        let f = self.families.entry(name.to_string()).or_insert_with(|| (kind, help.to_string(), Vec::new()));
        let mut line = name.to_string();
        if !labels.is_empty() {
            let l: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{}\"", escape_label(v))).collect();
            let _ = write!(line, "{{{}}}", l.join(","));
        }
        let _ = write!(line, " {}", fmt_value(v));
        f.2.push(line);
        self
    }

    pub fn counter(&mut self, name: &str, help: &str, labels: &[(&str, &str)], v: f64) -> &mut Self {
        self.sample(name, Kind::Counter, help, labels, v)
    }

    pub fn gauge(&mut self, name: &str, help: &str, labels: &[(&str, &str)], v: f64) -> &mut Self {
        self.sample(name, Kind::Gauge, help, labels, v)
    }

    /// Declares a family with no samples yet (so a dashboard sees it exist).
    pub fn declare(&mut self, name: &str, kind: Kind, help: &str) -> &mut Self {
        if !self.families.contains_key(name) {
            self.order.push(name.to_string());
            let _ = self.families.insert(name.to_string(), (kind, help.to_string(), Vec::new()));
        }
        self
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        for name in &self.order {
            let (kind, help, lines) = &self.families[name];
            let _ = writeln!(out, "# HELP {name} {}", help.replace('\\', "\\\\").replace('\n', "\\n"));
            let _ = writeln!(out, "# TYPE {name} {}", if *kind == Kind::Counter { "counter" } else { "gauge" });
            for l in lines {
                let _ = writeln!(out, "{l}");
            }
        }
        out
    }
}

/// The text the endpoint serves.
pub type Shared = Arc<Mutex<String>>;

pub fn shared() -> Shared {
    Arc::new(Mutex::new(String::new()))
}

pub fn publish(s: &Shared, text: String) {
    if let Ok(mut g) = s.lock() {
        *g = text;
    }
}

/// Binds `addr` (`host:port`, or `:port` for every interface) and serves
/// `GET /metrics` (any path, in fact) from `text`, one task per connection.
/// Returns the bound address. Errors only if the bind fails.
pub async fn serve(addr: &str, text: Shared) -> std::io::Result<std::net::SocketAddr> {
    let addr = if addr.starts_with(':') { format!("0.0.0.0{addr}") } else { addr.to_string() };
    let l = tokio::net::TcpListener::bind(&addr).await?;
    let local = l.local_addr()?;
    drop(tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = l.accept().await else { continue };
            let text = text.clone();
            drop(tokio::spawn(async move {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), answer(sock, text)).await;
            }));
        }
    }));
    Ok(local)
}

async fn answer(mut sock: tokio::net::TcpStream, text: Shared) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < 16 << 10 {
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let first = String::from_utf8_lossy(&buf).lines().next().unwrap_or("").to_string();
    let (status, body) = if first.starts_with("GET ") || first.starts_with("HEAD ") {
        ("200 OK", text.lock().map(|g| g.clone()).unwrap_or_default())
    } else {
        ("405 Method Not Allowed", String::new())
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(head.as_bytes()).await?;
    if !first.starts_with("HEAD ") {
        sock.write_all(body.as_bytes()).await?;
    }
    sock.shutdown().await
}

/// A worker's counters (`Worker::stats_json`, plus `consume`'s own `ch_*`
/// and `cpu_ms`), as Prometheus families.
pub fn worker_families(p: &mut Prom, v: &serde_json::Value, horizon_ms: Option<u64>) {
    let n = |k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    // No worker label: the worker's name carries a per-start nonce, and the
    // scrape's own instance label already says which process this is.
    let wl: &[(&str, &str)] = &[];
    let with = |extra: (&'static str, &'static str)| -> Vec<(&str, &str)> { vec![extra] };
    p.counter("consumer_objects_ingested_total", "Committed objects inserted into central (verified).", &with(("kind", "data")), n("objects_inserted"));
    p.counter("consumer_objects_ingested_total", "", &with(("kind", "series")), n("series_objects_inserted"));
    p.counter("consumer_rows_ingested_total", "Rows inserted into central.", wl, n("rows_inserted"));
    p.counter("consumer_statements_total", "INSERT statements sent (a statement holds up to --max-batch objects).", wl, n("statements"));
    p.counter("consumer_copies_skipped_total", "Objects the count check found already in central (copies and retries).", wl, n("dedup_skipped"));
    p.counter("consumer_repairs_total", "Objects re-inserted after a verify: missing (whole object) or partial (row repair).", &with(("kind", "missing")), n("retried_missing"));
    p.counter("consumer_repairs_total", "", &with(("kind", "partial")), n("repaired_partial"));
    p.counter("consumer_over_count_total", "Objects with more rows in central than committed (never fixed silently).", wl, n("over_count"));
    p.counter("consumer_insert_errors_total", "Statements that failed.", wl, n("insert_errors"));
    p.counter("consumer_unsettled_statements_total", "Statements with no answer, waited out before their lanes are touched again.", wl, n("unsettled"));
    p.counter("consumer_checks_total", "Count checks: restricted to a partition range, or over every partition.", &with(("range", "ranged")), n("range_checks"));
    p.counter("consumer_checks_total", "", &with(("range", "all")), n("full_checks"));
    p.counter("consumer_check_recounts_total", "Recounts over the horizon after a restricted verify fell short.", wl, n("range_recounts"));
    p.counter("consumer_range_guard_failures_total", "Inserts whose received_at assertion fired.", wl, n("range_guard_failures"));
    p.counter("consumer_lane_lists_total", "LISTs of held lanes' data.", wl, n("lane_lists"));
    p.counter("consumer_lane_lists_skipped_total", "Polls a lane skipped while backing off.", wl, n("lists_skipped"));
    if let Some(s3) = v.get("s3").and_then(|x| x.as_object()) {
        for (op, c) in s3 {
            let op: &str = op;
            p.counter("consumer_s3_requests_total", "S3 requests by kind.", &[("op", op)], c.as_f64().unwrap_or(0.0));
        }
    }
    for (ev, k) in [("taken", "lanes_taken"), ("released", "lanes_released"), ("lapsed", "lanes_lapsed"), ("lost_cas", "lanes_lost_cas")] {
        p.counter("consumer_lane_changes_total", "Lane lease changes of this worker.", &[("event", ev)], n(k));
    }
    p.counter("consumer_gaps_seen_total", "Free slots with a later slot listed (LIST lag or a deleted slot).", wl, n("gaps_seen"));
    p.counter("consumer_epochs_closed_total", "Epochs closed with a tombstone.", wl, n("epochs_closed"));
    p.counter("consumer_errors_total", "Errors on the worker's step (S3 or central).", wl, n("errors"));
    let held = v.get("held").and_then(|x| x.as_array()).map_or(0.0, |a| a.len() as f64);
    p.gauge("consumer_lanes_held", "Lanes this worker holds.", wl, held);
    p.gauge("consumer_lanes_known", "Lanes this worker knows of.", wl, n("lanes_known"));
    p.gauge("consumer_live_workers", "Live workers seen through the heartbeats.", wl, n("live_workers"));
    p.gauge("consumer_visible_seconds", "Receive (edge) to insert returned, over the run.", &with(("quantile", "0.5")), n("visible_ms_p50") / 1e3);
    p.gauge("consumer_visible_seconds", "", &with(("quantile", "0.99")), n("visible_ms_p99") / 1e3);
    p.counter("consumer_cpu_seconds_total", "The process's CPU time.", wl, n("cpu_ms") / 1e3);
    horizon_family(p, horizon_ms);
}

/// The configured copy horizon (`--check-horizon`); -1 for `all`.
pub fn horizon_family(p: &mut Prom, horizon_ms: Option<u64>) {
    p.gauge(
        "consumer_check_horizon_seconds",
        "The count check's copy horizon (-1: every partition).",
        &[],
        horizon_ms.map_or(-1.0, |h| h as f64 / 1e3),
    );
}

/// The audit's families, from its state and its last run.
#[derive(Clone, Debug, Default)]
pub struct AuditMetrics {
    pub runs_ok: u64,
    pub runs_err: u64,
    pub last_success_s: Option<f64>,
    pub last_duration_s: Option<f64>,
    pub last_candidates: u64,
    pub tables: u64,
}

pub fn audit_families(p: &mut Prom, st: &super::audit::AuditState, m: &AuditMetrics, horizon_ms: Option<u64>) {
    let split = |k: &str| -> (String, String) {
        let (s, t) = k.split_once('/').unwrap_or(("", k));
        (s.to_string(), t.to_string())
    };
    p.declare(
        "consumer_late_copies_total",
        Kind::Counter,
        "Copies ingested twice because they were received more than the check horizon after their original (horizon audit).",
    );
    for (k, v) in &st.late_total {
        let (s, t) = split(k);
        p.counter("consumer_late_copies_total", "", &[("signal", &s), ("table", &t)], *v as f64);
    }
    p.declare(
        "consumer_audit_unexplained_copies_total",
        Kind::Counter,
        "Copies ingested twice within the check horizon (the check should have skipped them).",
    );
    for (k, v) in &st.unexplained_total {
        let (s, t) = split(k);
        p.counter("consumer_audit_unexplained_copies_total", "", &[("signal", &s), ("table", &t)], *v as f64);
    }
    p.counter("consumer_audit_runs_total", "Horizon audit runs, by result (error: any table failed).", &[("result", "ok")], m.runs_ok as f64);
    p.counter("consumer_audit_runs_total", "", &[("result", "error")], m.runs_err as f64);
    p.declare("consumer_audit_last_success_timestamp_seconds", Kind::Gauge, "Unix time of the last audit run with no error.");
    if let Some(t) = m.last_success_s {
        p.gauge("consumer_audit_last_success_timestamp_seconds", "", &[], t);
    }
    p.declare("consumer_audit_duration_seconds", Kind::Gauge, "Wall time of the last audit run.");
    if let Some(d) = m.last_duration_s {
        p.gauge("consumer_audit_duration_seconds", "", &[], d);
    }
    p.gauge("consumer_audit_candidates", "Keys in more than one partition, last run (before confirmation).", &[], m.last_candidates as f64);
    p.gauge("consumer_audit_tables", "Tables the last run audited.", &[], m.tables as f64);
    horizon_family(p, horizon_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every line is a comment or `name{labels} value`, every family has one
    /// HELP and one TYPE before its samples, and label values are escaped.
    pub fn well_formed(text: &str) -> Result<(), String> {
        let mut typed = std::collections::HashSet::new();
        let mut helped = std::collections::HashSet::new();
        for l in text.lines() {
            if let Some(r) = l.strip_prefix("# HELP ") {
                let name = r.split(' ').next().unwrap_or("");
                if !helped.insert(name.to_string()) {
                    return Err(format!("two HELP lines for {name}"));
                }
                continue;
            }
            if let Some(r) = l.strip_prefix("# TYPE ") {
                let (name, t) = r.split_once(' ').ok_or(format!("bad TYPE {l}"))?;
                if !["counter", "gauge"].contains(&t) || !typed.insert(name.to_string()) {
                    return Err(format!("bad or repeated TYPE: {l}"));
                }
                continue;
            }
            let name: String = l.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':').collect();
            if name.is_empty() || !typed.contains(&name) {
                return Err(format!("sample before its TYPE: {l}"));
            }
            let rest = &l[name.len()..];
            let value = if let Some(r) = rest.strip_prefix('{') {
                let end = r.rfind("} ").ok_or(format!("bad labels: {l}"))?;
                &r[end + 2..]
            } else {
                rest.strip_prefix(' ').ok_or(format!("bad sample: {l}"))?
            };
            if value.parse::<f64>().is_err() && !["NaN", "+Inf", "-Inf"].contains(&value) {
                return Err(format!("bad value: {l}"));
            }
        }
        Ok(())
    }

    #[test]
    fn text_format() {
        let mut p = Prom::default();
        p.counter("a_total", "A thing.", &[("x", "1")], 3.0);
        p.gauge("b", "B.", &[], 0.25);
        p.counter("a_total", "", &[("x", "q\"u\\o\nte")], 1e20);
        p.declare("c_total", Kind::Counter, "Nothing yet.");
        let t = p.render();
        assert_eq!(
            t,
            "# HELP a_total A thing.\n# TYPE a_total counter\na_total{x=\"1\"} 3\na_total{x=\"q\\\"u\\\\o\\nte\"} 100000000000000000000\n\
             # HELP b B.\n# TYPE b gauge\nb 0.25\n# HELP c_total Nothing yet.\n# TYPE c_total counter\n"
        );
        well_formed(&t).unwrap();
        assert!(well_formed("x 1\n").is_err());
        assert_eq!(fmt_value(f64::NAN), "NaN");
    }

    #[test]
    fn worker_and_audit_families() {
        let v = serde_json::json!({"worker": "w-1", "objects_inserted": 7, "series_objects_inserted": 2, "rows_inserted": 70,
            "range_checks": 5, "full_checks": 1, "held": ["p/traces", "p/logs"], "s3": {"list": 4, "head": 9}, "cpu_ms": 1500.0,
            "visible_ms_p50": 250.0});
        let mut p = Prom::default();
        worker_families(&mut p, &v, Some(3 * 86_400_000));
        let t = p.render();
        well_formed(&t).unwrap();
        for want in [
            "consumer_objects_ingested_total{kind=\"data\"} 7",
            "consumer_objects_ingested_total{kind=\"series\"} 2",
            "consumer_checks_total{range=\"ranged\"} 5",
            "consumer_s3_requests_total{op=\"list\"} 4",
            "consumer_lanes_held 2",
            "consumer_visible_seconds{quantile=\"0.5\"} 0.25",
            "consumer_cpu_seconds_total 1.5",
            "consumer_check_horizon_seconds 259200",
        ] {
            assert!(t.contains(want), "{want} not in\n{t}");
        }
        let mut st = super::super::audit::AuditState::default();
        let _ = st.late_total.insert("logs/otel_logs".into(), 2);
        let m = AuditMetrics { runs_ok: 3, runs_err: 1, last_success_s: Some(1.79e9), last_duration_s: Some(0.5), last_candidates: 4, tables: 6 };
        let mut p = Prom::default();
        audit_families(&mut p, &st, &m, None);
        let t = p.render();
        well_formed(&t).unwrap();
        for want in [
            "consumer_late_copies_total{signal=\"logs\",table=\"otel_logs\"} 2",
            "# TYPE consumer_audit_unexplained_copies_total counter",
            "consumer_audit_runs_total{result=\"error\"} 1",
            "consumer_audit_last_success_timestamp_seconds 1790000000",
            "consumer_audit_duration_seconds 0.5",
            "consumer_check_horizon_seconds -1",
        ] {
            assert!(t.contains(want), "{want} not in\n{t}");
        }
    }

    /// The endpoint answers a scrape with the current text, and a later scrape
    /// with the text published since.
    #[tokio::test(flavor = "current_thread")]
    async fn endpoint_serves_the_text() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let s = shared();
        publish(&s, "# HELP x_total X.\n# TYPE x_total counter\nx_total 1\n".into());
        let addr = serve("127.0.0.1:0", s.clone()).await.unwrap();
        for want in ["x_total 1\n", "x_total 2\n"] {
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
            let mut r = String::new();
            let _ = c.read_to_string(&mut r).await.unwrap();
            assert!(r.starts_with("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4"), "{r}");
            assert!(r.ends_with(want), "{r}");
            let body = r.split("\r\n\r\n").nth(1).unwrap();
            assert!(r.contains(&format!("Content-Length: {}\r\n", body.len())));
            well_formed(body).unwrap();
            publish(&s, "# HELP x_total X.\n# TYPE x_total counter\nx_total 2\n".into());
        }
    }
}
