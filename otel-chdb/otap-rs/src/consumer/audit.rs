//! The horizon audit: finds the copies the count check's partition range
//! could not see, after the fact.
//!
//! The check (`plan::check_range`) reads only the partitions of a batch's own
//! `received_at` ± the copy horizon (`--check-horizon`, 3 days by default). A
//! copy of a request carries a new, later `received_at` (the edge stamps it at
//! receipt, so a durable buffer's replay or a sender's resend is "received"
//! again). A copy received more than the horizon after its original is not
//! found and is ingested a second time, and nothing on the ingest path can
//! tell. This audit finds exactly those: content keys whose rows are in
//! central twice, in partitions farther apart than the check reaches.
//!
//! **What it detects is the failure itself,** not a proxy: a content key is
//! one request's rows, and every row an insert writes for an object carries
//! that object's constant `received_at` (the insert asserts it), so one
//! ingestion of an object is in exactly one partition, and in one day
//! (`_partition_value.1`; under `(toDate(received_at), late_part)` the
//! second element is object-constant too, DECISIONS.md D34). A key on two
//! days was ingested at least twice.
//!
//! Per table, per run:
//!
//! 1. **Candidates, from the content projection** (`by_content`, the same
//!    one the check uses; `_partition_value` keeps it in use [M]): the keys
//!    present in a recent partition (`received_at` day ≥ now − lookback) that
//!    are also present in any other partition. It reads the projection of
//!    every part (the whole retention) for the recent keys: the cost of one
//!    unranged check, once per audit instead of once per statement.
//!    `sample_hex` restricts both sides to keys starting with that many `0`s
//!    (1/16 per digit; the projection is ordered by the key, so a prefix
//!    range reads that fraction of it).
//! 2. **Confirmation, from the table,** for the candidates only and only in
//!    their partitions: per key, rows − distinct `row_ordinal` (the rows that
//!    are there twice; an object whose rows carry several received times, a
//!    foreign producer's, is spread over partitions without a duplicated row
//!    and is not a copy), and per (day, producer, epoch) the earliest
//!    `received_at` and the rows: each such group is one ingestion.
//! 3. **Classification** (`classify`): ordered by received time, every
//!    ingestion after the first is a copy. It is **late** (the horizon's
//!    assumption broke) if, whichever of the two was ingested second, the
//!    check could have missed every earlier one: the earlier one's partition
//!    is before the later one's reach (`toDate(recv − h)`), or the later
//!    one's after the earlier one's reach (`toDate(recv + h)`). Otherwise it
//!    is **unexplained**: a duplicate the check should have prevented (a bug,
//!    or `--check-horizon` wider on the workers than here).
//!
//! Each copy is reported once (the state remembers them for the lookback;
//! `consume` keeps the state on S3), logged as a WARN with its lane, content
//! key and ages, and counted in `consumer_late_copies_total` /
//! `consumer_audit_unexplained_copies_total`. An audit failure is reported
//! (`consumer_audit_runs_total{result="error"}`, the last success time), never
//! fatal, and the audit runs outside the workers (`consume gc
//! --audit-every`, or `consume horizon-audit`), so it never delays ingest.
//!
//! What it does not see: a duplicate within one partition (a copy received
//! the same day, which the check can always see), and a table whose
//! partition key isn't one the check's range understands
//! (`sql::RANGE_PARTITION_KEYS`; skipped: its checks read every partition
//! anyway).
//!
//! **Duplicates by content** (AMBIGUITY.md E1, E2). A copy under a *new*
//! content key is invisible to everything above: a gateway SIGKILL re-cuts
//! the agents' resent requests into new pieces, and a sender's resend after
//! a lost answer lands in a new batch at a publisher with a batch step. Each
//! run therefore also counts, per table, the rows received in the lookback
//! whose **row identity** (`row_identity`: a few cheap columns a copy shares
//! with its original, whatever request carried it) appears under more than
//! one content key: Σ (distinct content keys − 1) over the identities
//! (`dup_rows_sql`). Rows repeated inside one request, and a late copy of
//! the same request (same key, counted above), are not counted. It reads
//! only the identity columns of the lookback's partitions (logs: the body's
//! length, `Body.size`, not the body), and with `dup_sample` n > 1 keeps
//! only the identities whose hash is 0 mod n (every copy of a row shares its
//! hash, so the sample is unbiased and a burst is seen at 1/n of its size)
//! and reports the count × n. It is a gauge,
//! `consumer_audit_duplicate_rows{signal,table}`: the duplicates within the
//! last lookback, as of the last run. The identities are estimates of
//! "the same item": two genuinely distinct items with the same identity (a
//! log line repeated with the same nanosecond, service, severity, trace,
//! span and body length in two requests; a metric point of one series at
//! the same second) count too.

use super::plan::DAY_NS;
use super::sql::{LaneKind, range_partition_key};
use otap_s3pq::Signal;
use otap_s3pq::central::{ClickHouse, sq};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub struct AuditConfig {
    /// The workers' copy horizon (`--check-horizon`); None: they read every
    /// partition, so any copy found is unexplained.
    pub horizon_ms: Option<u64>,
    /// The recent side: partitions from `toDate(now − lookback)` on.
    pub lookback_ms: u64,
    /// 0: every key; k: only keys starting with k `0`s (1/16^k of them).
    pub sample_hex: usize,
    /// At most this many candidate keys per table per run (the rest wait for
    /// the next run; `truncated` says so).
    pub max_candidates: usize,
    /// Only these tables (empty: every counted table in the database).
    pub tables: Vec<String>,
    /// `max_threads` for the audit's queries: it shares central with the
    /// inserts, and is in no hurry.
    pub max_threads: u32,
    /// A replicated central (`--sync-replica`): before a table is read,
    /// `SYSTEM SYNC REPLICA … LIGHTWEIGHT`, so the replica the audit reads
    /// holds every part committed on any replica. Without it a lagging
    /// replica answers without the parts it hasn't fetched, and a copy among
    /// them is missed by that run (central-replicated/README.md). A sync that
    /// fails (the source replica down, `sync_timeout_ms`) is that table's
    /// error: the run counts as failed and the next one covers the window.
    pub sync_replica: bool,
    pub sync_timeout_ms: u64,
    /// Duplicates by content (`dup_rows_sql`): 0 off, 1 exact, n: an
    /// estimate from 1/n of the row identities.
    pub dup_sample: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        AuditConfig { horizon_ms: Some(super::worker::DEFAULT_HORIZON_MS), lookback_ms: 2 * 86_400_000, sample_hex: 0, max_candidates: 1000, tables: Vec::new(), max_threads: 2, sync_replica: false, sync_timeout_ms: 60_000, dup_sample: 16 }
    }
}

/// One ingestion of a content key: its rows in one partition, from one
/// producer epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Group {
    pub day: u64,
    pub producer: String,
    pub epoch: String,
    /// The earliest `received_at` of its rows, ns.
    pub recv_ns: u64,
    pub rows: u64,
}

/// A copy found: `copy` is an ingestion after `original` (the key's first).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Found {
    pub signal: String,
    pub table: String,
    pub key: String,
    pub original: Group,
    pub copy: Group,
    /// Beyond the horizon (the assumption broke); false: unexplained.
    pub late: bool,
    /// Rows of the key that are in central more than once (all its groups).
    pub dup_rows: u64,
}

impl Found {
    /// The WARN line.
    pub fn log_line(&self, horizon_ms: Option<u64>) -> String {
        let h = |ns: u64| format!("{:.1}h", ns as f64 / 3.6e12);
        format!(
            "WARN horizon-audit: {} copy in {}: lane {}/{} epoch {} received {}, {} after its original (lane {}/{} epoch {} received {}); content_key {}, {} rows, {} rows duplicated; check horizon {}",
            if self.late { "late" } else { "unexplained" },
            self.table,
            self.copy.producer,
            self.signal,
            self.copy.epoch,
            utc(self.copy.recv_ns),
            h(self.copy.recv_ns.saturating_sub(self.original.recv_ns)),
            self.original.producer,
            self.signal,
            self.original.epoch,
            utc(self.original.recv_ns),
            self.key,
            self.copy.rows,
            self.dup_rows,
            horizon_ms.map_or("all".to_string(), |h| format!("{:.1}h", h as f64 / 3.6e6)),
        )
    }
}

/// `2026-09-21T12:00:00Z`.
pub fn utc(ns: u64) -> String {
    let s = ns / 1_000_000_000;
    let (y, m, d) = otap_s3pq::proto::civil_from_days((s / 86_400) as i64);
    let sod = s % 86_400;
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", sod / 3600, sod / 60 % 60, sod % 60)
}

/// Whether the count check could have missed `a` when ingesting `b`, or `b`
/// when ingesting `a` (`a` received first): the check of an object received
/// at r reads days `toDate(r − h) ..= toDate(r + h)` (`plan::check_range`).
pub fn beyond(a: &Group, b: &Group, horizon_ns: Option<u64>) -> bool {
    let Some(h) = horizon_ns else { return false };
    a.day < b.recv_ns.saturating_sub(h) / DAY_NS || b.day > a.recv_ns.saturating_add(h) / DAY_NS
}

/// The copies among one key's ingestions: every one after the first, by
/// received time; late if beyond the horizon of every earlier one.
pub fn classify(groups: &[Group], horizon_ns: Option<u64>) -> Vec<(usize, bool)> {
    let mut idx: Vec<usize> = (0..groups.len()).collect();
    idx.sort_by_key(|&i| (groups[i].recv_ns, groups[i].day, groups[i].epoch.clone()));
    let mut out = Vec::new();
    for (n, &i) in idx.iter().enumerate().skip(1) {
        let late = idx[..n].iter().all(|&j| beyond(&groups[j], &groups[i], horizon_ns));
        out.push((i, late));
    }
    out
}

fn sample_sql(sample_hex: usize) -> String {
    if sample_hex == 0 {
        return String::new();
    }
    let lo = "0".repeat(sample_hex);
    let hi = format!("{}1", "0".repeat(sample_hex - 1));
    format!(" AND content_key >= {} AND content_key < {}", sq(&lo), sq(&hi))
}

/// Step 1: keys in a partition from `since_ns`'s day on that are also in
/// another partition, with their partitions (day numbers). Answered by the
/// content projection.
pub fn candidates_sql(fq: &str, since_ns: u64, sample_hex: usize, limit: usize) -> String {
    let since = format!("toDate(fromUnixTimestamp64Nano(toInt64({})))", since_ns.min(i64::MAX as u64));
    let s = sample_sql(sample_hex);
    format!(
        "SELECT content_key, groupArray(toUInt32(d)) FROM (SELECT content_key, _partition_value.1 AS d FROM {fq} \
         WHERE content_key IN (SELECT content_key FROM {fq} WHERE _partition_value.1 >= {since}{s}){s} GROUP BY content_key, d) \
         GROUP BY content_key HAVING count() > 1 AND max(d) >= {since} ORDER BY content_key LIMIT {limit} FORMAT TSV"
    )
}

fn keys_days(keys: &[String], days: &BTreeSet<u64>) -> String {
    let k: Vec<String> = keys.iter().map(|k| sq(k)).collect();
    let d: Vec<String> = days.iter().map(|d| d.to_string()).collect();
    format!("content_key IN ({}) AND toUInt32(_partition_value.1) IN ({})", k.join(", "), d.join(", "))
}

/// Step 2a: per candidate key, rows that are in central more than once.
pub fn dups_sql(fq: &str, keys: &[String], days: &BTreeSet<u64>) -> String {
    format!("SELECT content_key, count() - uniqExact(row_ordinal) FROM {fq} WHERE {} GROUP BY content_key FORMAT TSV", keys_days(keys, days))
}

/// Step 2b: per candidate key, its ingestions: (day, producer, epoch), the
/// earliest received time and the rows.
pub fn groups_sql(fq: &str, keys: &[String], days: &BTreeSet<u64>) -> String {
    format!(
        "SELECT content_key, toUInt32(toDate(received_at)) AS d, producer_id, producer_epoch, toUnixTimestamp64Nano(min(received_at)), count() \
         FROM {fq} WHERE {} GROUP BY content_key, d, producer_id, producer_epoch FORMAT TSV",
        keys_days(keys, days)
    )
}

/// The columns that identify a row whatever request carried it, per table:
/// cheap ones (no map, no body), so the count reads a fraction of the table.
pub fn row_identity(table: &str) -> Option<&'static str> {
    Some(match table {
        "otel_traces" => "TraceId, SpanId, Timestamp",
        "otel_logs" => "Timestamp, ServiceName, SeverityNumber, TraceId, SpanId, EventName, Body.size",
        t if t.starts_with("otel_metrics_") && t.ends_with("_points") => "series_id, StartTimeUnix, TimeUnix",
        t if t.starts_with("otel_metrics_") => "ServiceName, MetricName, Attributes, StartTimeUnix, TimeUnix",
        _ => return None,
    })
}

/// Duplicates by content: rows received from `since_ns`'s day on whose
/// identity is under more than one content key, Σ (keys − 1), in the sample
/// (`sample` > 1: identities whose hash is 0 mod `sample`).
pub fn dup_rows_sql(fq: &str, identity: &str, since_ns: u64, sample: u64) -> String {
    let since = format!("toDate(fromUnixTimestamp64Nano(toInt64({})))", since_ns.min(i64::MAX as u64));
    let s = if sample > 1 { format!(" AND cityHash64({identity}) % {sample} = 0") } else { String::new() };
    format!(
        "SELECT sum(k - 1) FROM (SELECT uniqExact(content_key) AS k FROM {fq} WHERE _partition_value.1 >= {since}{s} \
         GROUP BY cityHash64({identity}) HAVING k > 1) FORMAT TSV"
    )
}

/// `[19000,19004]` → the days.
fn parse_days(s: &str) -> Result<Vec<u64>, String> {
    s.trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse::<u64>().map_err(|e| format!("audit: day {x:?}: {e}")))
        .collect()
}

/// TSV unescaping for the few escapes ClickHouse writes.
fn unescape(s: &str) -> String {
    if !s.contains('\\') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('0') => out.push('\0'),
            Some(x) => out.push(x),
            None => out.push('\\'),
        }
    }
    out
}

pub fn parse_candidates(tsv: &str) -> Result<Vec<(String, Vec<u64>)>, String> {
    tsv.lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (k, d) = l.split_once('\t').ok_or_else(|| format!("audit: bad candidate line {l:?}"))?;
            Ok((unescape(k), parse_days(d)?))
        })
        .collect()
}

pub fn parse_dups(tsv: &str) -> Result<BTreeMap<String, u64>, String> {
    tsv.lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (k, n) = l.split_once('\t').ok_or_else(|| format!("audit: bad dups line {l:?}"))?;
            Ok((unescape(k), n.trim().parse::<i64>().map_err(|e| format!("audit: {e}"))?.max(0) as u64))
        })
        .collect()
}

pub fn parse_groups(tsv: &str) -> Result<BTreeMap<String, Vec<Group>>, String> {
    let mut out: BTreeMap<String, Vec<Group>> = BTreeMap::new();
    for l in tsv.lines().filter(|l| !l.is_empty()) {
        let f: Vec<&str> = l.split('\t').collect();
        if f.len() != 6 {
            return Err(format!("audit: bad group line {l:?}"));
        }
        let num = |s: &str| s.trim().parse::<u64>().map_err(|e| format!("audit: {s:?}: {e}"));
        out.entry(unescape(f[0])).or_default().push(Group {
            day: num(f[1])?,
            producer: unescape(f[2]),
            epoch: unescape(f[3]),
            recv_ns: num(f[4])?,
            rows: num(f[5])?,
        });
    }
    Ok(out)
}

/// The copies of the candidates, from the confirmation queries' answers.
pub fn findings(signal: &str, table: &str, dups: &BTreeMap<String, u64>, groups: &BTreeMap<String, Vec<Group>>, horizon_ns: Option<u64>) -> Vec<Found> {
    let mut out = Vec::new();
    for (key, gs) in groups {
        let dup_rows = dups.get(key).copied().unwrap_or(0);
        if dup_rows == 0 || gs.len() < 2 {
            continue; // spread over partitions without a repeated row: not a copy
        }
        let first = gs.iter().min_by_key(|g| (g.recv_ns, g.day, g.epoch.clone())).expect("groups").clone();
        for (i, late) in classify(gs, horizon_ns) {
            out.push(Found { signal: signal.into(), table: table.into(), key: key.clone(), original: first.clone(), copy: gs[i].clone(), late, dup_rows });
        }
    }
    out
}

/// A table the audit covers, and the signal it is named after in metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub signal: String,
    pub table: String,
}

/// The counted tables (every lane kind but the series lane), one per table.
pub fn targets(only: &[String]) -> Vec<Target> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for s in Signal::ALL {
        let Some(k) = LaneKind::for_signal(s.name()) else { continue };
        if !k.counted || (!only.is_empty() && !only.contains(&k.table)) || !seen.insert(k.table.clone()) {
            continue;
        }
        out.push(Target { signal: k.signal, table: k.table });
    }
    out
}

/// What the audit remembers between runs (kept on S3 by `consume`): the
/// copies already reported, so each is counted once, and the totals.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuditState {
    /// table → "key/epoch/day" → the copy's received time (ns); forgotten
    /// once it is older than the lookback (it can't be found again then).
    pub reported: BTreeMap<String, BTreeMap<String, u64>>,
    /// (signal, table) → copies, since the state began.
    pub late_total: BTreeMap<String, u64>,
    pub unexplained_total: BTreeMap<String, u64>,
    /// (signal, table) → duplicates by content in the last run's lookback
    /// (an estimate when sampled): a gauge, replaced by each run that counts.
    #[serde(default)]
    pub duplicate_rows: BTreeMap<String, u64>,
}

/// One run's outcome, per table and overall.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AuditReport {
    pub tables: usize,
    pub skipped_tables: Vec<String>,
    pub candidates: usize,
    pub truncated: Vec<String>,
    /// New copies found in this run (already-reported ones excluded).
    pub late: Vec<Found>,
    pub unexplained: Vec<Found>,
    pub errors: Vec<String>,
    pub duration_ms: u64,
    /// (signal/table) → duplicates by content, estimated (`dup_rows_sql`).
    pub duplicate_rows: BTreeMap<String, u64>,
    /// The replica this run read (`run_replicas`).
    pub replica: String,
}

impl AuditState {
    /// Records the new findings; returns those not reported before.
    pub fn record(&mut self, found: Vec<Found>) -> Vec<Found> {
        let mut new = Vec::new();
        for f in found {
            let id = format!("{}/{}/{}", f.key, f.copy.epoch, f.copy.day);
            let r = self.reported.entry(f.table.clone()).or_default();
            if r.contains_key(&id) {
                continue;
            }
            let _ = r.insert(id, f.copy.recv_ns);
            let label = format!("{}/{}", f.signal, f.table);
            *(if f.late { &mut self.late_total } else { &mut self.unexplained_total }).entry(label).or_default() += 1;
            new.push(f);
        }
        new
    }

    /// Forgets copies received before `before_ns`'s day minus one (the recent
    /// side of any later run starts after it).
    pub fn prune(&mut self, before_ns: u64) {
        let cut = (before_ns / DAY_NS).saturating_sub(1) * DAY_NS;
        for r in self.reported.values_mut() {
            r.retain(|_, recv| *recv >= cut);
        }
        self.reported.retain(|_, r| !r.is_empty());
    }
}

/// One audit of one database: every covered table whose partition key the
/// check's range understands (`toDate(received_at)`, or first in a tuple with
/// `late_part`). Errors are per table and reported; nothing is fatal.
pub async fn run(ch: &ClickHouse, db: &str, cfg: &AuditConfig, state: &mut AuditState, now_ns: u64) -> AuditReport {
    let t0 = super::mono_ms();
    let mut rep = AuditReport::default();
    let since = now_ns.saturating_sub(cfg.lookback_ms.saturating_mul(1_000_000));
    let h = cfg.horizon_ms.map(|h| h.saturating_mul(1_000_000));
    let keys = match ch.query(&format!("SELECT name, partition_key FROM system.tables WHERE database = {} FORMAT TSV", sq(db)), &[]).await {
        Ok(s) => s.lines().filter_map(|l| l.split_once('\t')).map(|(n, k)| (n.to_string(), k.to_string())).collect::<BTreeMap<_, _>>(),
        Err(e) => {
            rep.errors.push(format!("system.tables: {e}"));
            rep.duration_ms = super::mono_ms() - t0;
            return rep;
        }
    };
    let mut found = Vec::new();
    for t in targets(&cfg.tables) {
        match keys.get(&t.table) {
            Some(k) if range_partition_key(k) => {}
            Some(_) => {
                rep.skipped_tables.push(t.table.clone());
                continue;
            }
            None => continue,
        }
        rep.tables += 1;
        // Every audited table has its counters, at 0 until a copy is found
        // (an alert on `increase()` needs the series to exist).
        let label = format!("{}/{}", t.signal, t.table);
        let _ = state.late_total.entry(label.clone()).or_default();
        let _ = state.unexplained_total.entry(label).or_default();
        match audit_table(ch, db, &t, cfg, since, h, &mut rep).await {
            Ok(f) => found.extend(f),
            Err(e) => rep.errors.push(format!("{}: {e}", t.table)),
        }
        if let (Some(id), n @ 1..) = (row_identity(&t.table), cfg.dup_sample) {
            let fq = format!("{db}.{}", t.table);
            let threads = cfg.max_threads.max(1).to_string();
            let st = [("max_threads", threads.as_str())];
            match ch.query(&dup_rows_sql(&fq, id, since, n), &super::sql::no_partial_results(&st)).await {
                Ok(v) => match v.trim().parse::<u64>() {
                    Ok(d) => {
                        let label = format!("{}/{}", t.signal, t.table);
                        let _ = rep.duplicate_rows.insert(label.clone(), d.saturating_mul(n));
                        let _ = state.duplicate_rows.insert(label, d.saturating_mul(n));
                    }
                    Err(e) => rep.errors.push(format!("{}: duplicates: {v:?}: {e}", t.table)),
                },
                Err(e) => rep.errors.push(format!("{}: duplicates: {e}", t.table)),
            }
        }
    }
    for f in state.record(found) {
        if f.late { rep.late.push(f) } else { rep.unexplained.push(f) }
    }
    state.prune(since);
    rep.duration_ms = super::mono_ms() - t0;
    rep
}

/// `run` on the first of `replicas` (from `*cur`) that answers; `*cur`
/// sticks to it for the next run. Each run reads one replica (with
/// `sync_replica`, synced first, so any replica gives the same answer).
pub async fn run_replicas(replicas: &[ClickHouse], cur: &mut usize, db: &str, cfg: &AuditConfig, state: &mut AuditState, now_ns: u64) -> AuditReport {
    let n = replicas.len().max(1);
    let mut first_err = None;
    for i in 0..n {
        let j = (*cur + i) % n;
        let Some(ch) = replicas.get(j) else { break };
        match ch.query("SELECT 1", &[]).await {
            Ok(_) => {
                *cur = j;
                let mut rep = run(ch, db, cfg, state, now_ns).await;
                rep.replica = ch.url.clone();
                return rep;
            }
            Err(e) => {
                let _ = first_err.get_or_insert(format!("{}: {e}", ch.url));
            }
        }
    }
    AuditReport { errors: vec![format!("no replica answers ({})", first_err.unwrap_or_default())], ..Default::default() }
}

async fn audit_table(ch: &ClickHouse, db: &str, t: &Target, cfg: &AuditConfig, since_ns: u64, h: Option<u64>, rep: &mut AuditReport) -> Result<Vec<Found>, String> {
    let fq = format!("{db}.{}", t.table);
    if cfg.sync_replica {
        let to = format!("{:.3}", cfg.sync_timeout_ms as f64 / 1000.0);
        let _ = ch.query(&format!("SYSTEM SYNC REPLICA {fq} LIGHTWEIGHT"), &[("receive_timeout", to.as_str())]).await.map_err(|e| format!("sync replica: {e}"))?;
    }
    let threads = cfg.max_threads.max(1).to_string();
    let st = [("optimize_use_projections", "1"), ("max_threads", threads.as_str())];
    let c = parse_candidates(&ch.query(&candidates_sql(&fq, since_ns, cfg.sample_hex, cfg.max_candidates), &super::sql::no_partial_results(&st)).await?)?;
    rep.candidates += c.len();
    if c.len() >= cfg.max_candidates {
        rep.truncated.push(t.table.clone());
    }
    if c.is_empty() {
        return Ok(Vec::new());
    }
    let keys: Vec<String> = c.iter().map(|(k, _)| k.clone()).collect();
    let days: BTreeSet<u64> = c.iter().flat_map(|(_, d)| d.iter().copied()).collect();
    let dups = parse_dups(&ch.query(&dups_sql(&fq, &keys, &days), &super::sql::no_partial_results(&st)).await?)?;
    let groups = parse_groups(&ch.query(&groups_sql(&fq, &keys, &days), &super::sql::no_partial_results(&st)).await?)?;
    Ok(findings(&t.signal, &t.table, &dups, &groups, h))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const D: u64 = DAY_NS;
    const HR: u64 = 3_600_000_000_000;

    fn g(day: u64, hour: u64, epoch: &str) -> Group {
        Group { day, producer: "p1".into(), epoch: epoch.into(), recv_ns: day * D + hour * HR, rows: 5 }
    }

    #[test]
    fn beyond_is_the_checks_reach_in_either_order() {
        let h = Some(3 * D);
        // whole-day horizon: the check of a copy on day 24 reads days 21..27
        assert!(!beyond(&g(21, 1, "a"), &g(24, 23, "b"), h), "day 21 is in reach of day 24 - 3 d");
        assert!(beyond(&g(20, 23, "a"), &g(24, 0, "b"), h), "day 20 is not, even 3 d 1 h apart");
        // a fractional horizon: reach depends on the time of day
        let h36 = Some(36 * HR);
        assert!(!beyond(&g(20, 23, "a"), &g(22, 6, "b"), h36), "22 06:00 - 36 h = 20 18:00: day 20 read");
        // the copy ingested first: the original's check reads up to toDate(20 01:00 + 36 h) = day 21
        assert!(beyond(&g(20, 1, "a"), &g(22, 13, "b"), h36));
        assert!(!beyond(&g(20, 1, "a"), &g(21, 13, "b"), h36));
        // no horizon (the workers read every partition): never beyond
        assert!(!beyond(&g(1, 0, "a"), &g(80, 0, "b"), None));
    }

    #[test]
    fn classify_orders_by_received_time() {
        let h = Some(3 * D);
        // stored out of order: a late copy (4 days later) and a same-partition second epoch
        let gs = vec![g(24, 12, "E3"), g(20, 12, "E1"), g(24, 13, "E4")];
        let c = classify(&gs, h);
        assert_eq!(c, vec![(0, true), (2, false)], "E3 is late (E1 out of reach); E4 is within reach of E3");
        // a copy within the horizon (which the check should have skipped): unexplained
        assert_eq!(classify(&[g(21, 12, "E1"), g(23, 12, "E2")], h), vec![(1, false)]);
        // one ingestion: nothing
        assert!(classify(&[g(21, 12, "E1")], h).is_empty());
    }

    #[test]
    fn findings_need_a_repeated_row() {
        let h = Some(3 * D);
        let mut groups = BTreeMap::new();
        let _ = groups.insert("dup".to_string(), vec![g(20, 12, "E1"), g(24, 12, "E2")]);
        let _ = groups.insert("spread".to_string(), vec![g(10, 12, "E1"), g(20, 12, "E1")]);
        let _ = groups.insert("near".to_string(), vec![g(22, 12, "E1"), g(23, 12, "E2")]);
        let dups: BTreeMap<String, u64> = [("dup".to_string(), 5), ("spread".to_string(), 0), ("near".to_string(), 5)].into();
        let f = findings("logs", "otel_logs", &dups, &groups, h);
        assert_eq!(f.len(), 2, "{f:?}");
        assert!(f[0].key == "dup" && f[0].late && f[0].copy.epoch == "E2" && f[0].original.epoch == "E1");
        assert!(f[1].key == "near" && !f[1].late);
        let line = f[0].log_line(Some(3 * 86_400_000));
        assert_eq!(
            line,
            "WARN horizon-audit: late copy in otel_logs: lane p1/logs epoch E2 received 1970-01-25T12:00:00Z, 96.0h after its original \
             (lane p1/logs epoch E1 received 1970-01-21T12:00:00Z); content_key dup, 5 rows, 5 rows duplicated; check horizon 72.0h"
        );
        assert_eq!(utc(1_790_000_000_000_000_000), "2026-09-21T14:13:20Z");
    }

    #[test]
    fn state_counts_each_copy_once_and_forgets_old_ones() {
        let mut st = AuditState::default();
        let f = Found { signal: "logs".into(), table: "otel_logs".into(), key: "k".into(), original: g(20, 1, "E1"), copy: g(24, 1, "E2"), late: true, dup_rows: 5 };
        assert_eq!(st.record(vec![f.clone()]).len(), 1);
        assert_eq!(st.record(vec![f.clone()]).len(), 0, "the next run finds it again: not counted twice");
        assert_eq!(st.late_total.get("logs/otel_logs"), Some(&1));
        let mut u = f.clone();
        u.late = false;
        u.copy = g(24, 2, "E3");
        assert_eq!(st.record(vec![u]).len(), 1);
        assert_eq!(st.unexplained_total.get("logs/otel_logs"), Some(&1));
        st.prune(25 * D);
        assert_eq!(st.reported["otel_logs"].len(), 2, "day 24 is still in reach of a run from day 25");
        st.prune(26 * D);
        assert!(st.reported.is_empty());
        // round trip (kept on S3 between runs)
        let s = serde_json::to_string(&st).unwrap();
        let back: AuditState = serde_json::from_str(&s).unwrap();
        assert_eq!(back.late_total, st.late_total);
    }

    #[test]
    fn statements_and_parsing() {
        let c = candidates_sql("db.otel_logs", 5 * D, 0, 100);
        assert!(c.contains("WHERE content_key IN (SELECT content_key FROM db.otel_logs WHERE _partition_value.1 >= toDate(fromUnixTimestamp64Nano(toInt64(432000000000000))))"), "{c}");
        assert!(c.contains("HAVING count() > 1 AND max(d) >= toDate(") && c.ends_with("LIMIT 100 FORMAT TSV"), "{c}");
        let s = candidates_sql("db.t", 0, 2, 10);
        assert_eq!(s.matches(" AND content_key >= '00' AND content_key < '01'").count(), 2, "{s}");
        let days: BTreeSet<u64> = [20, 24].into();
        let keys = vec!["a'b".to_string(), "c".to_string()];
        let q = groups_sql("db.t", &keys, &days);
        assert!(q.contains("content_key IN ('a\\'b', 'c') AND toUInt32(_partition_value.1) IN (20, 24)"), "{q}");
        assert!(dups_sql("db.t", &keys, &days).starts_with("SELECT content_key, count() - uniqExact(row_ordinal) FROM db.t WHERE "));
        assert_eq!(parse_candidates("k1\t[20,24]\nk\\t2\t[1,2,3]\n").unwrap(), vec![("k1".into(), vec![20, 24]), ("k\t2".into(), vec![1, 2, 3])]);
        assert_eq!(parse_dups("k1\t5\nk2\t0\n").unwrap(), [("k1".to_string(), 5), ("k2".to_string(), 0)].into());
        let gs = parse_groups("k1\t20\tp1\tE1\t1728000000000000000\t5\n").unwrap();
        assert_eq!(gs["k1"], vec![Group { day: 20, producer: "p1".into(), epoch: "E1".into(), recv_ns: 1_728_000_000_000_000_000, rows: 5 }]);
        assert!(parse_groups("k1\t20\n").is_err());
        let t = targets(&[]);
        assert!(t.iter().any(|t| t.table == "otel_logs") && t.iter().any(|t| t.table == "otel_traces"));
        assert!(!t.iter().any(|t| t.signal == "metrics_series"), "the series lane isn't counted");
        assert_eq!(targets(&["otel_logs".into()]).len(), 1);
    }

    // ---- against ClickHouse (and SeaweedFS), skipped when they aren't up ----

    fn ch_url() -> String {
        std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into())
    }

    async fn ch_up(ch: &ClickHouse) -> bool {
        ch.query("SELECT 1", &[]).await.is_ok()
    }

    fn run_id() -> String {
        format!("{:08x}", rand::random::<u32>())
    }

    /// A replicated central (the replicas of central-replicated/, skipped
    /// when they aren't up; `OTAPRS_REPLICAS=url1,url2`): r2 stops fetching,
    /// a late copy lands on r1. Without the sync r2's audit misses it (the
    /// gap this guards); with `sync_replica` r2's run fails (the sync times
    /// out) instead of reporting a clean table; once r2 fetches, both
    /// replicas report the same copy; and a dead first URL fails over.
    #[tokio::test(flavor = "current_thread")]
    async fn a_lagging_replica_is_synced_or_the_run_fails() {
        // CAST row 6: nightly `replicated` starts the replicas (ci/replicated.sh)
        let _trace = otap_s3pq::oscope_trace::covers("FI", &["CAST-6", "H-2"]);
        let urls = std::env::var("OTAPRS_REPLICAS").unwrap_or_else(|_| "http://127.0.0.1:28123,http://127.0.0.1:38123".into());
        let reps: Vec<ClickHouse> = urls.split(',').map(ClickHouse::new).collect();
        if reps.len() < 2 || !ch_up(&reps[0]).await || !ch_up(&reps[1]).await {
            return otap_s3pq::testgate::skip("clickhouse-replicated", "no replicated ClickHouse");
        }
        let (r1, r2) = (&reps[0], &reps[1]);
        let db = format!("repl_audit_it_{}", run_id());
        let fq = format!("{db}.otel_logs");
        let k = LaneKind::for_signal("logs").unwrap();
        let ddl = k
            .create_table(&fq)
            .replacen("ENGINE = MergeTree", &format!("ENGINE = ReplicatedMergeTree('/clickhouse/tables/audit_it/{db}/otel_logs', '{{replica}}')"), 1)
            .replace("SETTINGS non_replicated_deduplication_window = 1000, ", "SETTINGS ")
            .replace("SETTINGS non_replicated_deduplication_window = 1000", "");
        for r in [r1, r2] {
            r.query(&format!("CREATE DATABASE {db}"), &[]).await.unwrap();
            r.query(&ddl, &[]).await.unwrap();
        }
        let now = super::super::wall_ms() * 1_000_000;
        let today = now / D;
        let put = |key: &str, epoch: &str, days_ago: u64| {
            let recv = (today - days_ago) * D + HR;
            format!(
                "INSERT INTO {fq} (Timestamp, ServiceName, Body, received_at, row_ordinal, producer_id, producer_epoch, content_key) \
                 SELECT now64(9), 'svc', 'b', fromUnixTimestamp64Nano(toInt64({recv})), number, 'p1', '{epoch}', '{key}' FROM numbers(6)"
            )
        };
        r1.query(&put("late", "E1", 5), &[]).await.unwrap();
        r2.query(&format!("SYSTEM SYNC REPLICA {fq} LIGHTWEIGHT"), &[]).await.unwrap();
        r2.query(&format!("SYSTEM STOP FETCHES {fq}"), &[]).await.unwrap();
        r1.query(&put("late", "E2", 0), &[]).await.unwrap();
        let plain = AuditConfig { lookback_ms: 2 * 86_400_000, ..AuditConfig::default() };
        let synced = AuditConfig { sync_replica: true, sync_timeout_ms: 2000, ..plain.clone() };
        // r2 hasn't fetched the copy: unsynced, it reports a clean table.
        let rep = run(r2, &db, &plain, &mut AuditState::default(), now).await;
        assert!(rep.errors.is_empty() && rep.late.is_empty(), "{rep:?}");
        // Synced, the run fails rather than say so.
        let mut st = AuditState::default();
        let rep = run(r2, &db, &synced, &mut st, now).await;
        assert!(rep.late.is_empty() && rep.errors.len() == 1 && rep.errors[0].contains("sync replica"), "{rep:?}");
        assert!(st.late_total.values().all(|n| *n == 0), "nothing counted by a failed run");
        r2.query(&format!("SYSTEM START FETCHES {fq}"), &[]).await.unwrap();
        let rep = run(r2, &db, &synced, &mut st, now).await;
        assert!(rep.errors.is_empty() && rep.late.len() == 1 && rep.late[0].copy.epoch == "E2", "{rep:?}");
        let rep1 = run(r1, &db, &synced, &mut AuditState::default(), now).await;
        assert_eq!(rep1.late.iter().map(|f| (&f.key, &f.copy.epoch, f.dup_rows)).collect::<Vec<_>>(), rep.late.iter().map(|f| (&f.key, &f.copy.epoch, f.dup_rows)).collect::<Vec<_>>());
        // A dead first URL: the run reads the next replica, and sticks to it.
        let dead_then_r2 = vec![ClickHouse::new("http://127.0.0.1:9"), ClickHouse::new(&r2.url)];
        let mut cur = 0;
        let rep = run_replicas(&dead_then_r2, &mut cur, &db, &synced, &mut AuditState::default(), now).await;
        assert!(rep.errors.is_empty() && rep.late.len() == 1 && rep.replica == r2.url && cur == 1, "{rep:?}");
        let none = vec![ClickHouse::new("http://127.0.0.1:9")];
        let rep = run_replicas(&none, &mut 0, &db, &synced, &mut AuditState::default(), now).await;
        assert!(rep.errors.len() == 1 && rep.errors[0].starts_with("no replica answers"), "{rep:?}");
        for r in [r1, r2] {
            r.query(&format!("DROP DATABASE {db} SYNC"), &[]).await.unwrap();
        }
    }

    /// Rows planted the way ingestions leave them: a copy 5 days after its
    /// original (late), one 2 days after (within the 3-day horizon: a
    /// duplicate the check should have prevented), an object spread over
    /// two days without a repeated row, a pair older than the lookback, and
    /// a single ingestion. Only the first is a late copy; the second is
    /// unexplained; a second run reports nothing new.
    #[tokio::test(flavor = "current_thread")]
    async fn planted_copies_on_clickhouse() {
        let ch = ClickHouse::new(&ch_url());
        if !ch_up(&ch).await {
            return otap_s3pq::testgate::skip("clickhouse", "no ClickHouse");
        }
        let db = format!("audit_it_{}", run_id());
        let k = LaneKind::for_signal("logs").unwrap();
        ch.query(&format!("CREATE DATABASE {db}"), &[]).await.unwrap();
        ch.query(&k.create_table(&format!("{db}.otel_logs")), &[]).await.unwrap();
        let now = super::super::wall_ms() * 1_000_000;
        let today = now / D;
        let put = |key: &str, epoch: &str, days_ago: u64, ords: std::ops::Range<u32>| {
            let recv = (today - days_ago) * D + 12 * HR;
            format!(
                "INSERT INTO {db}.otel_logs (Timestamp, ServiceName, Body, received_at, row_ordinal, producer_id, producer_epoch, content_key) \
                 SELECT now64(9), 'svc', 'b', fromUnixTimestamp64Nano(toInt64({recv})), number, 'p1', '{epoch}', '{key}' FROM numbers({}, {})",
                ords.start,
                ords.end - ords.start
            )
        };
        for q in [
            put("late", "E1", 5, 0..10),
            put("late", "E2", 0, 0..10),
            put("near", "E1", 2, 0..10),
            put("near", "E2", 0, 0..10),
            put("spread", "E1", 10, 0..5),
            put("spread", "E1", 0, 5..10),
            put("old", "E1", 20, 0..10),
            put("old", "E2", 15, 0..10),
            put("single", "E1", 0, 0..10),
        ] {
            ch.query(&q, &[]).await.unwrap();
        }
        let cfg = AuditConfig { lookback_ms: 2 * 86_400_000, ..AuditConfig::default() };
        assert_eq!(cfg.horizon_ms, Some(3 * 86_400_000), "the default horizon");
        let mut st = AuditState::default();
        let rep = run(&ch, &db, &cfg, &mut st, now).await;
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert_eq!(rep.tables, 1);
        assert_eq!(rep.late.len(), 1, "{rep:?}");
        let f = &rep.late[0];
        assert_eq!((f.key.as_str(), f.copy.epoch.as_str(), f.original.epoch.as_str(), f.dup_rows), ("late", "E2", "E1", 10));
        assert_eq!(f.copy.day - f.original.day, 5);
        assert_eq!(rep.unexplained.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(), vec!["near"]);
        assert_eq!(rep.candidates, 3, "late, near and spread are in two partitions; old is before the lookback");
        assert_eq!(st.late_total.get("logs/otel_logs"), Some(&1));
        let rep2 = run(&ch, &db, &cfg, &mut st, now).await;
        assert!(rep2.late.is_empty() && rep2.unexplained.is_empty() && rep2.errors.is_empty(), "{rep2:?}");
        assert_eq!(st.late_total.get("logs/otel_logs"), Some(&1), "counted once");
        // The metrics say so.
        let mut p = super::super::metrics::Prom::default();
        super::super::metrics::audit_families(&mut p, &st, &Default::default(), cfg.horizon_ms);
        let t = p.render();
        assert!(t.contains("consumer_late_copies_total{signal=\"logs\",table=\"otel_logs\"} 1\n"), "{t}");
        assert!(t.contains("consumer_audit_unexplained_copies_total{signal=\"logs\",table=\"otel_logs\"} 1\n"), "{t}");
        // A sampled audit reads only keys starting with '0': none of these.
        let mut st2 = AuditState::default();
        let rep3 = run(&ch, &db, &AuditConfig { sample_hex: 1, ..cfg.clone() }, &mut st2, now).await;
        assert!(rep3.late.is_empty() && rep3.candidates == 0 && rep3.errors.is_empty(), "{rep3:?}");
        assert_eq!(st2.late_total.get("logs/otel_logs"), Some(&0), "an audited table's counter exists at 0");
        // Both steps are answered as designed: the candidates by the projection.
        let ex = ch.query(&format!("EXPLAIN projections = 1 {}", candidates_sql(&format!("{db}.otel_logs"), now - 2 * D, 0, 10).replace(" FORMAT TSV", "")), &[]).await.unwrap();
        assert_eq!(ex.matches("ReadFromMergeTree (by_content)").count(), 1, "{ex}");
        assert!(!ex.contains("ReadFromMergeTree (default)") && !ex.contains(&format!("ReadFromMergeTree ({db}")), "{ex}");
        // An unreachable table is an error in the report, not a panic.
        let rep4 = run(&ClickHouse::new("http://127.0.0.1:1"), &db, &cfg, &mut AuditState::default(), now).await;
        assert_eq!(rep4.errors.len(), 1);
        ch.query(&format!("DROP DATABASE {db} SYNC"), &[]).await.unwrap();
    }

    #[test]
    fn duplicate_statements() {
        assert_eq!(row_identity("otel_traces"), Some("TraceId, SpanId, Timestamp"));
        assert!(row_identity("otel_logs").unwrap().ends_with("Body.size"), "the body's length, never the body");
        assert_eq!(row_identity("otel_metrics_gauge_points"), Some("series_id, StartTimeUnix, TimeUnix"));
        assert!(row_identity("otel_metrics_sum").unwrap().starts_with("ServiceName, MetricName"));
        assert_eq!(row_identity("other"), None);
        for t in targets(&[]) {
            assert!(row_identity(&t.table).is_some(), "{} has an identity", t.table);
        }
        let q = dup_rows_sql("db.t", "a, b", 5 * D, 16);
        assert_eq!(
            q,
            "SELECT sum(k - 1) FROM (SELECT uniqExact(content_key) AS k FROM db.t WHERE _partition_value.1 >= toDate(fromUnixTimestamp64Nano(toInt64(432000000000000))) \
             AND cityHash64(a, b) % 16 = 0 GROUP BY cityHash64(a, b) HAVING k > 1) FORMAT TSV"
        );
        assert!(!dup_rows_sql("db.t", "a", 0, 1).contains('%'), "1: exact");
    }

    /// Copies under new content keys, planted the way a gateway SIGKILL
    /// leaves them (AMBIGUITY.md E2, `deploy/results/route-gwkill-batched.txt`):
    /// the agents' resent requests re-cut into new pieces, so half of one
    /// logs request and a third of a traces request come back inside
    /// requests with other keys. Not counted: rows repeated within one
    /// request, a late copy of a whole request (same key: counted as a late
    /// copy), and anything received before the lookback. Every counted table
    /// answers the statement (its identity columns exist).
    #[tokio::test(flavor = "current_thread")]
    async fn recut_copies_are_counted_by_content() {
        let ch = ClickHouse::new(&ch_url());
        if !ch_up(&ch).await {
            return otap_s3pq::testgate::skip("clickhouse", "no ClickHouse");
        }
        let db = format!("audit_dup_{}", run_id());
        ch.query(&format!("CREATE DATABASE {db}"), &[]).await.unwrap();
        for t in targets(&[]) {
            let k = LaneKind::for_signal(&t.signal).unwrap();
            ch.query(&k.create_table(&format!("{db}.{}", t.table)), &[]).await.unwrap();
        }
        let now = super::super::wall_ms() * 1_000_000;
        let today = now / D;
        let logs = |key: &str, epoch: &str, days_ago: u64, items: std::ops::Range<u64>, same_ts: bool| {
            let recv = (today - days_ago) * D + HR;
            let ts = if same_ts { "toDateTime64('2026-09-20 11:00:00', 9)".to_string() } else { "toDateTime64('2026-09-20 10:00:00', 9) + toIntervalMillisecond(number)".to_string() };
            format!(
                "INSERT INTO {db}.otel_logs (Timestamp, ServiceName, Body, received_at, row_ordinal, producer_id, producer_epoch, content_key) \
                 SELECT {ts}, 'svc', concat('line ', toString(number)), fromUnixTimestamp64Nano(toInt64({recv})), number - {}, 'p1', '{epoch}', '{key}' FROM numbers({}, {})",
                items.start,
                items.start,
                items.end - items.start
            )
        };
        let traces = |key: &str, items: std::ops::Range<u64>| {
            format!(
                "INSERT INTO {db}.otel_traces (Timestamp, TraceId, SpanId, ServiceName, SpanName, received_at, row_ordinal, producer_id, producer_epoch, content_key) \
                 SELECT toDateTime64('2026-09-20 10:00:00', 9) + toIntervalMillisecond(number), hex(intDiv(number, 10)), hex(number), 'svc', 'op', \
                 fromUnixTimestamp64Nano(toInt64({})), number - {}, 'p2', 'E1', '{key}' FROM numbers({}, {})",
                today * D + HR,
                items.start,
                items.start,
                items.end - items.start
            )
        };
        for q in [
            logs("orig", "E1", 0, 0..1000, false),
            logs("recut", "E2", 0, 500..1500, false),     // items 500..999 again: 500
            logs("late", "E1", 1, 2000..2100, false),
            logs("late", "E3", 0, 2000..2100, false),     // the same request again: a late copy, not this
            logs("repeat", "E1", 0, 0..10, true),         // 10 identical rows in one request
            logs("old", "E1", 9, 3000..3100, false),
            logs("old2", "E2", 9, 3000..3100, false),     // before the lookback
            traces("t1", 0..300),
            traces("t2", 100..400),                       // spans 100..299 again: 200
        ] {
            ch.query(&q, &[]).await.unwrap();
        }
        let exact = AuditConfig { lookback_ms: 2 * 86_400_000, dup_sample: 1, ..AuditConfig::default() };
        let mut st = AuditState::default();
        let rep = run(&ch, &db, &exact, &mut st, now).await;
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert_eq!(rep.duplicate_rows.get("logs/otel_logs"), Some(&500), "{:?}", rep.duplicate_rows);
        assert_eq!(rep.duplicate_rows.get("traces/otel_traces"), Some(&200), "{:?}", rep.duplicate_rows);
        assert_eq!(rep.duplicate_rows.len(), targets(&[]).len(), "every counted table, at 0 when clean: {:?}", rep.duplicate_rows);
        assert_eq!(rep.duplicate_rows.values().sum::<u64>(), 700);
        assert_eq!(st.duplicate_rows, rep.duplicate_rows);
        // Sampled (the default, 1/16 of the identities): an estimate, in steps of 16.
        let rep = run(&ch, &db, &AuditConfig { dup_sample: 16, ..exact.clone() }, &mut AuditState::default(), now).await;
        let (l, t) = (rep.duplicate_rows["logs/otel_logs"], rep.duplicate_rows["traces/otel_traces"]);
        assert!(l % 16 == 0 && (250..=1000).contains(&l) && (100..=400).contains(&t), "{:?}", rep.duplicate_rows);
        eprintln!("sampled 1/16: logs {l}, traces {t} (exact: 500, 200)");
        // Off: no statement, no gauge.
        let rep = run(&ch, &db, &AuditConfig { dup_sample: 0, ..exact.clone() }, &mut AuditState::default(), now).await;
        assert!(rep.duplicate_rows.is_empty() && rep.errors.is_empty(), "{rep:?}");
        // The metric.
        let mut p = super::super::metrics::Prom::default();
        super::super::metrics::audit_families(&mut p, &st, &Default::default(), exact.horizon_ms);
        let text = p.render();
        assert!(text.contains("consumer_audit_duplicate_rows{signal=\"logs\",table=\"otel_logs\"} 500\n"), "{text}");
        assert!(text.contains("consumer_audit_duplicate_rows{signal=\"traces\",table=\"otel_traces\"} 200\n"), "{text}");
        // What the logs statement reads: the identity columns, not the bodies.
        let fq = format!("{db}.otel_logs");
        let ex = ch.query(&format!("EXPLAIN actions = 1 {}", dup_rows_sql(&fq, row_identity("otel_logs").unwrap(), now - 2 * D, 16).replace(" FORMAT TSV", "")), &[]).await.unwrap();
        assert!(ex.contains("Body.size") && !ex.contains("Body String"), "{ex}");
        ch.query(&format!("DROP DATABASE {db} SYNC"), &[]).await.unwrap();
    }

    /// A minimal OTLP ExportLogsServiceRequest with `n` records (protobuf by hand).
    pub(crate) fn otlp_logs(n: usize, tag: &str) -> Vec<u8> {
        fn len(buf: &mut Vec<u8>, field: u8, body: &[u8]) {
            buf.push(field << 3 | 2);
            let mut l = body.len();
            loop {
                let b = (l & 0x7f) as u8;
                l >>= 7;
                if l == 0 {
                    buf.push(b);
                    break;
                }
                buf.push(b | 0x80);
            }
            buf.extend_from_slice(body);
        }
        let mut scope = Vec::new();
        for i in 0..n {
            let mut rec = Vec::new();
            rec.push(1 << 3 | 1); // time_unix_nano, fixed64
            rec.extend_from_slice(&(1_790_000_000_000_000_000u64 + i as u64).to_le_bytes());
            let mut body = Vec::new();
            len(&mut body, 1, format!("{tag} record {i}").as_bytes());
            len(&mut rec, 5, &body);
            len(&mut scope, 2, &rec);
        }
        let mut rl = Vec::new();
        len(&mut rl, 2, &scope);
        let mut req = Vec::new();
        len(&mut req, 1, &rl);
        req
    }

    /// End to end, the way it happens: an edge's objects on S3, the worker
    /// ingesting them with the default 3-day horizon, and the audit. Two
    /// requests were received 5 days and 2 days ago and ingested; each is
    /// then resent with new custody (a sender's resend after its own outage:
    /// a new epoch, a new received time; an edge buffer's replay keeps its
    /// received_at since 8efc34f and stays in the original's partition). The worker's check skips the copy within the
    /// horizon and ingests the other one again; the audit reports exactly
    /// that one, as late.
    #[tokio::test(flavor = "current_thread")]
    async fn a_copy_beyond_the_horizon_end_to_end() {
        use super::super::bucket::{Bucket, Cond, Put, S3Bucket};
        use super::super::sql::ClickHouseCentral;
        use super::super::worker::{Config, RealClock, Worker};
        use otap_s3pq::batch::{Encoder, Format, Input};
        use otap_s3pq::encode::ParquetOptions;
        use otap_s3pq::flatten::Envelope;
        use otap_s3pq::proto;
        use std::rc::Rc;
        let ch = ClickHouse::new(&ch_url());
        let s3 = std::env::var("OTAPRS_S3").unwrap_or_else(|_| "http://127.0.0.1:18333/audit-consumer".into());
        if !ch_up(&ch).await {
            return otap_s3pq::testgate::skip("clickhouse", "no ClickHouse");
        }
        let id = run_id();
        let db = format!("audit_e2e_{id}");
        let store = otap_s3pq::store::S3Config {
            url: format!("{s3}/it-{id}/edges"),
            access_key_id: Some("otel".into()),
            secret_access_key: Some("otelsecret".into()),
            ..Default::default()
        }
        .build()
        .unwrap();
        let root = store.prefix.clone();
        let bucket = Rc::new(S3Bucket::new(store));
        if let Err(e) = bucket.list(&root, None).await {
            return otap_s3pq::testgate::skip("s3", format!("no S3 at {s3}: {e}"));
        }
        // format v2: cluster/producer/signal (a lane without its cluster is never
        // discovered: this test found nothing to ingest while it was skipped)
        let lane = format!("{root}/c1/p1/logs");
        let now = super::super::wall_ms() * 1_000_000;
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let mut put = |epoch: String, seq: u64, req: Vec<u8>, recv: u64| {
            let f = enc.flatten(&Input::Otlp(Signal::Logs, &req)).unwrap();
            let o = enc.encode(&f, &Envelope { producer: "p1".into(), epoch: epoch.clone(), batch: seq, received_ns: recv }).unwrap();
            let mut meta = o.meta.clone();
            for (k, v) in [(proto::META_KIND, proto::KIND_DATA.to_string()), (proto::META_EPOCH, epoch.clone()), (proto::META_SEQ, seq.to_string()), (proto::META_CONTENT, f.content.clone())] {
                let _ = meta.insert(k.to_string(), v);
            }
            (proto::slot_key(&lane, &epoch, seq), o.body, meta, f.content)
        };
        let (far, near) = (otlp_logs(7, "far"), otlp_logs(5, "near"));
        let e1 = proto::new_epoch();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let e2 = proto::new_epoch();
        let originals = [put(e1.clone(), 0, far.clone(), now - 5 * D), put(e1.clone(), 1, near.clone(), now - 2 * D)];
        let copies = [put(e2.clone(), 0, far, now), put(e2.clone(), 1, near, now)];
        let (far_key, near_key) = (originals[0].3.clone(), originals[1].3.clone());
        assert_eq!((copies[0].3.as_str(), copies[1].3.as_str()), (far_key.as_str(), near_key.as_str()), "a resent request has the same content key");
        let central = Rc::new(ClickHouseCentral::new(&ch_url(), &db, bucket.clone(), "otel", "otelsecret", 20_000));
        let mut cfg = Config::new(&root, &format!("{root}/_consumer"), "w");
        cfg.solo = true;
        cfg.quiet_ms = 0;
        cfg.discover_ms = 0;
        cfg.lanes_every_ms = 0;
        cfg.poll_ms = 0;
        cfg.backoff = super::super::discovery::Backoff::off();
        assert_eq!(cfg.horizon_ms, Some(3 * 86_400_000));
        let mut w = Worker::new(cfg, bucket.clone(), central, RealClock);
        let count = |k: &str| {
            let q = format!("SELECT count() FROM {db}.otel_logs WHERE content_key = {}", sq(k));
            let ch = &ch;
            async move { ch.query(&q, &[]).await.unwrap().parse::<u64>().unwrap() }
        };
        for (objs, want) in [(&originals, (7, 5)), (&copies, (14, 5))] {
            for (key, body, meta, _) in objs.iter() {
                let r = bucket.put(key, body.clone(), Cond::Create, meta).await;
                assert!(matches!(r, Put::Ok(_)), "PUT {key}: {r:?}");
            }
            for _ in 0..40 {
                let _ = w.step().await;
                if w.checkpoint("p1/logs").is_some_and(|c| c.next(if want.0 == 7 { &e1 } else { &e2 }) == 2) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert_eq!((count(&far_key).await, count(&near_key).await), want, "{:?}", w.stats);
        }
        assert_eq!(w.stats.dedup_skipped, 1, "the copy within the horizon was skipped by the check");
        assert_eq!(w.stats.full_checks, 0, "every check was restricted to a range");
        w.release_all().await;
        let mut st = AuditState::default();
        let rep = run(&ch, &db, &AuditConfig::default(), &mut st, super::super::wall_ms() * 1_000_000).await;
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert_eq!(rep.late.len(), 1, "{rep:?}");
        assert!(rep.unexplained.is_empty(), "{rep:?}");
        let f = &rep.late[0];
        assert_eq!((f.key.as_str(), f.copy.epoch.as_str(), f.original.epoch.as_str(), f.copy.producer.as_str(), f.dup_rows), (far_key.as_str(), e2.as_str(), e1.as_str(), "p1", 7));
        assert_eq!(f.copy.recv_ns - f.original.recv_ns, 5 * D);
        eprintln!("{}", f.log_line(Some(3 * 86_400_000)));
        ch.query(&format!("DROP DATABASE {db} SYNC"), &[]).await.unwrap();
        let keys: Vec<String> = bucket.list(&root, None).await.unwrap().into_iter().map(|i| i.key).collect();
        let _ = bucket.delete(&keys).await;
    }
}
