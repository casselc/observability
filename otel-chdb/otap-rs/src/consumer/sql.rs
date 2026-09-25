//! Lane kinds and central: which table a signal's objects go to, and the
//! statements, behind a `Central` trait (ClickHouse over HTTP, or in memory
//! for tests).
//!
//! The insert is one statement for several committed objects:
//!
//! ```sql
//! INSERT INTO db.t (cols…, content_key)
//! SELECT cols…, transform(_path, ['bucket/k1', 'bucket/k2'], ['H1', 'H2'], '')
//! FROM s3('http://host/bucket/{k1,k2}', key, secret, 'Parquet', 'structure')
//! WHERE now64(3) <= fromUnixTimestamp64Milli(<fence>)
//! SETTINGS <single block>, max_execution_time = <budget>, insert_deduplication_token = <hash of the key list>
//! ```
//!
//! - `{k1,k2}` is expanded to exact keys, with no LIST (1 HEAD + 1 GET per
//!   object at the server) [M].
//! - Under the single-block settings (no squashing) each object becomes its
//!   own block, hence its own part per partition: an object is atomic, a
//!   statement is not. The verify step after it finds what is missing.
//! - The dedup token covers an exact retry of the same statement. Blocks'
//!   dedup ids are position-dependent (token + block index; without a token
//!   `INSERT … SELECT` isn't deduplicated at all in 26.10 unless the select
//!   is "stable") [M], so a regrouped retry is NOT deduplicated: the check
//!   against the projection is what makes ingest exactly-once.
//! - The fence makes a statement sent after its lease window a no-op, on
//!   the server's clock (`coord.rs`).

use super::bucket::Bucket;
use super::plan::Obj;
use async_trait::async_trait;
use otap_s3pq::central::{self, ClickHouse, ONE_BLOCK, sq};
use otap_s3pq::Signal;
use otap_s3pq::series::{self, SeriesOptions};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

/// How a signal's objects are ingested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneKind {
    pub signal: String,
    /// The table name (without database).
    pub table: String,
    /// Checked against central by content key (every lane but the series lane).
    pub counted: bool,
    pub structure: String,
    /// Target columns, and the matching select expressions.
    pub cols: String,
    pub select: String,
}

impl LaneKind {
    pub fn for_signal(signal: &str) -> Option<LaneKind> {
        let s = Signal::from_name(signal)?;
        if s.is_series_layout() {
            // Layout B: the structure and select list are the edge's own
            // (`series.rs`), parsed out of its one-object statement.
            let o = SeriesOptions::default();
            let structure = series::structure(s, &o);
            let sql = series::insert_select(s, &o, "DB", "SRC");
            let (head, sel) = sql.split_once(") SELECT ")?;
            let cols = head.split_once(" (")?.1.to_string();
            let select = sel.strip_suffix(" FROM SRC")?.to_string();
            return Some(LaneKind {
                signal: signal.into(),
                table: s.table().into(),
                counted: s != Signal::MetricsSeries,
                structure,
                cols,
                select,
            });
        }
        let cols = central::cols(s);
        Some(LaneKind {
            signal: signal.into(),
            table: s.table().into(),
            counted: true,
            structure: central::structure(s),
            select: cols.clone(),
            cols,
        })
    }

    pub fn create_table(&self, fq: &str) -> String {
        let s = Signal::from_name(&self.signal).expect("a known signal");
        if s.is_series_layout() {
            return central::series_layout_create_table(fq, s.table(), self.counted)
                .unwrap_or_else(|| panic!("{} is not in sql/series_tables.sql", s.table()));
        }
        central::create_table(fq, s)
    }
}

/// The server-side fence of a statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fence {
    /// Rows are read only if the statement starts by this wall time (ms).
    pub wall_ms: u64,
    /// `max_execution_time`.
    pub budget_ms: u64,
}

#[async_trait(?Send)]
pub trait Central {
    async fn ensure(&self, k: &LaneKind) -> Result<(), String>;
    /// Rows per content key (the aggregating projection).
    async fn counts(&self, k: &LaneKind, contents: &[&str]) -> Result<HashMap<String, u64>, String>;
    /// One statement for these objects. An error is ambiguous: the caller
    /// verifies (the implementation first makes sure the statement is no
    /// longer running).
    async fn insert(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, token: &str) -> Result<(), String>;
    /// Inserts the rows of `obj` whose `row_ordinal` central doesn't hold.
    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, token: &str) -> Result<(), String>;
}

// ---- ClickHouse --------------------------------------------------------------------

pub struct ClickHouseCentral<B: Bucket> {
    pub ch: ClickHouse,
    /// A replicated central: every replica's URL, `ch` first (`--ch a,b`).
    /// The worker sticks to one; a transport error moves it to the next
    /// (central-replicated/README.md).
    pub replicas: Vec<ClickHouse>,
    pub cur: Cell<usize>,
    /// `--sync-replica`: before a check that doesn't follow this worker's
    /// own statement on the same replica, `SYSTEM SYNC REPLICA … LIGHTWEIGHT`,
    /// so the check sees every statement a previous lane holder (or this
    /// worker, before a switch) committed on another replica.
    pub sync_replica: bool,
    pub sync_timeout_ms: u64,
    /// After a switch, checks fail until this long has passed, so a
    /// statement still running on the old replica has ended (budget plus
    /// the Keeper operation timeout: a commit can outlive max_execution_time
    /// by one Keeper request).
    pub switch_hold_ms: u64,
    switched_at: Cell<u64>,
    /// Tables whose last operation on the current replica was our own statement.
    own_last: RefCell<std::collections::HashSet<String>>,
    /// `--no-ddl`: tables must exist (created by the operator's replicated DDL).
    pub no_ddl: bool,
    /// `--insert-setting k=v`: extra settings on every insert (e.g. insert_quorum).
    pub insert_settings: Vec<(String, String)>,
    pub syncs: Cell<u64>,
    pub sync_errors: Cell<u64>,
    pub switches: Cell<u64>,
    pub db: String,
    pub bucket: Rc<B>,
    pub s3_key: String,
    pub s3_secret: String,
    /// Statements sent (inserts + repairs), and checks.
    pub statements: Cell<u64>,
    pub checks: Cell<u64>,
    /// signal -> table name, instead of the lane kind's.
    pub table_override: RefCell<HashMap<String, String>>,
    /// Squash a statement's objects into one block (one part per partition).
    pub squash: bool,
}

impl<B: Bucket> ClickHouseCentral<B> {
    /// `url` may list several replicas, comma-separated.
    pub fn new(url: &str, db: &str, bucket: Rc<B>, key: &str, secret: &str, timeout_ms: u64) -> Self {
        let mk = |u: &str| {
            let mut ch = ClickHouse::new(u);
            ch.http = reqwest::Client::builder()
                .timeout(std::time::Duration::from_millis(timeout_ms))
                .build()
                .expect("http client");
            ch
        };
        let replicas: Vec<ClickHouse> = url.split(',').filter(|u| !u.is_empty()).map(mk).collect();
        Self {
            ch: mk(url.split(',').next().unwrap_or(url)),
            replicas,
            cur: Cell::new(0),
            sync_replica: false,
            sync_timeout_ms: 5000,
            switch_hold_ms: 0,
            switched_at: Cell::new(0),
            own_last: RefCell::new(Default::default()),
            no_ddl: false,
            insert_settings: Vec::new(),
            syncs: Cell::new(0),
            sync_errors: Cell::new(0),
            switches: Cell::new(0),
            db: db.into(),
            bucket,
            s3_key: key.into(),
            s3_secret: secret.into(),
            statements: Cell::new(0),
            checks: Cell::new(0),
            table_override: RefCell::new(HashMap::new()),
            squash: true,
        }
    }

    /// A query on the current replica. A transport error (no HTTP answer)
    /// moves the worker to the next replica.
    async fn q(&self, sql: &str, settings: &[(&str, &str)]) -> Result<String, String> {
        let i = self.cur.get();
        let ch = self.replicas.get(i).unwrap_or(&self.ch);
        let r = ch.query(sql, settings).await;
        if let Err(e) = &r {
            if self.replicas.len() > 1 && !e.starts_with("clickhouse ") && self.cur.get() == i {
                self.cur.set((i + 1) % self.replicas.len());
                self.switched_at.set(super::mono_ms());
                self.switches.set(self.switches.get() + 1);
                self.own_last.borrow_mut().clear();
                eprintln!("central: {} unreachable ({e}); switching to {}", ch.url, self.replicas[self.cur.get()].url);
            }
        }
        r
    }

    /// Makes the current replica hold everything committed on any replica
    /// before now (for a check that doesn't follow our own statement here).
    async fn sync(&self, fq: &str) -> Result<(), String> {
        if !self.sync_replica || self.own_last.borrow_mut().remove(fq) {
            return Ok(());
        }
        let since = super::mono_ms().saturating_sub(self.switched_at.get());
        if self.switches.get() > 0 && since < self.switch_hold_ms {
            return Err(format!("switched replica {since} ms ago; waiting out statements on the old one"));
        }
        self.syncs.set(self.syncs.get() + 1);
        let t = format!("{:.3}", self.sync_timeout_ms as f64 / 1000.0);
        self.q(&format!("SYSTEM SYNC REPLICA {fq} LIGHTWEIGHT"), &[("receive_timeout", &t)]).await.map(|_| ()).map_err(|e| {
            self.sync_errors.set(self.sync_errors.get() + 1);
            format!("sync replica: {e}")
        })
    }

    pub fn fq(&self, k: &LaneKind) -> String {
        match self.table_override.borrow().get(&k.signal) {
            Some(t) => format!("{}.{t}", self.db),
            None => format!("{}.{}", self.db, k.table),
        }
    }

    /// `s3(<url with {k1,k2}>, …)`.
    fn source(&self, k: &LaneKind, keys: &[&str]) -> String {
        let url = if keys.len() == 1 {
            self.bucket.object_url(keys[0])
        } else {
            let base = self.bucket.object_url("");
            format!("{}{{{}}}", base, keys.join(","))
        };
        format!("s3({}, {}, {}, 'Parquet', {})", sq(&url), sq(&self.s3_key), sq(&self.s3_secret), sq(&k.structure))
    }

    pub fn insert_sql(&self, k: &LaneKind, objs: &[&Obj], fence: Fence) -> String {
        let keys: Vec<&str> = objs.iter().map(|o| o.key.as_str()).collect();
        let src = self.source(k, &keys);
        let guard = format!("now64(3) <= fromUnixTimestamp64Milli(toInt64({}))", fence.wall_ms);
        if !k.counted {
            return format!("INSERT INTO {} ({}) SELECT {} FROM {src} WHERE {guard}", self.fq(k), k.cols, k.select);
        }
        let paths: Vec<String> = objs.iter().map(|o| sq(&self.bucket.path_of(&o.key))).collect();
        let contents: Vec<String> = objs.iter().map(|o| sq(&o.content)).collect();
        let ck = if objs.len() == 1 {
            contents[0].clone()
        } else {
            format!("transform(_path, [{}], [{}], '')", paths.join(", "), contents.join(", "))
        };
        format!("INSERT INTO {} ({}, content_key) SELECT {}, {ck} FROM {src} WHERE {guard}", self.fq(k), k.cols, k.select)
    }

    async fn run_insert(&self, fq: &str, sql: &str, fence: Fence, token: &str) -> Result<(), String> {
        self.statements.set(self.statements.get() + 1);
        let at = (self.cur.get(), self.switches.get());
        let qid = format!("otaprs-consumer-{token}-{:08x}", rand::random::<u32>());
        let budget_s = format!("{:.3}", fence.budget_ms as f64 / 1000.0);
        let mut st: Vec<(&str, &str)> = ONE_BLOCK.to_vec();
        if self.squash {
            // Squash the statement's objects into one block: one part per
            // partition, so the statement lands whole or not at all.
            for (k, v) in st.iter_mut() {
                match *k {
                    "min_insert_block_size_rows" => *v = "1048576",
                    "min_insert_block_size_bytes" => *v = "4294967296",
                    _ => {}
                }
            }
        }
        st.extend_from_slice(&[
            ("insert_deduplication_token", token),
            ("insert_deduplicate", "1"),
            ("deduplicate_insert", "enable"),
            ("deduplicate_insert_select", "force_enable"),
            ("max_execution_time", &budget_s),
            ("timeout_overflow_mode", "throw"),
            ("query_id", &qid),
        ]);
        for (k, v) in &self.insert_settings {
            st.push((k.as_str(), v.as_str()));
        }
        let r = match self.q(sql, &st).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Ambiguous: make sure it isn't still running before anyone verifies.
                // (After a switch this reaches the new replica, harmlessly; the
                // switch hold covers the old one.)
                let kill = format!("KILL QUERY WHERE query_id = {} SYNC", sq(&qid));
                let _ = self.q(&kill, &[]).await;
                Err(e)
            }
        };
        // The verify that follows reads the replica this statement ran on:
        // no sync needed, unless the worker switched meanwhile.
        if (self.cur.get(), self.switches.get()) == at {
            let _ = self.own_last.borrow_mut().insert(fq.to_string());
        }
        r
    }
}

#[async_trait(?Send)]
impl<B: Bucket> Central for ClickHouseCentral<B> {
    async fn ensure(&self, k: &LaneKind) -> Result<(), String> {
        if self.no_ddl {
            // A replicated central's tables are the operator's (ReplicatedMergeTree,
            // storage policy, TTL): creating the plain one here would silently
            // make an unreplicated table on this replica.
            let fq = self.fq(k);
            let (db, t) = fq.split_once('.').unwrap_or(("default", &fq));
            let e = self
                .q(&format!("SELECT engine FROM system.tables WHERE database = {} AND name = {}", sq(db), sq(t)), &[])
                .await?;
            return match e.trim() {
                "" => Err(format!("{fq} does not exist (--no-ddl: create it with the replicated DDL)")),
                _ => Ok(()),
            };
        }
        self.q(&format!("CREATE DATABASE IF NOT EXISTS {}", self.db), &[]).await?;
        self.q(&k.create_table(&self.fq(k)), &[]).await.map(|_| ())
    }

    async fn counts(&self, k: &LaneKind, contents: &[&str]) -> Result<HashMap<String, u64>, String> {
        let mut out = HashMap::new();
        if contents.is_empty() || !k.counted {
            return Ok(out);
        }
        let fq = self.fq(k);
        self.sync(&fq).await?;
        self.checks.set(self.checks.get() + 1);
        let list: Vec<String> = contents.iter().map(|c| sq(c)).collect();
        let r = self
            .q(
                &format!(
                    "SELECT content_key, count() FROM {} WHERE content_key IN ({}) GROUP BY content_key FORMAT TSV",
                    self.fq(k),
                    list.join(", ")
                ),
                &[("optimize_use_projections", "1")],
            )
            .await?;
        for line in r.lines().filter(|l| !l.is_empty()) {
            let (c, n) = line.split_once('\t').ok_or_else(|| format!("counts: bad line {line:?}"))?;
            let _ = out.insert(c.to_string(), n.parse().map_err(|e| format!("counts: {e}"))?);
        }
        Ok(out)
    }

    async fn insert(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, token: &str) -> Result<(), String> {
        let sql = self.insert_sql(k, objs, fence);
        self.run_insert(&self.fq(k), &sql, fence, token).await
    }

    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, token: &str) -> Result<(), String> {
        let src = self.source(k, &[&obj.key]);
        let sql = format!(
            "INSERT INTO {t} ({cols}, content_key) SELECT {sel}, {c} FROM {src} WHERE now64(3) <= fromUnixTimestamp64Milli(toInt64({f})) AND row_ordinal NOT IN (SELECT row_ordinal FROM {t} WHERE content_key = {c})",
            t = self.fq(k),
            cols = k.cols,
            sel = k.select,
            c = sq(&obj.content),
            f = fence.wall_ms
        );
        self.run_insert(&self.fq(k), &sql, fence, token).await
    }
}

// ---- in memory ---------------------------------------------------------------------

/// Central as a count per (table, content key), for tests of the worker.
/// An insert is evaluated against `wall` like the server-side fence.
#[derive(Default)]
pub struct MemCentral {
    pub rows: RefCell<BTreeMap<(String, String), u64>>,
    /// Inserts applied (per object), for "exactly once" checks.
    pub applied: RefCell<Vec<(String, String)>>,
    /// The server's wall clock (ms): a shared test clock (0: the real clock) plus a skew.
    pub clock: Rc<Cell<u64>>,
    pub skew_ms: Cell<u64>,
    /// Every n-th statement: only its first object applies, then an error.
    pub partial_every: Cell<u64>,
    /// Every n-th statement: applied, then an error (a lost answer).
    pub lost_answer_every: Cell<u64>,
    /// Statements refused by the fence.
    pub fenced: Cell<u64>,
    pub n: Cell<u64>,
}

impl MemCentral {
    fn now(&self) -> u64 {
        match self.clock.get() {
            0 => super::wall_ms(),
            w => w + self.skew_ms.get(),
        }
    }
    pub fn count(&self, table: &str, content: &str) -> u64 {
        self.rows.borrow().get(&(table.to_string(), content.to_string())).copied().unwrap_or(0)
    }
}

#[async_trait(?Send)]
impl Central for MemCentral {
    async fn ensure(&self, _k: &LaneKind) -> Result<(), String> {
        Ok(())
    }

    async fn counts(&self, k: &LaneKind, contents: &[&str]) -> Result<HashMap<String, u64>, String> {
        Ok(contents
            .iter()
            .filter_map(|c| {
                let n = self.count(&k.table, c);
                (n > 0).then(|| (c.to_string(), n))
            })
            .collect())
    }

    async fn insert(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, _token: &str) -> Result<(), String> {
        self.n.set(self.n.get() + 1);
        let n = self.n.get();
        if self.now() > fence.wall_ms {
            self.fenced.set(self.fenced.get() + 1);
            return Ok(()); // the WHERE selects nothing
        }
        let partial = self.partial_every.get() > 0 && n % self.partial_every.get() == 0;
        for (i, o) in objs.iter().enumerate() {
            if partial && i > 0 {
                return Err("injected: the statement died after its first part".into());
            }
            *self.rows.borrow_mut().entry((k.table.clone(), o.content.clone())).or_default() += o.rows;
            self.applied.borrow_mut().push((k.table.clone(), o.content.clone()));
        }
        if self.lost_answer_every.get() > 0 && n % self.lost_answer_every.get() == 0 {
            return Err("injected: the answer was lost".into());
        }
        Ok(())
    }

    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, _token: &str) -> Result<(), String> {
        if self.now() > fence.wall_ms {
            self.fenced.set(self.fenced.get() + 1);
            return Ok(());
        }
        let mut r = self.rows.borrow_mut();
        let e = r.entry((k.table.clone(), obj.content.clone())).or_default();
        *e = (*e).max(obj.rows);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::bucket::MemBucket;

    fn obj(k: &str, c: &str) -> Obj {
        Obj { lane: "l".into(), epoch: "E".into(), seq: 0, key: k.into(), size: 1, content: c.into(), rows: 5, received_ns: 0 }
    }

    #[test]
    fn statements() {
        let b = Rc::new(MemBucket::default());
        let c = ClickHouseCentral::new("http://x", "db", b, "k", "s", 1000);
        let tr = LaneKind::for_signal("traces").unwrap();
        let (a, z) = (obj("r/p/traces/E/1.parquet", "H1"), obj("r/p/traces/E/2.parquet", "H2"));
        let f = Fence { wall_ms: 1234, budget_ms: 3000 };
        let sql = c.insert_sql(&tr, &[&a, &z], f);
        assert!(sql.starts_with("INSERT INTO db.otel_traces ("), "{sql}");
        assert!(sql.contains("transform(_path, ['mem/r/p/traces/E/1.parquet', 'mem/r/p/traces/E/2.parquet'], ['H1', 'H2'], '')"), "{sql}");
        assert!(sql.contains("s3('mem://{r/p/traces/E/1.parquet,r/p/traces/E/2.parquet}'"), "{sql}");
        assert!(sql.ends_with("WHERE now64(3) <= fromUnixTimestamp64Milli(toInt64(1234))"), "{sql}");
        let one = c.insert_sql(&tr, &[&a], f);
        assert!(one.contains(", 'H1' FROM s3('mem://r/p/traces/E/1.parquet'"), "{one}");
        let se = LaneKind::for_signal("metrics_series").unwrap();
        assert!(!se.counted);
        let s = c.insert_sql(&se, &[&a], f);
        assert!(s.contains("mapFromArrays(") && !s.contains("content_key"), "{s}");
        assert!(se.create_table("db.s").contains("AggregatingMergeTree"));
        for sig in ["metrics_number_points", "metrics_gauge_points", "metrics_sum_points", "metrics_histogram_points",
                    "metrics_exponential_histogram_points", "metrics_summary_points"] {
            let k = LaneKind::for_signal(sig).unwrap();
            let ddl = k.create_table("db.t");
            assert!(k.counted && ddl.starts_with("CREATE TABLE IF NOT EXISTS db.t\n(") && ddl.contains("PROJECTION by_content"), "{ddl}");
            assert!(ddl.contains("content_key LowCardinality(String),\n    PROJECTION") && ddl.ends_with("index_granularity = 8192"), "{ddl}");
        }
        assert!(LaneKind::for_signal("metrics_gauge").unwrap().counted);
        assert!(LaneKind::for_signal("nope").is_none());
    }
}
