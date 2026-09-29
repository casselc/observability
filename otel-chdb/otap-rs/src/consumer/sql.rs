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
use super::plan::{CheckRange, DAY_NS, Obj};
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
    /// Traces and logs: objects may announce resources (`oscope-announce`),
    /// inserted into `otel_resources` before their rows.
    pub announce: bool,
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
                announce: false,
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
            announce: matches!(s, Signal::Traces | Signal::Logs),
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

    /// What runs after `create_table`, one statement each: for traces and
    /// logs ClickStack's key-value rollup table (`<fq>_kv_rollup_15m`) and
    /// the materialized view that fills it (`central::create_rollups`); for
    /// metrics nothing.
    pub fn create_rollups(&self, fq: &str) -> Vec<String> {
        let s = Signal::from_name(&self.signal).expect("a known signal");
        if s.is_series_layout() {
            return Vec::new();
        }
        central::create_rollups(fq, s)
    }
}

/// The object a `CREATE TABLE` / `CREATE MATERIALIZED VIEW … IF NOT EXISTS`
/// statement creates (fully qualified as written).
pub fn created_name(ddl: &str) -> Option<&str> {
    let rest = ddl.split_once(" IF NOT EXISTS ")?.1;
    rest.split(|c: char| c.is_whitespace() || c == '(').next().filter(|n| !n.is_empty())
}

/// Why a statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertErr {
    pub msg: String,
    /// Nothing of the statement can land after this answer: the server
    /// answered with an error raised before anything was written
    /// (`settles_at_once`). `false`: no answer (a timeout, a reset, a replica
    /// switch: it may still be running, or not have arrived yet), or an error
    /// that may come with a commit still resolving (TIMEOUT_EXCEEDED after a
    /// Keeper request hung: the part lands at the end of the server's retry
    /// loop, measured 19 s past max_execution_time). Such a statement may land
    /// until its lease version's `Held::settled_by`. A KILL doesn't change
    /// that: it can reach the server before the statement does.
    pub settled: bool,
    /// The server answered (with an error), settled or not.
    pub answered: bool,
    /// The partition-range assertion fired: an object's rows don't all carry
    /// the `received_at` of its metadata. Nothing of the statement was written
    /// (squashed: one block), or only whole other objects (not squashed).
    pub range: bool,
}

/// Error codes a server raises before an INSERT … SELECT writes anything
/// (parsing, analysis, access, admission): an answer with one of them means
/// nothing of the statement can land later. Every other error may follow a
/// commit that is still resolving: TIMEOUT_EXCEEDED (159) and
/// KEEPER_EXCEPTION (999) after a hung Keeper request, UNKNOWN_STATUS_OF_INSERT
/// (319), and TABLE_IS_READ_ONLY (242), which the commit's retry loop also
/// raises once the session has expired. The range assertion
/// (`FUNCTION_THROW_IF_VALUE_IS_NON_ZERO`, 395) fires while reading, before
/// the statement's single block is written.
pub const SETTLING_CODES: &[u32] = &[
    16,  // NO_SUCH_COLUMN_IN_TABLE
    36,  // BAD_ARGUMENTS
    43,  // ILLEGAL_TYPE_OF_ARGUMENT
    47,  // UNKNOWN_IDENTIFIER
    60,  // UNKNOWN_TABLE
    62,  // SYNTAX_ERROR
    81,  // UNKNOWN_DATABASE
    115, // UNKNOWN_SETTING
    202, // TOO_MANY_SIMULTANEOUS_QUERIES
    252, // TOO_MANY_PARTS (checked before the insert starts writing)
    395, // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO (the range assertion)
    497, // ACCESS_DENIED
    516, // AUTHENTICATION_FAILED
];

/// Limits whose `break` (or `any`) mode answers a query with part of its
/// result, HTTP 200 and no error: a server or user profile that sets one
/// (with `max_rows_to_read`, `max_execution_time`, `max_rows_to_group_by`,
/// `max_rows_in_set`, ...) would make a count check short and the worker
/// re-insert rows central already holds (AMBIGUITY.md, hazard H-2). Every
/// consumer query pins them to `throw`: a limit hit is an error, which the
/// worker already handles (the check fails and is retried).
pub const NO_PARTIAL_RESULTS: &[(&str, &str)] = &[
    ("timeout_overflow_mode", "throw"),
    ("timeout_overflow_mode_leaf", "throw"),
    ("read_overflow_mode", "throw"),
    ("read_overflow_mode_leaf", "throw"),
    ("result_overflow_mode", "throw"),
    ("group_by_overflow_mode", "throw"),
    ("set_overflow_mode", "throw"),
    ("join_overflow_mode", "throw"),
    ("sort_overflow_mode", "throw"),
    ("distinct_overflow_mode", "throw"),
    ("transfer_overflow_mode", "throw"),
];

/// `settings` with [`NO_PARTIAL_RESULTS`] pinned: any value given for one of
/// those names (an `--insert-setting`, say) is replaced by `throw`.
pub fn no_partial_results<'a>(settings: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut v: Vec<(&str, &str)> =
        settings.iter().filter(|(k, _)| !NO_PARTIAL_RESULTS.iter().any(|(p, _)| p == k)).copied().collect();
    v.extend_from_slice(NO_PARTIAL_RESULTS);
    v
}

/// The `Code: N` of a ClickHouse error answer.
pub fn error_code(msg: &str) -> Option<u32> {
    let i = msg.find("Code: ")? + 6;
    msg[i..].chars().take_while(char::is_ascii_digit).collect::<String>().parse().ok()
}

/// Whether a server's error answer to an insert means nothing of it can land later.
pub fn settles_at_once(msg: &str) -> bool {
    msg.contains(RANGE_GUARD) || msg.contains(FENCED) || error_code(msg).is_some_and(|c| SETTLING_CODES.contains(&c))
}

impl std::fmt::Display for InsertErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let note = match (self.settled, self.answered) {
            (true, _) => "",
            (false, true) => " (an error that may come with a commit still resolving: unsettled)",
            (false, false) => " (no answer: unsettled)",
        };
        write!(f, "{}{note}", self.msg)
    }
}

/// The marker of the range assertion's exception.
pub const RANGE_GUARD: &str = "OTAPRS_RANGE_GUARD";

/// The marker of an announcement statement that started after its fence.
/// An insert's fence is silent (`WHERE now64(3) <= fence` selects nothing,
/// and the verify's count finds the rows missing); an announcement has no
/// verify, so its fence is loud (`throwIf`): a server whose clock is past
/// the fence answers this error, settled (nothing written), and the lane
/// sits the round out. With a silent fence the worker took the empty
/// answer for the announcement and inserted the rows before it
/// (dst_consumer seed 1950: the server's clock 359 ms ahead of the
/// worker's; `sameLane`).
pub const FENCED: &str = "OTAPRS_FENCED";

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
    /// Rows per content key (the aggregating projection), in the partitions
    /// of `range` only (None: every partition; also what a table whose
    /// partition key isn't in `RANGE_PARTITION_KEYS` always gets).
    async fn counts(&self, k: &LaneKind, contents: &[&str], range: Option<CheckRange>) -> Result<HashMap<String, u64>, String>;
    /// One statement for these objects. With `guard`, every row of each
    /// object must carry the object's `received_at` (`received_ns`), or the
    /// statement fails (`InsertErr::range`) before writing that object.
    async fn insert(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, token: &str, guard: bool) -> Result<(), InsertErr>;
    /// Inserts the rows of `obj` whose `row_ordinal` central doesn't hold (anywhere).
    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, token: &str) -> Result<(), InsertErr>;
    /// Whether `counts` honours ranges for this lane kind's table (after `ensure`).
    fn ranged(&self, k: &LaneKind) -> bool;
    /// One statement inserting the resource announcements of these objects
    /// (`oscope-announce` > 0) into `otel_resources`. Idempotent: the table
    /// keeps each (resource, object) once.
    async fn announce(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, token: &str) -> Result<(), InsertErr>;
}

// ---- ClickHouse --------------------------------------------------------------------

/// Credentials in a statement's `s3()`.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Keys {
    pub key: String,
    pub secret: String,
    /// A temporary credential's session token (`AWS_SESSION_TOKEN`).
    pub token: Option<String>,
}

impl std::fmt::Debug for S3Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3Keys {{ key: {:?}, secret: [HIDDEN], token: {} }}", self.key, if self.token.is_some() { "[HIDDEN]" } else { "None" })
    }
}

/// How ClickHouse's `s3()` is authorised (`consume --ch-s3-auth`).
#[derive(Clone)]
pub enum S3Auth {
    /// Fixed keys in every statement (`--key`/`--secret`[/`--session-token`]).
    Keys(S3Keys),
    /// The consumer's own credential chain (environment, profile, IRSA, Pod
    /// Identity, IMDS, AssumeRole), resolved before every statement and
    /// passed with its session token (`--ch-s3-auth pass`, the default).
    Chain(object_store::aws::AwsCredentialProvider),
    /// No credentials in the statement: the server's own (`--ch-s3-auth
    /// server`: an `<s3>` endpoint entry, `use_environment_credentials`, the
    /// ClickHouse pod's IRSA or Pod Identity role; the ingest user needs
    /// `s3_allow_server_credentials_in_user_queries = 1`, D18).
    Server,
}

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
    /// How ClickHouse's `s3()` is given credentials ([`S3Auth`]).
    pub s3_auth: S3Auth,
    /// The credentials the next statement carries: resolved from `s3_auth`
    /// before each statement (`s3_creds`); None: none in the statement.
    s3_now: RefCell<Option<S3Keys>>,
    /// Statements sent (inserts + repairs), and checks.
    pub statements: Cell<u64>,
    pub checks: Cell<u64>,
    /// signal -> table name, instead of the lane kind's.
    pub table_override: RefCell<HashMap<String, String>>,
    /// Squash a statement's objects into one block (one part per partition).
    pub squash: bool,
    /// Tables (fq) whose partition key is `toDate(received_at)`: their checks
    /// may be restricted to a partition range (read at `ensure`).
    ranged_tables: RefCell<std::collections::HashSet<String>>,
    /// `--check-range off`: never restrict a check.
    pub use_ranges: bool,
    /// Tables (fq) with a `late_part` column (read at `ensure`): their
    /// inserts write each object's part (DECISIONS.md D34). A table without
    /// it (created before the migration) gets the same statements as before.
    late_tables: RefCell<std::collections::HashSet<String>>,
}

/// The partition keys the check's range understands, as `system.tables`
/// prints them with the whitespace taken out: the day of `received_at`
/// alone, or first in a tuple with the object-constant `late_part`
/// (DECISIONS.md D34). Both put every row of an object in one partition,
/// and in both `_partition_value.1` is that day, which is all `range_sql`
/// reads. Any other key (ClickStack's `toDate(Timestamp)`, a tuple in
/// another order, a finer day function, a column that isn't
/// object-constant) is checked over every partition: exact, only slower. A
/// key wrongly accepted here would make the check miss rows it should see
/// (a duplicate, or a lost verdict): hence an exact list, not a pattern
/// (tests/hegel_props.rs `prop_range_partition_key_is_exact`).
pub const RANGE_PARTITION_KEYS: &[&str] = &["toDate(received_at)", "(toDate(received_at),late_part)"];

/// A partition key the check's range understands.
pub fn range_partition_key(key: &str) -> bool {
    let k: String = key.chars().filter(|c| !c.is_whitespace()).collect();
    RANGE_PARTITION_KEYS.contains(&k.as_str())
}

/// The object-constant column that splits late parts into partitions of
/// their own (DECISIONS.md D34).
pub const LATE_PART_COLUMN: &str = "late_part";

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
            s3_auth: S3Auth::Keys(S3Keys { key: key.into(), secret: secret.into(), token: None }),
            s3_now: RefCell::new(Some(S3Keys { key: key.into(), secret: secret.into(), token: None })),
            statements: Cell::new(0),
            checks: Cell::new(0),
            table_override: RefCell::new(HashMap::new()),
            squash: true,
            ranged_tables: RefCell::new(Default::default()),
            use_ranges: true,
            late_tables: RefCell::new(Default::default()),
        }
    }

    /// Records whether this lane kind's table has the `late_part` column, as
    /// `ensure` learns it (tests, and tools that build statements offline).
    pub fn set_late_part(&self, k: &LaneKind, on: bool) {
        if on {
            let _ = self.late_tables.borrow_mut().insert(self.fq(k));
        } else {
            let _ = self.late_tables.borrow_mut().remove(&self.fq(k));
        }
    }

    /// Whether this lane kind's table has the `late_part` column (after `ensure`).
    pub fn writes_late_part(&self, k: &LaneKind) -> bool {
        k.counted && self.late_tables.borrow().contains(&self.fq(k))
    }

    /// The `late_part` select expression for these objects: a constant when
    /// they agree (always, from `plan::group`), else per object by `_path`.
    fn late_part_sql(&self, objs: &[&Obj]) -> String {
        match objs.first() {
            Some(f) if objs.iter().all(|o| o.late == f.late) => format!("toUInt8({})", f.late as u8),
            _ => {
                let paths: Vec<String> = objs.iter().map(|o| sq(&self.bucket.path_of(&o.key))).collect();
                let v: Vec<String> = objs.iter().map(|o| (o.late as u8).to_string()).collect();
                format!("transform(_path, [{}], [{}], 0)::UInt8", paths.join(", "), v.join(", "))
            }
        }
    }

    /// Reads the table's partition key (for `ranged`).
    async fn learn_partition_key(&self, fq: &str) -> Result<(), String> {
        let (db, t) = fq.split_once('.').unwrap_or(("default", fq));
        let key = self
            .q(&format!("SELECT partition_key FROM system.tables WHERE database = {} AND name = {}", sq(db), sq(t)), &[])
            .await?;
        if self.use_ranges && range_partition_key(&key) {
            let _ = self.ranged_tables.borrow_mut().insert(fq.to_string());
        } else {
            let _ = self.ranged_tables.borrow_mut().remove(fq);
        }
        let n = self
            .q(
                &format!(
                    "SELECT count() FROM system.columns WHERE database = {} AND table = {} AND name = {}",
                    sq(db),
                    sq(t),
                    sq(LATE_PART_COLUMN)
                ),
                &[],
            )
            .await?;
        if n.trim() == "1" {
            let _ = self.late_tables.borrow_mut().insert(fq.to_string());
        } else {
            let _ = self.late_tables.borrow_mut().remove(fq);
        }
        Ok(())
    }

    /// The count check's partition predicate (on `_partition_value`, which
    /// keeps the aggregating projection in use).
    pub fn range_sql(r: &CheckRange) -> String {
        format!(
            " AND _partition_value.1 BETWEEN toDate(fromUnixTimestamp64Nano(toInt64({}))) AND toDate(fromUnixTimestamp64Nano(toInt64({})))",
            r.lo_ns.min(i64::MAX as u64),
            r.hi_ns.min(i64::MAX as u64)
        )
    }

    /// The range assertion: every row carries its object's `received_at`
    /// (`throwIf` in the SELECT: with the single-block settings it fires
    /// before the object's block is written).
    fn guard_sql(&self, objs: &[&Obj]) -> String {
        let want = if objs.len() == 1 {
            format!("toInt64({})", objs[0].received_ns)
        } else {
            let paths: Vec<String> = objs.iter().map(|o| sq(&self.bucket.path_of(&o.key))).collect();
            let ns: Vec<String> = objs.iter().map(|o| format!("toInt64({})", o.received_ns)).collect();
            format!("transform(_path, [{}], [{}], toInt64(-1))", paths.join(", "), ns.join(", "))
        };
        let msg = sq(&format!("{RANGE_GUARD}: a row's received_at differs from its object's metadata"));
        format!(" AND NOT throwIf(toUnixTimestamp64Nano(received_at) != {want}, {msg})")
    }

    /// A query on the current replica. A transport error (no HTTP answer)
    /// moves the worker to the next replica.
    async fn q(&self, sql: &str, settings: &[(&str, &str)]) -> Result<String, String> {
        let i = self.cur.get();
        let ch = self.replicas.get(i).unwrap_or(&self.ch);
        let r = ch.query(sql, &no_partial_results(settings)).await;
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

    /// The largest Keeper session timeout among the replicas that answer
    /// (`system.zookeeper_connection`), or None if none does.
    pub async fn keeper_session_timeout_ms(&self) -> Option<u64> {
        let mut best = None;
        for r in &self.replicas {
            if let Ok(s) = r.query("SELECT max(session_timeout_ms) FROM system.zookeeper_connection", &[]).await {
                if let Ok(v) = s.trim().parse::<u64>() {
                    best = Some(best.map_or(v, |b: u64| b.max(v)));
                }
            }
        }
        best
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
        self.s3_fn(&url, &k.structure)
    }

    /// `s3(url, [key, secret, [token,]] 'Parquet', structure)` with the
    /// credentials `s3_creds` resolved for this statement.
    fn s3_fn(&self, url: &str, structure: &str) -> String {
        match &*self.s3_now.borrow() {
            Some(S3Keys { key, secret, token: Some(t) }) => {
                format!("s3({}, {}, {}, {}, 'Parquet', {})", sq(url), sq(key), sq(secret), sq(t), sq(structure))
            }
            Some(S3Keys { key, secret, token: None }) => format!("s3({}, {}, {}, 'Parquet', {})", sq(url), sq(key), sq(secret), sq(structure)),
            None => format!("s3({}, 'Parquet', {})", sq(url), sq(structure)),
        }
    }

    /// Resolves the credentials the next statement carries. With
    /// `S3Auth::Chain` this asks the consumer's credential provider, which
    /// caches a temporary credential and refreshes it 5 minutes before it
    /// expires (object_store's `TokenCache`, `creds.rs`), so a statement
    /// (bounded by `max_execution_time`, far below 5 minutes) starts with a
    /// credential that outlives it. If it does expire mid-statement anyway
    /// (a clock off, a provider handing out a short one), the server's S3
    /// read fails with 403 (`S3_ERROR`, 499): not a settling code, so the
    /// worker waits the statement out and checks, as for any unsettled
    /// error. A credential that can't be had is an error before anything
    /// is sent: settled, and the objects are checked and retried.
    pub async fn s3_creds(&self) -> Result<(), InsertErr> {
        let now = match &self.s3_auth {
            S3Auth::Keys(k) => Some(k.clone()),
            S3Auth::Server => None,
            S3Auth::Chain(p) => {
                let c = p.get_credential().await.map_err(|e| InsertErr {
                    msg: format!("s3 credentials for ClickHouse: {e}"),
                    settled: true,
                    answered: false,
                    range: false,
                })?;
                Some(S3Keys { key: c.key_id.clone(), secret: c.secret_key.clone(), token: c.token.clone().filter(|t| !t.is_empty()) })
            }
        };
        *self.s3_now.borrow_mut() = now;
        Ok(())
    }

    /// `msg` with the statement's secret and session token replaced by
    /// `[HIDDEN]`. ClickHouse masks them in its own logs and in most error
    /// messages, but a syntax error answers with the raw statement text
    /// from the failing position on (26.10, measured), and that answer is
    /// what the worker logs.
    pub fn redact(&self, msg: &str) -> String {
        let mut out = msg.to_string();
        if let Some(k) = &*self.s3_now.borrow() {
            for s in [Some(&k.secret), k.token.as_ref()].into_iter().flatten().filter(|s| s.len() >= 4) {
                // Raw, and as it appears inside a quoted literal.
                let quoted = sq(s);
                let inner = &quoted[1..quoted.len() - 1];
                for v in [s.as_str(), inner] {
                    out = out.replace(v, "[HIDDEN]");
                }
            }
        }
        out
    }

    /// `{db}.otel_resources`.
    pub fn resources_fq(&self) -> String {
        format!("{}.{}", self.db, central::RESOURCES_TABLE)
    }

    /// The announcement statement for these objects: their
    /// `resource_announce` rows, fenced like an insert.
    pub fn announce_sql(&self, k: &LaneKind, objs: &[&Obj], fence: Fence) -> String {
        let keys: Vec<&str> = objs.iter().map(|o| o.key.as_str()).collect();
        let url = if keys.len() == 1 { self.bucket.object_url(keys[0]) } else { format!("{}{{{}}}", self.bucket.object_url(""), keys.join(",")) };
        let src = self.s3_fn(&url, central::ANNOUNCE_STRUCTURE);
        let sig = Signal::from_name(&k.signal).unwrap_or(Signal::Traces);
        let msg = sq(&format!("{FENCED}: the announcement started after its fence"));
        format!(
            "{} AND NOT throwIf(now64(3) > fromUnixTimestamp64Milli(toInt64({})), {msg})",
            central::announce_insert(&self.resources_fq(), sig, &src),
            fence.wall_ms
        )
    }

    pub fn insert_sql(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, guard: bool) -> String {
        let keys: Vec<&str> = objs.iter().map(|o| o.key.as_str()).collect();
        let src = self.source(k, &keys);
        let guard_fence = format!("now64(3) <= fromUnixTimestamp64Milli(toInt64({}))", fence.wall_ms);
        if !k.counted {
            return format!("INSERT INTO {} ({}) SELECT {} FROM {src} WHERE {guard_fence}", self.fq(k), k.cols, k.select);
        }
        let paths: Vec<String> = objs.iter().map(|o| sq(&self.bucket.path_of(&o.key))).collect();
        let contents: Vec<String> = objs.iter().map(|o| sq(&o.content)).collect();
        let ck = if objs.len() == 1 {
            contents[0].clone()
        } else {
            format!("transform(_path, [{}], [{}], '')", paths.join(", "), contents.join(", "))
        };
        let assert = if guard && objs.iter().all(|o| o.received_ns > 0) { self.guard_sql(objs) } else { String::new() };
        let (lc, lv) = if self.writes_late_part(k) { (format!(", {LATE_PART_COLUMN}"), format!(", {}", self.late_part_sql(objs))) } else { Default::default() };
        format!("INSERT INTO {} ({}, content_key{lc}) SELECT {}, {ck}{lv} FROM {src} WHERE {fence_sql}{assert}", self.fq(k), k.cols, k.select, fence_sql = guard_fence)
    }

    async fn run_insert(&self, fq: &str, sql: &str, fence: Fence, token: &str) -> Result<(), InsertErr> {
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
        // Keyless s3() on the server's own credentials: 26.10 refuses it
        // (ACCESS_DENIED, 497) unless this is set (D18).
        const SERVER_CREDS: &str = "s3_allow_server_credentials_in_user_queries";
        if matches!(self.s3_auth, S3Auth::Server) && !self.insert_settings.iter().any(|(k, _)| k == SERVER_CREDS) {
            st.push((SERVER_CREDS, "1"));
        }
        let r = match self.q(sql, &st).await.map_err(|e| self.redact(&e)) {
            Ok(_) => Ok(()),
            // The server answered: over, unless the error may come with a
            // commit still resolving in Keeper (`settles_at_once`).
            Err(e) if e.starts_with("clickhouse ") => Err(InsertErr { range: e.contains(RANGE_GUARD), settled: settles_at_once(&e), answered: true, msg: e }),
            Err(e) => {
                // No answer: stop it early if it is running (saves work), but
                // don't trust that: the KILL can reach the server before the
                // statement does. The caller waits until `settled_by`.
                let kill = format!("KILL QUERY WHERE query_id = {} SYNC", sq(&qid));
                let _ = self.q(&kill, &[]).await;
                Err(InsertErr { msg: e, settled: false, answered: false, range: false })
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

// ---- the recovered tables (D35 (3), `consume admit`) ---------------------------------

/// A recovered table is its main table's name plus this.
pub const RECOVERED_SUFFIX: &str = "_recovered";
/// The columns a recovered table has beside its main table's.
pub const RECOVERED_COLUMNS: [&str; 3] =
    ["recovered_at DateTime64(3, 'UTC') DEFAULT now64(3)", "retired_lane LowCardinality(String) DEFAULT ''", "quarantine_ref String DEFAULT ''"];

/// Where `consume admit` puts a quarantined object's rows: a recovered
/// table, never the main one (`../../FORMAT.md` §3.1).
#[async_trait(?Send)]
pub trait Recover {
    /// The recovered table of this lane kind (`{main}_recovered`).
    fn recovered_table(&self, k: &LaneKind) -> String;
    /// Inserts the object's rows into the recovered table, with
    /// `retired_lane` and `quarantine_ref` (the slot's key), idempotent by
    /// content key: nothing when the table already holds its rows. Returns
    /// the rows the table holds for it after.
    async fn admit(&self, k: &LaneKind, obj: &Obj, lane: &str) -> Result<u64, String>;
}

impl<B: Bucket> ClickHouseCentral<B> {
    fn recovered_fq(&self, k: &LaneKind) -> String {
        format!("{}{RECOVERED_SUFFIX}", self.fq(k))
    }

    /// `CREATE TABLE {main}_recovered AS {main}` (structure, engine,
    /// partition key and projections; no materialized view reads it), plus
    /// `RECOVERED_COLUMNS`.
    pub async fn ensure_recovered(&self, k: &LaneKind) -> Result<(), String> {
        self.ensure(k).await?;
        let fq = self.recovered_fq(k);
        let _ = self.q(&format!("CREATE TABLE IF NOT EXISTS {fq} AS {}", self.fq(k)), &[]).await?;
        let adds: Vec<String> = RECOVERED_COLUMNS.iter().map(|c| format!("ADD COLUMN IF NOT EXISTS {c}")).collect();
        let _ = self.q(&format!("ALTER TABLE {fq} {}", adds.join(", ")), &[]).await?;
        Ok(())
    }

    async fn recovered_rows(&self, fq: &str, content: &str) -> Result<u64, String> {
        let n = self.q(&format!("SELECT count() FROM {fq} WHERE content_key = {}", sq(content)), &[]).await?;
        n.trim().parse().map_err(|e| format!("{fq} count: {e}: {n}"))
    }
}

#[async_trait(?Send)]
impl<B: Bucket> Recover for ClickHouseCentral<B> {
    fn recovered_table(&self, k: &LaneKind) -> String {
        self.recovered_fq(k)
    }

    async fn admit(&self, k: &LaneKind, obj: &Obj, lane: &str) -> Result<u64, String> {
        if !k.counted {
            return Err(format!("{}: not a counted table (series objects are never quarantined)", k.table));
        }
        self.ensure_recovered(k).await?;
        let fq = self.recovered_fq(k);
        let have = self.recovered_rows(&fq, &obj.content).await?;
        if have >= obj.rows && obj.rows > 0 {
            return Ok(have);
        }
        if have > 0 {
            return Err(format!(
                "{fq} holds {have} of {} rows of {}: a partial earlier admit; remove them (ALTER TABLE {fq} DELETE WHERE content_key = {}) and admit again",
                obj.rows,
                obj.content,
                sq(&obj.content)
            ));
        }
        self.s3_creds().await.map_err(|e| e.msg)?;
        let src = self.source(k, &[obj.key.as_str()]);
        let (lc, lv) = if self.writes_late_part(k) { (format!(", {LATE_PART_COLUMN}"), format!(", toUInt8({})", obj.late as u8)) } else { Default::default() };
        let sql = format!(
            "INSERT INTO {fq} ({}, content_key{lc}, recovered_at, retired_lane, quarantine_ref) SELECT {}, {}{lv}, now64(3), {}, {} FROM {src}",
            k.cols,
            k.select,
            sq(&obj.content),
            sq(lane),
            sq(&obj.key)
        );
        let token = format!("admit-{}", obj.content);
        let mut st: Vec<(&str, &str)> = ONE_BLOCK.to_vec();
        st.extend_from_slice(&[("insert_deduplication_token", token.as_str()), ("insert_deduplicate", "1"), ("deduplicate_insert_select", "force_enable")]);
        let _ = self.q(&sql, &st).await.map_err(|e| self.redact(&e))?;
        self.recovered_rows(&fq, &obj.content).await
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
            if e.trim().is_empty() {
                return Err(format!("{fq} does not exist (--no-ddl: create it with the replicated DDL)"));
            }
            // The rollup is HyperDX's, not the consumer's: a missing one is
            // said, not fatal (ingestion is exactly-once without it).
            for st in k.create_rollups(&fq) {
                let Some(name) = created_name(&st) else { continue };
                let (rdb, rt) = name.split_once('.').unwrap_or((db, name));
                let r = self.q(&format!("SELECT count() FROM system.tables WHERE database = {} AND name = {}", sq(rdb), sq(rt)), &[]).await?;
                if r.trim() == "0" {
                    eprintln!("consume: {name} does not exist (--no-ddl): {fq}'s key-value rollup is not maintained");
                }
            }
            return self.learn_partition_key(&fq).await;
        }
        let fq = self.fq(k);
        self.q(&format!("CREATE DATABASE IF NOT EXISTS {}", self.db), &[]).await?;
        self.q(&k.create_table(&fq), &[]).await?;
        // Then the rollup table and its view (traces, logs), each IF NOT
        // EXISTS: a table created by an earlier consumer gets them now, and
        // its rows from before are not in the rollup.
        for st in k.create_rollups(&fq) {
            self.q(&st, &[]).await?;
        }
        if k.announce {
            for st in central::resources_statements(&self.resources_fq()) {
                self.q(&st, &[]).await?;
            }
        }
        self.learn_partition_key(&fq).await
    }

    async fn announce(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, token: &str) -> Result<(), InsertErr> {
        self.s3_creds().await?;
        let sql = self.announce_sql(k, objs, fence);
        let fq = self.resources_fq();
        self.run_insert(&fq, &sql, fence, token).await
    }

    fn ranged(&self, k: &LaneKind) -> bool {
        self.ranged_tables.borrow().contains(&self.fq(k))
    }

    async fn counts(&self, k: &LaneKind, contents: &[&str], range: Option<CheckRange>) -> Result<HashMap<String, u64>, String> {
        let mut out = HashMap::new();
        if contents.is_empty() || !k.counted {
            return Ok(out);
        }
        let fq = self.fq(k);
        self.sync(&fq).await?;
        self.checks.set(self.checks.get() + 1);
        let list: Vec<String> = contents.iter().map(|c| sq(c)).collect();
        let pred = match range {
            Some(r) if self.ranged(k) => Self::range_sql(&r),
            _ => String::new(),
        };
        let r = self
            .q(
                &format!(
                    "SELECT content_key, count() FROM {} WHERE content_key IN ({}){pred} GROUP BY content_key FORMAT TSV",
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

    async fn insert(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, token: &str, guard: bool) -> Result<(), InsertErr> {
        self.s3_creds().await?;
        let sql = self.insert_sql(k, objs, fence, guard && self.ranged(k));
        self.run_insert(&self.fq(k), &sql, fence, token).await
    }

    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, token: &str) -> Result<(), InsertErr> {
        self.s3_creds().await?;
        let src = self.source(k, &[&obj.key]);
        let (lc, lv) = if self.writes_late_part(k) { (format!(", {LATE_PART_COLUMN}"), format!(", {}", self.late_part_sql(&[obj]))) } else { Default::default() };
        let sql = format!(
            "INSERT INTO {t} ({cols}, content_key{lc}) SELECT {sel}, {c}{lv} FROM {src} WHERE now64(3) <= fromUnixTimestamp64Milli(toInt64({f})) AND row_ordinal NOT IN (SELECT row_ordinal FROM {t} WHERE content_key = {c})",
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
/// An insert is evaluated against `wall` like the server-side fence. Rows
/// are also kept per day of `received_at` (the partition), so a check with
/// a range reads only those days.
#[derive(Default)]
pub struct MemCentral {
    pub rows: RefCell<BTreeMap<(String, String), u64>>,
    /// Rows per (table, content key, day of received_at).
    pub by_day: RefCell<BTreeMap<(String, String, u64), u64>>,
    /// What an object's rows actually carry as received_at (ns), by object
    /// key, when it isn't the object's metadata (default: `received_ns` for
    /// every row); several values spread the rows over them.
    pub true_recv: RefCell<HashMap<String, Vec<u64>>>,
    /// Tables whose checks honour ranges (default: all).
    pub unranged_tables: RefCell<std::collections::HashSet<String>>,
    /// Checks that read only a range, and every partition.
    pub range_checks: Cell<u64>,
    pub full_checks: Cell<u64>,
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
    /// Every n-th statement gets no answer and lands later: `late_by_ms`
    /// after it was sent, but never after its fence + budget + `slack_ms`
    /// (what a slow network, or a Keeper request past max_execution_time, does).
    pub late_every: Cell<u64>,
    /// Every n-th statement is answered at once with TIMEOUT_EXCEEDED, and
    /// lands later all the same (as late as `late_every`'s): a commit whose
    /// Keeper request hung, resolved after the server gave up on the time limit.
    pub late_error_every: Cell<u64>,
    pub late_by_ms: Cell<u64>,
    pub slack_ms: Cell<u64>,
    pub late: RefCell<Vec<(u64, String, Vec<Obj>)>>,
    pub landed_late: Cell<u64>,
    /// Announcement statements applied, per object key (the table keeps
    /// one row per announcement whatever this counts).
    pub announced: RefCell<BTreeMap<String, u64>>,
    /// Every n-th announcement statement fails before writing.
    pub announce_fail_every: Cell<u64>,
    pub announce_n: Cell<u64>,
    /// Insert statements that mixed late parts and bulk objects (DECISIONS.md
    /// D34: `plan::group` never builds one), and rows landed per
    /// (table, content key, late).
    pub mixed_statements: Cell<u64>,
    pub by_part: RefCell<BTreeMap<(String, String, bool), u64>>,
    /// The recovered tables (`consume admit`): (table, content key) -> rows,
    /// and the admits applied.
    pub recovered: RefCell<BTreeMap<(String, String), u64>>,
    pub admits: Cell<u64>,
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

    /// Lands the late statements due by now (every call does this first).
    pub fn flush_late(&self) {
        let now = self.now();
        let due: Vec<(u64, String, Vec<Obj>)> = {
            let mut l = self.late.borrow_mut();
            let (due, keep): (Vec<_>, Vec<_>) = l.drain(..).partition(|(at, _, _)| *at <= now);
            *l = keep;
            due
        };
        for (_, table, objs) in due {
            self.landed_late.set(self.landed_late.get() + 1);
            for o in &objs {
                self.add(&table, o, o.rows);
                self.applied.borrow_mut().push((table.clone(), o.content.clone()));
            }
        }
    }

    fn recv_of(&self, o: &Obj) -> Vec<u64> {
        self.true_recv.borrow().get(&o.key).cloned().unwrap_or_else(|| vec![o.received_ns])
    }

    fn add(&self, table: &str, o: &Obj, rows: u64) {
        let recv = self.recv_of(o);
        *self.rows.borrow_mut().entry((table.to_string(), o.content.clone())).or_default() += rows;
        for i in 0..rows {
            let d = recv[(i as usize) % recv.len()] / DAY_NS;
            *self.by_day.borrow_mut().entry((table.to_string(), o.content.clone(), d)).or_default() += 1;
        }
    }
}

#[async_trait(?Send)]
impl Recover for MemCentral {
    fn recovered_table(&self, k: &LaneKind) -> String {
        format!("{}{RECOVERED_SUFFIX}", k.table)
    }
    async fn admit(&self, k: &LaneKind, obj: &Obj, _lane: &str) -> Result<u64, String> {
        let key = (self.recovered_table(k), obj.content.clone());
        let mut r = self.recovered.borrow_mut();
        let have = r.entry(key).or_insert(0);
        if *have == 0 {
            *have = obj.rows;
            self.admits.set(self.admits.get() + 1);
        }
        Ok(*have)
    }
}

#[async_trait(?Send)]
impl Central for MemCentral {
    async fn ensure(&self, _k: &LaneKind) -> Result<(), String> {
        Ok(())
    }

    fn ranged(&self, k: &LaneKind) -> bool {
        !self.unranged_tables.borrow().contains(&k.table)
    }

    async fn counts(&self, k: &LaneKind, contents: &[&str], range: Option<CheckRange>) -> Result<HashMap<String, u64>, String> {
        self.flush_late();
        let range = range.filter(|_| self.ranged(k));
        match range {
            Some(_) => self.range_checks.set(self.range_checks.get() + 1),
            None => self.full_checks.set(self.full_checks.get() + 1),
        }
        let by_day = self.by_day.borrow();
        Ok(contents
            .iter()
            .filter_map(|c| {
                let n = match range {
                    None => self.count(&k.table, c),
                    Some(r) => by_day
                        .range((k.table.clone(), c.to_string(), r.lo_ns / DAY_NS)..=(k.table.clone(), c.to_string(), r.hi_ns / DAY_NS))
                        .map(|(_, n)| *n)
                        .sum(),
                };
                (n > 0).then(|| (c.to_string(), n))
            })
            .collect())
    }

    async fn insert(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, _token: &str, guard: bool) -> Result<(), InsertErr> {
        self.flush_late();
        if objs.iter().any(|o| objs.first().is_some_and(|f| o.late != f.late)) {
            self.mixed_statements.set(self.mixed_statements.get() + 1);
        }
        for o in objs {
            *self.by_part.borrow_mut().entry((k.table.clone(), o.content.clone(), o.late)).or_default() += o.rows;
        }
        self.n.set(self.n.get() + 1);
        let n = self.n.get();
        if self.late_error_every.get() > 0 && n % self.late_error_every.get() == 0 && self.now() <= fence.wall_ms {
            let at = self.now() + fence.budget_ms + self.slack_ms.get();
            self.late.borrow_mut().push((at, k.table.clone(), objs.iter().map(|o| (*o).clone()).collect()));
            let msg = "clickhouse 500 Internal Server Error: Code: 159. DB::Exception: Timeout exceeded: elapsed 28870.471 ms, maximum: 10000.000 ms. (TIMEOUT_EXCEEDED)".to_string();
            return Err(InsertErr { settled: settles_at_once(&msg), answered: true, range: false, msg });
        }
        if self.late_every.get() > 0 && n % self.late_every.get() == 0 {
            // No answer. It arrives late: if by its fence, it runs, and
            // lands as late as it can: its whole budget plus the slack after.
            let arrive = self.now() + self.late_by_ms.get();
            if arrive <= fence.wall_ms {
                let at = arrive + fence.budget_ms + self.slack_ms.get();
                self.late.borrow_mut().push((at, k.table.clone(), objs.iter().map(|o| (*o).clone()).collect()));
            }
            return Err(InsertErr { msg: "injected: no answer (the statement is late)".into(), settled: false, answered: false, range: false });
        }
        if self.now() > fence.wall_ms {
            self.fenced.set(self.fenced.get() + 1);
            return Ok(()); // the WHERE selects nothing
        }
        // The range assertion, as squashed: one bad object fails the statement before anything is written.
        if guard && self.ranged(k) && objs.iter().any(|o| o.received_ns > 0 && self.recv_of(o).iter().any(|r| *r != o.received_ns)) {
            return Err(InsertErr { msg: format!("clickhouse 500: {RANGE_GUARD}"), settled: true, answered: true, range: true });
        }
        let partial = self.partial_every.get() > 0 && n % self.partial_every.get() == 0;
        for (i, o) in objs.iter().enumerate() {
            if partial && i > 0 {
                return Err(InsertErr { msg: "injected: the statement died after its first part".into(), settled: true, answered: true, range: false });
            }
            self.add(&k.table, o, o.rows);
            self.applied.borrow_mut().push((k.table.clone(), o.content.clone()));
        }
        if self.lost_answer_every.get() > 0 && n % self.lost_answer_every.get() == 0 {
            return Err(InsertErr { msg: "injected: the answer was lost".into(), settled: false, answered: false, range: false });
        }
        Ok(())
    }

    async fn announce(&self, _k: &LaneKind, objs: &[&Obj], fence: Fence, _token: &str) -> Result<(), InsertErr> {
        self.announce_n.set(self.announce_n.get() + 1);
        if self.announce_fail_every.get() > 0 && self.announce_n.get() % self.announce_fail_every.get() == 0 {
            return Err(InsertErr { msg: "injected: announcement refused".into(), settled: true, answered: true, range: false });
        }
        if self.now() > fence.wall_ms {
            // the loud fence (`FENCED`): an error, nothing written
            self.fenced.set(self.fenced.get() + 1);
            return Err(InsertErr { msg: format!("clickhouse 500: {FENCED}"), settled: true, answered: true, range: false });
        }
        for o in objs {
            *self.announced.borrow_mut().entry(o.key.clone()).or_default() += 1;
        }
        Ok(())
    }

    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, _token: &str) -> Result<(), InsertErr> {
        self.flush_late();
        if self.now() > fence.wall_ms {
            self.fenced.set(self.fenced.get() + 1);
            return Ok(());
        }
        let have = self.count(&k.table, &obj.content);
        if have < obj.rows {
            self.add(&k.table, obj, obj.rows - have);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::bucket::MemBucket;

    fn obj(k: &str, c: &str) -> Obj {
        Obj { lane: "l".into(), epoch: "E".into(), seq: 0, key: k.into(), size: 1, content: c.into(), rows: 5, received_ns: 0, seen_ms: 0, announce: 0, late: false }
    }

    /// Whether every single-quoted literal closes (ClickHouse: `\\` and `\'` escape).
    fn quotes_balance(sql: &str) -> bool {
        let (mut open, mut esc) = (false, false);
        for ch in sql.chars() {
            match (open, esc, ch) {
                (true, true, _) => esc = false,
                (true, false, '\\') => esc = true,
                (_, _, '\'') => open = !open,
                _ => {}
            }
        }
        !open
    }

    #[test]
    fn every_query_pins_the_overflow_modes_to_throw() {
        let _trace = otap_s3pq::oscope_trace::covers("PH", &["CAST-16", "H-2"]);
        let st = no_partial_results(&[("read_overflow_mode", "break"), ("max_rows_to_read", "10"), ("optimize_use_projections", "1")]);
        assert!(st.contains(&("read_overflow_mode", "throw")) && !st.contains(&("read_overflow_mode", "break")), "{st:?}");
        assert!(st.contains(&("max_rows_to_read", "10")) && st.contains(&("optimize_use_projections", "1")));
        for (k, _) in NO_PARTIAL_RESULTS {
            assert_eq!(st.iter().filter(|(x, _)| x == k).count(), 1, "{k}");
        }
    }

    /// Hazard H-2 on a real server: a user profile with `read_overflow_mode =
    /// 'break'` and `max_rows_to_read`, and a table whose projection was added
    /// after its parts were written (so the check reads the table). Unpinned,
    /// the consumer's count query answers HTTP 200 with short counts; pinned,
    /// it fails. Skipped without a ClickHouse that allows CREATE USER.
    #[tokio::test(flavor = "current_thread")]
    async fn a_profile_with_break_modes_cannot_shorten_the_count_check() {
        let _trace = otap_s3pq::oscope_trace::covers("IT", &["CAST-16", "H-2"]);
        let url = std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
        let admin = otap_s3pq::central::ClickHouse::new(&url);
        if admin.query("SELECT 1", &[]).await.is_err() {
            return otap_s3pq::testgate::skip("clickhouse", format!("no ClickHouse at {url}"));
        }
        let id = format!("{:08x}", rand::random::<u32>());
        let (db, user, prof) = (format!("am_h2_{id}"), format!("am_h2_u_{id}"), format!("am_h2_p_{id}"));
        let setup = [
            format!("CREATE SETTINGS PROFILE {prof} SETTINGS read_overflow_mode = 'break', max_rows_to_read = 100000"),
            format!("CREATE USER {user} IDENTIFIED WITH plaintext_password BY 'pw' SETTINGS PROFILE {prof}"),
            format!("CREATE DATABASE {db}"),
            format!("GRANT ALL ON {db}.* TO {user}"),
        ];
        for q in &setup {
            if let Err(e) = admin.query(q, &[]).await {
                let _ = admin.query(&format!("DROP USER IF EXISTS {user}"), &[]).await;
                let _ = admin.query(&format!("DROP SETTINGS PROFILE IF EXISTS {prof}"), &[]).await;
                return otap_s3pq::testgate::skip("clickhouse", format!("a ClickHouse that allows CREATE USER: {q}: {e}"));
            }
        }
        let t = format!("{db}.otel_logs");
        for q in [
            format!("CREATE TABLE {t} (received_at DateTime64(9), content_key LowCardinality(String), row_ordinal UInt32) ENGINE MergeTree PARTITION BY toDate(received_at) ORDER BY row_ordinal"),
            format!("INSERT INTO {t} SELECT now64(9), concat('k', toString(number % 50)), number FROM numbers(400000)"),
            format!("ALTER TABLE {t} ADD PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)"),
        ] {
            admin.query(&q, &[]).await.unwrap();
        }
        let count = format!("SELECT content_key, count() FROM {t} WHERE content_key IN ('k1', 'k2') GROUP BY content_key ORDER BY 1 FORMAT TSV");
        let mut raw = otap_s3pq::central::ClickHouse::new(&url);
        raw.user = Some((user.clone(), "pw".into()));
        let short = raw.query(&count, &[("optimize_use_projections", "1")]).await;
        let mut c = ClickHouseCentral::new(&url, &db, Rc::new(MemBucket::default()), "k", "s", 10_000);
        for r in c.replicas.iter_mut().chain(std::iter::once(&mut c.ch)) {
            r.user = Some((user.clone(), "pw".into()));
        }
        let lk = LaneKind::for_signal("logs").unwrap();
        let pinned = c.counts(&lk, &["k1", "k2"], None).await;
        for q in [format!("DROP DATABASE {db} SYNC"), format!("DROP USER {user}"), format!("DROP SETTINGS PROFILE {prof}")] {
            let _ = admin.query(&q, &[]).await;
        }
        eprintln!("unpinned: {short:?}\npinned: {pinned:?}");
        let short = short.expect("unpinned: HTTP 200");
        let n: Vec<u64> = short.lines().filter_map(|l| l.split_once('\t')?.1.parse().ok()).collect();
        assert!(!n.is_empty() && n.iter().all(|&x| x < 8000), "the hazard: 200 with short counts (8,000 each): {short:?}");
        let e = pinned.expect_err("pinned: the limit is an error, not a short count");
        assert!(e.contains("Code: 158"), "{e}");
    }

    /// The partition parse on a real server (DECISIONS.md D34): under both
    /// keys the check's `_partition_value.1` range reads exactly the rows
    /// whose `received_at` day `CheckRange::covers`, late or not, for rows
    /// on and around day boundaries and random ranges; under a key whose
    /// first element is a day but not `received_at`'s (refused by
    /// `range_partition_key`) the same predicate silently reads the wrong
    /// partitions: the hazard the refusal avoids (a check that misses rows
    /// re-inserts them: a duplicate).
    #[tokio::test(flavor = "current_thread")]
    async fn the_range_reads_the_first_element_under_both_keys() {
        let url = std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
        let ch = otap_s3pq::central::ClickHouse::new(&url);
        match ch.query("SELECT timezone()", &[]).await {
            Ok(tz) if matches!(tz.trim(), "UTC" | "Etc/UTC") => {}
            r => return otap_s3pq::testgate::skip("clickhouse", format!("no ClickHouse at {url}, or not on UTC ({r:?})")),
        }
        let db = format!("pk_range_{:08x}", rand::random::<u32>());
        ch.query(&format!("CREATE DATABASE {db}"), &[]).await.unwrap();
        let day0 = 20_000 * DAY_NS;
        // (received ns, late): around midnights, with a late row each
        let mut rows: Vec<(u64, u8)> = Vec::new();
        for d in 0..6u64 {
            for off in [0, 1, DAY_NS / 2, DAY_NS - 1] {
                rows.push((day0 + d * DAY_NS + off, (off % 2) as u8));
                rows.push((day0 + d * DAY_NS + off, 1 - (off % 2) as u8));
            }
        }
        let values: Vec<String> = rows.iter().enumerate().map(|(i, (ns, l))| format!("(fromUnixTimestamp64Nano(toInt64({ns})), {l}, 'k', {i})")).collect();
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for (t, key, ok) in [("a", "toDate(received_at)", true), ("b", "(toDate(received_at), late_part)", true), ("c", "(toDate(addHours(received_at, 12)), late_part)", false)] {
            let fq = format!("{db}.{t}");
            ch.query(&format!("CREATE TABLE {fq} (received_at DateTime64(9), late_part UInt8, content_key String, row_ordinal UInt32, PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)) ENGINE MergeTree PARTITION BY {key} ORDER BY row_ordinal"), &[]).await.unwrap();
            ch.query(&format!("INSERT INTO {fq} VALUES {}", values.join(", ")), &[]).await.unwrap();
            let pk = ch.query(&format!("SELECT partition_key FROM system.tables WHERE database = {} AND name = {}", sq(&db), sq(t)), &[]).await.unwrap();
            assert_eq!(range_partition_key(&pk), ok, "{pk}");
            let mut wrong = 0;
            for _ in 0..40 {
                let lo = day0 - DAY_NS + next() % (8 * DAY_NS);
                let hi = lo + next() % (3 * DAY_NS);
                let r = CheckRange { lo_ns: lo, hi_ns: hi };
                let want = rows.iter().filter(|(ns, _)| r.covers(*ns)).count();
                let q = format!("SELECT count() FROM {fq} WHERE content_key IN ('k'){} FORMAT TSV", ClickHouseCentral::<MemBucket>::range_sql(&r));
                let got: usize = ch.query(&q, &[("optimize_use_projections", "1")]).await.unwrap().trim().parse().unwrap();
                // (the key in the other order, `(late_part, toDate(…))`, fails
                // loudly instead: UInt8 BETWEEN Date is ILLEGAL_TYPE_OF_ARGUMENT)
                if ok {
                    assert_eq!(got, want, "{key}: range {r:?}");
                } else if got != want {
                    wrong += 1;
                }
            }
            assert!(ok || wrong > 0, "the refused key would have read the wrong partitions");
        }
        ch.query(&format!("DROP DATABASE {db} SYNC"), &[]).await.unwrap();
    }

    /// The generated statements parse on a real server (skipped when none is up).
    #[tokio::test(flavor = "current_thread")]
    async fn statements_parse_on_clickhouse() {
        let url = std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
        let ch = otap_s3pq::central::ClickHouse::new(&url);
        if ch.query("SELECT 1", &[]).await.is_err() {
            return otap_s3pq::testgate::skip("clickhouse", format!("no ClickHouse at {url}"));
        }
        let b = Rc::new(MemBucket::default());
        let c = ClickHouseCentral::new(&url, "db", b, "k", "s", 1000);
        let tr = LaneKind::for_signal("traces").unwrap();
        let mut a = obj("r/p/traces/E/1.parquet", "H'1");
        let mut z = obj("r/p/traces/E/2.parquet", "H2");
        a.received_ns = 11;
        z.received_ns = 22;
        let f = Fence { wall_ms: 1234, budget_ms: 3000 };
        let r = CheckRange { lo_ns: 1, hi_ns: 2 };
        let count = format!("SELECT content_key, count() FROM db.t WHERE content_key IN ('a'){} GROUP BY content_key", ClickHouseCentral::<MemBucket>::range_sql(&r));
        for sql in [c.insert_sql(&tr, &[&a, &z], f, true), c.insert_sql(&tr, &[&a], f, true), c.insert_sql(&tr, &[&a, &z], f, false), count, c.announce_sql(&tr, &[&a, &z], f), c.announce_sql(&tr, &[&a], f)] {
            let r = ch.query(&format!("EXPLAIN AST {sql}"), &[]).await;
            assert!(r.is_ok(), "{sql}\n{r:?}");
        }
    }

    #[test]
    fn statements() {
        let b = Rc::new(MemBucket::default());
        let c = ClickHouseCentral::new("http://x", "db", b, "k", "s", 1000);
        let tr = LaneKind::for_signal("traces").unwrap();
        let (a, z) = (obj("r/p/traces/E/1.parquet", "H1"), obj("r/p/traces/E/2.parquet", "H2"));
        let f = Fence { wall_ms: 1234, budget_ms: 3000 };
        let sql = c.insert_sql(&tr, &[&a, &z], f, false);
        assert!(sql.starts_with("INSERT INTO db.otel_traces ("), "{sql}");
        assert!(sql.contains("transform(_path, ['mem/r/p/traces/E/1.parquet', 'mem/r/p/traces/E/2.parquet'], ['H1', 'H2'], '')"), "{sql}");
        assert!(sql.contains("s3('mem://{r/p/traces/E/1.parquet,r/p/traces/E/2.parquet}'"), "{sql}");
        assert!(sql.ends_with("WHERE now64(3) <= fromUnixTimestamp64Milli(toInt64(1234))"), "{sql}");
        let one = c.insert_sql(&tr, &[&a], f, false);
        assert!(one.contains(", 'H1' FROM s3('mem://r/p/traces/E/1.parquet'"), "{one}");
        // the range assertion (only for objects that carry a received time)
        assert_eq!(c.insert_sql(&tr, &[&a], f, true), one, "no received time: no assertion");
        let (mut a2, mut z2) = (a.clone(), z.clone());
        a2.received_ns = 11;
        z2.received_ns = 22;
        let g = c.insert_sql(&tr, &[&a2, &z2], f, true);
        assert!(g.contains("AND NOT throwIf(toUnixTimestamp64Nano(received_at) != transform(_path, ['mem/r/p/traces/E/1.parquet', 'mem/r/p/traces/E/2.parquet'], [toInt64(11), toInt64(22)], toInt64(-1)), 'OTAPRS_RANGE_GUARD: a row"), "{g}");
        // Every literal is quoted by `sq`: the statements' quotes balance (a
        // message with an apostrophe once broke every guarded insert).
        for q in [&g, &sql, &one, &c.insert_sql(&tr, &[&a2], f, true)] {
            assert!(quotes_balance(q), "unbalanced quotes: {q}");
        }
        let r = ClickHouseCentral::<MemBucket>::range_sql(&CheckRange { lo_ns: 1, hi_ns: 2 });
        assert_eq!(r, " AND _partition_value.1 BETWEEN toDate(fromUnixTimestamp64Nano(toInt64(1))) AND toDate(fromUnixTimestamp64Nano(toInt64(2)))");
        assert!(range_partition_key("toDate(received_at)") && range_partition_key(" toDate( received_at ) "));
        assert!(!range_partition_key("toDate(Timestamp)") && !range_partition_key("toYYYYMM(received_at)") && !range_partition_key(""));
        // D34: the day first, then the object-constant late_part; nothing else
        assert!(range_partition_key("(toDate(received_at), late_part)") && range_partition_key("(toDate(received_at),late_part)"));
        assert!(!range_partition_key("(late_part, toDate(received_at))"), "_partition_value.1 would be late_part");
        assert!(!range_partition_key("(toDate(received_at), ServiceName)") && !range_partition_key("(toDate(Timestamp), late_part)"));
        // late_part: only where the table has the column, a constant for a
        // statement of one part, per object by path otherwise
        assert!(!c.writes_late_part(&tr) && !sql.contains("late_part"));
        c.set_late_part(&tr, true);
        let lz = Obj { late: true, ..z.clone() };
        let bulk = c.insert_sql(&tr, &[&a, &z], f, false);
        assert!(bulk.contains(", content_key, late_part) SELECT ") && bulk.contains("['H1', 'H2'], ''), toUInt8(0) FROM s3("), "{bulk}");
        assert!(c.insert_sql(&tr, &[&lz], f, false).contains(", 'H2', toUInt8(1) FROM s3("));
        let mixed = c.insert_sql(&tr, &[&a, &lz], f, false);
        assert!(mixed.contains(", transform(_path, ['mem/r/p/traces/E/1.parquet', 'mem/r/p/traces/E/2.parquet'], [0, 1], 0)::UInt8 FROM s3("), "{mixed}");
        let ga = LaneKind::for_signal("metrics_gauge").unwrap();
        assert!(!c.writes_late_part(&ga) && !c.insert_sql(&ga, &[&lz], f, false).contains("late_part"), "another table: no column");
        c.set_late_part(&tr, false);
        assert_eq!(c.insert_sql(&tr, &[&a, &z], f, false), sql);
        let se = LaneKind::for_signal("metrics_series").unwrap();
        assert!(!se.counted);
        let s = c.insert_sql(&se, &[&a], f, true);
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

    /// Traces and logs get ClickStack's rollup table and view after the
    /// table, named after the (possibly overridden) table; metrics get none.
    #[test]
    fn rollups_follow_the_table() {
        for (sig, mv) in [("traces", "db.t_kv_rollup_15m_mv"), ("logs", "db.t_attr_kv_rollup_15m_mv")] {
            let k = LaneKind::for_signal(sig).unwrap();
            let r = k.create_rollups("db.t");
            assert_eq!(r.len(), 2, "{r:?}");
            assert_eq!(r, central::create_rollups("db.t", Signal::from_name(sig).unwrap()));
            assert_eq!(r.iter().map(|s| created_name(s).unwrap()).collect::<Vec<_>>(), vec!["db.t_kv_rollup_15m", mv]);
            assert!(r[1].contains(" TO db.t_kv_rollup_15m\n") && r[1].contains("FROM db.t\n"), "{}", r[1]);
        }
        for sig in ["metrics_gauge", "metrics_sum", "metrics_series", "metrics_number_points", "metrics_summary_points"] {
            assert!(LaneKind::for_signal(sig).unwrap().create_rollups("db.t").is_empty(), "{sig}");
        }
        assert_eq!(created_name("CREATE TABLE IF NOT EXISTS db.x (a UInt8)"), Some("db.x"));
        assert_eq!(created_name("CREATE TABLE IF NOT EXISTS db.x\n("), Some("db.x"));
        assert_eq!(created_name("SELECT 1"), None);
    }

    /// Against ClickHouse and SeaweedFS (skipped when either is down;
    /// `OTAPRS_CH`, `OTAPRS_S3=http://host/bucket`): `ensure` creates the
    /// logs and traces tables with their rollup tables and views, and is
    /// idempotent; an insert through the consumer's statement fills the
    /// rollup with one count per row and key; the exact retry of that
    /// statement (same dedup token) adds nothing to the table AND nothing to
    /// the rollup (the rollup target's dedup window).
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_creates_the_rollup_and_an_insert_fills_it() {
        use crate::consumer::audit::tests::otlp_logs;
        use crate::consumer::bucket::{Bucket, Cond, Put, S3Bucket};
        use otap_s3pq::batch::{Encoder, Format, Input};
        use otap_s3pq::encode::ParquetOptions;
        use otap_s3pq::flatten::Envelope;
        let url = std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
        let ch = ClickHouse::new(&url);
        if ch.query("SELECT 1", &[]).await.is_err() {
            return otap_s3pq::testgate::skip("clickhouse", format!("no ClickHouse at {url}"));
        }
        let s3 = std::env::var("OTAPRS_S3").unwrap_or_else(|_| "http://127.0.0.1:18333/audit-consumer".into());
        let id = format!("{:08x}", rand::random::<u32>());
        let store = otap_s3pq::store::S3Config {
            url: format!("{s3}/rollup-it-{id}/edges"),
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
        let db = format!("rollup_it_{id}");
        let c = ClickHouseCentral::new(&url, &db, bucket.clone(), "otel", "otelsecret", 20_000);
        let (lk, tk) = (LaneKind::for_signal("logs").unwrap(), LaneKind::for_signal("traces").unwrap());
        for _ in 0..2 {
            c.ensure(&lk).await.unwrap();
            c.ensure(&tk).await.unwrap();
        }
        let tables = ch.query(&format!("SELECT name, engine FROM system.tables WHERE database = {} ORDER BY name FORMAT TSV", sq(&db)), &[]).await.unwrap();
        assert_eq!(
            tables,
            "otel_logs\tMergeTree\notel_logs_attr_kv_rollup_15m_mv\tMaterializedView\notel_logs_kv_rollup_15m\tSummingMergeTree\n\
             otel_resources\tReplacingMergeTree\notel_resources_announced\tView\n\
             otel_traces\tMergeTree\notel_traces_kv_rollup_15m\tSummingMergeTree\notel_traces_kv_rollup_15m_mv\tMaterializedView"
        );
        // Two logs objects on S3, as an edge writes them.
        let now = crate::consumer::wall_ms() * 1_000_000;
        let lane = format!("{root}/p1/logs");
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let mut objs = Vec::new();
        for (seq, (n, tag)) in [(7, "a"), (5, "b")].into_iter().enumerate() {
            let f = enc.flatten(&Input::Otlp(Signal::Logs, &otlp_logs(n, tag))).unwrap();
            let o = enc.encode(&f, &Envelope { producer: "p1".into(), epoch: "E1".into(), batch: seq as u64, received_ns: now }).unwrap();
            let key = format!("{lane}/E1/{seq}.parquet");
            assert!(matches!(bucket.put(&key, o.body, Cond::Create, &o.meta).await, Put::Ok(_)));
            // the second is a late part (D34): one statement, two partitions
            objs.push(Obj { lane: lane.clone(), epoch: "E1".into(), seq: seq as u64, key, size: 1, content: f.content.clone(), rows: n as u64, received_ns: now, seen_ms: 0, announce: 0, late: seq == 1 });
        }
        assert!(c.writes_late_part(&lk) && c.writes_late_part(&tk) && c.ranged(&lk), "the new DDL: late_part, a ranged key");
        let refs: Vec<&Obj> = objs.iter().collect();
        let rollup = || {
            let q = format!("SELECT Key, sum(count) FROM {db}.otel_logs_kv_rollup_15m GROUP BY Key ORDER BY Key FORMAT TSV");
            let ch = &ch;
            async move { ch.query(&q, &[]).await.unwrap() }
        };
        let rows = || {
            let q = format!("SELECT count() FROM {db}.otel_logs");
            let ch = &ch;
            async move { ch.query(&q, &[]).await.unwrap() }
        };
        let fence = || Fence { wall_ms: crate::consumer::wall_ms() + 60_000, budget_ms: 10_000 };
        c.insert(&lk, &refs, fence(), "tok-1", true).await.unwrap();
        assert_eq!(rows().await, "12");
        let first = rollup().await;
        assert!(!first.is_empty() && first.lines().all(|l| l.ends_with("\t12")), "one count per row and key: {first}");
        assert!(first.lines().any(|l| l.starts_with("ServiceName\t")), "{first}");
        // The exact retry: deduplicated in the table and in the rollup.
        c.insert(&lk, &refs, fence(), "tok-1", true).await.unwrap();
        assert_eq!(rows().await, "12");
        assert_eq!(rollup().await, first, "an exact retry counts nothing twice");
        // Counts by the projection, as the check reads them.
        let n = c.counts(&lk, &[&objs[0].content, &objs[1].content], None).await.unwrap();
        assert_eq!((n[&objs[0].content], n[&objs[1].content]), (7, 5));
        // ... and in the objects' own range, which spans both late_part values
        let r = CheckRange { lo_ns: now, hi_ns: now };
        let n = c.counts(&lk, &[&objs[0].content, &objs[1].content], Some(r)).await.unwrap();
        assert_eq!((n[&objs[0].content], n[&objs[1].content]), (7, 5));
        let parts = ch
            .query(&format!("SELECT content_key = {}, late_part, count(), uniqExact(_part), any(_partition_value.2) FROM {db}.otel_logs GROUP BY 1, 2 ORDER BY 1, 2 FORMAT TSV", sq(&objs[1].content)), &[])
            .await
            .unwrap();
        assert_eq!(parts, "0\t0\t7\t1\t0\n1\t1\t5\t1\t1", "each object in its own late_part partition, one part each");
        ch.query(&format!("DROP DATABASE {db} SYNC"), &[]).await.unwrap();
        let keys: Vec<String> = bucket.list(&root, None).await.unwrap().into_iter().map(|i| i.key).collect();
        let _ = bucket.delete(&keys).await;
    }

    /// Resource announcements on a real server: an object announcing two
    /// resources (one of them twice in the object, rows of a third without
    /// covered attributes), its announcements inserted twice (a retry) and a
    /// copy of the object in another epoch announcing again: otel_resources
    /// holds each announcement once (FINAL), with the covered set only, the
    /// ids the rows carry, and the view one row per resource.
    #[tokio::test(flavor = "current_thread")]
    async fn announcements_land_once_with_the_rows_ids() {
        use crate::consumer::bucket::{Bucket, Cond, Put, S3Bucket};
        use otap_s3pq::batch::{Encoder, Format, Input};
        use otap_s3pq::encode::ParquetOptions;
        use otap_s3pq::flatten::Envelope;
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value::Value};
        use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
        use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
        use prost::Message;
        let url = std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
        let ch = ClickHouse::new(&url);
        if ch.query("SELECT 1", &[]).await.is_err() {
            return otap_s3pq::testgate::skip("clickhouse", format!("no ClickHouse at {url}"));
        }
        let s3 = std::env::var("OTAPRS_S3").unwrap_or_else(|_| "http://127.0.0.1:18333/audit-consumer".into());
        let id = format!("{:08x}", rand::random::<u32>());
        let store = otap_s3pq::store::S3Config {
            url: format!("{s3}/announce-it-{id}/edges"),
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
        let db = format!("announce_it_{id}");
        let c = ClickHouseCentral::new(&url, &db, bucket.clone(), "otel", "otelsecret", 20_000);
        let lk = LaneKind::for_signal("logs").unwrap();
        c.ensure(&lk).await.unwrap();
        let kv = |k: &str, v: &str| KeyValue { key: k.into(), value: Some(AnyValue { value: Some(Value::StringValue(v.into())) }) };
        let res = |pod: &str| Resource {
            attributes: vec![kv("k8s.pod.name", pod), kv("k8s.namespace.name", "shop"), kv("telemetry.sdk.name", "opentelemetry")],
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        };
        let rl = |r: Option<Resource>, n: u64| ResourceLogs {
            resource: r,
            schema_url: String::new(),
            scope_logs: vec![ScopeLogs {
                scope: None,
                schema_url: String::new(),
                log_records: (0..n).map(|i| LogRecord { time_unix_nano: 1_790_000_000_000_000_000 + i, ..Default::default() }).collect(),
            }],
        };
        let req = LogsData { resource_logs: vec![rl(Some(res("a-1")), 3), rl(None, 2), rl(Some(res("b-1")), 1), rl(Some(res("a-1")), 2)] }.encode_to_vec();
        let now = crate::consumer::wall_ms() * 1_000_000;
        let lane = format!("{root}/c1/p1/logs");
        let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
        let f = enc.flatten(&Input::Otlp(Signal::Logs, &req)).unwrap();
        let mut objs = Vec::new();
        for (epoch, seq) in [("E1", 0u64), ("E2", 0)] {
            let o = enc.encode(&f, &Envelope { producer: "p1".into(), epoch: epoch.into(), batch: seq, received_ns: now }).unwrap();
            assert_eq!(o.meta.get("oscope-announce").map(String::as_str), Some("2"));
            let key = format!("{lane}/{epoch}/{seq}.parquet");
            assert!(matches!(bucket.put(&key, o.body, Cond::Create, &o.meta).await, Put::Ok(_)));
            objs.push(Obj { lane: lane.clone(), epoch: epoch.into(), seq, key, size: 1, content: f.content.clone(), rows: 8, received_ns: now, seen_ms: 0, announce: 2, late: false });
        }
        let fence = || Fence { wall_ms: crate::consumer::wall_ms() + 60_000, budget_ms: 10_000 };
        c.announce(&lk, &[&objs[0]], fence(), "ann-1").await.unwrap();
        c.announce(&lk, &[&objs[0]], fence(), "ann-1").await.unwrap(); // an exact retry
        c.announce(&lk, &[&objs[0], &objs[1]], fence(), "ann-2").await.unwrap(); // regrouped, and the copy
        // Started after its fence: an error, settled, and nothing written (the
        // count below is unchanged by it).
        let past = Fence { wall_ms: crate::consumer::wall_ms() - 1_000, budget_ms: 10_000 };
        let e = c.announce(&lk, &[&objs[0], &objs[1]], past, "ann-3").await.unwrap_err();
        assert!(e.settled && e.answered && e.msg.contains(FENCED), "{e:?}");
        let q = |sql: String| {
            let ch = &ch;
            async move { ch.query(&sql, &[]).await.unwrap() }
        };
        assert_eq!(q(format!("SELECT count() FROM {db}.otel_resources FINAL")).await, "4", "2 resources x 2 announcing objects (epochs), each once");
        let view = q(format!("SELECT resource_id, toString(mapSort(ResourceAttributes)), announcements FROM {db}.otel_resources_announced ORDER BY resource_id FORMAT TSVRaw")).await;
        assert_eq!(view.lines().count(), 2, "{view}");
        assert!(view.lines().all(|l| l.ends_with("\t2") && !l.contains("telemetry.sdk") && l.contains("'k8s.namespace.name':'shop'")), "{view}");
        // The rows carry the same ids.
        c.insert(&lk, &[&objs[0]], fence(), "rows-1", true).await.unwrap();
        let joined = q(format!(
            "SELECT countIf(resource_id IN (SELECT resource_id FROM {db}.otel_resources_announced)), countIf(resource_id = {}), count() FROM {db}.otel_logs FORMAT TSV",
            otap_s3pq::resource::empty_id()
        ))
        .await;
        assert_eq!(joined, "6\t2\t8", "rows of announced resources, rows without covered attributes, all rows");
        ch.query(&format!("DROP DATABASE {db} SYNC"), &[]).await.unwrap();
        let keys: Vec<String> = bucket.list(&root, None).await.unwrap().into_iter().map(|i| i.key).collect();
        let _ = bucket.delete(&keys).await;
    }

    /// A provider that hands out a new temporary credential on every call
    /// (as a chain does after each refresh), or fails.
    #[derive(Debug)]
    struct Rotating {
        n: std::sync::atomic::AtomicU32,
        fail: bool,
    }

    #[async_trait]
    impl object_store::CredentialProvider for Rotating {
        type Credential = object_store::aws::AwsCredential;
        async fn get_credential(&self) -> object_store::Result<std::sync::Arc<Self::Credential>> {
            if self.fail {
                return Err(object_store::Error::Generic { store: "S3", source: "no IMDS answer".into() });
            }
            let i = self.n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(std::sync::Arc::new(object_store::aws::AwsCredential {
                key_id: format!("ASIA{i}"),
                secret_key: format!("sec'ret/{i}"),
                token: Some(format!("FwoGZXIvYXdzE{i}")),
            }))
        }
    }

    /// What `s3()` carries: fixed keys; a temporary credential from the
    /// chain, resolved again for every statement, with its session token;
    /// nothing (the server's own); and a credential that can't be had.
    #[tokio::test(flavor = "current_thread")]
    async fn s3_credentials_per_statement() {
        use crate::consumer::bucket::MemBucket;
        let b = Rc::new(MemBucket::default());
        let mut c = ClickHouseCentral::new("http://x", "db", b, "k", "s", 1000);
        let tr = LaneKind::for_signal("traces").unwrap();
        let (a, z) = (obj("r/p/traces/E/1.parquet", "H1"), obj("r/p/traces/E/2.parquet", "H2"));
        let f = Fence { wall_ms: 1_790_000_000_000, budget_ms: 10_000 };
        c.s3_creds().await.unwrap();
        let s = c.insert_sql(&tr, &[&a], f, false);
        assert!(s.contains("FROM s3('mem://r/p/traces/E/1.parquet', 'k', 's', 'Parquet', '"), "{s}");

        c.s3_auth = S3Auth::Chain(std::sync::Arc::new(Rotating { n: Default::default(), fail: false }));
        for i in 0..3 {
            c.s3_creds().await.unwrap();
            let s = c.insert_sql(&tr, &[&a, &z], f, false);
            let want = format!("', 'ASIA{i}', 'sec\\'ret/{i}', 'FwoGZXIvYXdzE{i}', 'Parquet', '");
            assert!(s.contains(&want), "statement {i}: {s}");
            assert!(quotes_balance(&s), "{s}");
            let an = c.announce_sql(&tr, &[&a], f);
            assert!(an.contains(&format!("'ASIA{i}', 'sec\\'ret/{i}', 'FwoGZXIvYXdzE{i}', 'Parquet', ")), "{an}");
            // An error answer echoing the statement (a syntax error does) is logged redacted.
            let echo = format!("Code: 62. DB::Exception: Syntax error: failed at position 9: {s}. (SYNTAX_ERROR)");
            let r = c.redact(&echo);
            assert!(!r.contains(&format!("ret/{i}")) && !r.contains(&format!("FwoGZXIvYXdzE{i}")), "{r}");
            assert!(r.contains(&format!("'ASIA{i}', '[HIDDEN]', '[HIDDEN]', 'Parquet'")), "{r}");
        }
        assert!(!format!("{:?}", c.s3_now.borrow()).contains("ret/"), "Debug never prints the secret");

        c.s3_auth = S3Auth::Server;
        c.s3_creds().await.unwrap();
        let s = c.insert_sql(&tr, &[&a], f, false);
        assert!(s.contains("FROM s3('mem://r/p/traces/E/1.parquet', 'Parquet', '"), "{s}");

        // No credential to be had: nothing is sent, so nothing can land.
        c.s3_auth = S3Auth::Chain(std::sync::Arc::new(Rotating { n: Default::default(), fail: true }));
        let e = c.insert(&tr, &[&a], f, "tok", false).await.unwrap_err();
        assert!(e.settled && !e.answered && e.msg.contains("s3 credentials"), "{e:?}");
        assert_eq!(c.statements.get(), 0, "no statement was sent");
    }

    /// A credential that expires (or is revoked) while ClickHouse reads the
    /// objects: its S3 GET or HEAD gets 403, which 26.10 answers as
    /// `S3_ERROR` (499), measured in the next test. That is not a settling
    /// code: the worker waits the statement out and then checks what landed
    /// (an unsquashed statement may have written the objects read before
    /// the 403), like any other ambiguous outcome.
    #[test]
    fn an_s3_403_mid_statement_is_not_settled_at_once() {
        let m = "clickhouse 500 Internal Server Error: Code: 499. DB::Exception: Failed to get object info: No response body.. \
                 HTTP response code: 403. Please check your AWS credentials and permissions: while reading 'a.parquet' in bucket 'b' \
                 on disk 'StorageS3': While executing ReadFromObjectStorage. (S3_ERROR) (version 26.10.1.618 (official build))";
        assert_eq!(error_code(m), Some(499));
        assert!(!settles_at_once(m));
    }

    /// Against ClickHouse and SeaweedFS (skipped when either is down): a
    /// statement whose credentials are refused is an unsettled error
    /// answer, and the error kept carries no secret, even when the server
    /// echoes the statement; the server's own credentials
    /// (`S3Auth::Server`) are refused (497, settled) without the setting
    /// D18 names, which `run_insert` adds in that mode.
    #[tokio::test(flavor = "current_thread")]
    async fn refused_credentials_are_unsettled_and_redacted() {
        let _trace = otap_s3pq::oscope_trace::covers("IT", &["CAST-31", "H-6"]);
        use crate::consumer::bucket::{Bucket, Cond, Put, S3Bucket};
        let url = std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
        if ClickHouse::new(&url).query("SELECT 1", &[]).await.is_err() {
            return otap_s3pq::testgate::skip("clickhouse", format!("no ClickHouse at {url}"));
        }
        let s3 = std::env::var("OTAPRS_S3").unwrap_or_else(|_| "http://127.0.0.1:18333/otel".into());
        let id = format!("{:08x}", rand::random::<u32>());
        let store = otap_s3pq::store::S3Config {
            url: format!("{s3}/gap-creds-{id}"),
            access_key_id: Some("otel".into()),
            secret_access_key: Some("otelsecret".into()),
            ..Default::default()
        }
        .build()
        .unwrap();
        let root = store.prefix.clone();
        let bucket = Rc::new(S3Bucket::new(store));
        let key = format!("{root}/x.parquet");
        let put = bucket.put(&key, bytes::Bytes::from_static(b"PAR1"), Cond::Create, &BTreeMap::new()).await;
        if !matches!(put, Put::Ok(_)) {
            return otap_s3pq::testgate::skip("s3", format!("no S3 at {s3}: {put:?}"));
        }
        let mut c = ClickHouseCentral::new(&url, "default", bucket.clone(), "", "", 20_000);
        c.s3_auth = S3Auth::Keys(S3Keys { key: "otel".into(), secret: "GAPWRONGSECRET".into(), token: Some("GAPFOREIGNTOKEN".into()) });
        let f = Fence { wall_ms: crate::consumer::wall_ms() + 60_000, budget_ms: 10_000 };
        c.s3_creds().await.unwrap();
        let src = c.s3_fn(&bucket.object_url(&key), "x UInt64");
        let e = c.run_insert("db.t", &format!("SELECT count() FROM {src}"), f, "t1").await.unwrap_err();
        assert!(e.answered && !e.settled && error_code(&e.msg) == Some(499), "{e:?}");
        assert!(!e.msg.contains("GAPWRONG") && !e.msg.contains("GAPFOREIGN"), "{}", e.msg);
        // A syntax error echoes the statement text raw; what we keep is redacted.
        let e = c.run_insert("db.t", &format!("SELECT count() FRO M {src}"), f, "t2").await.unwrap_err();
        assert!(e.msg.contains("SYNTAX_ERROR") && e.msg.contains("[HIDDEN]"), "{}", e.msg);
        assert!(!e.msg.contains("GAPWRONG") && !e.msg.contains("GAPFOREIGN"), "{}", e.msg);
        // The server's own credentials: refused without the setting.
        c.s3_auth = S3Auth::Server;
        c.s3_creds().await.unwrap();
        let src = c.s3_fn(&bucket.object_url(&key), "x UInt64");
        let direct = c.q(&format!("SELECT count() FROM {src}"), &[]).await.unwrap_err();
        assert_eq!(error_code(&direct), Some(497), "{direct}");
        assert!(settles_at_once(&direct));
        // With it (run_insert), the read gets past authorisation: this
        // 4-byte object then fails as Parquet, not on access.
        let e = c.run_insert("db.t", &format!("SELECT count() FROM {src}"), f, "t3").await.unwrap_err();
        assert!(
            error_code(&e.msg) != Some(497) && !e.msg.contains("403"),
            "{}\n(a 403 here means the ClickHouse server has no S3 credentials of its own for {s3}: \
             ci/services.sh gives its container AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY; a local \
             server must be started with them in its environment)",
            e.msg
        );
        let _ = bucket.delete(&[key]).await;
    }
}
