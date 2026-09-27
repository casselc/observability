//! An in-memory ClickHouse that speaks the HTTP interface for exactly the
//! consumer's statements (`consumer/sql.rs`): `ensure`'s DDL and
//! `system.tables` reads, `INSERT … SELECT … FROM s3(…) WHERE now64(3) <=
//! fence` (single object or `{k1,k2}` with the `transform(_path, …)`
//! content keys; the row repair's `row_ordinal NOT IN`), the count check
//! `SELECT content_key, count() … GROUP BY content_key` (with the partition
//! range), `KILL QUERY`, `SYSTEM SYNC REPLICA`. Anything else is a syntax
//! error, so a new statement shape fails loudly instead of being guessed.
//!
//! An object's rows are its S3 metadata (`oscope-rows`, `oscope-received`),
//! looked up by URL (the emulator stands in for ClickHouse reading the
//! Parquet). Inserts deduplicate by `insert_deduplication_token` per table
//! and partition (window 1,000, as `non_replicated_deduplication_window`).
//! The fence is evaluated on the server's clock when the statement starts.
//!
//! Faults (AMBIGUITY.md C1, C2): an insert refused before writing (252,
//! settles), committed with its answer lost (the connection drops),
//! committed late with no answer, answered TIMEOUT_EXCEEDED (159) and
//! committed later; a count check that fails; and, for a query that doesn't
//! pin the `*_overflow_mode` settings to `throw`, a short count with HTTP 200
//! (the partial-result hazard H-2).
//!
//! `tests/dst_net.rs` checks it against a real server
//! (`ch_emulator_matches_clickhouse`).

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

const DAY_NS: u64 = 86_400_000_000_000;

#[derive(Clone, Copy, Debug)]
pub struct ObjInfo {
    pub rows: u64,
    pub received_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertFault {
    None,
    /// TOO_MANY_PARTS before anything is written.
    SettledErr,
    /// Committed, then the connection drops.
    Lost,
    /// No answer; commits as late as `fence + budget + slack`.
    Late,
    /// TIMEOUT_EXCEEDED at the budget; commits later anyway.
    TimeoutCommit,
}

pub trait ChFaults {
    fn insert(&self) -> InsertFault {
        InsertFault::None
    }
    fn check_err(&self) -> bool {
        false
    }
    /// The server's profile has `*_overflow_mode = 'break'` limits (C2).
    fn break_profile(&self) -> bool {
        false
    }
    /// Execution time of a statement, ms.
    fn exec_ms(&self) -> u64 {
        5
    }
    /// A random point in [lo, hi] (for late commits).
    fn between(&self, lo: u64, hi: u64) -> u64 {
        hi.max(lo)
    }
}

pub struct NoChFaults;
impl ChFaults for NoChFaults {}

#[derive(Default)]
pub struct Table {
    pub partition_key: String,
    /// content key -> rows
    pub rows: BTreeMap<String, u64>,
    /// (content key, day) -> rows
    pub by_day: BTreeMap<(String, u64), u64>,
    tokens: VecDeque<String>,
}

pub enum Reply {
    Ok(String),
    Err(u16, String),
    /// Close the connection without an answer.
    Drop,
}

/// What landed: (table, [(object URL, content key, rows added)]).
pub type LandHook = Box<dyn Fn(u64, &str, &[(String, String, u64)])>;

pub struct ChEmu {
    /// The server's wall clock, ms.
    pub clock: Box<dyn Fn() -> u64>,
    pub objects: Box<dyn Fn(&str) -> Option<ObjInfo>>,
    pub tables: RefCell<BTreeMap<String, Table>>,
    pub databases: RefCell<Vec<String>>,
    pub faults: RefCell<Rc<dyn ChFaults>>,
    /// How long a commit can outlive `max_execution_time` (Keeper).
    pub slack_ms: u64,
    /// Called when a statement starts (inside its fence: its number, the
    /// object URLs, the client's host) and when it lands.
    pub on_arrive: RefCell<Option<Box<dyn Fn(u64, &[String], &str)>>>,
    pub on_land: RefCell<Option<LandHook>>,
    pub log: RefCell<Option<Box<dyn Fn(&str)>>>,
    n: Cell<u64>,
    pub fenced: Cell<u64>,
    pub deduplicated: Cell<u64>,
}

/// The single-quoted literals of `s`, in order (ClickHouse escapes `\\`, `\'`).
pub fn literals(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Option<String> = None;
    let mut esc = false;
    for ch in s.chars() {
        match (&mut cur, esc, ch) {
            (Some(c), true, x) => {
                c.push(x);
                esc = false;
            }
            (Some(_), false, '\\') => esc = true,
            (Some(_), false, '\'') => out.push(cur.take().unwrap()),
            (Some(c), false, x) => c.push(x),
            (None, _, '\'') => cur = Some(String::new()),
            _ => {}
        }
    }
    out
}

fn after<'a>(s: &'a str, pat: &str) -> Option<&'a str> {
    s.find(pat).map(|i| &s[i + pat.len()..])
}

fn number_after(s: &str, pat: &str) -> Option<u64> {
    let r = after(s, pat)?;
    r.chars().take_while(|c| c.is_ascii_digit()).collect::<String>().parse().ok()
}

fn syntax(msg: &str) -> Reply {
    Reply::Err(400, format!("Code: 62. DB::Exception: Syntax error (not emulated): {msg}. (SYNTAX_ERROR)"))
}

fn unknown_table(t: &str) -> Reply {
    Reply::Err(404, format!("Code: 60. DB::Exception: Table {t} does not exist. (UNKNOWN_TABLE)"))
}

struct Insert {
    table: String,
    /// (object URL, content key)
    objs: Vec<(String, String)>,
    fence_ms: u64,
    repair: bool,
}

impl ChEmu {
    pub fn new(clock: Box<dyn Fn() -> u64>, objects: Box<dyn Fn(&str) -> Option<ObjInfo>>, slack_ms: u64) -> Self {
        ChEmu {
            clock,
            objects,
            tables: RefCell::new(BTreeMap::new()),
            databases: RefCell::new(Vec::new()),
            faults: RefCell::new(Rc::new(NoChFaults)),
            slack_ms,
            on_arrive: RefCell::new(None),
            on_land: RefCell::new(None),
            log: RefCell::new(None),
            n: Cell::new(0),
            fenced: Cell::new(0),
            deduplicated: Cell::new(0),
        }
    }

    fn log(&self, l: &str) {
        if let Some(f) = self.log.borrow().as_ref() {
            f(l);
        }
    }

    pub fn count(&self, table: &str, content: &str) -> u64 {
        self.tables.borrow().get(table).and_then(|t| t.rows.get(content).copied()).unwrap_or(0)
    }

    /// One HTTP request: the settings (query string) and the SQL (body).
    pub async fn handle(self: Rc<Self>, settings: BTreeMap<String, String>, sql: String, peer: &str) -> Reply {
        let q = sql.trim();
        let r = self.clone().dispatch(&settings, q, peer).await;
        let head: String = q.chars().take(90).collect();
        match &r {
            Reply::Ok(s) => self.log(&format!("CH {head} -> 200 {:?}", s.chars().take(120).collect::<String>())),
            Reply::Err(c, s) => self.log(&format!("CH {head} -> {c} {}", s.chars().take(100).collect::<String>())),
            Reply::Drop => self.log(&format!("CH {head} -> connection dropped")),
        }
        r
    }

    async fn dispatch(self: Rc<Self>, st: &BTreeMap<String, String>, q: &str, peer: &str) -> Reply {
        if q == "SELECT 1" {
            return Reply::Ok("1\n".into());
        }
        if let Some(db) = q.strip_prefix("CREATE DATABASE IF NOT EXISTS ") {
            let mut d = self.databases.borrow_mut();
            if !d.iter().any(|x| x == db.trim()) {
                d.push(db.trim().to_string());
            }
            return Reply::Ok(String::new());
        }
        if let Some(rest) = q.strip_prefix("CREATE TABLE IF NOT EXISTS ") {
            let name: String = rest.chars().take_while(|c| !c.is_whitespace() && *c != '(').collect();
            let pk = after(rest, "PARTITION BY ").map(|r| r.lines().next().unwrap_or("").trim().to_string()).unwrap_or_default();
            let _ = self.tables.borrow_mut().entry(name).or_insert_with(|| Table { partition_key: pk, ..Default::default() });
            return Reply::Ok(String::new());
        }
        if q.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS ") || q.starts_with("CREATE TABLE ") {
            return Reply::Ok(String::new());
        }
        if q.starts_with("KILL QUERY ") || q.starts_with("SYSTEM SYNC REPLICA ") {
            return Reply::Ok(String::new());
        }
        if let Some(rest) = q.strip_prefix("SELECT ") {
            if rest.contains(" FROM system.tables WHERE database = ") {
                let l = literals(rest);
                let (db, t) = match (l.first(), l.get(1)) {
                    (Some(d), Some(t)) => (d.clone(), t.clone()),
                    _ => return syntax(q),
                };
                let tables = self.tables.borrow();
                let tb = tables.get(&format!("{db}.{t}"));
                return Reply::Ok(match rest.split(' ').next() {
                    Some("partition_key") => tb.map_or(String::new(), |t| format!("{}\n", t.partition_key)),
                    Some("engine") => tb.map_or(String::new(), |_| "MergeTree\n".into()),
                    Some("count()") => format!("{}\n", tb.is_some() as u8),
                    _ => return syntax(q),
                });
            }
            if rest.starts_with("content_key, count() FROM ") {
                return self.counts(st, rest);
            }
            return syntax(q);
        }
        if q.starts_with("INSERT INTO ") {
            return self.insert(st, q, peer).await;
        }
        syntax(q)
    }

    fn counts(&self, st: &BTreeMap<String, String>, rest: &str) -> Reply {
        let table: String = rest["content_key, count() FROM ".len()..].split(' ').next().unwrap_or("").to_string();
        let Some(inlist) = after(rest, "WHERE content_key IN (") else { return syntax(rest) };
        let keys = literals(&inlist[..inlist.find(')').unwrap_or(inlist.len())]);
        let lo = number_after(rest, "BETWEEN toDate(fromUnixTimestamp64Nano(toInt64(");
        let hi = number_after(rest, ") AND toDate(fromUnixTimestamp64Nano(toInt64(");
        let range = lo.zip(hi);
        let faults = self.faults.borrow().clone();
        if faults.check_err() {
            return Reply::Err(500, "Code: 159. DB::Exception: Timeout exceeded (injected). (TIMEOUT_EXCEEDED)".into());
        }
        // C2: a profile with break modes shortens a count unless the query pins throw.
        let pinned = st.get("read_overflow_mode").map(String::as_str) == Some("throw");
        let short = faults.break_profile();
        if short && pinned {
            return Reply::Err(500, "Code: 158. DB::Exception: Limit for rows to read exceeded (injected profile). (TOO_MANY_ROWS)".into());
        }
        let tables = self.tables.borrow();
        let Some(t) = tables.get(&table) else { return unknown_table(&table) };
        let mut out = String::new();
        for k in keys {
            let n: u64 = match range {
                Some((lo, hi)) if t.partition_key == "toDate(received_at)" => {
                    t.by_day.range((k.clone(), lo / DAY_NS)..=(k.clone(), hi / DAY_NS)).map(|(_, n)| *n).sum()
                }
                _ => t.rows.get(&k).copied().unwrap_or(0),
            };
            let n = if short { n / 2 } else { n };
            if n > 0 {
                out.push_str(&format!("{k}\t{n}\n"));
            }
        }
        Reply::Ok(out)
    }

    fn parse_insert(q: &str) -> Result<Insert, String> {
        let table: String = q["INSERT INTO ".len()..].split(' ').next().unwrap_or("").to_string();
        let from = q.find(" FROM s3(").ok_or("no s3() source")?;
        let src = literals(&q[from..]);
        let url = src.first().ok_or("no s3 url")?.clone();
        let urls: Vec<String> = match (url.find('{'), url.ends_with('}')) {
            (Some(i), true) => url[i + 1..url.len() - 1].split(',').map(|k| format!("{}{k}", &url[..i])).collect(),
            _ => vec![url.clone()],
        };
        let fence_ms = number_after(q, "fromUnixTimestamp64Milli(toInt64(").ok_or("no fence")?;
        let repair = q.contains("row_ordinal NOT IN (SELECT row_ordinal FROM ");
        let select = &q[..from];
        let contents: Vec<String> = if repair {
            let c = literals(after(q, "WHERE content_key = ").ok_or("repair without a content key")?);
            vec![c.first().cloned().ok_or("repair content")?]
        } else if let Some(tr) = after(select, "transform(_path, [") {
            // [paths], [contents]: map each URL's path ("bucket/key") to its content key.
            let lits = literals(tr);
            let n = lits.len().saturating_sub(1) / 2;
            let (paths, cs) = (&lits[..n], &lits[n..2 * n]);
            urls.iter()
                .map(|u| {
                    let path = u.splitn(4, '/').nth(3).unwrap_or("");
                    paths.iter().position(|p| p == path).map(|i| cs[i].clone()).unwrap_or_default()
                })
                .collect()
        } else {
            vec![literals(&select[select.rfind(", '").ok_or("no content key")?..]).first().cloned().ok_or("content key")?]
        };
        if contents.len() != urls.len() || contents.iter().any(String::is_empty) {
            return Err(format!("content keys {contents:?} for {urls:?}"));
        }
        Ok(Insert { table, objs: urls.into_iter().zip(contents).collect(), fence_ms, repair })
    }

    async fn insert(self: Rc<Self>, st: &BTreeMap<String, String>, q: &str, peer: &str) -> Reply {
        let ins = match Self::parse_insert(q) {
            Ok(i) => i,
            Err(e) => return syntax(&e),
        };
        if !self.tables.borrow().contains_key(&ins.table) {
            return unknown_table(&ins.table);
        }
        let n = self.n.get() + 1;
        self.n.set(n);
        let urls: Vec<String> = ins.objs.iter().map(|(u, _)| u.clone()).collect();
        let now = (self.clock)();
        if now > ins.fence_ms {
            self.fenced.set(self.fenced.get() + 1);
            self.log(&format!("CH #{n} fenced: server {now} > fence {}", ins.fence_ms));
            return Reply::Ok(String::new());
        }
        if let Some(f) = self.on_arrive.borrow().as_ref() {
            f(n, &urls, peer);
        }
        let mut infos = Vec::new();
        for (u, c) in &ins.objs {
            match (self.objects)(u) {
                Some(i) => infos.push((u.clone(), c.clone(), i)),
                None => return Reply::Err(500, format!("Code: 499. DB::Exception: The specified key does not exist: {u}. (S3_ERROR)")),
            }
        }
        let budget_ms = st.get("max_execution_time").and_then(|v| v.parse::<f64>().ok()).map_or(10_000, |s| (s * 1000.0) as u64);
        let token = st.get("insert_deduplication_token").cloned();
        let faults = self.faults.borrow().clone();
        let fault = faults.insert();
        let latest = ins.fence_ms + budget_ms + self.slack_ms;
        let exec = faults.exec_ms().min(budget_ms.saturating_sub(1));
        let land = {
            let me = self.clone();
            let table = ins.table.clone();
            let repair = ins.repair;
            move || me.land(n, &table, &infos, token.as_deref(), repair)
        };
        match fault {
            InsertFault::None => {
                tokio::time::sleep(std::time::Duration::from_millis(exec)).await;
                land();
                Reply::Ok(String::new())
            }
            InsertFault::SettledErr => Reply::Err(500, "Code: 252. DB::Exception: Too many parts (injected). (TOO_MANY_PARTS)".into()),
            InsertFault::Lost => {
                tokio::time::sleep(std::time::Duration::from_millis(exec)).await;
                land();
                Reply::Drop
            }
            InsertFault::Late | InsertFault::TimeoutCommit => {
                let now = (self.clock)();
                let at = faults.between(now, latest);
                let me = self.clone();
                std::mem::drop(tokio::task::spawn_local(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(at.saturating_sub(now))).await;
                    me.log(&format!("CH #{n} commits late (server {}, latest {latest})", (me.clock)()));
                    land();
                }));
                if fault == InsertFault::Late {
                    Reply::Drop
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(budget_ms)).await;
                    Reply::Err(
                        500,
                        format!("Code: 159. DB::Exception: Timeout exceeded: elapsed {budget_ms}.000 ms, maximum: {budget_ms}.000 ms (injected). (TIMEOUT_EXCEEDED)"),
                    )
                }
            }
        }
    }

    fn land(&self, n: u64, table: &str, objs: &[(String, String, ObjInfo)], token: Option<&str>, repair: bool) {
        let mut tables = self.tables.borrow_mut();
        let Some(t) = tables.get_mut(table) else { return };
        if let Some(tok) = token.filter(|_| !repair) {
            if t.tokens.iter().any(|x| x == tok) {
                self.deduplicated.set(self.deduplicated.get() + 1);
                drop(tables);
                self.log(&format!("CH #{n} deduplicated (token {tok})"));
                return;
            }
            t.tokens.push_back(tok.to_string());
            if t.tokens.len() > 1000 {
                let _ = t.tokens.pop_front();
            }
        }
        let mut landed = Vec::new();
        for (u, c, i) in objs {
            let have = t.rows.get(c).copied().unwrap_or(0);
            let add = if repair { i.rows.saturating_sub(have) } else { i.rows };
            *t.rows.entry(c.clone()).or_default() += add;
            if add > 0 {
                *t.by_day.entry((c.clone(), i.received_ns / DAY_NS)).or_default() += add;
            }
            landed.push((u.clone(), c.clone(), add));
        }
        drop(tables);
        if let Some(f) = self.on_land.borrow().as_ref() {
            f(n, table, &landed);
        }
    }
}
