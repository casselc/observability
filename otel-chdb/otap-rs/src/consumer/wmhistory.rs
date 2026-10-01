//! The history of the published `complete_through` (DECISIONS.md D29,
//! amendment 2026-10-01; `../../FORMAT.md` §4.1; `../../model/wmHistory.qnt`).
//!
//! The watermark documents (`watermark.json`, `watermark/{cluster}.json`)
//! hold only a running max. Each also holds its scope's open hour here
//! (`History`): steps `(at_ms, values)`, each saying "by wall time `at_ms`,
//! every request of the scope received below these values was in central".
//!
//! - A step's `at_ms` is the publisher's clock read **after** the document's
//!   GET (inside the CAS), plus the clock-skew bound, raised to the last
//!   step's (`advance`'s clamp). The values were observed before it: ours
//!   before the GET, the previous document's before its PUT, which landed
//!   before our GET (model mutant `stampEarly`: a stamp read before the GET
//!   carries a value another publisher observed later; `noSkew`: a clock
//!   behind claims a value early).
//! - At most one step per `every_ms`, and only when a value changed.
//! - A step in a later hour **freezes** the open hour: it moves to `sealing`
//!   in the same CAS, and no later step can enter it (the clamp keeps every
//!   later stamp in the new hour; model mutant `noClamp`).
//! - After the CAS, each frozen hour is written create-only to
//!   `{ctl}/watermark-history/{scope}/{YYYY-MM-DD}T{HH}.json` (`seal`) and
//!   then dropped from `sealing` by a second CAS. Its content is the frozen
//!   hour, the same bytes whoever writes it, so a 412 is "already sealed"
//!   once read back equal; a lost answer, or a 412 with nothing stored,
//!   leaves the hour in `sealing` for the next run (rows 74/83/84; model
//!   mutant `sealFromRead`: an object written from a read, not a freeze).
//!
//! Readers (`as_of`): the step with the largest `at_ms <= T`, final when T's
//! hour is sealed or frozen.

use super::bucket::{Bucket, Cond, Put};
use super::coord::join;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const HOUR_MS: u64 = 3_600_000;
/// The scope of the fleet document's history (a cluster name never starts
/// with `_`).
pub const FLEET: &str = "_fleet";
/// Frozen hours kept in a document while their objects cannot be written;
/// beyond, the oldest is dropped (a gap: readers fall back to an earlier
/// step, which is lower, so still sound) and counted.
pub const MAX_SEALING: usize = 48;

/// Deliberate bugs for the tests (the model's mutants); `None` in production.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HistMutation {
    #[default]
    None,
    /// The stamp is the run's clock before the document's GET (`stampEarly`).
    StampEarly,
    /// The stamp is the bare clock, without the skew bound (`noSkew`).
    NoSkew,
    /// A stamp before the last step's is neither raised to it nor dropped:
    /// appended (`noClamp`).
    NoClamp,
}

/// One step: the scope's published values, true by wall time `at_ms`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistStep {
    pub at_ms: u64,
    /// `complete_through` (ns): every request below it is in central.
    pub ct_ns: u64,
    /// Per signal (ns).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub signals: BTreeMap<String, u64>,
    /// A signal not in `signals` (ns).
    #[serde(default)]
    pub unlisted_ns: u64,
}

impl HistStep {
    fn same_values(&self, o: &HistStep) -> bool {
        self.ct_ns == o.ct_ns && self.signals == o.signals && self.unlisted_ns == o.unlisted_ns
    }
    /// The value for `signals` (the minimum over them; `None`: the scope's
    /// `complete_through`, which is at or below every signal's).
    pub fn value_for(&self, signals: Option<&[String]>) -> u64 {
        match signals {
            Some(s) if !s.is_empty() && !self.signals.is_empty() => {
                let v = s.iter().map(|x| self.signals.get(x).copied().unwrap_or(self.unlisted_ns)).min().unwrap_or(self.ct_ns);
                v.max(self.ct_ns)
            }
            _ => self.ct_ns,
        }
    }
}

/// One hour's steps; `carry` is the last step before the hour (its value
/// holds from the hour's start until the first step).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistHour {
    pub hour_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carry: Option<HistStep>,
    #[serde(default)]
    pub steps: Vec<HistStep>,
}

impl HistHour {
    /// The last step at or before `t_ms` (the carry when it is the one).
    pub fn at(&self, t_ms: u64) -> Option<&HistStep> {
        self.steps.iter().rev().find(|s| s.at_ms <= t_ms).or(self.carry.as_ref().filter(|c| c.at_ms <= t_ms))
    }
    pub fn last(&self) -> Option<&HistStep> {
        self.steps.last().or(self.carry.as_ref())
    }
}

/// The history a watermark document holds: the open hour, and the hours
/// frozen but not yet confirmed sealed.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct History {
    #[serde(flatten)]
    pub open: HistHour,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sealing: Vec<HistHour>,
    /// Frozen hours dropped unsealed (`MAX_SEALING`), ever.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub dropped: u64,
}

fn is_zero(x: &u64) -> bool {
    *x == 0
}

impl History {
    pub fn is_empty(&self) -> bool {
        self.open.steps.is_empty() && self.open.carry.is_none() && self.sealing.is_empty() && self.dropped == 0
    }
    /// The last step recorded (open hour, else the newest frozen one).
    pub fn last(&self) -> Option<&HistStep> {
        self.open.last().or_else(|| self.sealing.last().and_then(|h| h.last()))
    }
}

/// The stamp of a step: `clock_ms` read after the document's GET, plus the
/// skew bound (`run_wall_ms` is the run's clock before its LIST: the
/// `StampEarly` mutant's).
pub fn stamp(clock_ms: u64, run_wall_ms: u64, skew_ms: u64, m: HistMutation) -> u64 {
    match m {
        HistMutation::StampEarly => run_wall_ms + skew_ms,
        HistMutation::NoSkew => clock_ms,
        _ => clock_ms + skew_ms,
    }
}

/// The history after a publication of `step` (its `at_ms` the `stamp`):
/// clamped to the last step, a later hour freezes the open one, at most one
/// step per `every_ms` and only on a change. Pure.
pub fn advance(prev: &History, mut step: HistStep, every_ms: u64, m: HistMutation) -> History {
    let mut h = prev.clone();
    let last = prev.last().cloned();
    if m != HistMutation::NoClamp {
        if let Some(l) = &last {
            step.at_ms = step.at_ms.max(l.at_ms);
        }
    }
    let hour = step.at_ms / HOUR_MS * HOUR_MS;
    if prev.open.steps.is_empty() && prev.open.carry.is_none() {
        // the first step of this document's history (or after every hour was frozen)
        h.open = HistHour { hour_ms: hour, carry: last.filter(|l| l.at_ms < hour), steps: vec![step] };
        return h;
    }
    if hour > prev.open.hour_ms {
        if !prev.open.steps.is_empty() {
            h.sealing.push(prev.open.clone());
            while h.sealing.len() > MAX_SEALING {
                let _ = h.sealing.remove(0);
                h.dropped += 1;
            }
        }
        h.open = HistHour { hour_ms: hour, carry: prev.open.last().cloned(), steps: vec![step] };
        return h;
    }
    match prev.open.last() {
        Some(l) if l.same_values(&step) => {}
        // Within `every` of the last step: not recorded. This also drops a
        // stamp before the last step, so the clamp and this check are one
        // rule ("never before the last step"); the NoClamp mutant removes
        // both (the model's `noClamp`: an earlier step appended).
        Some(l) if prev.open.steps.last().is_some() && step.at_ms < l.at_ms.saturating_add(every_ms) && !(m == HistMutation::NoClamp && step.at_ms < l.at_ms) => {}
        _ => h.open.steps.push(step),
    }
    h
}

/// `YYYY-MM-DDTHH` (UTC) of an hour.
pub fn hour_name(hour_ms: u64) -> String {
    let days = (hour_ms / 86_400_000) as i64;
    let hh = (hour_ms % 86_400_000) / HOUR_MS;
    // Howard Hinnant's civil_from_days
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mo <= 2);
    format!("{y:04}-{mo:02}-{d:02}T{hh:02}")
}

/// `{ctl}/watermark-history/{scope}/{YYYY-MM-DD}T{HH}.json`.
pub fn hour_key(ctl: &str, scope: &str, hour_ms: u64) -> String {
    join(ctl, &format!("watermark-history/{scope}/{}.json", hour_name(hour_ms)))
}

/// A sealed hour's object (create-only, never rewritten).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistObj {
    pub format: u32,
    pub scope: String,
    #[serde(flatten)]
    pub hour: HistHour,
}

pub fn obj_body(scope: &str, hour: &HistHour) -> Bytes {
    Bytes::from(serde_json::to_vec(&HistObj { format: otap_s3pq::proto::FORMAT_VERSION, scope: scope.to_string(), hour: hour.clone() }).expect("json"))
}

/// What sealing did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sealed {
    /// Hours now stored (written here, or already there with the same content).
    pub done: Vec<u64>,
    /// Hours already stored with other content (kept; counted).
    pub conflicts: Vec<u64>,
    /// Hours left for the next run (a lost answer, a 412 with nothing stored, an error).
    pub pending: Vec<String>,
}

/// Writes every frozen hour of `h` create-only. Never overwrites.
pub async fn seal<B: Bucket + ?Sized>(b: &B, ctl: &str, scope: &str, h: &History) -> Sealed {
    let mut out = Sealed::default();
    for hour in &h.sealing {
        let key = hour_key(ctl, scope, hour.hour_ms);
        let body = obj_body(scope, hour);
        match b.put(&key, body.clone(), Cond::Create, &BTreeMap::new()).await {
            Put::Ok(_) => out.done.push(hour.hour_ms),
            // Stored already (by another publisher, or our own write whose
            // answer was lost), or a create racing one that never completed:
            // only a read back decides.
            Put::Conflict | Put::Unknown(_) => match b.get(&key).await {
                Ok(Some((stored, _))) if stored == body => out.done.push(hour.hour_ms),
                Ok(Some(_)) => out.conflicts.push(hour.hour_ms),
                Ok(None) => out.pending.push(format!("{key}: not stored yet")),
                Err(e) => out.pending.push(format!("{key}: {e}")),
            },
        }
    }
    out
}

/// Drops the sealed hours from a document's `sealing` (for the second CAS):
/// `None` when none of them is there.
pub fn without_sealed(h: &History, sealed: &[u64]) -> Option<History> {
    if !h.sealing.iter().any(|x| sealed.contains(&x.hour_ms)) {
        return None;
    }
    let mut n = h.clone();
    n.sealing.retain(|x| !sealed.contains(&x.hour_ms));
    Some(n)
}

/// An answer "as of T".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsOf {
    pub step: HistStep,
    /// T's hour is sealed or frozen: no later write can change the answer.
    pub final_: bool,
}

/// "As of `t_ms`" from a scope's document history and its hour objects
/// (`get_hour(hour_ms)`: the sealed object of that hour, if any), looking
/// back at most `lookback_h` hours. `None`: no step at or before T within
/// reach (unknown).
pub fn as_of(doc: &History, t_ms: u64, lookback_h: u64, get_hour: impl Fn(u64) -> Option<HistHour>) -> Option<AsOf> {
    let t_hour = t_ms / HOUR_MS * HOUR_MS;
    let final_ = t_hour < doc.open.hour_ms;
    let in_doc = |h: u64| -> Option<HistHour> {
        if doc.open.hour_ms == h && (!doc.open.steps.is_empty() || doc.open.carry.is_some()) {
            Some(doc.open.clone())
        } else {
            doc.sealing.iter().find(|x| x.hour_ms == h).cloned()
        }
    };
    for back in 0..=lookback_h {
        let Some(h) = t_hour.checked_sub(back * HOUR_MS) else { break };
        if let Some(hour) = get_hour(h).or_else(|| in_doc(h)) {
            if let Some(s) = hour.at(t_ms) {
                return Some(AsOf { step: s.clone(), final_ });
            }
        }
    }
    None
}

/// The first step whose value for `signals` is above `x` (ns), and the last
/// one at or below it before that, from the document's history and the hour
/// objects from `from_ms` (the hour the search starts in) to the document's
/// open hour, at most `max_hours` of them. For an audit: every basis minted
/// before the "below" step's time is at or below `x`.
pub fn first_above(
    doc: &History,
    x: u64,
    signals: Option<&[String]>,
    from_ms: u64,
    max_hours: u64,
    get_hour: impl Fn(u64) -> Option<HistHour>,
) -> (Option<HistStep>, Option<HistStep>) {
    let mut below = None;
    let start = from_ms / HOUR_MS * HOUR_MS;
    let end = doc.open.hour_ms.max(start);
    let mut h = start;
    let mut n = 0;
    while h <= end && n < max_hours {
        let hour = get_hour(h).or_else(|| if doc.open.hour_ms == h { Some(doc.open.clone()) } else { doc.sealing.iter().find(|x| x.hour_ms == h).cloned() });
        if let Some(hour) = hour {
            for s in hour.carry.iter().chain(hour.steps.iter()) {
                if s.value_for(signals) > x {
                    return (below, Some(s.clone()));
                }
                below = Some(s.clone());
            }
        }
        h += HOUR_MS;
        n += 1;
    }
    (below, None)
}

/// The sealed hour objects of `scope` from `from_ms`'s hour through
/// `to_ms`'s, at most `max_hours` GETs (absent hours are absent).
pub async fn load_hours<B: Bucket + ?Sized>(b: &B, ctl: &str, scope: &str, from_ms: u64, to_ms: u64, max_hours: u64) -> Result<BTreeMap<u64, HistHour>, String> {
    let mut out = BTreeMap::new();
    let mut h = from_ms / HOUR_MS * HOUR_MS;
    let mut n = 0;
    while h <= to_ms && n < max_hours {
        let key = hour_key(ctl, scope, h);
        if let Some((body, _)) = b.get(&key).await? {
            let o: HistObj = serde_json::from_slice(&body).map_err(|e| format!("{key}: {e}"))?;
            let _ = out.insert(h, o.hour);
        }
        h += HOUR_MS;
        n += 1;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(at_ms: u64, ct: u64) -> HistStep {
        HistStep { at_ms, ct_ns: ct, ..Default::default() }
    }

    #[test]
    fn hour_names_are_utc_civil_hours() {
        assert_eq!(hour_name(0), "1970-01-01T00");
        assert_eq!(hour_name(1_790_000_000_000 / HOUR_MS * HOUR_MS), "2026-09-21T14");
        assert_eq!(hour_name(951_782_400_000 + 23 * HOUR_MS), "2000-02-29T23");
        assert_eq!(hour_key("r/_consumer", FLEET, 0), "r/_consumer/watermark-history/_fleet/1970-01-01T00.json");
    }

    #[test]
    fn steps_are_clamped_downsampled_and_frozen_by_the_hour() {
        let h0 = 10 * HOUR_MS;
        let mut h = advance(&History::default(), st(h0 + 1_000, 5), 60_000, HistMutation::None);
        assert_eq!((h.open.hour_ms, h.open.steps.len()), (h0, 1));
        // within `every`: not recorded; unchanged values: not recorded
        h = advance(&h, st(h0 + 30_000, 6), 60_000, HistMutation::None);
        h = advance(&h, st(h0 + 120_000, 5), 60_000, HistMutation::None);
        assert_eq!(h.open.steps.len(), 1);
        h = advance(&h, st(h0 + 120_000, 7), 60_000, HistMutation::None);
        assert_eq!(h.open.steps.last(), Some(&st(h0 + 120_000, 7)));
        // a clock behind: clamped to the last step
        h = advance(&h, st(h0 + 500, 9), 0, HistMutation::None);
        assert_eq!(h.open.steps.last(), Some(&st(h0 + 120_000, 9)));
        // the next hour freezes this one; the carry is its last step
        h = advance(&h, st(h0 + HOUR_MS + 5, 10), 60_000, HistMutation::None);
        assert_eq!(h.sealing.len(), 1);
        assert_eq!(h.sealing[0].steps.len(), 3);
        assert_eq!((h.open.hour_ms, h.open.carry.clone(), h.open.steps.len()), (h0 + HOUR_MS, Some(st(h0 + 120_000, 9)), 1));
        // a clock behind after the freeze never enters the frozen hour
        h = advance(&h, st(h0 + 1, 11), 0, HistMutation::None);
        assert_eq!((h.sealing[0].steps.len(), h.open.steps.last().unwrap().at_ms), (3, h0 + HOUR_MS + 5));
        // ...and the mutant appends a step before the last one
        let m = advance(&h, st(h0 + 1, 12), 0, HistMutation::NoClamp);
        assert_eq!(m.open.steps.last().unwrap().at_ms, h0 + 1);
        // a gap of hours: the open hour freezes once, empty hours have nothing
        h = advance(&h, st(h0 + 5 * HOUR_MS, 13), 60_000, HistMutation::None);
        assert_eq!(h.sealing.iter().map(|x| x.hour_ms).collect::<Vec<_>>(), vec![h0, h0 + HOUR_MS]);
    }

    #[test]
    fn as_of_is_the_last_step_at_or_before_t_final_once_frozen() {
        let h0 = 10 * HOUR_MS;
        let mut h = History::default();
        for (at, v) in [(h0 + 10, 1), (h0 + 70_000, 2), (h0 + HOUR_MS + 10, 3), (h0 + 3 * HOUR_MS + 10, 4)] {
            h = advance(&h, st(at, v), 60_000, HistMutation::None);
        }
        let none = |_: u64| -> Option<HistHour> { None };
        assert_eq!(as_of(&h, h0 + 9, 48, none), None, "before the first step: unknown");
        assert_eq!(as_of(&h, h0 + 10, 48, none).unwrap().step.ct_ns, 1, "a step counts at its own stamp");
        assert_eq!(as_of(&h, h0 + 69_999, 48, none).unwrap().step.ct_ns, 1);
        let a = as_of(&h, h0 + 70_000, 48, none).unwrap();
        assert_eq!((a.step.ct_ns, a.final_), (2, true));
        // hour h0+2 has no step: the carry of an earlier hour, by looking back
        assert_eq!(as_of(&h, h0 + 2 * HOUR_MS + 5, 48, none).unwrap().step.ct_ns, 3);
        assert_eq!(as_of(&h, h0 + 2 * HOUR_MS + 5, 0, none), None, "beyond the lookback: unknown");
        let o = as_of(&h, h0 + 3 * HOUR_MS + 20, 48, none).unwrap();
        assert_eq!((o.step.ct_ns, o.final_), (4, false), "the open hour: provisional");
        // from a sealed object, once the document dropped the hour
        let sealed = h.sealing[0].clone();
        let d = without_sealed(&h, &[h0]).unwrap();
        assert_eq!(as_of(&d, h0 + 70_000, 48, |x| (x == h0).then(|| sealed.clone())).unwrap().step.ct_ns, 2);
        // the audit search
        let (below, above) = first_above(&h, 2, None, h0, 10, none);
        assert_eq!((below.unwrap().ct_ns, above.unwrap().ct_ns), (2, 3));
        let (below, above) = first_above(&h, 9, None, h0, 10, none);
        assert_eq!((below.unwrap().ct_ns, above), (4, None));
    }

    #[test]
    fn a_step_value_for_signals_is_their_minimum_floored_by_the_scope() {
        let s = HistStep { at_ms: 0, ct_ns: 5, signals: BTreeMap::from([("logs".into(), 9), ("traces".into(), 7)]), unlisted_ns: 12 };
        let v = |x: &[&str]| s.value_for(Some(&x.iter().map(|y| y.to_string()).collect::<Vec<_>>()));
        assert_eq!((v(&["logs"]), v(&["logs", "traces"]), v(&["metrics_gauge"]), s.value_for(None)), (9, 7, 12, 5));
    }
}
