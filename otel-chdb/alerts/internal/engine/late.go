package engine

// Late data (D30). A window is evaluated once, at a basis: the rows
// received before the basis's bound of their cluster. Rows of the same
// window received later are LATE: they were not in the verdict. The query
// service can count and evaluate them precisely, as a delta between two
// bases (the rows with basis_from <= received_at < basis). This file keeps
// what that needs (the windows evaluated within the rule's late_horizon,
// each with its basis and the groups that held) and decides what a late
// delta does, by the rule's on_late policy:
//
//   - reevaluate: the window is re-evaluated at the newer basis (one
//     query over the whole window, so no row is counted twice: never
//     "old verdict + delta"); a group that now holds and did not fires a
//     new episode marked late if its run of holding windows (with the late
//     rows) satisfies `for`, or, when the run reaches the latest evaluated
//     window and the group is live and pending, moves its start back (and
//     fires it as usual when that satisfies `for`);
//   - page: a window whose verdict changed sends one notice saying so;
//   - ignore: nothing is kept or checked.
//
// Safety, checked by the simulation and model/alertEvaluator.qnt:
//   - late data never resolves anything and never touches a firing group:
//     a firing alert resolves only by a later window evaluated in order;
//   - no row is counted twice: a window's basis only moves forward (a
//     delta from B1 to B2 sets it to B2, and the next starts at B2; the
//     service refuses a delta whose basis_from is above its basis), and a
//     group's late episode fires once per run.

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
)

// MaxRecent bounds the windows a rule keeps for late checks.
const MaxRecent = 360

// Recent is one evaluated window.
type Recent struct {
	EndNs int64 `json:"end_ns"`
	// Basis: the token it was evaluated (or last checked) at; "" when the
	// service gave none (it is then never checked). C: its bounds.
	Basis string            `json:"basis,omitempty"`
	C     map[string]uint64 `json:"c,omitempty"`
	// Holds: the groups that held, by group key.
	Holds     map[string]Hold `json:"holds,omitempty"`
	LateRows  int64           `json:"late_rows,omitempty"`
	Revisions int             `json:"revisions,omitempty"`
	// LateFired: groups a late episode was raised for with this window in
	// its run (so the same run never fires twice).
	LateFired map[string]bool `json:"late_fired,omitempty"`
}

// Hold is a group that held in a window.
type Hold struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

// Delta is the answer to a late-data query: the rows a delta admitted.
type Delta struct {
	Outcome Outcome // Complete: counted; Partial: try later (a regressed or unsettled basis); failures as for Result
	Rows    int64
	Basis   string // the delta's upper bound (a token)
	C       map[string]uint64
	Err     string
	// Reason is the service's refusal reason, if any.
	Reason string
}

// record keeps a window just evaluated (Apply).
func record(r *rule.Rule, st *State, end int64, res Result, holds map[string]rule.Row) {
	if r.OnLate == rule.LateIgnore {
		st.Recent = nil
		return
	}
	w := &Recent{EndNs: end, Basis: res.Basis, C: res.BasisC, Holds: map[string]Hold{}}
	for k, row := range holds {
		w.Holds[k] = Hold{Labels: row.Labels, Value: row.Value}
	}
	st.Recent = append(st.Recent, w)
	trimRecent(r, st, end)
}

func trimRecent(r *rule.Rule, st *State, lastEnd int64) {
	cut := 0
	for cut < len(st.Recent) && (st.Recent[cut].EndNs <= lastEnd-int64(r.LateHorizon) || len(st.Recent)-cut > MaxRecent) {
		cut++
	}
	st.Recent = st.Recent[cut:]
}

// LateDue: the rule has windows to check and did not check within
// late_every.
func LateDue(r *rule.Rule, st *State, nowMs int64) bool {
	if r.OnLate == rule.LateIgnore || nowMs-st.LateMs < r.LateEvery.D().Milliseconds() {
		return false
	}
	for _, w := range st.Recent {
		if w.Basis != "" {
			return true
		}
	}
	return false
}

// LateSpan is the one query that checks every kept window at once: the
// window from the first kept window's start to the last one's end, from
// the lowest recorded basis. ok is false when no recorded basis is at or
// below all the others for every cluster (then windows are checked one by
// one). A span delta of zero rows proves no window has late rows (each
// window's own delta is a subset of it); rows in it may still be rows a
// later window already counted, so a non-zero answer only says "look".
func LateSpan(r *rule.Rule, st *State) (rule.Window, string, bool) {
	var low *Recent
	var first, last int64
	for _, w := range st.Recent {
		if w.Basis == "" {
			continue
		}
		if low == nil {
			first = w.EndNs
		}
		last = w.EndNs
		if low == nil || below(w.C, low.C) {
			low = w
		}
	}
	if low == nil {
		return rule.Window{}, "", false
	}
	for _, w := range st.Recent {
		if w.Basis != "" && !below(low.C, w.C) {
			return rule.Window{}, "", false
		}
	}
	return rule.Window{FromNs: first - int64(r.Window), ToNs: last}, low.Basis, true
}

// below: a <= b for every cluster either names (a basis naming a cluster
// the other lacks is not comparable).
func below(a, b map[string]uint64) bool {
	if len(a) == 0 || len(a) != len(b) {
		return false
	}
	for c, v := range a {
		w, ok := b[c]
		if !ok || v > w {
			return false
		}
	}
	return true
}

// LateChecked records a check that found no late rows in windows (a span
// delta of zero, or one window's): each moves to the new basis, never
// back.
func LateChecked(st *State, ends []int64, tok string, c map[string]uint64) {
	for _, w := range st.Recent {
		if w.Basis == "" || !slices.Contains(ends, w.EndNs) || !below(w.C, c) {
			continue
		}
		w.Basis, w.C = tok, maps.Clone(c)
	}
}

// LateUnverifiable drops the kept windows' bases when the service no
// longer accepts them (a rotated key, a restart with a per-process key):
// they cannot be checked; counted in LateLost.
func LateUnverifiable(st *State) {
	for _, w := range st.Recent {
		if w.Basis != "" {
			w.Basis, w.C = "", nil
			st.LateLost++
		}
	}
}

// Pending is the windows to check one by one, oldest first.
func (st *State) LatePending() []*Recent {
	var out []*Recent
	for _, w := range st.Recent {
		if w.Basis != "" {
			out = append(out, w)
		}
	}
	return out
}

// LateApply records one window's late delta (rows > 0) and its
// re-evaluation at the delta's upper basis (res, Complete), by policy. It
// never resolves anything and never touches a firing group.
func LateApply(r *rule.Rule, st *State, end int64, d Delta, res Result, nowMs int64) {
	var w *Recent
	for _, x := range st.Recent {
		if x.EndNs == end {
			w = x
		}
	}
	if w == nil || !below(w.C, d.C) {
		return // gone, or the new basis is not above it: nothing may move back
	}
	holds := holdsOf(r, res.Rows)
	now := map[string]Hold{}
	for k, row := range holds {
		now[k] = Hold{Labels: row.Labels, Value: row.Value}
	}
	before := w.Holds
	w.Holds, w.Basis, w.C = now, d.Basis, maps.Clone(d.C)
	w.LateRows += d.Rows
	w.Revisions++
	st.LateRows += d.Rows
	switch r.OnLate {
	case rule.LatePage:
		if !sameKeys(before, now) {
			pageLate(r, st, w, before, now, d, nowMs)
		}
	case rule.LateReevaluate:
		var added []string
		for k := range now {
			if _, ok := before[k]; !ok {
				added = append(added, k)
			}
		}
		sort.Strings(added)
		lastEnd := st.NextEndNs - int64(r.Every)
		for _, g := range added {
			start, stop, run := runOf(r, st, end, g)
			if live := st.Groups[g]; live != nil && stop == lastEnd {
				// the late rows lengthen a live group's run
				if live.Phase == Pending && start < live.SinceNs {
					live.SinceNs = start
					if lastEnd-live.SinceNs >= int64(r.For) {
						live.Phase, live.ActiveNs = Firing, lastEnd
						live.Episode = fmt.Sprintf("%d", lastEnd/int64(time.Second))
						fire(r, st, g, live, nowMs)
						if n := st.Notices[NoticeKey(r.Name, g, live.Episode)]; n != nil {
							n.Annotations["fired_by_late_data"] = fmt.Sprintf("late rows in window ending %s", fmtNs(end))
						}
					}
				}
				continue
			}
			if stop-start < int64(r.For) {
				continue
			}
			fired := false
			for _, x := range run {
				fired = fired || x.LateFired[g]
			}
			if fired {
				continue // this run already paged late: never twice
			}
			lateEpisode(r, st, g, now[g], start, stop, end, d, nowMs)
			for _, x := range run {
				if x.LateFired == nil {
					x.LateFired = map[string]bool{}
				}
				x.LateFired[g] = true
			}
		}
	}
}

// runOf is the run of consecutive kept windows around end in which g
// holds: its first and last window ends.
func runOf(r *rule.Rule, st *State, end int64, g string) (int64, int64, []*Recent) {
	by := map[int64]*Recent{}
	for _, w := range st.Recent {
		by[w.EndNs] = w
	}
	step := int64(r.Every)
	start, stop := end, end
	run := []*Recent{by[end]}
	for {
		w := by[start-step]
		if w == nil {
			break
		}
		if _, ok := w.Holds[g]; !ok {
			break
		}
		start -= step
		run = append(run, w)
	}
	for {
		w := by[stop+step]
		if w == nil {
			break
		}
		if _, ok := w.Holds[g]; !ok {
			break
		}
		stop += step
		run = append(run, w)
	}
	return start, stop, run
}

func sameKeys(a, b map[string]Hold) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// lateEpisode raises a late episode: sent firing, then resolved (the
// windows are past), under its own key (episode "late-{window end}").
func lateEpisode(r *rule.Rule, st *State, g string, h Hold, start, stop, end int64, d Delta, nowMs int64) {
	grp := &Group{Labels: h.Labels, Phase: Firing, SinceNs: start, ActiveNs: stop, Value: h.Value,
		Episode: fmt.Sprintf("late-%d", end/int64(time.Second))}
	fire(r, st, g, grp, nowMs)
	n := st.Notices[NoticeKey(r.Name, g, grp.Episode)]
	n.Labels["alert_late"] = "true"
	n.Annotations["late"] = fmt.Sprintf("late data: %d rows received after window [%s, %s) was evaluated now make it meet the condition",
		d.Rows, fmtNs(end-int64(r.Window)), fmtNs(end))
	n.Want, n.EndsNs, n.WantMs = Resolved, stop+int64(r.Every), nowMs
	st.LateEpisodes++
}

// pageLate sends one "late data changed window W" notice (policy page).
func pageLate(r *rule.Rule, st *State, w *Recent, before, now map[string]Hold, d Delta, nowMs int64) {
	ep := fmt.Sprintf("%d.%d", w.EndNs/int64(time.Second), w.Revisions)
	key := NoticeKey(r.Name, "late_data", ep)
	lb := map[string]string{"alertname": "AlertLateData", "severity": r.Severity, "alert_rule": r.Name, "alert_kind": "late_data",
		"alert_episode": ep, "alert_late": "true"}
	for k, v := range r.Labels {
		if _, ok := lb[k]; !ok {
			lb[k] = v
		}
	}
	names := func(m map[string]Hold, not map[string]Hold) string {
		var out []string
		for k, h := range m {
			if _, ok := not[k]; !ok {
				out = append(out, fmt.Sprint(h.Labels))
			}
		}
		sort.Strings(out)
		return fmt.Sprint(out)
	}
	an := map[string]string{
		"summary":           fmt.Sprintf("late data changed window [%s, %s) of alert rule %s", fmtNs(w.EndNs-int64(r.Window)), fmtNs(w.EndNs), r.Name),
		"window":            fmt.Sprintf("[%s, %s)", fmtNs(w.EndNs-int64(r.Window)), fmtNs(w.EndNs)),
		"late_rows":         fmt.Sprint(d.Rows),
		"now_holding":       names(now, before),
		"no_longer_holding": names(before, now),
		"dedup_key":         key,
		"description":       "Rows received after this window was evaluated change which groups meet the condition. No alert state was changed.",
	}
	st.Notices[key] = &Notice{Key: key, Kind: "late_data", Labels: lb, Annotations: an, StartsNs: w.EndNs,
		EndsNs: w.EndNs + int64(r.Every), Want: Resolved, CreatedMs: nowMs, WantMs: nowMs}
	st.LateEpisodes++
}
