// Package engine is the evaluator's state machine, with no I/O: which window
// a rule evaluates next, what an evaluation's outcome does to the rule's
// groups, when a rule pages that it cannot be evaluated, and the
// notification ledger (what the sink must still be told).
//
// Everything that decides a group's phase is a function of the rule and the
// rows of complete windows, evaluated in window order, so two replicas that
// evaluate the same window reach the same state and the same dedup keys;
// the state store's compare-and-swap picks one of them (runner, README §4).
//
// STPA R-S3 / AMBIGUITY.md X5, X6:
//   - a window is evaluated only when the query service labels it complete;
//     partial, unknown, an error, a timeout and a refusal all leave the
//     window unevaluated and are recorded as an attempt;
//   - "no rows" means "no group holds" only in a complete window;
//   - a window that stays unevaluated past a bound, and repeated failures,
//     page as "cannot evaluate", with the reason;
//   - a notice stays in the ledger until the sink answers 2xx for its
//     resolution; no answer is never read as delivered.
package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
)

// Outcome of one evaluation attempt.
type Outcome string

// Outcomes. Only Complete advances a rule.
const (
	Complete  Outcome = "complete"
	Partial   Outcome = "partial"    // the window extends past complete_through
	Unknown   Outcome = "unknown"    // complete_through is unknown (watermark stale, missing, unreadable)
	Failed    Outcome = "error"      // the query service answered 5xx, or no answer
	Timeout   Outcome = "timeout"    // no answer within the evaluation timeout
	Refused   Outcome = "refused"    // 4xx: the statement or the identity is refused; it will not fix itself
	BadResult Outcome = "bad_result" // a complete answer the rule cannot read (a non-numeric value, too many rows)
)

// IsFailure: the evaluation failed (as opposed to waiting for data).
func (o Outcome) IsFailure() bool {
	return o == Failed || o == Timeout || o == Refused || o == BadResult
}

// Lane is a lane holding complete_through back, as the query service names it.
type Lane struct {
	Lane string  `json:"lane"`
	LagS float64 `json:"lag_s"`
}

// Result is one evaluation's answer.
type Result struct {
	Outcome Outcome
	Rows    []rule.Row // Complete only
	Err     string
	// What the query service said about completeness (for reasons).
	CompleteThroughNs int64 // 0: unknown
	WatermarkStatus   string
	WatermarkAgeS     float64
	Holding, Stale    []Lane
	RequestID         string
	// Basis is the basis token the answer was computed at (D30; "" from a
	// service without bases), BasisC its bounds ({cluster|"*": ns}).
	Basis  string
	BasisC map[string]uint64
}

// Group is one label set of a rule that currently holds.
type Group struct {
	Labels   map[string]string `json:"labels"`
	Phase    string            `json:"phase"`     // pending | firing
	SinceNs  int64             `json:"since_ns"`  // end of the first window it held in
	ActiveNs int64             `json:"active_ns"` // firing since (window end)
	Episode  string            `json:"episode,omitempty"`
	Value    float64           `json:"value"`
}

// Phases.
const (
	Pending  = "pending"
	Firing   = "firing"
	Resolved = "resolved"
)

// Notice is one alert episode in the notification ledger. Its Key is the
// dedup key: stable per (rule, group, episode), the same on every replica
// and every re-send.
type Notice struct {
	Key         string            `json:"key"`
	Kind        string            `json:"kind"` // rule | cannot_evaluate
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsNs    int64             `json:"starts_ns"`
	EndsNs      int64             `json:"ends_ns,omitempty"` // set once resolved
	Want        string            `json:"want"`              // firing | resolved: what the sink must know
	Acked       string            `json:"acked,omitempty"`   // what the sink acknowledged (2xx)
	AckedMs     int64             `json:"acked_ms,omitempty"`
	FiringAckMs int64             `json:"firing_ack_ms,omitempty"`
	Attempts    int               `json:"attempts,omitempty"` // failed or unanswered sends since the last 2xx
	Sends       int               `json:"sends"`              // every send
	LastMs      int64             `json:"last_ms,omitempty"`
	LastOutcome string            `json:"last_outcome,omitempty"`
	NextMs      int64             `json:"next_ms,omitempty"` // backoff
	CreatedMs   int64             `json:"created_ms"`
	WantMs      int64             `json:"want_ms"` // when Want last changed
}

// Attempt records the tries on the window a rule is stuck at.
type Attempt struct {
	EndNs             int64   `json:"end_ns"`
	FirstMs           int64   `json:"first_ms"`
	LastMs            int64   `json:"last_ms"`
	Outcome           Outcome `json:"outcome"`
	Tries             int     `json:"tries"`
	Failures          int     `json:"failures"` // consecutive failures (not waits)
	Err               string  `json:"err,omitempty"`
	WatermarkStatus   string  `json:"watermark_status,omitempty"`
	WatermarkAgeS     float64 `json:"watermark_age_s,omitempty"`
	CompleteThroughNs int64   `json:"complete_through_ns,omitempty"`
	Holding           []Lane  `json:"holding,omitempty"`
	Stale             []Lane  `json:"stale,omitempty"`
	RequestID         string  `json:"request_id,omitempty"`
}

// Meta is the rule's own "cannot evaluate" alert.
type Meta struct {
	Firing bool   `json:"firing"`
	Key    string `json:"key,omitempty"`
}

// State is everything a rule keeps between evaluations, one document per
// rule in the store.
type State struct {
	Format    int                `json:"format"`
	Rule      string             `json:"rule"`
	Spec      string             `json:"spec"`
	NextEndNs int64              `json:"next_end_ns"` // the end of the next window to evaluate
	Groups    map[string]*Group  `json:"groups"`
	Notices   map[string]*Notice `json:"notices"`
	Attempt   *Attempt           `json:"attempt,omitempty"`
	Meta      Meta               `json:"meta"`
	Evaluated int64              `json:"evaluated"` // windows evaluated
	Skipped   int64              `json:"skipped"`   // windows skipped past the backlog bound
	// LastCompleteThroughNs is the highest complete_through the query
	// service reported to this rule.
	LastCompleteThroughNs int64 `json:"last_complete_through_ns"`
	LastEvalMs            int64 `json:"last_eval_ms,omitempty"` // wall time of the last complete evaluation
	// Recent: the windows evaluated within the rule's late_horizon, each
	// with the basis it was evaluated (or last checked for late data) at
	// and the groups that held (late.go, D30).
	Recent []*Recent `json:"recent,omitempty"`
	// LateMs: when late data was last checked for (wall ms).
	LateMs int64 `json:"late_ms,omitempty"`
	// LateRows: late rows found in evaluated windows; LateEpisodes: late
	// episodes and late-data notices raised; LateLost: windows dropped
	// before they could be checked (their basis no longer verifies).
	LateRows     int64 `json:"late_rows,omitempty"`
	LateEpisodes int64 `json:"late_episodes,omitempty"`
	LateLost     int64 `json:"late_lost,omitempty"`
	// Writer bookkeeping: who wrote this version, and a random id that
	// tells a writer whether an unanswered write of its own landed.
	Writer    string `json:"writer"`
	Seq       int64  `json:"seq"`
	WriteID   string `json:"write_id"`
	UpdatedMs int64  `json:"updated_ms"`
}

// Config is the engine's policy.
type Config struct {
	CannotEvaluateAfter time.Duration // default 5m (DECISIONS: alerts ~5 min behind accepted)
	FailuresToPage      int           // default 3
	MaxBacklog          time.Duration // windows further behind than this are skipped (default 6h)
	Refresh             time.Duration // re-send a firing notice this often (Alertmanager needs it; 0: never)
	HoldResolved        time.Duration // send a resolution only this long after the firing was acknowledged
	BackoffBase         time.Duration // first retry after a failed or unanswered send
	BackoffMax          time.Duration
}

// Defaults fills zero fields.
func (c *Config) Defaults() {
	if c.CannotEvaluateAfter == 0 {
		c.CannotEvaluateAfter = 5 * time.Minute
	}
	if c.FailuresToPage == 0 {
		c.FailuresToPage = 3
	}
	if c.MaxBacklog == 0 {
		c.MaxBacklog = 6 * time.Hour
	}
	if c.BackoffBase == 0 {
		c.BackoffBase = 2 * time.Second
	}
	if c.BackoffMax == 0 {
		c.BackoffMax = 2 * time.Minute
	}
}

// New is the state of a rule seen for the first time: it starts at the
// latest window that has ended by now (history before it is not paged).
func New(r *rule.Rule, nowNs int64) *State {
	return &State{Format: 1, Rule: r.Name, Spec: r.Spec(), NextEndNs: r.AlignDown(nowNs),
		Groups: map[string]*Group{}, Notices: map[string]*Notice{}}
}

// Reconcile adapts a stored state to the rule as loaded now: a changed spec
// drops the groups (their history belonged to another rule; firing ones are
// resolved) and realigns the next window. It reports whether it changed
// anything.
func Reconcile(r *rule.Rule, st *State, nowNs, nowMs int64) bool {
	if st.Groups == nil {
		st.Groups = map[string]*Group{}
	}
	if st.Notices == nil {
		st.Notices = map[string]*Notice{}
	}
	if st.Spec == r.Spec() {
		return false
	}
	end := r.AlignDown(st.NextEndNs)
	if end < st.NextEndNs {
		end += int64(r.Every)
	}
	for k, g := range st.Groups {
		if g.Phase == Firing {
			resolve(st, g, st.NextEndNs, nowMs, "the rule changed")
		}
		delete(st.Groups, k)
	}
	st.Spec, st.NextEndNs, st.Attempt, st.Recent = r.Spec(), end, nil, nil
	return true
}

// Due: the next window has ended, and its lateness allowance has passed.
func Due(r *rule.Rule, st *State, nowNs int64) bool { return st.NextEndNs+int64(r.Lateness) <= nowNs }

// SkipBacklog moves the next window forward when it is further behind than
// the backlog bound, and returns how many windows it skipped (0 normally).
// Catching up is otherwise in order, one window after another.
func SkipBacklog(r *rule.Rule, st *State, nowNs int64, c Config) int64 {
	if nowNs-st.NextEndNs <= int64(c.MaxBacklog) {
		return 0
	}
	next := r.AlignDown(nowNs - int64(c.MaxBacklog))
	if next <= st.NextEndNs {
		return 0
	}
	n := (next - st.NextEndNs) / int64(r.Every)
	st.NextEndNs, st.Skipped, st.Attempt = next, st.Skipped+n, nil
	return n
}

// Apply records the result of evaluating the window ending at st.NextEndNs.
// Only a Complete result changes groups and advances the rule; anything
// else is an attempt on the same window.
func Apply(r *rule.Rule, st *State, res Result, nowMs int64) (advanced bool) {
	if res.CompleteThroughNs > st.LastCompleteThroughNs {
		st.LastCompleteThroughNs = res.CompleteThroughNs
	}
	end := st.NextEndNs
	if res.Outcome != Complete {
		a := st.Attempt
		if a == nil || a.EndNs != end {
			a = &Attempt{EndNs: end, FirstMs: nowMs}
			st.Attempt = a
		}
		a.LastMs, a.Outcome, a.Tries, a.Err = nowMs, res.Outcome, a.Tries+1, res.Err
		a.WatermarkStatus, a.WatermarkAgeS, a.CompleteThroughNs = res.WatermarkStatus, res.WatermarkAgeS, res.CompleteThroughNs
		a.Holding, a.Stale, a.RequestID = res.Holding, res.Stale, res.RequestID
		if res.Outcome.IsFailure() {
			a.Failures++
		} else {
			a.Failures = 0
		}
		return false
	}
	holds := holdsOf(r, res.Rows)
	for k, g := range st.Groups {
		if _, ok := holds[k]; ok {
			continue
		}
		if g.Phase == Firing {
			resolve(st, g, end, nowMs, "")
		}
		delete(st.Groups, k)
	}
	keys := make([]string, 0, len(holds))
	for k := range holds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		row := holds[k]
		g := st.Groups[k]
		if g == nil {
			g = &Group{Labels: row.Labels, Phase: Pending, SinceNs: end}
			st.Groups[k] = g
		}
		g.Value = row.Value
		if g.Phase == Pending && end-g.SinceNs >= int64(r.For) {
			g.Phase, g.ActiveNs = Firing, end
			g.Episode = fmt.Sprintf("%d", end/int64(time.Second))
			fire(r, st, k, g, nowMs)
		}
	}
	record(r, st, end, res, holds)
	st.NextEndNs += int64(r.Every)
	st.Evaluated++
	st.Attempt = nil
	st.LastEvalMs = nowMs
	return true
}

// holdsOf is the groups a complete answer's rows make hold.
func holdsOf(r *rule.Rule, rows []rule.Row) map[string]rule.Row {
	holds := map[string]rule.Row{}
	for _, row := range rows {
		if r.Condition.Holds(row.Value) {
			holds[rule.GroupKey(row.Labels)] = row
		}
	}
	if len(rows) == 0 && r.OnNoRows == "fire" {
		holds[rule.GroupKey(map[string]string{})] = rule.Row{Labels: map[string]string{}}
	}
	return holds
}

// NoticeKey is the dedup key of a group's episode.
func NoticeKey(ruleName, group, episode string) string {
	return ruleName + "/" + group + "/" + episode
}

func fire(r *rule.Rule, st *State, gk string, g *Group, nowMs int64) {
	lb := map[string]string{}
	for k, v := range r.Labels {
		lb[k] = v
	}
	for k, v := range g.Labels {
		if rule.Reserved[k] {
			k = "exported_" + k
		}
		lb[k] = v
	}
	lb["alertname"], lb["severity"], lb["alert_rule"], lb["alert_episode"], lb["alert_kind"] = r.Name, r.Severity, r.Name, g.Episode, "rule"
	w := r.WindowEnding(g.ActiveNs)
	an := map[string]string{}
	for k, v := range r.Annotations {
		an[k] = expand(v, g.Labels, g.Value)
	}
	an["value"] = fmt.Sprintf("%g", g.Value)
	an["window"] = fmt.Sprintf("[%s, %s)", fmtNs(w.FromNs), fmtNs(w.ToNs))
	an["evaluated_at"] = time.UnixMilli(nowMs).UTC().Format(time.RFC3339)
	if lag := time.Duration(nowMs*int64(time.Millisecond) - g.ActiveNs); lag > 0 {
		an["evaluation_delay"] = lag.Round(time.Second).String()
	}
	key := NoticeKey(r.Name, gk, g.Episode)
	an["dedup_key"] = key
	st.Notices[key] = &Notice{Key: key, Kind: "rule", Labels: lb, Annotations: an, StartsNs: g.ActiveNs,
		Want: Firing, CreatedMs: nowMs, WantMs: nowMs}
}

func resolve(st *State, g *Group, endNs, nowMs int64, why string) {
	key := NoticeKey(st.Rule, rule.GroupKey(g.Labels), g.Episode)
	n := st.Notices[key]
	if n == nil { // acknowledged-resolved notices are deleted; a firing one never is
		return
	}
	n.Want, n.EndsNs, n.WantMs = Resolved, endNs, nowMs
	if why != "" {
		n.Annotations["resolved_because"] = why
	}
	if n.NextMs > nowMs { // a resolution is news: don't wait out a firing's backoff
		n.NextMs = nowMs
	}
}

// expand substitutes {{value}} and {{labels.NAME}} (no template engine: a
// rule file is data, and annotations are not code).
func expand(s string, labels map[string]string, v float64) string {
	s = strings.ReplaceAll(s, "{{value}}", fmt.Sprintf("%g", v))
	for k, x := range labels {
		s = strings.ReplaceAll(s, "{{labels."+k+"}}", x)
	}
	return s
}

func fmtNs(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339) }

// ---- cannot evaluate --------------------------------------------------------

// Stuck says whether the rule cannot be evaluated: the window it is at
// ended longer than the bound ago and its last attempt did not complete, or
// it failed FailuresToPage times in a row, or it was refused.
func Stuck(r *rule.Rule, st *State, nowNs int64, c Config) bool {
	a := st.Attempt
	if a == nil || a.EndNs != st.NextEndNs {
		return false
	}
	after, fails := c.CannotEvaluateAfter, c.FailuresToPage
	if r.CannotEvaluateAfter > 0 {
		after = r.CannotEvaluateAfter.D()
	}
	if r.FailuresToPage > 0 {
		fails = r.FailuresToPage
	}
	return a.Outcome == Refused || a.Failures >= fails || nowNs-st.NextEndNs > int64(after)
}

// Reason says why, in words, from the last attempt.
func Reason(a *Attempt) string {
	if a == nil {
		return ""
	}
	var parts []string
	switch a.Outcome {
	case Partial:
		parts = append(parts, "the window extends past complete_through "+fmtNs(a.CompleteThroughNs))
	case Unknown:
		parts = append(parts, "completeness unknown: watermark "+orDash(a.WatermarkStatus))
		if a.WatermarkStatus == "stale" {
			parts[0] += fmt.Sprintf(" (published %.0fs ago)", a.WatermarkAgeS)
		}
	default:
		parts = append(parts, fmt.Sprintf("evaluation %s (%d in a row): %s", a.Outcome, a.Failures, a.Err))
	}
	if ls := laneList(a.Holding); ls != "" {
		parts = append(parts, "lanes behind: "+ls)
	}
	held := map[string]bool{}
	for _, l := range a.Holding {
		held[l.Lane] = true
	}
	var stale []Lane
	for _, l := range a.Stale {
		if !held[l.Lane] {
			stale = append(stale, l)
		}
	}
	if len(a.Stale) > 0 && len(stale) == 0 {
		parts = append(parts, "the lanes behind are stale (no heartbeat: an edge or its store path is down)")
	}
	if ls := laneList(stale); ls != "" {
		parts = append(parts, "stale lanes: "+ls)
	}
	if cs := Clusters(a); len(cs) > 0 {
		parts = append(parts, "clusters: "+strings.Join(cs, ", "))
	}
	return strings.Join(parts, "; ")
}

// Clusters are the clusters of the lanes holding complete_through back or
// stale (a lane is {cluster}/{producer}/{signal}).
func Clusters(a *Attempt) []string {
	set := map[string]bool{}
	for _, l := range append(append([]Lane{}, a.Holding...), a.Stale...) {
		c, _, _ := strings.Cut(l.Lane, "/")
		set[c] = true
	}
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func laneList(ls []Lane) string {
	var s []string
	for _, l := range ls {
		s = append(s, fmt.Sprintf("%s (%.0fs behind)", l.Lane, l.LagS))
	}
	return strings.Join(s, ", ")
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// UpdateMeta raises or resolves the rule's "cannot evaluate" alert, and
// keeps its reason current while it fires. It reports whether it changed
// the state.
func UpdateMeta(r *rule.Rule, st *State, nowNs, nowMs int64, c Config) bool {
	stuck := Stuck(r, st, nowNs, c)
	switch {
	case stuck && !st.Meta.Firing:
		a := st.Attempt
		key := NoticeKey(r.Name, "cannot_evaluate", fmt.Sprintf("%d", a.EndNs/int64(time.Second)))
		lb := map[string]string{"alertname": "AlertCannotEvaluate", "severity": "page", "alert_rule": r.Name,
			"alert_kind": "cannot_evaluate", "alert_episode": fmt.Sprintf("%d", a.EndNs/int64(time.Second))}
		for k, v := range r.Labels {
			if _, ok := lb[k]; !ok {
				lb[k] = v
			}
		}
		st.Notices[key] = &Notice{Key: key, Kind: "cannot_evaluate", Labels: lb, Annotations: metaAnnotations(r, st, key),
			StartsNs: nowNs, Want: Firing, CreatedMs: nowMs, WantMs: nowMs}
		st.Meta = Meta{Firing: true, Key: key}
		return true
	case stuck && st.Meta.Firing:
		if n := st.Notices[st.Meta.Key]; n != nil {
			an := metaAnnotations(r, st, st.Meta.Key)
			if an["reason"] != n.Annotations["reason"] {
				n.Annotations = an
				return true
			}
		}
		return false
	case !stuck && st.Meta.Firing:
		if n := st.Notices[st.Meta.Key]; n != nil {
			n.Want, n.EndsNs, n.WantMs = Resolved, nowNs, nowMs
			n.Annotations["resolved_because"] = "the rule evaluated its window"
			n.NextMs = 0
		}
		st.Meta = Meta{}
		return true
	}
	return false
}

func metaAnnotations(r *rule.Rule, st *State, key string) map[string]string {
	a := st.Attempt
	w := r.WindowEnding(a.EndNs)
	an := map[string]string{
		"summary":           fmt.Sprintf("alert rule %s cannot evaluate window [%s, %s)", r.Name, fmtNs(w.FromNs), fmtNs(w.ToNs)),
		"reason":            Reason(a),
		"outcome":           string(a.Outcome),
		"window":            fmt.Sprintf("[%s, %s)", fmtNs(w.FromNs), fmtNs(w.ToNs)),
		"watermark_status":  a.WatermarkStatus,
		"clusters":          strings.Join(Clusters(a), ","),
		"attempts":          fmt.Sprintf("%d", a.Tries),
		"dedup_key":         key,
		"description":       "The rule is NOT being evaluated: its alerts may be missing. Nothing about this window is known to be OK.",
		"first_attempt_at":  time.UnixMilli(a.FirstMs).UTC().Format(time.RFC3339),
		"complete_through":  "",
		"last_request_id":   a.RequestID,
		"windows_evaluated": fmt.Sprintf("%d", st.Evaluated),
	}
	if a.CompleteThroughNs > 0 {
		an["complete_through"] = fmtNs(a.CompleteThroughNs)
	}
	return an
}

// ---- the ledger ---------------------------------------------------------------

// Send is one notice to send, and in which phase.
type Send struct {
	Notice *Notice
	Phase  string // firing | resolved
}

// DueSends lists what the sink must be told now, in key order:
//   - a notice whose wanted phase the sink has not acknowledged;
//   - a firing notice last acknowledged Refresh ago (Alertmanager forgets an
//     alert that is not re-sent);
//   - a resolution is sent only after its firing was acknowledged, and
//     HoldResolved after that (an Alertmanager that never notified the firing
//     does not notify a resolution: an episode that fired and resolved
//     during a catch-up would otherwise never reach anyone).
func DueSends(st *State, nowMs int64, c Config) []Send {
	var out []Send
	keys := make([]string, 0, len(st.Notices))
	for k := range st.Notices {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		n := st.Notices[k]
		if n.NextMs > nowMs {
			continue
		}
		switch {
		case n.Acked != Firing && n.Acked != Resolved:
			out = append(out, Send{n, Firing})
		case n.Want == Firing && c.Refresh > 0 && nowMs-n.AckedMs >= c.Refresh.Milliseconds():
			out = append(out, Send{n, Firing})
		case n.Want == Resolved && n.Acked == Firing && nowMs-n.FiringAckMs >= c.HoldResolved.Milliseconds():
			out = append(out, Send{n, Resolved})
		}
	}
	return out
}

// Delivered records the sink's answer to sends. Only a definite 2xx
// (ok=true) moves a notice forward; a notice whose resolution the sink
// acknowledged leaves the ledger. Anything else (no answer, a timeout, a
// 5xx, a 4xx) counts as not delivered and is retried with the same key
// after a backoff.
func Delivered(st *State, sends []Send, ok bool, outcome string, nowMs int64, c Config) {
	for _, s := range sends {
		n := st.Notices[s.Notice.Key]
		if n == nil {
			continue
		}
		n.Sends++
		n.LastMs, n.LastOutcome = nowMs, outcome
		if !ok {
			n.Attempts++
			d := c.BackoffBase << min(n.Attempts-1, 16)
			if d > c.BackoffMax || d <= 0 {
				d = c.BackoffMax
			}
			n.NextMs = nowMs + d.Milliseconds()
			continue
		}
		n.Attempts, n.NextMs = 0, 0
		n.Acked, n.AckedMs = s.Phase, nowMs
		if s.Phase == Firing && n.FiringAckMs == 0 {
			n.FiringAckMs = nowMs
		}
		if s.Phase == Resolved {
			delete(st.Notices, n.Key)
		}
	}
}

// Undelivered counts notices whose wanted phase the sink has not
// acknowledged, and the age of the oldest (ms).
func Undelivered(st *State, nowMs int64) (n int, oldestMs int64) {
	for _, x := range st.Notices {
		if x.Acked == x.Want {
			continue
		}
		n++
		if age := nowMs - x.WantMs; age > oldestMs {
			oldestMs = age
		}
	}
	return n, oldestMs
}

// SetWrite stamps the writer and a write id (store.Doc).
func (s *State) SetWrite(writer, id string) { s.Writer, s.WriteID = writer, id; s.Seq++ }
