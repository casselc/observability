// Package runner drives the engine: per rule and per tick it reads the
// rule's state from the store, evaluates the windows that are due in order
// (bounded per tick), raises or resolves "cannot evaluate", commits the new
// state by compare-and-swap, and only then sends what the ledger says is
// due, committing the sink's answers the same way.
//
// Replicas. Every replica runs every rule; nothing is elected. A replica
// that lost the swap discards its work and re-reads. Evaluation is
// deterministic in the windows' rows, so the state either replica commits
// is the same, and the dedup keys are the same. The one thing two replicas
// can both do is send the same notice, with the same key: at-least-once,
// which the sink deduplicates. A replica skips a rule that another replica
// wrote less than MinGap ago; that saves queries and is not needed for
// safety.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/metrics"
	"github.com/casselc/observability/otel-chdb/alerts/internal/notify"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
)

// Evaluator runs one rule over one window.
type Evaluator interface {
	Evaluate(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result
}

// LateEvaluator is what the late-data check needs (D30): deltas between
// bases, and a window re-evaluated at a given basis. An Evaluator without it
// never checks for late data.
type LateEvaluator interface {
	Delta(ctx context.Context, r *rule.Rule, w rule.Window, from, to string) engine.Delta
	EvaluateAt(ctx context.Context, r *rule.Rule, w rule.Window, basis string) engine.Result
}

// Sender delivers notices.
type Sender interface {
	Send(ctx context.Context, sends []engine.Send, now time.Time) (notify.Answer, string)
}

// Runner is one replica.
type Runner struct {
	Rules       []*rule.Rule
	Store       store.Store
	Prefix      string // state keys: {Prefix}/{rule}.json
	Eval        Evaluator
	Sink        Sender
	Engine      engine.Config
	Writer      string        // this replica's id
	Tick        time.Duration // default 15s
	MaxPerTick  int           // windows per rule per tick (catch-up rate); default 20
	MinGap      time.Duration // skip a rule another replica wrote this recently; default Tick/2
	Concurrency int           // rules processed at once; default 4
	// MaxLatePerTick bounds the windows checked one by one for late data
	// per rule and tick (after a span check found rows); default 5.
	MaxLatePerTick int
	Now            func() time.Time
	Metrics        *metrics.Registry
	Log            *slog.Logger

	lastTick atomic.Int64 // unix ms of the last completed tick
	storeOK  atomic.Bool
}

// Init fills defaults and declares metrics.
func (r *Runner) Init() {
	r.Engine.Defaults()
	if r.Tick == 0 {
		r.Tick = 15 * time.Second
	}
	if r.MaxPerTick == 0 {
		r.MaxPerTick = 20
	}
	if r.MinGap == 0 {
		r.MinGap = r.Tick / 2
	}
	if r.Concurrency == 0 {
		r.Concurrency = 4
	}
	if r.MaxLatePerTick == 0 {
		r.MaxLatePerTick = 5
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
	if r.Metrics == nil {
		r.Metrics = metrics.New()
	}
	m := r.Metrics
	m.Counter("alr_evaluations_total", "Evaluation attempts by outcome (complete advances the rule; partial and unknown wait; error, timeout, refused, bad_result are failures).", "rule", "outcome")
	m.Counter("alr_windows_skipped_total", "Windows skipped because they were further behind than max_backlog.", "rule")
	m.Gauge("alr_complete_through_seconds", "The highest complete_through the query service reported to the rule.", "rule")
	m.Gauge("alr_evaluated_through_seconds", "End of the last window the rule evaluated.", "rule")
	m.Gauge("alr_lag_behind_complete_through_seconds", "complete_through minus the end of the last evaluated window: evaluable but not evaluated yet.", "rule")
	m.Gauge("alr_evaluation_delay_seconds", "Now minus the end of the last evaluated window.", "rule")
	m.Gauge("alr_pending_windows", "Windows that have ended and are not evaluated yet (waiting for completeness, failing, or catching up).", "rule")
	m.Gauge("alr_pending_window_outcome", "1 for the outcome of the last attempt on the window the rule is at.", "rule", "outcome")
	m.Gauge("alr_cannot_evaluate", "1 while the rule's cannot-evaluate alert fires.", "rule")
	m.Gauge("alr_alerts_firing", "Groups firing.", "rule")
	m.Gauge("alr_notices_undelivered", "Notices whose current phase the sink has not acknowledged.", "rule")
	m.Gauge("alr_notice_oldest_undelivered_seconds", "Age of the oldest unacknowledged notice phase.", "rule")
	m.Counter("alr_deliveries_total", "Notices sent, by the sink's answer (acked; ambiguous: no answer; 5xx; rejected: 4xx).", "outcome")
	m.Counter("alr_delivery_requests_total", "Requests to the sink, by answer.", "outcome")
	m.Counter("alr_state_writes_total", "State writes by outcome (ok, ambiguous_landed, conflict, ambiguous_lost, error).", "outcome")
	m.Counter("alr_state_errors_total", "State reads that failed or could not be decoded.", "op")
	m.Counter("alr_rules_skipped_total", "Rule ticks skipped because another replica wrote the state within min_gap.", "rule")
	m.Gauge("alr_last_tick_seconds", "Wall time of the last completed tick.")
	m.Counter("alr_late_checks_total", "Late-data delta queries by kind (span: every kept window at once; window: one) and outcome.", "rule", "kind", "outcome")
	m.Counter("alr_late_rows_total", "Late rows found in windows already evaluated (received after the basis they were evaluated at).", "rule")
	m.Gauge("alr_late_windows", "Evaluated windows kept for late-data checks.", "rule")
	m.Gauge("alr_late_episodes", "Late episodes and late-data notices raised (on_late reevaluate / page).", "rule")
}

func (r *Runner) key(name string) string {
	return strings.TrimRight(r.Prefix, "/") + "/" + name + ".json"
}

// Load reads a rule's state (nil, "" when there is none yet).
func (r *Runner) Load(ctx context.Context, name string) (*engine.State, string, error) {
	b, etag, err := r.Store.Get(ctx, r.key(name))
	if errors.Is(err, store.ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	var st engine.State
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, "", fmt.Errorf("decoding %s: %w", r.key(name), err)
	}
	return &st, etag, nil
}

func writeID(b []byte) string {
	var x struct {
		WriteID string `json:"write_id"`
	}
	_ = json.Unmarshal(b, &x)
	return x.WriteID
}

func (r *Runner) commit(ctx context.Context, name, etag string, st *engine.State) (string, bool) {
	st.UpdatedMs = r.Now().UnixMilli()
	e, out, err := store.Commit(ctx, r.Store, r.key(name), etag, r.Writer, st, writeID)
	r.Metrics.Inc("alr_state_writes_total", string(out))
	if !out.OK() {
		lvl := slog.LevelInfo
		if out != store.Conflict {
			lvl = slog.LevelWarn
		}
		r.Log.Log(ctx, lvl, "state not written", "rule", name, "outcome", out, "err", err)
		return "", false
	}
	return e, true
}

type attemptSig struct {
	outcome  engine.Outcome
	failures int
	wm       string
	lanes    string
}

func sig(a *engine.Attempt) attemptSig {
	if a == nil {
		return attemptSig{}
	}
	var ls []string
	for _, l := range append(append([]engine.Lane{}, a.Holding...), a.Stale...) {
		ls = append(ls, l.Lane)
	}
	return attemptSig{a.Outcome, a.Failures, a.WatermarkStatus, strings.Join(ls, ",")}
}

// ProcessRule is one tick of one rule.
func (r *Runner) ProcessRule(ctx context.Context, ru *rule.Rule) error {
	st, etag, err := r.Load(ctx, ru.Name)
	if err != nil {
		r.Metrics.Inc("alr_state_errors_total", "read")
		r.storeOK.Store(false)
		return err
	}
	r.storeOK.Store(true)
	now := r.Now()
	changed := false
	if st == nil {
		st, changed = engine.New(ru, now.UnixNano()), true
	} else {
		if st.Writer != r.Writer && now.UnixMilli()-st.UpdatedMs < r.MinGap.Milliseconds() && now.UnixMilli() >= st.UpdatedMs {
			r.Metrics.Inc("alr_rules_skipped_total", ru.Name)
			r.gauges(ru, st, now)
			return nil
		}
		changed = engine.Reconcile(ru, st, now.UnixNano(), now.UnixMilli())
	}
	if n := engine.SkipBacklog(ru, st, now.UnixNano(), r.Engine); n > 0 {
		changed = true
		r.Metrics.Add("alr_windows_skipped_total", float64(n), ru.Name)
		r.Log.Warn("windows skipped: further behind than max_backlog", "rule", ru.Name, "windows", n)
	}
	for i := 0; i < r.MaxPerTick && engine.Due(ru, st, r.Now().UnixNano()); i++ {
		w := ru.WindowEnding(st.NextEndNs)
		before := sig(st.Attempt)
		res := r.Eval.Evaluate(ctx, ru, w)
		r.Metrics.Inc("alr_evaluations_total", ru.Name, string(res.Outcome))
		if res.Outcome.IsFailure() {
			r.Log.Warn("evaluation failed", "rule", ru.Name, "window_end", time.Unix(0, w.ToNs).UTC(), "outcome", res.Outcome, "err", res.Err)
		}
		if engine.Apply(ru, st, res, r.Now().UnixMilli()) {
			changed = true
			continue
		}
		if sig(st.Attempt) != before {
			changed = true
		}
		break
	}
	if le, ok := r.Eval.(LateEvaluator); ok && engine.LateDue(ru, st, r.Now().UnixMilli()) {
		r.lateCheck(ctx, ru, st, le)
		changed = true
	}
	now = r.Now()
	if engine.UpdateMeta(ru, st, now.UnixNano(), now.UnixMilli(), r.Engine) {
		changed = true
		if st.Meta.Firing {
			r.Log.Warn("cannot evaluate", "rule", ru.Name, "reason", engine.Reason(st.Attempt))
		}
	}
	if changed {
		var ok bool
		if etag, ok = r.commit(ctx, ru.Name, etag, st); !ok {
			return nil // someone else's state is newer: start over next tick
		}
	}
	if sends := engine.DueSends(st, now.UnixMilli(), r.Engine); len(sends) > 0 {
		ans, detail := r.Sink.Send(ctx, sends, r.Now())
		r.Metrics.Inc("alr_delivery_requests_total", string(ans))
		r.Metrics.Add("alr_deliveries_total", float64(len(sends)), string(ans))
		if ans != notify.Acked {
			r.Log.Warn("delivery not acknowledged", "rule", ru.Name, "notices", len(sends), "answer", ans, "detail", detail)
		}
		engine.Delivered(st, sends, ans == notify.Acked, string(ans), r.Now().UnixMilli(), r.Engine)
		// Losing this swap loses only the record of the answer: the notices
		// are sent again with the same keys.
		if _, ok := r.commit(ctx, ru.Name, etag, st); !ok {
			return nil
		}
	}
	r.gauges(ru, st, r.Now())
	return nil
}

// lateCheck looks for late rows in the windows the rule kept (D30): one
// span delta over all of them from the lowest recorded basis to the latest;
// when it finds none, every window moves to the new basis. Otherwise each
// window (oldest first, MaxLatePerTick) gets its own delta from its own
// basis to the same new one, and a window with late rows is re-evaluated at
// it and handed to the engine's on_late policy. Anything that does not
// complete is tried again at the next check; a basis the service no longer
// accepts drops that window from the checks (counted).
func (r *Runner) lateCheck(ctx context.Context, ru *rule.Rule, st *engine.State, le LateEvaluator) {
	st.LateMs = r.Now().UnixMilli()
	pend := st.LatePending()
	tok := ""
	if w, from, ok := engine.LateSpan(ru, st); ok {
		d := le.Delta(ctx, ru, w, from, "latest")
		r.Metrics.Inc("alr_late_checks_total", ru.Name, "span", string(d.Outcome))
		switch {
		case d.Outcome == engine.Complete && d.Rows == 0:
			ends := make([]int64, 0, len(pend))
			for _, x := range pend {
				ends = append(ends, x.EndNs)
			}
			engine.LateChecked(st, ends, d.Basis, d.C)
			return
		case d.Outcome == engine.Complete:
			tok = d.Basis
		case d.Outcome == engine.Refused:
			r.Log.Warn("late-data check refused: the kept windows' bases are dropped", "rule", ru.Name, "err", d.Err)
			engine.LateUnverifiable(st)
			return
		default:
			return
		}
	}
	for i, w := range pend {
		if i >= r.MaxLatePerTick {
			break
		}
		win := ru.WindowEnding(w.EndNs)
		to := tok
		if to == "" {
			to = "latest"
		}
		d := le.Delta(ctx, ru, win, w.Basis, to)
		r.Metrics.Inc("alr_late_checks_total", ru.Name, "window", string(d.Outcome))
		if d.Outcome == engine.Refused {
			w.Basis, w.C = "", nil
			st.LateLost++
			continue
		}
		if d.Outcome != engine.Complete {
			return
		}
		tok = d.Basis
		if d.Rows == 0 {
			engine.LateChecked(st, []int64{w.EndNs}, d.Basis, d.C)
			continue
		}
		res := le.EvaluateAt(ctx, ru, win, d.Basis)
		r.Metrics.Inc("alr_evaluations_total", ru.Name, "late_"+string(res.Outcome))
		if res.Outcome != engine.Complete {
			return
		}
		r.Metrics.Add("alr_late_rows_total", float64(d.Rows), ru.Name)
		r.Log.Info("late rows in an evaluated window", "rule", ru.Name, "window_end", time.Unix(0, w.EndNs).UTC(), "rows", d.Rows, "policy", ru.OnLate)
		engine.LateApply(ru, st, w.EndNs, d, res, r.Now().UnixMilli())
	}
}

func (r *Runner) gauges(ru *rule.Rule, st *engine.State, now time.Time) {
	m, n := r.Metrics, ru.Name
	m.Set("alr_late_windows", float64(len(st.Recent)), n)
	m.Set("alr_late_episodes", float64(st.LateEpisodes), n)
	last := st.NextEndNs - int64(ru.Every)
	m.Set("alr_complete_through_seconds", float64(st.LastCompleteThroughNs)/1e9, n)
	m.Set("alr_evaluated_through_seconds", float64(last)/1e9, n)
	lag := float64(st.LastCompleteThroughNs-last) / 1e9
	if lag < 0 || st.LastCompleteThroughNs == 0 {
		lag = 0
	}
	m.Set("alr_lag_behind_complete_through_seconds", lag, n)
	m.Set("alr_evaluation_delay_seconds", float64(now.UnixNano()-last)/1e9, n)
	pending := 0.0
	if now.UnixNano() >= st.NextEndNs {
		pending = float64((now.UnixNano()-st.NextEndNs)/int64(ru.Every) + 1)
	}
	m.Set("alr_pending_windows", pending, n)
	for _, o := range []engine.Outcome{engine.Partial, engine.Unknown, engine.Failed, engine.Timeout, engine.Refused, engine.BadResult} {
		v := 0.0
		if st.Attempt != nil && st.Attempt.EndNs == st.NextEndNs && st.Attempt.Outcome == o {
			v = 1
		}
		m.Set("alr_pending_window_outcome", v, n, string(o))
	}
	b := 0.0
	if st.Meta.Firing {
		b = 1
	}
	m.Set("alr_cannot_evaluate", b, n)
	firing := 0
	for _, g := range st.Groups {
		if g.Phase == engine.Firing {
			firing++
		}
	}
	m.Set("alr_alerts_firing", float64(firing), n)
	u, oldest := engine.Undelivered(st, now.UnixMilli())
	m.Set("alr_notices_undelivered", float64(u), n)
	m.Set("alr_notice_oldest_undelivered_seconds", float64(oldest)/1e3, n)
}

// TickOnce processes every rule once.
func (r *Runner) TickOnce(ctx context.Context) {
	sem := make(chan struct{}, r.Concurrency)
	var wg sync.WaitGroup
	for _, ru := range r.Rules {
		wg.Add(1)
		sem <- struct{}{}
		go func(ru *rule.Rule) {
			defer func() { <-sem; wg.Done() }()
			if err := r.ProcessRule(ctx, ru); err != nil {
				r.Log.Warn("rule tick failed", "rule", ru.Name, "err", err)
			}
		}(ru)
	}
	wg.Wait()
	now := r.Now()
	r.lastTick.Store(now.UnixMilli())
	r.Metrics.Set("alr_last_tick_seconds", float64(now.UnixMilli())/1e3)
}

// Run ticks until ctx ends.
func (r *Runner) Run(ctx context.Context) {
	t := time.NewTicker(r.Tick)
	defer t.Stop()
	for {
		r.TickOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Handler serves /metrics, /healthz and /state/{rule}.
func (r *Runner) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.Metrics.Write(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		last := r.lastTick.Load()
		age := r.Now().UnixMilli() - last
		ok := last > 0 && age < 3*r.Tick.Milliseconds()+r.Engine.BackoffMax.Milliseconds() && r.storeOK.Load()
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": ok, "writer": r.Writer, "last_tick_age_s": float64(age) / 1e3,
			"store_ok": r.storeOK.Load(), "rules": len(r.Rules)})
	})
	mux.HandleFunc("/state/", func(w http.ResponseWriter, req *http.Request) {
		name := strings.TrimPrefix(req.URL.Path, "/state/")
		known := false
		for _, ru := range r.Rules {
			known = known || ru.Name == name
		}
		if !known {
			http.Error(w, "no such rule", http.StatusNotFound)
			return
		}
		st, _, err := r.Load(req.Context(), name)
		if err != nil || st == nil {
			http.Error(w, "no state", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	return mux
}
