package runner

// Run's own timer paths in fake time (testing/synctest): the real
// time.NewTicker, the replicas' min_gap skip, the cannot-evaluate page
// after CannotEvaluateAfter of a stalled pipeline, and shutdown. The rapid
// simulations drive ProcessRule by hand and never execute Run
// (research/go-verification.md §5).

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
)

// pipeline is a query service whose complete_through follows the fake
// clock lag behind, unless stalled.
type pipeline struct {
	fakeQS
	lag     time.Duration
	stalled bool
}

func (p *pipeline) Evaluate(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result {
	p.mu.Lock()
	if !p.stalled {
		p.ct = time.Now().Add(-p.lag).UnixNano()
	}
	p.mu.Unlock()
	return p.fakeQS.Evaluate(ctx, r, w)
}

func (p *pipeline) stall(on bool) {
	p.mu.Lock()
	p.stalled = on
	p.mu.Unlock()
}

// runReplicas starts n replicas' Run loops on one store; stop cancels them
// and returns once every loop has returned.
func runReplicas(t *testing.T, n int, st store.Store, qs Evaluator, sink Sender, tune func(*Runner)) (reps []*Runner, stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range n {
		r := &Runner{Rules: []*rule.Rule{testRule(t)}, Store: st, Prefix: "alr/state", Eval: qs, Sink: sink,
			Writer: "r" + string(rune('1'+i)), Tick: 10 * time.Second, MaxPerTick: 5, Log: quiet,
			Engine: engine.Config{CannotEvaluateAfter: 3 * time.Minute, HoldResolved: 30 * time.Second, Refresh: time.Minute}}
		if tune != nil {
			tune(r)
		}
		r.Init()
		reps = append(reps, r)
		wg.Add(1)
		go func() { defer wg.Done(); r.Run(ctx) }()
	}
	return reps, func() { cancel(); wg.Wait() }
}

// Two replicas tick for an hour of fake time: every window is evaluated
// once in the committed history, in order, and the state keeps up with the
// pipeline; the loops stop on cancel.
func TestRunTicksInFakeTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := store.NewMem()
		var mu sync.Mutex
		var commits []engine.State
		st.OnApply = func(_ string, b []byte) {
			var s engine.State
			if err := json.Unmarshal(b, &s); err == nil {
				mu.Lock()
				commits = append(commits, s)
				mu.Unlock()
			}
		}
		qs := &pipeline{lag: 20 * time.Second}
		start := time.Now()
		reps, stop := runReplicas(t, 2, st, qs, &fakeSink{}, nil)
		time.Sleep(time.Hour)
		stop()
		if took := time.Since(start); took != time.Hour {
			t.Fatalf("the loops took %v past the hour to stop", took-time.Hour)
		}
		s, _, err := reps[0].Load(context.Background(), "errors")
		if err != nil || s == nil {
			t.Fatal(err)
		}
		// caught up: the next window ends after complete_through (now − 20 s)
		// minus at most one tick's worth of windows
		ct := time.Now().Add(-20 * time.Second)
		ru := testRule(t)
		if next := time.Unix(0, s.NextEndNs); next.Before(ct.Add(-time.Minute)) || s.Skipped != 0 {
			t.Fatalf("behind: next window end %v, complete_through %v, skipped %d", next, ct, s.Skipped)
		}
		// every window once, in order, in the committed history
		for i := 1; i < len(commits); i++ {
			a, b := commits[i-1], commits[i]
			if b.NextEndNs < a.NextEndNs || b.Evaluated-a.Evaluated != (b.NextEndNs-a.NextEndNs)/int64(ru.Every) {
				t.Fatalf("committed history went from (next %d, evaluated %d) to (next %d, evaluated %d)",
					a.NextEndNs, a.Evaluated, b.NextEndNs, b.Evaluated)
			}
		}
		t.Logf("an hour: %d windows evaluated, %d commits, next window end %v behind complete_through",
			s.Evaluated, len(commits), ct.Sub(time.Unix(0, s.NextEndNs)))
		if s.Evaluated < 55 {
			t.Fatalf("%d windows evaluated in an hour", s.Evaluated)
		}
		// both replicas ticked (their own last-tick gauges are fresh)
		for _, r := range reps {
			if age := time.Since(time.UnixMilli(r.lastTick.Load())); age > r.Tick {
				t.Fatalf("%s: last tick %v ago", r.Writer, age)
			}
		}
	})
}

// A pipeline that stops advancing pages "cannot evaluate" once the rule has
// had nothing complete for CannotEvaluateAfter (3 min), not before, and the
// page resolves once it catches up; all on Run's ticker.
func TestRunPagesAStalledPipelineInFakeTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		qs := &pipeline{lag: 20 * time.Second}
		sink := &fakeSink{}
		_, stop := runReplicas(t, 1, store.NewMem(), qs, sink, nil)
		defer stop()
		time.Sleep(10 * time.Minute)
		qs.stall(true)
		stalledAt := time.Now()
		page := func() (firing, resolved bool) {
			for _, x := range sink.receipts() {
				if strings.Contains(x.key, "/cannot_evaluate/") && x.acked {
					if x.phase == engine.Resolved {
						resolved = true
					} else {
						firing = true
					}
				}
			}
			return
		}
		for {
			time.Sleep(time.Second)
			if f, _ := page(); f {
				break
			}
			if time.Since(stalledAt) > 15*time.Minute {
				t.Fatal("no page after 15 minutes of a stalled pipeline")
			}
		}
		// Nothing complete since complete_through froze: the page follows
		// CannotEvaluateAfter (plus the windows that were already complete, and
		// a tick) after the stall.
		if d := time.Since(stalledAt); d < 3*time.Minute || d > 3*time.Minute+2*time.Minute {
			t.Fatalf("paged %v after the stall; want about CannotEvaluateAfter (3m)", d)
		}
		t.Logf("paged %v after the stall", time.Since(stalledAt))
		qs.stall(false)
		time.Sleep(5 * time.Minute)
		if _, r := page(); !r {
			t.Fatal("the page did not resolve after the pipeline caught up")
		}
	})
}

// A replica skips a rule the other wrote within MinGap. Two replicas
// ticking 3 s apart with MinGap equal to the tick: the second always finds
// the first's write 3 s old and skips, so no window is evaluated twice;
// when the first stops, the second takes over within a tick and no window
// is left out. (Replicas whose tickers fire at the same instant both
// evaluate, and one loses the CAS: waste, not a fault; the scheduler
// decides which, so that case is not asserted.)
func TestRunMinGapSharesTheWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		qs := &pipeline{lag: 20 * time.Second}
		st := store.NewMem()
		tune := func(r *Runner) { r.MinGap = r.Tick }
		a, stopA := runReplicas(t, 1, st, qs, &fakeSink{}, tune)
		time.Sleep(3 * time.Second)
		b, stopB := runReplicas(t, 1, st, qs, &fakeSink{}, func(r *Runner) { tune(r); r.Writer = "r2" })
		time.Sleep(15 * time.Minute)
		stopA()
		time.Sleep(15 * time.Minute)
		stopB()
		qs.mu.Lock()
		defer qs.mu.Unlock()
		var ends []int64
		for e, n := range qs.perEnd {
			if n > 1 {
				t.Fatalf("window ending %v evaluated %d times", time.Unix(0, e), n)
			}
			ends = append(ends, e)
		}
		slices.Sort(ends)
		for i := 1; i < len(ends); i++ {
			if ends[i]-ends[i-1] != int64(time.Minute) {
				t.Fatalf("windows %v and %v: a gap after the first replica stopped", time.Unix(0, ends[i-1]), time.Unix(0, ends[i]))
			}
		}
		s, _, _ := b[0].Load(context.Background(), "errors")
		if s == nil || len(ends) < 29 || time.Now().Add(-20*time.Second).Sub(time.Unix(0, s.NextEndNs)) > time.Minute {
			t.Fatalf("%d windows; the survivor is behind", len(ends))
		}
		_ = a
		t.Logf("%d windows, each once; the second replica took over", len(ends))
	})
}
