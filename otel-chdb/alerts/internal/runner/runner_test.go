package runner

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/notify"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// fakeQS answers like the query service: complete up to ct, partial after,
// unknown while stale; rows from data by window end.
type fakeQS struct {
	mu      sync.Mutex
	ct      int64
	stale   bool
	fail    engine.Outcome // non-empty: every call fails so
	data    func(end int64) []rule.Row
	calls   int
	perEnd  map[int64]int // complete answers per window end
	holding []engine.Lane
}

func (f *fakeQS) Evaluate(_ context.Context, r *rule.Rule, w rule.Window) engine.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	switch {
	case f.fail != "":
		return engine.Result{Outcome: f.fail, Err: "injected"}
	case f.stale:
		return engine.Result{Outcome: engine.Unknown, WatermarkStatus: "stale", WatermarkAgeS: 400, CompleteThroughNs: f.ct, Stale: f.holding}
	case w.ToNs > f.ct:
		return engine.Result{Outcome: engine.Partial, WatermarkStatus: "ok", CompleteThroughNs: f.ct, Holding: f.holding}
	}
	if f.perEnd == nil {
		f.perEnd = map[int64]int{}
	}
	f.perEnd[w.ToNs]++
	var rows []rule.Row
	if f.data != nil {
		rows = f.data(w.ToNs)
	}
	return engine.Result{Outcome: engine.Complete, Rows: rows, WatermarkStatus: "ok", CompleteThroughNs: f.ct}
}

type receipt struct {
	key, phase string
	acked      bool
}

// fakeSink records what reached it; answer decides what the caller hears.
type fakeSink struct {
	mu       sync.Mutex
	got      []receipt
	answer   func() (reached bool, ans notify.Answer)
	payloads [][]notify.Alert
}

func (s *fakeSink) Send(_ context.Context, sends []engine.Send, now time.Time) (notify.Answer, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reached, ans := true, notify.Acked
	if s.answer != nil {
		reached, ans = s.answer()
	}
	if reached {
		for _, x := range sends {
			s.got = append(s.got, receipt{x.Notice.Key, x.Phase, ans == notify.Acked})
		}
		s.payloads = append(s.payloads, notify.New(notify.Config{}).Payload(sends, now))
	}
	return ans, ""
}

func (s *fakeSink) receipts() []receipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]receipt(nil), s.got...)
}

func testRule(t testing.TB) *rule.Rule {
	r := &rule.Rule{Name: "errors", SQL: "SELECT 1", Window: rule.Duration(time.Minute), Condition: rule.Condition{Op: ">", Threshold: 2}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newRunner(name string, rules []*rule.Rule, st store.Store, qs Evaluator, sink Sender, clk *clock) *Runner {
	r := &Runner{Rules: rules, Store: st, Prefix: "alr/state", Eval: qs, Sink: sink, Writer: name, Tick: 10 * time.Second,
		MaxPerTick: 5, MinGap: -1, Now: clk.Now, Log: quiet,
		Engine: engine.Config{CannotEvaluateAfter: 5 * time.Minute, HoldResolved: 30 * time.Second, Refresh: time.Minute}}
	r.Init()
	return r
}

var t0 = time.Unix(1_789_999_980, 0).UTC() // a whole minute

func TestCatchUpInOrderBounded(t *testing.T) {
	clk := &clock{t: t0.Add(5 * time.Second)}
	qs := &fakeQS{ct: t0.UnixNano(), data: func(end int64) []rule.Row {
		if (end/1e9/60)%3 == 0 {
			return []rule.Row{{Labels: map[string]string{"s": "a"}, Value: 5}}
		}
		return nil
	}}
	sink := &fakeSink{}
	r := newRunner("r1", []*rule.Rule{testRule(t)}, store.NewMem(), qs, sink, clk)
	ctx := context.Background()
	r.TickOnce(ctx)
	st, _, _ := r.Load(ctx, "errors")
	start := st.NextEndNs
	if start != t0.UnixNano()+60e9 { // t0's window was evaluated at once (ct = t0)
		t.Fatalf("start %v", time.Unix(0, start))
	}
	// the evaluator is down for 30 minutes while the pipeline keeps up
	clk.Advance(30 * time.Minute)
	qs.ct = clk.Now().UnixNano()
	r.TickOnce(ctx)
	st, _, _ = r.Load(ctx, "errors")
	if st.Evaluated != 1+5 {
		t.Fatalf("catch-up not bounded per tick: evaluated %d", st.Evaluated)
	}
	for i := 0; i < 10; i++ {
		r.TickOnce(ctx)
	}
	st, _, _ = r.Load(ctx, "errors")
	if st.NextEndNs != r.Rules[0].AlignDown(clk.Now().UnixNano())+60e9 || st.Evaluated != 31 {
		t.Fatalf("not caught up: next %v evaluated %d", time.Unix(0, st.NextEndNs), st.Evaluated)
	}
	for end, n := range qs.perEnd {
		if n != 1 {
			t.Fatalf("window %v evaluated %d times", time.Unix(0, end), n)
		}
	}
	// every 3rd minute fires and the next resolves: all episodes reached the sink
	fired := map[string]bool{}
	for _, x := range sink.receipts() {
		if x.phase == "firing" {
			fired[x.key] = true
		}
	}
	want := 0
	for end := t0.UnixNano(); end < st.NextEndNs; end += 60e9 {
		if (end/1e9/60)%3 == 0 {
			want++
		}
	}
	if len(fired) != want || want < 10 {
		t.Fatalf("%d episodes delivered, want %d", len(fired), want)
	}
	if v := r.Metrics.Value("alr_evaluations_total", "errors", "complete"); v != 31 {
		t.Fatalf("metric %v", v)
	}
}

func TestStalledLanePagesThenCatchesUp(t *testing.T) {
	clk := &clock{t: t0.Add(time.Second)}
	qs := &fakeQS{ct: t0.UnixNano(), holding: []engine.Lane{{Lane: "prod-b/pub-0/logs", LagS: 400}}}
	sink := &fakeSink{}
	r := newRunner("r1", []*rule.Rule{testRule(t)}, store.NewMem(), qs, sink, clk)
	ctx := context.Background()
	for i := 0; i < 36; i++ { // 6 minutes, the watermark stuck at t0
		clk.Advance(10 * time.Second)
		r.TickOnce(ctx)
	}
	if r.Metrics.Value("alr_cannot_evaluate", "errors") != 1 {
		t.Fatal("no cannot-evaluate page after 6 minutes of a stalled lane")
	}
	var page *notify.Alert
	for _, p := range sink.payloads {
		for i := range p {
			if p[i].Labels["alertname"] == "AlertCannotEvaluate" {
				page = &p[i]
			}
		}
	}
	if page == nil || !strings.Contains(page.Annotations["reason"], "prod-b/pub-0/logs") || page.Annotations["clusters"] != "prod-b" {
		t.Fatalf("page %+v", page)
	}
	if v := r.Metrics.Value("alr_pending_windows", "errors"); v < 5 {
		t.Fatalf("pending windows %v", v)
	}
	// the watermark stops being published: the reason changes to stale
	qs.stale = true
	clk.Advance(10 * time.Second)
	r.TickOnce(ctx)
	st, _, _ := r.Load(ctx, "errors")
	if !strings.Contains(st.Notices[st.Meta.Key].Annotations["reason"], "watermark stale") {
		t.Fatalf("reason %q", st.Notices[st.Meta.Key].Annotations["reason"])
	}
	// the lane comes back
	qs.stale, qs.ct = false, clk.Now().UnixNano()
	for i := 0; i < 10; i++ {
		clk.Advance(10 * time.Second)
		r.TickOnce(ctx)
	}
	st, _, _ = r.Load(ctx, "errors")
	if st.Meta.Firing || r.Metrics.Value("alr_cannot_evaluate", "errors") != 0 {
		t.Fatal("still cannot evaluate after catching up")
	}
	var resolved bool
	for _, x := range sink.receipts() {
		resolved = resolved || (strings.Contains(x.key, "cannot_evaluate") && x.phase == "resolved" && x.acked)
	}
	if !resolved || len(st.Notices) != 0 {
		t.Fatalf("the page was not resolved at the sink (%d notices left)", len(st.Notices))
	}
}

func TestFailuresAreNotOK(t *testing.T) {
	clk := &clock{t: t0.Add(time.Second)}
	qs := &fakeQS{ct: t0.UnixNano() + 3600e9, data: func(int64) []rule.Row {
		return []rule.Row{{Labels: map[string]string{"s": "a"}, Value: 9}}
	}}
	sink := &fakeSink{}
	r := newRunner("r1", []*rule.Rule{testRule(t)}, store.NewMem(), qs, sink, clk)
	ctx := context.Background()
	r.TickOnce(ctx) // fires
	qs.fail = engine.Timeout
	for i := 0; i < 3; i++ {
		clk.Advance(60 * time.Second)
		r.TickOnce(ctx)
	}
	st, _, _ := r.Load(ctx, "errors")
	firing := 0
	for _, n := range st.Notices {
		if n.Kind == "rule" && n.Want == engine.Firing {
			firing++
		}
	}
	if firing != 1 || !st.Meta.Firing {
		t.Fatalf("a failing query resolved the alert or did not page: firing %d meta %v", firing, st.Meta.Firing)
	}
}

func TestAmbiguousStateWrites(t *testing.T) {
	ctx := context.Background()
	m := store.NewMem()
	r := newRunner("r1", []*rule.Rule{testRule(t)}, m, &fakeQS{}, &fakeSink{}, &clock{t: t0})
	st := engine.New(r.Rules[0], t0.UnixNano())
	m.Faults = func(op, _ string) store.Fault {
		if op == "put" {
			return store.LoseAnswer
		}
		return store.NoFault
	}
	e, ok := r.commit(ctx, "errors", "", st)
	if !ok || e == "" || r.Metrics.Value("alr_state_writes_total", "ambiguous_landed") != 1 {
		t.Fatal("a landed write with no answer was not recognised")
	}
	m.Faults = func(op, _ string) store.Fault {
		if op == "put" {
			return store.LoseRequest
		}
		return store.NoFault
	}
	if _, ok := r.commit(ctx, "errors", e, st); ok || r.Metrics.Value("alr_state_writes_total", "ambiguous_lost") != 1 {
		t.Fatal("a lost write was taken as landed")
	}
	// a delayed copy lands later only if nothing was written since
	m.Faults = func(op, _ string) store.Fault {
		if op == "put" {
			return store.DelayApply
		}
		return store.NoFault
	}
	if _, ok := r.commit(ctx, "errors", e, st); ok {
		t.Fatal("delayed copy taken as landed")
	}
	m.Faults = nil
	e2, ok := r.commit(ctx, "errors", e, st)
	if !ok {
		t.Fatal()
	}
	m.Flush() // the late copy's If-Match is stale now
	_, e3, _ := m.Get(ctx, "alr/state/errors.json")
	if e3 != e2 {
		t.Fatal("a late copy overwrote a newer state")
	}
}

func TestHealthAndMetrics(t *testing.T) {
	clk := &clock{t: t0}
	r := newRunner("r1", []*rule.Rule{testRule(t)}, store.NewMem(), &fakeQS{}, &fakeSink{}, clk)
	hs := httptest.NewServer(r.Handler())
	defer hs.Close()
	get := func(p string) (int, string) {
		resp, err := hs.Client().Get(hs.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if c, _ := get("/healthz"); c != 503 {
		t.Fatalf("healthy before the first tick: %d", c)
	}
	r.TickOnce(context.Background())
	if c, b := get("/healthz"); c != 200 {
		t.Fatalf("%d %s", c, b)
	}
	_, m := get("/metrics")
	for _, want := range []string{`alr_evaluations_total{rule="errors",outcome="partial"} 1`, "alr_pending_windows", "alr_lag_behind_complete_through_seconds", "alr_deliveries_total"} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %s", want)
		}
	}
	if c, _ := get("/state/errors"); c != 200 {
		t.Fatal(c)
	}
	if c, _ := get("/state/..%2Fx"); c != 404 {
		t.Fatal(c)
	}
	clk.Advance(10 * time.Minute)
	if c, _ := get("/healthz"); c != 503 {
		t.Fatal("healthy with no tick for 10 minutes")
	}
}
