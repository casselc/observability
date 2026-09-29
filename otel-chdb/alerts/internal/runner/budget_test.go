package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// peakQS counts the statements in flight at once against the query service,
// across every replica that shares it (evaluations, late-data deltas and
// re-evaluations alike).
type peakQS struct {
	*lateQS
	cur, peak atomic.Int64
	deltas    atomic.Int64
}

func (p *peakQS) enter() func() {
	n := p.cur.Add(1)
	for m := p.peak.Load(); n > m && !p.peak.CompareAndSwap(m, n); m = p.peak.Load() {
	}
	time.Sleep(2 * time.Millisecond) // a statement takes time: let the others overlap it
	return func() { p.cur.Add(-1) }
}

func (p *peakQS) Evaluate(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result {
	defer p.enter()()
	return p.lateQS.Evaluate(ctx, r, w)
}

func (p *peakQS) EvaluateAt(ctx context.Context, r *rule.Rule, w rule.Window, basis string) engine.Result {
	defer p.enter()()
	return p.lateQS.EvaluateAt(ctx, r, w, basis)
}

func (p *peakQS) Delta(ctx context.Context, r *rule.Rule, w rule.Window, from, to string) engine.Delta {
	p.deltas.Add(1)
	defer p.enter()()
	return p.lateQS.Delta(ctx, r, w, from, to)
}

// CAST row 35: the evaluator's load on one identity (two replicas, every
// rule with late-data checks) exceeded the query service's per-caller
// max_concurrent (4); a 429 is a failed evaluation, so it paged "cannot
// evaluate". The fix is a budget, not code: every evaluator identity is in
// the limits-only group alert-evaluator (queryd.example.json). This checks
// the shipped pair of configs against the evaluator's measured peak: two
// replicas (README: "Run two replicas") at alertd.example.yaml's
// concurrency, many rules, late checks due, run at once against one
// identity. The peak must stay within concurrency × replicas (one statement
// at a time per rule) and within the service's limit for that identity.
// The production value is the owner's (16); a change to either file that
// breaks the budget fails here.
func TestEvaluatorPeakFitsTheQueryServiceLimit(t *testing.T) {
	tracetag.Covers(t, "P", "CAST-35", "H-4")
	const replicas = 2
	var alertd struct {
		Evaluation struct {
			Concurrency int `yaml:"concurrency"`
		} `yaml:"evaluation"`
	}
	b, err := os.ReadFile("../../alertd.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(b, &alertd); err != nil {
		t.Fatal(err)
	}
	conc := alertd.Evaluation.Concurrency
	if conc == 0 {
		conc = 4 // Runner's default
	}
	type lim struct {
		MaxConcurrent int `json:"max_concurrent"`
	}
	var queryd struct {
		Limits struct {
			Default lim            `json:"default"`
			Groups  map[string]lim `json:"groups"`
		} `json:"limits"`
	}
	b, err = os.ReadFile("../../../query/queryd.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &queryd); err != nil {
		t.Fatal(err)
	}
	limit := max(queryd.Limits.Default.MaxConcurrent, queryd.Limits.Groups["alert-evaluator"].MaxConcurrent)

	ctx := context.Background()
	clk := &clock{t: t0.Add(5 * time.Second)}
	qs := &peakQS{lateQS: &lateQS{ct: clk.Now().UnixNano(), seed: 35, win: int64(time.Minute)}}
	var rules []*rule.Rule
	for i := 0; i < 3*conc; i++ {
		ru := &rule.Rule{Name: fmt.Sprintf("r%02d", i), SQL: "SELECT 1", Window: rule.Duration(time.Minute),
			Condition: rule.Condition{Op: ">=", Threshold: 2}, OnLate: rule.LateReevaluate}
		if err := ru.Validate(); err != nil {
			t.Fatal(err)
		}
		rules = append(rules, ru)
	}
	mem := store.NewMem()
	var reps []*Runner
	for i := 0; i < replicas; i++ {
		r := newRunner(fmt.Sprintf("replica-%d", i), rules, mem, qs, &fakeSink{}, clk)
		r.Concurrency = alertd.Evaluation.Concurrency
		r.Engine.MaxBacklog = 24 * time.Hour
		reps = append(reps, r)
	}
	for round := 0; round < 40; round++ {
		clk.Advance(20 * time.Second)
		qs.mu.Lock()
		qs.ct = clk.Now().Add(-10 * time.Second).UnixNano()
		qs.mu.Unlock()
		var wg sync.WaitGroup
		for _, r := range reps {
			wg.Add(1)
			go func() { defer wg.Done(); r.TickOnce(ctx) }()
		}
		wg.Wait()
		mem.Flush()
	}
	peak := int(qs.peak.Load())
	t.Logf("peak %d statements at once (concurrency %d × %d replicas; %d late-data deltas); the identity's limit %d", peak, conc, replicas, qs.deltas.Load(), limit)
	if qs.deltas.Load() == 0 {
		t.Fatal("no late-data check ran: the load is not the one CAST row 35 saw")
	}
	if peak <= conc {
		t.Fatalf("peak %d: the replicas never overlapped, the measurement is vacuous", peak)
	}
	if peak > conc*replicas {
		t.Errorf("peak %d exceeds concurrency × replicas (%d): a rule issued statements in parallel", peak, conc*replicas)
	}
	if conc*replicas > limit {
		t.Errorf("the evaluator's peak (%d × %d = %d) exceeds the query service's max_concurrent for alert-evaluator (%d): its 429s would page \"cannot evaluate\"",
			conc, replicas, conc*replicas, limit)
	}
}
