package runner

// A deterministic simulation of two replicas sharing one state store, with
// the store's writes lost, answered-but-lost and delayed (a late copy), the
// query service stalling and failing, the sink losing requests and answers
// and failing, and one replica's whole tick run between the other's read
// and write. rapid draws the schedule; each run ends with the faults off
// and the pipeline caught up, and checks against a reference evaluation of
// the same windows:
//
//   - every window from the start to the end was evaluated, in order, once
//     in the committed history (the counter and the position agree);
//   - the episodes in the sink are exactly the reference's: each firing
//     was acknowledged, each resolved one's resolution too, and no rule
//     notice reached the sink that the reference does not have;
//   - a resolution never reached the sink before a 2xx for its firing;
//   - every "cannot evaluate" page was resolved once the rule caught up;
//   - the store's history, as the replicas saw it (lost and late answers
//     pending), is linearizable as one CAS register (casreg, porcupine).

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/notify"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
	"github.com/casselc/observability/otel-chdb/casreg"
	"pgregory.net/rapid"
)

// hooked is one replica's view of the shared store: before each write it
// may run the other replica's tick.
type hooked struct {
	store.Store
	before func()
}

func (h hooked) PutIfMatch(ctx context.Context, k, e string, b []byte) (string, error) {
	h.before()
	return h.Store.PutIfMatch(ctx, k, e, b)
}

func (h hooked) PutIfAbsent(ctx context.Context, k string, b []byte) (string, error) {
	h.before()
	return h.Store.PutIfAbsent(ctx, k, b)
}

type script struct {
	vals []int
	i    int
}

func (s *script) next() int {
	if len(s.vals) == 0 {
		return 0
	}
	v := s.vals[s.i%len(s.vals)]
	s.i++
	return v
}

func simRows(seed uint64, end int64) []rule.Row {
	var out []rule.Row
	for _, g := range []string{"a", "b"} {
		h := fnv.New64a()
		fmt.Fprintf(h, "%d/%d/%s", seed, end, g)
		v := h.Sum64() % 6
		if v == 5 {
			continue // no row for this group
		}
		out = append(out, rule.Row{Labels: map[string]string{"svc": g}, Value: float64(v)})
	}
	return out
}

// simReference: the episodes of the complete windows [start, end), in order.
func simReference(r *rule.Rule, seed uint64, start, end int64) map[string]bool {
	since := map[string]int64{}
	active := map[string]string{}
	eps := map[string]bool{} // key → resolved
	for e := start; e < end; e += int64(r.Every) {
		hold := map[string]bool{}
		for _, row := range simRows(seed, e) {
			if r.Condition.Holds(row.Value) {
				hold[rule.GroupKey(row.Labels)] = true
			}
		}
		for g := range since {
			if !hold[g] {
				if k, ok := active[g]; ok {
					eps[k] = true
					delete(active, g)
				}
				delete(since, g)
			}
		}
		for g := range hold {
			if _, ok := since[g]; !ok {
				since[g] = e
			}
			if _, ok := active[g]; !ok && e-since[g] >= int64(r.For) {
				k := engine.NoticeKey(r.Name, g, fmt.Sprint(e/int64(time.Second)))
				active[g], eps[k] = k, false
			}
		}
	}
	return eps
}

func TestSimTwoReplicas(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) { simulate(t) })
}

func simulate(t *rapid.T) {
	ctx := context.Background()
	ru := &rule.Rule{Name: "sim", SQL: "SELECT 1", Window: rule.Duration(time.Minute),
		For:       rule.Duration(time.Duration(rapid.IntRange(0, 2).Draw(t, "for")) * time.Minute),
		Condition: rule.Condition{Op: ">=", Threshold: float64(rapid.IntRange(1, 4).Draw(t, "thr"))}}
	if err := ru.Validate(); err != nil {
		t.Fatal(err)
	}
	seed := rapid.Uint64().Draw(t, "seed")
	clk := &clock{t: t0.Add(time.Duration(rapid.IntRange(0, 59).Draw(t, "off")) * time.Second)}
	mem := store.NewMem()
	// the committed history: the position only moves forward, and only by
	// evaluating each window in turn (no window evaluated twice, none skipped)
	var last *engine.State
	var historyErr string
	mem.OnApply = func(_ string, b []byte) {
		var s engine.State
		if err := json.Unmarshal(b, &s); err != nil {
			historyErr = err.Error()
			return
		}
		if last != nil && historyErr == "" {
			if s.NextEndNs < last.NextEndNs || s.Evaluated-last.Evaluated != (s.NextEndNs-last.NextEndNs)/int64(time.Minute) {
				historyErr = fmt.Sprintf("committed history went from (next %d, evaluated %d) to (next %d, evaluated %d)",
					last.NextEndNs, last.Evaluated, s.NextEndNs, s.Evaluated)
			}
		}
		last = &s
	}
	storeFaults := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 11), 1, 64).Draw(t, "storeFaults")}
	faultsOn := true
	mem.Faults = func(op, _ string) store.Fault {
		if !faultsOn || op != "put" {
			return store.NoFault
		}
		switch storeFaults.next() {
		case 0:
			return store.LoseRequest
		case 1:
			return store.LoseAnswer
		case 2:
			return store.DelayApply
		}
		return store.NoFault
	}
	qs := &fakeQS{ct: clk.Now().UnixNano(), data: func(end int64) []rule.Row { return simRows(seed, end) },
		holding: []engine.Lane{{Lane: "c9/p0/logs", LagS: 99}}}
	qsFaults := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 9), 1, 64).Draw(t, "qsFaults")}
	eval := evalFunc(func(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result {
		if faultsOn {
			switch qsFaults.next() {
			case 0:
				return engine.Result{Outcome: engine.Timeout, Err: "sim"}
			case 1:
				return engine.Result{Outcome: engine.Failed, Err: "sim"}
			}
		}
		return qs.Evaluate(ctx, r, w)
	})
	sinkFaults := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 7), 1, 64).Draw(t, "sinkFaults")}
	sink := &fakeSink{answer: func() (bool, notify.Answer) {
		if !faultsOn {
			return true, notify.Acked
		}
		switch sinkFaults.next() {
		case 0:
			return false, notify.NoAnswer // lost request
		case 1:
			return true, notify.NoAnswer // taken, answer lost
		case 2:
			return true, notify.ServerErr
		case 3:
			return false, notify.Rejected
		}
		return true, notify.Acked
	}}
	interleave := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 3), 1, 32).Draw(t, "interleave")}
	var reps [2]*Runner
	depth := 0
	// every replica's store calls, for the linearizability check at the end
	hist, etags := &casreg.Recorder{}, &casreg.Etags{}
	for i := range reps {
		other := 1 - i
		h := hooked{Store: linz{mem, hist, etags, i}, before: func() {
			if depth == 0 && interleave.next() == 0 {
				depth++
				_ = reps[other].ProcessRule(ctx, ru)
				depth--
			}
		}}
		reps[i] = newRunner(fmt.Sprintf("r%d", i+1), []*rule.Rule{ru}, h, eval, sink, clk)
		reps[i].MaxPerTick = 3
		reps[i].Engine.CannotEvaluateAfter = 3 * time.Minute
		reps[i].Engine.MaxBacklog = 24 * time.Hour
	}
	steps := rapid.SliceOfN(rapid.IntRange(0, 6), 10, 80).Draw(t, "steps")
	for _, s := range steps {
		switch s {
		case 0, 1:
			_ = reps[s].ProcessRule(ctx, ru)
		case 2, 3:
			clk.Advance(20 * time.Second)
		case 4:
			qs.ct = clk.Now().UnixNano() - 10e9 // the pipeline catches up to 10 s ago
			qs.stale = false
		case 5:
			qs.stale = !qs.stale
		case 6:
			mem.Flush()
		}
	}
	// the end: faults off, the pipeline caught up; both replicas keep ticking
	faultsOn = false
	qs.stale = false
	mem.Flush()
	var end int64
	for i := 0; i < 400; i++ {
		end = clk.Now().Add(-10 * time.Second).UnixNano()
		qs.ct = end
		_ = reps[i%2].ProcessRule(ctx, ru)
		clk.Advance(5 * time.Second)
	}
	if historyErr != "" {
		t.Fatal(historyErr)
	}
	hist.Verify(t)
	st, _, err := reps[0].Load(ctx, ru.Name)
	if err != nil || st == nil {
		t.Fatalf("no state: %v", err)
	}
	start := st.NextEndNs - st.Evaluated*int64(ru.Every)
	if st.Skipped != 0 || st.NextEndNs <= ru.AlignDown(end) {
		t.Fatalf("not caught up: next %v, ct %v, skipped %d", time.Unix(0, st.NextEndNs), time.Unix(0, end), st.Skipped)
	}
	for e := start; e < st.NextEndNs; e += int64(ru.Every) {
		if qs.perEnd[e] == 0 {
			t.Fatalf("window %v counted as evaluated but never answered complete", time.Unix(0, e))
		}
	}
	want := simReference(ru, seed, start, st.NextEndNs)
	firingAcked := map[string]bool{}
	resolvedAcked := map[string]bool{}
	for _, x := range sink.receipts() {
		if strings.Contains(x.key, "/cannot_evaluate/") {
			if x.phase == engine.Resolved && x.acked {
				resolvedAcked[x.key] = true
			} else if x.acked {
				firingAcked[x.key] = true
			}
			continue
		}
		if _, ok := want[x.key]; !ok {
			t.Fatalf("the sink got %s %s, which the reference does not have (want %v)", x.key, x.phase, want)
		}
		switch {
		case x.phase == engine.Resolved && !firingAcked[x.key]:
			t.Fatalf("resolution of %s reached the sink before its firing was acknowledged", x.key)
		case x.phase == engine.Resolved && x.acked:
			resolvedAcked[x.key] = true
		case x.acked:
			firingAcked[x.key] = true
		}
	}
	for k, resolved := range want {
		if !firingAcked[k] {
			t.Fatalf("episode %s never acknowledged firing", k)
		}
		if resolved && !resolvedAcked[k] {
			t.Fatalf("episode %s resolved but its resolution was never acknowledged", k)
		}
		if !resolved && resolvedAcked[k] {
			t.Fatalf("episode %s resolved at the sink but still firing", k)
		}
	}
	for k := range firingAcked {
		if strings.Contains(k, "/cannot_evaluate/") && !resolvedAcked[k] {
			t.Fatalf("cannot-evaluate page %s never resolved after catching up", k)
		}
	}
	if st.Meta.Firing {
		t.Fatal("still cannot evaluate")
	}
	for k, n := range st.Notices {
		if n.Want != engine.Firing || n.Acked != engine.Firing {
			t.Fatalf("ledger holds %s want %s acked %s", k, n.Want, n.Acked)
		}
	}
}

type evalFunc func(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result

func (f evalFunc) Evaluate(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result {
	return f(ctx, r, w)
}
