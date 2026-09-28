package runner

// The two-replica simulation with late data (D30). The query service here
// has rows with custody times: each window's on-time rows are received
// before its end, and some windows get LATE rows, received minutes after
// the window was (or could have been) evaluated. Answers are at a basis
// (a custody time C: rows received before it), deltas count the rows
// received between two bases. With the same store, query and sink faults
// and the same interleaving as TestSimTwoReplicas, each run checks:
//
//   - late data never resolves: in the committed history a firing notice
//     turns resolved only through a window evaluated in order in that very
//     commit (its end is one of the windows the commit evaluated);
//   - no late row is counted twice or missed: at the end every kept
//     window's late_rows equals the late rows received between the basis
//     it was evaluated at and the basis it was last checked at, and its
//     verdict equals the verdict at that basis;
//   - a late episode is raised only for a group that holds in its window;
//   - every notice that reached the sink was committed first, and a
//     resolution never before its firing's acknowledgement.

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/notify"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
	"pgregory.net/rapid"
)

type lateRow struct {
	group string
	recv  int64
	inc   float64
}

// lateQS answers at bases "B<ns>" (a fleet bound).
type lateQS struct {
	mu    sync.Mutex
	ct    int64
	stale bool
	seed  uint64
	win   int64
	fault func() engine.Outcome
}

func h64(parts ...any) uint64 {
	h := fnv.New64a()
	fmt.Fprint(h, parts...)
	return h.Sum64()
}

// base is the on-time value of group g in the window ending e (received
// before e).
func (q *lateQS) base(e int64, g string) float64 { return float64(h64(q.seed, "/", e, "/", g) % 4) }

// late are the window's late rows: received 2 to 20 minutes after its end.
func (q *lateQS) late(e int64) []lateRow {
	var out []lateRow
	for i, g := range []string{"a", "b"} {
		v := h64(q.seed, "/late/", e, "/", i)
		if v%3 != 0 {
			continue
		}
		n := int(v/3%2) + 1
		for j := 0; j < n; j++ {
			w := h64(v, j)
			out = append(out, lateRow{group: g, recv: e + int64(2*time.Minute) + int64(w%uint64(18*time.Minute)), inc: float64(w%3 + 1)})
		}
	}
	return out
}

func (q *lateQS) rows(e, c int64) []rule.Row {
	var out []rule.Row
	for _, g := range []string{"a", "b"} {
		v := q.base(e, g)
		for _, l := range q.late(e) {
			if l.group == g && l.recv < c {
				v += l.inc
			}
		}
		out = append(out, rule.Row{Labels: map[string]string{"svc": g}, Value: v})
	}
	return out
}

func tokC(tok string) (int64, bool) {
	v, err := strconv.ParseInt(strings.TrimPrefix(tok, "B"), 10, 64)
	return v, err == nil && strings.HasPrefix(tok, "B")
}

func (q *lateQS) at(basis string) (int64, *engine.Result) {
	if basis == "latest" {
		if q.stale {
			return 0, &engine.Result{Outcome: engine.Unknown, WatermarkStatus: "stale", CompleteThroughNs: q.ct}
		}
		return q.ct, nil
	}
	c, ok := tokC(basis)
	switch {
	case !ok:
		return 0, &engine.Result{Outcome: engine.Refused, Err: "basis_invalid"}
	case c > q.ct:
		return 0, &engine.Result{Outcome: engine.Partial, Err: "basis_ahead"}
	}
	return c, nil
}

func (q *lateQS) Evaluate(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result {
	return q.EvaluateAt(ctx, r, w, "latest")
}

func (q *lateQS) EvaluateAt(_ context.Context, r *rule.Rule, w rule.Window, basis string) engine.Result {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.fault != nil {
		if o := q.fault(); o != "" {
			return engine.Result{Outcome: o, Err: "sim"}
		}
	}
	c, res := q.at(basis)
	if res != nil {
		return *res
	}
	if c < w.ToNs {
		return engine.Result{Outcome: engine.Partial, WatermarkStatus: "ok", CompleteThroughNs: c}
	}
	return engine.Result{Outcome: engine.Complete, Rows: q.rows(w.ToNs, c), WatermarkStatus: "ok", CompleteThroughNs: c,
		Basis: fmt.Sprintf("B%d", c), BasisC: map[string]uint64{"*": uint64(c)}}
}

func (q *lateQS) Delta(_ context.Context, r *rule.Rule, w rule.Window, from, to string) engine.Delta {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.fault != nil {
		if o := q.fault(); o != "" {
			return engine.Delta{Outcome: o, Err: "sim"}
		}
	}
	cf, ok := tokC(from)
	if !ok {
		return engine.Delta{Outcome: engine.Refused, Reason: "basis_invalid"}
	}
	ct, res := q.at(to)
	if res != nil {
		return engine.Delta{Outcome: res.Outcome, Err: res.Err}
	}
	if cf > ct {
		return engine.Delta{Outcome: engine.Partial, Reason: "basis_regressed"}
	}
	var n int64
	// the windows whose rows (event time end − 1 ns) are in w
	for e := w.FromNs + 1; e <= w.ToNs; e++ {
		if e%q.win != 0 {
			e += q.win - e%q.win - 1
			continue
		}
		for _, l := range q.late(e) {
			if l.recv >= cf && l.recv < ct {
				n++
			}
		}
	}
	return engine.Delta{Outcome: engine.Complete, Rows: n, Basis: fmt.Sprintf("B%d", ct), C: map[string]uint64{"*": uint64(ct)}}
}

// what the runs reached (the simulation must reach late data)
var simLate struct {
	mu                        sync.Mutex
	runs, lateRows, episodes  int64
	revisedWindows, lostBasis int64
}

func TestSimLateData(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) { simulateLate(t) })
	simLate.mu.Lock()
	defer simLate.mu.Unlock()
	t.Logf("runs %d: late rows found %d, windows revised %d, late episodes/notices %d, bases lost %d",
		simLate.runs, simLate.lateRows, simLate.revisedWindows, simLate.episodes, simLate.lostBasis)
	if simLate.lateRows == 0 || simLate.revisedWindows == 0 || simLate.episodes == 0 {
		t.Fatal("the simulation never reached late data")
	}
}

func simulateLate(t *rapid.T) {
	ctx := context.Background()
	ru := &rule.Rule{Name: "late", SQL: "SELECT 1", Window: rule.Duration(time.Minute),
		For:       rule.Duration(time.Duration(rapid.IntRange(0, 2).Draw(t, "for")) * time.Minute),
		Condition: rule.Condition{Op: rapid.SampledFrom([]string{">=", "<"}).Draw(t, "op"), Threshold: float64(rapid.IntRange(2, 5).Draw(t, "thr"))},
		OnLate:    rapid.SampledFrom([]string{rule.LateReevaluate, rule.LatePage}).Draw(t, "policy")}
	if err := ru.Validate(); err != nil {
		t.Fatal(err)
	}
	clk := &clock{t: t0.Add(time.Duration(rapid.IntRange(0, 59).Draw(t, "off")) * time.Second)}
	mem := store.NewMem()
	qs := &lateQS{ct: clk.Now().UnixNano(), seed: rapid.Uint64().Draw(t, "seed"), win: int64(time.Minute)}
	var last *engine.State
	var historyErr string
	cEval := map[int64]int64{}     // window end → the basis it was evaluated at
	committed := map[string]bool{} // every notice key ever committed
	mem.OnApply = func(_ string, b []byte) {
		var s engine.State
		if err := json.Unmarshal(b, &s); err != nil {
			historyErr = err.Error()
			return
		}
		for k := range s.Notices {
			committed[k] = true
		}
		for _, w := range s.Recent {
			if _, ok := cEval[w.EndNs]; !ok && w.Revisions == 0 && w.C != nil {
				cEval[w.EndNs] = int64(w.C["*"])
			}
		}
		if last != nil && historyErr == "" {
			if s.NextEndNs < last.NextEndNs {
				historyErr = "the position went back"
			}
			for k, n := range last.Notices {
				sn := s.Notices[k]
				if n.Kind != "rule" || n.Want != engine.Firing || sn == nil || sn.Want != engine.Resolved {
					continue
				}
				if sn.EndsNs < last.NextEndNs || sn.EndsNs >= s.NextEndNs {
					historyErr = fmt.Sprintf("%s resolved at %v, not by a window evaluated in order in this commit [%v, %v)",
						k, time.Unix(0, sn.EndsNs).UTC(), time.Unix(0, last.NextEndNs).UTC(), time.Unix(0, s.NextEndNs).UTC())
				}
			}
			for _, w := range s.Recent {
				for _, lw := range last.Recent {
					if lw.EndNs == w.EndNs && lw.C != nil && w.C != nil && w.C["*"] < lw.C["*"] {
						historyErr = fmt.Sprintf("window %d's basis went back", w.EndNs)
					}
				}
			}
		}
		last = &s
	}
	faultsOn := true
	storeFaults := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 11), 1, 64).Draw(t, "storeFaults")}
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
	qsFaults := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 9), 1, 64).Draw(t, "qsFaults")}
	qs.fault = func() engine.Outcome {
		if !faultsOn {
			return ""
		}
		switch qsFaults.next() {
		case 0:
			return engine.Timeout
		case 1:
			return engine.Failed
		}
		return ""
	}
	sinkFaults := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 7), 1, 64).Draw(t, "sinkFaults")}
	sink := &fakeSink{answer: func() (bool, notify.Answer) {
		if !faultsOn {
			return true, notify.Acked
		}
		switch sinkFaults.next() {
		case 0:
			return false, notify.NoAnswer
		case 1:
			return true, notify.NoAnswer
		case 2:
			return true, notify.ServerErr
		}
		return true, notify.Acked
	}}
	interleave := &script{vals: rapid.SliceOfN(rapid.IntRange(0, 3), 1, 32).Draw(t, "interleave")}
	var reps [2]*Runner
	depth := 0
	for i := range reps {
		other := 1 - i
		h := hooked{Store: mem, before: func() {
			if depth == 0 && interleave.next() == 0 {
				depth++
				_ = reps[other].ProcessRule(ctx, ru)
				depth--
			}
		}}
		reps[i] = newRunner(fmt.Sprintf("r%d", i+1), []*rule.Rule{ru}, h, qs, sink, clk)
		reps[i].MaxPerTick = 3
		reps[i].MaxLatePerTick = rapid.IntRange(1, 4).Draw(t, "latePerTick")
		reps[i].Engine.CannotEvaluateAfter = 3 * time.Minute
		reps[i].Engine.MaxBacklog = 24 * time.Hour
	}
	for _, s := range rapid.SliceOfN(rapid.IntRange(0, 6), 10, 80).Draw(t, "steps") {
		switch s {
		case 0, 1:
			_ = reps[s].ProcessRule(ctx, ru)
		case 2, 3:
			clk.Advance(20 * time.Second)
		case 4:
			qs.mu.Lock()
			qs.ct = max(qs.ct, clk.Now().UnixNano()-10e9)
			qs.stale = false
			qs.mu.Unlock()
		case 5:
			qs.mu.Lock()
			qs.stale = !qs.stale
			qs.mu.Unlock()
		case 6:
			mem.Flush()
		}
	}
	faultsOn = false
	qs.stale = false
	mem.Flush()
	for i := 0; i < 400; i++ {
		qs.mu.Lock()
		qs.ct = max(qs.ct, clk.Now().Add(-10*time.Second).UnixNano())
		qs.mu.Unlock()
		_ = reps[i%2].ProcessRule(ctx, ru)
		clk.Advance(5 * time.Second)
	}
	if historyErr != "" {
		t.Fatal(historyErr)
	}
	st, _, err := reps[0].Load(ctx, ru.Name)
	if err != nil || st == nil {
		t.Fatalf("no state: %v", err)
	}
	for _, w := range st.Recent {
		if w.Basis == "" {
			continue
		}
		ce, ok := cEval[w.EndNs]
		if !ok {
			t.Fatalf("window %d kept with no evaluation seen", w.EndNs)
		}
		cNow := int64(w.C["*"])
		var want int64
		for _, l := range qs.late(w.EndNs) {
			if l.recv >= ce && l.recv < cNow {
				want++
			}
		}
		if w.LateRows != want {
			t.Fatalf("window %v: late_rows %d, want %d (evaluated at %d, checked through %d)", time.Unix(0, w.EndNs).UTC(), w.LateRows, want, ce, cNow)
		}
		// the verdict kept is the verdict at the basis kept
		hold := map[string]bool{}
		for _, row := range qs.rows(w.EndNs, cNow) {
			if ru.Condition.Holds(row.Value) {
				hold[rule.GroupKey(row.Labels)] = true
			}
		}
		if len(hold) != len(w.Holds) {
			t.Fatalf("window %v: verdict %v, want %v at its basis", time.Unix(0, w.EndNs).UTC(), w.Holds, hold)
		}
		for g := range w.Holds {
			if !hold[g] {
				t.Fatalf("window %v: %s kept as holding", time.Unix(0, w.EndNs).UTC(), g)
			}
		}
		// a late episode is raised only for a group that held in its window
		// at a basis the evaluator saw
		for g := range w.LateFired {
			if _, ok := w.Holds[g]; !ok && ru.Condition.Op == ">=" {
				t.Fatalf("late episode for %s in a window where it does not hold", g)
			}
		}
	}
	simLate.mu.Lock()
	simLate.runs++
	simLate.lateRows += st.LateRows
	simLate.episodes += st.LateEpisodes
	simLate.lostBasis += st.LateLost
	for _, w := range st.Recent {
		if w.Revisions > 0 {
			simLate.revisedWindows++
		}
	}
	simLate.mu.Unlock()
	firingAcked := map[string]bool{}
	for _, x := range sink.receipts() {
		if !committed[x.key] {
			t.Fatalf("the sink got %s, which was never committed", x.key)
		}
		if x.phase == engine.Resolved && !firingAcked[x.key] {
			t.Fatalf("resolution of %s before its firing was acknowledged", x.key)
		}
		if x.phase == engine.Firing && x.acked {
			firingAcked[x.key] = true
		}
	}
}
