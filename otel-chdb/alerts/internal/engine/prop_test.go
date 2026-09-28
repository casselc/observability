package engine

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"pgregory.net/rapid"
)

var outcomes = []Outcome{Complete, Complete, Complete, Partial, Unknown, Failed, Timeout, Refused, BadResult}

func genRows(t *rapid.T, label string) []rule.Row {
	var rs []rule.Row
	for _, g := range []string{"a", "b", "c"} {
		if rapid.Bool().Draw(t, label+g+"?") {
			rs = append(rs, rule.Row{Labels: map[string]string{"service": g}, Value: float64(rapid.IntRange(0, 5).Draw(t, label+g))})
		}
	}
	return rs
}

// reference evaluates the complete windows alone, independently of the
// engine: per group, when it started holding; an episode per firing onset.
type refEpisode struct {
	key      string
	resolved bool
}

func reference(r *rule.Rule, start int64, windows [][]rule.Row) (map[string]*refEpisode, int64) {
	since := map[string]int64{}
	active := map[string]string{} // group → episode key
	eps := map[string]*refEpisode{}
	end := start
	for _, rows := range windows {
		hold := map[string]bool{}
		for _, row := range rows {
			if r.Condition.Holds(row.Value) {
				hold[rule.GroupKey(row.Labels)] = true
			}
		}
		if len(rows) == 0 && r.OnNoRows == "fire" {
			hold[rule.GroupKey(map[string]string{})] = true
		}
		for g := range since {
			if !hold[g] {
				if k, ok := active[g]; ok {
					eps[k].resolved = true
					delete(active, g)
				}
				delete(since, g)
			}
		}
		for g := range hold {
			if _, ok := since[g]; !ok {
				since[g] = end
			}
			if _, ok := active[g]; !ok && end-since[g] >= int64(r.For) {
				k := NoticeKey(r.Name, g, fmt.Sprint(end/int64(time.Second)))
				active[g] = k
				eps[k] = &refEpisode{key: k}
			}
		}
		end += int64(r.Every)
	}
	return eps, end
}

func drawRule(t *rapid.T) *rule.Rule {
	r := &rule.Rule{Name: "r", SQL: "SELECT 1", Window: rule.Duration(time.Minute),
		For:       rule.Duration(time.Duration(rapid.IntRange(0, 3).Draw(t, "for")) * time.Minute),
		Condition: rule.Condition{Op: rapid.SampledFrom([]string{">", ">=", "<", "=="}).Draw(t, "op"), Threshold: float64(rapid.IntRange(0, 5).Draw(t, "thr"))},
		OnNoRows:  rapid.SampledFrom([]string{"ok", "fire"}).Draw(t, "nrows")}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

// TestPropOnlyCompleteWindowsDecide: whatever sequence of outcomes the query
// service gives, the groups, episodes and position are exactly those of
// the complete windows evaluated alone, in order; nothing else moves them.
func TestPropOnlyCompleteWindowsDecide(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		r := drawRule(t)
		st := New(r, 3600*sec)
		start := st.NextEndNs
		var completes [][]rule.Row
		n := rapid.IntRange(0, 40).Draw(t, "n")
		for i := 0; i < n; i++ {
			o := rapid.SampledFrom(outcomes).Draw(t, "o")
			res := Result{Outcome: o}
			if o == Complete {
				res.Rows = genRows(t, "rows")
				completes = append(completes, res.Rows)
			} else if rapid.Bool().Draw(t, "junk") {
				res.Rows = genRows(t, "junk") // rows on a non-complete answer must be ignored
			}
			before := st.NextEndNs
			if adv := Apply(r, st, res, int64(i)); adv != (o == Complete) {
				t.Fatalf("%s: advanced=%v", o, adv)
			}
			if o != Complete && st.NextEndNs != before {
				t.Fatalf("%s moved the window", o)
			}
			if st.NextEndNs%int64(r.Every) != 0 {
				t.Fatalf("unaligned window end %d", st.NextEndNs)
			}
		}
		eps, end := reference(r, start, completes)
		if st.NextEndNs != end || st.Evaluated != int64(len(completes)) {
			t.Fatalf("position %d want %d (evaluated %d of %d)", st.NextEndNs, end, st.Evaluated, len(completes))
		}
		if len(st.Notices) != len(eps) {
			t.Fatalf("notices %v, reference %v", keys(st.Notices), eps)
		}
		for k, e := range eps {
			nt := st.Notices[k]
			if nt == nil {
				t.Fatalf("missing episode %s", k)
			}
			if (nt.Want == Resolved) != e.resolved {
				t.Fatalf("episode %s: want %s, reference resolved=%v", k, nt.Want, e.resolved)
			}
		}
	})
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestPropLedger: under any sequence of sink answers (2xx, no answer, 5xx,
// 4xx) interleaved with evaluations,
//   - a resolution is sent only after a 2xx for the same key's firing;
//   - a notice leaves the ledger only after a 2xx for its resolution;
//   - once the sink answers 2xx again, every episode's firing is
//     acknowledged and every resolved episode's resolution is too.
func TestPropLedger(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		r := drawRule(t)
		c := Config{HoldResolved: time.Duration(rapid.IntRange(0, 60).Draw(t, "hold")) * time.Second,
			Refresh: time.Duration(rapid.IntRange(0, 2).Draw(t, "refresh")) * time.Minute}
		c.Defaults()
		st := New(r, 3600*sec)
		start := st.NextEndNs
		var completes [][]rule.Row
		firingAcked := map[string]bool{}
		resolvedAcked := map[string]bool{}
		now := int64(0)
		n := rapid.IntRange(0, 60).Draw(t, "n")
		for i := 0; i < n; i++ {
			now += int64(rapid.IntRange(0, 30_000).Draw(t, "dt"))
			if rapid.Bool().Draw(t, "eval") {
				rows := genRows(t, "rows")
				completes = append(completes, rows)
				Apply(r, st, complete(rows), now)
				continue
			}
			sends := DueSends(st, now, c)
			ok := rapid.SampledFrom([]bool{true, false, false}).Draw(t, "ack")
			before := map[string]bool{}
			for k := range st.Notices {
				before[k] = true
			}
			for _, s := range sends {
				if s.Phase == Resolved && !firingAcked[s.Notice.Key] {
					t.Fatalf("resolution of %s sent before its firing was acknowledged", s.Notice.Key)
				}
			}
			Delivered(st, sends, ok, "x", now, c)
			if ok {
				for _, s := range sends {
					if s.Phase == Firing {
						firingAcked[s.Notice.Key] = true
					} else {
						resolvedAcked[s.Notice.Key] = true
					}
				}
			}
			for k := range before {
				if st.Notices[k] == nil && !resolvedAcked[k] {
					t.Fatalf("%s left the ledger without an acknowledged resolution", k)
				}
			}
		}
		// the sink recovers
		for i := 0; i < 20; i++ {
			now += c.BackoffMax.Milliseconds() + c.HoldResolved.Milliseconds() + 1
			sends := DueSends(st, now, c)
			Delivered(st, sends, true, "acked", now, c)
			for _, s := range sends {
				if s.Phase == Firing {
					firingAcked[s.Notice.Key] = true
				} else {
					resolvedAcked[s.Notice.Key] = true
				}
			}
		}
		eps, _ := reference(r, start, completes)
		for k, e := range eps {
			if !firingAcked[k] {
				t.Fatalf("episode %s never acknowledged firing", k)
			}
			if e.resolved != resolvedAcked[k] {
				t.Fatalf("episode %s resolved=%v, resolution acknowledged=%v", k, e.resolved, resolvedAcked[k])
			}
			if e.resolved == (st.Notices[k] != nil) {
				t.Fatalf("episode %s: ledger holds it=%v, resolved=%v", k, st.Notices[k] != nil, e.resolved)
			}
		}
	})
}

// TestPropCannotEvaluate: the rule pages exactly when its window has been
// unevaluable past the bound (or failed often enough, or was refused), and
// never while it is evaluating complete windows.
func TestPropCannotEvaluate(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		r := drawRule(t)
		c := Config{CannotEvaluateAfter: time.Duration(rapid.IntRange(1, 10).Draw(t, "bound")) * time.Minute,
			FailuresToPage: rapid.IntRange(1, 4).Draw(t, "fails")}
		c.Defaults()
		st := New(r, 3600*sec)
		now := st.NextEndNs
		fails := 0
		for i := rapid.IntRange(0, 50).Draw(t, "n"); i > 0; i-- {
			now += int64(rapid.IntRange(0, 120).Draw(t, "dt")) * sec
			if now < st.NextEndNs {
				continue
			}
			o := rapid.SampledFrom(outcomes).Draw(t, "o")
			Apply(r, st, Result{Outcome: o, Rows: nil}, now/1e6)
			switch {
			case o == Complete:
				fails = 0
			case o.IsFailure():
				fails++
			default:
				fails = 0
			}
			UpdateMeta(r, st, now, now/1e6, c)
			want := o != Complete && (o == Refused || fails >= c.FailuresToPage || now-st.NextEndNs > int64(c.CannotEvaluateAfter))
			if st.Meta.Firing != want {
				t.Fatalf("after %s (fails %d, behind %ds): firing=%v want %v", o, fails, (now-st.NextEndNs)/sec, st.Meta.Firing, want)
			}
			if want && st.Notices[st.Meta.Key] == nil {
				t.Fatal("paging without a notice")
			}
		}
	})
}
