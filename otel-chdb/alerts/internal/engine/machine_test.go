package engine

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"pgregory.net/rapid"
)

// engineMachine is a rapid state machine over the evaluator's state and its
// delivery ledger, checked against the reference evaluation of the
// complete windows (reference, prop_test.go) and a model of what the sink
// acknowledged. Swarm: each case enables a random subset of the actions
// (hand-rolled; rapid has none), so rare combinations (only lost
// deliveries, no complete answers for a long time, ...) get whole runs to
// themselves and shrinking also minimises the enabled set
// (research/go-verification.md §3).
type engineMachine struct {
	r         *rule.Rule
	st        *State
	cfg       Config
	start     int64
	nowMs     int64
	completes [][]rule.Row
	// the sink: the last phase it acknowledged per notice key
	sink map[string]string
}

func (m *engineMachine) init(t *rapid.T) {
	m.r = drawRule(t)
	m.st = New(m.r, 3600*sec)
	m.start = m.st.NextEndNs
	m.cfg = Config{BackoffBase: time.Second, BackoffMax: time.Minute, HoldResolved: 30 * time.Second, Refresh: 10 * time.Minute}
	m.cfg.Defaults()
	m.sink = map[string]string{}
}

func (m *engineMachine) complete(t *rapid.T) {
	rows := genRows(t, "rows")
	if !Apply(m.r, m.st, Result{Outcome: Complete, Rows: rows}, m.nowMs) {
		t.Fatal("a complete answer did not advance")
	}
	m.completes = append(m.completes, rows)
}

func (m *engineMachine) notComplete(t *rapid.T) {
	o := rapid.SampledFrom([]Outcome{Partial, Unknown, Failed, Timeout, Refused, BadResult}).Draw(t, "outcome")
	before := m.st.NextEndNs
	if Apply(m.r, m.st, Result{Outcome: o, Rows: genRows(t, "junk")}, m.nowMs) || m.st.NextEndNs != before {
		t.Fatalf("%s advanced the rule", o)
	}
}

func (m *engineMachine) deliver(t *rapid.T, ok bool) {
	sends := DueSends(m.st, m.nowMs, m.cfg)
	for _, s := range sends {
		if s.Phase == Resolved && m.sink[s.Notice.Key] != Firing {
			t.Fatalf("resolution of %s due before its firing was acknowledged", s.Notice.Key)
		}
		if ok {
			m.sink[s.Notice.Key] = s.Phase
		}
	}
	Delivered(m.st, sends, ok, map[bool]string{true: "2xx", false: "lost"}[ok], m.nowMs, m.cfg)
}

func (m *engineMachine) advance(t *rapid.T) {
	m.nowMs += int64(rapid.IntRange(1, 120_000).Draw(t, "ms"))
}

// check: the state is the reference's, and the ledger agrees with the sink.
func (m *engineMachine) check(t *rapid.T) {
	eps, end := reference(m.r, m.start, m.completes)
	if m.st.NextEndNs != end || m.st.Evaluated != int64(len(m.completes)) {
		t.Fatalf("position %d want %d (evaluated %d of %d)", m.st.NextEndNs, end, m.st.Evaluated, len(m.completes))
	}
	for k, e := range eps {
		n := m.st.Notices[k]
		switch {
		case n == nil && !(e.resolved && m.sink[k] == Resolved):
			t.Fatalf("episode %s left the ledger before the sink acknowledged its resolution", k)
		case n != nil && (n.Want == Resolved) != e.resolved:
			t.Fatalf("episode %s: want %s, reference resolved=%v", k, n.Want, e.resolved)
		case n != nil && n.Acked != "" && n.Acked != m.sink[k]:
			t.Fatalf("episode %s: ledger acked %s, the sink last acknowledged %q", k, n.Acked, m.sink[k])
		}
	}
	for k := range m.st.Notices {
		if eps[k] == nil {
			t.Fatalf("notice %s the reference has no episode for", k)
		}
	}
	if m.st.NextEndNs%int64(m.r.Every) != 0 {
		t.Fatalf("unaligned window end %d", m.st.NextEndNs)
	}
}

func TestEngineStateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := &engineMachine{}
		m.init(t)
		all := map[string]func(*rapid.T){
			"complete":     m.complete,
			"not_complete": m.notComplete,
			"deliver_ok":   func(t *rapid.T) { m.deliver(t, true) },
			"deliver_lost": func(t *rapid.T) { m.deliver(t, false) },
			"advance":      m.advance,
		}
		names := make([]string, 0, len(all))
		for k := range all {
			names = append(names, k)
		}
		slices.Sort(names)
		on := rapid.SliceOfNDistinct(rapid.SampledFrom(names), 1, len(names), rapid.ID[string]).Draw(t, "swarm")
		actions := map[string]func(*rapid.T){"": m.check}
		for _, k := range on {
			actions[k] = all[k]
		}
		t.Repeat(actions)
		// the end: everything complete and delivered, the sink agrees
		m.deliverAll(t)
		m.check(t)
		for k, n := range m.st.Notices {
			if n.Acked != n.Want && n.NextMs <= m.nowMs {
				t.Fatalf("notice %s undelivered at the end: want %s acked %s", k, n.Want, n.Acked)
			}
		}
	})
}

// deliverAll delivers until nothing is due, waiting out backoffs and the
// hold before a resolution.
func (m *engineMachine) deliverAll(t *rapid.T) {
	for i := 0; i < 50; i++ {
		m.nowMs += m.cfg.BackoffMax.Milliseconds() + m.cfg.HoldResolved.Milliseconds()
		sends := DueSends(m.st, m.nowMs, m.cfg)
		if len(sends) == 0 {
			return
		}
		for _, s := range sends {
			m.sink[s.Notice.Key] = s.Phase
		}
		Delivered(m.st, sends, true, "2xx", m.nowMs, m.cfg)
	}
	t.Fatal(fmt.Sprintf("still sending after 50 rounds: %d notices", len(m.st.Notices)))
}
