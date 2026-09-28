package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
)

func at(c int64, rs []rule.Row) Result {
	r := complete(rs)
	r.Basis, r.BasisC = fmt.Sprintf("B%d", c), map[string]uint64{"*": uint64(c)}
	return r
}

func delta(c, n int64) Delta {
	return Delta{Outcome: Complete, Rows: n, Basis: fmt.Sprintf("B%d", c), C: map[string]uint64{"*": uint64(c)}}
}

var ga = rule.GroupKey(map[string]string{"service": "a"})

// Windows evaluated at a basis are kept with it; a clean check moves them
// forward, never back.
func TestRecentAndChecks(t *testing.T) {
	r := testRule(t, nil)
	st := New(r, 600*sec)
	for i := int64(0); i < 3; i++ {
		Apply(r, st, at(700+i, rows("a", 1)), 0)
	}
	if len(st.Recent) != 3 || st.Recent[0].Basis != "B700" || !LateDue(r, st, 60_000) {
		t.Fatalf("%+v", st.Recent)
	}
	w, from, ok := LateSpan(r, st)
	if !ok || from != "B700" || w.FromNs != 540*sec || w.ToNs != 720*sec {
		t.Fatal(w, from, ok)
	}
	LateChecked(st, []int64{600 * sec, 660 * sec, 720 * sec}, "B800", map[string]uint64{"*": 800})
	for _, x := range st.Recent {
		if x.Basis != "B800" {
			t.Fatal(x.Basis)
		}
	}
	LateChecked(st, []int64{600 * sec}, "B750", map[string]uint64{"*": 750})
	if st.Recent[0].Basis != "B800" {
		t.Fatal("a check moved a window's basis back")
	}
	// bases that are not comparable: no span check
	st.Recent[1].C = map[string]uint64{"a": 900}
	if _, _, ok := LateSpan(r, st); ok {
		t.Fatal("span over incomparable bases")
	}
	// ignore keeps nothing; the horizon bounds what is kept
	ig := testRule(t, func(r *rule.Rule) { r.OnLate = rule.LateIgnore })
	st2 := New(ig, 600*sec)
	Apply(ig, st2, at(700, nil), 0)
	if len(st2.Recent) != 0 || LateDue(ig, st2, 1<<40) {
		t.Fatal("ignore keeps windows")
	}
	short := testRule(t, func(r *rule.Rule) { r.LateHorizon = rule.Duration(2 * time.Minute) })
	st3 := New(short, 600*sec)
	for i := int64(0); i < 5; i++ {
		Apply(short, st3, at(700, nil), 0)
	}
	if len(st3.Recent) != 2 {
		t.Fatal(len(st3.Recent))
	}
}

// reevaluate: a group that holds only with the late rows fires a late
// episode (once per run), which resolves by itself; a firing group is never
// touched and nothing is resolved.
func TestLateReevaluate(t *testing.T) {
	r := testRule(t, nil) // for 0, value > 2
	st := New(r, 600*sec)
	Apply(r, st, at(700, rows("a", 1)), 0)         // window 600: a does not hold
	Apply(r, st, at(701, rows("a", 1, "b", 5)), 0) // 660: b fires
	Apply(r, st, at(702, rows("a", 1, "b", 5)), 0) // 720
	firing := len(st.Notices)
	LateApply(r, st, 600*sec, delta(800, 4), at(800, rows("a", 3)), 1000)
	if st.LateRows != 4 || st.Recent[0].Basis != "B800" || st.Recent[0].Revisions != 1 {
		t.Fatalf("%+v", st.Recent[0])
	}
	key := NoticeKey(r.Name, ga, "late-600")
	n := st.Notices[key]
	if n == nil || n.Want != Resolved || n.Labels["alert_late"] != "true" || n.Labels["alert_episode"] != "late-600" || len(st.Notices) != firing+1 {
		t.Fatalf("late episode: %+v", n)
	}
	// the same run with more late rows: not paged twice
	LateApply(r, st, 600*sec, delta(900, 1), at(900, rows("a", 9)), 2000)
	if len(st.Notices) != firing+1 || st.LateRows != 5 {
		t.Fatal("paged twice")
	}
	// late rows that make b stop holding in 660 resolve nothing
	gb := rule.GroupKey(map[string]string{"service": "b"})
	LateApply(r, st, 660*sec, delta(900, 2), at(900, rows("a", 1, "b", 1)), 3000)
	if g := st.Groups[gb]; g == nil || g.Phase != Firing {
		t.Fatal("a late row resolved a firing group")
	}
	for k, n := range st.Notices {
		if n.Kind == "rule" && n.Want == Resolved && k != key {
			t.Fatalf("%s resolved by late data", k)
		}
	}
	// a delta whose basis is not above the window's: nothing moves
	before := st.LateRows
	LateApply(r, st, 600*sec, delta(850, 7), at(850, rows("a", 1)), 4000)
	if st.LateRows != before || st.Recent[0].Basis != "B900" {
		t.Fatal("a window's basis went back")
	}
}

// for: the late episode needs the run the late rows complete; a live
// pending group whose run they lengthen fires as a live episode.
func TestLateFor(t *testing.T) {
	r := testRule(t, func(r *rule.Rule) { r.For = rule.Duration(2 * time.Minute) })
	st := New(r, 600*sec)
	Apply(r, st, at(700, rows("a", 5)), 0) // 600 holds
	Apply(r, st, at(701, rows("a", 1)), 0) // 660 does not
	Apply(r, st, at(702, rows("a", 5)), 0) // 720 holds: pending since 720
	if g := st.Groups[ga]; g == nil || g.Phase != Pending || g.SinceNs != 720*sec {
		t.Fatalf("%+v", st.Groups)
	}
	// late rows make 660 hold: the run is 600..720 (2 min): the live group fires
	LateApply(r, st, 660*sec, delta(800, 1), at(800, rows("a", 5)), 1000)
	g := st.Groups[ga]
	if g.Phase != Firing || g.SinceNs != 600*sec || st.Notices[NoticeKey(r.Name, ga, "720")] == nil {
		t.Fatalf("live group: %+v", g)
	}
	if st.Notices[NoticeKey(r.Name, ga, "720")].Annotations["fired_by_late_data"] == "" {
		t.Fatal("not marked")
	}
	// a past run too short for `for` fires nothing
	st2 := New(r, 600*sec)
	Apply(r, st2, at(700, rows("a", 1)), 0)
	Apply(r, st2, at(701, rows("a", 1)), 0)
	Apply(r, st2, at(702, rows("a", 1)), 0)
	LateApply(r, st2, 600*sec, delta(800, 1), at(800, rows("a", 5)), 1000)
	if len(st2.Notices) != 0 {
		t.Fatal("a one-window run fired with for 2m")
	}
}

// page: one notice when a window's verdict changed, none when it did not.
func TestLatePage(t *testing.T) {
	r := testRule(t, func(r *rule.Rule) { r.OnLate = rule.LatePage })
	st := New(r, 600*sec)
	Apply(r, st, at(700, rows("a", 1)), 0)
	LateApply(r, st, 600*sec, delta(800, 1), at(800, rows("a", 2)), 1000)
	if len(st.Notices) != 0 {
		t.Fatal("paged without a changed verdict")
	}
	LateApply(r, st, 600*sec, delta(900, 3), at(900, rows("a", 3)), 2000)
	n := st.Notices[NoticeKey(r.Name, "late_data", "600.2")]
	if n == nil || n.Kind != "late_data" || n.Want != Resolved || n.Annotations["late_rows"] != "3" || len(st.Groups) != 0 {
		t.Fatalf("%+v %+v", n, st.Groups)
	}
	sends := DueSends(st, 3000, cfg)
	if len(sends) != 1 || sends[0].Phase != Firing {
		t.Fatalf("a late notice is sent firing first: %+v", sends)
	}
}
