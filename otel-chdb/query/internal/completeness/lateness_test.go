package completeness

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func okState(ctNs int64, now time.Time) State {
	return State{Status: StatusOK, Doc: &Doc{Format: 2, CompleteThroughNs: uint64(ctNs), WallMs: uint64(now.UnixMilli())}, FetchedAt: now}
}

// TestLateRowNotComplete is CAST row 26's regression: a row whose event
// time is in the window but that is received after the window's end is not
// in central yet when complete_through reaches the window's end, so the
// window must not be labelled complete until complete_through passes its
// end + max_lateness.
func TestLateRowNotComplete(t *testing.T) {
	now := t0
	w := &Window{FromNs: t0.Add(-10 * time.Minute).UnixNano(), ToNs: t0.Add(-5 * time.Minute).UnixNano()}
	// the late row: event time a second before the window's end, received
	// 30 s after it; complete_through is 10 s past the end
	event := w.ToNs - int64(time.Second)
	received := w.ToNs + int64(30*time.Second)
	ct := w.ToNs + int64(10*time.Second)
	inCentral := received < ct
	if inCentral {
		t.Fatal("test setup: the row must still be in custody")
	}
	s := okState(ct, now)
	// before the fix (max_lateness 0 is the old behaviour) the label said complete
	if l := MakeLabel("central", s, w, now, "k", nil, 0); l.Completeness != "complete" {
		t.Fatalf("max_lateness 0 should reproduce the custody-time label: %+v", l)
	}
	l := MakeLabel("central", s, w, now, "k", nil, time.Minute)
	if l.Completeness != "partial" || !l.Partial || l.MaxLatenessS != 60 {
		t.Fatalf("late row missing, but the label says %+v", l)
	}
	// incomplete from complete_through − max_lateness, in event time: before the row
	if *l.IncompleteFromNs != ct-int64(time.Minute) || *l.IncompleteFromNs > event || *l.SettledThroughNs != ct-int64(time.Minute) {
		t.Fatalf("incomplete_from %d, settled %d, event %d", *l.IncompleteFromNs, *l.SettledThroughNs, event)
	}
	// once complete_through passes end + max_lateness, it is complete
	s = okState(w.ToNs+int64(time.Minute), now)
	if l := MakeLabel("central", s, w, now, "k", nil, time.Minute); l.Completeness != "complete" || l.Partial || l.IncompleteFrom != nil {
		t.Fatalf("%+v", l)
	}
	// one ns short is not
	s = okState(w.ToNs+int64(time.Minute)-1, now)
	if l := MakeLabel("central", s, w, now, "k", nil, time.Minute); l.Completeness != "partial" || *l.IncompleteFromNs != w.ToNs-1 {
		t.Fatalf("%+v", l)
	}
	// a window after settled_through is incomplete from its start (clamped)
	s = okState(w.FromNs+int64(30*time.Second), now)
	if l := MakeLabel("central", s, w, now, "k", nil, time.Minute); *l.IncompleteFromNs != w.FromNs {
		t.Fatalf("%+v", l)
	}
}

func TestSettledNsSaturates(t *testing.T) {
	if SettledNs(5, time.Minute) != 5-int64(time.Minute) {
		t.Fatal("small")
	}
	if SettledNs(math.MaxUint64, 0) != math.MaxInt64 {
		t.Fatal("big")
	}
	if SettledNs(0, time.Duration(math.MaxInt64)) != -math.MaxInt64 {
		t.Fatal("negative")
	}
	if SettledNs(7, -time.Second) != 7 {
		t.Fatal("negative lateness is 0")
	}
}

// TestCompleteMeansAllRowsWithinLateness is the property the label claims,
// over data whose event time and receive time diverge (the lesson of CAST
// row 26: every earlier test had them agree). Central holds exactly the
// rows received before complete_through (FORMAT.md §3). Then:
//
//   - a window labelled complete holds every row with its event time in the
//     window received before the window's end + max_lateness, so every row
//     within the policy (received − event ≤ max_lateness);
//   - a partial window holds every such row with its event time before
//     incomplete_from;
//   - the label's incomplete_from lies inside the window.
func TestCompleteMeansAllRowsWithinLateness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		sec := int64(time.Second)
		L := time.Duration(rapid.Int64Range(0, 120).Draw(t, "lateness_s")) * time.Second
		from := rapid.Int64Range(0, 600).Draw(t, "from_s") * sec
		to := from + rapid.Int64Range(1, 600).Draw(t, "len_s")*sec
		ct := rapid.Int64Range(0, 1500).Draw(t, "ct_s")*sec + rapid.Int64Range(0, sec-1).Draw(t, "ct_ns")
		type row struct{ e, r int64 }
		n := rapid.IntRange(0, 40).Draw(t, "rows")
		var rows []row
		for i := 0; i < n; i++ {
			e := rapid.Int64Range(0, 1200).Draw(t, "e_s") * sec
			// delays from a clock 5 s ahead to twice the policy and beyond
			d := rapid.Int64Range(-5*sec, 2*int64(L)+30*sec).Draw(t, "delay")
			rows = append(rows, row{e, e + d})
		}
		inCentral := func(x row) bool { return x.r < ct }
		now := time.Unix(0, 2000*sec)
		l := MakeLabel("central", okState(ct, now), &Window{FromNs: from, ToNs: to}, now, "k", nil, L)
		switch l.Completeness {
		case "complete":
			if ct < to+int64(L) {
				t.Fatalf("complete with ct %d < to %d + L %d", ct, to, L)
			}
			for _, x := range rows {
				if x.e >= from && x.e < to && x.r < to+int64(L) && !inCentral(x) {
					t.Fatalf("row %+v in the window, received before to+L, not in a complete result (ct %d)", x, ct)
				}
				if x.e >= from && x.e < to && x.r-x.e <= int64(L) && !inCentral(x) {
					t.Fatalf("row %+v within the policy, not in a complete result", x)
				}
			}
		case "partial":
			f := *l.IncompleteFromNs
			if f < from || f >= to {
				t.Fatalf("incomplete_from %d outside [%d, %d)", f, from, to)
			}
			for _, x := range rows {
				if x.e >= from && x.e < f && x.r-x.e <= int64(L) && !inCentral(x) {
					t.Fatalf("row %+v before incomplete_from %d within the policy, not in central (ct %d)", x, f, ct)
				}
			}
		default:
			t.Fatalf("label %q with an ok watermark", l.Completeness)
		}
	})
}
