package bitemp

import (
	"sort"
	"testing"

	"pgregory.net/rapid"
)

// brute resolves one point by the rule's definition, independently of
// Resolve and of the replay: the covering events as of st, ranked by an
// explicit (tier, effective time, controller first, seq) key.
func brute(events []Event, entity uint64, vt, st Time, w Time) Row {
	type ranked struct {
		k [4]int64
		e Event
	}
	var as, ns []ranked
	for _, e := range events {
		if e.SystemFrom > st || (e.Entity != entity && e.Entity != All) || vt < e.ValidFrom || vt >= e.ValidTo {
			continue
		}
		if e.Source == Announce {
			ns = append(ns, ranked{[4]int64{0, int64(e.SystemFrom), 0, int64(e.Seq)}, e})
			continue
		}
		eff, ctl := int64(e.SystemFrom), int64(0)
		if e.Source == Controller {
			eff, ctl = eff+int64(w), 1
		}
		as = append(as, ranked{[4]int64{1, eff, ctl, int64(e.Seq)}, e})
	}
	desc := func(r []ranked) {
		sort.Slice(r, func(i, j int) bool {
			for x := 0; x < 4; x++ {
				if r[i].k[x] != r[j].k[x] {
					return r[i].k[x] > r[j].k[x]
				}
			}
			return false
		})
	}
	desc(as)
	desc(ns)
	r := Row{Entity: entity, From: vt, To: vt + 1}
	if len(as) > 0 && as[0].e.Kind != Unknown {
		r.Source = as[0].e.Source
		if as[0].e.Kind == Assert {
			r.State, r.Version = Asserted, as[0].e.Version
		} else {
			r.State = Retracted
		}
		return r
	}
	if len(ns) > 0 {
		r.State, r.Version, r.Source, r.Uncertain = Asserted, ns[0].e.Version, Announce, len(as) > 0
		return r
	}
	if len(as) > 0 {
		r.State = Unknowable
	}
	return r
}

func samePoint(a, b Row) bool {
	return a.State == b.State && a.Version == b.Version && a.Uncertain == b.Uncertain &&
		(a.State == Absent || a.State == Unknowable || a.Source == b.Source)
}

// rowAt finds the row covering vt (Absent if none).
func rowAt(rows []Row, vt Time) Row {
	for _, r := range rows {
		if r.From <= vt && vt < r.To {
			return r
		}
	}
	return Row{State: Absent}
}

const W = 20

// genEvents draws an event set over a small time domain so that intervals
// overlap and ties happen.
func genEvents(t *rapid.T, maxN int) []Event {
	n := rapid.IntRange(0, maxN).Draw(t, "n")
	evs := make([]Event, 0, n)
	st := Time(0)
	for i := 0; i < n; i++ {
		st += Time(rapid.IntRange(0, 15).Draw(t, "dst"))
		src := Source(rapid.IntRange(0, 2).Draw(t, "src"))
		kind := Kind(rapid.IntRange(0, 2).Draw(t, "kind"))
		ent := uint64(rapid.IntRange(0, 3).Draw(t, "ent"))
		if src == Announce {
			kind = Assert
			if ent == All {
				ent = 1
			}
		}
		vf := Time(rapid.IntRange(0, 100).Draw(t, "vf"))
		vt := Inf
		if rapid.Bool().Draw(t, "bounded") {
			vt = vf + Time(rapid.IntRange(1, 60).Draw(t, "len"))
		}
		var v uint64
		if kind == Assert {
			v = uint64(rapid.IntRange(1, 3).Draw(t, "ver"))
		}
		evs = append(evs, Event{Entity: ent, ValidFrom: vf, ValidTo: vt, SystemFrom: st, Seq: uint64(i + 1), Source: src, Kind: kind, Version: v})
	}
	return evs
}

func maxST(evs []Event) Time {
	m := Time(0)
	for _, e := range evs {
		m = max(m, e.SystemFrom)
	}
	return m
}

// Resolve, the replay (ResolveRange) and the brute force agree at every
// point, as of every system time.
func TestResolveMatchesBrute(t *testing.T) {
	p := Policy{TrustWindow: W}
	rapid.Check(t, func(t *rapid.T) {
		evs := genEvents(t, 12)
		ent := uint64(rapid.IntRange(1, 3).Draw(t, "qent"))
		for st := Time(-1); st <= maxST(evs)+1; st++ {
			rows := Rows(evs, ent, 0, 200, st, p)
			for i := 1; i < len(rows); i++ {
				if rows[i].From < rows[i-1].To {
					t.Fatalf("rows overlap: %+v %+v", rows[i-1], rows[i])
				}
			}
			for vt := Time(0); vt < 200; vt++ {
				want := brute(evs, ent, vt, st, W)
				if got := Resolve(evs, ent, vt, st, p); !samePoint(got, want) {
					t.Fatalf("Resolve(%d, vt=%d, st=%d) = %+v, brute %+v\n%+v", ent, vt, st, got, want, evs)
				}
				if got := rowAt(rows, vt); !samePoint(got, want) {
					t.Fatalf("replay(%d, vt=%d, st=%d) = %+v, brute %+v\nrows %+v\n%+v", ent, vt, st, got, want, rows, evs)
				}
			}
		}
	})
}

// (ii): where the top authority event is Unknown the answer is Unknown or an
// uncertain announcement, never a certain assertion or retraction.
func TestUnknownNeverAsserts(t *testing.T) {
	p := Policy{TrustWindow: W}
	rapid.Check(t, func(t *rapid.T) {
		evs := genEvents(t, 12)
		ent := uint64(rapid.IntRange(1, 3).Draw(t, "qent"))
		vt := Time(rapid.IntRange(0, 150).Draw(t, "vt"))
		st := maxST(evs)
		var top *Event
		for i := range evs {
			e := &evs[i]
			if e.Source != Announce && applies(e, ent) && e.ValidFrom <= vt && vt < e.ValidTo && (top == nil || p.Higher(e, top)) {
				top = e
			}
		}
		r := Resolve(evs, ent, vt, st, p)
		if top != nil && top.Kind == Unknown && r.State != Unknowable && !(r.State == Asserted && r.Source == Announce && r.Uncertain) {
			t.Fatalf("top authority unknown but resolved %+v", r)
		}
	})
}

// (iii): events added later (SystemFrom > S) never change an answer as of S.
func TestMonotoneHistory(t *testing.T) {
	p := Policy{TrustWindow: W}
	rapid.Check(t, func(t *rapid.T) {
		evs := genEvents(t, 14)
		if len(evs) == 0 {
			return
		}
		cut := rapid.IntRange(0, len(evs)-1).Draw(t, "cut")
		S := evs[cut].SystemFrom
		var before []Event
		for _, e := range evs {
			if e.SystemFrom <= S {
				before = append(before, e)
			}
		}
		ent := uint64(rapid.IntRange(1, 3).Draw(t, "qent"))
		a, b := Rows(before, ent, 0, 200, S, p), Rows(evs, ent, 0, 200, S, p)
		if len(a) != len(b) {
			t.Fatalf("as of %d: %+v vs %+v", S, a, b)
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("as of %d: %+v vs %+v", S, a, b)
			}
		}
	})
}

// (iv): the current view equals resolution over every event at (now, now),
// as events arrive and the clock moves; and it prunes.
func TestCurrentIsResolved(t *testing.T) {
	p := Policy{TrustWindow: W}
	rapid.Check(t, func(t *rapid.T) {
		evs := genEvents(t, 20)
		c := NewCurrent(p, 0)
		var seen []Event
		for _, e := range evs {
			// the clock is at or past every arrival
			c.Advance(e.SystemFrom)
			c.Add(e)
			seen = append(seen, e)
			if rapid.Bool().Draw(t, "advance") {
				c.Advance(c.Now() + Time(rapid.IntRange(0, 40).Draw(t, "dt")))
			}
			if rapid.Bool().Draw(t, "compact") {
				c.Compact()
			}
			for ent := uint64(1); ent <= 3; ent++ {
				want := Resolve(seen, ent, c.Now(), Inf, p)
				if got := c.Get(ent); !samePoint(got, want) {
					t.Fatalf("now %d entity %d: current %+v, resolved %+v\n%+v", c.Now(), ent, got, want, seen)
				}
			}
		}
	})
}

// Early stop: an as-of-now range query over a closed range reads only the
// events above the ceiling.
func TestEarlyStop(t *testing.T) {
	p := Policy{TrustWindow: W}
	var evs []Event
	for i := 0; i < 1000; i++ { // an old history of versions
		evs = append(evs, Event{Entity: 1, ValidFrom: Time(i), ValidTo: Inf, SystemFrom: Time(i), Seq: uint64(i + 1), Source: Controller, Kind: Assert, Version: uint64(i)})
	}
	n := ResolveRange(evs, 1, 990, 1000, 5000, p, nil)
	if n > 10 {
		t.Fatalf("examined %d events, want <= 10", n)
	}
	rows := Rows(evs, 1, 990, 1000, 5000, p)
	if len(rows) != 10 || rows[0].Version != 990 || rows[9].Version != 999 {
		t.Fatalf("%+v", rows)
	}
}

// The failure cases D32 names, as in the model's scripted runs.
func TestScenarios(t *testing.T) {
	p := Policy{TrustWindow: 2}
	ev := func(seq uint64, ent uint64, vf, vt, st Time, src Source, k Kind, v uint64) Event {
		return Event{Entity: ent, ValidFrom: vf, ValidTo: vt, SystemFrom: st, Seq: seq, Source: src, Kind: k, Version: v}
	}
	at := func(evs []Event, ent uint64, vt, st Time) Row { return Resolve(evs, ent, vt, st, p) }
	// controller restart: pod 1 asserted at 0; outage [1, 3); pod 2 born and
	// dead in it, announced at 1; the new incarnation writes the restart gap
	// and its full-state sync's "nothing else exists".
	restart := []Event{
		ev(1, 1, 0, Inf, 0, Controller, Assert, 11),
		ev(2, 2, 1, 2, 1, Announce, Assert, 21),
		ev(3, All, 1, 3, 3, Controller, Unknown, 0),
		ev(4, All, 3, Inf, 3, Controller, Retract, 0),
	}
	for _, c := range []struct {
		name   string
		evs    []Event
		ent    uint64
		vt, st Time
		want   Row
	}{
		{"restart: before", restart, 1, 0, 3, Row{State: Asserted, Version: 11, Source: Controller}},
		{"restart: in the outage", restart, 1, 1, 3, Row{State: Unknowable}},
		{"restart: after the sync", restart, 1, 3, 3, Row{State: Retracted, Source: Controller}},
		{"restart: announced in the outage", restart, 2, 1, 3, Row{State: Asserted, Version: 21, Source: Announce, Uncertain: true}},
		{"restart: as of 2, pod 1 lived", restart, 1, 2, 2, Row{State: Asserted, Version: 11, Source: Controller}},
		{"overseer within the window", []Event{ev(1, 1, 0, Inf, 0, Controller, Assert, 1), ev(2, 1, 0, Inf, 1, Overseer, Assert, 2)}, 1, 0, 9,
			Row{State: Asserted, Version: 1, Source: Controller}},
		{"overseer after the window", []Event{ev(1, 1, 0, Inf, 0, Controller, Assert, 1), ev(2, 1, 0, Inf, 3, Overseer, Retract, 0)}, 1, 0, 9,
			Row{State: Retracted, Source: Overseer}},
		{"controller re-asserted by a sync", []Event{ev(1, 1, 0, Inf, 0, Controller, Assert, 1), ev(2, 1, 0, Inf, 2, Controller, Assert, 1), ev(3, 1, 0, Inf, 3, Overseer, Retract, 0)}, 1, 0, 9,
			Row{State: Asserted, Version: 1, Source: Controller}},
		{"announcement under an authority assert", []Event{ev(1, 1, 0, Inf, 0, Controller, Assert, 1), ev(2, 1, 0, Inf, 5, Announce, Assert, 2)}, 1, 0, 9,
			Row{State: Asserted, Version: 1, Source: Controller}},
	} {
		got := at(c.evs, c.ent, c.vt, c.st)
		if !samePoint(got, c.want) {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}
	// the current view keeps an announcement an authority assert hides (a
	// later authority unknown reopens it)
	cur := NewCurrent(p, 0)
	cur.Add(ev(1, 1, 0, Inf, 0, Announce, Assert, 2))
	cur.Add(ev(2, 1, 0, Inf, 0, Controller, Assert, 1))
	cur.Advance(1)
	cur.Compact()
	cur.Add(ev(3, 1, 0, Inf, 1, Controller, Unknown, 0))
	if got := cur.Get(1); !samePoint(got, Row{State: Asserted, Version: 2, Source: Announce, Uncertain: true}) {
		t.Fatalf("current: %+v", got)
	}
}

func TestIvset(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var s ivset
		in := map[Time]bool{}
		for i := rapid.IntRange(0, 8).Draw(t, "n"); i > 0; i-- {
			a := Time(rapid.IntRange(0, 50).Draw(t, "a"))
			b := a + Time(rapid.IntRange(0, 10).Draw(t, "l"))
			s.add(iv{a, b})
			for x := a; x < b; x++ {
				in[x] = true
			}
		}
		for i := 1; i < len(s); i++ {
			if s[i].a <= s[i-1].b {
				t.Fatalf("not disjoint/merged: %v", s)
			}
		}
		x := iv{Time(rapid.IntRange(0, 60).Draw(t, "xa")), 0}
		x.b = x.a + Time(rapid.IntRange(0, 10).Draw(t, "xl"))
		all := true
		for v := x.a; v < x.b; v++ {
			all = all && in[v]
		}
		if s.covers(x) != all {
			t.Fatalf("covers(%v) on %v", x, s)
		}
		for _, y := range s.subtractFrom([]iv{x}) {
			for v := y.a; v < y.b; v++ {
				if in[v] {
					t.Fatalf("subtract kept %d", v)
				}
			}
		}
		for _, y := range s.intersect(x) {
			for v := y.a; v < y.b; v++ {
				if !in[v] {
					t.Fatalf("intersect has %d", v)
				}
			}
		}
	})
}
