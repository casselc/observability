package bitemp

import (
	"testing"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
	"pgregory.net/rapid"
)

// Names are versions 1..3 (genEvents); pseudonyms are 100 and up.
const pseudoBase = 100

// genWithCorrections is genEvents with corrections (O-G9) mixed in: a
// Steward Pseudonymise for a named entity, sometimes several for one entity
// (a duplicate or re-keyed signal), at arrival order like the rest.
func genWithCorrections(t *rapid.T, maxN int) []Event {
	evs := genEvents(t, maxN)
	n := rapid.IntRange(0, 4).Draw(t, "corrections")
	for i := 0; i < n; i++ {
		at := rapid.IntRange(0, len(evs)).Draw(t, "at")
		st := Time(0)
		if at > 0 {
			st = evs[at-1].SystemFrom
		}
		c := Event{Entity: uint64(rapid.IntRange(1, 3).Draw(t, "pent")), ValidFrom: 0, ValidTo: Inf, SystemFrom: st,
			Source: Steward, Kind: Pseudonymise, Version: uint64(pseudoBase + rapid.IntRange(0, 2).Draw(t, "pver"))}
		evs = append(evs[:at], append([]Event{c}, evs[at:]...)...)
	}
	for i := range evs { // arrival order: unique seqs
		evs[i].Seq = uint64(i + 1)
	}
	return evs
}

func withoutCorrections(evs []Event) []Event {
	var out []Event
	for _, e := range evs {
		if e.Kind != Pseudonymise {
			out = append(out, e)
		}
	}
	return out
}

// pseudonymHidesName, stateUnchanged and the stable pseudonym (model (v),
// (viii), (x)): once a correction is recorded, no answer at ANY basis (also
// one before the correction) shows a name of that entity; the answer is the
// uncorrected one with the first correction's pseudonym in place of the
// version, and nothing else changes; entities without a correction are
// untouched. Resolve, the replay and the brute force agree.
func TestPseudonymHidesNameAtEveryBasis(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "R-G9", "LS-G8", "UCA-G7")
	p := Policy{TrustWindow: W}
	rapid.Check(t, func(t *rapid.T) {
		evs := genWithCorrections(t, 14)
		life := withoutCorrections(evs)
		for ent := uint64(1); ent <= 3; ent++ {
			first, corrected := FirstCorrection(evs, ent)
			for st := Time(0); st <= maxST(evs)+1; st++ {
				rows := RowsNamed(evs, ent, 0, 170, st, p)
				for vt := Time(0); vt < 170; vt += 3 {
					raw := brute(life, ent, vt, st, W)
					got := ResolveNamed(evs, ent, vt, st, p)
					if got.State != raw.State || got.Source != raw.Source || got.Uncertain != raw.Uncertain {
						t.Fatalf("entity %d vt %d st %d: the correction changed the lifecycle: %+v, uncorrected %+v", ent, vt, st, got, raw)
					}
					want := raw
					if corrected && raw.State == Asserted {
						want.Version = first.Version
						if got.Version < pseudoBase || !got.Pseudonymised {
							t.Fatalf("entity %d vt %d st %d (corrected at st %d): a name resolves: %+v", ent, vt, st, first.SystemFrom, got)
						}
					} else if got.Pseudonymised {
						t.Fatalf("entity %d vt %d st %d: pseudonymised without a correction: %+v", ent, vt, st, got)
					}
					if !samePoint(got, want) {
						t.Fatalf("entity %d vt %d st %d: %+v, want %+v", ent, vt, st, got, want)
					}
					if r := rowAt(rows, vt); !samePoint(r, want) || r.Pseudonymised != got.Pseudonymised {
						t.Fatalf("entity %d vt %d st %d: replay %+v, want %+v", ent, vt, st, r, want)
					}
				}
			}
		}
	})
}

// Late and duplicate signals (CAST 50, 74; model (ix) idempotent): a
// departure signal applied again, later, under another key, or a name fact
// arriving after the correction (Graph delta lag), changes no answer that
// was pseudonymised; the order events are handed over in does not matter.
func TestPseudonymiseIsIdempotent(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "R-G9", "H-E8")
	p := Policy{TrustWindow: W}
	rapid.Check(t, func(t *rapid.T) {
		evs := genWithCorrections(t, 12)
		ent := uint64(rapid.IntRange(1, 3).Draw(t, "ent"))
		if _, ok := FirstCorrection(evs, ent); !ok {
			evs = append(evs, Event{Entity: ent, ValidTo: Inf, SystemFrom: maxST(evs), Seq: uint64(len(evs) + 1),
				Source: Steward, Kind: Pseudonymise, Version: pseudoBase})
		}
		top := maxST(evs)
		before := map[[2]Time]Row{}
		for st := Time(0); st <= top; st++ {
			for vt := Time(0); vt < 170; vt += 5 {
				before[[2]Time{st, vt}] = ResolveNamed(evs, ent, vt, st, p)
			}
		}
		// the same signal again, later, maybe under a rotated key; and a late name
		later := top + 1 + Time(rapid.IntRange(0, 5).Draw(t, "later")) // arrives after every basis compared
		more := append(append([]Event(nil), evs...),
			Event{Entity: ent, ValidTo: Inf, SystemFrom: later, Seq: uint64(len(evs) + 1), Source: Steward, Kind: Pseudonymise,
				Version: uint64(pseudoBase + rapid.IntRange(0, 2).Draw(t, "dupver"))},
			Event{Entity: ent, ValidFrom: 0, ValidTo: Inf, SystemFrom: later, Seq: uint64(len(evs) + 2), Source: Controller, Kind: Assert, Version: 1})
		// any order
		perm := rapid.Permutation(more).Draw(t, "perm")
		for st := Time(0); st <= top; st++ {
			for vt := Time(0); vt < 170; vt += 5 {
				b := before[[2]Time{st, vt}]
				for _, set := range [][]Event{more, perm} {
					if got := ResolveNamed(set, ent, vt, st, p); !samePoint(got, b) || got.Pseudonymised != b.Pseudonymised {
						t.Fatalf("vt %d st %d: %+v after a duplicate signal and a late name, %+v before", vt, st, got, b)
					}
				}
			}
		}
		// as of now, the late name is pseudonymised too
		for vt := Time(0); vt < 170; vt += 5 {
			if got := ResolveNamed(more, ent, vt, later, p); got.State == Asserted && got.Version < pseudoBase {
				t.Fatalf("vt %d: the late name resolves: %+v", vt, got)
			}
		}
	})
}

// The current view (model (iv) with corrections): Get equals ResolveNamed
// over every event at (now, now), and corrections are never pruned.
func TestCurrentIsPseudonymised(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "R-G9")
	p := Policy{TrustWindow: W}
	rapid.Check(t, func(t *rapid.T) {
		evs := genWithCorrections(t, 20)
		c := NewCurrent(p, 0)
		var seen []Event
		for _, e := range evs {
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
				want := ResolveNamed(seen, ent, c.Now(), Inf, p)
				if got := c.Get(ent); !samePoint(got, want) || got.Pseudonymised != want.Pseudonymised {
					t.Fatalf("now %d entity %d: current %+v, resolved %+v\n%+v", c.Now(), ent, got, want, seen)
				}
			}
		}
	})
}

// The departure as a story (the model's departureDesignTest): two people
// named at t0; person 1 deleted in Graph at 20 and pseudonymised at st 30.
func TestDepartureScenario(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "R-G9", "LS-G8")
	p := Policy{TrustWindow: W}
	evs := []Event{
		{Entity: 1, ValidTo: Inf, SystemFrom: 0, Seq: 1, Source: Controller, Kind: Assert, Version: 1},
		{Entity: 2, ValidTo: Inf, SystemFrom: 0, Seq: 2, Source: Controller, Kind: Assert, Version: 2},
		{Entity: 1, ValidFrom: 20, ValidTo: Inf, SystemFrom: 20, Seq: 3, Source: Controller, Kind: Retract},
		{Entity: 1, ValidTo: Inf, SystemFrom: 30, Seq: 4, Source: Steward, Kind: Pseudonymise, Version: 101},
	}
	check := func(name string, got Row, state State, ver uint64, pseu bool) {
		t.Helper()
		if got.State != state || got.Version != ver || got.Pseudonymised != pseu {
			t.Fatalf("%s: %+v, want %v version %d pseudonymised %v", name, got, state, ver, pseu)
		}
	}
	check("before departure, as of now", ResolveNamed(evs, 1, 5, 40, p), Asserted, 101, true)
	check("before departure, at an old basis", ResolveNamed(evs, 1, 5, 10, p), Asserted, 101, true)
	check("what the old basis said then", Resolve(evs, 1, 5, 10, p), Asserted, 1, false)
	check("after departure", ResolveNamed(evs, 1, 25, 40, p), Retracted, 0, false)
	check("the other person", ResolveNamed(evs, 2, 5, 40, p), Asserted, 2, false)
	// a late Graph delta still naming person 1, and a second signal under another key
	evs = append(evs,
		Event{Entity: 1, ValidTo: Inf, SystemFrom: 35, Seq: 5, Source: Controller, Kind: Assert, Version: 1},
		Event{Entity: 1, ValidTo: Inf, SystemFrom: 36, Seq: 6, Source: Steward, Kind: Pseudonymise, Version: 102})
	check("late name", ResolveNamed(evs, 1, 25, 40, p), Asserted, 101, true)
	check("old basis after the duplicate", ResolveNamed(evs, 1, 5, 10, p), Asserted, 101, true)
}
