package completeness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

const hkey = "r/_consumer/watermark.json"

// draw is a source of ints in [0, n).
type draw func(n int) int

func buildHistory(r *Reader, scope string, steps []HistStep, d draw, objs map[string][]byte) *History {
	if len(steps) == 0 {
		return nil
	}
	byHour := map[int64][]HistStep{}
	var hours []int64
	for _, s := range steps {
		h := s.AtMs / HourMs * HourMs
		if _, ok := byHour[h]; !ok {
			hours = append(hours, h)
		}
		byHour[h] = append(byHour[h], s)
	}
	doc := &History{}
	var carry *HistStep
	for i, h := range hours {
		hh := HistHour{HourMs: h, Carry: carry, Steps: byHour[h]}
		last := hh.Steps[len(hh.Steps)-1]
		carry = &last
		if i == len(hours)-1 {
			doc.HistHour = hh
			break
		}
		if d(3) == 0 {
			doc.Sealing = append(doc.Sealing, hh) // frozen, not sealed yet
			continue
		}
		b, _ := json.Marshal(histObj{Format: 2, Scope: scope, HistHour: hh})
		objs[r.HistKey(scope, h)] = b
	}
	return doc
}

func randSteps(d draw, base int64, signals bool) []HistStep {
	n := 1 + d(30)
	at, v := base+int64(d(int(HourMs))), uint64(1+d(1000))
	lg, tr, un := v, v, v // per signal: running maxes too, at or above the scope's
	var out []HistStep
	for range n {
		s := HistStep{AtMs: at, CtNs: v}
		if signals {
			lg, tr, un = max(lg+uint64(d(500)), v), max(tr+uint64(d(500)), v), max(un+uint64(d(500)), v)
			s.Signals = map[string]uint64{"logs": lg, "traces": tr}
			s.UnlistedNs = un
		}
		out = append(out, s)
		// gaps from none (an equal stamp) to several hours
		switch d(4) {
		case 0:
		case 1:
			at += int64(d(60_000))
		default:
			at += int64(d(int(3 * HourMs)))
		}
		v += uint64(d(1000))
	}
	return out
}

func newHistReader(objs map[string][]byte, now time.Time) *Reader {
	r := NewReader(func(ctx context.Context, k string) ([]byte, error) {
		if b, ok := objs[k]; ok {
			return b, nil
		}
		return nil, nil
	}, hkey, time.Second, time.Hour)
	r.SetClock(func() time.Time { return now })
	return r
}

// brute is the reference: the largest value of a step stamped at or before t.
func brute(steps []HistStep, t int64, value func(HistStep) uint64) (uint64, bool) {
	var v uint64
	ok := false
	for _, s := range steps {
		if s.AtMs <= t {
			v, ok = max(v, value(s)), true
		}
	}
	return v, ok
}

// asOfCase: the fleet and one cluster's histories, and "as of t" for the
// fleet and for the cluster with a signal, against the reference. mut is a
// deliberate bug of the reader (0: none).
func asOfCase(d draw, mut int) error {
	base := int64(400_000) * HourMs
	now := time.UnixMilli(base + 40*HourMs)
	objs := map[string][]byte{}
	r := newHistReader(objs, now)
	r.mutant = mut
	fleet := randSteps(d, base, false)
	cl := randSteps(d, base, true)
	fd := buildHistory(r, FleetScope, fleet, d, objs)
	cd := buildHistory(r, "c1", cl, d, objs)
	fb, _ := json.Marshal(Doc{Format: 2, CompleteThroughNs: fleet[len(fleet)-1].CtNs, WallMs: uint64(now.UnixMilli()), History: fd})
	objs[hkey] = fb
	cb, _ := json.Marshal(ClusterDoc{Format: 2, Cluster: "c1", CompleteThroughNs: cl[len(cl)-1].CtNs, WallMs: uint64(now.UnixMilli()), History: cd})
	objs[r.ClusterKey("c1")] = cb
	// times: before everything, exactly at a stamp, between, after
	var t int64
	switch d(3) {
	case 0:
		s := fleet[d(len(fleet))]
		if d(2) == 0 {
			s = cl[d(len(cl))]
		}
		t = s.AtMs
	default:
		t = base - HourMs + int64(d(int(40*HourMs)))
	}
	ctx := context.Background()
	// the fleet scope
	want, ok := brute(fleet, t, func(s HistStep) uint64 { return s.CtNs })
	got, err := r.AsOf(ctx, Scope{}, t, 1000)
	switch {
	case !ok && !errors.Is(err, ErrNoHistory):
		return fmt.Errorf("fleet as of %d: no step at or before it, got %+v, %v", t, got, err)
	case ok && err != nil:
		return fmt.Errorf("fleet as of %d: %v", t, err)
	case ok && got.CompleteThroughNs != want:
		return fmt.Errorf("fleet as of %d: %d, want %d", t, got.CompleteThroughNs, want)
	case ok && got.Final != (t/HourMs*HourMs < fd.HourMs):
		return fmt.Errorf("fleet as of %d: final %v, open hour %d", t, got.Final, fd.HourMs)
	}
	// the cluster, for logs: max(the fleet's step, the cluster's logs value)
	fv, fok := brute(fleet, t, func(s HistStep) uint64 { return s.CtNs })
	cv, cok := brute(cl, t, func(s HistStep) uint64 { return s.ValueFor([]string{"logs"}) })
	got, err = r.AsOf(ctx, Scope{Clusters: []string{"c1"}, Signals: []string{"logs"}}, t, 1000)
	switch {
	case !fok && !cok:
		if !errors.Is(err, ErrNoHistory) {
			return fmt.Errorf("c1 as of %d: no history, got %+v, %v", t, got, err)
		}
	case err != nil:
		return fmt.Errorf("c1 as of %d: %v", t, err)
	case got.CompleteThroughNs != max(fv, cv) || len(got.By) != 1:
		return fmt.Errorf("c1 as of %d: %+v, want max(%d, %d)", t, got, fv, cv)
	}
	return nil
}

// The property: "as of T" is the reference's value (the step with the
// largest stamp at or before T, a step counting from its own stamp, CAST
// 34) wherever the hour sits (sealed object, frozen in the document, the
// open hour, an earlier hour by looking back), final exactly for the hours
// before the open one; per cluster the highest of the fleet's and the
// cluster's own per-signal value.
func TestAsOfProperty(t *testing.T) {
	tracetag.Covers(t, "P2C", "CAST-26", "CAST-34", "H-2", "H-5", "R-S1")
	rapid.Check(t, func(t *rapid.T) {
		d := func(n int) int { return rapid.IntRange(0, n-1).Draw(t, "d") }
		if err := asOfCase(d, 0); err != nil {
			t.Fatal(err)
		}
	})
}

// The off-by-one mutants (a step counted only after its stamp; the next step
// after T) are caught by the same cases.
func TestAsOfMutantsCaught(t *testing.T) {
	tracetag.Covers(t, "MU", "CAST-34", "H-2", "H-5")
	for _, m := range []int{mutStrict, mutNext} {
		caught := false
		for seed := int64(1); seed <= 3000 && !caught; seed++ {
			rng := rand.New(rand.NewSource(seed))
			caught = asOfCase(func(n int) int { return rng.Intn(n) }, m) != nil
		}
		if !caught {
			t.Errorf("mutant %d not caught in 3,000 cases", m)
		}
	}
}

func TestHourNamesMatchTheConsumer(t *testing.T) {
	// the same names as otap-rs wmhistory::hour_name
	for ms, want := range map[int64]string{0: "1970-01-01T00", 1_790_000_000_000 / HourMs * HourMs: "2026-09-21T14", 951_782_400_000 + 23*HourMs: "2000-02-29T23"} {
		if got := HourName(ms); got != want {
			t.Errorf("HourName(%d) = %s, want %s", ms, got, want)
		}
	}
	r := NewReader(nil, hkey, time.Second, time.Hour)
	if k := r.HistKey(FleetScope, 0); k != "r/_consumer/watermark-history/_fleet/1970-01-01T00.json" {
		t.Error(k)
	}
}

// A sealed hour is read once (immutable); a misplaced object is an error,
// never a value.
func TestHistoryHoursCachedAndChecked(t *testing.T) {
	base := int64(400_000) * HourMs
	now := time.UnixMilli(base + 3*HourMs)
	objs := map[string][]byte{}
	gets := 0
	r := NewReader(func(ctx context.Context, k string) ([]byte, error) {
		gets++
		return objs[k], nil
	}, hkey, time.Second, time.Hour)
	r.SetClock(func() time.Time { return now })
	h0 := HistHour{HourMs: base, Steps: []HistStep{{AtMs: base + 10, CtNs: 7}}}
	b, _ := json.Marshal(histObj{Format: 2, Scope: FleetScope, HistHour: h0})
	objs[r.HistKey(FleetScope, base)] = b
	doc, _ := json.Marshal(Doc{Format: 2, CompleteThroughNs: 9, WallMs: uint64(now.UnixMilli()),
		History: &History{HistHour: HistHour{HourMs: base + 2*HourMs, Carry: &h0.Steps[0], Steps: []HistStep{{AtMs: base + 2*HourMs + 5, CtNs: 9}}}}})
	objs[hkey] = doc
	ctx := context.Background()
	for range 3 {
		a, err := r.AsOf(ctx, Scope{}, base+20, 48)
		if err != nil || a.CompleteThroughNs != 7 || !a.Final {
			t.Fatalf("%+v %v", a, err)
		}
	}
	if gets != 2 { // the fleet document, the hour once
		t.Errorf("%d GETs", gets)
	}
	// hour base+1h has no object and no step: the hour before answers
	if a, err := r.AsOf(ctx, Scope{}, base+HourMs+5, 48); err != nil || a.CompleteThroughNs != 7 {
		t.Errorf("%+v %v", a, err)
	}
	// the open hour: its carry before its first step, provisional
	if a, err := r.AsOf(ctx, Scope{}, base+2*HourMs+1, 48); err != nil || a.CompleteThroughNs != 7 || a.Final {
		t.Errorf("%+v %v", a, err)
	}
	// a misplaced object
	bad, _ := json.Marshal(histObj{Format: 2, Scope: "c9", HistHour: HistHour{HourMs: base + 5*HourMs}})
	objs[r.HistKey(FleetScope, base+5*HourMs)] = bad
	doc2, _ := json.Marshal(Doc{Format: 2, CompleteThroughNs: 9, WallMs: uint64(now.UnixMilli()),
		History: &History{HistHour: HistHour{HourMs: base + 6*HourMs, Steps: []HistStep{{AtMs: base + 6*HourMs, CtNs: 9}}}}})
	objs[hkey] = doc2
	now = now.Add(time.Hour)
	if _, err := r.AsOf(ctx, Scope{}, base+5*HourMs+1, 48); err == nil {
		t.Error("a misplaced hour object must not answer")
	}
}
