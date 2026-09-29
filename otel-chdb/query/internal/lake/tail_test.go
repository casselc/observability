package lake

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"pgregory.net/rapid"
)

// flakyHead fails the HEAD of some keys (the planner then cannot date them).
type flakyHead struct {
	*store.Mem
	fail map[string]bool
}

func (f *flakyHead) Head(ctx context.Context, k string) (map[string]string, error) {
	if f.fail[k] {
		return nil, errors.New("HEAD: injected failure")
	}
	return f.Mem.Head(ctx, k)
}

// A plan with tail (AMBIGUITY.md #10 (b)), read the way its rules say:
//
//   - every listed data object whose rows may overlap the window is in
//     exactly one of objects (the basis part) and tail_objects;
//   - nothing in the basis part was received at or after its cluster's
//     bound, and nothing there needs a footer check: an object the planner
//     cannot date is in the tail with basis_check, never in the basis part;
//   - the basis answer (the basis part, plus tail objects with basis_check
//     whose footer says received below the bound) holds exactly the objects
//     received before the bound whose rows overlap the window, whatever
//     the HEAD budget and whichever HEADs fail, and the same objects after
//     any number of later arrivals; the tail answer is the rest;
//   - with every HEAD answered, the basis part's object list and
//     objects_hash do not change when data arrives (a later arrival never
//     takes the HEAD budget of an object the earlier plan dated).
func TestPlanTailProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := store.NewMem()
		st := &flakyHead{Mem: m, fail: map[string]bool{}}
		var objs []genObj
		seq := map[string]int{}
		var lastLM time.Time
		put := func(c, prod string, recv int64, arrival bool) {
			lane := c + "/" + prod
			seq[lane]++
			o := genObj{key: fmt.Sprintf("lake/%s/%s/logs/%s/%020d.parquet", c, prod, ep, seq[lane]), cluster: c, recv: recv}
			o.lm = time.Unix(0, recv).Add(time.Duration(rapid.Int64Range(0, int64(3*time.Second)).Draw(rt, "put")))
			if arrival {
				if rapid.IntRange(0, 5).Draw(rt, "much-later") == 0 {
					o.lm = o.lm.Add(time.Hour) // stuck in custody, written long after
				}
				// a later PUT has a later LastModified than every object an
				// earlier LIST saw (the store's clock)
				if !o.lm.After(lastLM) {
					o.lm = lastLM.Add(time.Millisecond)
				}
			}
			if o.lm.After(lastLM) {
				lastLM = o.lm
			}
			o.minT = recv - rapid.Int64Range(0, int64(20*time.Minute)).Draw(rt, "late")
			o.maxT = o.minT + rapid.Int64Range(0, recv+int64(time.Minute)-o.minT).Draw(rt, "span")
			o.undated = rapid.IntRange(0, 4).Draw(rt, "undated") == 0
			meta := map[string]string{"oscope-kind": "data", "oscope-cluster": c}
			if !o.undated {
				meta["oscope-rows"] = "1"
				meta["oscope-min-time"] = fmt.Sprint(o.minT)
				meta["oscope-max-time"] = fmt.Sprint(o.maxT)
				meta["oscope-received"] = fmt.Sprint(o.recv)
			}
			m.Put(o.key, make([]byte, 10), meta, o.lm)
			objs = append(objs, o)
		}
		clusters := []string{"c1", "c2"}
		prods := []string{"p0", "p1"}
		n := rapid.IntRange(0, 12).Draw(rt, "n")
		for i := 0; i < n; i++ {
			put(rapid.SampledFrom(clusters).Draw(rt, "c"), rapid.SampledFrom(prods).Draw(rt, "p"),
				base.Add(time.Duration(rapid.Int64Range(0, int64(30*time.Minute)).Draw(rt, "recv"))).UnixNano(), false)
		}
		b := &basis.Basis{Clusters: map[string]uint64{}, Signals: []string{"logs"}}
		var hi int64
		for _, c := range clusters {
			v := base.Add(time.Duration(rapid.Int64Range(0, int64(30*time.Minute)).Draw(rt, "C"))).UnixNano()
			b.Clusters[c] = uint64(v)
			hi = max(hi, v)
		}
		b.IssuedNs = hi + rapid.Int64Range(0, int64(time.Minute)).Draw(rt, "iat")
		for _, o := range objs {
			if o.recv < int64(b.Clusters[o.cluster]) && o.lm.UnixNano() > b.IssuedNs {
				return // not sound with respect to the basis (see TestPlanAtBasisProperty)
			}
		}
		from, to := base.Add(5*time.Minute).UnixNano(), base.Add(25*time.Minute).UnixNano()
		get := func(ctx context.Context, k string) ([]byte, error) { x, _, err := m.Get(ctx, k); return x, err }
		wm := completeness.NewReader(get, "lake/_consumer/watermark.json", time.Second, time.Hour)
		p := New(Config{Root: "lake", MaxHeads: rapid.IntRange(1, n+2).Draw(rt, "heads")}, st, wm)
		p.SetClock(func() time.Time { return time.Unix(0, b.IssuedNs).Add(time.Hour) })
		flaky := rapid.Bool().Draw(rt, "flaky")
		overlaps := func(o genObj) bool { return o.maxT >= from && o.minT < to }

		type answer struct {
			basisKeys, tailKeys map[string]bool // after the reader's footer rule and row filter
			hash                string
			list                []string // the basis part's keys, as planned
		}
		read := func() answer {
			st.fail = map[string]bool{}
			if flaky {
				for _, o := range objs {
					if rapid.IntRange(0, 3).Draw(rt, "fail") == 0 {
						st.fail[o.key] = true
					}
				}
			}
			byKey := map[string]genObj{}
			for _, o := range objs {
				byKey[o.key] = o
			}
			pl, err := p.Plan(context.Background(), fleet, Request{Signal: "logs", FromNs: from, ToNs: to, Basis: b, Tail: true})
			if err != nil {
				rt.Fatal(err)
			}
			if pl.Tail == nil || pl.Tail.Completeness != "incomplete" || pl.Tail.Cache != "never" || pl.AfterBasis != 0 || pl.BasisUnverified != 0 {
				rt.Fatalf("tail label: %+v after_basis %d", pl.Tail, pl.AfterBasis)
			}
			a := answer{basisKeys: map[string]bool{}, tailKeys: map[string]bool{}, hash: pl.ObjectsHash}
			seen := map[string]bool{}
			for _, po := range pl.Objects {
				o := byKey[po.Key]
				seen[po.Key] = true
				a.list = append(a.list, po.Key)
				if po.Tail || po.BasisCheck {
					rt.Fatalf("basis part holds a tail or undated object: %+v", po)
				}
				if o.recv >= int64(b.Clusters[o.cluster]) {
					rt.Fatalf("basis part holds %s, received at or after the bound", po.Key)
				}
				if overlaps(o) {
					a.basisKeys[po.Key] = true
				}
			}
			unplaced := 0
			for _, po := range pl.TailObjects {
				o := byKey[po.Key]
				if seen[po.Key] {
					rt.Fatalf("%s in both parts", po.Key)
				}
				seen[po.Key] = true
				if !po.Tail {
					rt.Fatalf("tail object not marked: %+v", po)
				}
				inBasis := false
				if po.BasisCheck {
					unplaced++
					if po.ReceivedBeforeNs == nil || *po.ReceivedBeforeNs != b.Clusters[o.cluster] {
						rt.Fatalf("basis_check without the bound: %+v", po)
					}
					inBasis = o.recv < int64(*po.ReceivedBeforeNs) // the footer places it
				} else if o.recv < int64(b.Clusters[o.cluster]) {
					rt.Fatalf("tail holds %s, dated below the bound, without a check", po.Key)
				}
				if overlaps(o) {
					if inBasis {
						a.basisKeys[po.Key] = true
					} else {
						a.tailKeys[po.Key] = true
					}
				}
			}
			if unplaced != pl.Tail.Unplaced || len(pl.TailObjects) != pl.Tail.Objects {
				rt.Fatalf("tail counts: %+v", pl.Tail)
			}
			// nothing that may hold window rows is missing from both parts
			for _, o := range objs {
				if overlaps(o) && !seen[o.key] {
					rt.Fatalf("%s planned in neither part", o.key)
				}
			}
			return a
		}
		want := func() (bm, tm map[string]bool) {
			bm, tm = map[string]bool{}, map[string]bool{}
			for _, o := range objs {
				if !overlaps(o) {
					continue
				}
				if o.recv < int64(b.Clusters[o.cluster]) {
					bm[o.key] = true
				} else {
					tm[o.key] = true
				}
			}
			return bm, tm
		}
		same := func(got, want map[string]bool, what string) {
			if len(got) != len(want) {
				rt.Fatalf("%s: got %v, want %v", what, got, want)
			}
			for k := range want {
				if !got[k] {
					rt.Fatalf("%s: missing %s", what, k)
				}
			}
		}
		first := read()
		wb, wt := want()
		same(first.basisKeys, wb, "basis, first")
		same(first.tailKeys, wt, "tail, first")
		for i := rapid.IntRange(0, 8).Draw(rt, "more"); i > 0; i-- {
			c := rapid.SampledFrom(clusters).Draw(rt, "c2")
			put(c, rapid.SampledFrom(prods).Draw(rt, "p2"), int64(b.Clusters[c])+rapid.Int64Range(0, int64(10*time.Minute)).Draw(rt, "after"), true)
		}
		later := read()
		same(later.basisKeys, wb, "basis, after new data") // the same basis, the same basis answer
		_, wt = want()
		same(later.tailKeys, wt, "tail, after new data")
		if !flaky && later.hash != first.hash {
			rt.Fatalf("basis part changed with arrivals: %v then %v", first.list, later.list)
		}
	})
}
