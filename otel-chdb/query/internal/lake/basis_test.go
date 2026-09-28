package lake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"pgregory.net/rapid"
)

type genObj struct {
	key, cluster     string
	recv, minT, maxT int64
	lm               time.Time
	undated          bool
}

// A plan at a basis, read the way the plan's rules say (objects marked
// basis_check dropped unless their footer's custody time is below the
// bound), holds exactly the objects received before their cluster's bound
// whose rows overlap the window, whatever the HEAD budget, and the same
// objects after any number of later arrivals (new data, late data into the
// same window, objects written long after the basis).
func TestPlanAtBasisProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := store.NewMem()
		var objs []genObj
		seq := map[string]int{}
		put := func(c, prod string, recv int64, early bool) {
			lane := c + "/" + prod
			seq[lane]++
			o := genObj{key: fmt.Sprintf("lake/%s/%s/logs/%s/%020d.parquet", c, prod, ep, seq[lane]), cluster: c, recv: recv}
			o.lm = time.Unix(0, recv).Add(time.Duration(rapid.Int64Range(0, int64(3*time.Second)).Draw(rt, "put")))
			if !early && rapid.IntRange(0, 5).Draw(rt, "much-later") == 0 {
				o.lm = o.lm.Add(time.Hour) // stuck in custody, written long after
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
				base.Add(time.Duration(rapid.Int64Range(0, int64(30*time.Minute)).Draw(rt, "recv"))).UnixNano(), true)
		}
		b := &basis.Basis{Clusters: map[string]uint64{}, Signals: []string{"logs"}}
		var hi int64
		for _, c := range clusters {
			v := base.Add(time.Duration(rapid.Int64Range(0, int64(30*time.Minute)).Draw(rt, "C"))).UnixNano()
			b.Clusters[c] = uint64(v)
			hi = max(hi, v)
		}
		// issued after every C (the watermark was read before minting)
		b.IssuedNs = hi + rapid.Int64Range(0, int64(time.Minute)).Draw(rt, "iat")
		// objects are sound with respect to the basis: every object received
		// before C was written before the basis was issued
		for _, o := range objs {
			if o.recv < int64(b.Clusters[o.cluster]) && o.lm.UnixNano() > b.IssuedNs {
				return
			}
		}
		from, to := base.Add(5*time.Minute).UnixNano(), base.Add(25*time.Minute).UnixNano()
		get := func(ctx context.Context, k string) ([]byte, error) { x, _, err := m.Get(ctx, k); return x, err }
		wm := completeness.NewReader(get, "lake/_consumer/watermark.json", time.Second, time.Hour)
		p := New(Config{Root: "lake", MaxHeads: rapid.IntRange(1, n+2).Draw(rt, "heads")}, m, wm)
		p.SetClock(func() time.Time { return time.Unix(0, b.IssuedNs).Add(time.Hour) })
		want := map[string]bool{}
		for _, o := range objs {
			if o.recv < int64(b.Clusters[o.cluster]) && o.maxT >= from && o.minT < to {
				want[o.key] = true
			}
		}
		byKey := map[string]genObj{}
		read := func() map[string]bool {
			byKey = map[string]genObj{}
			for _, o := range objs {
				byKey[o.key] = o
			}
			pl, err := p.Plan(context.Background(), fleet, Request{Signal: "logs", FromNs: from, ToNs: to, Basis: b})
			if err != nil {
				rt.Fatal(err)
			}
			got := map[string]bool{}
			for _, po := range pl.Objects {
				o := byKey[po.Key]
				if po.BasisCheck {
					if po.ReceivedBeforeNs == nil || *po.ReceivedBeforeNs != b.Clusters[o.cluster] {
						rt.Fatalf("basis_check without the bound: %+v", po)
					}
					if o.recv >= int64(*po.ReceivedBeforeNs) {
						continue // the footer says: after the basis
					}
				} else if o.recv >= int64(b.Clusters[o.cluster]) {
					rt.Fatalf("planned %s received after the basis without a check", po.Key)
				}
				if o.maxT >= from && o.minT < to { // the reader's own row filter
					got[po.Key] = true
				}
			}
			return got
		}
		check := func(got map[string]bool, when string) {
			if len(got) != len(want) {
				rt.Fatalf("%s: got %v, want %v", when, got, want)
			}
			for k := range want {
				if !got[k] {
					rt.Fatalf("%s: missing %s", when, k)
				}
			}
		}
		check(read(), "first")
		// later arrivals: received at or after each cluster's bound
		for i := rapid.IntRange(0, 8).Draw(rt, "more"); i > 0; i-- {
			c := rapid.SampledFrom(clusters).Draw(rt, "c2")
			put(c, rapid.SampledFrom(prods).Draw(rt, "p2"), int64(b.Clusters[c])+rapid.Int64Range(0, int64(10*time.Minute)).Draw(rt, "after"), false)
		}
		check(read(), "after new data")
	})
}
