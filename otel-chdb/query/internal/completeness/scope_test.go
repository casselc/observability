package completeness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// multiStore serves documents by key.
type multiStore struct {
	docs  map[string][]byte
	errs  map[string]error
	reads map[string]int
}

func (m *multiStore) get(_ context.Context, key string) ([]byte, error) {
	if m.reads == nil {
		m.reads = map[string]int{}
	}
	m.reads[key]++
	if err := m.errs[key]; err != nil {
		return nil, err
	}
	return m.docs[key], nil
}

const fleetKey = "otel/_consumer/watermark.json"

func js(v any) []byte { b, _ := json.Marshal(v); return b }

// twoClusters: c2's logs lane is stalled at 10 s; c1 is current.
func twoClusters(now time.Time) *multiStore {
	s := func(x int) uint64 { return uint64(now.Add(time.Duration(x-100) * time.Second).UnixNano()) }
	wall := uint64(now.Add(-5 * time.Second).UnixMilli())
	return &multiStore{docs: map[string][]byte{
		fleetKey: js(Doc{Format: 2, CompleteThroughNs: s(10), WallMs: wall, ListCapNs: s(97),
			Holding:  []LaneWm{{Lane: "c2/p0/logs", WmNs: s(10)}, {Lane: "c1/p0/traces", WmNs: s(90)}},
			Stale:    []LaneWm{{Lane: "c2/p0/logs", WmNs: s(10)}},
			Clusters: map[string]uint64{"c1": s(90), "c2": s(10)}, Signals: map[string]uint64{"logs": s(10), "traces": s(90)},
			UnlistedSignalsNs: s(97)}),
		"otel/_consumer/watermark/c1.json": js(ClusterDoc{Format: 2, Cluster: "c1", CompleteThroughNs: s(90), WallMs: wall,
			Signals: map[string]uint64{"logs": s(95), "traces": s(90)}, UnlistedSignalsNs: s(97),
			Holding: []LaneWm{{Lane: "c1/p0/traces", WmNs: s(90)}, {Lane: "c1/p0/logs", WmNs: s(95)}}}),
		"otel/_consumer/watermark/c2.json": js(ClusterDoc{Format: 2, Cluster: "c2", CompleteThroughNs: s(10), WallMs: wall,
			Signals: map[string]uint64{"logs": s(10)}, UnlistedSignalsNs: s(97),
			Holding: []LaneWm{{Lane: "c2/p0/logs", WmNs: s(10)}}, Stale: []LaneWm{{Lane: "c2/p0/logs", WmNs: s(10)}}}),
	}}
}

func TestScopedWatermark(t *testing.T) {
	now := t0
	at := func(x int) uint64 { return uint64(now.Add(time.Duration(x-100) * time.Second).UnixNano()) }
	m := twoClusters(now)
	r := NewReader(m.get, fleetKey, 15*time.Second, 5*time.Minute)
	r.SetClock(func() time.Time { return now })
	ctx := context.Background()
	cases := []struct {
		name     string
		sc       Scope
		want     uint64
		basis    []string
		holding0 string
	}{
		{"fleet", Scope{}, at(10), nil, "c2/p0/logs"},
		{"fleet traces", Scope{Signals: []string{"traces"}}, at(90), nil, "c1/p0/traces"},
		{"fleet, a signal no lane carries", Scope{Signals: []string{"metrics_series"}}, at(97), nil, ""},
		{"c1", Scope{Clusters: []string{"c1"}}, at(90), []string{"cluster"}, "c1/p0/traces"},
		{"c1 logs", Scope{Clusters: []string{"c1"}, Signals: []string{"logs"}}, at(95), []string{"cluster_signals"}, "c1/p0/logs"},
		{"c2 traces: no traces lane in c2", Scope{Clusters: []string{"c2"}, Signals: []string{"traces"}}, at(97), []string{"cluster_signals"}, ""},
		{"c1 and c2: the stalled one holds it", Scope{Clusters: []string{"c1", "c2"}, Signals: []string{"logs"}}, at(10),
			[]string{"cluster_signals", "cluster_signals"}, "c2/p0/logs"},
		{"c3: no lane at the last LIST", Scope{Clusters: []string{"c3"}}, at(97), []string{"unlisted"}, ""},
	}
	for _, tc := range cases {
		s := r.For(ctx, tc.sc)
		if s.Status != StatusOK || s.Doc.CompleteThroughNs != tc.want {
			t.Errorf("%s: status %s, complete_through %d, want %d", tc.name, s.Status, s.Doc.CompleteThroughNs, tc.want)
			continue
		}
		var basis []string
		for _, b := range s.Scope.By {
			basis = append(basis, b.Basis)
			if b.CompleteThroughNs < tc.want {
				t.Errorf("%s: cluster %s below the scope's value", tc.name, b.Cluster)
			}
		}
		if !slices.Equal(basis, tc.basis) {
			t.Errorf("%s: basis %v, want %v", tc.name, basis, tc.basis)
		}
		h := ""
		if len(s.Doc.Holding) > 0 {
			h = s.Doc.Holding[0].Lane
		}
		if h != tc.holding0 {
			t.Errorf("%s: holding %v, want %q first", tc.name, s.Doc.Holding, tc.holding0)
		}
		for _, l := range append(s.Doc.Holding, s.Doc.Stale...) {
			c, _, _ := strings.Cut(l.Lane, "/")
			if tc.sc.Clusters != nil && !slices.Contains(tc.sc.Clusters, c) {
				t.Errorf("%s: a lane outside the scope: %s", tc.name, l.Lane)
			}
		}
	}
	// the label: c1's window is complete while c2 (and the fleet) is not
	w := &Window{FromNs: int64(at(30)), ToNs: int64(at(60))}
	if l := MakeLabel("central", r.For(ctx, Scope{Clusters: []string{"c1"}, Signals: []string{"logs"}}), w, now, r.Key(), nil, 30*time.Second); l.Completeness != "complete" || l.Watermark.Scope == nil {
		t.Fatalf("c1: %+v", l)
	}
	for _, sc := range []Scope{{Clusters: []string{"c2"}}, {}, {Clusters: []string{"c1", "c2"}}} {
		if l := MakeLabel("central", r.For(ctx, sc), w, now, r.Key(), nil, 30*time.Second); l.Completeness != "partial" {
			t.Fatalf("%v: %+v", sc, l)
		}
	}
	// the cluster documents are cached like the fleet one
	if n := m.reads["otel/_consumer/watermark/c1.json"]; n != 1 {
		t.Fatalf("c1's document read %d times within the cache window", n)
	}
}

// A per-cluster document that is missing (a consumer before D29), that
// cannot be read, or that names another cluster falls back to the fleet
// document: never above it, never nothing.
func TestScopedWatermarkFallback(t *testing.T) {
	now := t0
	at := func(x int) uint64 { return uint64(now.Add(time.Duration(x-100) * time.Second).UnixNano()) }
	ctx := context.Background()
	m := twoClusters(now)
	// a consumer before D29: no per-cluster values anywhere
	var old Doc
	_ = json.Unmarshal(m.docs[fleetKey], &old)
	old.Clusters, old.Signals, old.UnlistedSignalsNs = nil, nil, 0
	m.docs = map[string][]byte{fleetKey: js(old)}
	r := NewReader(m.get, fleetKey, 15*time.Second, 5*time.Minute)
	r.SetClock(func() time.Time { return now })
	s := r.For(ctx, Scope{Clusters: []string{"c1"}, Signals: []string{"logs"}})
	if s.Doc.CompleteThroughNs != at(10) || s.Scope.By[0].Basis != "fleet" {
		t.Fatalf("old consumer: %d %+v", s.Doc.CompleteThroughNs, s.Scope.By)
	}
	if s.Doc.Holding != nil {
		t.Fatalf("c2's lane named to a c1 scope: %+v", s.Doc.Holding)
	}
	// unreadable, and misplaced
	m = twoClusters(now)
	m.errs = map[string]error{"otel/_consumer/watermark/c1.json": errors.New("503")}
	m.docs["otel/_consumer/watermark/c2.json"] = m.docs["otel/_consumer/watermark/c1.json"]
	r = NewReader(m.get, fleetKey, 15*time.Second, 5*time.Minute)
	r.SetClock(func() time.Time { return now })
	s = r.For(ctx, Scope{Clusters: []string{"c1"}, Signals: []string{"logs"}})
	if s.Doc.CompleteThroughNs != at(90) || s.Scope.By[0].Basis != "cluster" || s.Scope.By[0].Error == "" {
		t.Fatalf("unreadable: the fleet document's value for c1: %d %+v", s.Doc.CompleteThroughNs, s.Scope.By)
	}
	s = r.For(ctx, Scope{Clusters: []string{"c2"}})
	if s.Doc.CompleteThroughNs != at(10) || !strings.Contains(s.Scope.By[0].Error, "names cluster") {
		t.Fatalf("misplaced: %d %+v", s.Doc.CompleteThroughNs, s.Scope.By)
	}
	// no fleet document: unknown, whatever the cluster documents say
	m = twoClusters(now)
	delete(m.docs, fleetKey)
	r = NewReader(m.get, fleetKey, 15*time.Second, 5*time.Minute)
	r.SetClock(func() time.Time { return now })
	if s := r.For(ctx, Scope{Clusters: []string{"c1"}}); s.Status != StatusMissing || s.Doc != nil {
		t.Fatalf("no fleet document: %+v", s)
	}
	// a stale fleet document: stale, whatever the cluster documents say
	m = twoClusters(now)
	r = NewReader(m.get, fleetKey, 15*time.Second, 5*time.Minute)
	later := now.Add(10 * time.Minute)
	r.SetClock(func() time.Time { return later })
	if s := r.For(ctx, Scope{Clusters: []string{"c1"}}); s.Status != StatusStale {
		t.Fatalf("stale: %s", s.Status)
	}
}

// The consumer's rule for one run (watermark.rs compute_doc /
// compute_cluster without a previous document), as the property's model.
func publish(lanes map[string]uint64, cap uint64) (Doc, map[string]ClusterDoc) {
	d := Doc{Format: 2, CompleteThroughNs: cap, ListCapNs: cap, UnlistedSignalsNs: cap, Clusters: map[string]uint64{}, Signals: map[string]uint64{}}
	cds := map[string]ClusterDoc{}
	for l, w := range lanes {
		parts := strings.Split(l, "/")
		c, sig := parts[0], parts[2]
		d.CompleteThroughNs = min(d.CompleteThroughNs, w)
		if _, ok := d.Clusters[c]; !ok {
			d.Clusters[c] = cap
			cds[c] = ClusterDoc{Cluster: c, CompleteThroughNs: cap, UnlistedSignalsNs: cap, Signals: map[string]uint64{}}
		}
		d.Clusters[c] = min(d.Clusters[c], w)
		if _, ok := d.Signals[sig]; !ok {
			d.Signals[sig] = cap
		}
		d.Signals[sig] = min(d.Signals[sig], w)
		cd := cds[c]
		cd.CompleteThroughNs = min(cd.CompleteThroughNs, w)
		if _, ok := cd.Signals[sig]; !ok {
			cd.Signals[sig] = cap
		}
		cd.Signals[sig] = min(cd.Signals[sig], w)
		cds[c] = cd
	}
	return d, cds
}

// The scoped value is sound (never above the lowest lane of the scope, nor
// the cap) and never below the fleet value, whatever per-cluster documents
// are missing or unreadable.
func TestScopedWatermarkProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		now := t0
		clusters := []string{"a", "b", "c"}
		signals := []string{"logs", "traces", "metrics_series"}
		cap := uint64(now.UnixNano())
		lanes := map[string]uint64{}
		n := rapid.IntRange(0, 12).Draw(t, "lanes")
		for i := 0; i < n; i++ {
			l := fmt.Sprintf("%s/p%d/%s", rapid.SampledFrom(clusters).Draw(t, "c"), rapid.IntRange(0, 2).Draw(t, "p"), rapid.SampledFrom(signals).Draw(t, "s"))
			lanes[l] = cap - uint64(rapid.IntRange(0, 1000).Draw(t, "lag"))*uint64(time.Second)
		}
		d, cds := publish(lanes, cap)
		d.WallMs = uint64(now.UnixMilli())
		m := &multiStore{docs: map[string][]byte{fleetKey: js(d)}, errs: map[string]error{}}
		for c, cd := range cds {
			key := "otel/_consumer/watermark/" + c + ".json"
			switch rapid.IntRange(0, 3).Draw(t, "doc") {
			case 0: // missing
			case 1:
				m.errs[key] = errors.New("unreadable")
			default:
				m.docs[key] = js(cd)
			}
		}
		r := NewReader(m.get, fleetKey, 15*time.Second, 5*time.Minute)
		r.SetClock(func() time.Time { return now })
		var sc Scope
		if rapid.Bool().Draw(t, "restricted") {
			sc.Clusters = rapid.SliceOfNDistinct(rapid.SampledFrom(clusters), 1, 3, rapid.ID[string]).Draw(t, "clusters")
		}
		if rapid.Bool().Draw(t, "bySignal") {
			sc.Signals = rapid.SliceOfNDistinct(rapid.SampledFrom(signals), 1, 3, rapid.ID[string]).Draw(t, "signals")
		}
		truth := cap
		for l, w := range lanes {
			parts := strings.Split(l, "/")
			if (sc.Clusters == nil || slices.Contains(sc.Clusters, parts[0])) && (sc.Signals == nil || slices.Contains(sc.Signals, parts[2])) {
				truth = min(truth, w)
			}
		}
		got := r.For(context.Background(), sc).Doc.CompleteThroughNs
		if got > truth {
			t.Fatalf("unsound: %d above the scope's lowest lane %d (lanes %v, scope %+v)", got, truth, lanes, sc)
		}
		if got < d.CompleteThroughNs {
			t.Fatalf("below the fleet value: %d < %d", got, d.CompleteThroughNs)
		}
		if got == math.MaxUint64 {
			t.Fatal("no value")
		}
	})
}
