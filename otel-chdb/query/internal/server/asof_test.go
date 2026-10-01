package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// POST /v1/basis with as_of (D29 amendment 2026-10-01): the basis is the
// watermark's history at that wall time, per cluster the highest of the
// fleet's and the cluster's own (per signal) step; final for a sealed hour,
// provisional in the open one; an answer at it reads exactly the rows
// received before those values. Refusals: in the future, before the
// history, a bad value.
func TestBasisAsOf(t *testing.T) {
	tracetag.Covers(t, "P2C", "CAST-26", "H-2", "H-5", "R-S1")
	f := newBasisFixture(t)
	fleet := f.token(fleetClaims)
	ms := func(x time.Time) int64 { return x.UnixMilli() }
	ns := func(x time.Time) uint64 { return uint64(x.UnixNano()) }
	h := f.now.Truncate(time.Hour)
	h3, h2 := h.Add(-3*time.Hour), h.Add(-2*time.Hour)
	// the fleet: a step at h3+10m (A), sealed; the open hour h2 with a step at h2+10m (B)
	A, B := h3.Add(5*time.Minute), h2.Add(5*time.Minute)
	fs1 := completeness.HistStep{AtMs: ms(h3.Add(10 * time.Minute)), CtNs: ns(A)}
	fs2 := completeness.HistStep{AtMs: ms(h2.Add(10 * time.Minute)), CtNs: ns(B)}
	put := func(key string, v any) {
		b, _ := json.Marshal(v)
		f.mem.Put(key, b, nil, f.now)
	}
	put(f.srv.Watermark.HistKey(completeness.FleetScope, ms(h3)), map[string]any{"format": 2, "scope": completeness.FleetScope,
		"hour_ms": ms(h3), "steps": []completeness.HistStep{fs1}})
	// prod-a: its own step at h3+20m, logs above the fleet's value
	A2 := h3.Add(8 * time.Minute)
	cs1 := completeness.HistStep{AtMs: ms(h3.Add(20 * time.Minute)), CtNs: ns(A), Signals: map[string]uint64{"logs": ns(A2), "traces": ns(A)}, UnlistedNs: ns(A2)}
	put(f.srv.Watermark.HistKey("prod-a", ms(h3)), map[string]any{"format": 2, "scope": "prod-a", "hour_ms": ms(h3), "steps": []completeness.HistStep{cs1}})
	f.now = f.now.Add(20 * time.Second)
	put(root+"/_consumer/watermark.json", completeness.Doc{Format: 2, CompleteThroughNs: uint64(f.ct["prod-a"]), WallMs: uint64(f.now.UnixMilli()),
		Clusters: map[string]uint64{"prod-a": uint64(f.ct["prod-a"]), "prod-b": uint64(f.ct["prod-b"])},
		History:  &completeness.History{HistHour: completeness.HistHour{HourMs: ms(h2), Carry: &fs1, Steps: []completeness.HistStep{fs2}}}})
	put(f.srv.Watermark.ClusterKey("prod-a"), completeness.ClusterDoc{Format: 2, Cluster: "prod-a", CompleteThroughNs: uint64(f.ct["prod-a"]),
		WallMs: uint64(f.now.UnixMilli()), History: &completeness.History{HistHour: completeness.HistHour{HourMs: ms(h2), Carry: &cs1}}})
	basisOf := func(body map[string]any) (int, map[string]any) { return f.post(t, "/v1/basis", fleet, body) }
	bound := func(out map[string]any) map[string]float64 {
		m := map[string]float64{}
		for _, c := range out["basis_info"].(map[string]any)["clusters"].([]any) {
			c := c.(map[string]any)
			m[c["cluster"].(string)] = c["received_before_ns"].(float64)
		}
		return m
	}
	// the fleet, as of h3+30m: A, final
	code, out := basisOf(map[string]any{"as_of": h3.Add(30 * time.Minute).Format(time.RFC3339)})
	if code != http.StatusOK || bound(out)["*"] != float64(ns(A)) || out["as_of"].(map[string]any)["final"] != true {
		t.Fatalf("fleet as of h3+30m: %d %v", code, out)
	}
	tokA := out["basis"].(string)
	// a step counts from its stamp (CAST 34): exactly at it, A; a millisecond before, nothing
	if code, out := basisOf(map[string]any{"as_of": fs1.AtMs * 1_000_000}); code != 200 || bound(out)["*"] != float64(ns(A)) {
		t.Fatalf("as of the stamp: %d %v", code, out)
	}
	if code, out := basisOf(map[string]any{"as_of": (fs1.AtMs - 1) * 1_000_000}); code != http.StatusNotFound || out["error"] != "as_of_unknown" {
		t.Fatalf("before the history: %d %v", code, out)
	}
	// prod-a for logs: its own step (A2) above the fleet's (A)
	code, out = basisOf(map[string]any{"as_of": h3.Add(30 * time.Minute).Format(time.RFC3339), "clusters": []string{"prod-a"}, "signals": []string{"logs"}})
	if code != 200 || bound(out)["prod-a"] != float64(ns(A2)) {
		t.Fatalf("prod-a logs: %d %v", code, out)
	}
	by := out["as_of"].(map[string]any)["by_cluster"].([]any)[0].(map[string]any)
	if by["basis"] != "cluster_signals" || by["step_at_ms"] != float64(cs1.AtMs) {
		t.Fatalf("by cluster: %v", by)
	}
	// prod-b has no history of its own: the fleet's
	code, out = basisOf(map[string]any{"as_of": h3.Add(30 * time.Minute).Format(time.RFC3339), "clusters": []string{"prod-b"}})
	if code != 200 || bound(out)["prod-b"] != float64(ns(A)) {
		t.Fatalf("prod-b: %d %v", code, out)
	}
	// the open hour: B, provisional
	code, out = basisOf(map[string]any{"as_of": h2.Add(30 * time.Minute).Format(time.RFC3339)})
	if code != 200 || bound(out)["*"] != float64(ns(B)) || out["as_of"].(map[string]any)["final"] != false {
		t.Fatalf("open hour: %d %v", code, out)
	}
	// refusals
	for _, c := range []struct {
		asOf   any
		code   int
		reason string
	}{{f.now.Add(time.Minute).Format(time.RFC3339), 400, "as_of_future"}, {"yesterday", 400, "bad_as_of"}, {h3.Add(-48 * time.Hour).Format(time.RFC3339), 404, "as_of_unknown"}} {
		if code, out := basisOf(map[string]any{"as_of": c.asOf}); code != c.code || out["error"] != c.reason {
			t.Errorf("as_of %v: %d %v, want %d %s", c.asOf, code, out, c.code, c.reason)
		}
	}
	// an answer at the as_of basis: exactly the rows received before A
	f.rows.mu.Lock()
	f.rows.rows = nil
	f.rows.mu.Unlock()
	for i, recv := range []time.Time{A.Add(-time.Second), A, A.Add(time.Second), h2} {
		f.rows.add(row{Cluster: []string{"prod-a", "prod-b"}[i%2], Namespace: "shop", TsNs: recv.UnixNano(), RecvNs: recv.UnixNano()})
	}
	code, out, n := f.count(t, fleet, map[string]any{"basis": tokA, "window": map[string]int64{"from": h3.Add(-time.Hour).UnixNano(), "to": h.UnixNano()}})
	if code != 200 || n != 1 {
		t.Fatalf("at the as_of basis: %d rows (%d %v), want 1", n, code, out)
	}
}
