package server

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func recvMeta(cluster string, min, max, recv time.Time) map[string]string {
	m := dataMeta(cluster, min, max)
	m["oscope-received"] = fmt.Sprint(recv.UnixNano())
	return m
}

func planKeys(out map[string]any) map[string]map[string]any {
	keys := map[string]map[string]any{}
	for _, o := range out["objects"].([]any) {
		keys[o.(map[string]any)["key"].(string)] = o.(map[string]any)
	}
	return keys
}

// A plan at a basis lists the same objects however much data arrives
// meanwhile (a later object, a late object into the same window); a plan
// at a newer basis includes them. Objects that cannot be dated are planned
// with a footer check, and GC truncation refuses the basis.
func TestPlanAtBasis(t *testing.T) {
	f := newBasisFixture(t)
	fleet := f.token(fleetClaims)
	a1 := key("prod-a", "pub-0", "logs", 1)
	f.mem.Put(a1, make([]byte, 100), recvMeta("prod-a", t0.Add(-10*time.Minute), t0.Add(-6*time.Minute), t0.Add(-5*time.Minute)), t0.Add(-5*time.Minute))
	req := map[string]any{"signal": "logs", "from": t0.Add(-20 * time.Minute).UnixNano(), "to": t0.Add(-4 * time.Minute).UnixNano(), "basis": "latest"}
	code, out := f.post(t, "/v1/plan", fleet, req)
	if code != 200 || out["at_basis"] != true || len(planKeys(out)) != 1 {
		t.Fatalf("%d %v", code, out)
	}
	b1, hash1 := out["basis"].(string), out["objects_hash"]
	if out["completeness"] != "complete" { // the basis (t0 − 1 min) is past the window's end + max_lateness
		t.Fatal(out["completeness"])
	}
	// more data: a new object after the basis, a late one into the same window
	a2 := key("prod-a", "pub-0", "logs", 2)
	a3 := key("prod-a", "pub-0", "logs", 3)
	f.mem.Put(a2, make([]byte, 100), recvMeta("prod-a", t0.Add(-8*time.Minute), t0.Add(-5*time.Minute), t0.Add(-30*time.Second)), t0.Add(-29*time.Second))
	f.mem.Put(a3, make([]byte, 100), recvMeta("prod-a", t0.Add(-15*time.Minute), t0.Add(-15*time.Minute), t0.Add(10*time.Second)), t0.Add(11*time.Second))
	f.ct = map[string]int64{"prod-a": t0.Add(time.Minute).UnixNano(), "prod-b": t0.Add(time.Minute).UnixNano()}
	f.publish()
	req["basis"] = b1
	code, out = f.post(t, "/v1/plan", fleet, req)
	if code != 200 || out["objects_hash"] != hash1 || len(planKeys(out)) != 1 || out["after_basis"].(float64) != 2 {
		t.Fatalf("re-plan at the basis: %d %v", code, out)
	}
	req["basis"] = "latest"
	code, out = f.post(t, "/v1/plan", fleet, req)
	if k := planKeys(out); code != 200 || k[a2] == nil || k[a3] == nil || k[a3]["late"] != true {
		t.Fatalf("newer basis: %d %v", code, out)
	}
	b2 := out["basis"].(string)
	// objects without custody metadata: dated by LastModified when that
	// proves them in, else planned with a footer check
	a4 := key("prod-a", "pub-0", "logs", 4)
	a5 := key("prod-a", "pub-0", "logs", 5)
	undated := map[string]string{"oscope-kind": "data", "oscope-cluster": "prod-a"}
	f.mem.Put(a4, make([]byte, 100), undated, t0.Add(-20*time.Minute))
	f.mem.Put(a5, make([]byte, 100), undated, t0.Add(-2*time.Minute))
	req["basis"] = b2
	code, out = f.post(t, "/v1/plan", fleet, req)
	k := planKeys(out)
	if code != 200 || k[a4] == nil || k[a4]["basis_check"] != nil || k[a5] == nil || k[a5]["basis_check"] != true ||
		k[a5]["received_before_ns"] == nil || out["basis_unverified"].(float64) != 1 {
		t.Fatalf("undated: %d %v", code, out)
	}
	// objects written long after the basis was issued are left out unHEADed
	a6 := key("prod-a", "pub-0", "logs", 6)
	f.mem.Put(a6, make([]byte, 100), undated, f.now.Add(time.Hour))
	heads := f.mem.Heads
	code, out = f.post(t, "/v1/plan", fleet, req)
	if code != 200 || planKeys(out)[a6] != nil || f.mem.Heads-heads != 5 {
		t.Fatalf("after issue: %d heads %d %v", code, f.mem.Heads-heads, out)
	}
	// without a basis: the answer names the current one, and is not at it
	delete(req, "basis")
	code, out = f.post(t, "/v1/plan", fleet, req)
	if code != 200 || out["at_basis"] != false || out["basis"] == nil {
		t.Fatalf("%d %v", code, out)
	}
	// a restricted caller cannot plan at a fleet caller's two-cluster basis
	code, out = f.post(t, "/v1/plan", f.token(teamA), map[string]any{"signal": "logs", "from": req["from"], "to": req["to"], "basis": b2})
	if code != 403 || out["error"] != "basis_not_in_scope" {
		t.Fatalf("%d %v", code, out)
	}
	// a traces plan at a logs basis
	code, out = f.post(t, "/v1/plan", fleet, map[string]any{"signal": "traces", "from": req["from"], "to": req["to"], "basis": b2})
	if code != 400 || out["error"] != "basis_scope" {
		t.Fatalf("%d %v", code, out)
	}
	// GC deleted slots of a planned lane before the window's start:
	// refused, never answered with less data
	gc, _ := json.Marshal(map[string]any{"deleted_below": map[string]any{"prod-a/pub-0/logs": map[string]int{epoch: 1}}})
	f.mem.Put(root+"/_consumer/gc.json", gc, nil, t0)
	f.now = f.now.Add(40 * time.Second) // past the planner's gc.json cache
	req["from"] = t0.Add(-40 * time.Minute).UnixNano()
	req["basis"] = b2
	code, out = f.post(t, "/v1/plan", fleet, req)
	if code != 410 || out["error"] != "basis_expired" {
		t.Fatalf("gc: %d %v", code, out)
	}
}
