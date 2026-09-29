package server

import (
	"net/http"
	"testing"
	"time"
)

func tailKeys(out map[string]any) map[string]map[string]any {
	keys := map[string]map[string]any{}
	if l, ok := out["tail_objects"].([]any); ok {
		for _, o := range l {
			keys[o.(map[string]any)["key"].(string)] = o.(map[string]any)
		}
	}
	return keys
}

// A plan asked with tail (D30 amendment "the tail", AMBIGUITY.md #10 (b)):
// the basis part is D30's plan (the same objects, the same objects_hash,
// however much arrives), and the objects received at or after the basis are
// in tail_objects, marked, labelled incomplete and never cached; an object
// that cannot be dated goes to the tail with a footer check, never into the
// basis part.
func TestPlanTail(t *testing.T) {
	f := newBasisFixture(t)
	fleet := f.token(fleetClaims)
	a1 := key("prod-a", "pub-0", "logs", 1)
	f.mem.Put(a1, make([]byte, 100), recvMeta("prod-a", t0.Add(-10*time.Minute), t0.Add(-6*time.Minute), t0.Add(-5*time.Minute)), t0.Add(-5*time.Minute))
	req := map[string]any{"signal": "logs", "from": t0.Add(-20 * time.Minute).UnixNano(), "to": t0.Add(-4 * time.Minute).UnixNano(), "basis": "latest", "tail": true}

	// tail needs a basis
	code, out := f.post(t, "/v1/plan", fleet, map[string]any{"signal": "logs", "from": req["from"], "to": req["to"], "tail": true})
	if code != http.StatusBadRequest || out["error"] != "tail_needs_basis" {
		t.Fatalf("tail without a basis: %d %v", code, out)
	}

	code, out = f.post(t, "/v1/plan", fleet, req)
	if code != 200 || out["at_basis"] != true || len(planKeys(out)) != 1 || len(tailKeys(out)) != 0 {
		t.Fatalf("%d %v", code, out)
	}
	tail := out["tail"].(map[string]any)
	if tail["completeness"] != "incomplete" || tail["cache"] != "never" || tail["objects"].(float64) != 0 {
		t.Fatalf("empty tail: %v", tail)
	}
	b1, hash1 := out["basis"].(string), out["objects_hash"]

	// more data: after the basis (a2), late into the same window (a3), undated
	// close to the bound (a5), written long after the basis was issued (a6)
	a2 := key("prod-a", "pub-0", "logs", 2)
	a3 := key("prod-a", "pub-0", "logs", 3)
	a5 := key("prod-a", "pub-0", "logs", 5)
	a6 := key("prod-a", "pub-0", "logs", 6)
	f.mem.Put(a2, make([]byte, 100), recvMeta("prod-a", t0.Add(-8*time.Minute), t0.Add(-5*time.Minute), t0.Add(-30*time.Second)), t0.Add(-29*time.Second))
	f.mem.Put(a3, make([]byte, 100), recvMeta("prod-a", t0.Add(-15*time.Minute), t0.Add(-15*time.Minute), t0.Add(10*time.Second)), t0.Add(11*time.Second))
	undated := map[string]string{"oscope-kind": "data", "oscope-cluster": "prod-a"}
	f.mem.Put(a5, make([]byte, 100), undated, t0.Add(-2*time.Minute))
	f.mem.Put(a6, make([]byte, 100), recvMeta("prod-a", t0.Add(-12*time.Minute), t0.Add(-11*time.Minute), f.now.Add(time.Hour)), f.now.Add(time.Hour))
	f.ct = map[string]int64{"prod-a": t0.Add(time.Minute).UnixNano(), "prod-b": t0.Add(time.Minute).UnixNano()}
	f.publish()

	req["basis"] = b1
	n0 := len(f.sink.Records)
	code, out = f.post(t, "/v1/plan", fleet, req)
	k, tk := planKeys(out), tailKeys(out)
	if code != 200 || out["objects_hash"] != hash1 || len(k) != 1 || k[a1] == nil || len(tk) != 4 {
		t.Fatalf("re-plan at the basis with tail: %d %v", code, out)
	}
	for _, key := range []string{a2, a3, a5, a6} {
		o := tk[key]
		if o == nil || o["tail"] != true || o["url"] == "" {
			t.Fatalf("tail object %s: %v", key, o)
		}
	}
	if tk[a3]["late"] != true || tk[a5]["basis_check"] != true || tk[a5]["received_before_ns"] == nil || tk[a2]["basis_check"] != nil {
		t.Fatalf("tail marks: %v", tk)
	}
	if out["after_basis"] != nil || out["basis_unverified"] != nil {
		t.Fatalf("with tail nothing is left out: %v", out)
	}
	tail = out["tail"].(map[string]any)
	if tail["objects"].(float64) != 4 || tail["unplaced"].(float64) != 1 || tail["unrefined"].(float64) != 1 || tail["late_objects"].(float64) != 3 ||
		tail["bytes"].(float64) != 400 || tail["cache"] != "never" || len(tail["received_from"].([]any)) != 1 || tail["objects_hash"] == hash1 {
		t.Fatalf("tail report: %v", tail)
	}
	if out["total_bytes"].(float64) != 100 || out["late_objects"].(float64) != 1 {
		t.Fatalf("the basis part's counters: %v", out)
	}
	// the same plan without tail: the same basis part (D30), the rest left out
	delete(req, "tail")
	code, plain := f.post(t, "/v1/plan", fleet, req)
	if code != 200 || plain["tail"] != nil || plain["tail_objects"] != nil || plain["after_basis"].(float64) != 3 {
		t.Fatalf("without tail: %d %v", code, plain)
	}
	// D30 plans the undated object with a footer check in the basis part;
	// with tail it is in the tail: the basis parts differ by exactly it
	if pk := planKeys(plain); len(pk) != 2 || pk[a5]["basis_check"] != true {
		t.Fatalf("D30's basis part: %v", pk)
	}
	// the audit record names the tail too (its URLs were issued)
	recs := f.sink.Records[n0:]
	var found bool
	for _, r := range recs {
		if r.Action == "plan" && r.Decision == "allow" && r.TailObjects == 4 && r.TailBytes == 400 && len(r.Keys) == 5 {
			found = true
		}
	}
	if !found {
		t.Fatalf("audit: %+v", recs)
	}
}
