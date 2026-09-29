package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/auth/authtest"
	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/lake"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"github.com/golang-jwt/jwt/v5"
	"pgregory.net/rapid"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// bfix is a service over rowsCH (which applies the filters it is given)
// and a watermark with per-cluster values the test moves.
type bfix struct {
	*fixture
	rows *rowsCH
	ct   map[string]int64
	pl   *lake.Planner
}

var fleetClaims = jwt.MapClaims{"groups": []any{"sre"}}

func newBasisFixture(t *testing.T) *bfix {
	t.Helper()
	f := &bfix{fixture: &fixture{is: authtest.New(), sink: &audit.Memory{}, mem: store.NewMem(), now: t0}, rows: &rowsCH{},
		ct: map[string]int64{"prod-a": t0.Add(-time.Minute).UnixNano(), "prod-b": t0.Add(-time.Minute).UnixNano()}}
	t.Cleanup(f.is.Close)
	v, err := auth.NewVerifier(auth.OIDCConfig{Issuer: f.is.URL, Audience: "otel-query"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := sqlscope.NewPolicy("otel", []*sqlscope.Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", ReceivedColumn: "received_at", Scope: "columns", Signals: []string{"logs"},
			Cluster: "`__hdx_materialized_k8s.cluster.name`", Namespace: "`__hdx_materialized_k8s.namespace.name`"},
		{Name: "otel_traces", TimeColumn: "Timestamp", Scope: "columns", Signals: []string{"traces"},
			Cluster: "ResourceAttributes['k8s.cluster.name']", Namespace: "ResourceAttributes['k8s.namespace.name']"},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return f.now }
	wm := completeness.NewReader(func(ctx context.Context, k string) ([]byte, error) {
		b, _, err := f.mem.Get(ctx, k)
		return b, err
	}, root+"/_consumer/watermark.json", 15*time.Second, 5*time.Minute)
	wm.SetClock(clock)
	f.pl = lake.New(lake.Config{Root: root}, f.mem, wm)
	f.pl.SetClock(clock)
	f.srv = &Server{Verifier: v, Audit: f.sink, Policy: policy, Central: f.rows, Watermark: wm, Planner: f.pl, Bases: basis.Ephemeral(),
		Mapping: &auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups",
			Groups: map[string]auth.Grant{"sre": {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{"query", "plan"}}}},
		Limits: Limits{Default: central.Limits{MaxExecutionTimeS: 7, MaxResultRows: 1000, MaxConcurrent: 4}}, Now: clock}
	f.srv.Init()
	f.http = httptest.NewServer(f.srv.Handler())
	t.Cleanup(f.http.Close)
	f.publish()
	return f
}

// publish writes the watermark with the current per-cluster values, and
// moves the clock past the reader's cache.
func (f *bfix) publish() {
	f.now = f.now.Add(20 * time.Second)
	lo := int64(1 << 62)
	cl := map[string]uint64{}
	for c, v := range f.ct {
		lo = min(lo, v)
		cl[c] = uint64(v)
	}
	doc, _ := json.Marshal(completeness.Doc{Format: 2, CompleteThroughNs: uint64(lo), WallMs: uint64(f.now.UnixMilli()), Clusters: cl})
	f.mem.Put(root+"/_consumer/watermark.json", doc, nil, f.now)
}

var win = map[string]int64{"from": t0.Add(-2 * time.Hour).UnixNano(), "to": t0.Add(2 * time.Hour).UnixNano()}

func (f *bfix) count(t testing.TB, tok string, body map[string]any) (int, map[string]any, int64) {
	t.Helper()
	body["sql"] = "SELECT count() AS c FROM otel_logs"
	if _, ok := body["window"]; !ok {
		body["window"] = win
	}
	code, out := f.post(asT(t), "/v1/query", tok, body)
	if code != http.StatusOK {
		return code, out, -1
	}
	res := out["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["c"].(string)
	var n int64
	fmt.Sscan(res, &n)
	return code, out, n
}

// asT lets rapid's T use the fixture's helpers.
func asT(t testing.TB) *testing.T {
	if tt, ok := t.(*testing.T); ok {
		return tt
	}
	return nil
}

type pinned struct {
	tok      string
	clusters []string
	n        int64
	label    string
}

// The property: an answer at a basis never changes while data keeps
// arriving (rows received after it, late rows in the same window, the
// watermark moving), and it holds exactly the rows received before the
// basis's bound of their cluster.
func TestBasisSameAnswerWhileDataArrives(t *testing.T) {
	tracetag.Covers(t, "P2C", "CAST-34", "H-2", "H-4", "R-S1")
	f := newBasisFixture(t)
	fleet := f.token(fleetClaims)
	rapid.Check(t, func(rt *rapid.T) {
		f.rows.mu.Lock()
		f.rows.rows = nil
		f.rows.mu.Unlock()
		f.ct = map[string]int64{"prod-a": t0.Add(-time.Minute).UnixNano(), "prod-b": t0.Add(-time.Minute).UnixNano()}
		f.publish()
		var pins []pinned
		post := func(body map[string]any) (int, map[string]any, int64) {
			body["sql"] = "SELECT count() AS c FROM otel_logs"
			body["window"] = win
			b, _ := json.Marshal(body)
			req, _ := http.NewRequest(http.MethodPost, f.http.URL+"/v1/query", strings.NewReader(string(b)))
			req.Header.Set("Authorization", "Bearer "+fleet)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				rt.Fatal(err)
			}
			defer resp.Body.Close()
			var out map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&out)
			if resp.StatusCode != 200 {
				return resp.StatusCode, out, -1
			}
			var n int64
			fmt.Sscan(out["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["c"].(string), &n)
			return 200, out, n
		}
		model := func(bounds map[string]int64) int64 {
			var n int64
			for _, r := range f.rows.rows {
				if c, ok := bounds[r.Cluster]; ok && r.RecvNs < c {
					n++
				}
			}
			return n
		}
		steps := rapid.IntRange(1, 25).Draw(rt, "steps")
		for i := 0; i < steps; i++ {
			switch rapid.IntRange(0, 3).Draw(rt, "op") {
			case 0: // a row arrives: received at or after its cluster's complete_through (sound), any event time (late ones too)
				c := rapid.SampledFrom([]string{"prod-a", "prod-b"}).Draw(rt, "c")
				recv := f.ct[c] + rapid.Int64Range(0, int64(2*time.Minute)).Draw(rt, "recv")
				ts := recv - rapid.Int64Range(-int64(5*time.Second), int64(20*time.Minute)).Draw(rt, "late")
				f.rows.add(row{Cluster: c, Namespace: "shop", TsNs: ts, RecvNs: recv})
			case 1: // the consumer moves a cluster's complete_through
				c := rapid.SampledFrom([]string{"prod-a", "prod-b"}).Draw(rt, "c")
				f.ct[c] += rapid.Int64Range(0, int64(90*time.Second)).Draw(rt, "adv")
				f.publish()
			case 2: // pin a basis: "latest", for the fleet or some clusters
				body := map[string]any{"basis": "latest"}
				want := map[string]int64{}
				var cs []string
				switch rapid.IntRange(0, 2).Draw(rt, "scope") {
				case 0:
					lo := min(f.ct["prod-a"], f.ct["prod-b"])
					want = map[string]int64{"prod-a": lo, "prod-b": lo}
				case 1:
					cs = []string{"prod-a"}
				case 2:
					cs = []string{"prod-a", "prod-b"}
				}
				if cs != nil {
					body["clusters"] = cs
					for _, c := range cs {
						want[c] = f.ct[c]
					}
				}
				code, out, n := post(body)
				if code != 200 {
					rt.Fatalf("latest: %d %v", code, out)
				}
				if n != model(want) {
					rt.Fatalf("at the latest basis %v: %d rows, want %d", want, n, model(want))
				}
				if out["at_basis"] != true || out["basis"] == nil {
					rt.Fatalf("no basis: %v", out)
				}
				pins = append(pins, pinned{tok: out["basis"].(string), clusters: cs, n: n, label: out["completeness"].(string)})
			case 3: // re-read every pinned basis: same answer, same label
				for _, p := range pins {
					body := map[string]any{"basis": p.tok}
					if p.clusters != nil && rapid.Bool().Draw(rt, "narrow") {
						body["clusters"] = p.clusters
					}
					code, out, n := post(body)
					if code != 200 || n != p.n || out["completeness"] != p.label {
						rt.Fatalf("basis re-read: %d %d (was %d) %v (was %s): %v", code, n, p.n, out["completeness"], p.label, out)
					}
				}
			}
		}
	})
}

// A basis never widens scope: a token may use a basis only when it may read
// every cluster the basis names, and what it reads stays its own scope's.
func TestBasisNeverWidensScope(t *testing.T) {
	f := newBasisFixture(t)
	f.rows.add(row{Cluster: "prod-a", Namespace: "shop", TsNs: t0.Add(-10 * time.Minute).UnixNano(), RecvNs: t0.Add(-10 * time.Minute).UnixNano()},
		row{Cluster: "prod-b", Namespace: "shop", TsNs: t0.Add(-10 * time.Minute).UnixNano(), RecvNs: t0.Add(-10 * time.Minute).UnixNano()},
		row{Cluster: "prod-b", Namespace: "pay", TsNs: t0.Add(-10 * time.Minute).UnixNano(), RecvNs: t0.Add(-10 * time.Minute).UnixNano()})
	fleet := f.token(fleetClaims)
	a := f.token(teamA)
	aShop := f.token(jwt.MapClaims{"clusters": []any{"prod-a", "prod-b"}, "namespaces": []any{"shop"}, "roles": []any{"query"}})
	mint := func(tok string, body map[string]any) string {
		code, out := f.post(t, "/v1/basis", tok, body)
		if code != 200 {
			t.Fatalf("mint: %d %v", code, out)
		}
		return out["basis"].(string)
	}
	both := mint(fleet, map[string]any{"clusters": []string{"prod-a", "prod-b"}})
	onlyA := mint(fleet, map[string]any{"clusters": []string{"prod-a"}})
	fleetB := mint(fleet, map[string]any{})
	for _, c := range []struct {
		name, tok, b string
		clusters     []string
		code         int
		reason       string
		n            int64
	}{
		{"fleet at both", fleet, both, nil, 200, "", 3},
		{"fleet narrows a two-cluster basis", fleet, both, []string{"prod-b"}, 200, "", 2},
		{"a with a basis naming prod-b", a, both, nil, 403, "basis_not_in_scope", 0},
		{"a narrowing to its own cluster", a, both, []string{"prod-a"}, 403, "basis_not_in_scope", 0},
		{"a with a fleet basis", a, fleetB, nil, 403, "basis_not_in_scope", 0},
		{"a with its own cluster's basis", a, onlyA, nil, 200, "", 1},
		{"a basis cannot add a cluster to the request", fleet, onlyA, []string{"prod-b"}, 400, "basis_scope", 0},
		{"a namespace-restricted caller keeps its namespace", aShop, both, nil, 200, "", 2},
	} {
		body := map[string]any{"basis": c.b}
		if c.clusters != nil {
			body["clusters"] = c.clusters
		}
		code, out, n := f.count(t, c.tok, body)
		if code != c.code || (c.reason != "" && out["error"] != c.reason) || (code == 200 && n != c.n) {
			t.Errorf("%s: %d %v rows %d", c.name, code, out["error"], n)
		}
	}
	// a restricted caller's own basis (latest) covers only its clusters
	code, out, n := f.count(t, a, map[string]any{"basis": "latest"})
	if code != 200 || n != 1 {
		t.Fatal(code, out)
	}
	info := out["basis_info"].(map[string]any)["clusters"].([]any)
	if len(info) != 1 || info[0].(map[string]any)["cluster"] != "prod-a" {
		t.Fatal(info)
	}
	// every refusal was audited as a deny
	denies := 0
	for _, r := range f.sink.Snapshot() {
		if r.Decision == "deny" && strings.HasPrefix(r.Reason, "basis_") {
			denies++
		}
	}
	if denies != 4 {
		t.Fatalf("%d basis denials audited", denies)
	}
}

func TestBasisRefusals(t *testing.T) {
	f := newBasisFixture(t)
	fleet := f.token(fleetClaims)
	code, out := f.post(t, "/v1/basis", fleet, map[string]any{"clusters": []string{"prod-a", "prod-b"}, "signals": []string{"logs"}})
	if code != 200 {
		t.Fatal(code, out)
	}
	good := out["basis"].(string)
	if out["basis_info"].(map[string]any)["signals"].([]any)[0] != "logs" {
		t.Fatal(out)
	}
	// ahead: a basis (from our own key) above the scope's complete_through
	ahead, _ := f.srv.Bases.Mint(context.Background(), &basis.Basis{IssuedNs: f.now.UnixNano(), Clusters: map[string]uint64{"prod-a": uint64(f.ct["prod-a"] + 1)}})
	// expired: a bound older than retention
	old, _ := f.srv.Bases.Mint(context.Background(), &basis.Basis{IssuedNs: f.now.UnixNano(), Clusters: map[string]uint64{"prod-a": uint64(t0.Add(-100 * 24 * time.Hour).UnixNano())}})
	other := basis.Ephemeral()
	foreign, _ := other.Encode(basis.Basis{Clusters: map[string]uint64{"prod-a": 1}})
	cases := []struct {
		name string
		body map[string]any
		code int
		why  string
	}{
		{"garbage", map[string]any{"basis": "nope"}, 400, "basis_invalid"},
		{"another service's key", map[string]any{"basis": foreign}, 400, "basis_invalid"},
		{"ahead", map[string]any{"basis": ahead}, 409, "basis_ahead"},
		{"expired", map[string]any{"basis": old}, 410, "basis_expired"},
		{"window before retention", map[string]any{"basis": good, "window": map[string]int64{"from": t0.Add(-95 * 24 * time.Hour).UnixNano(), "to": t0.UnixNano()}}, 410, "basis_expired"},
		{"a logs basis for traces", map[string]any{"basis": good, "sql": "SELECT count() AS c FROM otel_traces"}, 400, "basis_scope"},
		{"a table without a received column", map[string]any{"basis": "latest", "sql": "SELECT count() AS c FROM otel_traces"}, 400, "basis_unservable"},
		{"basis_from alone", map[string]any{"basis_from": good}, 400, "basis_from_needs_basis"},
		{"basis_from latest", map[string]any{"basis": good, "basis_from": "latest"}, 400, "basis_invalid"},
	}
	for _, c := range cases {
		if _, ok := c.body["sql"]; !ok {
			c.body["sql"] = "SELECT count() AS c FROM otel_logs"
		}
		if _, ok := c.body["window"]; !ok {
			c.body["window"] = win
		}
		code, out := f.post(t, "/v1/query", fleet, c.body)
		if code != c.code || out["error"] != c.why {
			t.Errorf("%s: %d %v %v", c.name, code, out["error"], out["detail"])
		}
		for _, sql := range f.rows.sqls {
			_ = sql
		}
	}
	if len(f.rows.sqls) != 0 {
		t.Fatalf("a refused basis ran %d statements", len(f.rows.sqls))
	}
	// a missing watermark: no basis can be minted or checked
	delete(f.mem.Objects, root+"/_consumer/watermark.json")
	f.now = f.now.Add(time.Minute)
	for _, b := range []string{"latest", good} {
		code, out, _ := f.count(t, fleet, map[string]any{"basis": b})
		if code != 503 || out["error"] != "basis_unverifiable" {
			t.Errorf("%s with no watermark: %d %v", b[:6], code, out)
		}
	}
	// without a basis the answer still names none rather than a wrong one
	code, out, _ = f.count(t, fleet, map[string]any{})
	if code != 200 || out["basis"] != nil || out["at_basis"] != false {
		t.Fatal(code, out)
	}
}

// A delta reads exactly the rows received between two bases, and a
// regressed pair is refused (a delta must never re-count rows).
func TestBasisDelta(t *testing.T) {
	f := newBasisFixture(t)
	fleet := f.token(fleetClaims)
	ev := t0.Add(-30 * time.Minute).UnixNano()
	w30 := map[string]int64{"from": ev - int64(time.Minute), "to": ev + int64(time.Minute)}
	f.rows.add(row{Cluster: "prod-a", Namespace: "shop", TsNs: ev, RecvNs: f.ct["prod-a"] - 1})
	_, out, n1 := f.count(t, fleet, map[string]any{"window": w30, "basis": "latest", "clusters": []string{"prod-a"}})
	b1 := out["basis"].(string)
	if n1 != 1 || out["completeness"] != "complete" {
		t.Fatal(n1, out)
	}
	// two late rows arrive for the same window, the watermark passes them
	f.rows.add(row{Cluster: "prod-a", Namespace: "shop", TsNs: ev, RecvNs: f.ct["prod-a"]},
		row{Cluster: "prod-a", Namespace: "shop", TsNs: ev, RecvNs: f.ct["prod-a"] + 10})
	f.ct["prod-a"] += int64(time.Minute)
	f.publish()
	_, out, n := f.count(t, fleet, map[string]any{"window": w30, "basis": b1})
	if n != 1 {
		t.Fatalf("the old basis moved: %d", n)
	}
	_, out, n2 := f.count(t, fleet, map[string]any{"window": w30, "basis": "latest", "clusters": []string{"prod-a"}})
	b2 := out["basis"].(string)
	if n2 != 3 {
		t.Fatal(n2)
	}
	code, out, nd := f.count(t, fleet, map[string]any{"window": w30, "basis": b2, "basis_from": b1})
	if code != 200 || nd != 2 {
		t.Fatal(code, nd, out)
	}
	d := out["delta"].(map[string]any)
	if d["status"] != "counted" || d["rows"].(float64) != 2 || out["basis_from"] != b1 {
		t.Fatal(d)
	}
	// the delta's rows are all late (received after the window settled)
	if l := out["late"].(map[string]any); l["rows"].(float64) != 2 {
		t.Fatal(l)
	}
	// the next delta starts where this one ended: nothing counted twice
	_, _, nd2 := f.count(t, fleet, map[string]any{"window": w30, "basis": b2, "basis_from": b2})
	if nd2 != 0 {
		t.Fatal(nd2)
	}
	code, out, _ = f.count(t, fleet, map[string]any{"window": w30, "basis": b1, "basis_from": b2})
	if code != 409 || out["error"] != "basis_regressed" {
		t.Fatal(code, out)
	}
}
