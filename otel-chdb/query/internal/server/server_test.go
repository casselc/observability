package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/auth/authtest"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/lake"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"github.com/golang-jwt/jwt/v5"
)

type fakeCH struct {
	mu      sync.Mutex
	calls   []url.Values
	sqls    []string
	err     error
	block   chan struct{}
	started chan struct{}
}

func (f *fakeCH) Query(ctx context.Context, sql string, settings url.Values) ([]byte, central.Summary, error) {
	f.mu.Lock()
	f.calls = append(f.calls, settings)
	f.sqls = append(f.sqls, sql)
	block, started := f.block, f.started
	f.mu.Unlock()
	if started != nil {
		started <- struct{}{}
	}
	if block != nil {
		<-block
	}
	if f.err != nil {
		return nil, central.Summary{}, f.err
	}
	return []byte(`{"meta":[{"name":"c","type":"UInt64"}],"data":[{"c":"7"}],"rows":1}`), central.Summary{ReadRows: 10}, nil
}

type fixture struct {
	is   *authtest.Issuer
	srv  *Server
	http *httptest.Server
	ch   *fakeCH
	sink *audit.Memory
	mem  *store.Mem
	now  time.Time
}

const root = "lake"

var (
	t0    = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	epoch = "20260928T110000.000Z-0a1b2c3d"
)

func key(cluster, producer, signal string, seq int) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%020d.parquet", root, cluster, producer, signal, epoch, seq)
}

func dataMeta(cluster string, min, max time.Time) map[string]string {
	return map[string]string{"oscope-kind": "data", "oscope-cluster": cluster, "oscope-min-time": fmt.Sprint(min.UnixNano()),
		"oscope-max-time": fmt.Sprint(max.UnixNano()), "oscope-rows": "100"}
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{is: authtest.New(), ch: &fakeCH{}, sink: &audit.Memory{}, mem: store.NewMem(), now: t0}
	t.Cleanup(f.is.Close)
	v, err := auth.NewVerifier(auth.OIDCConfig{Issuer: f.is.URL, Audience: "otel-query"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := sqlscope.NewPolicy("otel", []*sqlscope.Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", Scope: "columns", Cluster: "`__hdx_materialized_k8s.cluster.name`", Namespace: "`__hdx_materialized_k8s.namespace.name`"},
		{Name: "otel_traces", TimeColumn: "Timestamp", Scope: "columns", Cluster: "ResourceAttributes['k8s.cluster.name']", Namespace: "ResourceAttributes['k8s.namespace.name']"},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	// the watermark: complete through t0 − 40 s, published 10 s ago
	wmDoc, _ := json.Marshal(completeness.Doc{Format: 2, CompleteThroughNs: uint64(t0.Add(-40 * time.Second).UnixNano()),
		WallMs:  uint64(t0.Add(-10 * time.Second).UnixMilli()),
		Holding: []completeness.LaneWm{{Lane: "prod-a/pub-0/logs"}, {Lane: "prod-b/pub-0/logs"}}})
	f.mem.Put(root+"/_consumer/watermark.json", wmDoc, nil, t0)
	clock := func() time.Time { return f.now }
	wm := completeness.NewReader(func(ctx context.Context, k string) ([]byte, error) {
		b, _, err := f.mem.Get(ctx, k)
		return b, err
	}, root+"/_consumer/watermark.json", 15*time.Second, 5*time.Minute)
	wm.SetClock(clock)
	pl := lake.New(lake.Config{Root: root}, f.mem, wm)
	pl.SetClock(clock)
	f.srv = &Server{Verifier: v, Audit: f.sink, Policy: policy, Central: f.ch, Watermark: wm, Planner: pl,
		Mapping: &auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups",
			Groups: map[string]auth.Grant{"sre": {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{"query", "plan"}}}},
		Limits:  Limits{Default: central.Limits{MaxExecutionTimeS: 7, MaxResultRows: 1000, MaxConcurrent: 1}},
		Origins: []string{"https://lake-ui.example"}, Now: clock}
	f.srv.Init()
	f.http = httptest.NewServer(f.srv.Handler())
	t.Cleanup(f.http.Close)
	// lanes: prod-a and prod-b, one publisher each; data, a heartbeat, an
	// old object, and in prod-a an object claiming to be prod-b's
	lm := t0.Add(-5 * time.Minute)
	for _, c := range []string{"prod-a", "prod-b"} {
		f.mem.Put(key(c, "pub-0", "logs", 1), make([]byte, 1000), dataMeta(c, t0.Add(-9*time.Minute), t0.Add(-6*time.Minute)), lm)
		f.mem.Put(key(c, "pub-0", "logs", 2), nil, map[string]string{"oscope-kind": "beat"}, lm)
		f.mem.Put(key(c, "pub-0", "logs", 3), make([]byte, 500), dataMeta(c, t0.Add(-3*time.Hour), t0.Add(-2*time.Hour)), lm)
		f.mem.Put(key(c, "pub-0", "traces", 1), make([]byte, 700), dataMeta(c, t0.Add(-9*time.Minute), t0.Add(-6*time.Minute)), lm)
	}
	f.mem.Put(key("prod-a", "pub-0", "logs", 4), make([]byte, 10), dataMeta("prod-b", t0.Add(-9*time.Minute), t0.Add(-6*time.Minute)), lm)
	return f
}

func (f *fixture) token(claims jwt.MapClaims) string {
	c := jwt.MapClaims{"aud": "otel-query", "sub": "alice"}
	for k, v := range claims {
		c[k] = v
	}
	return f.is.Mint(c)
}

var teamA = jwt.MapClaims{"clusters": []any{"prod-a"}, "namespaces": "*", "roles": []any{"query", "plan"}}

func (f *fixture) post(t *testing.T, path, tok string, body any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, f.http.URL+path, bytes.NewReader(b))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestUnauthenticatedAndRoleless(t *testing.T) {
	f := newFixture(t)
	code, out := f.post(t, "/v1/query", "", map[string]any{"sql": "SELECT 1"})
	if code != 401 || out["error"] != "no_token" {
		t.Fatalf("%d %v", code, out)
	}
	code, out = f.post(t, "/v1/query", "garbage", map[string]any{"sql": "SELECT 1"})
	if code != 401 || out["error"] != "bad_token" {
		t.Fatalf("%d %v", code, out)
	}
	code, out = f.post(t, "/v1/plan", f.token(jwt.MapClaims{"clusters": "prod-a", "roles": "query"}), map[string]any{})
	if code != 403 || out["error"] != "role_missing" {
		t.Fatalf("%d %v", code, out)
	}
	recs := f.sink.Snapshot()
	if len(recs) != 3 || recs[0].Decision != "deny" || recs[2].Subject != "alice" {
		t.Fatalf("every refusal is audited: %+v", recs)
	}
	if len(f.ch.calls) != 0 {
		t.Fatal("nothing may run")
	}
}

func TestQueryScopedLabelledAudited(t *testing.T) {
	f := newFixture(t)
	window := map[string]any{"from": t0.Add(-time.Hour).Format(time.RFC3339), "to": t0.Format(time.RFC3339)}
	code, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "select count() from otel_logs", "window": window})
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	s := f.ch.calls[0]
	if f.ch.sqls[0] != "SELECT count() FROM otel.otel_logs" {
		t.Fatalf("sql %q", f.ch.sqls[0])
	}
	flt := s.Get("additional_table_filters")
	if !strings.Contains(flt, `IN (\'prod-a\')`) || !strings.Contains(flt, "Timestamp >= fromUnixTimestamp64Nano") || strings.Contains(flt, "prod-b") {
		t.Fatalf("filter %q", flt)
	}
	for _, m := range central.OverflowModes {
		if s.Get(m) != "throw" {
			t.Errorf("%s = %q", m, s.Get(m))
		}
	}
	if s.Get("max_execution_time") != "7" || s.Get("max_result_rows") != "1000" || s.Get("wait_end_of_query") != "1" {
		t.Fatalf("limits %v", s)
	}
	if out["source"] != "central" || out["complete_through"] == nil || out["completeness"] != "partial" || out["partial"] != true {
		t.Fatalf("label %v", out)
	}
	if out["incomplete_from"] != t0.Add(-40*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("incomplete_from %v", out["incomplete_from"])
	}
	wm := out["watermark"].(map[string]any)
	if h := wm["holding"].([]any); len(h) != 1 || !strings.HasPrefix(h[0].(map[string]any)["lane"].(string), "prod-a/") {
		t.Fatalf("another cluster's lanes in the label: %v", wm)
	}
	if out["catalog"].(map[string]any)["status"] != "not_configured" {
		t.Fatalf("catalog %v", out["catalog"])
	}
	recs := f.sink.Snapshot()
	if len(recs) != 2 || recs[0].Event != "decision" || recs[0].Decision != "allow" || recs[1].Event != "outcome" ||
		recs[0].QueryHash == "" || recs[0].QueryHash != recs[1].QueryHash || strings.Join(recs[0].Clusters, ",") != "prod-a" {
		t.Fatalf("audit %+v", recs)
	}
	// a closed window before complete_through is complete
	window = map[string]any{"from": t0.Add(-time.Hour).UnixNano(), "to": t0.Add(-time.Minute).UnixNano()}
	_, out = f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT count() FROM otel_logs", "window": window})
	if out["completeness"] != "complete" || out["partial"] != false {
		t.Fatalf("closed window: %v", out)
	}
}

func TestQueryRefusals(t *testing.T) {
	f := newFixture(t)
	for sql, want := range map[string]string{
		"SELECT * FROM system.users":                          "table_not_allowed",
		"SELECT * FROM s3('http://x')":                        "table_function",
		"SELECT count() FROM otel_logs SETTINGS readonly = 0": "settings_clause",
		"DROP TABLE otel.otel_logs":                           "not_select",
	} {
		code, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": sql})
		if code != 403 || out["error"] != want {
			t.Errorf("%q: %d %v", sql, code, out)
		}
	}
	code, out := f.post(t, "/v1/query", f.token(jwt.MapClaims{"roles": "query"}), map[string]any{"sql": "SELECT count() FROM otel_logs"})
	if code != 403 || out["error"] != "empty_scope" {
		t.Fatalf("no clusters: %d %v", code, out)
	}
	if len(f.ch.calls) != 0 {
		t.Fatal("a refused statement ran")
	}
	for _, r := range f.sink.Snapshot() {
		if r.Decision != "deny" || r.InputHash == "" {
			t.Fatalf("%+v", r)
		}
	}
}

func TestAuditFailureRefuses(t *testing.T) {
	f := newFixture(t)
	f.sink.Fail = errors.New("disk full")
	code, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT count() FROM otel_logs"})
	if code != 503 || out["error"] != "audit_unavailable" || len(f.ch.calls) != 0 {
		t.Fatalf("%d %v, ran %d", code, out, len(f.ch.calls))
	}
	code, out = f.post(t, "/v1/plan", f.token(teamA), map[string]any{"signal": "logs", "from": t0.Add(-time.Hour).UnixNano(), "to": t0.UnixNano()})
	if code != 503 || out["objects"] != nil {
		t.Fatalf("plan without audit: %d %v", code, out)
	}
}

func TestMissingWatermarkIsUnknown(t *testing.T) {
	f := newFixture(t)
	delete(f.mem.Objects, root+"/_consumer/watermark.json")
	window := map[string]any{"from": t0.Add(-3 * time.Hour).UnixNano(), "to": t0.Add(-2 * time.Hour).UnixNano()}
	_, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT count() FROM otel_logs", "window": window})
	if out["completeness"] != "unknown" || out["partial"] != true || out["complete_through"] != nil {
		t.Fatalf("%v", out)
	}
	if out["watermark"].(map[string]any)["status"] != "missing" {
		t.Fatalf("%v", out["watermark"])
	}
}

func TestConcurrencyLimit(t *testing.T) {
	f := newFixture(t)
	f.ch.block, f.ch.started = make(chan struct{}), make(chan struct{}, 1)
	done := make(chan int)
	go func() {
		code, _ := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT count() FROM otel_logs"})
		done <- code
	}()
	<-f.ch.started
	code, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT count() FROM otel_logs"})
	close(f.ch.block)
	if code != 429 || out["error"] != "too_many_concurrent" {
		t.Fatalf("%d %v", code, out)
	}
	if c := <-done; c != 200 {
		t.Fatal(c)
	}
}

func TestLimitErrorIsAnError(t *testing.T) {
	f := newFixture(t)
	f.ch.err = &central.Error{HTTPStatus: 500, Code: 158, Message: "Limit for rows to read exceeded"}
	code, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT count() FROM otel_logs"})
	if code != 422 || out["error"] != "limit_exceeded:TOO_MANY_ROWS" {
		t.Fatalf("%d %v", code, out)
	}
	recs := f.sink.Snapshot()
	if recs[len(recs)-1].Event != "outcome" || recs[len(recs)-1].Status != 422 {
		t.Fatalf("%+v", recs)
	}
}

func TestPlanScoped(t *testing.T) {
	f := newFixture(t)
	req := map[string]any{"signal": "logs", "from": t0.Add(-time.Hour).Format(time.RFC3339), "to": t0.Format(time.RFC3339)}
	code, out := f.post(t, "/v1/plan", f.token(teamA), req)
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	objs := out["objects"].([]any)
	if len(objs) != 1 {
		t.Fatalf("want only prod-a's in-window data object, got %v", objs)
	}
	o := objs[0].(map[string]any)
	if o["key"] != key("prod-a", "pub-0", "logs", 1) || o["size"].(float64) != 1000 || !strings.Contains(o["url"].(string), "X-Amz-Expires=300") {
		t.Fatalf("%v", o)
	}
	if out["source"] != "lake" || out["snapshot"] != nil || out["mismatched"].(float64) != 1 || out["completeness"] != "partial" {
		t.Fatalf("%v", out)
	}
	exp, _ := time.Parse(time.RFC3339Nano, out["expires_at"].(string))
	replan, _ := time.Parse(time.RFC3339Nano, out["replan_after"].(string))
	if exp.Sub(t0) != 5*time.Minute || exp.Sub(replan) != time.Minute {
		t.Fatalf("expiry %v replan %v", exp, replan)
	}
	if len(out["rules"].([]any)) == 0 {
		t.Fatal("the X8 rules travel with the plan")
	}
	recs := f.sink.Snapshot()
	last := recs[len(recs)-1]
	if last.Decision != "allow" || last.Objects != 1 || last.Bytes != 1000 || len(last.Keys) != 1 || last.ObjectsHash == "" {
		t.Fatalf("%+v", last)
	}
	if got := f.srv.Metrics.Get("qs_planned_objects_total"); got != 1 {
		t.Fatalf("planned objects metric %v", got)
	}
}

func TestPlanDenials(t *testing.T) {
	f := newFixture(t)
	req := map[string]any{"signal": "logs", "from": t0.Add(-time.Hour).UnixNano(), "to": t0.UnixNano(), "clusters": []string{"prod-b"}}
	code, out := f.post(t, "/v1/plan", f.token(teamA), req)
	if code != 403 || out["error"] != "cluster_not_in_scope" {
		t.Fatalf("cross-cluster plan: %d %v", code, out)
	}
	nsOnly := jwt.MapClaims{"clusters": "prod-a", "namespaces": "shop", "roles": "plan"}
	req["clusters"] = nil
	code, out = f.post(t, "/v1/plan", f.token(nsOnly), req)
	if code != 403 || out["error"] != "namespace_scope_needs_filtering_reader" {
		t.Fatalf("namespace-scoped plan: %d %v", code, out)
	}
	req["to"] = t0.Add(48 * time.Hour).UnixNano()
	code, out = f.post(t, "/v1/plan", f.token(teamA), req)
	if code != 400 || out["error"] != "window_too_long" {
		t.Fatalf("%d %v", code, out)
	}
	for _, r := range f.sink.Snapshot() {
		if r.Decision != "deny" || r.Objects != 0 {
			t.Fatalf("%+v", r)
		}
	}
}

func TestPlanFleetAndGC(t *testing.T) {
	f := newFixture(t)
	gc, _ := json.Marshal(map[string]any{"deleted_below": map[string]any{"prod-b/pub-0/logs": map[string]int{epoch: 1}}})
	f.mem.Put(root+"/_consumer/gc.json", gc, nil, t0)
	req := map[string]any{"signal": "logs", "from": t0.Add(-30 * time.Hour).UnixNano(), "to": t0.Add(-6 * time.Hour).UnixNano()}
	code, out := f.post(t, "/v1/plan", f.token(jwt.MapClaims{"groups": "sre"}), req)
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	if strings.Join(toStrings(out["clusters"]), ",") != "prod-a,prod-b" {
		t.Fatalf("fleet clusters %v", out["clusters"])
	}
	if out["start_complete"] != false || strings.Join(toStrings(out["gc_truncated_lanes"]), ",") != "prod-b/pub-0/logs" || out["partial"] != true {
		t.Fatalf("gc: %v", out)
	}
}

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestCORSAndMetrics(t *testing.T) {
	f := newFixture(t)
	req, _ := http.NewRequest(http.MethodOptions, f.http.URL+"/v1/plan", nil)
	req.Header.Set("Origin", "https://lake-ui.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Origin") != "https://lake-ui.example" {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
	req.Header.Set("Origin", "https://evil.example")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unknown origin allowed")
	}
	f.post(t, "/v1/query", "", map[string]any{})
	resp, _ = http.Get(f.http.URL + "/metrics")
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`qs_denials_total{endpoint="query",reason="no_token"} 1`, "qs_watermark_age_seconds 10", `qs_watermark_status{status="ok"} 1`} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("metrics lack %q:\n%s", want, b.String())
		}
	}
	resp, _ = http.Get(f.http.URL + "/healthz")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
}

func TestQueryOutputSettings(t *testing.T) {
	f := newFixture(t)
	code, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT now()", "output": map[string]string{"date_time_output_format": "iso"}})
	if code != 200 || f.ch.calls[0].Get("date_time_output_format") != "iso" {
		t.Fatalf("%d %v %v", code, out, f.ch.calls)
	}
	// anything that could change rows, limits or scope is refused
	for _, o := range []map[string]string{{"date_time_output_format": "iso'"}, {"additional_table_filters": "{}"},
		{"max_result_rows": "0"}, {"result_overflow_mode": "break"}} {
		code, out := f.post(t, "/v1/query", f.token(teamA), map[string]any{"sql": "SELECT 1", "output": o})
		if code != 400 || out["error"] != "bad_output" {
			t.Fatalf("%v: %d %v", o, code, out)
		}
	}
	if len(f.ch.calls) != 1 {
		t.Fatalf("a refused output setting ran: %d calls", len(f.ch.calls))
	}
}
