package qclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"pgregory.net/rapid"
)

func testRule(t testing.TB) *rule.Rule {
	r := &rule.Rule{Name: "r", SQL: "SELECT ServiceName AS service, count() AS value FROM otel_logs GROUP BY service",
		Window: rule.Duration(time.Minute), Condition: rule.Condition{Op: ">", Threshold: 1}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

var win = rule.Window{FromNs: 1_000 * 1e9, ToNs: 1_060 * 1e9}

type resp struct {
	Completeness string
	Partial      bool
	Status       string
	CT           *int64
	WinFrom      int64
	WinTo        int64
	NoWindow     bool
	Data         []map[string]any
}

func (r resp) body() []byte {
	m := map[string]any{"request_id": "rq", "source": "central", "completeness": r.Completeness, "partial": r.Partial,
		"watermark": map[string]any{"status": r.Status, "age_s": 12.5, "holding": []any{map[string]any{"lane": "c1/p/logs", "lag_s": 40}}},
		"result":    map[string]any{"data": r.Data, "rows": len(r.Data)}}
	if r.CT != nil {
		m["complete_through_ns"] = *r.CT
	}
	if !r.NoWindow {
		m["query"] = map[string]any{"window": map[string]any{"from_ns": r.WinFrom, "to_ns": r.WinTo}}
	}
	b, _ := json.Marshal(m)
	return b
}

func i64(v int64) *int64 { return &v }

func good() resp {
	return resp{Completeness: "complete", Status: "ok", CT: i64(win.ToNs + 5e9), WinFrom: win.FromNs, WinTo: win.ToNs,
		Data: []map[string]any{{"service": "a", "value": "7"}, {"service": "b", "value": 1}}}
}

func TestInterpret(t *testing.T) {
	r := testRule(t)
	res := Interpret(200, good().body(), r, win)
	if res.Outcome != engine.Complete || len(res.Rows) != 2 || res.Rows[0].Value != 7 {
		t.Fatalf("%+v", res)
	}
	for name, x := range map[string]struct {
		mod  func(*resp)
		want engine.Outcome
	}{
		"partial":                   {func(r *resp) { r.Completeness, r.Partial = "partial", true }, engine.Partial},
		"unknown":                   {func(r *resp) { r.Completeness, r.Status, r.CT = "unknown", "stale", nil }, engine.Unknown},
		"complete but stale":        {func(r *resp) { r.Status = "stale" }, engine.Unknown},
		"complete, ct too low":      {func(r *resp) { r.CT = i64(win.ToNs - 1) }, engine.Unknown},
		"complete, no ct":           {func(r *resp) { r.CT = nil }, engine.Unknown},
		"other window":              {func(r *resp) { r.WinTo = win.ToNs + 1 }, engine.Unknown},
		"no window applied":         {func(r *resp) { r.NoWindow = true }, engine.Unknown},
		"complete, flagged partial": {func(r *resp) { r.Partial = true }, engine.Unknown},
		"null value":                {func(r *resp) { r.Data = []map[string]any{{"service": "a", "value": nil}} }, engine.BadResult},
		"nan value":                 {func(r *resp) { r.Data = []map[string]any{{"service": "a", "value": "nan"}} }, engine.BadResult},
		"no value column":           {func(r *resp) { r.Data = []map[string]any{{"service": "a"}} }, engine.BadResult},
		"duplicate group":           {func(r *resp) { r.Data = []map[string]any{{"service": "a", "value": 1}, {"service": "a", "value": 2}} }, engine.BadResult},
	} {
		g := good()
		x.mod(&g)
		if got := Interpret(200, g.body(), r, win); got.Outcome != x.want {
			t.Errorf("%s: %s (%s), want %s", name, got.Outcome, got.Err, x.want)
		}
	}
	for status, want := range map[int]engine.Outcome{401: engine.Refused, 403: engine.Refused, 400: engine.Refused,
		422: engine.Failed, 429: engine.Failed, 500: engine.Failed, 502: engine.Failed, 503: engine.Failed} {
		if got := Interpret(status, []byte(`{"error":"x"}`), r, win); got.Outcome != want {
			t.Errorf("HTTP %d: %s want %s", status, got.Outcome, want)
		}
	}
	late := testRule(t)
	late.Lateness = rule.Duration(10 * time.Second)
	if got := Interpret(200, good().body(), late, win); got.Outcome != engine.Partial {
		t.Errorf("complete_through within the lateness allowance must wait: %s", got.Outcome)
	}
	if got := Interpret(200, []byte("<html>"), r, win); got.Outcome != engine.Failed {
		t.Errorf("garbage: %s", got.Outcome)
	}
	// no rows is only a result in a complete window
	g := good()
	g.Data = nil
	if got := Interpret(200, g.body(), r, win); got.Outcome != engine.Complete || len(got.Rows) != 0 {
		t.Errorf("empty complete: %+v", got)
	}
	g.Completeness, g.Partial = "partial", true
	if got := Interpret(200, g.body(), r, win); got.Outcome != engine.Partial {
		t.Errorf("empty partial: %+v", got)
	}
}

// TestPropInterpretGate: an answer is Complete exactly when every gate
// holds; any status but 200 is never Complete.
func TestPropInterpretGate(tt *testing.T) {
	rapid.Check(tt, func(t *rapid.T) {
		r := testRule(tt)
		r.Lateness = rule.Duration(time.Duration(rapid.IntRange(0, 2).Draw(t, "lateness")) * time.Second)
		x := resp{
			Completeness: rapid.SampledFrom([]string{"complete", "partial", "unknown", ""}).Draw(t, "c"),
			Partial:      rapid.Bool().Draw(t, "p"),
			Status:       rapid.SampledFrom([]string{"ok", "stale", "missing", "error"}).Draw(t, "s"),
			WinFrom:      win.FromNs + int64(rapid.IntRange(-1, 1).Draw(t, "df")),
			WinTo:        win.ToNs + int64(rapid.IntRange(-1, 1).Draw(t, "dt")),
			NoWindow:     rapid.Bool().Draw(t, "nw"),
			Data:         []map[string]any{{"service": "a", "value": rapid.IntRange(0, 9).Draw(t, "v")}},
		}
		if rapid.Bool().Draw(t, "hasct") {
			x.CT = i64(win.ToNs + int64(rapid.IntRange(-2, 2).Draw(t, "ct"))*1e9)
		}
		status := rapid.SampledFrom([]int{200, 200, 200, 400, 401, 403, 422, 429, 500, 502, 503}).Draw(t, "http")
		got := Interpret(status, x.body(), r, win)
		gate := status == 200 && x.Completeness == "complete" && !x.Partial && x.Status == "ok" && x.CT != nil && *x.CT >= win.ToNs+int64(r.Lateness) &&
			!x.NoWindow && x.WinFrom == win.FromNs && x.WinTo == win.ToNs
		if (got.Outcome == engine.Complete) != gate {
			t.Fatalf("status %d %+v: %s (gate %v)", status, x, got.Outcome, gate)
		}
		if got.Outcome != engine.Complete && len(got.Rows) != 0 {
			t.Fatal("rows on a non-complete result")
		}
	})
}

func jwtWithExp(exp time.Time) string {
	p := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp.Unix())))
	return "e30." + p + ".sig"
}

func TestClientCredentialsAndRetryOn401(t *testing.T) {
	var issued atomic.Int32
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		_ = r.ParseForm()
		if u != "alertd" || p != "s3cret" || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("audience") != "otel-query" {
			http.Error(w, "bad client", 401)
			return
		}
		n := issued.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("tok%d", n), "expires_in": 300})
	}))
	defer idp.Close()
	t.Setenv("ALR_TEST_SECRET", "s3cret")
	ts, err := NewTokenSource(Identity{TokenURL: idp.URL, ClientID: "alertd", ClientSecretEnv: "ALR_TEST_SECRET", Audience: "otel-query"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	qs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		seen = append(seen, auth)
		if auth == "Bearer tok1" { // the first token was revoked
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"error":"bad_token"}`))
			return
		}
		_, _ = w.Write(good().body())
	}))
	defer qs.Close()
	c := &Client{URL: qs.URL, Identity: map[string]TokenSource{"default": ts}, Timeout: 5 * time.Second}
	res := c.Evaluate(context.Background(), testRule(t), win)
	if res.Outcome != engine.Complete || strings.Join(seen, ",") != "Bearer tok1,Bearer tok2" {
		t.Fatalf("%s %v", res.Outcome, seen)
	}
	// cached: no new token
	c.Evaluate(context.Background(), testRule(t), win)
	if issued.Load() != 2 {
		t.Fatalf("issued %d tokens", issued.Load())
	}
}

func TestFileTokenAndTimeout(t *testing.T) {
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(jwtWithExp(time.Now().Add(time.Hour))+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ts, _ := NewTokenSource(Identity{TokenFile: p}, nil)
	block := make(chan struct{})
	qs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer e30.") {
			w.WriteHeader(401)
			return
		}
		<-block
	}))
	defer qs.Close()
	defer close(block)
	c := &Client{URL: qs.URL, Identity: map[string]TokenSource{"default": ts}, Timeout: 200 * time.Millisecond}
	if res := c.Evaluate(context.Background(), testRule(t), win); res.Outcome != engine.Timeout {
		t.Fatalf("no answer must be a timeout (a failure), got %s", res.Outcome)
	}
	r := testRule(t)
	r.Identity = "team-x"
	if res := c.Evaluate(context.Background(), r, win); res.Outcome != engine.Refused {
		t.Fatalf("unknown identity: %s", res.Outcome)
	}
	if _, err := NewTokenSource(Identity{}, nil); err == nil {
		t.Fatal("an identity with no source")
	}
}

func TestJWTExp(t *testing.T) {
	exp := time.Now().Add(90 * time.Second).Truncate(time.Second)
	got, ok := jwtExp(jwtWithExp(exp))
	if !ok || !got.Equal(exp) {
		t.Fatal(got, ok)
	}
}
