package qclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
)

func withBasis(b []byte, extra map[string]any) []byte {
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for k, v := range extra {
		m[k] = v
	}
	out, _ := json.Marshal(m)
	return out
}

var info = map[string]any{"clusters": []any{map[string]any{"cluster": "c1", "received_before_ns": 1790000000123456789}}}

// An answer at a basis carries it into the result (the window's record);
// an answer that is not at a basis carries none.
func TestInterpretBasis(t *testing.T) {
	r := testRule(t)
	res := Interpret(200, withBasis(good().body(), map[string]any{"at_basis": true, "basis": "b1.x.y", "basis_info": info}), r, win)
	if res.Outcome != engine.Complete || res.Basis != "b1.x.y" || res.BasisC["c1"] != 1790000000123456789 {
		t.Fatalf("%+v", res)
	}
	res = Interpret(200, withBasis(good().body(), map[string]any{"at_basis": false, "basis": "b1.x.y", "basis_info": info}), r, win)
	if res.Outcome != engine.Complete || res.Basis != "" {
		t.Fatalf("a basis the answer was not computed at was recorded: %+v", res)
	}
	// no watermark to mint from: unknown, as without a basis (not a failure)
	res = Interpret(503, []byte(`{"error":"basis_unverifiable","detail":"the watermark is unknown"}`), r, win)
	if res.Outcome != engine.Unknown {
		t.Fatalf("%+v", res)
	}
}

func TestInterpretDelta(t *testing.T) {
	ok := withBasis(good().body(), map[string]any{"at_basis": true, "basis": "b1.new", "basis_info": info,
		"delta": map[string]any{"status": "counted", "rows": 3}})
	d := InterpretDelta(200, ok)
	if d.Outcome != engine.Complete || d.Rows != 3 || d.Basis != "b1.new" || d.C["c1"] == 0 {
		t.Fatalf("%+v", d)
	}
	for name, c := range map[string]struct {
		status int
		body   []byte
		want   engine.Outcome
	}{
		"regressed":      {409, []byte(`{"error":"basis_regressed"}`), engine.Partial},
		"invalid":        {400, []byte(`{"error":"basis_invalid"}`), engine.Refused},
		"5xx":            {502, []byte(`{}`), engine.Failed},
		"no delta block": {200, withBasis(good().body(), map[string]any{"at_basis": true, "basis": "b1.new", "basis_info": info}), engine.Failed},
		"not counted":    {200, withBasis(good().body(), map[string]any{"at_basis": true, "basis": "b1.new", "basis_info": info, "delta": map[string]any{"status": "error", "rows": nil}}), engine.Failed},
		"ignored basis":  {200, withBasis(good().body(), map[string]any{"delta": map[string]any{"status": "counted", "rows": 0}}), engine.Failed},
		"garbage":        {200, []byte(`{`), engine.Failed},
	} {
		if d := InterpretDelta(c.status, c.body); d.Outcome != c.want {
			t.Errorf("%s: %+v", name, d)
		}
	}
}

// The client sends basis "latest" for an evaluation, and basis_from with
// basis for a delta.
func TestClientSendsBases(t *testing.T) {
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var m map[string]any
		_ = json.NewDecoder(req.Body).Decode(&m)
		got = append(got, m)
		_, _ = w.Write(withBasis(good().body(), map[string]any{"at_basis": true, "basis": "b1.new", "basis_info": info,
			"delta": map[string]any{"status": "counted", "rows": 0}}))
	}))
	defer srv.Close()
	c := &Client{URL: srv.URL, Identity: map[string]TokenSource{"default": staticToken("t")}}
	r := testRule(t)
	if res := c.Evaluate(context.Background(), r, win); res.Basis != "b1.new" {
		t.Fatalf("%+v", res)
	}
	if d := c.Delta(context.Background(), r, win, "b1.old", "latest"); d.Outcome != engine.Complete {
		t.Fatalf("%+v", d)
	}
	if got[0]["basis"] != "latest" || got[0]["basis_from"] != nil || got[1]["basis"] != "latest" || got[1]["basis_from"] != "b1.old" {
		t.Fatalf("%v", got)
	}
}

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }
func (s staticToken) Invalidate()                           {}
