package ingress

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// FuzzBearer: the Authorization header parser never panics, and a token it
// returns is a substring of the header with no surrounding space.
func FuzzBearer(f *testing.F) {
	for _, h := range []string{"Bearer abc", "bearer abc", "Bearer  abc ", "Basic x", "Bearer", "", "Bearer a b"} {
		f.Add(h)
	}
	f.Fuzz(func(t *testing.T, h string) {
		tok, ok := Bearer(h)
		if ok && (tok == "" || !strings.Contains(h, tok) || strings.TrimSpace(tok) != tok) {
			t.Fatalf("Bearer(%q) = %q", h, tok)
		}
	})
}

// FuzzVerify: no byte string is accepted as a token unless the issuer
// signed it, and none panics the verifier. The seeds include a real token
// and mutations of it.
func FuzzVerify(f *testing.F) {
	r := newRig(f, "ingress-0", nil, nil, nil)
	good := userToken(r.is, alice, "Team.Payments")
	f.Add(good)
	f.Add(good[:len(good)-2])
	f.Add(strings.Replace(good, ".", "..", 1))
	f.Add("e30.e30.")
	f.Add("")
	f.Fuzz(func(t *testing.T, tok string) {
		id, err := r.srv.Verifier.Verify(context.Background(), tok)
		if err == nil && tok != good {
			t.Fatalf("accepted a token the issuer did not mint: %q (%+v)", tok, id)
		}
	})
}

// FuzzBody: an authenticated caller's body, in any encoding, never panics
// the handler or gets a 5xx other than 503 (not committed; retry); a 200
// means the edge committed an object.
func FuzzBody(f *testing.F) {
	lim := DefaultLimits()
	lim.RequestsPerMinute, lim.DecodedBytesPerMinute = 1e12, 1e15
	r := newRig(f, "ingress-0", nil, nil, &lim)
	tok := "Bearer " + userToken(r.is, alice, "Team.Payments")
	good, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(agentTraces())
	jsonBody, _ := (&ptrace.JSONMarshaler{}).MarshalTraces(agentTraces())
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(good)
	_ = zw.Close()
	f.Add(good, uint8(0))
	f.Add(jsonBody, uint8(1))
	f.Add(gz.Bytes(), uint8(2))
	f.Add([]byte{0x0a, 0xff, 0xff, 0xff, 0x0f}, uint8(0))
	h := r.srv.Handler()
	f.Fuzz(func(t *testing.T, body []byte, mode uint8) {
		req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
		req.Header.Set("Authorization", tok)
		switch mode % 4 {
		case 0:
			req.Header.Set("Content-Type", "application/x-protobuf")
		case 1:
			req.Header.Set("Content-Type", "application/json")
		case 2:
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("Content-Encoding", "gzip")
		case 3:
			req.Header.Set("Content-Type", "text/plain")
		}
		before := len(r.store.Keys("root/"))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		switch c := w.Code; {
		case c >= 500 && c != http.StatusServiceUnavailable:
			t.Fatalf("%d for a %d-byte body (mode %d): %s", c, len(body), mode%4, w.Body.String())
		case c == http.StatusOK && len(r.store.Keys("root/")) == before:
			// an empty request (no spans) is ACKed without an object
			td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(body)
			if mode%4 == 0 && err == nil && td.SpanCount() > 0 {
				t.Fatalf("200 but nothing committed for %d spans", td.SpanCount())
			}
		}
	})
}
