package ingress

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// FuzzBearer: the Authorization header parser never panics, and a token it
// returns is a substring of the header with no surrounding space. The seeds
// run as a unit test on every push: "Bearer " followed by blanks is CAST row
// 67's case (ok with an empty token, before a9b7be9).
func FuzzBearer(f *testing.F) {
	tracetag.Covers(f, "FZ", "CAST-67", "H-6")
	for _, h := range []string{"Bearer abc", "bearer abc", "Bearer  abc ", "Basic x", "Bearer", "", "Bearer a b",
		"Bearer ", "Bearer    ", "Bearer \t ", "bearer\t"} {
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
	// Requests this rig ACKed, by their decoded content (canonical protobuf):
	// the edge ACKs a resend of committed content without a second object
	// (exactly once), and the fuzzer repeats inputs within a worker. Nightly
	// fuzz run 34 failed on exactly that ("200 but nothing committed"),
	// which the minimised input alone, on a fresh rig, does not reproduce.
	acked := map[string]bool{}
	canonical := func(body []byte, mode uint8) (string, int, bool) {
		var td ptrace.Traces
		var err error
		switch mode % 4 {
		case 0:
			td, err = (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(body)
		case 1:
			td, err = (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(body)
		case 2:
			zr, zerr := gzip.NewReader(bytes.NewReader(body))
			if zerr != nil {
				return "", 0, false
			}
			raw, rerr := io.ReadAll(zr)
			if rerr != nil {
				return "", 0, false
			}
			td, err = (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(raw)
		default:
			return "", 0, false
		}
		if err != nil {
			return "", 0, false
		}
		b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		return string(b), td.SpanCount(), err == nil
	}
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
			// an empty request (no spans) is ACKed without an object, and so is
			// a resend of content this rig already committed
			if key, spans, ok := canonical(body, mode); mode%4 == 0 && ok && spans > 0 && !acked[key] {
				t.Fatalf("200 but nothing committed for %d spans", spans)
			}
		case c == http.StatusOK:
			if key, spans, ok := canonical(body, mode); ok && spans > 0 {
				acked[key] = true
			}
		}
	})
}

// The shape FuzzBody's nightly run 34 hit: the same request twice to one
// ingress is ACKed twice and committed once (the edge knows the content), so
// "200 and no new object" is a resend, not a lost request. The fuzz oracle
// tells the two apart by the content it ACKed before.
func TestAResendIsACKedWithoutASecondObject(t *testing.T) {
	tracetag.Covers(t, "FZ", "CAST-80")
	lim := DefaultLimits()
	lim.RequestsPerMinute, lim.DecodedBytesPerMinute = 1e12, 1e15
	r := newRig(t, "ingress-0", nil, nil, &lim)
	tok := "Bearer " + userToken(r.is, alice, "Team.Payments")
	body, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(agentTraces())
	h := r.srv.Handler()
	for i, want := range []int{1, 1, 1} {
		req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
		req.Header.Set("Authorization", tok)
		req.Header.Set("Content-Type", "application/x-protobuf")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK || len(r.store.Keys("root/")) != want {
			t.Fatalf("send %d: %d, %d objects, want 200 and %d", i, w.Code, len(r.store.Keys("root/")), want)
		}
	}
}
