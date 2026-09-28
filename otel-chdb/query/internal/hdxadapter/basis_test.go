package hdxadapter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// basisService answers /v1/basis (a fresh token per mint) and /v1/query
// (echoing the basis it was sent, or none).
type basisService struct {
	mints   atomic.Int64
	mu      sync.Mutex
	sent    []any
	refuse  string // a /v1/query refusal for the next statement at a basis
	refused int
}

func (s *basisService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/basis" {
		n := s.mints.Add(1)
		fmt.Fprintf(w, `{"request_id":"m%d","basis":"b1.tok%d.mac","basis_info":{"clusters":[{"cluster":"*","received_before":"2026-09-28T12:00:0%dZ"}]}}`, n, n, n)
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	s.mu.Lock()
	s.sent = append(s.sent, body["basis"])
	ref := s.refuse
	if ref != "" && body["basis"] != nil {
		s.refuse = ""
		s.refused++
	}
	s.mu.Unlock()
	if ref != "" && body["basis"] != nil {
		w.WriteHeader(http.StatusGone)
		fmt.Fprintf(w, `{"error":%q,"detail":"x","request_id":"r"}`, ref)
		return
	}
	ans := strings.Replace(okAnswer, `"request_id":"r1",`, `"request_id":"r1","at_basis":false,`, 1)
	if b, ok := body["basis"].(string); ok {
		ans = strings.Replace(okAnswer, `"request_id":"r1",`, fmt.Sprintf(`"request_id":"r1","at_basis":true,"basis":%q,"basis_info":{"clusters":[{"cluster":"*","received_before":"2026-09-28T12:00:00Z"}]},`, b), 1)
	}
	io.WriteString(w, ans)
}

func newBasisAdapter(t *testing.T) (*basisService, *httptest.Server) {
	s := &basisService{}
	svc := httptest.NewServer(s)
	t.Cleanup(svc.Close)
	hs := httptest.NewServer(New(Config{QueryURL: svc.URL + "/v1/query", DefaultDatabase: "otel"}, nil))
	t.Cleanup(hs.Close)
	return s, hs
}

// Every statement of one refresh (one group) reads at one basis, however
// many run at once; another group, or another caller, gets its own.
func TestBasisGroupPinsOneBasisPerRefresh(t *testing.T) {
	s, hs := newBasisAdapter(t)
	q := "SELECT count() FROM otel_logs FORMAT JSON"
	var wg sync.WaitGroup
	got := make([]string, 12)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, _ := post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer tok-1"}, "X-Otel-Basis-Group": {"refresh-0001"}}, strings.NewReader(q))
			got[i] = resp.Header.Get("X-Otel-Basis") + " " + resp.Header.Get("X-Otel-At-Basis")
		}(i)
	}
	wg.Wait()
	for _, g := range got {
		if g != got[0] || g != "b1.tok1.mac true" {
			t.Fatalf("one refresh, several bases: %v", got)
		}
	}
	if s.mints.Load() != 1 {
		t.Fatalf("%d mints for one group", s.mints.Load())
	}
	// a new refresh: a new basis; another caller in the same group id: its own
	resp, _ := post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer tok-1"}, "X-Otel-Basis-Group": {"refresh-0002"}}, strings.NewReader(q))
	resp2, _ := post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer tok-2"}, "X-Otel-Basis-Group": {"refresh-0001"}}, strings.NewReader(q))
	if resp.Header.Get("X-Otel-Basis") == got[0] || resp2.Header.Get("X-Otel-Basis") == strings.Fields(got[0])[0] || s.mints.Load() != 3 {
		t.Fatal(resp.Header.Get("X-Otel-Basis"), resp2.Header.Get("X-Otel-Basis"))
	}
	if resp.Header.Get("X-Otel-Basis-Info") != "*<2026-09-28T12:00:00Z" {
		t.Fatal(resp.Header.Get("X-Otel-Basis-Info"))
	}
	// an explicit basis is passed through; none: the answer says it is not at one
	resp, _ = post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer tok-1"}, "X-Otel-Basis": {"b1.given.mac"}}, strings.NewReader(q))
	if resp.Header.Get("X-Otel-Basis") != "b1.given.mac" {
		t.Fatal(resp.Header.Get("X-Otel-Basis"))
	}
	resp, _ = post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer tok-1"}}, strings.NewReader(q))
	if resp.Header.Get("X-Otel-At-Basis") != "false" || resp.Header.Get("X-Otel-Basis") != "" {
		t.Fatal(resp.Header)
	}
	for _, bad := range []http.Header{{"X-Otel-Basis": {"b1.x;DROP.y"}}, {"X-Otel-Basis-Group": {"short"}}, {"X-Otel-Basis-Group": {strings.Repeat("g", 65)}}} {
		bad.Set("Authorization", "Bearer tok-1")
		resp, body := post(t, hs.URL+"/", bad, strings.NewReader(q))
		if resp.StatusCode != 400 || !strings.Contains(body, "BAD_ARGUMENTS") {
			t.Errorf("%v: %d %s", bad, resp.StatusCode, body)
		}
	}
}

// A group's basis the service no longer accepts is re-minted once; an
// explicit basis is never replaced (the refusal reaches the fork).
func TestBasisGroupRemintOnRefusal(t *testing.T) {
	s, hs := newBasisAdapter(t)
	q := "SELECT count() FROM otel_logs FORMAT JSON"
	h := http.Header{"Authorization": {"Bearer tok-1"}, "X-Otel-Basis-Group": {"refresh-0001"}}
	post(t, hs.URL+"/", h, strings.NewReader(q))
	s.mu.Lock()
	s.refuse = "basis_expired"
	s.mu.Unlock()
	resp, body := post(t, hs.URL+"/", h, strings.NewReader(q))
	if resp.StatusCode != 200 || resp.Header.Get("X-Otel-Basis") != "b1.tok2.mac" || s.refused != 1 {
		t.Fatalf("%d %s %s", resp.StatusCode, resp.Header.Get("X-Otel-Basis"), body)
	}
	s.mu.Lock()
	s.refuse = "basis_expired"
	s.mu.Unlock()
	resp, body = post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer tok-1"}, "X-Otel-Basis": {"b1.given.mac"}}, strings.NewReader(q))
	if resp.StatusCode != 410 || resp.Header.Get("X-Otel-Refusal") != "basis_expired" || !strings.Contains(body, "BAD_ARGUMENTS") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}
