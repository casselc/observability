package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// fakeCH records what reaches "ClickHouse" and answers with a fixed,
// compressed-looking body; statements containing FAIL get a 500 before any
// byte of the result.
type fakeCH struct {
	mu   sync.Mutex
	seen []string
}

func (f *fakeCH) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	q := r.URL.Query().Get("query") + string(b)
	f.mu.Lock()
	f.seen = append(f.seen, q)
	f.mu.Unlock()
	if strings.Contains(q, "resource_kv_missing") {
		w.Header().Set("X-ClickHouse-Exception-Code", "60")
		w.WriteHeader(500)
		io.WriteString(w, "Code: 60. DB::Exception: Unknown table")
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("X-ClickHouse-Summary", `{"read_rows":"1"}`)
	w.WriteHeader(200)
	w.Write([]byte{0x1f, 0x8b, 1, 2, 3}) // opaque bytes: must arrive unchanged
}

func proxyFor(t *testing.T, mode, kv string) (*httptest.Server, *fakeCH) {
	ch := &fakeCH{}
	up := httptest.NewServer(ch)
	t.Cleanup(up.Close)
	cfg := testConfig(mode)
	cfg.Upstream = up.URL
	cfg.Fallback = true
	if kv != "" {
		cfg.KV = kv
	}
	s, err := newServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ps := httptest.NewServer(s)
	t.Cleanup(ps.Close)
	return ps, ch
}

const testStmt = "SELECT count() FROM {db:Identifier}.{t:Identifier} WHERE ResourceAttributes['k8s.pod.name'] = 'p'"

func TestProxyBody(t *testing.T) {
	ps, ch := proxyFor(t, "catalog", "")
	qs := url.Values{"param_db": {"rw_c"}, "param_t": {"otel_logs"}, "max_threads": {"2"}}
	// a client that doesn't decode gzip itself: the bytes must arrive as sent
	cl := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, _ := http.NewRequest("POST", ps.URL+"/?"+qs.Encode(), strings.NewReader(testStmt))
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := cl.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, []byte{0x1f, 0x8b, 1, 2, 3}) || resp.Header.Get("Content-Encoding") != "gzip" || resp.Header.Get("X-ClickHouse-Summary") == "" {
		t.Fatalf("response changed: %v %q", resp.Header, body)
	}
	if got := ch.seen[0]; !strings.Contains(got, "resource_id IN (SELECT resource_id FROM cat.kv") {
		t.Fatalf("not rewritten: %s", got)
	}
}

func TestProxyURLQueryAndGzip(t *testing.T) {
	ps, ch := proxyFor(t, "catalog", "")
	qs := url.Values{"param_db": {"rw_c"}, "param_t": {"otel_logs"}, "query": {testStmt}}
	if _, err := http.Get(ps.URL + "/?" + qs.Encode()); err != nil {
		t.Fatal(err)
	}
	var zb bytes.Buffer
	zw := gzip.NewWriter(&zb)
	zw.Write([]byte(testStmt))
	zw.Close()
	req, _ := http.NewRequest("POST", ps.URL+"/?param_db=rw_c&param_t=otel_logs", &zb)
	req.Header.Set("Content-Encoding", "gzip")
	if _, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}
	for _, q := range ch.seen {
		if !strings.Contains(q, "cat.kv") {
			t.Fatalf("not rewritten: %s", q)
		}
	}
}

func TestProxyPassthroughInsert(t *testing.T) {
	ps, ch := proxyFor(t, "catalog", "")
	data := "{\"a\":1}\n"
	qs := url.Values{"query": {"INSERT INTO rw_c.otel_logs FORMAT JSONEachRow"}}
	if _, err := http.Post(ps.URL+"/?"+qs.Encode(), "text/plain", strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if ch.seen[0] != "INSERT INTO rw_c.otel_logs FORMAT JSONEachRow"+data {
		t.Fatalf("changed: %q", ch.seen[0])
	}
}

// the rewritten statement fails before streaming (here: the catalog table is
// missing): the proxy re-sends the original
func TestProxyFallback(t *testing.T) {
	ps, ch := proxyFor(t, "catalog", "resource_kv_missing")
	resp, err := http.Post(ps.URL+"/?param_db=rw_c&param_t=otel_logs", "text/plain", strings.NewReader(testStmt))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || len(ch.seen) != 2 || ch.seen[1] != testStmt {
		t.Fatalf("status %d, seen %q", resp.StatusCode, ch.seen)
	}
}

// @clickhouse/client-web's multipart/form-data body: query and param_* are
// fields; the query field is rewritten, the rest re-sent as it came
func TestProxyMultipart(t *testing.T) {
	ps, ch := proxyFor(t, "catalog", "")
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	w.WriteField("param_db", "rw_c")
	w.WriteField("param_t", "otel_logs")
	w.WriteField("query", testStmt)
	w.Close()
	if _, err := http.Post(ps.URL+"/?query_id=x", w.FormDataContentType(), &b); err != nil {
		t.Fatal(err)
	}
	mb, ok := parseMultipart(w.FormDataContentType(), []byte(ch.seen[0]))
	if !ok || mb.fields["param_db"] != "rw_c" || !strings.Contains(mb.fields["query"], "cat.kv") {
		t.Fatalf("%q", ch.seen[0])
	}
}
