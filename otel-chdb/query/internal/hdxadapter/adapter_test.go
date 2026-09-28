package hdxadapter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// fakeService stands in for POST /v1/query.
type fakeService struct {
	mu     sync.Mutex
	bodies []map[string]any
	auths  []string
	status int
	answer string
}

func (f *fakeService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	f.auths = append(f.auths, r.Header.Get("Authorization"))
	status, answer := f.status, f.answer
	f.mu.Unlock()
	if status == 0 {
		status = 200
	}
	w.WriteHeader(status)
	io.WriteString(w, answer)
}

const okAnswer = `{"request_id":"r1","source":"central","complete_through":"2026-09-28T12:00:00Z","complete_through_ns":1,
 "max_lateness_s":60,"settled_through":"2026-09-28T11:59:00Z","late":{"status":"counted","rows":4},
 "completeness":"partial","partial":true,"incomplete_from":"2026-09-28T11:59:00Z",
 "watermark":{"status":"ok","lag_s":28.84,"holding":[{"lane":"prod-a/pub-0/logs","wm_ns":1,"lag_s":28.84}],
  "scope":{"clusters":["prod-a"],"signals":["logs"]}},"query":{"rows_read":5,"elapsed_ms":1.5},
 "result":{"meta":[{"name":"b","type":"String"},{"name":"a","type":"UInt64"},{"name":"b","type":"Array(String)"}],
  "data":[{"b":"x/y","a":"7","b":["p", "q"]},{"b":"é","a":"8","b":[]}],"rows":2,"statistics":{"elapsed":0.001}}}`

func newTestAdapter(t *testing.T) (*fakeService, *httptest.Server) {
	t.Helper()
	fs := &fakeService{answer: okAnswer}
	svc := httptest.NewServer(fs)
	t.Cleanup(svc.Close)
	a := New(Config{QueryURL: svc.URL + "/v1/query", DefaultDatabase: "otel", TokenHeader: "X-Forwarded-Access-Token",
		TimeColumns: map[string]string{"otel_logs": "Timestamp"}}, nil)
	hs := httptest.NewServer(a)
	t.Cleanup(hs.Close)
	return fs, hs
}

func post(t *testing.T, u string, header http.Header, body io.Reader) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, u, body)
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

func TestAdapterForwardsWithTokenAndLabels(t *testing.T) {
	fs, hs := newTestAdapter(t)
	v := url.Values{"param_a": {"1000"}, "param_b": {"2000"}, "param_s": {`it\'s`}, "query_id": {"q-1"},
		"date_time_output_format": {"iso"}, "max_execution_time": {"99"}, "result_overflow_mode": {"break"}, "add_http_cors_header": {"1"}}
	q := "SELECT b, a, b FROM otel_logs WHERE Timestamp >= fromUnixTimestamp64Milli({a:Int64}) AND Timestamp <= fromUnixTimestamp64Milli({b:Int64}) AND Body = {s:String} \nFORMAT JSONCompactEachRowWithNamesAndTypes"
	resp, body := post(t, hs.URL+"/?"+v.Encode(), http.Header{"Authorization": {"Bearer tok-1"}, "X-Clickhouse-User": {"default"}}, strings.NewReader(q))
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if fs.auths[0] != "Bearer tok-1" {
		t.Fatalf("token %q", fs.auths[0])
	}
	sent := fs.bodies[0]
	if sent["sql"] != `SELECT b, a, b FROM otel_logs WHERE Timestamp >= fromUnixTimestamp64Milli(toInt64(1000)) AND Timestamp <= fromUnixTimestamp64Milli(toInt64(2000)) AND Body = 'it\'s'` {
		t.Fatalf("sql %v", sent["sql"])
	}
	if w := sent["window"].(map[string]any); w["from"] != float64(1000e6) || w["to"] != float64(2000e6+1) {
		t.Fatalf("window %v", w)
	}
	if o := sent["output"].(map[string]any); len(o) != 1 || o["date_time_output_format"] != "iso" {
		t.Fatalf("output %v", o)
	}
	h := resp.Header
	for k, want := range map[string]string{"X-Otel-Completeness": "partial", "X-Otel-Complete-Through": "2026-09-28T12:00:00Z",
		"X-Otel-Source": "central", "X-Otel-Watermark-Status": "ok", "X-Otel-Watermark-Lag-S": "28.8", "X-Otel-Request-Id": "r1",
		"X-Otel-Dropped-Settings": "add_http_cors_header,max_execution_time,result_overflow_mode", "X-Clickhouse-Query-Id": "q-1",
		"X-Otel-Window-From": "1970-01-01T00:00:01Z", "X-Otel-Window-To": "1970-01-01T00:00:02.000000001Z",
		"X-Otel-Max-Lateness-S": "60", "X-Otel-Settled-Through": "2026-09-28T11:59:00Z", "X-Otel-Incomplete-From": "2026-09-28T11:59:00Z",
		"X-Otel-Late-Rows": "4", "X-Otel-Watermark-Scope": "clusters=prod-a; signals=logs",
		"X-Otel-Watermark-Holding": "prod-a/pub-0/logs 29s"} {
		if h.Get(k) != want {
			t.Errorf("%s = %q, want %q", k, h.Get(k), want)
		}
	}
	want := "[\"b\",\"a\",\"b\"]\n[\"String\",\"UInt64\",\"Array(String)\"]\n[\"x/y\",\"7\",[\"p\",\"q\"]]\n[\"é\",\"8\",[]]\n"
	if body != want {
		t.Fatalf("body %q", body)
	}
}

func TestAdapterFormats(t *testing.T) {
	_, hs := newTestAdapter(t)
	for format, want := range map[string]string{
		"JSONEachRow":        "{\"b\":\"x/y\",\"a\":\"7\",\"b\":[\"p\",\"q\"]}\n{\"b\":\"é\",\"a\":\"8\",\"b\":[]}\n",
		"JSONCompactEachRow": "[\"x/y\",\"7\",[\"p\",\"q\"]]\n[\"é\",\"8\",[]]\n",
	} {
		resp, body := post(t, hs.URL, http.Header{"Authorization": {"Bearer t"}}, strings.NewReader("SELECT 1 FORMAT "+format))
		if resp.StatusCode != 200 || body != want {
			t.Errorf("%s: %d %q", format, resp.StatusCode, body)
		}
	}
	resp, body := post(t, hs.URL, http.Header{"Authorization": {"Bearer t"}}, strings.NewReader("SELECT 1 FORMAT JSON"))
	var r map[string]any
	if resp.StatusCode != 200 || json.Unmarshal([]byte(body), &r) != nil || r["rows"] != float64(2) {
		t.Fatalf("JSON: %s", body)
	}
	// SELECT 1 reads no table: schema-like, no completeness label
	if resp.Header.Get("X-Otel-Source") != "metadata" || resp.Header.Get("X-Otel-Completeness") != "" {
		t.Fatalf("headers %v", resp.Header)
	}
}

func TestAdapterMultipartGzipAndTokenHeader(t *testing.T) {
	fs, hs := newTestAdapter(t)
	var mb bytes.Buffer
	mw := multipart.NewWriter(&mb)
	mw.WriteField("query", "SELECT count() FROM otel_logs WHERE Body = {p:String} FORMAT JSON")
	mw.WriteField("param_p", `a\tb`)
	mw.Close()
	resp, body := post(t, hs.URL+"/?query_id=x", http.Header{"Content-Type": {mw.FormDataContentType()}, "X-Forwarded-Access-Token": {"tok-2"}}, &mb)
	if resp.StatusCode != 200 || fs.auths[0] != "Bearer tok-2" || fs.bodies[0]["sql"] != `SELECT count() FROM otel_logs WHERE Body = 'a\x09b'` {
		t.Fatalf("%d %s %v %v", resp.StatusCode, body, fs.auths, fs.bodies)
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte("SELECT 2 FORMAT JSON"))
	zw.Close()
	resp, body = post(t, hs.URL, http.Header{"Content-Encoding": {"gzip"}, "Authorization": {"Bearer t"}}, &gz)
	if resp.StatusCode != 200 || fs.bodies[1]["sql"] != "SELECT 2" {
		t.Fatalf("%d %s %v", resp.StatusCode, body, fs.bodies[1])
	}
}

func TestAdapterRefusals(t *testing.T) {
	fs, hs := newTestAdapter(t)
	auth := http.Header{"Authorization": {"Bearer t"}}
	cases := []struct {
		name, url, sql string
		header         http.Header
		status, code   int
	}{
		{"no token (basic auth is not a token)", "", "SELECT 1 FORMAT JSON", http.Header{"Authorization": {"Basic ZGVmYXVsdDo="}}, 401, 516},
		{"explain", "", "EXPLAIN ESTIMATE SELECT 1 FORMAT JSON", auth, 403, 497},
		{"csv", "", "SELECT 1 FORMAT CSV", auth, 400, 73},
		{"bad param", "?param_n=1%20OR%201", "SELECT {n:Int32} FORMAT JSON", auth, 400, 457},
		{"missing param", "", "SELECT {n:Int32} FORMAT JSON", auth, 400, 456},
		{"url and body", "?query=INSERT+INTO+t+FORMAT+JSONEachRow", "{\"a\":1}", auth, 403, 497},
		{"another database", "?database=secret", "SELECT 1 FORMAT JSON", auth, 403, 497},
	}
	for _, c := range cases {
		resp, body := post(t, hs.URL+"/"+c.url, c.header, strings.NewReader(c.sql))
		if resp.StatusCode != c.status || resp.Header.Get("X-ClickHouse-Exception-Code") != strconvI(c.code) ||
			!strings.HasPrefix(body, "Code: "+strconvI(c.code)+". DB::Exception: ") {
			t.Errorf("%s: %d %v %q", c.name, resp.StatusCode, resp.Header, body)
		}
	}
	if len(fs.bodies) != 0 {
		t.Fatalf("a refused statement reached the service: %v", fs.bodies)
	}
	// the service's refusals become ClickHouse errors the client parses
	for _, c := range []struct {
		status int
		answer string
		code   int
		name   string
	}{
		{403, `{"error":"table_function","detail":"table functions are not allowed: mergeTreeTextIndex('a', 'b', 'c')","request_id":"r"}`, 497, "ACCESS_DENIED"},
		{401, `{"error":"bad_token","detail":"token is expired","request_id":"r"}`, 516, "AUTHENTICATION_FAILED"},
		{422, `{"error":"limit_exceeded:TOO_MANY_ROWS","detail":"clickhouse 500 (code 158): Limit for rows","request_id":"r"}`, 158, "TOO_MANY_ROWS"},
		{429, `{"error":"too_many_concurrent","detail":"at most 4","request_id":"r"}`, 202, "TOO_MANY_SIMULTANEOUS_QUERIES"},
		{400, `{"error":"central_rejected","detail":"clickhouse 400 (code 47): Unknown identifier","request_id":"r"}`, 47, "CLICKHOUSE_ERROR"},
		{503, `{"error":"audit_unavailable","detail":"nothing ran","request_id":"r"}`, 210, "NETWORK_ERROR"},
	} {
		fs.status, fs.answer = c.status, c.answer
		resp, body := post(t, hs.URL, auth, strings.NewReader("SELECT 1 FORMAT JSON"))
		if resp.StatusCode != c.status || resp.Header.Get("X-ClickHouse-Exception-Code") != strconvI(c.code) {
			t.Errorf("%s: %d %q", c.answer, resp.StatusCode, body)
		}
		// the client's own pattern (@clickhouse/client-common error.js)
		if !clientErrorRE.MatchString(body) || clientErrorRE.FindStringSubmatch(body)[3] != c.name {
			t.Errorf("the client would not parse %q", body)
		}
	}
	// DESCRIBE of a table the service does not show: UNKNOWN_TABLE, not an
	// empty column list
	fs.status, fs.answer = 200, `{"request_id":"r","source":"central","result":{"meta":[{"name":"name","type":"String"}],"data":[],"rows":0}}`
	resp, body := post(t, hs.URL+"/?param_d=otel&param_t=nope", auth, strings.NewReader("DESCRIBE {d:Identifier}.{t:Identifier} FORMAT JSON"))
	if resp.StatusCode != 404 || resp.Header.Get("X-ClickHouse-Exception-Code") != "60" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}
