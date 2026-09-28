package hdxadapter

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const sampleAnswer = `{"request_id":"r2","source":"central","completeness":"sample","partial":true,
 "watermark":{"status":"ok"},"late":{"status":"sample"},"query":{"rows_read":3000123,"elapsed_ms":1},
 "sample":{"sample":true,"rows_read":3000123,"max_rows_to_read":3000000,"reached_bound":true,"data_completeness":"complete"},
 "settings":{"use_skip_indexes":"1"},
 "result":{"meta":[{"name":"k","type":"String"}],"data":[{"k":"a"}],"rows":1}}`

func adapterWith(t *testing.T, answer string, pass ...string) (*fakeService, *httptest.Server) {
	t.Helper()
	fs := &fakeService{answer: answer}
	svc := httptest.NewServer(fs)
	t.Cleanup(svc.Close)
	a := New(Config{QueryURL: svc.URL + "/v1/query", DefaultDatabase: "otel", PassSettings: pass}, nil)
	hs := httptest.NewServer(a)
	t.Cleanup(hs.Close)
	return fs, hs
}

// TestAdapterSample: HyperDX's flagged read sample (read_overflow_mode
// break at max_rows_to_read) becomes the service's labelled sample; the
// label comes back as completeness "sample" with X-Otel-Sample.
func TestAdapterSample(t *testing.T) {
	fs, hs := adapterWith(t, sampleAnswer)
	v := url.Values{"read_overflow_mode": {"break"}, "max_rows_to_read": {"3000000"}, "read_overflow_mode_leaf": {"throw"},
		"result_overflow_mode": {"throw"}}
	resp, body := post(t, hs.URL+"/?"+v.Encode(), http.Header{"Authorization": {"Bearer t"}},
		strings.NewReader("SELECT DISTINCT arrayJoin(mapKeys(LogAttributes)) AS k FROM otel_logs LIMIT 10 FORMAT JSON"))
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	smp, ok := fs.bodies[0]["sample"].(map[string]any)
	if !ok || smp["rows"] != 3e6 {
		t.Fatalf("sample %v", fs.bodies[0])
	}
	if resp.Header.Get("X-Otel-Completeness") != "sample" ||
		resp.Header.Get("X-Otel-Sample") != "rows_read=3000123; max_rows_to_read=3000000; reached_bound=true; data_completeness=complete" {
		t.Fatalf("headers %v", resp.Header)
	}
	if d := resp.Header.Get("X-Otel-Dropped-Settings"); d != "read_overflow_mode_leaf,result_overflow_mode" {
		t.Fatalf("dropped %q", d)
	}
	// without break, no sample: the limit stays an error
	post(t, hs.URL+"/?max_rows_to_read=5", http.Header{"Authorization": {"Bearer t"}}, strings.NewReader("SELECT 1 FORMAT JSON"))
	if _, ok := fs.bodies[1]["sample"]; ok {
		t.Fatal("a statement without read_overflow_mode=break asked for a sample")
	}
}

// TestAdapterSettings: allow-listed URL settings and the statement's own
// trailing SETTINGS go to the service as settings (it checks them); others
// are dropped and named; SETTINGS in a subquery stays in the text.
func TestAdapterSettings(t *testing.T) {
	fs, hs := adapterWith(t, sampleAnswer, "use_skip_indexes", "optimize_read_in_order")
	v := url.Values{"use_skip_indexes": {"1"}, "max_threads": {"8"}, "allow_experimental_analyzer": {"1"}}
	resp, body := post(t, hs.URL+"/?"+v.Encode(), http.Header{"Authorization": {"Bearer t"}},
		strings.NewReader("SELECT count() FROM otel_logs WHERE Body = 'x' LIMIT 1 SETTINGS optimize_read_in_order = 1, max_threads = 2, join_algorithm = 'hash' FORMAT JSON"))
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	sent := fs.bodies[0]
	if sent["sql"] != "SELECT count() FROM otel_logs WHERE Body = 'x' LIMIT 1" {
		t.Fatalf("sql %v", sent["sql"])
	}
	st := sent["settings"].(map[string]any)
	want := map[string]string{"use_skip_indexes": "1", "optimize_read_in_order": "1", "max_threads": "2", "join_algorithm": "hash"}
	if len(st) != len(want) {
		t.Fatalf("settings %v", st)
	}
	for k, v := range want {
		if st[k] != v {
			t.Errorf("%s = %v, want %s", k, st[k], v)
		}
	}
	if d := resp.Header.Get("X-Otel-Dropped-Settings"); d != "allow_experimental_analyzer,max_threads" {
		t.Fatalf("dropped %q", d)
	}
	if resp.Header.Get("X-Otel-Settings") != "use_skip_indexes=1" {
		t.Fatalf("X-Otel-Settings %q", resp.Header.Get("X-Otel-Settings"))
	}
	// a SETTINGS value that is an expression is refused before the service
	resp, _ = post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer t"}},
		strings.NewReader("SELECT 1 SETTINGS max_threads = 1 + 1 FORMAT JSON"))
	if resp.StatusCode == 200 || len(fs.bodies) != 1 {
		t.Fatalf("expression value: %d, %d calls", resp.StatusCode, len(fs.bodies))
	}
	// SETTINGS in a subquery stays for the service to refuse
	post(t, hs.URL+"/", http.Header{"Authorization": {"Bearer t"}},
		strings.NewReader("SELECT * FROM (SELECT 1 SETTINGS max_threads = 1) FORMAT JSON"))
	if s, _ := fs.bodies[1]["sql"].(string); !strings.Contains(s, "SETTINGS max_threads=1") && !strings.Contains(s, "SETTINGS max_threads = 1") {
		t.Fatalf("nested SETTINGS: %v", fs.bodies[1])
	}
}
