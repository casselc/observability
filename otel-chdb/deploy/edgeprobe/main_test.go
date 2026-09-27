package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The Rust engine's text (labels in a different order per series, a
// trailing timestamp) and the Go collector's (per-signal queues).
const rustText = `# HELP storage_bytes_used_bytes Current bytes used by persistent storage.
# TYPE storage_bytes_used_bytes gauge
storage_bytes_used_bytes{otel_scope_name="processor.durable_buffer",otel_scope_core_id="0",otel_scope_node_id="buffer"} 250000000 1790523111792
storage_bytes_cap_bytes{otel_scope_node_id="buffer",otel_scope_core_id="0",otel_scope_name="processor.durable_buffer"} 268435456 1790523111792
storage_bytes_used_bytes{otel_scope_name="processor.durable_buffer",otel_scope_core_id="1",otel_scope_node_id="buffer"} 10 1790523111792
storage_bytes_cap_bytes{otel_scope_node_id="buffer",otel_scope_core_id="1",otel_scope_name="processor.durable_buffer"} 268435456 1790523111792
storage_bytes_used_bytes{otel_scope_name="something.else"} 99 1
`

const goText = `otelcol_exporter_queue_capacity{data_type="traces",exporter="s3pq"} 5000
otelcol_exporter_queue_size{data_type="traces",exporter="s3pq"} 12
otelcol_exporter_queue_capacity{data_type="logs",exporter="s3pq"} 5000
otelcol_exporter_queue_size{data_type="logs",exporter="s3pq"} 5000
otelcol_exporter_queue_size{data_type="logs",exporter="other"} 5000
`

func TestFill(t *testing.T) {
	f, err := fill(strings.NewReader(rustText), "storage_bytes_used_bytes", "storage_bytes_cap_bytes",
		[]string{"otel_scope_name=processor.durable_buffer"})
	if err != nil || f < 0.93 || f > 0.94 {
		t.Fatal(f, err)
	}
	f, err = fill(strings.NewReader(goText), "otelcol_exporter_queue_size", "otelcol_exporter_queue_capacity", []string{"exporter=s3pq"})
	if err != nil || f != 1 {
		t.Fatal(f, err)
	}
	if _, err := fill(strings.NewReader(goText), "nope", "otelcol_exporter_queue_capacity", nil); err == nil {
		t.Fatal("no pair must fail")
	}
}

func TestRun(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/unready" {
			http.Error(w, "process memory pressure at hard limit", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(rustText))
	}))
	defer srv.Close()
	dir := t.TempDir()
	for _, c := range []struct {
		args []string
		rc   int
		want string
	}{
		{[]string{"-dir", dir, "-min-free", "1Ki"}, 0, "ready: available="},
		{[]string{"-dir", dir, "-min-free", "1000000Gi"}, 1, "buffer volume full"},
		{[]string{"-dir", dir + "/missing"}, 1, "statfs"},
		{[]string{"-metrics", srv.URL, "-used", "storage_bytes_used_bytes", "-cap", "storage_bytes_cap_bytes",
			"-match", "otel_scope_name=processor.durable_buffer"}, 0, "fill=0.931"},
		{[]string{"-metrics", srv.URL, "-used", "storage_bytes_used_bytes", "-cap", "storage_bytes_cap_bytes",
			"-match", "otel_scope_name=processor.durable_buffer", "-max-fill", "0.9"}, 1, "at its cap"},
		{[]string{"-metrics", "http://127.0.0.1:1/x", "-used", "a", "-cap", "b"}, 1, "scrape"},
		{[]string{"-ready", srv.URL + "/ok", "-dir", dir}, 0, "ready"},
		{[]string{"-ready", srv.URL + "/unready", "-dir", dir}, 1, "503 Service Unavailable process memory pressure"},
		{[]string{}, 2, "usage"},
	} {
		var out bytes.Buffer
		if rc := run(c.args, &out); rc != c.rc || !strings.Contains(out.String(), c.want) {
			t.Errorf("%v: rc %d %q", c.args, rc, out.String())
		}
	}
}
