package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"testing"
)

// BenchmarkProxy: one request through the proxy to a fake upstream that
// answers at once; what the hop itself costs (client and fake upstream
// included).
func BenchmarkProxy(b *testing.B) {
	for _, tc := range []struct{ name, q string }{
		{"passthrough", "SELECT 1"},
		{"rewritten", "SELECT count() FROM rw_c.otel_logs WHERE ResourceAttributes['k8s.namespace.name'] = 'payments-core' AND indexHint(mapContains(ResourceAttributes, 'k8s.namespace.name'))"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			defer debug.SetGCPercent(debug.SetGCPercent(800))
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				io.WriteString(w, "1\n")
			}))
			defer up.Close()
			cfg := testConfig("exact")
			cfg.Upstream = up.URL
			s, _ := newServer(cfg)
			ps := httptest.NewServer(s)
			defer ps.Close()
			cl := &http.Client{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := cl.Post(ps.URL+"/", "text/plain", strings.NewReader(tc.q))
				if err != nil {
					b.Fatal(err)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		})
	}
}
