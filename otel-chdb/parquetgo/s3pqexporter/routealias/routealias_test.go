package routealias

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/collector/extension/extensionmiddleware"
)

func TestRewrite(t *testing.T) {
	pre := DefaultPrefixes
	for _, c := range []struct {
		in, to      string
		aliased, ok bool
	}{
		{"/v1/traces", "/v1/traces", false, true},
		{"/api/public/otel/v1/traces", "/v1/traces", true, true},
		{"/api/public/otel/v1/logs", "/v1/logs", true, true},
		{"/api/public/otel/v1/metrics", "/v1/metrics", true, true},
		{"/api/public/otel", "", true, false},
		{"/api/public/otel/", "", true, false},
		{"/api/public/otel/v1/traces/", "", true, false},
		{"/api/public/otel/../otel/v1/traces", "", true, false}, // not clean: refused, not normalized
		{"/api/public/otel//v1/traces", "", true, false},
		{"/api/public/otel/admin", "", true, false},
		{"/api/public/otelx/v1/traces", "/api/public/otelx/v1/traces", false, true},
		{"/api/public/ingestion", "/api/public/ingestion", false, true}, // the ingestion API: not an alias (D36)
	} {
		to, aliased, ok := Rewrite(pre, c.in)
		if to != c.to || aliased != c.aliased || ok != c.ok {
			t.Errorf("%s: (%q, %v, %v), want (%q, %v, %v)", c.in, to, aliased, ok, c.to, c.aliased, c.ok)
		}
	}
}

func TestMiddleware(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "/", "api", "/a/", "/a/../b"} {
		if (&Config{Prefixes: []string{bad}}).Validate() == nil {
			t.Errorf("prefix %q validated", bad)
		}
	}
	ext := &routeAlias{prefixes: cfg.Prefixes}
	var _ extensionmiddleware.HTTPServer = ext
	wrap, err := ext.GetHTTPHandler(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var seen string
	h, err := wrap(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.URL.Path + " " + r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		path string
		code int
		seen string
	}{
		{"/api/public/otel/v1/traces", 200, "/v1/traces Basic cGs6c2s="},
		{"/v1/logs", 200, "/v1/logs Basic cGs6c2s="},
		{"/api/public/otel/v1/other", 404, ""},
	} {
		seen = ""
		req := httptest.NewRequest(http.MethodPost, c.path, nil)
		req.Header.Set("Authorization", "Basic cGs6c2s=")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.code || seen != c.seen {
			t.Errorf("%s: %d %q, want %d %q", c.path, rec.Code, seen, c.code, c.seen)
		}
	}
}
