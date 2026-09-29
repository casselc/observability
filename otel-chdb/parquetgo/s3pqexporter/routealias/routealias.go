// Package routealias is the `routealias` extension: an HTTP server
// middleware for the OTLP receiver that serves OTLP/HTTP under other path
// prefixes as well, by stripping the prefix before the receiver's router
// sees the request. Its purpose is Langfuse's OTLP endpoint (DECISIONS.md
// D36, owner item 1): Langfuse's SDKs and its opencode, Codex and Claude
// Code integrations post to `{LANGFUSE_BASE_URL}/api/public/otel/v1/traces`
// (research/langfuse.md §6.1), which with this extension reaches the
// receiver's `/v1/traces`. Nothing else about the request changes: its
// Basic-auth project keys are not read (the tenant is the edge's, R-L1),
// and the Rust edge's alias is the same rule (otap-rs patch 0007).
//
//	extensions:
//	  routealias:
//	    prefixes: [/api/public/otel]   # the default
//	receivers:
//	  otlp:
//	    protocols:
//	      http:
//	        middlewares: [{id: routealias}]
//
// A path is rewritten only when it is exactly a prefix followed by
// `/v1/traces`, `/v1/logs` or `/v1/metrics` (after cleaning: no `..`, no
// doubled slashes); anything else under a prefix is 404 here, so the alias
// never widens what the receiver serves.
package routealias

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionmiddleware"
)

// Type is the extension's type.
var Type = component.MustNewType("routealias")

// Config lists the prefixes served as aliases of the receiver's root.
type Config struct {
	Prefixes []string `mapstructure:"prefixes"`
}

// DefaultPrefixes: Langfuse's OTLP path.
var DefaultPrefixes = []string{"/api/public/otel"}

// Validate: every prefix absolute, clean, not the root, no trailing slash.
func (c *Config) Validate() error {
	var errs []error
	for _, p := range c.Prefixes {
		if p == "" || p == "/" || !strings.HasPrefix(p, "/") || path.Clean(p) != p {
			errs = append(errs, fmt.Errorf("routealias prefix %q: want an absolute, clean path other than /", p))
		}
	}
	return errors.Join(errs...)
}

// Signals are the OTLP/HTTP paths an alias may reach.
var Signals = []string{"/v1/traces", "/v1/logs", "/v1/metrics"}

// Rewrite is the alias rule: the receiver's path for p, whether p is under
// a prefix, and whether it may be served (false: 404).
func Rewrite(prefixes []string, p string) (to string, aliased, ok bool) {
	c := path.Clean("/" + p)
	for _, pre := range prefixes {
		if c == pre || strings.HasPrefix(c, pre+"/") {
			rest := strings.TrimPrefix(c, pre)
			for _, s := range Signals {
				if rest == s && p == c {
					return s, true, true
				}
			}
			return "", true, false
		}
	}
	return p, false, true
}

type routeAlias struct {
	component.StartFunc
	component.ShutdownFunc
	prefixes []string
}

var _ extensionmiddleware.HTTPServer = (*routeAlias)(nil)

// GetHTTPHandler wraps the receiver's handler.
func (r *routeAlias) GetHTTPHandler(context.Context) (extensionmiddleware.WrapHTTPHandlerFunc, error) {
	return func(_ context.Context, next http.Handler) (http.Handler, error) {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			to, aliased, ok := Rewrite(r.prefixes, req.URL.Path)
			if !aliased {
				next.ServeHTTP(w, req)
				return
			}
			if !ok {
				http.NotFound(w, req)
				return
			}
			r2 := req.Clone(req.Context())
			r2.URL.Path, r2.URL.RawPath = to, ""
			r2.RequestURI = to
			if req.URL.RawQuery != "" {
				r2.RequestURI += "?" + req.URL.RawQuery
			}
			next.ServeHTTP(w, r2)
		}), nil
	}, nil
}

// NewFactory is the extension's factory.
func NewFactory() extension.Factory {
	return extension.NewFactory(Type,
		func() component.Config { return &Config{Prefixes: append([]string(nil), DefaultPrefixes...)} },
		func(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
			c := cfg.(*Config)
			return &routeAlias{prefixes: append([]string(nil), c.Prefixes...)}, nil
		},
		component.StabilityLevelDevelopment)
}
