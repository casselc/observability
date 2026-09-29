// Package testgate is the one gate for Go tests that need a service
// (ClickHouse, S3, ...): skip where the service is absent, FAIL where it is
// supposed to be present.
//
// A test that skips without its service keeps the suite runnable on a
// laptop, but a skip reads as a pass: two tests skipped silently for lack of
// a bucket and were broken underneath (STPA.md CAST row 43). So a job that
// provides services says so, OSCOPE_REQUIRE_SERVICES=clickhouse,s3
// (ci/services.sh start writes it to $GITHUB_ENV), and there the gate fails
// the test instead of skipping it. The Rust twin is otap-rs/src/testgate.rs.
//
// OSCOPE_REQUIRE_SERVICES: unset, empty or "0": nothing required; "1", "all"
// or "*": every service; otherwise service names separated by commas or
// spaces (case-insensitive). Names in use: clickhouse, s3, kms.
//
//	if _, err := c.Query("SELECT 1"); err != nil {
//		testgate.Skip(t, "clickhouse", "no ClickHouse at %s: %v", url, err)
//	}
//
// Opt-in gates (a slow or measuring test behind its own variable, libchdb, a
// binaries directory) are not service gates and keep t.Skip.
//
// This module has no dependencies. A module that imports it in its tests
// requires it with a filesystem replace, and so does every module (and ocb
// builder config) that builds one of those: replace directives are not
// inherited.
package testgate

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// Env names the services a run requires.
const Env = "OSCOPE_REQUIRE_SERVICES"

// RequiredIn reports whether spec (the variable's value) requires service.
func RequiredIn(spec, service string) bool {
	switch spec = strings.TrimSpace(spec); spec {
	case "", "0":
		return false
	case "1", "all", "*":
		return true
	}
	for _, s := range strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' }) {
		if strings.EqualFold(s, service) {
			return true
		}
	}
	return false
}

// Required reports whether this run requires service (OSCOPE_REQUIRE_SERVICES).
func Required(service string) bool { return RequiredIn(os.Getenv(Env), service) }

// Skip skips tb for lack of service, or fails it (FailNow) if this run
// requires the service. Either way it does not return.
func Skip(tb testing.TB, service, format string, args ...any) {
	tb.Helper()
	gate(tb, os.Getenv(Env), service, fmt.Sprintf(format, args...))
}

// The subset of testing.TB the gate uses, so its test can observe it.
type skipFataler interface {
	Helper()
	Fatalf(format string, args ...any)
	Skipf(format string, args ...any)
}

func gate(tb skipFataler, spec, service, why string) {
	tb.Helper()
	if RequiredIn(spec, service) {
		tb.Fatalf("%s: service %s is required here (%s=%s), so this test fails instead of skipping", why, service, Env, spec)
	}
	tb.Skipf("%s: skipped (service %s; %s does not require it)", why, service, Env)
}
