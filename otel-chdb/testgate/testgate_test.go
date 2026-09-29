package testgate

import (
	"fmt"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

func TestRequiredIn(t *testing.T) {
	for _, c := range []struct {
		spec, svc string
		want      bool
	}{
		{"", "clickhouse", false},
		{"0", "s3", false},
		{"1", "clickhouse-replicated", true},
		{"all", "s3", true},
		{" * ", "kms", true},
		{"clickhouse,s3", "s3", true},
		{"clickhouse, s3", "s3", true},
		{"ClickHouse s3", "clickhouse", true},
		{"clickhouse,s3", "kms", false},
		{"s3x", "s3", false},
		{",,", "s3", false},
	} {
		if got := RequiredIn(c.spec, c.svc); got != c.want {
			t.Errorf("RequiredIn(%q, %q) = %v, want %v", c.spec, c.svc, got, c.want)
		}
	}
}

// rec records the gate's verdict; its Fatalf and Skipf stop like the real ones.
type rec struct{ fatal, skip string }

type stop struct{}

func (r *rec) Helper()                   {}
func (r *rec) Fatalf(f string, a ...any) { r.fatal = fmt.Sprintf(f, a...); panic(stop{}) }
func (r *rec) Skipf(f string, a ...any)  { r.skip = fmt.Sprintf(f, a...); panic(stop{}) }
func run(spec, svc, why string) (r rec) {
	defer func() {
		if p := recover(); p != (stop{}) {
			panic(fmt.Sprintf("the gate returned instead of stopping the test (%v)", p))
		}
	}()
	gate(&r, spec, svc, why)
	return
}

func TestGateFailsExactlyWhenRequired(t *testing.T) {
	tracetag.Covers(t, "G", "CAST-43", "CAST-46")
	r := run("clickhouse,s3", "s3", "no S3 at http://x/b")
	if r.skip != "" || !strings.HasPrefix(r.fatal, "no S3 at http://x/b: service s3 is required here (OSCOPE_REQUIRE_SERVICES=clickhouse,s3)") {
		t.Fatalf("required: want a failure, got %+v", r)
	}
	for _, spec := range []string{"", "clickhouse"} {
		r := run(spec, "s3", "no S3")
		if r.fatal != "" || !strings.HasPrefix(r.skip, "no S3: skipped (service s3;") {
			t.Fatalf("spec %q: want a skip, got %+v", spec, r)
		}
	}
}

// Skip itself, on the real testing.T: a subtest that skips (s3 not required).
func TestSkipSkips(t *testing.T) {
	t.Setenv(Env, "clickhouse")
	var ran bool
	t.Run("gated", func(t *testing.T) {
		Skip(t, "s3", "no S3")
		ran = true
	})
	if ran {
		t.Fatal("Skip returned")
	}
}
