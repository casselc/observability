package lake

import (
	"testing"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// A URL lifetime at or below the re-plan margin made every plan stale as it
// was issued (replan_after = signed_at + ttl − margin ≤ signed_at): a client
// following X8 re-plans forever and never reads. The margin is cut to half
// the lifetime.
func TestReplanMarginBelowTTL(t *testing.T) {
	tracetag.Covers(t, "P", "CAST-25", "R-S1", "R-S2")
	for _, c := range []struct{ ttl, margin, want int }{
		{60, 0, 30},    // the default margin (60 s) with the shortest TTL
		{60, 60, 30},   // equal
		{60, 90, 30},   // larger
		{60, 20, 20},   // fine as given
		{300, 0, 60},   // the defaults
		{30, 0, 30},    // TTL clamped to 60 first
		{900, 600, 450},
	} {
		cfg := Config{URLTTLS: c.ttl, ReplanMarginS: c.margin}
		cfg.defaults()
		if cfg.ReplanMarginS != c.want || cfg.ReplanMarginS >= cfg.URLTTLS {
			t.Errorf("ttl %d margin %d: got margin %d (ttl %d), want %d", c.ttl, c.margin, cfg.ReplanMarginS, cfg.URLTTLS, c.want)
		}
	}
}
