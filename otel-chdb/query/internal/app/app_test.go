package app

import (
	"testing"

	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
)

func TestExampleConfigLoads(t *testing.T) {
	t.Setenv("QS_CH_PASSWORD", "x")
	c, err := Load("../../queryd.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if c.Central.Password != "x" || len(c.Central.Tables) != 12 || !c.LakeEnabled || c.Limits.Groups["sre"].MaxConcurrent != 8 {
		t.Fatalf("%+v", c)
	}
	// the example's policy builds (the metadata tables included)
	p, err := sqlscope.NewPolicy(c.Central.Database, c.Central.Tables, c.Central.MaxSQLBytes)
	if err != nil {
		t.Fatal(err)
	}
	// its dictionaries (D33) and performance settings are accepted
	if err := p.SetDictionaries(c.Central.Dictionaries); err != nil || len(p.Dictionaries) != 6 {
		t.Fatalf("dictionaries: %v %d", err, len(p.Dictionaries))
	}
	if len(c.Central.PerformanceSettings) < 10 || c.Sample.MaxRows != 10_000_000 || c.Sample.DefaultRows != 3_000_000 {
		t.Fatalf("performance settings %d, sample %+v", len(c.Central.PerformanceSettings), c.Sample)
	}
	if _, err := Load(""); err == nil {
		t.Fatal("a config without an audit path must be refused")
	}
	if *c.Watermark.MaxLatenessS != 60 || c.Watermark.CountLate == nil || !*c.Watermark.CountLate {
		t.Fatalf("watermark %+v", c.Watermark)
	}
	// max_lateness is policy: the environment overrides it, 0 included
	t.Setenv("QS_MAX_LATENESS_S", "0")
	t.Setenv("QS_COUNT_LATE", "false")
	c, err = Load("../../queryd.example.json")
	if err != nil || *c.Watermark.MaxLatenessS != 0 || *c.Watermark.CountLate {
		t.Fatalf("env: %v %+v", err, c.Watermark)
	}
	for _, bad := range []string{"-1", "x", "1e9"} {
		t.Setenv("QS_MAX_LATENESS_S", bad)
		if _, err := Load("../../queryd.example.json"); err == nil {
			t.Errorf("max_lateness %s accepted", bad)
		}
	}
}
