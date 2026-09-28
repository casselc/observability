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
	if c.Central.Password != "x" || len(c.Central.Tables) != 9 || !c.LakeEnabled || c.Limits.Groups["sre"].MaxConcurrent != 8 {
		t.Fatalf("%+v", c)
	}
	// the example's policy builds (the metadata tables included)
	if _, err := sqlscope.NewPolicy(c.Central.Database, c.Central.Tables, c.Central.MaxSQLBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(""); err == nil {
		t.Fatal("a config without an audit path must be refused")
	}
}
