package config

import (
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
)

func TestExamplesLoad(t *testing.T) {
	c, err := Load("../../alertd.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	rules, err := rule.Load("../../" + c.RulesFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rules {
		if _, ok := c.Identities[r.Identity]; !ok {
			t.Errorf("rule %s: identity %s not in the example config", r.Name, r.Identity)
		}
	}
	e := c.Engine()
	if e.CannotEvaluateAfter != 5*time.Minute || e.Refresh != time.Minute || e.HoldResolved != 45*time.Second {
		t.Fatalf("%+v", e)
	}
}

func TestLoadRequires(t *testing.T) {
	t.Setenv("ALR_QUERY_URL", "")
	if _, err := Load(""); err == nil {
		t.Fatal("loaded with no query url")
	}
	t.Setenv("ALR_QUERY_URL", "http://q")
	t.Setenv("ALR_RULES", "r.yaml")
	t.Setenv("ALR_SINK_URL", "http://am")
	if _, err := Load(""); err == nil {
		t.Fatal("loaded with no state bucket")
	}
	t.Setenv("ALR_S3_BUCKET", "b")
	if _, err := Load(""); err != nil {
		t.Fatal(err)
	}
}
