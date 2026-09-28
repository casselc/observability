package app

import "testing"

func TestExampleConfigLoads(t *testing.T) {
	t.Setenv("QS_CH_PASSWORD", "x")
	c, err := Load("../../queryd.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if c.Central.Password != "x" || len(c.Central.Tables) != 3 || !c.LakeEnabled || c.Limits.Groups["sre"].MaxConcurrent != 8 {
		t.Fatalf("%+v", c)
	}
	if _, err := Load(""); err == nil {
		t.Fatal("a config without an audit path must be refused")
	}
}
