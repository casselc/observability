package itf

import (
	"encoding/json"
	"testing"
)

func TestParseQuintRoundTrip(t *testing.T) {
	for _, src := range []string{
		`{ bk: { batch: 3, epoch: 1 }, gk: { epoch: 1, gen: 2 }, payload: 7, phase: Encoded }`,
		`Set(1, 2, 3)`, `Set()`, `Map(1 -> "a", 2 -> "b")`, `(1, true)`, `[1, 2]`, `Some(-4)`, `Ok`, `"x\"y"`,
	} {
		v, err := ParseQuint(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := Parse([]byte(`{"vars":["v"],"states":[{"v":` + string(b) + `}]}`))
		if err != nil {
			t.Fatalf("%s: %v (%s)", src, err, b)
		}
		got := tr.States[0].Vars["v"]
		if got.Quint() != v.Quint() {
			t.Fatalf("%s: round trip %s -> %s", src, v.Quint(), got.Quint())
		}
		if again, err := ParseQuint(got.Quint()); err != nil || again.Quint() != v.Quint() {
			t.Fatalf("%s: reparse %v", src, err)
		}
	}
	if _, err := ParseQuint(`px(1, 2)`); err == nil {
		t.Fatal("an operator call must not parse as a literal")
	}
}
