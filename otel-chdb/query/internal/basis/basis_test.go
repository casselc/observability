package basis

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func ring(t testing.TB) *Keyring {
	k, err := NewKeyring(map[string][]byte{"k1": []byte(strings.Repeat("a", 32)), "k0": []byte(strings.Repeat("b", 32))}, "k1")
	if err != nil {
		t.Fatal(err)
	}
	return k
}

var t0 = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func sample() Basis {
	return Basis{IssuedNs: t0.UnixNano(), Clusters: map[string]uint64{"prod-a": uint64(t0.Add(-30 * time.Second).UnixNano()),
		"prod-b": uint64(t0.Add(-50 * time.Second).UnixNano())}, Signals: []string{"logs", "traces"}, MaxLatenessNs: int64(time.Minute)}
}

func TestRoundTripAndView(t *testing.T) {
	k := ring(t)
	tok, err := k.Encode(sample())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok, "b1.") {
		t.Fatalf("token %q", tok)
	}
	again, _ := k.Encode(sample())
	if again != tok {
		t.Fatal("equal bases must encode to equal tokens")
	}
	b, err := k.Decode(tok)
	if err != nil {
		t.Fatal(err)
	}
	if b.Kid != "k1" || b.Version != 1 || b.IsFleet() || b.MinFor(nil) != sample().Clusters["prod-b"] || b.MinFor([]string{"prod-a"}) != sample().Clusters["prod-a"] {
		t.Fatalf("%+v", b)
	}
	if !b.CoversSignals([]string{"logs"}) || b.CoversSignals([]string{"metrics_gauge"}) || b.CoversSignals(nil) {
		t.Fatal("signals")
	}
	v := b.View()
	if len(v.Clusters) != 2 || v.Clusters[0].Cluster != "prod-a" || v.MaxLatenessS != 60 || v.Signals[1] != "traces" {
		t.Fatalf("%+v", v)
	}
	// every signal: "*" on the wire, nil decoded, covers anything
	all := sample()
	all.Signals = nil
	tok, _ = k.Encode(all)
	b, err = k.Decode(tok)
	if err != nil || b.Signals != nil || !b.CoversSignals(nil) || b.View().Signals[0] != "*" {
		t.Fatalf("%v %+v", err, b)
	}
}

func TestDecodeRefusals(t *testing.T) {
	k := ring(t)
	tok, _ := k.Encode(sample())
	payload := strings.Split(tok, ".")[1]
	other, _ := NewKeyring(map[string][]byte{"k1": []byte(strings.Repeat("z", 32))}, "k1")
	forged, _ := other.Encode(sample())
	cases := map[string]string{
		"empty":           "",
		"no prefix":       tok[3:],
		"v2":              "b2" + tok[2:],
		"no mac":          "b1." + payload,
		"extra part":      tok + ".x",
		"bad b64":         "b1.@@@." + strings.Split(tok, ".")[2],
		"flipped mac":     tok[:len(tok)-2] + flip(tok[len(tok)-2:]),
		"other key":       forged,
		"too long":        "b1." + strings.Repeat("A", MaxTokenBytes),
		"payload swapped": "b1." + base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"k":"k1","t":0,"c":{"prod-a":1},"s":["*"],"l":0}`)) + "." + strings.Split(tok, ".")[2],
	}
	for name, c := range cases {
		if _, err := k.Decode(c); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// rotation: a token under k0 still verifies while k0 is in the ring
	old, _ := NewKeyring(map[string][]byte{"k0": []byte(strings.Repeat("b", 32))}, "k0")
	tok0, _ := old.Encode(sample())
	if _, err := k.Decode(tok0); err != nil {
		t.Fatal(err)
	}
	gone, _ := NewKeyring(map[string][]byte{"k1": []byte(strings.Repeat("a", 32))}, "k1")
	if _, err := gone.Decode(tok0); !errors.Is(err, ErrInvalid) {
		t.Fatal("a rotated-out key must be refused")
	}
	// minting refuses what could never decode
	for _, b := range []Basis{
		{Clusters: map[string]uint64{}},
		{Clusters: map[string]uint64{"*": 1, "a": 2}},
		{Clusters: map[string]uint64{"Bad!": 1}},
		{Clusters: map[string]uint64{"a": 1}, Signals: []string{}},
		{Clusters: map[string]uint64{"a": 1}, Signals: []string{"traces", "logs"}},
		{Clusters: map[string]uint64{"a": 1 << 63}},
	} {
		if _, err := k.Encode(b); err == nil {
			t.Errorf("minted %+v", b)
		}
	}
	if _, err := ParseKeys("k:"+base64.StdEncoding.EncodeToString([]byte("short")), ""); err == nil {
		t.Fatal("short key accepted")
	}
	if kr, err := ParseKeys(" a:"+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))+",b:"+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("y", 40))), "b"); err != nil || kr.current != "b" {
		t.Fatal(err)
	}
}

func flip(s string) string {
	b := []byte(s)
	if b[0] == 'A' {
		b[0] = 'B'
	} else {
		b[0] = 'A'
	}
	return string(b)
}

// Any change to a token is refused, never read as another basis.
func TestTamperProperty(t *testing.T) {
	k := ring(t)
	rapid.Check(t, func(t *rapid.T) {
		b := genBasis(t)
		tok, err := k.Encode(b)
		if err != nil {
			t.Fatal(err)
		}
		i := rapid.IntRange(0, len(tok)-1).Draw(t, "i")
		c := rapid.Byte().Draw(t, "c")
		if tok[i] == c {
			return
		}
		mut := tok[:i] + string([]byte{c}) + tok[i+1:]
		got, err := k.Decode(mut)
		if err == nil {
			// base64url's last character carries spare bits: a flip there
			// can decode to the same bytes, and then to the same basis
			again, _ := k.Encode(Basis{IssuedNs: got.IssuedNs, Clusters: got.Clusters, Signals: got.Signals, MaxLatenessNs: got.MaxLatenessNs})
			if again != tok {
				t.Fatalf("a changed token decoded to another basis: %q", mut)
			}
		}
	})
}

func genBasis(t *rapid.T) Basis {
	b := Basis{IssuedNs: rapid.Int64Range(0, 1<<62).Draw(t, "iat"), Clusters: map[string]uint64{},
		MaxLatenessNs: rapid.Int64Range(0, int64(time.Hour)).Draw(t, "ml")}
	if rapid.Bool().Draw(t, "fleet") {
		b.Clusters[Fleet] = rapid.Uint64Range(0, 1<<62).Draw(t, "c")
	} else {
		for _, c := range rapid.SliceOfNDistinct(rapid.SampledFrom([]string{"a", "b", "prod-eu-1", "c.d"}), 1, 4, rapid.ID[string]).Draw(t, "cl") {
			b.Clusters[c] = rapid.Uint64Range(0, 1<<62).Draw(t, "c-"+c)
		}
	}
	if rapid.Bool().Draw(t, "sig") {
		b.Signals = []string{"logs"}
		if rapid.Bool().Draw(t, "sig2") {
			b.Signals = append(b.Signals, "traces")
		}
	}
	return b
}

func may(cs ...string) func(string) bool {
	return func(c string) bool {
		for _, x := range cs {
			if x == c {
				return true
			}
		}
		return false
	}
}

func TestResolve(t *testing.T) {
	b := sample()
	// every named cluster must be the caller's: never intersected
	if _, rf := Resolve(&b, may("prod-a"), false, nil); rf == nil || rf.Reason != ReasonNotInScope || rf.Status != http.StatusForbidden {
		t.Fatalf("%v", rf)
	}
	if _, rf := Resolve(&b, may("prod-a"), false, []string{"prod-a"}); rf == nil || rf.Reason != ReasonNotInScope {
		t.Fatalf("narrowing does not make another cluster's basis usable: %v", rf)
	}
	cs, rf := Resolve(&b, may("prod-a", "prod-b"), false, nil)
	if rf != nil || len(cs) != 2 {
		t.Fatal(cs, rf)
	}
	cs, rf = Resolve(&b, may("prod-a", "prod-b", "prod-c"), false, []string{"prod-b"})
	if rf != nil || len(cs) != 1 || cs[0] != "prod-b" {
		t.Fatal(cs, rf)
	}
	if _, rf := Resolve(&b, may("prod-a", "prod-b", "prod-c"), false, []string{"prod-c"}); rf == nil || rf.Reason != ReasonScope {
		t.Fatalf("an uncovered cluster: %v", rf)
	}
	fleet := Basis{Clusters: map[string]uint64{Fleet: 5}}
	if _, rf := Resolve(&fleet, may("prod-a"), false, nil); rf == nil || rf.Reason != ReasonNotInScope {
		t.Fatalf("a fleet basis for a restricted caller: %v", rf)
	}
	if cs, rf := Resolve(&fleet, may(), true, nil); rf != nil || cs != nil {
		t.Fatal(cs, rf)
	}
	if cs, rf := Resolve(&fleet, may(), true, []string{"x"}); rf != nil || cs[0] != "x" {
		t.Fatal(cs, rf)
	}
	if rf := CheckSignals(&b, []string{"metrics_gauge"}); rf == nil || rf.Reason != ReasonScope {
		t.Fatal(rf)
	}
	if rf := CheckSignals(&b, nil); rf == nil {
		t.Fatal("a per-signal basis does not cover every signal")
	}
}

func TestCheckCurrentExpiryDelta(t *testing.T) {
	b := sample()
	cur := func(v map[string]uint64) func(string) (uint64, bool) {
		return func(c string) (uint64, bool) { x, ok := v[c]; return x, ok }
	}
	names := []string{"prod-a", "prod-b"}
	if rf := CheckCurrent(&b, names, cur(b.Clusters)); rf != nil {
		t.Fatal(rf)
	}
	lower := map[string]uint64{"prod-a": b.Clusters["prod-a"] - 1, "prod-b": b.Clusters["prod-b"]}
	if rf := CheckCurrent(&b, names, cur(lower)); rf == nil || rf.Reason != ReasonAhead || rf.Status != http.StatusConflict {
		t.Fatal(rf)
	}
	// only the clusters the request reads are checked
	if rf := CheckCurrent(&b, []string{"prod-b"}, cur(lower)); rf != nil {
		t.Fatal(rf)
	}
	if rf := CheckCurrent(&b, names, cur(map[string]uint64{"prod-a": 1 << 62})); rf == nil || rf.Reason != ReasonUnverifiable {
		t.Fatal(rf)
	}
	now := t0
	if rf := CheckExpiry(&b, names, now, time.Hour, nil, 0); rf != nil {
		t.Fatal(rf)
	}
	if rf := CheckExpiry(&b, names, now.Add(time.Hour), time.Hour, nil, 0); rf == nil || rf.Reason != ReasonExpired || rf.Status != http.StatusGone {
		t.Fatal(rf)
	}
	from := t0.Add(-50 * time.Minute).UnixNano()
	if rf := CheckExpiry(&b, names, now, time.Hour, &from, 5*time.Minute); rf != nil {
		t.Fatal(rf)
	}
	if rf := CheckExpiry(&b, names, now, time.Hour, &from, 15*time.Minute); rf == nil || rf.Reason != ReasonExpired {
		t.Fatal("a window whose rows may predate retention", rf)
	}
	later := sample()
	later.Clusters = map[string]uint64{"prod-a": b.Clusters["prod-a"] + 10, "prod-b": b.Clusters["prod-b"]}
	if rf := CheckDelta(&b, &later, names); rf != nil {
		t.Fatal(rf)
	}
	if rf := CheckDelta(&later, &b, names); rf == nil || rf.Reason != ReasonRegressed {
		t.Fatal(rf)
	}
	if rf := CheckDelta(&later, &b, []string{"prod-b"}); rf != nil {
		t.Fatal(rf)
	}
	fleet := Basis{Clusters: map[string]uint64{Fleet: 5}}
	if rf := CheckDelta(&fleet, &b, names); rf == nil || rf.Reason != ReasonScope {
		t.Fatal(rf)
	}
	logs := sample()
	logs.Signals = []string{"logs"}
	if rf := CheckDelta(&logs, &b, names); rf == nil || rf.Reason != ReasonScope {
		t.Fatal(rf)
	}
}
