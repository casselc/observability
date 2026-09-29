package basis

import (
	"testing"
	"time"
)

// FuzzDecode: whatever a client sends as a basis token, Decode never
// panics, and a token it accepts is the one canonical encoding of what it
// decodes to (re-encoding under the same key gives the same bytes): no
// second spelling of a basis passes the MAC, so a token cannot be altered
// and still be accepted.
func FuzzDecode(f *testing.F) {
	k := ring(f)
	for _, b := range []Basis{sample(), {IssuedNs: 1, Clusters: map[string]uint64{Fleet: 7}},
		{Clusters: map[string]uint64{"c": 0}, Signals: []string{"logs"}, MaxLatenessNs: int64(time.Hour)}} {
		tok, err := k.Encode(b)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(tok)
	}
	f.Add("b1.")
	f.Add("b1.e30.AAAA")
	f.Fuzz(func(t *testing.T, tok string) {
		b, err := k.Decode(tok)
		if err != nil {
			return
		}
		again := &Keyring{keys: k.keys, current: b.Kid}
		out, err := again.Encode(*b)
		if err != nil {
			t.Fatalf("accepted %q, which does not encode: %v", tok, err)
		}
		if out != tok {
			t.Fatalf("accepted a second spelling:\n got %q\ncanonical %q", tok, out)
		}
	})
}

// Every token has exactly one accepted spelling: the MAC's last character
// carries two unused bits, which a lenient decoder ignores.
func TestTokenTrailingBitsAreRefused(t *testing.T) {
	k := ring(t)
	tok, err := k.Encode(sample())
	if err != nil {
		t.Fatal(err)
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	accepted := 0
	for _, c := range alphabet {
		if _, err := k.Decode(tok[:len(tok)-1] + string(c)); err == nil {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("%d spellings of the MAC's last character accepted, want 1", accepted)
	}
}
