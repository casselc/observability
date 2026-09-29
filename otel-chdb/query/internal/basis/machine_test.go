package basis

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"testing"

	"pgregory.net/rapid"
)

// TestKeyringStateMachine: minting, key rotation and retirement, decoding
// earlier tokens and tampered ones, as a rapid state machine with a
// hand-rolled swarm (each case enables a random subset of the actions).
// Model: a token decodes, to exactly what was minted, iff the key it was
// minted under is still held; a changed byte never decodes.
func TestKeyringStateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keys := map[string][]byte{"k0": bytes.Repeat([]byte{'0'}, 32)}
		current, next := "k0", 1
		ring := func() *Keyring {
			k, err := NewKeyring(maps.Clone(keys), current)
			if err != nil {
				t.Fatal(err)
			}
			return k
		}
		k := ring()
		type minted struct {
			tok, kid string
			b        Basis
		}
		var toks []minted
		all := map[string]func(*rapid.T){
			"mint": func(t *rapid.T) {
				b := Basis{IssuedNs: rapid.Int64Range(0, 1<<40).Draw(t, "issued"),
					Clusters: map[string]uint64{rapid.SampledFrom([]string{"prod-a", "prod-b", "dev-1"}).Draw(t, "cluster"): rapid.Uint64Range(0, 1<<60).Draw(t, "ct")}}
				if rapid.Bool().Draw(t, "signals") {
					b.Signals = []string{"logs", "traces"}
				}
				tok, err := k.Encode(b)
				if err != nil {
					t.Fatal(err)
				}
				toks = append(toks, minted{tok, current, b})
			},
			"rotate": func(t *rapid.T) {
				kid := fmt.Sprintf("k%d", next)
				next++
				keys[kid] = bytes.Repeat([]byte{byte('a' + next%26)}, 32)
				current = kid
				k = ring()
			},
			"retire": func(t *rapid.T) {
				var old []string
				for kid := range keys {
					if kid != current {
						old = append(old, kid)
					}
				}
				if len(old) == 0 {
					t.Skip("nothing to retire")
				}
				slices.Sort(old)
				delete(keys, rapid.SampledFrom(old).Draw(t, "retired"))
				k = ring()
			},
			"decode": func(t *rapid.T) {
				if len(toks) == 0 {
					t.Skip("no token")
				}
				m := rapid.SampledFrom(toks).Draw(t, "token")
				b, err := k.Decode(m.tok)
				_, held := keys[m.kid]
				switch {
				case held && err != nil:
					t.Fatalf("a token under held key %s refused: %v", m.kid, err)
				case !held && err == nil:
					t.Fatalf("a token under retired key %s accepted", m.kid)
				case held && (b.Kid != m.kid || b.IssuedNs != m.b.IssuedNs || !maps.Equal(b.Clusters, m.b.Clusters) || !slices.Equal(b.Signals, m.b.Signals)):
					t.Fatalf("decoded %+v, minted %+v", b, m.b)
				}
			},
			"tamper": func(t *rapid.T) {
				if len(toks) == 0 {
					t.Skip("no token")
				}
				m := rapid.SampledFrom(toks).Draw(t, "token")
				i := rapid.IntRange(0, len(m.tok)-1).Draw(t, "at")
				c := rapid.ByteRange(0x21, 0x7e).Draw(t, "byte")
				if m.tok[i] == c {
					t.Skip("same byte")
				}
				bad := m.tok[:i] + string(c) + m.tok[i+1:]
				if _, err := k.Decode(bad); err == nil {
					t.Fatalf("a token with byte %d changed decoded: %q", i, bad)
				}
			},
		}
		names := slices.Sorted(maps.Keys(all))
		on := rapid.SliceOfNDistinct(rapid.SampledFrom(names), 1, len(names), rapid.ID[string]).Draw(t, "swarm")
		actions := map[string]func(*rapid.T){}
		for _, n := range on {
			actions[n] = all[n]
		}
		if len(toks) == 0 && !slices.Contains(on, "mint") {
			actions["mint"] = all["mint"] // decode and tamper need a token
		}
		t.Repeat(actions)
	})
}
