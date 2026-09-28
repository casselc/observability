package lakeidx

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func tokensOf(s string) []string {
	out := []string{}
	Tokens(nil, s, func(t []byte) { out = append(out, string(t)) })
	return out
}

// The vector file is written by lakeui/test/fold.test.js from the
// browser engine's own toLowerCase.
func TestTokensAgreeWithTheBrowserVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/fold_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Cases []struct {
			Text   string   `json:"text"`
			Tokens []string `json:"tokens"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Cases) < 20 {
		t.Fatalf("%d cases", len(v.Cases))
	}
	for _, c := range v.Cases {
		if got := tokensOf(c.Text); !reflect.DeepEqual(got, c.Tokens) {
			t.Errorf("%q: got %q, the browser %q", c.Text, got, c.Tokens)
		}
	}
}

func TestConstraints(t *testing.T) {
	cases := map[string][]Constraint{
		"":                 nil,
		" - ":              nil,
		"timeout":          {{"timeout", Infix}},
		"TimeOut":          {{"timeout", Infix}},
		"timeout ":         {{"timeout", Suffix}},
		" timeout":         {{"timeout", Prefix}},
		" timeout ":        {{"timeout", Full}},
		"acct-7731":        {{"acct", Suffix}, {"7731", Prefix}},
		"a timeout-acct b": {{"a", Suffix}, {"timeout", Full}, {"acct", Full}, {"b", Prefix}},
		"İd":               {{"i", Suffix}, {"d", Prefix}},
		"\u212aey":         {{"key", Infix}},
		"café":             {{"caf", Suffix}},
		"status=500 ":      {{"status", Suffix}, {"500", Full}},
	}
	for text, want := range cases {
		if got := Constraints(text); !reflect.DeepEqual(got, want) {
			t.Errorf("%q: got %v, want %v", text, got, want)
		}
	}
}

// jsLower is String.prototype.toLowerCase over the generator's alphabet.
func jsLower(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == 0x130:
			b.WriteString("i̇")
		case r == 0x212A:
			b.WriteByte('k')
		case r == 'É':
			b.WriteRune('é')
		case r == 'Σ':
			b.WriteRune('σ') // (final sigma is ς in JS: non-ASCII either way)
		case r < 0x80:
			b.WriteString(strings.ToLower(string(r)))
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

var alphabet = []string{"a", "b", "Z", "9", "_", " ", "-", ".", ":", "/", "İ", "\u212a", "É", "é", "😀", "Σ", "ß", "\u0307", "I", "K", "\xff", "\xe2\x84"}

// Property: a body that contains the text (the page's test) satisfies every
// constraint of the text with one of its tokens: the tokenizer layer has no
// false negatives.
func TestConstraintsHoldOnEveryMatchingBody(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		parts := rapid.SliceOfN(rapid.SampledFrom(alphabet), 0, 40).Draw(t, "body")
		body := strings.Join(parts, "")
		i := rapid.IntRange(0, len(parts)).Draw(t, "i")
		j := rapid.IntRange(i, len(parts)).Draw(t, "j")
		text := strings.Join(parts[i:j], "")
		if rapid.Bool().Draw(t, "upper") {
			text = strings.ToUpper(text) // ASCII and more: the search is case-insensitive
		}
		if !strings.Contains(jsLower(body), jsLower(text)) {
			t.Skip("not a match in the page either")
		}
		toks := tokensOf(body)
		for _, c := range Constraints(text) {
			ok := false
			for _, tok := range toks {
				ok = ok || c.Holds(tok)
			}
			if !ok {
				t.Fatalf("body %q text %q: no token for %v (tokens %q)", body, text, c, toks)
			}
		}
	})
}
