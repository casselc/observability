// Package lakeidx is the lake's index format (FORMAT.md §7): per cluster,
// signal and commit hour, create-only segments mapping trace ids and log
// body terms to (object, row group). The indexer (indexer.go) writes them;
// the query service's planner (resolve.go) reads them to narrow a plan.
//
// An index may only ever narrow a plan to a superset of the true matches:
// every object a segment does not cover, and every segment that cannot be
// read or verified, is planned and scanned as if there were no index.
package lakeidx

// The tokenizer. The lake UI's text search is a case-insensitive SUBSTRING
// test in JavaScript: jsLower(body).includes(jsLower(text)) (lakeui
// src/queries.js). The index must answer a superset of that, so it folds a
// string the way that test sees it:
//
//  1. String.prototype.toLowerCase (full Unicode lowercasing, locale-free);
//  2. every resulting character that is an ASCII letter, digit or '_' is a
//     word character; every other character (any non-ASCII one included)
//     is a separator.
//
// Folding is character-wise, so a body that contains the text contains the
// folded text in its folded form, and the folded text's runs of word
// characters constrain the body's runs (tokens): see Constraints.
//
// Only two non-ASCII code points lowercase to something with an ASCII
// character in it (checked over every code point in Node 22 / V8, and
// asserted by lakeui's test/fold.test.js so a browser whose Unicode tables
// change is caught): U+0130 (İ → "i" + U+0307) and U+212A (Kelvin → "k").
// Everything else non-ASCII lowercases to non-ASCII characters only, so
// Go needs no Unicode tables to agree with the browser. Invalid UTF-8 is
// U+FFFD on both sides (Go's range loop, the browser's TextDecoder): a
// separator. No normalization on either side.

// Sep is the byte a separator folds to.
const Sep = ' '

func wordByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_'
}

// Fold appends s's folded form to dst: word characters lowercased,
// every separator as one Sep.
func Fold(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x80 {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			if wordByte(c) {
				dst = append(dst, c)
			} else {
				dst = append(dst, Sep)
			}
			continue
		}
		// a multi-byte sequence: decode just enough to spot the two
		// code points that lowercase to ASCII; anything else is a separator
		r, n := decodeRune(s[i:])
		switch r {
		case 0x130: // İ → "i̇": 'i' then COMBINING DOT ABOVE (a separator)
			dst = append(dst, 'i', Sep)
		case 0x212A: // KELVIN SIGN → 'k'
			dst = append(dst, 'k')
		default:
			dst = append(dst, Sep)
		}
		i += n - 1
	}
	return dst
}

// decodeRune decodes one UTF-8 sequence; an invalid byte is (U+FFFD, 1),
// as Go's range loop does.
func decodeRune(s string) (rune, int) {
	c := s[0]
	switch {
	case c&0xE0 == 0xC0 && len(s) >= 2 && s[1]&0xC0 == 0x80:
		r := rune(c&0x1F)<<6 | rune(s[1]&0x3F)
		if r >= 0x80 {
			return r, 2
		}
	case c&0xF0 == 0xE0 && len(s) >= 3 && s[1]&0xC0 == 0x80 && s[2]&0xC0 == 0x80:
		r := rune(c&0x0F)<<12 | rune(s[1]&0x3F)<<6 | rune(s[2]&0x3F)
		if r >= 0x800 && (r < 0xD800 || r > 0xDFFF) {
			return r, 3
		}
	case c&0xF8 == 0xF0 && len(s) >= 4 && s[1]&0xC0 == 0x80 && s[2]&0xC0 == 0x80 && s[3]&0xC0 == 0x80:
		r := rune(c&0x07)<<18 | rune(s[1]&0x3F)<<12 | rune(s[2]&0x3F)<<6 | rune(s[3]&0x3F)
		if r >= 0x10000 && r <= 0x10FFFF {
			return r, 4
		}
	}
	return 0xFFFD, 1
}

// Tokens calls f with every maximal run of word characters of s's folded
// form (buf is scratch space, returned for reuse).
func Tokens(buf []byte, s string, f func(tok []byte)) []byte {
	buf = Fold(buf[:0], s)
	start := -1
	for i := 0; i <= len(buf); i++ {
		if i < len(buf) && buf[i] != Sep {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			f(buf[start:i])
			start = -1
		}
	}
	return buf
}

// Match says how a dictionary term must relate to a constraint's text.
type Match uint8

const (
	// Full: the term equals the text (a run bounded by separators on both
	// sides inside the query).
	Full Match = iota
	// Prefix: the term starts with the text (the query's last run, when
	// the query does not end with a separator).
	Prefix
	// Suffix: the term ends with the text (the query's first run, when the
	// query does not start with a separator).
	Suffix
	// Infix: the term contains the text (a query that is one run with no
	// separator at all).
	Infix
)

func (m Match) String() string {
	return [...]string{"full", "prefix", "suffix", "infix"}[m]
}

// Constraint is one thing every matching body must hold: a token that
// relates to Text as Match says.
type Constraint struct {
	Text  string
	Match Match
}

// Holds says whether token tok satisfies c.
func (c Constraint) Holds(tok string) bool {
	switch c.Match {
	case Full:
		return tok == c.Text
	case Prefix:
		return len(tok) >= len(c.Text) && tok[:len(c.Text)] == c.Text
	case Suffix:
		return len(tok) >= len(c.Text) && tok[len(tok)-len(c.Text):] == c.Text
	default:
		return containsStr(tok, c.Text)
	}
}

func containsStr(s, sub string) bool {
	n := len(sub)
	for i := 0; i+n <= len(s); i++ {
		if s[i:i+n] == sub {
			return true
		}
	}
	return false
}

// Constraints are what a body containing text (case-insensitively, the
// UI's test) must hold, token-wise. None means the text constrains no token
// (empty, or only separators): the index cannot narrow such a search.
//
// Why each holds: fold(body) contains fold(text) = [s] r0 s r1 … s rn [s].
// A middle run ri is flanked by separators in the body too, so it is a whole
// token; r0 is preceded in the body by a word character or a separator, so
// it is a suffix of a token (a whole token if text starts with a
// separator); rn likewise a prefix; a single run without separators may sit
// anywhere inside a token.
func Constraints(text string) []Constraint {
	f := Fold(nil, text)
	type run struct{ a, b int }
	var runs []run
	start := -1
	for i := 0; i <= len(f); i++ {
		if i < len(f) && f[i] != Sep {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			runs = append(runs, run{start, i})
			start = -1
		}
	}
	if len(runs) == 0 {
		return nil
	}
	lead := runs[0].a > 0
	trail := runs[len(runs)-1].b < len(f)
	out := make([]Constraint, 0, len(runs))
	for i, r := range runs {
		first, last := i == 0, i == len(runs)-1
		m := Full
		if first && !lead { // open on the left: the token may begin earlier
			m = Suffix
		}
		if last && !trail { // open on the right: the token may go on
			if m == Suffix {
				m = Infix
			} else {
				m = Prefix
			}
		}
		out = append(out, Constraint{Text: string(f[r.a:r.b]), Match: m})
	}
	return out
}
