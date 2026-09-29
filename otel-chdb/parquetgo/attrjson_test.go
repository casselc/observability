package parquetgo

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// attrVector is one of testdata/attrjson_vectors.json's values (the format
// is in the file's comment).
type attrVector struct {
	S    *string      `json:"s"`
	SHex *string      `json:"s_hex"`
	I    *string      `json:"i"`
	D    *string      `json:"d"`
	B    *bool        `json:"b"`
	YHex *string      `json:"y_hex"`
	E    bool         `json:"e"`
	L    []attrVector `json:"l"`
	M    []struct {
		K    *string    `json:"k"`
		KHex *string    `json:"k_hex"`
		V    attrVector `json:"v"`
	} `json:"m"`
}

func (a attrVector) set(t *testing.T, v pcommon.Value) {
	unhex := func(h string) []byte {
		b, err := hex.DecodeString(h)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	switch {
	case a.S != nil:
		v.SetStr(*a.S)
	case a.SHex != nil:
		v.SetStr(string(unhex(*a.SHex)))
	case a.I != nil:
		i, err := strconv.ParseInt(*a.I, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		v.SetInt(i)
	case a.D != nil:
		f, err := strconv.ParseFloat(*a.D, 64)
		if err != nil && !math.IsInf(f, 0) {
			t.Fatal(err)
		}
		v.SetDouble(f)
	case a.B != nil:
		v.SetBool(*a.B)
	case a.YHex != nil:
		v.SetEmptyBytes().FromRaw(unhex(*a.YHex))
	case a.E:
	case a.L != nil:
		s := v.SetEmptySlice()
		for _, e := range a.L {
			e.set(t, s.AppendEmpty())
		}
	case a.M != nil:
		m := v.SetEmptyMap()
		for _, kv := range a.M {
			k := ""
			if kv.K != nil {
				k = *kv.K
			} else {
				k = string(unhex(*kv.KHex))
			}
			kv.V.set(t, m.PutEmpty(k))
		}
	default:
		t.Fatalf("empty vector value")
	}
}

// The shared vectors (testdata/attrjson_vectors.json, which the Rust edge's
// src/render.rs checks too): Go 1.27's bytes, pinned. Invalid UTF-8 in a
// map (values, nested values, keys) and in a slice is a raw U+FFFD per
// byte; until AMBIGUITY E10 was built (2026-09-29) it was Go 1.26's escape.
func TestAttrJSONSharedVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/attrjson_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Vectors []struct {
			Name  string     `json:"name"`
			Value attrVector `json:"value"`
			Want  string     `json:"want"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Vectors) < 8 {
		t.Fatalf("%d vectors", len(f.Vectors))
	}
	for _, vec := range f.Vectors {
		v := pcommon.NewValueEmpty()
		vec.Value.set(t, v)
		if got := AttrString(v); got != vec.Want {
			t.Errorf("%s:\ngot  %q\nwant %q", vec.Name, got, vec.Want)
		}
		if got := string(AppendAttrJSON([]byte("x"), v)); got != "x"+vec.Want {
			t.Errorf("%s: AppendAttrJSON does not append: %q", vec.Name, got)
		}
		// The toolchain this module builds with (go.mod) writes the same
		// bytes. If a later toolchain does not, this assertion fails and
		// the pin (the Want above) holds the edges' bytes: changing them
		// is an owner decision (it changes series ids), not a test fix.
		if tc := v.AsString(); tc != vec.Want {
			t.Errorf("%s: the toolchain's AsString differs from the pinned bytes:\ntoolchain %q\npinned    %q", vec.Name, tc, vec.Want)
		}
	}
}

// The pinned bytes of the nastiest case, written out here so a change of
// the vectors file alone cannot move them.
func TestAttrJSONPinnedBytes(t *testing.T) {
	m := pcommon.NewValueMap()
	m.Map().PutStr("s", "bad \xff\xfe x")
	m.Map().PutStr("u", string(rune(0x2028))+"<&>")
	m.Map().PutStr("c", "\x01\x7f\b\f\n\r\t\"\\")
	m.Map().PutDouble("f1", 1e21)
	m.Map().PutDouble("f2", 1e-7)
	m.Map().PutDouble("f3", 9007199254740992)
	m.Map().PutDouble("f4", math.Copysign(0, -1))
	m.Map().PutInt("i", -42)
	m.Map().PutBool("b", true)
	m.Map().PutEmpty("n")
	m.Map().PutEmptyBytes("y").FromRaw([]byte{0, 1, 0xff})
	in := m.Map().PutEmptyMap("m")
	in.PutStr("inner", "invalid \xc3\x28 utf8")
	in.PutEmptySlice("l").AppendEmpty().SetStr(string(rune(0x2029)))
	want := `{"b":true,"c":"@u0001` + "\x7f" + `\b\f\n\r\t\"\\","f1":1e+21,"f2":1e-7,"f3":9007199254740992,"f4":-0,"i":-42,` +
		`"m":{"inner":"invalid @R( utf8","l":["@u2029"]},"n":null,"s":"bad @R@R x","u":"@u2028<&>","y":"AAH/"}`
	want = strings.ReplaceAll(want, "@u", "\\u")
	want = strings.ReplaceAll(want, "@R", "\uFFFD") // a raw U+FFFD: EF BF BD
	if got := AttrString(m); got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
	// a slice with invalid UTF-8, nested
	sl := pcommon.NewValueSlice()
	sl.Slice().AppendEmpty().SetStr("\xff")
	sl.Slice().AppendEmpty().SetEmptySlice().AppendEmpty().SetStr("\xe2\x82")
	sl.Slice().AppendEmpty().SetEmptyMap().PutStr("k\xff", "\xed\xa0\x80")
	if got, want := AttrString(sl), "[\"\uFFFD\",[\"\uFFFD\uFFFD\"],{\"k\uFFFD\":\"\uFFFD\uFFFD\uFFFD\"}]"; got != want {
		t.Fatalf("slice: got %q, want %q", got, want)
	}
	// a NaN anywhere: AsString gives "" (the encoder fails)
	m.Map().PutDouble("nan", math.NaN())
	if got := AttrString(m); got != "" {
		t.Fatalf("NaN: %q", got)
	}
	// scalars are AsString
	for _, v := range []pcommon.Value{pcommon.NewValueStr("\xff"), pcommon.NewValueDouble(1e21), pcommon.NewValueInt(7)} {
		if AttrString(v) != v.AsString() {
			t.Fatal(v.AsString())
		}
	}
}

// The pinned rendering is the toolchain's encoding/json (Go 1.27) over
// random nested values, invalid UTF-8 in keys and strings included. Like
// the vectors' toolchain check, a failure after a toolchain bump means the
// toolchain moved, not that the pin should.
func TestAttrJSONMatchesEncodingJSON(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	alphabet := []string{"a", "Z", " ", "\"", "\\", "\n", "\x00", "\x1f", "\x7f", "<", "&", "é", string(rune(0x2028)), string(rune(0x2029)), "😀", string(rune(0xfffd)), "\xff", "\xc3", "\xe2\x82", "\xed\xa0\x80", "\xf4\x90"}
	str := func() string {
		var b strings.Builder
		for range rng.IntN(6) {
			b.WriteString(alphabet[rng.IntN(len(alphabet))])
		}
		return b.String()
	}
	var fill func(m pcommon.Map, depth int)
	fillSlice := func(s pcommon.Slice, depth int) {}
	fill = func(m pcommon.Map, depth int) {
		for range rng.IntN(5) {
			k := str()
			switch rng.IntN(8) {
			case 0:
				m.PutStr(k, str())
			case 1:
				m.PutInt(k, rng.Int64()-rng.Int64())
			case 2:
				m.PutDouble(k, math.Float64frombits(rng.Uint64())) // may be NaN/Inf: then skipped below
			case 3:
				m.PutDouble(k, float64(rng.IntN(2000)-1000)*math.Pow(10, float64(rng.IntN(50)-25)))
			case 4:
				m.PutBool(k, rng.IntN(2) == 0)
			case 5:
				m.PutEmptyBytes(k).FromRaw([]byte(str()))
			case 6:
				if depth < 3 {
					fill(m.PutEmptyMap(k), depth+1)
				}
			case 7:
				if depth < 3 {
					fillSlice(m.PutEmptySlice(k), depth+1)
				}
			}
		}
	}
	fillSlice = func(s pcommon.Slice, depth int) {
		for range rng.IntN(4) {
			switch rng.IntN(3) {
			case 0:
				s.AppendEmpty().SetStr(str())
			case 1:
				s.AppendEmpty().SetDouble(float64(rng.IntN(100)) / 7)
			case 2:
				if depth < 3 {
					fill(s.AppendEmpty().SetEmptyMap(), depth+1)
				}
			}
		}
	}
	checked := 0
	for range 3000 {
		v := pcommon.NewValueMap()
		fill(v.Map(), 0)
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(v.Map().AsRaw()); err != nil {
			if AttrString(v) != "" {
				t.Fatalf("unencodable value rendered: %s", AttrString(v))
			}
			continue
		}
		want := strings.TrimRight(buf.String(), "\n")
		if !utf8.ValidString(want) {
			t.Fatalf("encoding/json wrote invalid UTF-8: %q", want)
		}
		if got := AttrString(v); got != want {
			t.Fatalf("got  %q\nwant %q", got, want)
		}
		checked++
	}
	if checked < 1000 {
		t.Fatalf("only %d values checked", checked)
	}
}
