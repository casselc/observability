package parquetgo

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// The bytes Go ≤ 1.26's AsString gave (recorded with go1.26.0 on
// 2026-09-29), which the Rust edge mirrors: invalid UTF-8 as the escape
// for U+FFFD, U+2028 escaped, DEL raw, ES6 number forms. Go 1.27's own
// encoding/json writes the first case's invalid bytes as raw U+FFFD.
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
		`"m":{"inner":"invalid @ufffd( utf8","l":["@u2029"]},"n":null,"s":"bad @ufffd@ufffd x","u":"@u2028<&>","y":"AAH/"}`
	want = strings.ReplaceAll(want, "@u", "\\u")
	if got := AttrString(m); got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
	// a NaN anywhere: AsString gave "" (the encoder failed)
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

// Where the toolchain's encoding/json cannot differ (valid UTF-8, no NaN),
// the pinned rendering is encoding/json's, over random nested values.
func TestAttrJSONMatchesEncodingJSONOnValidInput(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	alphabet := []string{"a", "Z", " ", "\"", "\\", "\n", "\x00", "\x1f", "\x7f", "<", "&", "é", string(rune(0x2028)), string(rune(0x2029)), "😀", string(rune(0xfffd))}
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
			t.Fatal("generator made invalid UTF-8")
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
