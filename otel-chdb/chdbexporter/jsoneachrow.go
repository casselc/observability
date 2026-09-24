package chdbexporter

import (
	"encoding/hex"
	"strconv"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// jsonEachRow appends rows in JSONEachRow, one object per line keyed by
// column name. It is hand-written, appending into one buffer, so that the
// comparison with RowBinary is about the format and the insert path rather
// than about encoding/json's reflection.
type jsonEachRow struct {
	buf     []byte
	scratch []byte
	cols    []string
	col     int
	depth   int
	first   bool
}

func (w *jsonEachRow) row() {
	w.buf = append(w.buf, '{')
	w.col = 0
	w.depth = 0
}

func (w *jsonEachRow) endRow() { w.buf = append(w.buf, '}', '\n') }

// sep writes what comes before a value: the column key at the top level, a
// comma between array elements.
func (w *jsonEachRow) sep() {
	if w.depth == 0 {
		if w.col > 0 {
			w.buf = append(w.buf, ',')
		}
		w.buf = append(w.buf, '"')
		w.buf = append(w.buf, w.cols[w.col]...)
		w.buf = append(w.buf, '"', ':')
		w.col++
		return
	}
	if !w.first {
		w.buf = append(w.buf, ',')
	}
	w.first = false
}

func (w *jsonEachRow) arr(int) {
	w.sep()
	w.buf = append(w.buf, '[')
	w.depth++
	w.first = true
}

func (w *jsonEachRow) end() {
	w.buf = append(w.buf, ']')
	w.depth--
}

// ts writes DateTime64(9) as "seconds.nanoseconds", which ClickHouse parses
// exactly; a bare integer would be read as seconds.
func (w *jsonEachRow) ts(nanos uint64) {
	w.sep()
	w.buf = append(w.buf, '"')
	w.buf = strconv.AppendUint(w.buf, nanos/1e9, 10)
	w.buf = append(w.buf, '.')
	frac := nanos % 1e9
	for d := uint64(1e8); d > 0; d /= 10 {
		w.buf = append(w.buf, byte('0'+frac/d%10))
	}
	w.buf = append(w.buf, '"')
}

func (w *jsonEachRow) u8(v uint8) {
	w.sep()
	w.buf = strconv.AppendUint(w.buf, uint64(v), 10)
}

func (w *jsonEachRow) u64(v uint64) {
	w.sep()
	w.buf = strconv.AppendUint(w.buf, v, 10)
}

func (w *jsonEachRow) str(s string) {
	w.sep()
	w.buf = appendJSONString(w.buf, s)
}

func (w *jsonEachRow) hexID(id []byte) {
	w.sep()
	zero := true
	for _, b := range id {
		if b != 0 {
			zero = false
			break
		}
	}
	w.buf = append(w.buf, '"')
	if !zero {
		l := len(w.buf)
		w.buf = append(w.buf, make([]byte, len(id)*2)...)
		hex.Encode(w.buf[l:], id)
	}
	w.buf = append(w.buf, '"')
}

func (w *jsonEachRow) traceID(id pcommon.TraceID) { w.hexID(id[:]) }
func (w *jsonEachRow) spanID(id pcommon.SpanID)   { w.hexID(id[:]) }

func (w *jsonEachRow) attrs(m pcommon.Map) {
	w.sep()
	w.buf = append(w.buf, '{')
	first := true
	m.Range(func(k string, v pcommon.Value) bool {
		if !first {
			w.buf = append(w.buf, ',')
		}
		first = false
		w.buf = appendJSONString(w.buf, k)
		w.buf = append(w.buf, ':')
		if v.Type() == pcommon.ValueTypeStr {
			w.buf = appendJSONString(w.buf, v.Str())
		} else {
			w.scratch = valueString(w.scratch[:0], v)
			w.buf = appendJSONBytes(w.buf, w.scratch)
		}
		return true
	})
	w.buf = append(w.buf, '}')
}

const hexDigits = "0123456789abcdef"

func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c < utf8.RuneSelf {
			i++
			continue
		}
		if c >= utf8.RuneSelf {
			// Pass UTF-8 through; ClickHouse stores String as bytes either way.
			i++
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		i++
		start = i
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

func appendJSONBytes(dst, b []byte) []byte {
	// Values that reach here came from valueString: digits, true/false, or
	// AsString's JSON for maps and slices, which does need escaping.
	return appendJSONString(dst, unsafeString(b))
}
