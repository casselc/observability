package parquetgo

import (
	"encoding/base64"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// AttrString is v as the string the edge stores and hashes for it:
// pcommon.Value.AsString, except that a map or a slice is rendered by
// AppendAttrJSON, whose output is pinned here instead of following the
// toolchain's encoding/json.
//
// Why: AsString renders maps and slices with encoding/json, and Go 1.27
// made encoding/json v1 run on the json/v2 engine (GOEXPERIMENT jsonv2 on by
// default), which writes an invalid UTF-8 byte as a raw U+FFFD where Go ≤
// 1.26 wrote the six-byte escape (backslash, u, fffd). The same request then gave another
// AttributesValues string, and another series_id (series.go hashes the
// rendering), under Go 1.27 than under 1.26 and than the Rust edge, which
// mirrors the ≤ 1.26 bytes (otap-rs tests/series.rs
// same_as_go_prototype_corr, CI run 36532538131). An edge's output must
// not depend on the compiler it was built with.
func AttrString(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		return string(AppendAttrJSON(nil, v))
	}
	return v.AsString()
}

// AppendAttrJSON appends a map or slice value as JSON exactly as
// encoding/json v1 (Go ≤ 1.26) marshalled its AsRaw form with HTML
// escaping off, which is what pcommon's AsString produced: keys sorted
// bytewise; strings escaped with \" \\ \b \f \n \r \t and \u00XX for the
// other control bytes, invalid UTF-8 as the escape for U+FFFD per byte, U+2028 and U+2029
// escaped, everything else (DEL included) raw; numbers as ES6 would print
// them (exponent form below 1e-6 and from 1e21, no exponent padding);
// bytes in standard base64. A NaN or an infinity anywhere makes the whole
// value unencodable, and AsString then returned "": so does this. Other
// value types are appended as AsString.
func AppendAttrJSON(dst []byte, v pcommon.Value) []byte {
	var raw any
	switch v.Type() {
	case pcommon.ValueTypeMap:
		raw = v.Map().AsRaw()
	case pcommon.ValueTypeSlice:
		raw = v.Slice().AsRaw()
	default:
		return append(dst, v.AsString()...)
	}
	out, ok := appendJSONv1(dst, raw)
	if !ok {
		return dst
	}
	return out
}

func appendJSONv1(b []byte, v any) ([]byte, bool) {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...), true
	case bool:
		return strconv.AppendBool(b, x), true
	case int64:
		return strconv.AppendInt(b, x, 10), true
	case float64:
		return appendFloatV1(b, x)
	case string:
		return appendStringV1(b, x), true
	case []byte:
		if x == nil { // an empty Bytes value's AsRaw
			return append(b, "null"...), true
		}
		b = append(b, '"')
		b = base64.StdEncoding.AppendEncode(b, x)
		return append(b, '"'), true
	case []any:
		if x == nil {
			return append(b, "null"...), true
		}
		b = append(b, '[')
		for i, e := range x {
			if i > 0 {
				b = append(b, ',')
			}
			var ok bool
			if b, ok = appendJSONv1(b, e); !ok {
				return b, false
			}
		}
		return append(b, ']'), true
	case map[string]any:
		if x == nil {
			return append(b, "null"...), true
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, strings.Compare)
		b = append(b, '{')
		for i, k := range keys {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendStringV1(b, k)
			b = append(b, ':')
			var ok bool
			if b, ok = appendJSONv1(b, x[k]); !ok {
				return b, false
			}
		}
		return append(b, '}'), true
	}
	return b, false // AsRaw produces none of the other types
}

func appendFloatV1(b []byte, f float64) ([]byte, bool) {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return b, false
	}
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		// e-09 → e-9
		if n := len(b); n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return b, true
}

const hexDigits = "0123456789abcdef"

// appendStringV1 is encoding/json's appendString (Go 1.26) with
// escapeHTML false.
func appendStringV1(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch c {
			case '\\', '"':
				dst = append(dst, '\\', c)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', 'f', 'f', 'f', 'd')
			i += size
			start = i
			continue
		}
		if r == 0x2028 || r == 0x2029 { // LINE and PARAGRAPH SEPARATOR
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
