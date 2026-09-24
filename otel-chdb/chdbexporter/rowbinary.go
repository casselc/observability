package chdbexporter

import (
	"encoding/binary"
	"encoding/hex"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// rowBinary appends rows in ClickHouse's RowBinary format: fixed-width
// little-endian numbers, strings and arrays as a LEB128 length then the
// payload, maps as an array of key/value pairs. LowCardinality(String) is a
// plain String on the wire.
type rowBinary struct {
	buf     []byte
	scratch []byte
}

func (w *rowBinary) row()    {}
func (w *rowBinary) endRow() {}
func (w *rowBinary) end()    {}

func (w *rowBinary) ts(nanos uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, nanos) }
func (w *rowBinary) u8(v uint8)      { w.buf = append(w.buf, v) }
func (w *rowBinary) u64(v uint64)    { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }
func (w *rowBinary) arr(n int)       { w.buf = binary.AppendUvarint(w.buf, uint64(n)) }

func (w *rowBinary) str(s string) {
	w.buf = binary.AppendUvarint(w.buf, uint64(len(s)))
	w.buf = append(w.buf, s...)
}

// hexID writes id as lowercase hex, or "" when every byte is zero, matching
// traceutil.TraceIDToHexOrEmptyString.
func (w *rowBinary) hexID(id []byte) {
	zero := true
	for _, b := range id {
		if b != 0 {
			zero = false
			break
		}
	}
	if zero {
		w.buf = append(w.buf, 0)
		return
	}
	n := len(id) * 2
	w.buf = append(w.buf, byte(n)) // 32 and 16 are single-byte varints
	l := len(w.buf)
	w.buf = append(w.buf, make([]byte, n)...)
	hex.Encode(w.buf[l:], id)
}

func (w *rowBinary) traceID(id pcommon.TraceID) { w.hexID(id[:]) }
func (w *rowBinary) spanID(id pcommon.SpanID)   { w.hexID(id[:]) }

func (w *rowBinary) attrs(m pcommon.Map) {
	w.buf = binary.AppendUvarint(w.buf, uint64(m.Len()))
	m.Range(func(k string, v pcommon.Value) bool {
		w.str(k)
		if v.Type() == pcommon.ValueTypeStr {
			w.str(v.Str())
			return true
		}
		w.scratch = valueString(w.scratch[:0], v)
		w.buf = binary.AppendUvarint(w.buf, uint64(len(w.scratch)))
		w.buf = append(w.buf, w.scratch...)
		return true
	})
}
