package otap

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// OTAP columns come in several physical types (native, Dictionary(u8|u16)),
// and may be absent altogether. These accessors resolve a (column, row) to a
// value, looking through dictionaries; a nil column reads as null.

// deref follows a dictionary to its values array and index.
func deref(a arrow.Array, i int) (arrow.Array, int, bool) {
	if a == nil || a.IsNull(i) {
		return nil, 0, false
	}
	if d, ok := a.(*array.Dictionary); ok {
		return d.Dictionary(), d.GetValueIndex(i), true
	}
	return a, i, true
}

func strAt(a arrow.Array, i int) (string, bool) {
	v, j, ok := deref(a, i)
	if !ok || v.IsNull(j) {
		return "", false
	}
	switch t := v.(type) {
	case *array.String:
		return t.Value(j), true
	case *array.LargeString:
		return t.Value(j), true
	case *array.Binary:
		return string(t.Value(j)), true
	}
	return "", false
}

func bytesAt(a arrow.Array, i int) ([]byte, bool) {
	v, j, ok := deref(a, i)
	if !ok || v.IsNull(j) {
		return nil, false
	}
	switch t := v.(type) {
	case *array.Binary:
		return t.Value(j), true
	case *array.FixedSizeBinary:
		return t.Value(j), true
	case *array.String:
		return []byte(t.Value(j)), true
	}
	return nil, false
}

// uintAt reads any unsigned or signed integer column as uint64.
func uintAt(a arrow.Array, i int) (uint64, bool) {
	v, j, ok := deref(a, i)
	if !ok || v.IsNull(j) {
		return 0, false
	}
	switch t := v.(type) {
	case *array.Uint8:
		return uint64(t.Value(j)), true
	case *array.Uint16:
		return uint64(t.Value(j)), true
	case *array.Uint32:
		return uint64(t.Value(j)), true
	case *array.Uint64:
		return t.Value(j), true
	case *array.Int32:
		return uint64(t.Value(j)), true
	case *array.Int64:
		return uint64(t.Value(j)), true
	case *array.Duration:
		return uint64(t.Value(j)), true
	case *array.Timestamp:
		return uint64(t.Value(j)), true
	}
	return 0, false
}

func intAt(a arrow.Array, i int) (int64, bool) {
	v, j, ok := deref(a, i)
	if !ok || v.IsNull(j) {
		return 0, false
	}
	switch t := v.(type) {
	case *array.Int64:
		return t.Value(j), true
	case *array.Int32:
		return int64(t.Value(j)), true
	case *array.Uint8:
		return int64(t.Value(j)), true
	case *array.Uint16:
		return int64(t.Value(j)), true
	case *array.Uint32:
		return int64(t.Value(j)), true
	}
	return 0, false
}

func f64At(a arrow.Array, i int) (float64, bool) {
	v, j, ok := deref(a, i)
	if !ok || v.IsNull(j) {
		return 0, false
	}
	if t, ok := v.(*array.Float64); ok {
		return t.Value(j), true
	}
	return 0, false
}

func boolAt(a arrow.Array, i int) (bool, bool) {
	v, j, ok := deref(a, i)
	if !ok || v.IsNull(j) {
		return false, false
	}
	if t, ok := v.(*array.Boolean); ok {
		return t.Value(j), true
	}
	return false, false
}

// col returns the named top-level column, or nil.
func col(r arrow.Record, name string) arrow.Array {
	if r == nil {
		return nil
	}
	idx := r.Schema().FieldIndices(name)
	if len(idx) == 0 {
		return nil
	}
	return r.Column(idx[0])
}

// child returns the named field of a struct column, or nil.
func child(a arrow.Array, name string) arrow.Array {
	s, ok := a.(*array.Struct)
	if !ok {
		return nil
	}
	st := s.DataType().(*arrow.StructType)
	i, ok := st.FieldIdx(name)
	if !ok {
		return nil
	}
	return s.Field(i)
}

func rows(r arrow.Record) int {
	if r == nil {
		return 0
	}
	return int(r.NumRows())
}
