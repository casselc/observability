package chdbexporter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// A test-only RowBinary decoder driven by the exporter's own structure
// strings (structure(signal, env)). The property tests use it to read back
// what the encoders wrote, and the fake sessions use it to count the rows
// and batch ids of an insert, so neither needs chDB.

type rbType struct {
	kind     string // String, UInt8, UInt16, UInt32, UInt64, DateTime64, Map, Array
	elem     *rbType
	key, val *rbType
}

type rbColumn struct {
	name string
	typ  *rbType
}

// splitTop splits s on commas that are not inside parentheses.
func splitTop(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if rest := strings.TrimSpace(s[start:]); rest != "" {
		out = append(out, rest)
	}
	return out
}

func parseRBType(s string) (*rbType, error) {
	s = strings.TrimSpace(s)
	open := strings.IndexByte(s, '(')
	if open < 0 {
		switch s {
		case "String", "UInt8", "UInt16", "UInt32", "UInt64":
			return &rbType{kind: s}, nil
		}
		return nil, fmt.Errorf("unsupported type %q", s)
	}
	if !strings.HasSuffix(s, ")") {
		return nil, fmt.Errorf("unbalanced type %q", s)
	}
	head, inner := s[:open], s[open+1:len(s)-1]
	switch head {
	case "DateTime64":
		return &rbType{kind: "DateTime64"}, nil
	case "LowCardinality":
		return parseRBType(inner)
	case "Array":
		e, err := parseRBType(inner)
		return &rbType{kind: "Array", elem: e}, err
	case "Map":
		kv := splitTop(inner)
		if len(kv) != 2 {
			return nil, fmt.Errorf("map %q", s)
		}
		k, err := parseRBType(kv[0])
		if err != nil {
			return nil, err
		}
		v, err := parseRBType(kv[1])
		return &rbType{kind: "Map", key: k, val: v}, err
	}
	return nil, fmt.Errorf("unsupported type %q", s)
}

// parseStructure parses "name Type, `a.b` Type, ..." as used by structure().
func parseStructure(s string) ([]rbColumn, error) {
	var cols []rbColumn
	for _, part := range splitTop(s) {
		var name, typ string
		if strings.HasPrefix(part, "`") {
			end := strings.IndexByte(part[1:], '`')
			if end < 0 {
				return nil, fmt.Errorf("column %q", part)
			}
			name, typ = part[1:end+1], part[end+2:]
		} else {
			sp := strings.IndexByte(part, ' ')
			if sp < 0 {
				return nil, fmt.Errorf("column %q", part)
			}
			name, typ = part[:sp], part[sp+1:]
		}
		t, err := parseRBType(typ)
		if err != nil {
			return nil, err
		}
		cols = append(cols, rbColumn{name: name, typ: t})
	}
	return cols, nil
}

// rbKV is one Map entry.
type rbKV struct{ K, V any }

var errTruncated = errors.New("rowbinary: truncated")

type rbReader struct {
	b   []byte
	off int
}

func (r *rbReader) need(n int) error {
	if n < 0 || r.off+n > len(r.b) {
		return errTruncated
	}
	return nil
}

func (r *rbReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.off:])
	if n <= 0 {
		return 0, errTruncated
	}
	r.off += n
	return v, nil
}

func (r *rbReader) value(t *rbType) (any, error) {
	switch t.kind {
	case "String":
		n, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if n > uint64(len(r.b)) {
			return nil, errTruncated
		}
		if err := r.need(int(n)); err != nil {
			return nil, err
		}
		s := string(r.b[r.off : r.off+int(n)])
		r.off += int(n)
		return s, nil
	case "UInt8":
		if err := r.need(1); err != nil {
			return nil, err
		}
		r.off++
		return uint64(r.b[r.off-1]), nil
	case "UInt16":
		if err := r.need(2); err != nil {
			return nil, err
		}
		r.off += 2
		return uint64(binary.LittleEndian.Uint16(r.b[r.off-2:])), nil
	case "UInt32":
		if err := r.need(4); err != nil {
			return nil, err
		}
		r.off += 4
		return uint64(binary.LittleEndian.Uint32(r.b[r.off-4:])), nil
	case "UInt64", "DateTime64":
		if err := r.need(8); err != nil {
			return nil, err
		}
		r.off += 8
		return binary.LittleEndian.Uint64(r.b[r.off-8:]), nil
	case "Array":
		n, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if n > uint64(len(r.b)) {
			return nil, errTruncated
		}
		out := make([]any, 0, n)
		for i := uint64(0); i < n; i++ {
			v, err := r.value(t.elem)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case "Map":
		n, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if n > uint64(len(r.b)) {
			return nil, errTruncated
		}
		out := make([]rbKV, 0, n)
		for i := uint64(0); i < n; i++ {
			k, err := r.value(t.key)
			if err != nil {
				return nil, err
			}
			v, err := r.value(t.val)
			if err != nil {
				return nil, err
			}
			out = append(out, rbKV{k, v})
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported kind %s", t.kind)
}

// decodeRowBinary decodes every row of data; it fails on trailing or missing
// bytes.
func decodeRowBinary(cols []rbColumn, data []byte) ([][]any, error) {
	r := &rbReader{b: data}
	var rows [][]any
	for r.off < len(r.b) {
		row := make([]any, len(cols))
		for i, c := range cols {
			v, err := r.value(c.typ)
			if err != nil {
				return rows, fmt.Errorf("row %d column %s: %w", len(rows), c.name, err)
			}
			row[i] = v
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// decodeSignal decodes one signal's insert payload as the exporter writes it.
func decodeSignal(signal string, env bool, data []byte) ([]rbColumn, [][]any, error) {
	cols, err := parseStructure(structure(signal, env))
	if err != nil {
		return nil, nil, err
	}
	rows, err := decodeRowBinary(cols, data)
	return cols, rows, err
}

func colIndex(cols []rbColumn, name string) int {
	for i, c := range cols {
		if c.name == name {
			return i
		}
	}
	return -1
}
