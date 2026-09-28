package hdxadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Result is ClickHouse's FORMAT JSON answer with each row's values kept in
// column order and as the exact JSON text ClickHouse wrote.
type Result struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	Rows [][]json.RawMessage
	Raw  json.RawMessage // the answer as it came
}

// ParseResult reads a FORMAT JSON body. Rows are read as ordered key/value
// pairs, so column order and duplicate names survive, and must match meta.
func ParseResult(raw json.RawMessage) (*Result, error) {
	r := &Result{Raw: raw}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := expectDelim(dec, '{'); err != nil {
		return nil, err
	}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch k {
		case "meta":
			if err := dec.Decode(&r.Meta); err != nil {
				return nil, err
			}
		case "data":
			if err := expectDelim(dec, '['); err != nil {
				return nil, err
			}
			for dec.More() {
				if err := expectDelim(dec, '{'); err != nil {
					return nil, err
				}
				var row []json.RawMessage
				for dec.More() {
					if _, err := dec.Token(); err != nil { // the column name
						return nil, err
					}
					var v json.RawMessage
					if err := dec.Decode(&v); err != nil {
						return nil, err
					}
					row = append(row, compact(v))
				}
				if err := expectDelim(dec, '}'); err != nil {
					return nil, err
				}
				r.Rows = append(r.Rows, row)
			}
			if err := expectDelim(dec, ']'); err != nil {
				return nil, err
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, err
			}
		}
	}
	for i, row := range r.Rows {
		if len(row) != len(r.Meta) {
			return nil, fmt.Errorf("row %d has %d values for %d columns", i, len(row), len(r.Meta))
		}
	}
	return r, nil
}

func expectDelim(dec *json.Decoder, d json.Delim) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t != d {
		return fmt.Errorf("want %q, got %v", d, t)
	}
	return nil
}

func compact(v json.RawMessage) json.RawMessage {
	var b bytes.Buffer
	if err := json.Compact(&b, v); err != nil {
		return v
	}
	return b.Bytes()
}

// jsonString writes s as ClickHouse writes a JSON string (it escapes '/').
func jsonString(w *bytes.Buffer, s string) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	w.Write(bytes.ReplaceAll(bytes.TrimRight(b.Bytes(), "\n"), []byte("/"), []byte(`\/`)))
}

// Render writes r in format (one of Formats).
func (r *Result) Render(w io.Writer, format string) error {
	var b bytes.Buffer
	names := func() {
		b.WriteByte('[')
		for i, m := range r.Meta {
			if i > 0 {
				b.WriteByte(',')
			}
			jsonString(&b, m.Name)
		}
		b.WriteString("]\n")
	}
	types := func() {
		b.WriteByte('[')
		for i, m := range r.Meta {
			if i > 0 {
				b.WriteByte(',')
			}
			jsonString(&b, m.Type)
		}
		b.WriteString("]\n")
	}
	array := func(row []json.RawMessage) {
		b.WriteByte('[')
		for i, v := range row {
			if i > 0 {
				b.WriteByte(',')
			}
			b.Write(v)
		}
		b.WriteByte(']')
	}
	switch format {
	case "JSON":
		b.Write(r.Raw)
	case "JSONEachRow":
		for _, row := range r.Rows {
			b.WriteByte('{')
			for i, v := range row {
				if i > 0 {
					b.WriteByte(',')
				}
				jsonString(&b, r.Meta[i].Name)
				b.WriteByte(':')
				b.Write(v)
			}
			b.WriteString("}\n")
		}
	case "JSONCompactEachRowWithNamesAndTypes", "JSONCompactEachRowWithNames", "JSONCompactEachRow":
		if format != "JSONCompactEachRow" {
			names()
		}
		if format == "JSONCompactEachRowWithNamesAndTypes" {
			types()
		}
		for _, row := range r.Rows {
			array(row)
			b.WriteByte('\n')
		}
	case "JSONCompact":
		var head struct {
			Meta any `json:"meta"`
		}
		head.Meta = r.Meta
		mb, _ := json.Marshal(head.Meta)
		b.WriteString(`{"meta":`)
		b.Write(mb)
		b.WriteString(`,"data":[`)
		for i, row := range r.Rows {
			if i > 0 {
				b.WriteByte(',')
			}
			array(row)
		}
		fmt.Fprintf(&b, `],"rows":%d}`, len(r.Rows))
		b.WriteByte('\n')
	default:
		return fmt.Errorf("format %s", format)
	}
	_, err := w.Write(b.Bytes())
	return err
}
