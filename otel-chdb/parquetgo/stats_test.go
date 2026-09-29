package parquetgo

import (
	"bytes"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

func TestTruncateStatistics(t *testing.T) {
	tracetag.Covers(t, "PH", "CAST-9", "H-7")
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i, v := range []string{strings.Repeat("\xff", 100) + "z", strings.Repeat("b", 1<<20), "a"} {
		s := ss.Spans().AppendEmpty()
		s.SetName(v)
		s.Attributes().PutStr("k", strings.Repeat(string(rune('a'+i)), 200))
	}
	var buf bytes.Buffer
	if _, err := NewPGEncoder(DefaultOptions()).Traces(&buf, td, &Envelope{Producer: "p"}); err != nil {
		t.Fatal(err)
	}
	out, err := TruncateStatistics(buf.Bytes(), 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) > buf.Len() {
		t.Fatalf("footer grew: %d -> %d", buf.Len(), len(out))
	}
	f, err := parquet.OpenFile(bytes.NewReader(out), int64(len(out)))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range f.Metadata().RowGroups[0].Columns {
		st := c.MetaData.Statistics
		if len(st.MinValue) > 64 || len(st.MaxValue) > 64 {
			t.Fatalf("%v: %d / %d bytes", c.MetaData.PathInSchema, len(st.MinValue), len(st.MaxValue))
		}
		if strings.Join(c.MetaData.PathInSchema, ".") == "SpanName" {
			// min "a" untouched; max ("\xff"*100 + "z") has no shorter upper bound: dropped.
			if string(st.MinValue) != "" || st.MaxValue != nil {
				t.Fatalf("SpanName bounds %q %q", st.MinValue, st.MaxValue)
			}
		}
		if strings.Join(c.MetaData.PathInSchema, ".") == "SpanAttributes.key_value.value" {
			if string(st.MinValue) != strings.Repeat("a", 64) || string(st.MaxValue) != strings.Repeat("c", 63)+"d" {
				t.Fatalf("value bounds %q %q", st.MinValue, st.MaxValue)
			}
		}
	}
	// The rows read back unchanged.
	rows, err := parquet.Read[struct {
		Name string `parquet:"SpanName"`
	}](bytes.NewReader(out), int64(len(out)))
	if err != nil || len(rows) != 3 || len(rows[1].Name) != 1<<20 {
		t.Fatal(err, len(rows))
	}
}
