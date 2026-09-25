//go:build !noarrow

package parquetgo

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func sampleTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 50; i++ {
		s := ss.Spans().AppendEmpty()
		s.SetName("op\x00")
		if i%2 == 0 {
			s.SetTraceID(pcommon.TraceID{1, byte(i)})
		}
		s.SetStartTimestamp(pcommon.Timestamp(1_000_000_000 + i))
		s.Attributes().PutDouble("d", float64(i)/3)
		for e := 0; e < i%3; e++ {
			s.Events().AppendEmpty().Attributes().PutInt("n", int64(e))
		}
	}
	return td
}

// TestEnginesAgree: both engines' files read back (with arrow-go's reader)
// as equal tables. The comparison against chDB lives in ./compare.
func TestEnginesAgree(t *testing.T) {
	td := sampleTraces()
	env := func() *Envelope { return &Envelope{Producer: "p", Epoch: "e", Batch: 1, Received: 7, Schema: 1} }
	var a, b bytes.Buffer
	if _, err := NewEncoder(DefaultOptions(), nil).Traces(&a, td, env()); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPGEncoder(DefaultOptions()).Traces(&b, td, env()); err != nil {
		t.Fatal(err)
	}
	read := func(buf []byte) arrow.Table {
		r, err := file.NewParquetReader(bytes.NewReader(buf))
		if err != nil {
			t.Fatal(err)
		}
		fr, err := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := fr.ReadTable(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return tbl
	}
	ta, tb := read(a.Bytes()), read(b.Bytes())
	if ta.NumRows() != 50 || !array.TableEqual(ta, tb) {
		t.Fatalf("tables differ: %d vs %d rows\n%v\n%v", ta.NumRows(), tb.NumRows(), ta.Schema(), tb.Schema())
	}
}
