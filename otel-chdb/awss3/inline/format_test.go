package inline

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func spans(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "bench")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := range n {
		sp := ss.Spans().AppendEmpty()
		sp.SetName(fmt.Sprintf("GET /api/v1/items/%d", i%100))
		sp.SetTraceID(pcommon.TraceID([16]byte{byte(i), byte(i >> 8), byte(i >> 16), 1}))
		sp.SetSpanID(pcommon.SpanID([8]byte{byte(i), byte(i >> 8), 2}))
		sp.SetStartTimestamp(pcommon.Timestamp(1_758_800_000_000_000_000 + int64(i)))
		sp.SetEndTimestamp(sp.StartTimestamp() + 1000)
		sp.Attributes().PutStr("http.method", "GET")
		sp.Attributes().PutInt("http.status_code", 200)
		sp.Attributes().PutStr("net.peer.name", "db-1.internal")
	}
	return td
}

// The hash survives the persistent queue's protobuf round trip.
func TestContentHashStableAcrossProtoRoundTrip(t *testing.T) {
	td := spans(1000)
	h1, _ := ContentHashTraces(td)
	b, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	back, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := ContentHashTraces(back)
	if h1 != h2 {
		t.Fatalf("%s != %s", h1, h2)
	}
	td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(7).SetName("changed")
	if h3, _ := ContentHashTraces(td); h3 == h1 {
		t.Fatal("hash ignores content")
	}
}

func TestSlotKeyOrderAndParse(t *testing.T) {
	a, b := SlotKey("p/x", "e1", 9), SlotKey("p/x", "e1", 10)
	if !(a < b) {
		t.Fatal("slot keys do not sort by slot")
	}
	if e, s, ok := ParseSlotKey("p/x", b); !ok || e != "e1" || s != 10 {
		t.Fatal(e, s, ok)
	}
}

func TestAddFooterKVKeepsFileReadable(t *testing.T) {
	type row struct {
		A int64  `parquet:"a"`
		B string `parquet:"b"`
	}
	var buf bytes.Buffer
	if err := parquet.Write(&buf, []row{{1, "x"}, {2, "y"}}); err != nil {
		t.Fatal(err)
	}
	out, err := AddFooterKV(buf.Bytes(), map[string]string{MetaSeq: "7", MetaContent: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	kv, err := FooterKV(out)
	if err != nil || kv[MetaSeq] != "7" || kv[MetaContent] != "abc" {
		t.Fatal(kv, err)
	}
	rows, err := parquet.Read[row](bytes.NewReader(out), int64(len(out)))
	if err != nil || len(rows) != 2 || rows[1].B != "y" {
		t.Fatal(rows, err)
	}
}

// Cost of identifying a 10k-span request: protobuf encoding plus SHA-256.
func BenchmarkContentHashTraces10k(b *testing.B) {
	td := spans(10_000)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ContentHashTraces(td); err != nil {
			b.Fatal(err)
		}
	}
}
