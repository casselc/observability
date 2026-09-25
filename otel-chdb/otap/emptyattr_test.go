package otap

import (
	"testing"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// TestEmptyValuedAttributes: which attribute values survive the Go OTAP
// producer. The clickhouse exporter stores every key, with "" for an empty
// value; OTAP drops some keys entirely.
func TestEmptyValuedAttributes(t *testing.T) {
	ld := plog.NewLogs()
	r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	a := r.Attributes()
	a.PutEmpty("empty")
	a.PutStr("emptystr", "")
	a.PutEmptyBytes("emptybytes")
	a.PutEmptySlice("emptyslice")
	a.PutEmptyMap("emptymap")
	a.PutStr("s", "x")
	bar, err := EncodeLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	lds, _ := arrow_record.NewConsumer().LogsFrom(bar)
	got := lds[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	b, _ := Decode(bar)
	defer b.Release()
	rec := Flatten(b, &parquetgo.Envelope{}, nil)
	defer rec.Release()
	for _, k := range []string{"empty", "emptystr", "emptybytes", "emptyslice", "emptymap", "s"} {
		v, ok := got.Get(k)
		want, _ := a.Get(k)
		t.Logf("%-10s pdata %q -> library consumer present=%v %q", k, want.AsString(), ok, func() string {
			if ok {
				return v.AsString()
			}
			return ""
		}())
	}
	t.Logf("flattened LogAttributes: %s", rec.Column(14).ValueStr(0))
	_ = pcommon.NewValueEmpty
}
