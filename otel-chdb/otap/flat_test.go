package otap

import (
	"maps"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/casselc/observability/otel-chdb/chdbexporter/testgen"
	"github.com/casselc/observability/otel-chdb/parquetgo"
	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
)

// Regression: parquetgo's traces and logs schemas gained resource_id and
// resource_announce before the envelope (4d382ac); Flatten still wrote the
// envelope at a fixed index into them and panicked on every batch. The
// resource columns hold parquetgo's ids (CoveredOf), the envelope the
// producer and row ordinals.
func TestFlattenResourceAndEnvelope(t *testing.T) {
	td, ld := testgen.Traces(300), testgen.Logs(300)
	tb, err := EncodeTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	lb, err := EncodeLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	wantT, wantL := map[uint64]bool{}, map[uint64]bool{}
	for _, r := range td.ResourceSpans().All() {
		wantT[parquetgo.CoveredOf(r.Resource().Attributes()).ID] = true
	}
	for _, r := range ld.ResourceLogs().All() {
		wantL[parquetgo.CoveredOf(r.Resource().Attributes()).ID] = true
	}
	for _, c := range []struct {
		bar    *pb.BatchArrowRecords
		schema *arrow.Schema
		want   map[uint64]bool
	}{{tb, parquetgo.TracesSchema, wantT}, {lb, parquetgo.LogsSchema, wantL}} {
		b, err := Decode(c.bar)
		if err != nil {
			t.Fatal(err)
		}
		rec := Flatten(b, &parquetgo.Envelope{Producer: "p", Epoch: "e", Batch: 7}, nil)
		if !rec.Schema().Equal(c.schema) {
			t.Fatalf("schema: %v", rec.Schema())
		}
		idx := func(n string) arrow.Array { return rec.Column(c.schema.FieldIndices(n)[0]) }
		ids, ann := idx("resource_id").(*array.Uint64), idx("resource_announce").(*array.Map)
		prod, ord := idx("producer_id").(*array.String), idx("row_ordinal").(*array.Uint32)
		got := map[uint64]bool{}
		for i := range int(rec.NumRows()) {
			got[ids.Value(i)] = true
			if s, e := ann.ValueOffsets(i); s != e {
				t.Fatalf("row %d: an announcement", i)
			}
			if prod.Value(i) != "p" || ord.Value(i) != uint32(i) {
				t.Fatalf("row %d: envelope %q %d", i, prod.Value(i), ord.Value(i))
			}
		}
		if g, w := slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(c.want)); !slices.Equal(g, w) || len(w) < 2 || w[0] == 0 && len(w) == 1 {
			t.Fatalf("resource ids %v, want %v (parquetgo.CoveredOf)", g, w)
		}
		t.Logf("%d rows, resource ids %v", rec.NumRows(), slices.Sorted(maps.Keys(got)))
		rec.Release()
		b.Release()
	}
}
