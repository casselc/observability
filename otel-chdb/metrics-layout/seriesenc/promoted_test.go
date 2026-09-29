package seriesenc

// The prototype is promoted into ../../parquetgo (series.go, SeriesEncoder):
// with PrototypeSeriesOptions the promoted encoder must write this
// prototype's objects, column for column: the same Parquet schema (every
// leaf's path, physical and logical type) and the same values and levels,
// so the same series ids. (The promoted encoder's defaults add the Rust
// edge's merged number points and exemplar attributes; those are checked
// against the Rust edge in ../../conformance.)

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/casselc/observability/otel-chdb/metrics-layout/fleet"
	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// hostile: several resources and scopes (the scope-attributes bug the Rust
// port found), exemplars, NaN and extreme values, invalid UTF-8, empty and
// 20-entry maps, every value type, and every metric type.
func hostile() pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := 0; r < 3; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		ra := rm.Resource().Attributes()
		ra.PutStr("service.name", fmt.Sprintf("svc-%d-\xff", r))
		ra.PutInt("zz.int", int64(r)-1<<62)
		ra.PutDouble("aa.double", math.Inf(1))
		ra.PutBool("b", r%2 == 0)
		ra.PutEmptyBytes("bytes").FromRaw([]byte{0, 1, 2})
		ra.PutEmptySlice("slice").AppendEmpty().SetStr("x")
		rm.SetSchemaUrl("https://s/1")
		for s := 0; s < 2; s++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName(fmt.Sprintf("scope-%d", s))
			sm.Scope().SetVersion("v1")
			sm.Scope().Attributes().PutStr("scope.attr", fmt.Sprintf("s%d", s))
			sm.Scope().SetDroppedAttributesCount(uint32(s))
			sm.SetSchemaUrl("https://scope")
			ts := pcommon.Timestamp(1_700_000_000_000_000_000 + int64(s)*1e9)
			g := sm.Metrics().AppendEmpty()
			g.SetName("g")
			g.SetUnit("1")
			gd := g.SetEmptyGauge()
			for p := 0; p < 20; p++ {
				dp := gd.DataPoints().AppendEmpty()
				for k := 0; k < p; k++ {
					dp.Attributes().PutStr(fmt.Sprintf("k%02d", 19-k), fmt.Sprintf("v%d", k))
				}
				dp.SetTimestamp(ts)
				dp.SetStartTimestamp(ts - 1e9)
				if p%3 == 0 {
					dp.SetIntValue(math.MinInt64)
				} else {
					dp.SetDoubleValue(math.NaN())
				}
				if p%4 == 0 {
					ex := dp.Exemplars().AppendEmpty()
					ex.SetDoubleValue(1.5)
					ex.SetTimestamp(ts)
					ex.SetTraceID(pcommon.TraceID{1, 2, 3})
					ex.FilteredAttributes().PutStr("f", "g")
				}
			}
			sum := sm.Metrics().AppendEmpty()
			sum.SetName("s")
			sd := sum.SetEmptySum()
			sd.SetIsMonotonic(true)
			sd.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for p := 0; p < 5; p++ {
				dp := sd.DataPoints().AppendEmpty()
				dp.Attributes().PutInt("p", int64(p))
				dp.SetTimestamp(ts)
				dp.SetIntValue(int64(p))
			}
			h := sm.Metrics().AppendEmpty()
			h.SetName("h")
			hd := h.SetEmptyHistogram()
			hd.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			for p := 0; p < 4; p++ {
				dp := hd.DataPoints().AppendEmpty()
				dp.Attributes().PutStr("route", fmt.Sprintf("/r%d", p))
				dp.SetTimestamp(ts)
				dp.SetCount(uint64(p) * 3)
				dp.SetSum(math.Copysign(0, -1))
				dp.ExplicitBounds().FromRaw([]float64{0, 1, float64(p)})
				dp.BucketCounts().FromRaw([]uint64{1, 1, uint64(p), math.MaxUint64})
				if p%2 == 0 {
					dp.SetMin(-1)
					dp.SetMax(math.MaxFloat64)
				}
			}
			e := sm.Metrics().AppendEmpty()
			e.SetName("e")
			ed := e.SetEmptyExponentialHistogram()
			dp := ed.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts)
			dp.SetScale(-4)
			dp.SetZeroCount(3)
			dp.Positive().SetOffset(math.MinInt32)
			dp.Positive().BucketCounts().FromRaw([]uint64{1, 2})
			dp.Negative().BucketCounts().FromRaw([]uint64{7})
			su := sm.Metrics().AppendEmpty()
			su.SetName("sum\x00mary")
			sp := su.SetEmptySummary().DataPoints().AppendEmpty()
			sp.SetTimestamp(ts)
			sp.SetCount(2)
			sp.SetSum(3.5)
			q := sp.QuantileValues().AppendEmpty()
			q.SetQuantile(0.99)
			q.SetValue(math.Inf(-1))
		}
	}
	return md
}

// leaves reads every leaf column of a Parquet file: path -> (schema line,
// values with levels).
func leaves(t *testing.T, b []byte) map[string][]string {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for i, path := range f.Schema().Columns() {
		leaf, _ := f.Schema().Lookup(path...)
		key := fmt.Sprint(path)
		out[key] = []string{fmt.Sprintf("%v %v r%d d%d", leaf.Node.Type().Kind(), leaf.Node.Type().LogicalType(),
			leaf.MaxRepetitionLevel, leaf.MaxDefinitionLevel)}
		for _, rg := range f.RowGroups() {
			pages := rg.ColumnChunks()[i].Pages()
			for {
				p, err := pages.ReadPage()
				if err != nil {
					break
				}
				vals := make([]parquet.Value, p.NumValues())
				n, _ := p.Values().ReadValues(vals)
				for _, v := range vals[:n] {
					out[key] = append(out[key], fmt.Sprintf("%d/%d/%x", v.RepetitionLevel(), v.DefinitionLevel(), v.Bytes()))
				}
			}
			pages.Close()
		}
	}
	return out
}

func TestPromotedEncoderWritesThePrototypesObjects(t *testing.T) {
	f := fleet.New(small())
	inputs := []pmetric.Metrics{f.EmitRound(), f.EmitRound(), hostile()}
	proto := New()
	promoted := parquetgo.NewSeriesEncoder(parquetgo.PrototypeSeriesOptions(), parquetgo.Options{})
	names := map[int]string{Gauge: parquetgo.SigGaugePoints, Sum: parquetgo.SigSumPoints,
		Histogram: parquetgo.SigHistogramPoints, ExpHistogram: parquetgo.SigExpHistogramPoints,
		Summary: parquetgo.SigSummaryPoints, NumTypes: parquetgo.SigSeries}
	objects, rows := 0, 0
	for i, md := range inputs {
		env := &Envelope{ProducerID: "p", Epoch: "e", BatchID: uint64(i), ReceivedAtNs: 42, SchemaVersion: 1}
		proto.Encode(md, env)
		var pb Buffers
		if err := proto.Flush(pb.Dst()); err != nil {
			t.Fatal(err)
		}
		batch, err := promoted.Walk(md)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]*parquetgo.SeriesObject{}
		for _, o := range batch.Objects {
			got[o.Signal] = o
		}
		for ty := 0; ty <= NumTypes; ty++ {
			o := got[names[ty]]
			if pb[ty].Len() == 0 {
				if o != nil {
					t.Fatalf("input %d: %s: an object the prototype doesn't write", i, names[ty])
				}
				continue
			}
			if o == nil {
				t.Fatalf("input %d: %s: no object", i, names[ty])
			}
			var buf bytes.Buffer
			if err := o.Encode(&buf, &parquetgo.Envelope{Producer: "p", Epoch: "e", Batch: uint64(i), Received: 42, Schema: 1}, nil); err != nil {
				t.Fatal(err)
			}
			want, have := leaves(t, pb[ty].Bytes()), leaves(t, buf.Bytes())
			if len(want) != len(have) {
				t.Fatalf("input %d %s: %d leaves, want %d", i, names[ty], len(have), len(want))
			}
			for path, w := range want {
				h, ok := have[path]
				if !ok {
					t.Fatalf("input %d %s: no leaf %s", i, names[ty], path)
				}
				if w[0] != h[0] {
					t.Fatalf("input %d %s %s: type %q, want %q", i, names[ty], path, h[0], w[0])
				}
				if fmt.Sprint(w) != fmt.Sprint(h) {
					for j := range min(len(w), len(h)) {
						if w[j] != h[j] {
							t.Fatalf("input %d %s %s: value %d: %s, want %s (%d vs %d values)", i, names[ty], path, j, h[j], w[j], len(h), len(w))
						}
					}
					t.Fatalf("input %d %s %s: %d values, want %d", i, names[ty], path, len(h), len(w))
				}
			}
			if n := len(want["[series_id]"]) - 1; n != o.Rows || n == 0 {
				t.Fatalf("input %d %s: read %d series_id values for %d rows", i, names[ty], n, o.Rows)
			}
			objects++
			rows += o.Rows
		}
		// Announce after "commit", as both callers must.
		proto.Announced()
		promoted.Announced(batch.New, "e")
		if len(batch.New) != len(proto.New) {
			t.Fatalf("input %d: %d new series, prototype %d", i, len(batch.New), len(proto.New))
		}
		batch.Release()
	}
	t.Logf("%d objects, %d rows identical", objects, rows)
}
