package parquetgo

import (
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// sampleMetrics has every metric type, with exemplars, unsorted attribute
// keys and several resources; the exhaustive data is in ./compare.
func sampleMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := 0; r < 3; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "svc")
		rm.Resource().Attributes().PutInt("a.first", int64(r))
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("scope")
		for i := 0; i < 40; i++ {
			ts := pcommon.Timestamp(1_700_000_000_000_000_000 + uint64(i)*1e9)
			switch i % 5 {
			case 0:
				dp := addMetric(sm, "g").SetEmptyGauge().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetDoubleValue(float64(i) / 3)
				dp.Attributes().PutStr("z", "1")
				dp.Attributes().PutBool("a", true)
				e := dp.Exemplars().AppendEmpty()
				e.SetIntValue(int64(i))
				e.SetTimestamp(ts)
			case 1:
				s := addMetric(sm, "s").SetEmptySum()
				s.SetIsMonotonic(true)
				s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := s.DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.SetIntValue(int64(i))
			case 2:
				dp := addMetric(sm, "h").SetEmptyHistogram().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.ExplicitBounds().FromRaw([]float64{1, 10})
				dp.BucketCounts().FromRaw([]uint64{1, 2, uint64(i)})
			case 3:
				dp := addMetric(sm, "e").SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				dp.Negative().BucketCounts().FromRaw([]uint64{0, 3})
				dp.SetScale(-2)
			case 4:
				dp := addMetric(sm, "q").SetEmptySummary().DataPoints().AppendEmpty()
				dp.SetTimestamp(ts)
				q := dp.QuantileValues().AppendEmpty()
				q.SetQuantile(0.5)
				q.SetValue(math.NaN())
			}
		}
	}
	return md
}

func addMetric(sm pmetric.ScopeMetrics, name string) pmetric.Metric {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	return m
}

func envsFor(batch uint64) *[NumMetricTypes]*Envelope {
	var envs [NumMetricTypes]*Envelope
	for t := range envs {
		envs[t] = &Envelope{Producer: "p", Epoch: "e", Batch: batch + uint64(t), Received: 7, Schema: 1}
	}
	return &envs
}

func encodeAll(t *testing.T, e *PGEncoder, md pmetric.Metrics, envs *[NumMetricTypes]*Envelope) ([NumMetricTypes][]byte, [NumMetricTypes]int) {
	t.Helper()
	var bufs [NumMetricTypes]bytes.Buffer
	rows, err := e.Metrics(func(t MetricType) io.Writer { return &bufs[t] }, md, envs)
	if err != nil {
		t.Fatal(err)
	}
	var out [NumMetricTypes][]byte
	for i := range bufs {
		out[i] = bufs[i].Bytes()
	}
	return out, rows
}

// TestMetricsDeterministic: the same pdata and envelopes give the same bytes
// (so a retry is byte-identical), from a fresh or a reused encoder, serial or
// parallel, and encoding one type alone (MetricsOf) gives the same file as
// the combined walk.
func TestMetricsDeterministic(t *testing.T) {
	md := sampleMetrics()
	a, rows := encodeAll(t, NewPGEncoder(DefaultOptions()), md, envsFor(1))
	for mt, n := range rows {
		if n != 24 {
			t.Fatalf("type %d: %d rows, want 24", mt, n)
		}
	}
	reused := NewPGEncoder(DefaultOptions())
	encodeAll(t, reused, sampleMetrics(), envsFor(9))
	b, _ := encodeAll(t, reused, md, envsFor(1))
	par := DefaultOptions()
	par.Parallelism = 4
	c, _ := encodeAll(t, NewPGEncoder(par), md, envsFor(1))
	for mt := range NumMetricTypes {
		if !bytes.Equal(a[mt], b[mt]) || !bytes.Equal(a[mt], c[mt]) {
			t.Errorf("%s: output differs between runs", MetricType(mt))
		}
		var one bytes.Buffer
		n, err := reused.MetricsOf(&one, md, MetricType(mt), envsFor(1)[mt])
		if err != nil || n != rows[mt] || !bytes.Equal(one.Bytes(), a[mt]) {
			t.Errorf("%s: MetricsOf differs from Metrics (%d rows, %v)", MetricType(mt), n, err)
		}
	}
}

// TestMetricsSchema reads a file back with parquet-go and checks the
// column order and the leaf types METRICS_SCHEMA.md promises.
func TestMetricsSchema(t *testing.T) {
	out, rows := encodeAll(t, NewPGEncoder(DefaultOptions()), sampleMetrics(), envsFor(1))
	for mt := range NumMetricTypes {
		f, err := parquet.OpenFile(bytes.NewReader(out[mt]), int64(len(out[mt])))
		if err != nil {
			t.Fatal(err)
		}
		if f.NumRows() != int64(rows[mt]) || len(f.RowGroups()) != 1 {
			t.Errorf("%s: %d rows in %d row groups", MetricType(mt), f.NumRows(), len(f.RowGroups()))
		}
		fields := f.Schema().Fields()
		want := MetricColumns(MetricType(mt))
		if len(fields) != len(want) {
			t.Fatalf("%s: %d columns, want %d", MetricType(mt), len(fields), len(want))
		}
		for i, fl := range fields {
			if fl.Name() != want[i] || fl.Optional() {
				t.Errorf("%s column %d: %s (optional %v), want required %s", MetricType(mt), i, fl.Name(), fl.Optional(), want[i])
			}
		}
		lt := func(path ...string) string {
			c, ok := f.Schema().Lookup(path...)
			if !ok {
				t.Fatalf("%s: no %v", MetricType(mt), path)
			}
			return c.Node.Type().String()
		}
		if got := lt("TimeUnix"); got != "TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)" {
			t.Errorf("TimeUnix: %s", got)
		}
		if mt == MetricSum {
			if got := lt("IsMonotonic"); got != "BOOLEAN" {
				t.Errorf("IsMonotonic: %s", got)
			}
			if got := lt("AggregationTemporality"); got != "INT(32,true)" {
				t.Errorf("AggregationTemporality: %s", got)
			}
		}
	}
}

func TestDTMillis(t *testing.T) {
	for _, c := range []struct {
		ns   uint64
		want int64
	}{
		{0, 0},
		{999_999_999, 0},
		{1_700_000_000_999_999_999, 1_700_000_000_000},
		{math.MaxInt64, 633_437_444_000},
		{1 << 63, 3_661_529_851_000},
		{math.MaxUint64, int64(math.MaxUint32) * 1000},
	} {
		// The reference: what the exporter's time.Time goes through.
		ref := int64(uint32(pcommon.Timestamp(c.ns).AsTime().Unix())) * 1000
		if got := dtMillis(c.ns); got != c.want || got != ref {
			t.Errorf("dtMillis(%d) = %d, want %d (reference %d)", c.ns, got, c.want, ref)
		}
	}
}

// TestPushMetricsLocal publishes to a directory: one object and manifest per
// type, under the metrics_* signals; an arrow publisher refuses metrics.
func TestPushMetricsLocal(t *testing.T) {
	dir := t.TempDir()
	p, err := New(Config{URL: "file://" + dir, ProducerID: "p", Region: "r", SchemaVersion: 1, Epoch: "e",
		Engine: "parquet-go", Now: func() time.Time { return time.Unix(1_700_000_000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := p.PushMetrics(ctx, sampleMetrics()); err != nil {
		t.Fatal(err)
	}
	only := pmetric.NewMetrics()
	dp := addMetric(only.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty(), "g").SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	if err := p.PushMetrics(ctx, only); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for mt, sig := range MetricSignals {
		objs, _ := filepath.Glob(filepath.Join(dir, "r", sig, "v1", "p", "e", "g*", "*.parquet"))
		mans, _ := filepath.Glob(filepath.Join(dir, "r", sig, "v1", "p", "e", "manifests", "g*", "0*.json"))
		want := 1
		if MetricType(mt) == MetricGauge {
			want = 2
		}
		if len(objs) != want || len(mans) != want {
			t.Errorf("%s: %d objects, %d manifests, want %d", sig, len(objs), len(mans), want)
		}
		if _, err := os.Stat(filepath.Join(dir, "r", sig, "v1", "p", "e", "manifests")); err != nil {
			t.Error(err)
		}
	}
	bad := pmetric.NewMetrics()
	bad.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	if err := p.PushMetrics(ctx, bad); err != ErrMetricTypeUnset {
		t.Errorf("empty metric type: %v", err)
	}
	if newArrowEncoder != nil {
		pa, err := New(Config{URL: "file://" + dir, ProducerID: "p", Region: "r", Engine: "arrow"})
		if err != nil {
			t.Fatal(err)
		}
		if err := pa.PushMetrics(ctx, sampleMetrics()); err == nil {
			t.Error("arrow engine published metrics")
		}
	}
}

// TestReusedWriterIdentical: a reused encoder writes the same bytes as a
// fresh one for traces and logs too (parquet-go's Writer.Reset zeroes the
// column paths unless restoreColumnPaths puts them back).
func TestReusedWriterIdentical(t *testing.T) {
	enc := func(e *PGEncoder) []byte {
		var b bytes.Buffer
		if _, err := e.Traces(&b, sampleTracesPG(), &Envelope{Batch: 1}); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	reused := NewPGEncoder(DefaultOptions())
	enc(reused)
	if !bytes.Equal(enc(reused), enc(NewPGEncoder(DefaultOptions()))) {
		t.Fatal("a reused encoder's file differs from a fresh one's")
	}
	if !reused.traces.reusable {
		t.Fatal("restoreColumnPaths failed: every file now pays for a new parquet writer")
	}
}

func sampleTracesPG() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 50; i++ {
		s := ss.Spans().AppendEmpty()
		s.SetName("op")
		s.SetStartTimestamp(pcommon.Timestamp(1_000_000_000 + i))
		s.Attributes().PutInt("i", int64(i))
		s.Events().AppendEmpty().Attributes().PutStr("e", "v")
	}
	return td
}
