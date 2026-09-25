package parquetgo

import (
	"errors"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Metrics: one table per metric type, as the contrib clickhouseexporter
// v0.161.0 writes them (internal/metrics/*_metrics.go and
// internal/sqltemplates/metrics_*_table.sql). METRICS_SCHEMA.md is the
// column-by-column contract; this walker is its implementation.

// MetricType indexes the five metric tables.
type MetricType int

const (
	MetricGauge MetricType = iota
	MetricSum
	MetricHistogram
	MetricExponentialHistogram
	MetricSummary
	NumMetricTypes
)

// MetricSignals are the signal namespaces (and, prefixed with "otel_", the
// table names) of the metric types.
var MetricSignals = [NumMetricTypes]string{
	"metrics_gauge", "metrics_sum", "metrics_histogram", "metrics_exponential_histogram", "metrics_summary",
}

func (t MetricType) String() string { return MetricSignals[t] }

func metricTypeOf(t pmetric.MetricType) (MetricType, bool) {
	switch t {
	case pmetric.MetricTypeGauge:
		return MetricGauge, true
	case pmetric.MetricTypeSum:
		return MetricSum, true
	case pmetric.MetricTypeHistogram:
		return MetricHistogram, true
	case pmetric.MetricTypeExponentialHistogram:
		return MetricExponentialHistogram, true
	case pmetric.MetricTypeSummary:
		return MetricSummary, true
	}
	return 0, false
}

// ErrMetricTypeUnset is returned for a request holding a metric with no
// type; the clickhouse exporter rejects the whole request the same way.
var ErrMetricTypeUnset = errors.New("metrics type is unset")

// MetricPoints counts data points per type, which is the row count of each
// type's object; a zero means no object. It fails like the exporter on a
// metric of type Empty.
func MetricPoints(md pmetric.Metrics) ([NumMetricTypes]int, error) {
	var n [NumMetricTypes]int
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				t, ok := metricTypeOf(m.Type())
				if !ok {
					return n, ErrMetricTypeUnset
				}
				switch t {
				case MetricGauge:
					n[t] += m.Gauge().DataPoints().Len()
				case MetricSum:
					n[t] += m.Sum().DataPoints().Len()
				case MetricHistogram:
					n[t] += m.Histogram().DataPoints().Len()
				case MetricExponentialHistogram:
					n[t] += m.ExponentialHistogram().DataPoints().Len()
				case MetricSummary:
					n[t] += m.Summary().DataPoints().Len()
				}
			}
		}
	}
	return n, nil
}

// metricWriter is rowWriter plus the value kinds only metrics use.
type metricWriter interface {
	rowWriter
	dt(ns uint64) // DateTime: whole seconds, clickhouse-go's conversion
	f64(v float64)
	i32(v int32)
	boolean(v bool)
	hexTrace(id pcommon.TraceID) // exemplar ids: always hex, zero included
	hexSpan(id pcommon.SpanID)
	sortedAttrs(m pcommon.Map) // a map with keys in byte order, as the exporter sends them
	kvAttrs(kvs []attrKV)      // a map from entries already sorted (sortedKVs)
}

// dtMillis is what a DateTime column ends up holding for a pdata timestamp,
// in the milliseconds ClickHouse's Parquet writer uses for DateTime. The
// exporter passes Timestamp.AsTime() (time.Unix(0, int64(ns))) and ch-go
// stores uint32(t.Unix()): signed, floor-divided, wrapped mod 2^32.
func dtMillis(ns uint64) int64 {
	n := int64(ns)
	s := n / 1e9
	if n%1e9 < 0 {
		s--
	}
	return int64(uint32(s)) * 1000
}

// numberValue is the exporter's getValue for a data point.
func numberValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	}
	return 0
}

func exemplarValue(e pmetric.Exemplar) float64 {
	switch e.ValueType() {
	case pmetric.ExemplarValueTypeDouble:
		return e.DoubleValue()
	case pmetric.ExemplarValueTypeInt:
		return float64(e.IntValue())
	}
	return 0
}

// metricScope is what every row of one ScopeMetrics shares.
type metricScope struct {
	res, scopeAttrs  []attrKV // sorted once per resource / scope, not per row
	resURL, scopeURL string
	scope            pcommon.InstrumentationScope
	svc              string
}

func (ms *metricScope) write(w metricWriter, m pmetric.Metric, attrs pcommon.Map, start, ts uint64) {
	w.row()
	w.kvAttrs(ms.res)
	w.str(ms.resURL)
	w.str(ms.scope.Name())
	w.str(ms.scope.Version())
	w.kvAttrs(ms.scopeAttrs)
	w.u32(ms.scope.DroppedAttributesCount())
	w.str(ms.scopeURL)
	w.str(ms.svc)
	w.str(m.Name())
	w.str(m.Description())
	w.str(m.Unit())
	w.sortedAttrs(attrs)
	w.dt(start)
	w.dt(ts)
}

func writeExemplars(w metricWriter, ex pmetric.ExemplarSlice) {
	n := ex.Len()
	w.arr(n)
	for i := 0; i < n; i++ {
		w.sortedAttrs(ex.At(i).FilteredAttributes())
	}
	w.end()
	w.arr(n)
	for i := 0; i < n; i++ {
		w.dt(uint64(ex.At(i).Timestamp()))
	}
	w.end()
	w.arr(n)
	for i := 0; i < n; i++ {
		w.f64(exemplarValue(ex.At(i)))
	}
	w.end()
	w.arr(n)
	for i := 0; i < n; i++ {
		w.hexSpan(ex.At(i).SpanID())
	}
	w.end()
	w.arr(n)
	for i := 0; i < n; i++ {
		w.hexTrace(ex.At(i).TraceID())
	}
	w.end()
}

func writeU64s(w metricWriter, s pcommon.UInt64Slice) {
	w.arr(s.Len())
	for i := 0; i < s.Len(); i++ {
		w.u64(s.At(i))
	}
	w.end()
}

func writeF64s(w metricWriter, s pcommon.Float64Slice) {
	w.arr(s.Len())
	for i := 0; i < s.Len(); i++ {
		w.f64(s.At(i))
	}
	w.end()
}

// writeMetrics walks md once and writes each data point to the writer of
// its type; a nil writer skips that type. rows[t] counts what was written,
// and envs[t] (if the writer is set) gets the envelope columns.
func writeMetrics(ws *[NumMetricTypes]metricWriter, md pmetric.Metrics, envs *[NumMetricTypes]*Envelope) (rows [NumMetricTypes]int) {
	endRow := func(t MetricType, ts uint64) {
		w := ws[t]
		if env := envs[t]; env != nil {
			env.write(w, rows[t], ts)
		}
		w.endRow()
		rows[t]++
	}
	var ms metricScope
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := rm.Resource().Attributes()
		ms.res, ms.resURL, ms.svc = sortedKVs(ms.res, res), rm.SchemaUrl(), serviceName(res)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms.scope, ms.scopeURL = sm.Scope(), sm.SchemaUrl()
			ms.scopeAttrs = sortedKVs(ms.scopeAttrs, sm.Scope().Attributes())
			mets := sm.Metrics()
			for k := 0; k < mets.Len(); k++ {
				m := mets.At(k)
				t, ok := metricTypeOf(m.Type())
				if !ok || ws[t] == nil {
					continue
				}
				w := ws[t]
				switch t {
				case MetricGauge:
					dps := m.Gauge().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ms.write(w, m, dp.Attributes(), uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						w.f64(numberValue(dp))
						w.u32(uint32(dp.Flags()))
						writeExemplars(w, dp.Exemplars())
						endRow(t, uint64(dp.Timestamp()))
					}
				case MetricSum:
					sum := m.Sum()
					temp, mono := int32(sum.AggregationTemporality()), sum.IsMonotonic()
					dps := sum.DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ms.write(w, m, dp.Attributes(), uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						w.f64(numberValue(dp))
						w.u32(uint32(dp.Flags()))
						writeExemplars(w, dp.Exemplars())
						w.i32(temp)
						w.boolean(mono)
						endRow(t, uint64(dp.Timestamp()))
					}
				case MetricHistogram:
					h := m.Histogram()
					temp := int32(h.AggregationTemporality())
					dps := h.DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ms.write(w, m, dp.Attributes(), uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						w.u64(dp.Count())
						w.f64(dp.Sum())
						writeU64s(w, dp.BucketCounts())
						writeF64s(w, dp.ExplicitBounds())
						writeExemplars(w, dp.Exemplars())
						w.u32(uint32(dp.Flags()))
						w.f64(dp.Min())
						w.f64(dp.Max())
						w.i32(temp)
						endRow(t, uint64(dp.Timestamp()))
					}
				case MetricExponentialHistogram:
					h := m.ExponentialHistogram()
					temp := int32(h.AggregationTemporality())
					dps := h.DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ms.write(w, m, dp.Attributes(), uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						w.u64(dp.Count())
						w.f64(dp.Sum())
						w.i32(dp.Scale())
						w.u64(dp.ZeroCount())
						w.i32(dp.Positive().Offset())
						writeU64s(w, dp.Positive().BucketCounts())
						w.i32(dp.Negative().Offset())
						writeU64s(w, dp.Negative().BucketCounts())
						writeExemplars(w, dp.Exemplars())
						w.u32(uint32(dp.Flags()))
						w.f64(dp.Min())
						w.f64(dp.Max())
						w.i32(temp)
						endRow(t, uint64(dp.Timestamp()))
					}
				case MetricSummary:
					dps := m.Summary().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ms.write(w, m, dp.Attributes(), uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						w.u64(dp.Count())
						w.f64(dp.Sum())
						q := dp.QuantileValues()
						w.arr(q.Len())
						for x := 0; x < q.Len(); x++ {
							w.f64(q.At(x).Quantile())
						}
						w.end()
						w.arr(q.Len())
						for x := 0; x < q.Len(); x++ {
							w.f64(q.At(x).Value())
						}
						w.end()
						w.u32(uint32(dp.Flags()))
						endRow(t, uint64(dp.Timestamp()))
					}
				}
			}
		}
	}
	return rows
}
