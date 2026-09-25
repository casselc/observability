package parquetgo

import (
	"encoding/hex"
	"io"
	"slices"
	"strings"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// The metric tables' columns, in the exporter's DDL order (METRICS_SCHEMA.md).
var (
	pgMetricCommon = []pgCol{
		{"ResourceAttributes", kMap}, {"ResourceSchemaUrl", kStr}, {"ScopeName", kStr}, {"ScopeVersion", kStr},
		{"ScopeAttributes", kMap}, {"ScopeDroppedAttrCount", kU32}, {"ScopeSchemaUrl", kStr}, {"ServiceName", kStr},
		{"MetricName", kStr}, {"MetricDescription", kStr}, {"MetricUnit", kStr}, {"Attributes", kMap},
		{"StartTimeUnix", kDT}, {"TimeUnix", kDT},
	}
	pgExemplars = []pgCol{
		{"Exemplars.FilteredAttributes", kListMap}, {"Exemplars.TimeUnix", kListDT}, {"Exemplars.Value", kListF64},
		{"Exemplars.SpanId", kListStr}, {"Exemplars.TraceId", kListStr},
	}
	pgMetricCols = [NumMetricTypes][]pgCol{
		MetricGauge: cat(pgMetricCommon, []pgCol{{"Value", kF64}, {"Flags", kU32}}, pgExemplars, pgEnvelope),
		MetricSum: cat(pgMetricCommon, []pgCol{{"Value", kF64}, {"Flags", kU32}}, pgExemplars,
			[]pgCol{{"AggregationTemporality", kI32}, {"IsMonotonic", kBool}}, pgEnvelope),
		MetricHistogram: cat(pgMetricCommon, []pgCol{{"Count", kU64}, {"Sum", kF64}, {"BucketCounts", kListU64},
			{"ExplicitBounds", kListF64}}, pgExemplars,
			[]pgCol{{"Flags", kU32}, {"Min", kF64}, {"Max", kF64}, {"AggregationTemporality", kI32}}, pgEnvelope),
		MetricExponentialHistogram: cat(pgMetricCommon, []pgCol{{"Count", kU64}, {"Sum", kF64}, {"Scale", kI32},
			{"ZeroCount", kU64}, {"PositiveOffset", kI32}, {"PositiveBucketCounts", kListU64}, {"NegativeOffset", kI32},
			{"NegativeBucketCounts", kListU64}}, pgExemplars,
			[]pgCol{{"Flags", kU32}, {"Min", kF64}, {"Max", kF64}, {"AggregationTemporality", kI32}}, pgEnvelope),
		MetricSummary: cat(pgMetricCommon, []pgCol{{"Count", kU64}, {"Sum", kF64},
			{"ValueAtQuantiles.Quantile", kListF64}, {"ValueAtQuantiles.Value", kListF64}, {"Flags", kU32}}, pgEnvelope),
	}
)

func cat(parts ...[]pgCol) []pgCol {
	var out []pgCol
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// MetricColumns returns the published column names of a metric type's
// object, envelope included, in order.
func MetricColumns(t MetricType) []string {
	cols := pgMetricCols[t]
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

func (e *PGEncoder) metricSignal(t MetricType) (*pgSignal, error) {
	if e.metrics[t] == nil {
		s, err := newPGSignal(pgMetricCols[t], e.opts)
		if err != nil {
			return nil, err
		}
		e.metrics[t] = s
	}
	return e.metrics[t], nil
}

// Metrics encodes md as up to five Parquet files, one per metric type, in a
// single walk. dst(t) is called only for types with data points, before
// anything is written to it; envs[t] (nil for no envelope) gives each file
// its own batch identity. rows[t] is the row count of type t's file (0: no
// file). The output is deterministic: the same md and envelopes give the
// same bytes.
func (e *PGEncoder) Metrics(dst func(MetricType) io.Writer, md pmetric.Metrics, envs *[NumMetricTypes]*Envelope) (rows [NumMetricTypes]int, err error) {
	n, err := MetricPoints(md)
	if err != nil {
		return rows, err
	}
	var ws [NumMetricTypes]metricWriter
	var sigs [NumMetricTypes]*pgSignal
	for t := range NumMetricTypes {
		if n[t] == 0 {
			continue
		}
		s, err := e.metricSignal(t)
		if err != nil {
			return rows, err
		}
		s.reset()
		sigs[t], ws[t] = s, s
	}
	if envs == nil {
		envs = &[NumMetricTypes]*Envelope{}
	}
	rows = writeMetrics(&ws, md, envs)
	for t, s := range sigs {
		if s == nil {
			continue
		}
		if err := s.flush(dst(MetricType(t))); err != nil {
			return rows, err
		}
		s.reset() // drop references into md
	}
	return rows, nil
}

// MetricsOf encodes only type t's data points of md: the path for a
// manifest-less log that has to re-encode one type's object for another
// slot. It writes an empty file when md has no points of type t.
func (e *PGEncoder) MetricsOf(dst io.Writer, md pmetric.Metrics, t MetricType, env *Envelope) (int, error) {
	if _, err := MetricPoints(md); err != nil {
		return 0, err
	}
	s, err := e.metricSignal(t)
	if err != nil {
		return 0, err
	}
	s.reset()
	var ws [NumMetricTypes]metricWriter
	ws[t] = s
	var envs [NumMetricTypes]*Envelope
	envs[t] = env
	rows := writeMetrics(&ws, md, &envs)
	err = s.flush(dst)
	s.reset()
	return rows[t], err
}

func (s *pgSignal) dt(ns uint64)                { s.put(parquet.Int64Value(dtMillis(ns))) }
func (s *pgSignal) f64(v float64)               { s.put(parquet.DoubleValue(v)) }
func (s *pgSignal) i32(v int32)                 { s.put(parquet.Int32Value(v)) }
func (s *pgSignal) boolean(v bool)              { s.put(parquet.BooleanValue(v)) }
func (s *pgSignal) hexTrace(id pcommon.TraceID) { s.hexAlways(id[:]) }
func (s *pgSignal) hexSpan(id pcommon.SpanID)   { s.hexAlways(id[:]) }

// attrKV is one attribute, for writing a map in sorted key order.
type attrKV struct {
	k string
	v pcommon.Value
}

// sortedKVs appends m's entries to dst with keys in byte order, as the
// exporter's clickhouse-go orderedmap.CollectN sorts them (slices.SortFunc
// with cmp.Compare). Duplicate keys, which only wire-decoded pdata can hold,
// keep their pdata order here; the exporter's unstable sort leaves theirs
// unspecified.
func sortedKVs(dst []attrKV, m pcommon.Map) []attrKV {
	kvs := dst[:0]
	sorted := true
	m.Range(func(k string, v pcommon.Value) bool {
		if n := len(kvs); n > 0 && kvs[n-1].k > k {
			sorted = false
		}
		kvs = append(kvs, attrKV{k, v})
		return true
	})
	if !sorted {
		slices.SortStableFunc(kvs, func(a, b attrKV) int { return strings.Compare(a.k, b.k) })
	}
	return kvs
}

// sortedAttrs writes m with its keys in byte order.
func (s *pgSignal) sortedAttrs(m pcommon.Map) {
	s.kvs = sortedKVs(s.kvs, m)
	s.kvAttrs(s.kvs)
}

// kvAttrs writes a map from entries already in order.
func (s *pgSignal) kvAttrs(kvs []attrKV) {
	mp, ok := s.mapStart(len(kvs))
	if !ok {
		return
	}
	for _, e := range kvs {
		s.mapEntry(&mp, e.k, e.v)
	}
}

// hexAlways is hex.EncodeToString, as the exporter renders exemplar ids:
// an all-zero id is zeros, not "".
func (s *pgSignal) hexAlways(id []byte) {
	n := hex.Encode(s.hex[:], id)
	s.put(parquet.ByteArrayValue(s.stash(s.hex[:n])))
}
