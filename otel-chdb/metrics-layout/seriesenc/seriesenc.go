// Package seriesenc is the edge half of layout B: it encodes one OTLP
// metrics request as narrow per-type "points" Parquet objects keyed by a
// series id computed at the edge, plus one "series" object that carries
// the attribute maps of the series this edge has not announced yet in the
// current cache window.
//
// Series id (v1). The id is xxh3-64 over a length-prefixed canonical
// encoding, in two levels so that the resource and scope part is hashed once
// per ScopeMetrics:
//
//	h_rs      = xxh3_64( res_kvs, ResourceSchemaUrl, ScopeName, ScopeVersion,
//	                     scope_kvs, ScopeDroppedAttrCount, ScopeSchemaUrl )
//	series_id = xxh3_64( le64(h_rs), MetricType, MetricName, MetricDescription,
//	                     MetricUnit, AggregationTemporality, IsMonotonic,
//	                     point_kvs, ExplicitBounds )
//
// where every string is uvarint(len) || bytes, kvs are the map's entries
// sorted by key (byte order), each value tagged with its pdata type ('s'
// string bytes, 'i'/'d' 8 little-endian bytes, 'b' one byte, else 'x' and
// AsString()). Everything the contrib row holds except the per-point fields
// is in the id, so the series row plus the points row reproduce the contrib
// row exactly (the compatibility view relies on it).
//
// Series cache. A series is announced (written to the series object) the
// first time it is seen in a cache window: the hour of the point's
// TimeUnix. An Encoder is one producer epoch: a new epoch starts with an
// empty cache. The caller must call Announced only after the series object
// has committed; until then the series stays unannounced and the next
// request announces it again.
package seriesenc

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"slices"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Metric types, parquetgo's order.
const (
	Gauge = iota
	Sum
	Histogram
	ExpHistogram
	Summary
	NumTypes
)

// TypeNames are the table suffixes.
var TypeNames = [NumTypes]string{"gauge", "sum", "histogram", "exponential_histogram", "summary"}

// Envelope is the batch identity every object carries (as parquetgo's).
type Envelope struct {
	ProducerID, Epoch string
	BatchID           uint64
	ReceivedAtNs      int64
	SchemaVersion     uint16
}

// NumberRow is a gauge or sum point.
type NumberRow struct {
	MetricName    string    `parquet:"MetricName,dict"`
	ServiceName   string    `parquet:"ServiceName,dict"`
	SeriesID      uint64    `parquet:"series_id"`
	StartTimeUnix uint32    `parquet:"StartTimeUnix,dict"`
	TimeUnix      uint32    `parquet:"TimeUnix,dict"`
	Value         float64   `parquet:"Value"`
	Flags         uint32    `parquet:"Flags,dict"`
	ExTime        []uint32  `parquet:"Exemplars.TimeUnix,list"`
	ExValue       []float64 `parquet:"Exemplars.Value,list"`
	ExSpan        []string  `parquet:"Exemplars.SpanId,list"`
	ExTrace       []string  `parquet:"Exemplars.TraceId,list"`
	ProducerID    string    `parquet:"producer_id,dict"`
	Epoch         string    `parquet:"producer_epoch,dict"`
	BatchID       uint64    `parquet:"batch_id,dict"`
	RowOrdinal    uint32    `parquet:"row_ordinal,delta"`
	ReceivedAt    int64     `parquet:"received_at,dict,timestamp(nanosecond)"`
	SchemaVersion uint16    `parquet:"schema_version,dict"`
}

// HistRow is an explicit-bucket histogram point (bounds live in the series).
type HistRow struct {
	MetricName    string    `parquet:"MetricName,dict"`
	ServiceName   string    `parquet:"ServiceName,dict"`
	SeriesID      uint64    `parquet:"series_id"`
	StartTimeUnix uint32    `parquet:"StartTimeUnix,dict"`
	TimeUnix      uint32    `parquet:"TimeUnix,dict"`
	Count         uint64    `parquet:"Count"`
	Sum           float64   `parquet:"Sum"`
	BucketCounts  []uint64  `parquet:"BucketCounts,list"`
	Min           float64   `parquet:"Min"`
	Max           float64   `parquet:"Max"`
	Flags         uint32    `parquet:"Flags,dict"`
	ExTime        []uint32  `parquet:"Exemplars.TimeUnix,list"`
	ExValue       []float64 `parquet:"Exemplars.Value,list"`
	ExSpan        []string  `parquet:"Exemplars.SpanId,list"`
	ExTrace       []string  `parquet:"Exemplars.TraceId,list"`
	ProducerID    string    `parquet:"producer_id,dict"`
	Epoch         string    `parquet:"producer_epoch,dict"`
	BatchID       uint64    `parquet:"batch_id,dict"`
	RowOrdinal    uint32    `parquet:"row_ordinal,delta"`
	ReceivedAt    int64     `parquet:"received_at,dict,timestamp(nanosecond)"`
	SchemaVersion uint16    `parquet:"schema_version,dict"`
}

// ExpRow is an exponential histogram point.
type ExpRow struct {
	MetricName     string    `parquet:"MetricName,dict"`
	ServiceName    string    `parquet:"ServiceName,dict"`
	SeriesID       uint64    `parquet:"series_id"`
	StartTimeUnix  uint32    `parquet:"StartTimeUnix,dict"`
	TimeUnix       uint32    `parquet:"TimeUnix,dict"`
	Count          uint64    `parquet:"Count"`
	Sum            float64   `parquet:"Sum"`
	Scale          int32     `parquet:"Scale,dict"`
	ZeroCount      uint64    `parquet:"ZeroCount"`
	PositiveOffset int32     `parquet:"PositiveOffset"`
	PositiveCounts []uint64  `parquet:"PositiveBucketCounts,list"`
	NegativeOffset int32     `parquet:"NegativeOffset"`
	NegativeCounts []uint64  `parquet:"NegativeBucketCounts,list"`
	Min            float64   `parquet:"Min"`
	Max            float64   `parquet:"Max"`
	Flags          uint32    `parquet:"Flags,dict"`
	ExTime         []uint32  `parquet:"Exemplars.TimeUnix,list"`
	ExValue        []float64 `parquet:"Exemplars.Value,list"`
	ExSpan         []string  `parquet:"Exemplars.SpanId,list"`
	ExTrace        []string  `parquet:"Exemplars.TraceId,list"`
	ProducerID     string    `parquet:"producer_id,dict"`
	Epoch          string    `parquet:"producer_epoch,dict"`
	BatchID        uint64    `parquet:"batch_id,dict"`
	RowOrdinal     uint32    `parquet:"row_ordinal,delta"`
	ReceivedAt     int64     `parquet:"received_at,dict,timestamp(nanosecond)"`
	SchemaVersion  uint16    `parquet:"schema_version,dict"`
}

// SummaryRow is a summary point.
type SummaryRow struct {
	MetricName    string    `parquet:"MetricName,dict"`
	ServiceName   string    `parquet:"ServiceName,dict"`
	SeriesID      uint64    `parquet:"series_id"`
	StartTimeUnix uint32    `parquet:"StartTimeUnix,dict"`
	TimeUnix      uint32    `parquet:"TimeUnix,dict"`
	Count         uint64    `parquet:"Count"`
	Sum           float64   `parquet:"Sum"`
	Quantile      []float64 `parquet:"ValueAtQuantiles.Quantile,list"`
	QValue        []float64 `parquet:"ValueAtQuantiles.Value,list"`
	Flags         uint32    `parquet:"Flags,dict"`
	ProducerID    string    `parquet:"producer_id,dict"`
	Epoch         string    `parquet:"producer_epoch,dict"`
	BatchID       uint64    `parquet:"batch_id,dict"`
	RowOrdinal    uint32    `parquet:"row_ordinal,delta"`
	ReceivedAt    int64     `parquet:"received_at,dict,timestamp(nanosecond)"`
	SchemaVersion uint16    `parquet:"schema_version,dict"`
}

// SeriesRow is one announced series: everything of the contrib row that is
// not per point. Maps are key and value arrays (central builds the Map with
// mapFromArrays); keys are sorted, values are AsString(), as the exporter
// renders them.
type SeriesRow struct {
	SeriesID               uint64    `parquet:"series_id"`
	MetricType             uint8     `parquet:"MetricType,dict,uint(8)"`
	MetricName             string    `parquet:"MetricName,dict"`
	MetricDescription      string    `parquet:"MetricDescription,dict"`
	MetricUnit             string    `parquet:"MetricUnit,dict"`
	ServiceName            string    `parquet:"ServiceName,dict"`
	ResKeys                []string  `parquet:"ResourceAttributesKeys,list"`
	ResVals                []string  `parquet:"ResourceAttributesValues,list"`
	ResourceSchemaUrl      string    `parquet:"ResourceSchemaUrl,dict"`
	ScopeName              string    `parquet:"ScopeName,dict"`
	ScopeVersion           string    `parquet:"ScopeVersion,dict"`
	ScopeKeys              []string  `parquet:"ScopeAttributesKeys,list"`
	ScopeVals              []string  `parquet:"ScopeAttributesValues,list"`
	ScopeDroppedAttrCount  uint32    `parquet:"ScopeDroppedAttrCount,dict"`
	ScopeSchemaUrl         string    `parquet:"ScopeSchemaUrl,dict"`
	AttrKeys               []string  `parquet:"AttributesKeys,list"`
	AttrVals               []string  `parquet:"AttributesValues,list"`
	AggregationTemporality int32     `parquet:"AggregationTemporality,dict"`
	IsMonotonic            bool      `parquet:"IsMonotonic"`
	ExplicitBounds         []float64 `parquet:"ExplicitBounds,list"`
	FirstSeen              uint32    `parquet:"FirstSeen"`
	ProducerID             string    `parquet:"producer_id,dict"`
	Epoch                  string    `parquet:"producer_epoch,dict"`
	BatchID                uint64    `parquet:"batch_id,dict"`
	RowOrdinal             uint32    `parquet:"row_ordinal,delta"`
	ReceivedAt             int64     `parquet:"received_at,dict,timestamp(nanosecond)"`
	SchemaVersion          uint16    `parquet:"schema_version,dict"`
}

// Encoder is one producer epoch's encoder. Not safe for concurrent use.
type Encoder struct {
	cache map[uint64]int32 // series id -> cache window it was announced in

	num  [2][]NumberRow
	hist []HistRow
	exp  []ExpRow
	sum  []SummaryRow
	ser  []SeriesRow
	seen map[uint64]struct{} // announced by this request (dedups within it)
	New  []uint64            // the series ids the last Encode announced
	win  []int32

	wNum  [2]*parquet.GenericWriter[NumberRow]
	wHist *parquet.GenericWriter[HistRow]
	wExp  *parquet.GenericWriter[ExpRow]
	wSum  *parquet.GenericWriter[SummaryRow]
	wSer  *parquet.GenericWriter[SeriesRow]

	buf  []byte
	kvs  []kvp
	hex  []byte
	opts []parquet.WriterOption
	// WindowSeconds is the cache window (default 3600).
	WindowSeconds int64
}

type kvp struct {
	k string
	v pcommon.Value
}

// New returns an encoder with an empty series cache.
func New() *Encoder {
	return &Encoder{cache: map[uint64]int32{}, seen: map[uint64]struct{}{}, WindowSeconds: 3600,
		opts: []parquet.WriterOption{parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}),
			parquet.PageBufferSize(1 << 20)}}
}

// CacheLen is the number of cached series.
func (e *Encoder) CacheLen() int { return len(e.cache) }

// Announced marks the series of the last Encode as announced. Call it only
// once the series object has committed.
func (e *Encoder) Announced() {
	for i, id := range e.New {
		e.cache[id] = e.win[i]
	}
}

// dt is the exporter's DateTime conversion (seconds, signed floor, mod 2^32).
func dt(ns pcommon.Timestamp) uint32 {
	n := int64(ns)
	s := n / 1e9
	if n%1e9 < 0 {
		s--
	}
	return uint32(s)
}

func (e *Encoder) putStr(s string) {
	e.buf = binary.AppendUvarint(e.buf, uint64(len(s)))
	e.buf = append(e.buf, s...)
}

func (e *Encoder) putVal(v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		e.buf = append(e.buf, 's')
		e.putStr(v.Str())
	case pcommon.ValueTypeInt:
		e.buf = append(e.buf, 'i')
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		e.buf = append(e.buf, 'd')
		e.buf = binary.LittleEndian.AppendUint64(e.buf, math.Float64bits(v.Double()))
	case pcommon.ValueTypeBool:
		b := byte(0)
		if v.Bool() {
			b = 1
		}
		e.buf = append(e.buf, 'b', b)
	default:
		e.buf = append(e.buf, 'x')
		e.putStr(v.AsString())
	}
}

// sorted fills e.kvs with m's entries in key order (stable for duplicates).
func (e *Encoder) sorted(m pcommon.Map) []kvp {
	kvs := e.kvs[:0]
	ok := true
	m.Range(func(k string, v pcommon.Value) bool {
		if n := len(kvs); n > 0 && kvs[n-1].k > k {
			ok = false
		}
		kvs = append(kvs, kvp{k, v})
		return true
	})
	if !ok {
		slices.SortStableFunc(kvs, func(a, b kvp) int { return strings.Compare(a.k, b.k) })
	}
	e.kvs = kvs
	return kvs
}

func (e *Encoder) putKVs(kvs []kvp) {
	e.buf = binary.AppendUvarint(e.buf, uint64(len(kvs)))
	for _, a := range kvs {
		e.putStr(a.k)
		e.putVal(a.v)
	}
}

func render(kvs []kvp) (keys, vals []string) {
	keys, vals = make([]string, len(kvs)), make([]string, len(kvs))
	for i, a := range kvs {
		keys[i], vals[i] = a.k, a.v.AsString()
	}
	return
}

type scopeCtx struct {
	h                            uint64
	svc                          string
	resK, resV, scK, scV         []string
	resURL, scName, scVer, scURL string
	dropped                      uint32
	name, desc, unit             string
	typ                          uint8
	temp                         int32
	mono                         bool
	metricOff                    int // len(e.buf) after the metric part
}

// Encode walks md and fills the per-type point rows and the series rows.
// Objects are written with Flush. rows[t] is the number of points of type t.
func (e *Encoder) Encode(md pmetric.Metrics, env *Envelope) (rows [NumTypes]int) {
	e.num[0], e.num[1] = e.num[0][:0], e.num[1][:0]
	e.hist, e.exp, e.sum, e.ser = e.hist[:0], e.exp[:0], e.sum[:0], e.ser[:0]
	e.New, e.win = e.New[:0], e.win[:0]
	clear(e.seen)
	var sc scopeCtx
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := rm.Resource().Attributes()
		resKVs := slices.Clone(e.sorted(res))
		sc.resK, sc.resV = nil, nil
		sc.svc = ""
		if v, ok := res.Get("service.name"); ok {
			sc.svc = v.AsString()
		}
		sc.resURL = rm.SchemaUrl()
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			sc.scName, sc.scVer, sc.scURL, sc.dropped = scope.Name(), scope.Version(), sm.SchemaUrl(), scope.DroppedAttributesCount()
			scKVs := e.sorted(scope.Attributes())
			e.buf = e.buf[:0]
			e.putKVs(resKVs)
			e.putStr(sc.resURL)
			e.putStr(sc.scName)
			e.putStr(sc.scVer)
			e.putKVs(scKVs)
			e.buf = binary.LittleEndian.AppendUint32(e.buf, sc.dropped)
			e.putStr(sc.scURL)
			sc.h = xxh3.Hash(e.buf)
			sc.scK, sc.scV = nil, nil
			scKVsCopy := slices.Clone(scKVs)
			mets := sm.Metrics()
			for k := 0; k < mets.Len(); k++ {
				m := mets.At(k)
				sc.name, sc.desc, sc.unit, sc.temp, sc.mono = m.Name(), m.Description(), m.Unit(), 0, false
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					sc.typ = Gauge
				case pmetric.MetricTypeSum:
					sc.typ = Sum
					sc.temp, sc.mono = int32(m.Sum().AggregationTemporality()), m.Sum().IsMonotonic()
				case pmetric.MetricTypeHistogram:
					sc.typ = Histogram
					sc.temp = int32(m.Histogram().AggregationTemporality())
				case pmetric.MetricTypeExponentialHistogram:
					sc.typ = ExpHistogram
					sc.temp = int32(m.ExponentialHistogram().AggregationTemporality())
				case pmetric.MetricTypeSummary:
					sc.typ = Summary
				default:
					continue
				}
				e.buf = e.buf[:0]
				e.buf = binary.LittleEndian.AppendUint64(e.buf, sc.h)
				e.buf = append(e.buf, sc.typ)
				e.putStr(sc.name)
				e.putStr(sc.desc)
				e.putStr(sc.unit)
				e.buf = binary.LittleEndian.AppendUint32(e.buf, uint32(sc.temp))
				if sc.mono {
					e.buf = append(e.buf, 1)
				} else {
					e.buf = append(e.buf, 0)
				}
				sc.metricOff = len(e.buf)
				// id computes the series id of a point and announces it if new.
				id := func(attrs pcommon.Map, bounds pcommon.Float64Slice, ts uint32) uint64 {
					e.buf = e.buf[:sc.metricOff]
					kvs := e.sorted(attrs)
					e.putKVs(kvs)
					e.buf = binary.AppendUvarint(e.buf, uint64(bounds.Len()))
					for b := 0; b < bounds.Len(); b++ {
						e.buf = binary.LittleEndian.AppendUint64(e.buf, math.Float64bits(bounds.At(b)))
					}
					h := xxh3.Hash(e.buf)
					w := int32(int64(ts) / e.WindowSeconds)
					if cw, ok := e.cache[h]; ok && cw == w {
						return h
					}
					if _, ok := e.seen[h]; ok {
						return h
					}
					e.seen[h] = struct{}{}
					e.New = append(e.New, h)
					e.win = append(e.win, w)
					if sc.resK == nil {
						sc.resK, sc.resV = render(resKVs)
						sc.scK, sc.scV = render(scKVsCopy)
					}
					ak, av := render(kvs)
					row := SeriesRow{SeriesID: h, MetricType: sc.typ, MetricName: sc.name, MetricDescription: sc.desc,
						MetricUnit: sc.unit, ServiceName: sc.svc, ResKeys: sc.resK, ResVals: sc.resV, ResourceSchemaUrl: sc.resURL,
						ScopeName: sc.scName, ScopeVersion: sc.scVer, ScopeKeys: sc.scK, ScopeVals: sc.scV,
						ScopeDroppedAttrCount: sc.dropped, ScopeSchemaUrl: sc.scURL, AttrKeys: ak, AttrVals: av,
						AggregationTemporality: sc.temp, IsMonotonic: sc.mono, FirstSeen: ts}
					if bounds.Len() > 0 {
						row.ExplicitBounds = bounds.AsRaw()
					}
					e.ser = append(e.ser, row)
					return h
				}
				var noBounds pcommon.Float64Slice = pcommon.NewFloat64Slice()
				switch sc.typ {
				case Gauge, Sum:
					var dps pmetric.NumberDataPointSlice
					if sc.typ == Gauge {
						dps = m.Gauge().DataPoints()
					} else {
						dps = m.Sum().DataPoints()
					}
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ts := dt(dp.Timestamp())
						r := NumberRow{MetricName: sc.name, ServiceName: sc.svc, SeriesID: id(dp.Attributes(), noBounds, ts),
							StartTimeUnix: dt(dp.StartTimestamp()), TimeUnix: ts, Flags: uint32(dp.Flags())}
						switch dp.ValueType() {
						case pmetric.NumberDataPointValueTypeDouble:
							r.Value = dp.DoubleValue()
						case pmetric.NumberDataPointValueTypeInt:
							r.Value = float64(dp.IntValue())
						}
						r.ExTime, r.ExValue, r.ExSpan, r.ExTrace = e.exemplars(dp.Exemplars())
						e.num[sc.typ] = append(e.num[sc.typ], r)
					}
				case Histogram:
					dps := m.Histogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ts := dt(dp.Timestamp())
						r := HistRow{MetricName: sc.name, ServiceName: sc.svc, SeriesID: id(dp.Attributes(), dp.ExplicitBounds(), ts),
							StartTimeUnix: dt(dp.StartTimestamp()), TimeUnix: ts, Count: dp.Count(), Sum: dp.Sum(),
							BucketCounts: dp.BucketCounts().AsRaw(), Min: dp.Min(), Max: dp.Max(), Flags: uint32(dp.Flags())}
						r.ExTime, r.ExValue, r.ExSpan, r.ExTrace = e.exemplars(dp.Exemplars())
						e.hist = append(e.hist, r)
					}
				case ExpHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ts := dt(dp.Timestamp())
						r := ExpRow{MetricName: sc.name, ServiceName: sc.svc, SeriesID: id(dp.Attributes(), noBounds, ts),
							StartTimeUnix: dt(dp.StartTimestamp()), TimeUnix: ts, Count: dp.Count(), Sum: dp.Sum(), Scale: dp.Scale(),
							ZeroCount: dp.ZeroCount(), PositiveOffset: dp.Positive().Offset(), PositiveCounts: dp.Positive().BucketCounts().AsRaw(),
							NegativeOffset: dp.Negative().Offset(), NegativeCounts: dp.Negative().BucketCounts().AsRaw(),
							Min: dp.Min(), Max: dp.Max(), Flags: uint32(dp.Flags())}
						r.ExTime, r.ExValue, r.ExSpan, r.ExTrace = e.exemplars(dp.Exemplars())
						e.exp = append(e.exp, r)
					}
				case Summary:
					dps := m.Summary().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						ts := dt(dp.Timestamp())
						r := SummaryRow{MetricName: sc.name, ServiceName: sc.svc, SeriesID: id(dp.Attributes(), noBounds, ts),
							StartTimeUnix: dt(dp.StartTimestamp()), TimeUnix: ts, Count: dp.Count(), Sum: dp.Sum(), Flags: uint32(dp.Flags())}
						q := dp.QuantileValues()
						r.Quantile, r.QValue = make([]float64, q.Len()), make([]float64, q.Len())
						for x := 0; x < q.Len(); x++ {
							r.Quantile[x], r.QValue[x] = q.At(x).Quantile(), q.At(x).Value()
						}
						e.sum = append(e.sum, r)
					}
				}
			}
		}
	}
	rows = [NumTypes]int{len(e.num[0]), len(e.num[1]), len(e.hist), len(e.exp), len(e.sum)}
	e.envelope(env)
	return rows
}

func (e *Encoder) exemplars(ex pmetric.ExemplarSlice) (t []uint32, v []float64, s, tr []string) {
	n := ex.Len()
	if n == 0 {
		return nil, nil, nil, nil
	}
	t, v, s, tr = make([]uint32, n), make([]float64, n), make([]string, n), make([]string, n)
	for i := 0; i < n; i++ {
		x := ex.At(i)
		t[i] = dt(x.Timestamp())
		switch x.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			v[i] = x.DoubleValue()
		case pmetric.ExemplarValueTypeInt:
			v[i] = float64(x.IntValue())
		}
		sid, tid := x.SpanID(), x.TraceID()
		s[i], tr[i] = hex.EncodeToString(sid[:]), hex.EncodeToString(tid[:])
	}
	return
}

func (e *Encoder) envelope(env *Envelope) {
	if env == nil {
		return
	}
	for t := 0; t < 2; t++ {
		for i := range e.num[t] {
			r := &e.num[t][i]
			r.ProducerID, r.Epoch, r.BatchID, r.RowOrdinal, r.ReceivedAt, r.SchemaVersion = env.ProducerID, env.Epoch, env.BatchID, uint32(i), env.ReceivedAtNs, env.SchemaVersion
		}
	}
	for i := range e.hist {
		r := &e.hist[i]
		r.ProducerID, r.Epoch, r.BatchID, r.RowOrdinal, r.ReceivedAt, r.SchemaVersion = env.ProducerID, env.Epoch, env.BatchID, uint32(i), env.ReceivedAtNs, env.SchemaVersion
	}
	for i := range e.exp {
		r := &e.exp[i]
		r.ProducerID, r.Epoch, r.BatchID, r.RowOrdinal, r.ReceivedAt, r.SchemaVersion = env.ProducerID, env.Epoch, env.BatchID, uint32(i), env.ReceivedAtNs, env.SchemaVersion
	}
	for i := range e.sum {
		r := &e.sum[i]
		r.ProducerID, r.Epoch, r.BatchID, r.RowOrdinal, r.ReceivedAt, r.SchemaVersion = env.ProducerID, env.Epoch, env.BatchID, uint32(i), env.ReceivedAtNs, env.SchemaVersion
	}
	for i := range e.ser {
		r := &e.ser[i]
		r.ProducerID, r.Epoch, r.BatchID, r.RowOrdinal, r.ReceivedAt, r.SchemaVersion = env.ProducerID, env.Epoch, env.BatchID, uint32(i), env.ReceivedAtNs, env.SchemaVersion
	}
}

func write[T any](w **parquet.GenericWriter[T], dst io.Writer, rows []T, opts []parquet.WriterOption) error {
	if *w == nil {
		*w = parquet.NewGenericWriter[T](dst, opts...)
	} else {
		(*w).Reset(dst)
	}
	if _, err := (*w).Write(rows); err != nil {
		return err
	}
	return (*w).Close()
}

// Flush writes the objects of the last Encode: dst(t) for each point type
// with rows (t in 0..NumTypes-1) and dst(NumTypes) for the series object if
// any series is new. dst is called only for objects that exist.
func (e *Encoder) Flush(dst func(t int) io.Writer) error {
	if len(e.num[0]) > 0 {
		if err := write(&e.wNum[0], dst(Gauge), e.num[0], e.opts); err != nil {
			return err
		}
	}
	if len(e.num[1]) > 0 {
		if err := write(&e.wNum[1], dst(Sum), e.num[1], e.opts); err != nil {
			return err
		}
	}
	if len(e.hist) > 0 {
		if err := write(&e.wHist, dst(Histogram), e.hist, e.opts); err != nil {
			return err
		}
	}
	if len(e.exp) > 0 {
		if err := write(&e.wExp, dst(ExpHistogram), e.exp, e.opts); err != nil {
			return err
		}
	}
	if len(e.sum) > 0 {
		if err := write(&e.wSum, dst(Summary), e.sum, e.opts); err != nil {
			return err
		}
	}
	if len(e.ser) > 0 {
		if err := write(&e.wSer, dst(NumTypes), e.ser, e.opts); err != nil {
			return err
		}
	}
	return nil
}

// Buffers is a convenience dst for Flush: one bytes.Buffer per object.
type Buffers [NumTypes + 1]bytes.Buffer

// Dst returns Flush's dst over b, resetting each buffer it hands out.
func (b *Buffers) Dst() func(int) io.Writer {
	for i := range b {
		b[i].Reset()
	}
	return func(t int) io.Writer { return &b[t] }
}
