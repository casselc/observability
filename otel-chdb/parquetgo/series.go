package parquetgo

// Metrics layout B (DECISIONS.md D7; ../metrics-layout/README.md): narrow
// per-type **points** objects keyed by a series id computed here, at the
// edge, plus a **series** object with everything else of the contrib row,
// for the series this edge has not announced yet in the current cache
// window. This is ../metrics-layout/seriesenc (the spike's prototype),
// promoted, with the Rust edge's defaults (../otap-rs/src/series.rs), which
// the consumer's layout-B statements expect:
//
//   - gauge and sum points merged into `metrics_number_points`, with a
//     `MetricType` column after `series_id` (MergeNumberPoints);
//   - `Exemplars.FilteredAttributes` in the points objects (ExemplarAttributes);
//   - BYTE_STREAM_SPLIT on the float and count columns, and no statistics
//     (the wire encodings; they change no row and no Parquet schema).
//
// Series id (v1), identical to the prototype's and the Rust edge's:
//
//	h_rs      = xxh3_64( res_kvs, ResourceSchemaUrl, ScopeName, ScopeVersion,
//	                     scope_kvs, le32(ScopeDroppedAttrCount), ScopeSchemaUrl )
//	series_id = xxh3_64( le64(h_rs), MetricType, MetricName, MetricDescription,
//	                     MetricUnit, le32(AggregationTemporality), IsMonotonic,
//	                     point_kvs, uvarint(len) le64(ExplicitBounds...) )
//
// A string is uvarint(len) || bytes; kvs are uvarint(n) then each entry in
// key order (a stable sort: duplicate keys keep their wire order), the value
// tagged with its type: 's' string, 'i'/'d' 8 little-endian bytes, 'b' one
// byte, else 'x' and AsString().
//
// Maps in the series object and the exemplars are written in the order the
// contrib exporter writes them: clickhouse-go's orderedmap.CollectN, an
// unstable slices.SortFunc by key over the pdata order. It only differs from
// the id's stable order for duplicate keys (wire-decoded pdata), where the
// Rust edge ports this very sort (otap-rs/src/gosort.rs).
//
// The series cache: a series is announced the first time it is seen in a
// window (the hour of the point's TimeUnix), and within a request once. The
// caller marks a request's new series announced (SeriesEncoder.Announced)
// only once the series object has **committed**; until then every request
// announces them again. A commit in an epoch the cache hasn't seen empties
// it, so a new series-lane epoch re-announces everything.

import (
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// SeriesOptions are the Rust edge's `series` options, with its defaults.
type SeriesOptions struct {
	// Window is the cache window: a series is re-announced once per window
	// of its points' TimeUnix. Default 1h.
	Window time.Duration `mapstructure:"window"`
	// MergeNumberPoints: gauge and sum share metrics_number_points.
	MergeNumberPoints bool `mapstructure:"merge_number_points"`
	// ExemplarAttributes: carry Exemplars.FilteredAttributes in the points.
	ExemplarAttributes bool `mapstructure:"exemplar_attributes"`
	// ByteStreamSplit writes the float and count columns BYTE_STREAM_SPLIT.
	ByteStreamSplit bool `mapstructure:"byte_stream_split"`
	// Statistics: "none" (default: no min/max, no page statistics) or "page".
	Statistics string `mapstructure:"statistics"`
}

// DefaultSeriesOptions are the Rust edge's defaults.
func DefaultSeriesOptions() SeriesOptions {
	return SeriesOptions{Window: time.Hour, MergeNumberPoints: true, ExemplarAttributes: true,
		ByteStreamSplit: true, Statistics: "none"}
}

// PrototypeSeriesOptions is ../metrics-layout/seriesenc's layout, column for
// column (the wire encodings stay on: they change no row and no schema).
func PrototypeSeriesOptions() SeriesOptions {
	o := DefaultSeriesOptions()
	o.MergeNumberPoints, o.ExemplarAttributes = false, false
	return o
}

// Layout-B namespaces: the S3 path segment, content-key domain and table
// suffix, as the Rust edge names them.
const (
	SigNumberPoints       = "metrics_number_points"
	SigGaugePoints        = "metrics_gauge_points"
	SigSumPoints          = "metrics_sum_points"
	SigHistogramPoints    = "metrics_histogram_points"
	SigExpHistogramPoints = "metrics_exponential_histogram_points"
	SigSummaryPoints      = "metrics_summary_points"
	SigSeries             = "metrics_series"
)

// MetricType numbers, as the series table's Enum8 does.
const (
	mtGauge uint8 = iota
	mtSum
	mtHistogram
	mtExpHistogram
	mtSummary
)

// SeriesSignals are the namespaces a request can produce with o, in object
// order (points first, the series object last).
func (o SeriesOptions) SeriesSignals() []string {
	var v []string
	if o.MergeNumberPoints {
		v = []string{SigNumberPoints}
	} else {
		v = []string{SigGaugePoints, SigSumPoints}
	}
	return append(v, SigHistogramPoints, SigExpHistogramPoints, SigSummaryPoints, SigSeries)
}

// seriesCols is the column list of a layout-B object, envelope included,
// and its root name (the prototype's row type).
func seriesCols(signal string, o SeriesOptions) (string, []pgCol) {
	ex := []pgCol{}
	if o.ExemplarAttributes {
		ex = append(ex, pgCol{"Exemplars.FilteredAttributes", kListMap})
	}
	ex = append(ex, pgCol{"Exemplars.TimeUnix", kListU32}, pgCol{"Exemplars.Value", kListF64},
		pgCol{"Exemplars.SpanId", kListStr}, pgCol{"Exemplars.TraceId", kListStr})
	head := []pgCol{{"MetricName", kStr}, {"ServiceName", kStr}, {"series_id", kU64}}
	if signal == SigNumberPoints {
		head = append(head, pgCol{"MetricType", kU8})
	}
	head = append(head, pgCol{"StartTimeUnix", kU32}, pgCol{"TimeUnix", kU32})
	var root string
	var f []pgCol
	switch signal {
	case SigNumberPoints, SigGaugePoints, SigSumPoints:
		root, f = "NumberRow", cat(head, []pgCol{{"Value", kF64}, {"Flags", kU32}}, ex)
	case SigHistogramPoints:
		root, f = "HistRow", cat(head, []pgCol{{"Count", kU64}, {"Sum", kF64}, {"BucketCounts", kListU64},
			{"Min", kF64}, {"Max", kF64}, {"Flags", kU32}}, ex)
	case SigExpHistogramPoints:
		root, f = "ExpRow", cat(head, []pgCol{{"Count", kU64}, {"Sum", kF64}, {"Scale", kI32}, {"ZeroCount", kU64},
			{"PositiveOffset", kI32}, {"PositiveBucketCounts", kListU64}, {"NegativeOffset", kI32},
			{"NegativeBucketCounts", kListU64}, {"Min", kF64}, {"Max", kF64}, {"Flags", kU32}}, ex)
	case SigSummaryPoints:
		root, f = "SummaryRow", cat(head, []pgCol{{"Count", kU64}, {"Sum", kF64},
			{"ValueAtQuantiles.Quantile", kListF64}, {"ValueAtQuantiles.Value", kListF64}, {"Flags", kU32}})
	case SigSeries:
		root, f = "SeriesRow", []pgCol{{"series_id", kU64}, {"MetricType", kU8}, {"MetricName", kStr},
			{"MetricDescription", kStr}, {"MetricUnit", kStr}, {"ServiceName", kStr},
			{"ResourceAttributesKeys", kListStr}, {"ResourceAttributesValues", kListStr}, {"ResourceSchemaUrl", kStr},
			{"ScopeName", kStr}, {"ScopeVersion", kStr}, {"ScopeAttributesKeys", kListStr},
			{"ScopeAttributesValues", kListStr}, {"ScopeDroppedAttrCount", kU32}, {"ScopeSchemaUrl", kStr},
			{"AttributesKeys", kListStr}, {"AttributesValues", kListStr}, {"AggregationTemporality", kI32},
			{"IsMonotonic", kBool}, {"ExplicitBounds", kListF64}, {"FirstSeen", kU32}}
	default:
		panic("not a layout-B signal: " + signal)
	}
	return root, cat(f, pgEnvelope)
}

// SeriesColumns returns a layout-B object's column names, envelope included.
func SeriesColumns(signal string, o SeriesOptions) []string {
	_, cols := seriesCols(signal, o)
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

// seriesParquet is the Rust edge's layout-B encoding: dictionaries except on
// the near-unique numbers, row_ordinal DELTA_BINARY_PACKED, the floats and
// counts BYTE_STREAM_SPLIT, no statistics, no bloom filters.
func seriesParquet(o SeriesOptions, base Options) Options {
	p := base
	p.Dictionary = true
	p.PlainFor = []string{"Value", "Sum", "Exemplars.TimeUnix.list.element", "Exemplars.Value.list.element",
		"Exemplars.SpanId.list.element", "Exemplars.TraceId.list.element",
		"series_id", "Count", "Min", "Max", "ZeroCount", "FirstSeen", "BucketCounts.list.element",
		"PositiveBucketCounts.list.element", "NegativeBucketCounts.list.element", "ValueAtQuantiles.Value.list.element"}
	p.DeltaFor = []string{"row_ordinal"}
	p.ByteStreamSplitFor = nil
	if o.ByteStreamSplit {
		p.ByteStreamSplitFor = []string{"Value", "Sum", "Min", "Max", "Count", "ZeroCount", "BucketCounts.list.element",
			"PositiveBucketCounts.list.element", "NegativeBucketCounts.list.element"}
	}
	p.BloomFilters, p.BloomColumns = false, nil
	if o.Statistics == "" || o.Statistics == "none" {
		p.Statistics, p.PageIndex, p.NoBounds = false, false, true
	} else {
		p.Statistics, p.PageIndex, p.NoBounds = true, true, false
	}
	return p
}

// SeriesEncoder walks metrics requests into layout B. Walk is serialized
// internally (the series cache is shared); the SeriesBatch it returns is the
// caller's until Release.
type SeriesEncoder struct {
	opts  SeriesOptions
	popts Options
	pools map[string]*freeList[*pgSignal]

	mu          sync.Mutex
	window      int64
	cache       map[uint64]int32
	cacheEpochs map[string]struct{}
	maxWin      int32
	seen        map[uint64]struct{}

	// walk scratch
	buf   []byte
	rkv   seriesKV
	skv   seriesKV
	kv    seriesKV
	exkv  []attrKV
	bnds  []float64
	scrat []byte
}

// NewSeriesEncoder builds a layout-B encoder. base supplies the compression
// (zstd by default) and page size; everything else is layout B's own.
func NewSeriesEncoder(o SeriesOptions, base Options) *SeriesEncoder {
	if o.Window <= 0 {
		o.Window = time.Hour
	}
	if base.Compression == "" && base.DataPageSize == 0 {
		base = DefaultOptions()
	}
	e := &SeriesEncoder{opts: o, popts: seriesParquet(o, base), pools: map[string]*freeList[*pgSignal]{},
		window: max(int64(o.Window/time.Second), 1), cache: map[uint64]int32{}, cacheEpochs: map[string]struct{}{},
		maxWin: math.MinInt32, seen: map[uint64]struct{}{}}
	for _, sig := range o.SeriesSignals() {
		root, cols := seriesCols(sig, o)
		popts := e.popts
		e.pools[sig] = newFreeList(64, func() *pgSignal {
			s, err := newPGSignalNamed(root, cols, popts)
			if err != nil {
				panic(err) // a static schema
			}
			return s
		})
	}
	return e
}

// Options returns the encoder's options.
func (e *SeriesEncoder) Options() SeriesOptions { return e.opts }

// CacheLen is the number of cached (announced) series.
func (e *SeriesEncoder) CacheLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.cache)
}

// SeriesNew is a new series of a request: its id and cache window.
type SeriesNew struct {
	ID  uint64
	Win int32
}

// Announced marks series announced: call only once the series object that
// carried them has committed, in epoch (the series lane's). A commit in an
// epoch the cache hasn't seen yet empties the cache first.
func (e *SeriesEncoder) Announced(ids []SeriesNew, epoch string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.cacheEpochs[epoch]; !ok {
		if len(e.cacheEpochs) > 0 {
			clear(e.cache)
		}
		e.cacheEpochs[epoch] = struct{}{}
	}
	for _, s := range ids {
		e.cache[s.ID] = s.Win
		if s.Win > e.maxWin {
			e.maxWin = s.Win
			for id, w := range e.cache {
				if w < s.Win-1 {
					delete(e.cache, id)
				}
			}
		}
	}
}

// SeriesObject is one object of a request in layout B.
type SeriesObject struct {
	Signal string
	Rows   int
	// MinTS / MaxTS: the rows' TimeUnix range in ns (0: unset).
	MinTS, MaxTS uint64
	sig          *pgSignal
}

// SeriesBatch is a request walked into layout B: its non-empty objects in
// object order, and the series it announces (with the series object).
type SeriesBatch struct {
	Objects []*SeriesObject
	New     []SeriesNew
	enc     *SeriesEncoder
}

// Release returns the batch's buffers.
func (b *SeriesBatch) Release() {
	for _, o := range b.Objects {
		if o.sig != nil {
			o.sig.reset()
			b.enc.pools[o.Signal].Put(o.sig)
			o.sig = nil
		}
	}
}

// ContentKey is the series object's content key: BLAKE3 over its rows
// (the caller's hasher), since what a request announces depends on the
// cache. It feeds every content value with its levels.
func (o *SeriesObject) HashRows(write func([]byte)) {
	s := o.sig
	var b []byte
	nContent := len(s.cols) - len(pgEnvelope)
	for c := 0; c < nContent; c++ {
		for j, leaf := range s.leaf[c] {
			if j == 1 && s.cols[c].kind != kMap && s.cols[c].kind != kListMap {
				break
			}
			for _, v := range s.vals[leaf] {
				b = append(b[:0], byte(v.RepetitionLevel()), byte(v.DefinitionLevel()))
				if !v.IsNull() {
					raw := v.Bytes()
					b = binary.AppendUvarint(b, uint64(len(raw)))
					b = append(b, raw...)
				}
				write(b)
			}
		}
	}
}

// Encode writes the object with this envelope and footer key-values
// (sorted by key into the footer). Deterministic: the same batch, envelope
// and footer give the same bytes.
func (o *SeriesObject) Encode(dst interface{ Write([]byte) (int, error) }, env *Envelope, footer map[string]string) error {
	o.sig.setEnvelope(env)
	o.sig.setFooter(footer)
	return o.sig.flush(dst)
}

// Walk encodes one request into layout B. A metric with no data rejects the
// whole request (ErrMetricTypeUnset), as the contrib exporter does.
func (e *SeriesEncoder) Walk(md pmetric.Metrics) (*SeriesBatch, error) {
	if _, err := MetricPoints(md); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	clear(e.seen)
	merged := e.opts.MergeNumberPoints
	sigs := e.opts.SeriesSignals()
	objs := make(map[string]*SeriesObject, len(sigs))
	get := func(sig string) *SeriesObject {
		o := objs[sig]
		if o == nil {
			s := e.pools[sig].Get()
			s.reset()
			o = &SeriesObject{Signal: sig, sig: s}
			objs[sig] = o
		}
		return o
	}
	var news []SeriesNew
	var c seriesCtx
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := rm.Resource().Attributes()
		e.rkv.load(res)
		c.svc = serviceName(res)
		c.resURL = rm.SchemaUrl()
		c.resK, c.resV = nil, nil
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			sc := sm.Scope()
			e.skv.load(sc.Attributes())
			c.scName, c.scVer, c.scURL, c.dropped = sc.Name(), sc.Version(), sm.SchemaUrl(), sc.DroppedAttributesCount()
			c.scK, c.scV = nil, nil
			b := e.buf[:0]
			b = e.rkv.hashInto(b)
			b = putStr(b, c.resURL)
			b = putStr(b, c.scName)
			b = putStr(b, c.scVer)
			b = e.skv.hashInto(b)
			b = binary.LittleEndian.AppendUint32(b, c.dropped)
			b = putStr(b, c.scURL)
			hrs := xxh3.Hash(b)
			mets := sm.Metrics()
			for k := 0; k < mets.Len(); k++ {
				m := mets.At(k)
				c.name, c.desc, c.unit = m.Name(), m.Description(), m.Unit()
				c.temp, c.mono = 0, false
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					c.typ = mtGauge
				case pmetric.MetricTypeSum:
					c.typ, c.temp, c.mono = mtSum, int32(m.Sum().AggregationTemporality()), m.Sum().IsMonotonic()
				case pmetric.MetricTypeHistogram:
					c.typ, c.temp = mtHistogram, int32(m.Histogram().AggregationTemporality())
				case pmetric.MetricTypeExponentialHistogram:
					c.typ, c.temp = mtExpHistogram, int32(m.ExponentialHistogram().AggregationTemporality())
				case pmetric.MetricTypeSummary:
					c.typ = mtSummary
				}
				b = e.buf[:0]
				b = binary.LittleEndian.AppendUint64(b, hrs)
				b = append(b, c.typ)
				b = putStr(b, c.name)
				b = putStr(b, c.desc)
				b = putStr(b, c.unit)
				b = binary.LittleEndian.AppendUint32(b, uint32(c.temp))
				if c.mono {
					b = append(b, 1)
				} else {
					b = append(b, 0)
				}
				e.buf = b
				c.moff = len(b)
				e.bnds = e.bnds[:0]
				switch c.typ {
				case mtGauge, mtSum:
					var dps pmetric.NumberDataPointSlice
					sig := SigNumberPoints
					if c.typ == mtGauge {
						dps = m.Gauge().DataPoints()
						if !merged {
							sig = SigGaugePoints
						}
					} else {
						dps = m.Sum().DataPoints()
						if !merged {
							sig = SigSumPoints
						}
					}
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						id := e.pointID(&c, dp.Attributes(), uint64(dp.Timestamp()), get, &news)
						o := get(sig)
						s := o.sig
						e.head(o, &c, id, merged, uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						s.f64(numberValue(dp))
						s.u32(uint32(dp.Flags()))
						e.exemplars(s, dp.Exemplars())
						s.endRow()
					}
				case mtHistogram:
					dps := m.Histogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						e.bnds = append(e.bnds[:0], dp.ExplicitBounds().AsRaw()...)
						id := e.pointID(&c, dp.Attributes(), uint64(dp.Timestamp()), get, &news)
						o := get(SigHistogramPoints)
						s := o.sig
						e.head(o, &c, id, false, uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						s.u64(dp.Count())
						s.f64(dp.Sum())
						u64List(s, dp.BucketCounts())
						s.f64(dp.Min())
						s.f64(dp.Max())
						s.u32(uint32(dp.Flags()))
						e.exemplars(s, dp.Exemplars())
						s.endRow()
					}
					e.bnds = e.bnds[:0]
				case mtExpHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						id := e.pointID(&c, dp.Attributes(), uint64(dp.Timestamp()), get, &news)
						o := get(SigExpHistogramPoints)
						s := o.sig
						e.head(o, &c, id, false, uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						s.u64(dp.Count())
						s.f64(dp.Sum())
						s.i32(dp.Scale())
						s.u64(dp.ZeroCount())
						s.i32(dp.Positive().Offset())
						u64List(s, dp.Positive().BucketCounts())
						s.i32(dp.Negative().Offset())
						u64List(s, dp.Negative().BucketCounts())
						s.f64(dp.Min())
						s.f64(dp.Max())
						s.u32(uint32(dp.Flags()))
						e.exemplars(s, dp.Exemplars())
						s.endRow()
					}
				case mtSummary:
					dps := m.Summary().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						id := e.pointID(&c, dp.Attributes(), uint64(dp.Timestamp()), get, &news)
						o := get(SigSummaryPoints)
						s := o.sig
						e.head(o, &c, id, false, uint64(dp.StartTimestamp()), uint64(dp.Timestamp()))
						s.u64(dp.Count())
						s.f64(dp.Sum())
						q := dp.QuantileValues()
						s.arr(q.Len())
						for x := 0; x < q.Len(); x++ {
							s.f64(q.At(x).Quantile())
						}
						s.end()
						s.arr(q.Len())
						for x := 0; x < q.Len(); x++ {
							s.f64(q.At(x).Value())
						}
						s.end()
						s.u32(uint32(dp.Flags()))
						s.endRow()
					}
				}
			}
		}
	}
	out := &SeriesBatch{New: news, enc: e}
	for _, sig := range sigs {
		if o := objs[sig]; o != nil {
			o.Rows = o.sig.rows
			out.Objects = append(out.Objects, o)
		}
	}
	return out, nil
}

func u64List(s *pgSignal, v pcommon.UInt64Slice) {
	s.arr(v.Len())
	for i := 0; i < v.Len(); i++ {
		s.u64(v.At(i))
	}
	s.end()
}

// seriesCtx is what every row of a metric shares.
type seriesCtx struct {
	svc, resURL, scName, scVer, scURL string
	dropped                           uint32
	name, desc, unit                  string
	typ                               uint8
	temp                              int32
	mono                              bool
	moff                              int
	resK, resV, scK, scV              []string // rendered once per resource / scope, when first needed
}

func (e *SeriesEncoder) head(o *SeriesObject, c *seriesCtx, id uint64, withType bool, start, t uint64) {
	s := o.sig
	s.row()
	s.str(c.name)
	s.str(c.svc)
	s.u64(id)
	if withType {
		s.u8(c.typ)
	}
	s.u32(dtSec(start))
	s.u32(dtSec(t))
	if o.MinTS == 0 || t < o.MinTS {
		o.MinTS = t
	}
	if t > o.MaxTS {
		o.MaxTS = t
	}
}

// pointID is the series id of a point (its attributes and e.bnds on top of
// the metric prefix in e.buf), and a series row if it is new in its window.
func (e *SeriesEncoder) pointID(c *seriesCtx, attrs pcommon.Map, tsNs uint64,
	get func(string) *SeriesObject, news *[]SeriesNew) uint64 {
	e.kv.load(attrs)
	b := e.buf[:c.moff]
	b = e.kv.hashInto(b)
	b = binary.AppendUvarint(b, uint64(len(e.bnds)))
	for _, x := range e.bnds {
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
	}
	e.buf = b
	id := xxh3.Hash(b)
	ts := dtSec(tsNs)
	w := int32(int64(ts) / e.window)
	if cw, ok := e.cache[id]; ok && cw == w {
		return id
	}
	if _, ok := e.seen[id]; ok {
		return id
	}
	e.seen[id] = struct{}{}
	*news = append(*news, SeriesNew{id, w})
	o := get(SigSeries)
	s := o.sig
	s.row()
	s.u64(id)
	s.u8(c.typ)
	s.str(c.name)
	s.str(c.desc)
	s.str(c.unit)
	s.str(c.svc)
	if c.resK == nil {
		c.resK, c.resV = e.rkv.render(s)
	}
	strList(s, c.resK)
	strList(s, c.resV)
	s.str(c.resURL)
	s.str(c.scName)
	s.str(c.scVer)
	if c.scK == nil {
		c.scK, c.scV = e.skv.render(s)
	}
	strList(s, c.scK)
	strList(s, c.scV)
	s.u32(c.dropped)
	s.str(c.scURL)
	ak, av := e.kv.render(s)
	strList(s, ak)
	strList(s, av)
	s.i32(c.temp)
	s.boolean(c.mono)
	s.arr(len(e.bnds))
	for _, x := range e.bnds {
		s.f64(x)
	}
	s.end()
	s.u32(ts)
	s.endRow()
	if o.MinTS == 0 || tsNs < o.MinTS {
		o.MinTS = tsNs
	}
	if tsNs > o.MaxTS {
		o.MaxTS = tsNs
	}
	return id
}

func strList(s *pgSignal, v []string) {
	s.arr(len(v))
	for _, x := range v {
		s.str(x)
	}
	s.end()
}

// exemplars writes the exemplar columns of a point.
func (e *SeriesEncoder) exemplars(s *pgSignal, ex pmetric.ExemplarSlice) {
	n := ex.Len()
	if e.opts.ExemplarAttributes {
		s.arr(n)
		for i := 0; i < n; i++ {
			e.exkv = contribKVs(e.exkv, ex.At(i).FilteredAttributes())
			s.kvAttrs(e.exkv)
		}
		s.end()
	}
	s.arr(n)
	for i := 0; i < n; i++ {
		s.u32(dtSec(uint64(ex.At(i).Timestamp())))
	}
	s.end()
	s.arr(n)
	for i := 0; i < n; i++ {
		x := ex.At(i)
		switch x.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			s.f64(x.DoubleValue())
		case pmetric.ExemplarValueTypeInt:
			s.f64(float64(x.IntValue()))
		default:
			s.f64(0)
		}
	}
	s.end()
	s.arr(n)
	for i := 0; i < n; i++ {
		s.hexSpan(ex.At(i).SpanID())
	}
	s.end()
	s.arr(n)
	for i := 0; i < n; i++ {
		s.hexTrace(ex.At(i).TraceID())
	}
	s.end()
}

// dtSec is the exporter's DateTime: signed floor seconds, wrapped mod 2^32.
func dtSec(ns uint64) uint32 {
	n := int64(ns)
	s := n / 1e9
	if n%1e9 < 0 {
		s--
	}
	return uint32(s)
}

func putStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// seriesKV is one attribute map loaded once: its entries in pdata order, in
// the id's canonical (stable) order, and whether it has duplicate keys.
type seriesKV struct {
	kvs    []attrKV // pdata order
	stable []attrKV
	dup    bool
}

func (m *seriesKV) load(attrs pcommon.Map) {
	m.kvs = m.kvs[:0]
	sorted := true
	attrs.Range(func(k string, v pcommon.Value) bool {
		if n := len(m.kvs); n > 0 && m.kvs[n-1].k > k {
			sorted = false
		}
		m.kvs = append(m.kvs, attrKV{k, v})
		return true
	})
	m.stable = append(m.stable[:0], m.kvs...)
	if !sorted {
		slices.SortStableFunc(m.stable, func(a, b attrKV) int { return strings.Compare(a.k, b.k) })
	}
	m.dup = false
	for i := 1; i < len(m.stable); i++ {
		if m.stable[i-1].k == m.stable[i].k {
			m.dup = true
			break
		}
	}
}

func (m *seriesKV) hashInto(b []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(m.stable)))
	for _, a := range m.stable {
		b = putStr(b, a.k)
		v := a.v
		switch v.Type() {
		case pcommon.ValueTypeStr:
			b = append(b, 's')
			b = putStr(b, v.Str())
		case pcommon.ValueTypeInt:
			b = append(b, 'i')
			b = binary.LittleEndian.AppendUint64(b, uint64(v.Int()))
		case pcommon.ValueTypeDouble:
			b = append(b, 'd')
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v.Double()))
		case pcommon.ValueTypeBool:
			x := byte(0)
			if v.Bool() {
				x = 1
			}
			b = append(b, 'b', x)
		default:
			b = append(b, 'x')
			b = putStr(b, v.AsString())
		}
	}
	return b
}

// render returns the keys and values in the contrib exporter's order, the
// values as AsString (stashed in s's arena until the object is flushed).
func (m *seriesKV) render(s *pgSignal) (keys, vals []string) {
	order := m.stable
	if m.dup {
		order = slices.Clone(m.kvs)
		slices.SortFunc(order, func(a, b attrKV) int { return strings.Compare(a.k, b.k) })
	}
	keys, vals = make([]string, len(order)), make([]string, len(order))
	for i, a := range order {
		keys[i] = a.k
		if a.v.Type() == pcommon.ValueTypeStr {
			vals[i] = a.v.Str()
		} else {
			s.scratch = valueString(s.scratch[:0], a.v)
			vals[i] = string(s.stash(s.scratch))
		}
	}
	return keys, vals
}

// contribKVs is a map's entries as the contrib exporter writes them:
// orderedmap.CollectN's slices.SortFunc by key over the pdata order.
func contribKVs(dst []attrKV, m pcommon.Map) []attrKV {
	kvs := dst[:0]
	m.Range(func(k string, v pcommon.Value) bool {
		kvs = append(kvs, attrKV{k, v})
		return true
	})
	slices.SortFunc(kvs, func(a, b attrKV) int { return strings.Compare(a.k, b.k) })
	return kvs
}

// freeList keeps encoders (their column buffers, writers and zstd state)
// across requests. A sync.Pool would do, except that it empties at every GC,
// and an encoder rebuilt after each GC allocates far more than it saves.
type freeList[T any] struct {
	ch  chan T
	new func() T
}

func newFreeList[T any](n int, f func() T) *freeList[T] {
	return &freeList[T]{ch: make(chan T, n), new: f}
}

// NewFreeList is a free list of up to n values made by f, for callers that
// keep encoders (../edge).
func NewFreeList[T any](n int, f func() T) *FreeList[T] { return &FreeList[T]{*newFreeList(n, f)} }

// FreeList is freeList, exported.
type FreeList[T any] struct{ freeList[T] }

func (l *freeList[T]) Get() T {
	select {
	case v := <-l.ch:
		return v
	default:
		return l.new()
	}
}

func (l *freeList[T]) Put(v T) {
	select {
	case l.ch <- v:
	default:
	}
}

// ---- envelope-at-encode and footer, for objects walked once ----------------

// setEnvelope fills the envelope columns (the last len(pgEnvelope), which a
// content-only walk leaves empty) for s.rows rows.
func (s *pgSignal) setEnvelope(env *Envelope) {
	first := len(s.cols) - len(pgEnvelope)
	for i := first; i < len(s.cols); i++ {
		leaf := s.leaf[i][0]
		vals := s.vals[leaf][:0]
		for r := 0; r < s.rows; r++ {
			var v parquet.Value
			switch i - first {
			case 0:
				v = parquet.ByteArrayValue(bytesOf(env.Producer))
			case 1:
				v = parquet.ByteArrayValue(bytesOf(env.Epoch))
			case 2:
				v = parquet.Int64Value(int64(env.Batch))
			case 3:
				v = parquet.Int32Value(int32(r))
			case 4:
				v = parquet.Int64Value(int64(env.Received))
			case 5:
				v = parquet.Int32Value(int32(env.Schema))
			}
			vals = append(vals, v.Level(0, 0, leaf))
		}
		s.vals[leaf] = vals
		for k, b := range s.offs {
			b[leaf] = (k + 1) * pgPageRows
		}
	}
}
