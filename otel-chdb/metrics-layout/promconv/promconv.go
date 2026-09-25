// Package promconv turns OTel metrics into Prometheus-model samples, for the
// layouts that store Prometheus series: C (ClickHouse's TimeSeries engine)
// and D (the Prometheus TSDB). Full fidelity, like B: every resource and
// point attribute becomes a label (sanitised), so a series here is the same
// set of points as a B series. (Prometheus' own OTLP receiver keeps only
// job/instance plus promoted attributes and moves the rest to target_info,
// which is cheaper; see README.)
//
// Mapping (classic, what both C and D can store as float samples):
//   - gauge: <name>; sum: <name>_total (monotonic) or <name>;
//   - histogram: <name>_bucket{le=...} for every bound and +Inf (cumulative
//     counts), <name>_sum, <name>_count;
//   - exponential histogram: <name>_sum and <name>_count only, accumulated
//     from delta to cumulative (buckets need native histograms: Native);
//   - summary: <name>{quantile=...}, <name>_sum, <name>_count.
//
// Names: dots and other non [a-zA-Z0-9_:] characters become '_'.
package promconv

import (
	"slices"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Label is one label.
type Label struct{ Name, Value string }

// Hist is an OTel histogram point as a Prometheus native histogram: either
// custom buckets (Schema -53, Bounds set, Counts per bucket, not
// cumulative) or exponential (Schema = scale, Offset, Counts).
type Hist struct {
	Schema    int32
	Bounds    []float64
	Offset    int32
	Counts    []uint64
	ZeroCount uint64
	Count     uint64
	Sum       float64
	Delta     bool
}

// Sink receives samples. lbls is sorted by name and includes __name__;
// it is only valid during the call.
type Sink interface {
	Float(lbls []Label, tMs int64, v float64)
	Hist(lbls []Label, tMs int64, h *Hist)
}

func sanitize(b *strings.Builder, s string) string {
	ok := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' && i > 0) {
			ok = false
			break
		}
	}
	if ok {
		return s
	}
	b.Reset()
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' && i > 0 {
			b.WriteByte(c)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// Converter holds scratch state and the delta-to-cumulative accumulators.
type Converter struct {
	// Native: histograms and exponential histograms go to Sink.Hist (D's
	// native-histogram variant) instead of classic series.
	Native bool
	sb     strings.Builder
	base   []Label
	lbls   []Label
	names  map[string]string
	acc    map[string]*[2]float64 // exp-hist count/sum accumulators, by series key
	key    strings.Builder
}

// New returns a converter.
func New(native bool) *Converter {
	return &Converter{Native: native, names: map[string]string{}, acc: map[string]*[2]float64{}}
}

func (c *Converter) name(s string) string {
	if n, ok := c.names[s]; ok {
		return n
	}
	n := sanitize(&c.sb, s)
	c.names[s] = n
	return n
}

func (c *Converter) emit(sink Sink, name string, extra []Label, t int64, v float64) {
	l := append(c.lbls[:0], c.base...)
	l = append(l, Label{"__name__", name})
	l = append(l, extra...)
	slices.SortFunc(l, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	c.lbls = l
	sink.Float(l, t, v)
}

// Walk converts md and feeds sink. It returns the number of float samples
// and native histogram samples emitted.
func (c *Converter) Walk(md pmetric.Metrics, sink Sink) (floats, hists int) {
	rms := md.ResourceMetrics()
	var pointBase int
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		c.base = c.base[:0]
		rm.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
			c.base = append(c.base, Label{c.name(k), v.AsString()})
			return true
		})
		pointBase = len(c.base)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				name := c.name(m.Name())
				pa := func(a pcommon.Map) {
					c.base = c.base[:pointBase]
					a.Range(func(k string, v pcommon.Value) bool {
						c.base = append(c.base, Label{c.name(k), v.AsString()})
						return true
					})
				}
				switch m.Type() {
				case pmetric.MetricTypeGauge, pmetric.MetricTypeSum:
					var dps pmetric.NumberDataPointSlice
					n := name
					if m.Type() == pmetric.MetricTypeGauge {
						dps = m.Gauge().DataPoints()
					} else {
						dps = m.Sum().DataPoints()
						if m.Sum().IsMonotonic() {
							n = name + "_total"
						}
					}
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						pa(dp.Attributes())
						v := dp.DoubleValue()
						if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
							v = float64(dp.IntValue())
						}
						c.emit(sink, n, nil, ms2(dp.Timestamp()), v)
						floats++
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						pa(dp.Attributes())
						t := ms2(dp.Timestamp())
						if c.Native {
							h := &Hist{Schema: -53, Bounds: dp.ExplicitBounds().AsRaw(), Counts: dp.BucketCounts().AsRaw(), Count: dp.Count(), Sum: dp.Sum()}
							l := append(c.lbls[:0], c.base...)
							l = append(l, Label{"__name__", name})
							slices.SortFunc(l, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
							c.lbls = l
							sink.Hist(l, t, h)
							hists++
							continue
						}
						var cum uint64
						b := dp.ExplicitBounds()
						for x := 0; x < dp.BucketCounts().Len(); x++ {
							cum += dp.BucketCounts().At(x)
							le := "+Inf"
							if x < b.Len() {
								le = strconv.FormatFloat(b.At(x), 'g', -1, 64)
							}
							c.emit(sink, name+"_bucket", []Label{{"le", le}}, t, float64(cum))
							floats++
						}
						c.emit(sink, name+"_sum", nil, t, dp.Sum())
						c.emit(sink, name+"_count", nil, t, float64(dp.Count()))
						floats += 2
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						pa(dp.Attributes())
						t := ms2(dp.Timestamp())
						if c.Native {
							h := &Hist{Schema: dp.Scale(), Offset: dp.Positive().Offset(), Counts: dp.Positive().BucketCounts().AsRaw(),
								ZeroCount: dp.ZeroCount(), Count: dp.Count(), Sum: dp.Sum(), Delta: true}
							l := append(c.lbls[:0], c.base...)
							l = append(l, Label{"__name__", name})
							slices.SortFunc(l, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
							c.lbls = l
							sink.Hist(l, t, h)
							hists++
							continue
						}
						c.key.Reset()
						c.key.WriteString(name)
						for _, l := range c.base {
							c.key.WriteString(l.Name)
							c.key.WriteByte(0)
							c.key.WriteString(l.Value)
							c.key.WriteByte(0)
						}
						a := c.acc[c.key.String()]
						if a == nil {
							a = &[2]float64{}
							c.acc[c.key.String()] = a
						}
						a[0] += float64(dp.Count())
						a[1] += dp.Sum()
						c.emit(sink, name+"_count", nil, t, a[0])
						c.emit(sink, name+"_sum", nil, t, a[1])
						floats += 2
					}
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						pa(dp.Attributes())
						t := ms2(dp.Timestamp())
						q := dp.QuantileValues()
						for x := 0; x < q.Len(); x++ {
							c.emit(sink, name, []Label{{"quantile", strconv.FormatFloat(q.At(x).Quantile(), 'g', -1, 64)}}, t, q.At(x).Value())
						}
						c.emit(sink, name+"_sum", nil, t, dp.Sum())
						c.emit(sink, name+"_count", nil, t, float64(dp.Count()))
						floats += q.Len() + 2
					}
				}
			}
		}
	}
	return
}

func ms2(ts pcommon.Timestamp) int64 { return int64(ts) / 1e6 }
