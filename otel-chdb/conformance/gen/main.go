// gen writes the conformance suite's own hostile requests, beyond
// otap-rs/tools/cmd/otlpgen's testgen / nasty / extra datasets, as
// ExportTrace/Logs/MetricsServiceRequest protobuf files:
//
//	traces-hostile.pb   logs-hostile.pb   metrics-hostile.pb
//
// Unicode everywhere (emoji, RTL, combining marks, CJK, NUL, U+2028, BOM,
// invalid UTF-8), empty and 1,000-entry maps, an empty key, 1 MiB values,
// nested maps and slices, empty bodies and names, every metric type with
// hundreds of buckets, exemplars with filtered attributes, exponential
// histograms at the scale limits, summaries with 100 quantiles, and series
// that repeat across resources and scopes.
//
//	gen -out DIR
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

var words = []string{
	"", "ascii", "héllo wörld", "日本語テキスト", "中文字符", "한국어", "עברית RTL ‏mark", "العربية",
	"emoji 🚀🔥👩‍👩‍👧‍👦 🇺🇳", "combining é ä", "zero​width", "line sep para",
	"\ufeffbom", "nul\x00byte", "tab\tnew\nline\r", `quote"back\slash'`, "invalid \xff\xfe utf8", "\xc3\x28",
	"surrogate-ish \xed\xa0\x80", "𝔘𝔫𝔦𝔠𝔬𝔡𝔢 math", "<script>&amp;</script>",
}

func word(i int) string { return words[((i%len(words))+len(words))%len(words)] }

const base = int64(1_790_000_000_000_000_000) // 2026-09-21

func putAll(m pcommon.Map, i int) {
	m.PutStr("str", word(i))
	m.PutStr(word(i+1), word(i+2)) // unicode keys, the empty key once
	m.PutInt("int", math.MinInt64+int64(i))
	m.PutDouble("double", []float64{math.NaN(), math.Inf(1), math.Inf(-1), -0.0, 1e-300, 5e21, 0.1}[i%7])
	m.PutBool("bool", i%2 == 0)
	m.PutEmptyBytes("bytes").FromRaw([]byte(word(i)))
	s := m.PutEmptySlice("slice")
	s.AppendEmpty().SetStr(word(i + 3))
	s.AppendEmpty().SetInt(int64(i))
	s.AppendEmpty() // empty value
	mm := m.PutEmptyMap("map")
	mm.PutStr("nested", word(i+4))
	mm.PutEmptyMap("deeper").PutDouble("nan", math.NaN())
	m.PutEmpty("empty")
}

func bigMap(m pcommon.Map, n int) {
	for k := 0; k < n; k++ {
		m.PutStr(fmt.Sprintf("k%04d-%s", n-k, word(k)), word(k*7))
	}
}

func huge(n int) string { return strings.Repeat("🚀x", n/5) }

func resource(r pcommon.Resource, i int) {
	a := r.Attributes()
	switch i % 4 {
	case 0:
		a.PutStr("service.name", word(i+8)) // unicode service
	case 1:
		a.PutInt("service.name", 42) // not a string
	case 2:
		// no service.name
	default:
		a.PutStr("service.name", "svc-"+word(i))
	}
	putAll(a, i)
	if i%3 == 0 {
		bigMap(a, 1000)
	}
}

func traces() ptrace.Traces {
	td := ptrace.NewTraces()
	for r := 0; r < 6; r++ {
		rs := td.ResourceSpans().AppendEmpty()
		resource(rs.Resource(), r)
		rs.SetSchemaUrl(word(r))
		for s := 0; s < 2; s++ {
			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName(word(r + s))
			ss.Scope().SetVersion(word(s))
			for k := 0; k < 40; k++ {
				i := r*100 + s*40 + k
				sp := ss.Spans().AppendEmpty()
				sp.SetName(word(i))
				if k%7 != 0 {
					sp.SetTraceID(pcommon.TraceID{byte(r), byte(s), byte(k), 0xff})
					sp.SetSpanID(pcommon.SpanID{byte(i), byte(i >> 8), 1})
				}
				if k%3 == 0 {
					sp.SetParentSpanID(pcommon.SpanID{9, byte(k)})
				}
				sp.TraceState().FromRaw(word(i + 5))
				sp.SetKind(ptrace.SpanKind(k % 7))
				sp.SetStartTimestamp(pcommon.Timestamp(base + int64(i)*1e6))
				sp.SetEndTimestamp(pcommon.Timestamp(base + int64(i)*1e6 - int64(k%2)*5)) // negative durations too
				sp.Status().SetCode(ptrace.StatusCode(k % 3))
				if k == 13 {
					sp.Status().SetCode(ptrace.StatusCode(9)) // outside the enum
				}
				sp.Status().SetMessage(word(i + 9))
				putAll(sp.Attributes(), i)
				switch k {
				case 1:
					if s == 0 {
						sp.Attributes().PutStr("huge", huge(1<<20)) // 1 MiB, once per resource
					}
				case 2:
					bigMap(sp.Attributes(), 1000)
				case 3:
					sp.Attributes().Clear()
				}
				for e := 0; e < k%4; e++ {
					ev := sp.Events().AppendEmpty()
					ev.SetName(word(i + e))
					ev.SetTimestamp(pcommon.Timestamp(base + int64(i)*1e6 + int64(e)))
					putAll(ev.Attributes(), i+e)
				}
				for l := 0; l < k%3; l++ {
					ln := sp.Links().AppendEmpty()
					ln.SetTraceID(pcommon.TraceID{byte(l), 7})
					ln.SetSpanID(pcommon.SpanID{byte(l), 8})
					ln.TraceState().FromRaw(word(l))
					putAll(ln.Attributes(), i+l)
				}
			}
		}
	}
	return td
}

func logs() plog.Logs {
	ld := plog.NewLogs()
	for r := 0; r < 6; r++ {
		rl := ld.ResourceLogs().AppendEmpty()
		resource(rl.Resource(), r)
		rl.SetSchemaUrl(word(r + 1))
		for s := 0; s < 2; s++ {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.SetSchemaUrl(word(s))
			sl.Scope().SetName(word(r + s + 2))
			sl.Scope().SetVersion(word(s + 3))
			putAll(sl.Scope().Attributes(), r+s)
			for k := 0; k < 40; k++ {
				i := r*100 + s*40 + k
				lr := sl.LogRecords().AppendEmpty()
				if k%5 != 0 {
					lr.SetTimestamp(pcommon.Timestamp(base + int64(i)*1e6))
				}
				lr.SetObservedTimestamp(pcommon.Timestamp(base + int64(i)*1e6 + 1))
				lr.SetSeverityNumber(plog.SeverityNumber(k % 25))
				if k == 13 {
					lr.SetSeverityNumber(plog.SeverityNumber(300)) // outside the enum and uint8
				}
				lr.SetSeverityText(word(i))
				lr.SetEventName(word(i + 2))
				lr.SetFlags(plog.LogRecordFlags(k % 2))
				if k%4 != 0 {
					lr.SetTraceID(pcommon.TraceID{byte(i), 1})
					lr.SetSpanID(pcommon.SpanID{byte(i), 2})
				}
				switch k % 6 {
				case 0:
					lr.Body().SetStr(word(i))
				case 1:
					if k == 1 && s == 0 {
						lr.Body().SetStr(huge(1 << 20)) // 1 MiB, once per resource
					} else {
						lr.Body().SetStr(huge(1000))
					}
				case 2:
					putAll(lr.Body().SetEmptyMap(), i)
				case 3:
					lr.Body().SetEmptySlice().AppendEmpty().SetDouble(math.Inf(1))
				case 4:
					lr.Body().SetEmptyBytes().FromRaw([]byte{0, 0xff})
				}
				putAll(lr.Attributes(), i)
				if k == 7 {
					bigMap(lr.Attributes(), 1000)
				}
			}
		}
	}
	return ld
}

func exemplars(ex pmetric.ExemplarSlice, i int) {
	for e := 0; e < i%4; e++ {
		x := ex.AppendEmpty()
		x.SetTimestamp(pcommon.Timestamp(base + int64(e)))
		if e%2 == 0 {
			x.SetDoubleValue([]float64{math.NaN(), math.Inf(-1), 3.25}[i%3])
		} else {
			x.SetIntValue(math.MaxInt64 - int64(e))
		}
		if e > 0 {
			x.SetTraceID(pcommon.TraceID{byte(i), byte(e)})
			x.SetSpanID(pcommon.SpanID{byte(e)})
		}
		putAll(x.FilteredAttributes(), i+e)
		if e == 2 {
			bigMap(x.FilteredAttributes(), 50)
		}
	}
}

func metrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := 0; r < 5; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		resource(rm.Resource(), r)
		rm.SetSchemaUrl(word(r + 3))
		for s := 0; s < 2; s++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.SetSchemaUrl(word(s + 4))
			sm.Scope().SetName(word(r*2 + s))
			sm.Scope().SetVersion(word(s))
			sm.Scope().SetDroppedAttributesCount(uint32(s * 7))
			putAll(sm.Scope().Attributes(), s)
			for mi := 0; mi < 6; mi++ {
				i := r*100 + s*10 + mi
				ts := pcommon.Timestamp(base + int64(i)*1e9)
				name := word(i)
				g := sm.Metrics().AppendEmpty()
				g.SetName(name + ".gauge")
				g.SetDescription(word(i + 1))
				g.SetUnit(word(i + 2))
				gd := g.SetEmptyGauge()
				for p := 0; p < 12; p++ {
					dp := gd.DataPoints().AppendEmpty()
					putAll(dp.Attributes(), p)
					dp.Attributes().PutInt("p", int64(p))
					if p == 11 {
						bigMap(dp.Attributes(), 300)
					}
					dp.SetTimestamp(ts)
					dp.SetStartTimestamp(ts - pcommon.Timestamp(p)*1e9)
					if p%2 == 0 {
						dp.SetDoubleValue([]float64{math.NaN(), -0.0, math.MaxFloat64, math.SmallestNonzeroFloat64}[p%4])
					} else {
						dp.SetIntValue(math.MinInt64 + int64(p))
					}
					dp.SetFlags(pmetric.DataPointFlags(p % 2))
					exemplars(dp.Exemplars(), i+p)
				}
				sm2 := sm.Metrics().AppendEmpty()
				sm2.SetName(name + ".sum")
				sd := sm2.SetEmptySum()
				sd.SetIsMonotonic(mi%2 == 0)
				sd.SetAggregationTemporality(pmetric.AggregationTemporality(mi % 3))
				for p := 0; p < 6; p++ {
					dp := sd.DataPoints().AppendEmpty()
					dp.Attributes().PutStr("route", word(p))
					dp.SetTimestamp(ts)
					dp.SetDoubleValue(float64(p) * 1.5)
					exemplars(dp.Exemplars(), p)
				}
				h := sm.Metrics().AppendEmpty()
				h.SetName(name + ".histogram")
				hd := h.SetEmptyHistogram()
				hd.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				for p := 0; p < 4; p++ {
					dp := hd.DataPoints().AppendEmpty()
					dp.Attributes().PutStr("le.set", word(p))
					dp.SetTimestamp(ts)
					nb := []int{0, 1, 10, 500}[p]
					bounds := make([]float64, nb)
					for b := range bounds {
						bounds[b] = math.Pow(1.1, float64(b)) - 1
					}
					dp.ExplicitBounds().FromRaw(bounds)
					counts := make([]uint64, nb+1)
					for b := range counts {
						counts[b] = uint64(b) * 1_000_003
					}
					if p == 3 {
						counts[0] = math.MaxUint64
					}
					dp.BucketCounts().FromRaw(counts)
					dp.SetCount(uint64(p) * 17)
					if p != 1 {
						dp.SetSum(-1e300 * float64(p))
						dp.SetMin(math.Inf(-1))
						dp.SetMax(math.NaN())
					}
					exemplars(dp.Exemplars(), p+1)
				}
				e := sm.Metrics().AppendEmpty()
				e.SetName(name + ".exp")
				ed := e.SetEmptyExponentialHistogram()
				ed.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				for p := 0; p < 3; p++ {
					dp := ed.DataPoints().AppendEmpty()
					dp.Attributes().PutStr("kind", word(p+10))
					dp.SetTimestamp(ts)
					dp.SetScale([]int32{-10, 0, 20}[p])
					dp.SetZeroCount(uint64(p))
					dp.SetZeroThreshold(1e-9)
					dp.Positive().SetOffset([]int32{math.MinInt32, -5, math.MaxInt32 - 2000}[p])
					pos := make([]uint64, []int{0, 3, 1000}[p])
					for b := range pos {
						pos[b] = uint64(b * b)
					}
					dp.Positive().BucketCounts().FromRaw(pos)
					dp.Negative().SetOffset(int32(-p))
					dp.Negative().BucketCounts().FromRaw([]uint64{uint64(p), 0, math.MaxUint64})
					dp.SetCount(uint64(p) * 1000)
					if p > 0 {
						dp.SetSum(float64(p))
						dp.SetMin(-float64(p))
						dp.SetMax(float64(p))
					}
					exemplars(dp.Exemplars(), p+2)
				}
				su := sm.Metrics().AppendEmpty()
				su.SetName(name + ".summary")
				sud := su.SetEmptySummary()
				for p := 0; p < 2; p++ {
					dp := sud.DataPoints().AppendEmpty()
					dp.Attributes().PutStr("q", word(p))
					dp.SetTimestamp(ts)
					dp.SetCount(uint64(p))
					dp.SetSum(math.Inf(1))
					for q := 0; q < []int{0, 100}[p]; q++ {
						v := dp.QuantileValues().AppendEmpty()
						v.SetQuantile(float64(q) / 100)
						v.SetValue(float64(q) * math.Pi)
					}
				}
			}
		}
	}
	return md
}

func main() {
	out := flag.String("out", "", "output directory")
	flag.Parse()
	if *out == "" {
		log.Fatal("-out is required")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	tb, err := ptraceotlp.NewExportRequestFromTraces(traces()).MarshalProto()
	if err != nil {
		log.Fatal(err)
	}
	lb, err := plogotlp.NewExportRequestFromLogs(logs()).MarshalProto()
	if err != nil {
		log.Fatal(err)
	}
	mb, err := pmetricotlp.NewExportRequestFromMetrics(metrics()).MarshalProto()
	if err != nil {
		log.Fatal(err)
	}
	for name, b := range map[string][]byte{"traces-hostile.pb": tb, "logs-hostile.pb": lb, "metrics-hostile.pb": mb} {
		p := filepath.Join(*out, name)
		if err := os.WriteFile(p, b, 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\t%d bytes\n", p, len(b))
	}
}
