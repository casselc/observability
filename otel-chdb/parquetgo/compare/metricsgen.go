package compare

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Metrics test data. MetricsBatch is shaped like what an OTel SDK fleet
// exports on a 10 s interval: 20 resources (5 services x 4 pods) with
// k8s/host/SDK resource attributes, one instrumentation scope each, 10
// metric names per type, 10 attribute sets per metric (HTTP method, route,
// status), so 2,000 series per type. A batch holds consecutive collection
// rounds of every series; batch b continues where batch b-1 stopped, so a
// sequence of batches is a contiguous time series (cumulative counters keep
// growing, gauges random-walk). Deterministic.

// MetricsEpoch is the start time of every cumulative series.
var MetricsEpoch = time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

const (
	genServices  = 5
	genPods      = 4
	genNames     = 10
	genAttrSets  = 10
	genSeries    = genServices * genPods * genNames * genAttrSets // per type
	genInterval  = 10 * time.Second
	genSDKBounds = "0,5,10,25,50,75,100,250,500,750,1000,2500,5000,7500,10000"
)

var (
	genSvcNames = []string{"frontend", "checkout", "payments", "catalog", "shipping"}
	genMethods  = []string{"GET", "GET", "GET", "POST", "PUT"}
	genRoutes   = []string{"/api/cart", "/api/checkout", "/api/products/{id}", "/healthz", "/api/orders"}
	genStatus   = []int64{200, 200, 200, 201, 404, 500}
	sdkBounds   = []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000}
	genNamesBy  = [parquetgo.NumMetricTypes][]string{
		{"process.memory.usage", "system.cpu.utilization", "jvm.memory.used", "go.goroutine.count", "db.client.connection.count", "http.server.active_requests", "runtime.heap.alloc", "queue.depth", "cache.hit_ratio", "disk.io.utilization"},
		{"http.server.request.count", "http.client.request.count", "db.client.operation.count", "messaging.process.count", "process.cpu.time", "system.network.io", "rpc.server.requests", "errors.total", "bytes.sent", "gc.collections"},
		{"http.server.request.duration", "http.client.request.duration", "db.client.operation.duration", "rpc.server.duration", "messaging.process.duration", "http.server.request.body.size", "http.server.response.body.size", "queue.wait.time", "gc.pause", "cache.lookup.duration"},
		{"http.server.request.duration.exp", "http.client.request.duration.exp", "db.client.operation.duration.exp", "rpc.server.duration.exp", "messaging.process.duration.exp", "request.size.exp", "response.size.exp", "queue.wait.exp", "gc.pause.exp", "cache.lookup.exp"},
		{"legacy.request.latency", "legacy.db.latency", "legacy.gc.pause", "legacy.queue.latency", "legacy.render.time", "legacy.cache.latency", "legacy.io.latency", "legacy.lock.wait", "legacy.rpc.latency", "legacy.job.duration"},
	}
	genUnits = [parquetgo.NumMetricTypes]string{"By", "{request}", "ms", "ms", "s"}
)

// Metrics returns n data points of every metric type (batch 0).
func Metrics(n int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for t := range parquetgo.NumMetricTypes {
		addMetricsBatch(md, t, n, 0)
	}
	return md
}

// MetricsBatch returns n data points of type t, for batch number b of a
// contiguous stream.
func MetricsBatch(t parquetgo.MetricType, n, b int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	addMetricsBatch(md, t, n, b)
	return md
}

func addMetricsBatch(md pmetric.Metrics, t parquetgo.MetricType, n, b int) {
	// Point p of the stream is series p%genSeries in round p/genSeries.
	first := b * n
	for p := first; p < first+n; {
		round := p / genSeries
		s := p % genSeries
		res := s / (genNames * genAttrSets)
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
		setGenResource(rm.Resource().Attributes(), res)
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("go.opentelemetry.io/otel/sdk/metric")
		sm.Scope().SetVersion("1.38.0")
		ts := pcommon.NewTimestampFromTime(MetricsEpoch.Add(time.Duration(round+1) * genInterval))
		// Every metric of this resource for this round, as one export does.
		for p < first+n && p/genSeries == round && (p%genSeries)/(genNames*genAttrSets) == res {
			s = p % genSeries
			name := (s / genAttrSets) % genNames
			m := sm.Metrics().AppendEmpty()
			m.SetName(genNamesBy[t][name])
			m.SetDescription("Measures " + genNamesBy[t][name])
			m.SetUnit(genUnits[t])
			for p < first+n && p/genSeries == round && (p%genSeries)/genAttrSets == s/genAttrSets {
				a := p % genAttrSets
				addGenPoint(m, t, p%genSeries, a, round, ts)
				p++
			}
		}
	}
}

func setGenResource(ra pcommon.Map, res int) {
	svc, pod := genSvcNames[res/genPods], res%genPods
	ra.PutStr("service.name", svc)
	ra.PutStr("service.namespace", "shop")
	ra.PutStr("service.version", "1.4.2")
	ra.PutStr("service.instance.id", fmt.Sprintf("6f1c2a4e-%04x-4b1e-9d3a-%012x", res, res*7919))
	ra.PutStr("host.name", fmt.Sprintf("ip-10-0-%d-%d.eu-west-1.compute.internal", res%3, 10+res))
	ra.PutStr("k8s.namespace.name", "shop")
	ra.PutStr("k8s.pod.name", fmt.Sprintf("%s-7d9f8b6c4-%c%c%c%c%c", svc, 'a'+pod, 'k'+pod, 'q', 'x'+pod%2, 'm'))
	ra.PutStr("k8s.node.name", fmt.Sprintf("ip-10-0-%d-%d", res%3, 10+res%6))
	ra.PutStr("cloud.region", "eu-west-1")
	ra.PutStr("telemetry.sdk.language", "go")
	ra.PutStr("telemetry.sdk.name", "opentelemetry")
	ra.PutStr("telemetry.sdk.version", "1.38.0")
}

// genRand is a per-(series, round) generator, so points don't depend on
// batch boundaries.
func genRand(series, round int) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(series)*0x9e3779b97f4a7c15, uint64(round)))
}

func setGenAttrs(m pcommon.Map, a int) {
	m.PutStr("http.request.method", genMethods[a%len(genMethods)])
	m.PutStr("http.route", genRoutes[a%len(genRoutes)])
	m.PutInt("http.response.status_code", genStatus[a%len(genStatus)])
	m.PutStr("network.protocol.version", "1.1")
}

func addGenPoint(m pmetric.Metric, t parquetgo.MetricType, series, a, round int, ts pcommon.Timestamp) {
	start := pcommon.NewTimestampFromTime(MetricsEpoch)
	r := genRand(series, round)
	// Cumulative "request count so far" for this series, smooth in round.
	rate := 1 + float64(series%37)
	total := uint64(rate * float64(round+1) * 10)
	switch t {
	case parquetgo.MetricGauge:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptyGauge()
		}
		dp := m.Gauge().DataPoints().AppendEmpty()
		setGenAttrs(dp.Attributes(), a)
		dp.SetTimestamp(ts)
		if series%2 == 0 {
			dp.SetIntValue(int64(1e6 + series*1000 + round*17 + r.IntN(4096)))
		} else {
			dp.SetDoubleValue(0.2 + 0.1*math.Sin(float64(round)/7+float64(series)) + r.Float64()*0.01)
		}
	case parquetgo.MetricSum:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			m.Sum().SetIsMonotonic(true)
		}
		dp := m.Sum().DataPoints().AppendEmpty()
		setGenAttrs(dp.Attributes(), a)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		if series%3 == 0 {
			dp.SetDoubleValue(float64(total) * 0.013)
		} else {
			dp.SetIntValue(int64(total))
		}
	case parquetgo.MetricHistogram:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		}
		dp := m.Histogram().DataPoints().AppendEmpty()
		setGenAttrs(dp.Attributes(), a)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		dp.ExplicitBounds().FromRaw(sdkBounds)
		counts := make([]uint64, len(sdkBounds)+1)
		var sum float64
		for i := range counts {
			// A log-normal-ish latency shape, cumulative over rounds.
			w := math.Exp(-math.Pow(float64(i)-5-float64(series%4), 2) / 6)
			counts[i] = uint64(w * float64(total))
			sum += float64(counts[i]) * float64(sdkBounds[min(i, len(sdkBounds)-1)]) * 0.7
		}
		var c uint64
		for _, v := range counts {
			c += v
		}
		dp.BucketCounts().FromRaw(counts)
		dp.SetCount(c)
		dp.SetSum(sum)
		dp.SetMin(0.3 + float64(series%5))
		dp.SetMax(800 + float64(r.IntN(9000)))
		if r.IntN(10) == 0 {
			addGenExemplar(dp.Exemplars(), r, ts)
		}
	case parquetgo.MetricExponentialHistogram:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptyExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		}
		dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
		setGenAttrs(dp.Attributes(), a)
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(MetricsEpoch.Add(time.Duration(round) * genInterval)))
		dp.SetTimestamp(ts)
		dp.SetScale(3)
		dp.Positive().SetOffset(int32(20 + series%8))
		nb := 24 + r.IntN(24)
		counts := make([]uint64, nb)
		var c uint64
		for i := range counts {
			counts[i] = uint64(r.IntN(1 + int(8*math.Exp(-math.Pow(float64(i-nb/2), 2)/40))))
			c += counts[i]
		}
		dp.Positive().BucketCounts().FromRaw(counts)
		dp.SetZeroCount(uint64(r.IntN(2)))
		c += dp.ZeroCount()
		dp.SetCount(c)
		dp.SetSum(float64(c) * (5 + r.Float64()*50))
		dp.SetMin(0.5)
		dp.SetMax(float64(100 + r.IntN(400)))
		if r.IntN(10) == 0 {
			addGenExemplar(dp.Exemplars(), r, ts)
		}
	case parquetgo.MetricSummary:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptySummary()
		}
		dp := m.Summary().DataPoints().AppendEmpty()
		setGenAttrs(dp.Attributes(), a)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		dp.SetCount(total)
		dp.SetSum(float64(total) * (12 + float64(series%9)))
		for _, q := range []float64{0, 0.5, 0.9, 0.99, 1} {
			v := dp.QuantileValues().AppendEmpty()
			v.SetQuantile(q)
			v.SetValue(math.Round((1+q*q*40)*(5+float64(series%9))*100+float64(r.IntN(100))) / 100)
		}
	}
}

func addGenExemplar(ex pmetric.ExemplarSlice, r *rand.Rand, ts pcommon.Timestamp) {
	e := ex.AppendEmpty()
	e.SetTimestamp(ts - pcommon.Timestamp(r.IntN(10e9)))
	e.SetDoubleValue(float64(r.IntN(100000)) / 100)
	var tid pcommon.TraceID
	var sid pcommon.SpanID
	for i := range tid {
		tid[i] = byte(r.Uint32())
	}
	for i := range sid {
		sid[i] = byte(r.Uint32())
	}
	e.SetTraceID(tid)
	e.SetSpanID(sid)
}

// NastyMetrics builds n data points of every type full of hostile values:
// int and double values incl. NaN/±Inf/-0/extremes, exemplars (int, double,
// empty, zero ids, zero time, many), empty and huge attribute maps, every
// attribute value type, start time 0 and extreme timestamps, exponential
// histograms with zero counts, negative buckets and extreme scales, explicit
// histograms with mismatched bounds, unset sum/min/max, summaries with
// quantiles (and none), invalid UTF-8 everywhere, flags, temporality and
// monotonicity variants, schema URLs and dropped attribute counts.
func NastyMetrics(n int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	times := []uint64{0, 1, 999_999_999, 1_000_000_000, uint64(MetricsEpoch.UnixNano()) + 999_999_999,
		math.MaxUint32 * 1e9, math.MaxUint32*1e9 + 1e9 + 5, math.MaxInt64, 1 << 63, math.MaxUint64}
	pt := 0 // running point counter: drives every variant
	for i := 0; i < n; {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl(nastyStrings[i%len(nastyStrings)])
		ra := rm.Resource().Attributes()
		switch i % 3 {
		case 0:
			ra.PutStr("service.name", nastyStrings[i%len(nastyStrings)])
		case 1:
			ra.PutDouble("service.name", nastyDoubles[i%len(nastyDoubles)])
		}
		putNastyAttrs(ra, i, (i*3)%7)
		for sc := 0; sc < 2 && i < n; sc++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.SetSchemaUrl(nastyStrings[(i+sc+3)%len(nastyStrings)])
			sm.Scope().SetName(nastyStrings[(i+sc)%len(nastyStrings)])
			sm.Scope().SetVersion(nastyStrings[(i+sc+1)%len(nastyStrings)])
			sm.Scope().SetDroppedAttributesCount(uint32(i * 1_000_003))
			putNastyAttrs(sm.Scope().Attributes(), i+sc, i%4)
			for t := range parquetgo.NumMetricTypes {
				m := sm.Metrics().AppendEmpty()
				m.SetName(nastyStrings[(i+int(t))%len(nastyStrings)])
				m.SetDescription(nastyStrings[(i+int(t)+5)%len(nastyStrings)])
				m.SetUnit(nastyStrings[(i+int(t)+9)%len(nastyStrings)])
				k := 1 + (i/5+int(t))%5
				for d := 0; d < k; d++ {
					nastyPoint(m, t, pt, times)
					pt++
				}
			}
			i += 5
		}
	}
	return md
}

func nastyNumber(dp pmetric.NumberDataPoint, j int) {
	switch j % 3 {
	case 0:
		dp.SetDoubleValue(nastyDoubles[(j/3)%len(nastyDoubles)])
	case 1:
		dp.SetIntValue(nastyInts[(j/3)%len(nastyInts)])
	}
	// case 2: value left unset (Empty)
}

func nastyExemplars(ex pmetric.ExemplarSlice, j int, times []uint64) {
	k := []int{0, 1, 2, 0, 30}[j%5]
	for x := 0; x < k; x++ {
		e := ex.AppendEmpty()
		e.SetTimestamp(pcommon.Timestamp(times[(j+x)%len(times)]))
		switch (j + x) % 3 {
		case 0:
			e.SetDoubleValue(nastyDoubles[((j+x)/3)%len(nastyDoubles)])
		case 1:
			e.SetIntValue(nastyInts[((j+x)/3)%len(nastyInts)])
		}
		if (j+x)%4 != 0 {
			e.SetTraceID(pcommon.TraceID{byte(j), 0xff, byte(x), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID(pcommon.SpanID{0, byte(x), 0xab, 0, 0, 0, 0, byte(j)})
		}
		putNastyAttrs(e.FilteredAttributes(), j+x, (j+x)%4)
	}
}

func nastyPoint(m pmetric.Metric, t parquetgo.MetricType, j int, times []uint64) {
	start := pcommon.Timestamp(times[j%len(times)])
	ts := pcommon.Timestamp(times[(j*3+1)%len(times)])
	if j%4 == 0 {
		start = 0
	}
	attrN := []int{0, 1, 3, 0, 60}[j%5] // 60: a huge map, with 100 KB values
	flags := pmetric.DataPointFlags(uint32(j) * 0x9e3779b9)
	temps := []pmetric.AggregationTemporality{pmetric.AggregationTemporalityUnspecified, pmetric.AggregationTemporalityDelta, pmetric.AggregationTemporalityCumulative}
	switch t {
	case parquetgo.MetricGauge:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptyGauge()
		}
		dp := m.Gauge().DataPoints().AppendEmpty()
		putNastyAttrs(dp.Attributes(), j, attrN)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		dp.SetFlags(flags)
		nastyNumber(dp, j)
		nastyExemplars(dp.Exemplars(), j, times)
	case parquetgo.MetricSum:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptySum().SetAggregationTemporality(temps[j%3])
			m.Sum().SetIsMonotonic(j%2 == 0)
		}
		dp := m.Sum().DataPoints().AppendEmpty()
		putNastyAttrs(dp.Attributes(), j, attrN)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		dp.SetFlags(flags)
		nastyNumber(dp, j+1)
		nastyExemplars(dp.Exemplars(), j+1, times)
	case parquetgo.MetricHistogram:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptyHistogram().SetAggregationTemporality(temps[(j+1)%3])
		}
		dp := m.Histogram().DataPoints().AppendEmpty()
		putNastyAttrs(dp.Attributes(), j, attrN)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		dp.SetFlags(flags)
		switch j % 4 {
		case 0: // empty histogram, unset sum/min/max
		case 1:
			dp.ExplicitBounds().FromRaw([]float64{math.Inf(-1), -0.0, 1, math.NaN(), math.MaxFloat64})
			dp.BucketCounts().FromRaw([]uint64{0, math.MaxUint64, 1, 2, 3, 4})
			dp.SetCount(math.MaxUint64)
			dp.SetSum(math.NaN())
			dp.SetMin(math.Inf(-1))
			dp.SetMax(math.Inf(1))
		case 2: // mismatched lengths
			dp.ExplicitBounds().FromRaw([]float64{1, 2, 3})
			dp.BucketCounts().FromRaw([]uint64{7})
			dp.SetCount(7)
			dp.SetSum(-0.0)
		case 3:
			b := make([]float64, 200)
			c := make([]uint64, 201)
			for x := range b {
				b[x] = float64(x) * 1.5
				c[x] = uint64(x * x)
			}
			dp.ExplicitBounds().FromRaw(b)
			dp.BucketCounts().FromRaw(c)
			dp.SetCount(1 << 40)
			dp.SetSum(5e-324)
			dp.SetMin(-1e308)
			dp.SetMax(1e308)
		}
		nastyExemplars(dp.Exemplars(), j+2, times)
	case parquetgo.MetricExponentialHistogram:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptyExponentialHistogram().SetAggregationTemporality(temps[(j+2)%3])
		}
		dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
		putNastyAttrs(dp.Attributes(), j, attrN)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		dp.SetFlags(flags)
		dp.SetScale([]int32{0, -10, 20, math.MinInt32, math.MaxInt32}[j%5])
		switch j % 4 {
		case 0: // only zero count
			dp.SetZeroCount(12)
			dp.SetCount(12)
			dp.SetZeroThreshold(1e-9)
		case 1: // negative buckets, negative offsets
			dp.Negative().SetOffset(-5)
			dp.Negative().BucketCounts().FromRaw([]uint64{1, 0, 0, 4, math.MaxUint64})
			dp.Positive().SetOffset(math.MinInt32)
			dp.Positive().BucketCounts().FromRaw([]uint64{0})
			dp.SetCount(5)
			dp.SetSum(-123.25)
			dp.SetMin(-1e9)
			dp.SetMax(math.NaN())
		case 2: // zero counts everywhere
			dp.Positive().SetOffset(math.MaxInt32)
			dp.Positive().BucketCounts().FromRaw([]uint64{0, 0, 0})
			dp.Negative().BucketCounts().FromRaw([]uint64{0})
		case 3:
			c := make([]uint64, 160)
			for x := range c {
				c[x] = uint64(x)
			}
			dp.Positive().SetOffset(-80)
			dp.Positive().BucketCounts().FromRaw(c)
			dp.Negative().SetOffset(3)
			dp.Negative().BucketCounts().FromRaw(c[:40])
			dp.SetCount(99999)
			dp.SetSum(math.Inf(1))
		}
		nastyExemplars(dp.Exemplars(), j+3, times)
	case parquetgo.MetricSummary:
		if m.Type() == pmetric.MetricTypeEmpty {
			m.SetEmptySummary()
		}
		dp := m.Summary().DataPoints().AppendEmpty()
		putNastyAttrs(dp.Attributes(), j, attrN)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(ts)
		dp.SetFlags(flags)
		dp.SetCount(uint64(j) * 0x1234567890)
		dp.SetSum(nastyDoubles[j%len(nastyDoubles)])
		for x := 0; x < []int{0, 1, 5, 2}[j%4]; x++ {
			q := dp.QuantileValues().AppendEmpty()
			q.SetQuantile([]float64{0, 0.5, 1, math.NaN(), -0.0}[x%5])
			q.SetValue(nastyDoubles[(j+x)%len(nastyDoubles)])
		}
	}
}
