// hdxgen sends a small, shop-shaped telemetry set over OTLP/HTTP (protobuf)
// for the HyperDX evaluation (../../README.md): traces that cross five
// services (so the waterfall and the service map have something to draw),
// logs tied to those spans, and metrics of every OTLP type with the
// temporalities SDKs actually send:
//
//	gauge      container.cpu.utilization, container.memory.working_set, system.cpu.utilization{state}
//	sum        http.server.request.count      cumulative, monotonic   {http.route, http.response.status_code}
//	           app.orders.placed              delta, monotonic        {payment.method}          (checkout)
//	           db.client.connections.usage    cumulative, up-down     {state}                   (payment)
//	histogram  http.server.request.duration   cumulative, explicit bounds, exemplars {http.route}
//	exp hist   rpc.server.duration            delta, scale 3                                    (checkout, payment)
//	summary    jvm.gc.pause                   cumulative quantiles {gc}                         (inventory)
//	           app.custom.metric.N            -extra-metrics more gauge names, to grow the metric picker
//
// Points are stamped from now-backfill to now every -step, deterministically
// (seeded), then optionally every -step in real time (-live).
//
//	hdxgen -url http://127.0.0.1:14518 [-also-metrics http://127.0.0.1:14528] -backfill 3h -step 30s
//	       [-pods 2] [-traces 8] [-extra-metrics 0] [-live 0] [-cluster NAME] [-signals traces,logs,metrics]
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

var services = []string{"frontend", "checkout", "payment", "inventory", "cart"}

var routes = map[string][]string{
	"frontend":  {"/api/checkout", "/api/cart", "/api/products"},
	"checkout":  {"/checkout"},
	"payment":   {"/charge"},
	"inventory": {"/reserve", "/stock"},
	"cart":      {"/cart/items"},
}

var (
	url      = flag.String("url", "http://127.0.0.1:14518", "OTLP/HTTP base URL")
	backfill = flag.Duration("backfill", 3*time.Hour, "history to generate before now")
	step     = flag.Duration("step", 30*time.Second, "interval between points / batches")
	pods     = flag.Int("pods", 2, "pods per service")
	nTraces  = flag.Int("traces", 8, "traces per step")
	extra    = flag.Int("extra-metrics", 0, "extra gauge names per pod")
	live     = flag.Duration("live", 0, "keep sending in real time for this long after the backfill")
	seed     = flag.Int64("seed", 1, "random seed")
	only     = flag.String("signals", "traces,logs,metrics", "signals to send")
	alsoM    = flag.String("also-metrics", "", "comma-separated OTLP/HTTP base URLs that get the same metrics requests too")
	cluster  = flag.String("cluster", "", "k8s.cluster.name, also prefixed to pod names (to add pods beside another run)")
)

var rng *rand.Rand

type pod struct {
	svc, name, host string
	start           time.Time
	// cumulative state
	reqs    map[string]float64 // route|code -> count
	hist    map[string][]uint64
	histSum map[string]float64
	histN   map[string]uint64
	gcN     uint64
	gcSum   float64
}

var bounds = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

func resource(r pcommon.Resource, p *pod) {
	a := r.Attributes()
	a.PutStr("service.name", p.svc)
	a.PutStr("service.version", "1.4."+fmt.Sprint(len(p.svc)%3))
	a.PutStr("service.namespace", "shop")
	a.PutStr("deployment.environment", "prod")
	a.PutStr("k8s.namespace.name", "shop")
	if *cluster != "" {
		a.PutStr("k8s.cluster.name", *cluster)
	}
	a.PutStr("k8s.pod.name", p.name)
	a.PutStr("k8s.node.name", p.host)
	a.PutStr("host.name", p.host)
	a.PutStr("cloud.region", []string{"us-east-1", "eu-west-1"}[len(p.name)%2])
	a.PutStr("telemetry.sdk.language", map[string]string{"frontend": "nodejs", "checkout": "go", "payment": "go", "inventory": "java", "cart": "dotnet"}[p.svc])
}

func ts(t time.Time) pcommon.Timestamp { return pcommon.NewTimestampFromTime(t) }

func post(path string, body []byte) {
	postTo(*url, path, body)
	if path == "/v1/metrics" && *alsoM != "" {
		for _, u := range strings.Split(*alsoM, ",") {
			postTo(u, path, body)
		}
	}
}

func postTo(base, path string, body []byte) {
	for attempt := 0; ; attempt++ {
		resp, err := http.Post(base+path, "application/x-protobuf", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		if attempt > 50 {
			log.Fatalf("%s: %v", path, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func traceID() pcommon.TraceID {
	var b [16]byte
	rng.Read(b[:])
	return b
}
func spanID() pcommon.SpanID {
	var b [8]byte
	rng.Read(b[:])
	return b
}

type spanRec struct {
	p    *pod
	span ptrace.Span
}

// one request through the shop; returns spans grouped later by pod.
func request(t time.Time, podsBy map[string][]*pod, out *[]spanRec, logs *[]logRec) {
	tid := traceID()
	pick := func(svc string) *pod { ps := podsBy[svc]; return ps[rng.Intn(len(ps))] }
	route := routes["frontend"][rng.Intn(3)]
	fail := rng.Float64() < 0.04
	var mk func(p *pod, parent pcommon.SpanID, name string, kind ptrace.SpanKind, start time.Time, dur time.Duration, attrs map[string]any) ptrace.Span
	mk = func(p *pod, parent pcommon.SpanID, name string, kind ptrace.SpanKind, start time.Time, dur time.Duration, attrs map[string]any) ptrace.Span {
		s := ptrace.NewSpan()
		s.SetTraceID(tid)
		s.SetSpanID(spanID())
		s.SetParentSpanID(parent)
		s.SetName(name)
		s.SetKind(kind)
		s.SetStartTimestamp(ts(start))
		s.SetEndTimestamp(ts(start.Add(dur)))
		for k, v := range attrs {
			switch x := v.(type) {
			case string:
				s.Attributes().PutStr(k, x)
			case int:
				s.Attributes().PutInt(k, int64(x))
			}
		}
		*out = append(*out, spanRec{p, s})
		return s
	}
	fe := pick("frontend")
	total := time.Duration(20+rng.Intn(400)) * time.Millisecond
	code := 200
	if fail {
		code = 500
	}
	root := mk(fe, pcommon.SpanID{}, "GET "+route, ptrace.SpanKindServer, t, total,
		map[string]any{"http.request.method": "GET", "http.route": route, "http.response.status_code": code,
			"user.id": fmt.Sprintf("user-%d", rng.Intn(200)), "url.path": route})
	if fail {
		root.Status().SetCode(ptrace.StatusCodeError)
		root.Status().SetMessage("upstream failed")
	}
	*logs = append(*logs, logRec{fe, t.Add(total), tid, root.SpanID(), sev(fail), fmt.Sprintf("%s %s -> %d in %dms", "GET", route, code, total.Milliseconds()),
		map[string]string{"http.route": route, "http.response.status_code": fmt.Sprint(code)}})
	down := map[string]string{"/api/checkout": "checkout", "/api/cart": "cart", "/api/products": "inventory"}[route]
	dp := pick(down)
	cstart := t.Add(2 * time.Millisecond)
	cdur := total - 5*time.Millisecond
	client := mk(fe, root.SpanID(), "POST "+routes[down][0], ptrace.SpanKindClient, cstart, cdur,
		map[string]any{"http.request.method": "POST", "server.address": down, "http.response.status_code": code})
	srv := mk(dp, client.SpanID(), "POST "+routes[down][0], ptrace.SpanKindServer, cstart.Add(time.Millisecond), cdur-2*time.Millisecond,
		map[string]any{"http.request.method": "POST", "http.route": routes[down][0], "http.response.status_code": code})
	*logs = append(*logs, logRec{dp, cstart.Add(cdur / 2), tid, srv.SpanID(), sev(fail), "handled " + routes[down][0],
		map[string]string{"http.route": routes[down][0]}})
	if down == "checkout" {
		mk(dp, srv.SpanID(), "compute totals", ptrace.SpanKindInternal, cstart.Add(2*time.Millisecond), 3*time.Millisecond, map[string]any{"cart.items": 1 + rng.Intn(6)})
		pp := pick("payment")
		pc := mk(dp, srv.SpanID(), "POST /charge", ptrace.SpanKindClient, cstart.Add(6*time.Millisecond), cdur/2, map[string]any{"server.address": "payment"})
		ps := mk(pp, pc.SpanID(), "POST /charge", ptrace.SpanKindServer, cstart.Add(7*time.Millisecond), cdur/2-2*time.Millisecond,
			map[string]any{"http.route": "/charge", "payment.method": []string{"card", "paypal", "voucher"}[rng.Intn(3)], "http.response.status_code": code})
		mk(pp, ps.SpanID(), "INSERT payments", ptrace.SpanKindClient, cstart.Add(9*time.Millisecond), 4*time.Millisecond,
			map[string]any{"db.system": "postgresql", "db.name": "payments", "db.statement": "INSERT INTO payments VALUES ($1,$2,$3)", "server.address": "postgres"})
		if fail {
			ps.Status().SetCode(ptrace.StatusCodeError)
			ps.Status().SetMessage("card declined: insufficient funds")
			*logs = append(*logs, logRec{pp, cstart.Add(8 * time.Millisecond), tid, ps.SpanID(), plog.SeverityNumberError, "payment failed: card declined: insufficient funds",
				map[string]string{"payment.method": "card", "error.type": "CardDeclined"}})
		}
		ip := pick("inventory")
		ic := mk(dp, srv.SpanID(), "POST /reserve", ptrace.SpanKindClient, cstart.Add(cdur/2+8*time.Millisecond), cdur/4, map[string]any{"server.address": "inventory"})
		is := mk(ip, ic.SpanID(), "POST /reserve", ptrace.SpanKindServer, cstart.Add(cdur/2+9*time.Millisecond), cdur/4-2*time.Millisecond, map[string]any{"http.route": "/reserve"})
		mk(ip, is.SpanID(), "SET", ptrace.SpanKindClient, cstart.Add(cdur/2+10*time.Millisecond), time.Millisecond, map[string]any{"db.system": "redis", "server.address": "redis"})
	} else {
		mk(dp, srv.SpanID(), "SELECT", ptrace.SpanKindClient, cstart.Add(3*time.Millisecond), cdur/3,
			map[string]any{"db.system": "redis", "db.operation.name": "GET", "server.address": "redis"})
	}
}

type logRec struct {
	p     *pod
	t     time.Time
	tid   pcommon.TraceID
	sid   pcommon.SpanID
	sev   plog.SeverityNumber
	body  string
	attrs map[string]string
}

func sev(fail bool) plog.SeverityNumber {
	if fail {
		return plog.SeverityNumberError
	}
	if rng.Float64() < 0.1 {
		return plog.SeverityNumberWarn
	}
	return plog.SeverityNumberInfo
}

func sevText(s plog.SeverityNumber) string {
	switch s {
	case plog.SeverityNumberError:
		return "ERROR"
	case plog.SeverityNumberWarn:
		return "WARN"
	}
	return "INFO"
}

func sendTraces(recs []spanRec) {
	td := ptrace.NewTraces()
	by := map[*pod]ptrace.SpanSlice{}
	for _, r := range recs {
		ss, ok := by[r.p]
		if !ok {
			rs := td.ResourceSpans().AppendEmpty()
			resource(rs.Resource(), r.p)
			sc := rs.ScopeSpans().AppendEmpty()
			sc.Scope().SetName("hdxgen/" + r.p.svc)
			sc.Scope().SetVersion("0.1.0")
			ss = sc.Spans()
			by[r.p] = ss
		}
		r.span.CopyTo(ss.AppendEmpty())
	}
	b, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		log.Fatal(err)
	}
	post("/v1/traces", b)
}

func sendLogs(recs []logRec) {
	ld := plog.NewLogs()
	by := map[*pod]plog.LogRecordSlice{}
	for _, r := range recs {
		ls, ok := by[r.p]
		if !ok {
			rl := ld.ResourceLogs().AppendEmpty()
			resource(rl.Resource(), r.p)
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName("hdxgen/" + r.p.svc)
			ls = sl.LogRecords()
			by[r.p] = ls
		}
		l := ls.AppendEmpty()
		l.SetTimestamp(ts(r.t))
		l.SetObservedTimestamp(ts(r.t))
		l.SetTraceID(r.tid)
		l.SetSpanID(r.sid)
		l.SetSeverityNumber(r.sev)
		l.SetSeverityText(sevText(r.sev))
		l.Body().SetStr(r.body)
		for k, v := range r.attrs {
			l.Attributes().PutStr(k, v)
		}
	}
	b, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		log.Fatal(err)
	}
	post("/v1/logs", b)
}

func metric(sm pmetric.ScopeMetrics, name, desc, unit string) pmetric.Metric {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	m.SetUnit(unit)
	return m
}

func sendMetrics(t time.Time, all []*pod, i int, spans []spanRec) {
	md := pmetric.NewMetrics()
	// latencies observed this step, per pod and route, from the spans
	obs := map[*pod]map[string][]float64{}
	for _, r := range spans {
		if r.span.Kind() != ptrace.SpanKindServer {
			continue
		}
		if obs[r.p] == nil {
			obs[r.p] = map[string][]float64{}
		}
		rt, _ := r.span.Attributes().Get("http.route")
		d := float64(r.span.EndTimestamp()-r.span.StartTimestamp()) / 1e9
		obs[r.p][rt.Str()] = append(obs[r.p][rt.Str()], d)
	}
	for pi, p := range all {
		rm := md.ResourceMetrics().AppendEmpty()
		resource(rm.Resource(), p)
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("hdxgen/" + p.svc)
		sm.Scope().SetVersion("0.1.0")
		phase := float64(i)/40 + float64(pi)
		// gauges
		g := metric(sm, "container.cpu.utilization", "CPU utilization of the container", "1").SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts(t))
		dp.SetDoubleValue(0.3 + 0.2*math.Sin(phase) + 0.05*rng.Float64())
		g = metric(sm, "container.memory.working_set", "Working set memory", "By").SetEmptyGauge()
		dp = g.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts(t))
		dp.SetIntValue(int64(200e6 + 50e6*math.Sin(phase/3) + 1e6*float64(rng.Intn(10))))
		g = metric(sm, "system.cpu.utilization", "System CPU utilization by state", "1").SetEmptyGauge()
		for si, st := range []string{"user", "system", "idle"} {
			dp = g.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(t))
			dp.Attributes().PutStr("state", st)
			v := []float64{0.25, 0.1, 0.65}[si] + 0.05*math.Sin(phase+float64(si))
			dp.SetDoubleValue(v)
		}
		for k := 0; k < *extra; k++ {
			g = metric(sm, fmt.Sprintf("app.custom.metric.%03d", k), "synthetic", "1").SetEmptyGauge()
			dp = g.DataPoints().AppendEmpty()
			dp.SetTimestamp(ts(t))
			dp.SetDoubleValue(float64(k) + rng.Float64())
		}
		// cumulative monotonic counter, per route and code
		s := metric(sm, "http.server.request.count", "Server requests", "{request}").SetEmptySum()
		s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		s.SetIsMonotonic(true)
		for _, rt := range routes[p.svc] {
			for _, code := range []int{200, 500} {
				key := fmt.Sprintf("%s|%d", rt, code)
				inc := float64(len(obs[p][rt]))*5 + float64(rng.Intn(20))
				if code == 500 {
					inc = float64(rng.Intn(3))
				}
				p.reqs[key] += inc
				dp := s.DataPoints().AppendEmpty()
				dp.SetStartTimestamp(ts(p.start))
				dp.SetTimestamp(ts(t))
				dp.Attributes().PutStr("http.route", rt)
				dp.Attributes().PutInt("http.response.status_code", int64(code))
				dp.SetDoubleValue(p.reqs[key])
			}
		}
		if p.svc == "checkout" {
			s := metric(sm, "app.orders.placed", "Orders placed", "{order}").SetEmptySum()
			s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			s.SetIsMonotonic(true)
			for _, pm := range []string{"card", "paypal", "voucher"} {
				dp := s.DataPoints().AppendEmpty()
				dp.SetStartTimestamp(ts(t.Add(-*step)))
				dp.SetTimestamp(ts(t))
				dp.Attributes().PutStr("payment.method", pm)
				dp.SetIntValue(int64(rng.Intn(10)))
			}
		}
		if p.svc == "payment" {
			s := metric(sm, "db.client.connections.usage", "Connections in use", "{connection}").SetEmptySum()
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			s.SetIsMonotonic(false)
			used := int64(3 + rng.Intn(5))
			for _, st := range []string{"idle", "used"} {
				dp := s.DataPoints().AppendEmpty()
				dp.SetStartTimestamp(ts(p.start))
				dp.SetTimestamp(ts(t))
				dp.Attributes().PutStr("state", st)
				dp.Attributes().PutStr("pool.name", "payments")
				if st == "used" {
					dp.SetIntValue(used)
				} else {
					dp.SetIntValue(10 - used)
				}
			}
		}
		// cumulative explicit-bucket histogram, per route
		h := metric(sm, "http.server.request.duration", "Duration of HTTP server requests", "s").SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		for _, rt := range routes[p.svc] {
			if p.hist[rt] == nil {
				p.hist[rt] = make([]uint64, len(bounds)+1)
			}
			vals := obs[p][rt]
			for k := 0; k < 5+rng.Intn(10); k++ {
				vals = append(vals, math.Exp(rng.NormFloat64()*0.8-3))
			}
			for _, v := range vals {
				b := 0
				for b < len(bounds) && v > bounds[b] {
					b++
				}
				p.hist[rt][b]++
				p.histSum[rt] += v
				p.histN[rt]++
			}
			dp := h.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(ts(p.start))
			dp.SetTimestamp(ts(t))
			dp.Attributes().PutStr("http.route", rt)
			dp.ExplicitBounds().FromRaw(bounds)
			dp.BucketCounts().FromRaw(append([]uint64(nil), p.hist[rt]...))
			dp.SetCount(p.histN[rt])
			dp.SetSum(p.histSum[rt])
			if len(obs[p][rt]) > 0 {
				for _, r := range spans {
					if r.p == p && r.span.Kind() == ptrace.SpanKindServer {
						e := dp.Exemplars().AppendEmpty()
						e.SetTimestamp(r.span.StartTimestamp())
						e.SetDoubleValue(float64(r.span.EndTimestamp()-r.span.StartTimestamp()) / 1e9)
						e.SetTraceID(r.span.TraceID())
						e.SetSpanID(r.span.SpanID())
						break
					}
				}
			}
		}
		// delta exponential histogram
		if p.svc == "checkout" || p.svc == "payment" {
			eh := metric(sm, "rpc.server.duration", "RPC server duration", "ms").SetEmptyExponentialHistogram()
			eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			dp := eh.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(ts(t.Add(-*step)))
			dp.SetTimestamp(ts(t))
			dp.Attributes().PutStr("rpc.system", "grpc")
			dp.SetScale(3)
			base := math.Pow(2, 1.0/8)
			offset := int32(20) // bucket index i covers (base^i, base^(i+1)]
			counts := make([]uint64, 40)
			var n uint64
			var sum, mn, mx float64
			mn = math.Inf(1)
			for k := 0; k < 20+rng.Intn(30); k++ {
				v := math.Exp(rng.NormFloat64()*0.5 + 3.5)
				idx := int(math.Ceil(math.Log(v)/math.Log(base))) - 1 - int(offset)
				if idx < 0 {
					idx = 0
				}
				if idx >= len(counts) {
					idx = len(counts) - 1
				}
				counts[idx]++
				n++
				sum += v
				mn = math.Min(mn, v)
				mx = math.Max(mx, v)
			}
			dp.Positive().SetOffset(offset)
			dp.Positive().BucketCounts().FromRaw(counts)
			dp.SetCount(n)
			dp.SetSum(sum)
			dp.SetMin(mn)
			dp.SetMax(mx)
		}
		// summary
		if p.svc == "inventory" {
			su := metric(sm, "jvm.gc.pause", "GC pause time", "ms").SetEmptySummary()
			for _, gc := range []string{"G1 Young Generation", "G1 Old Generation"} {
				p.gcN += uint64(1 + rng.Intn(4))
				p.gcSum += 5 + 10*rng.Float64()
				dp := su.DataPoints().AppendEmpty()
				dp.SetStartTimestamp(ts(p.start))
				dp.SetTimestamp(ts(t))
				dp.Attributes().PutStr("gc", gc)
				dp.SetCount(p.gcN)
				dp.SetSum(p.gcSum)
				for _, q := range []float64{0, 0.5, 0.9, 0.99, 1} {
					v := dp.QuantileValues().AppendEmpty()
					v.SetQuantile(q)
					v.SetValue(2 + 20*q + rng.Float64())
				}
			}
		}
	}
	b, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		log.Fatal(err)
	}
	post("/v1/metrics", b)
}

func main() {
	flag.Parse()
	rng = rand.New(rand.NewSource(*seed))
	now := time.Now().Truncate(*step)
	begin := now.Add(-*backfill)
	prefix := ""
	if *cluster != "" {
		prefix = *cluster + "-"
	}
	var all []*pod
	podsBy := map[string][]*pod{}
	for _, svc := range services {
		for k := 0; k < *pods; k++ {
			p := &pod{svc: svc, name: fmt.Sprintf("%s%s-%x-%d", prefix, svc, len(svc)*7919, k), host: fmt.Sprintf("node-%d", (k+len(svc))%4),
				start: begin.Add(-time.Minute), reqs: map[string]float64{}, hist: map[string][]uint64{}, histSum: map[string]float64{}, histN: map[string]uint64{}}
			all = append(all, p)
			podsBy[svc] = append(podsBy[svc], p)
		}
	}
	want := map[string]bool{}
	for _, s := range bytes.Split([]byte(*only), []byte(",")) {
		want[string(s)] = true
	}
	emit := func(t time.Time, i int) {
		var spans []spanRec
		var logs []logRec
		for k := 0; k < *nTraces; k++ {
			request(t.Add(time.Duration(rng.Int63n(int64(*step)))), podsBy, &spans, &logs)
		}
		if want["traces"] {
			sendTraces(spans)
		}
		if want["logs"] {
			sendLogs(logs)
		}
		if want["metrics"] {
			sendMetrics(t, all, i, spans)
		}
	}
	i := 0
	for t := begin; !t.After(now); t = t.Add(*step) {
		emit(t, i)
		i++
	}
	log.Printf("backfill: %d steps from %s", i, begin.Format(time.RFC3339))
	end := time.Now().Add(*live)
	for time.Now().Before(end) {
		t := time.Now().Truncate(*step).Add(*step)
		time.Sleep(time.Until(t))
		emit(t, i)
		i++
	}
}
