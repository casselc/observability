// mixgen writes a cluster gateway's traces or logs as OTLP protobuf requests
// (ExportTraceServiceRequest / ExportLogsServiceRequest bytes), for the
// sorting experiment (bench/sorting/README.md). It extends the spike's two
// generators: chdbexporter/testgen (3 services, fixed shapes) gives the span
// and log shapes, metrics-layout/fleet gives the k8s resource attributes
// (same keys, same naming). What is new is the multi-service mix:
//
//   - S services (default 120) with Zipf(s) traffic shares (default s=1.1:
//     the top service carries ~25%, the median ~0.4%, the tail ~0.1%); the
//     rank→name assignment is shuffled so traffic is not alphabetical;
//   - P pods in the cluster (default 3000) spread over services by share
//     (at least one each), on 200 nodes, each pod its own resource;
//   - a batch is what one publisher sees: -rows rows sampled by service
//     share, then pod, with timestamps uniform over the batch's time window
//     (rows / publisher rate), grouped per pod into resources in arrival
//     (shuffled) order, rows in time order within a pod, as agents' requests
//     concatenated by a batch processor arrive at a gateway;
//   - -publishers N -route service -pub j: publisher j of N behind the
//     loadbalancing exporter with routing_key service (a crc32 ring with 100
//     virtual nodes per endpoint, like the exporter's): only the services the
//     ring gives j, and a window stretched to 10k rows at j's own rate.
//     -route none: a uniform 1/N share of everything.
//
// Deterministic for a seed. Also writes one JSON line per batch (-stats):
// rows per service, window.
//
//	mixgen -signal traces -out DIR [-batches 32] [-rows 10000] [-services 120] [-zipf 1.1]
//	       [-rate 30000] [-publishers 3 -route none|service -pub 0] [-seed 1] [-stats FILE]
//	mixgen -ring -services 120 -publishers 8   # print the ring's service → publisher map as JSON
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

var svcWords = []string{"frontend", "checkout", "payments", "catalog", "shipping", "cart", "search", "recommend",
	"inventory", "pricing", "auth", "users", "orders", "notify", "email", "ads", "reviews", "media", "gateway", "billing"}
var nsNames = []string{"shop", "platform", "payments", "catalog", "fulfilment", "identity", "marketing", "data"}
var langs = [][2]string{{"go", "1.38.0"}, {"java", "1.54.1"}, {"python", "1.37.0"}, {"dotnet", "1.12.0"}}

type service struct {
	name, ns, kind string
	share          float64
	pods           []int // pod indexes
	ops            []string
	tables         []string
}

type podT struct {
	svc  int
	res  [][2]string
	node int
}

type mix struct {
	svcs []service
	pods []podT
}

func hexID(r *rand.Rand, n int) string {
	const h = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = h[r.IntN(16)]
	}
	return string(b)
}

func uuid(r *rand.Rand) string {
	return hexID(r, 8) + "-" + hexID(r, 4) + "-4" + hexID(r, 3) + "-a" + hexID(r, 3) + "-" + hexID(r, 12)
}

func svcName(i int) string {
	n := svcWords[i%len(svcWords)]
	if i >= len(svcWords) {
		n = fmt.Sprintf("%s-%d", n, i/len(svcWords)+1)
	}
	return n
}

func newMix(nsvc int, s float64, npods, nodes int, cluster string) *mix {
	r := rand.New(rand.NewPCG(7, 11))
	// Zipf shares by rank, then a shuffled rank for each name.
	w := make([]float64, nsvc)
	tot := 0.0
	for i := range w {
		w[i] = 1 / math.Pow(float64(i+1), s)
		tot += w[i]
	}
	rank := r.Perm(nsvc)
	m := &mix{}
	kinds := []string{"http", "http", "db", "messaging"}
	for i := 0; i < nsvc; i++ {
		sv := service{name: svcName(i), ns: nsNames[i%len(nsNames)], kind: kinds[i%len(kinds)], share: w[rank[i]] / tot}
		nops := 6 + r.IntN(7)
		for k := 0; k < nops; k++ {
			switch k % 3 {
			case 0:
				sv.ops = append(sv.ops, fmt.Sprintf("GET /api/%s/%s", sv.name, []string{"items", "list", "status", "detail", "search"}[r.IntN(5)]))
			case 1:
				sv.ops = append(sv.ops, fmt.Sprintf("POST /api/%s/%s", sv.name, []string{"create", "update", "submit", "batch"}[r.IntN(4)]))
			default:
				t := fmt.Sprintf("%s_%s", sv.ns, []string{"orders", "items", "events", "sessions", "accounts"}[r.IntN(5)])
				sv.tables = append(sv.tables, t)
				sv.ops = append(sv.ops, "SELECT "+t)
			}
		}
		m.svcs = append(m.svcs, sv)
	}
	// Pods by share, at least one per service.
	for i := range m.svcs {
		n := int(math.Round(m.svcs[i].share * float64(npods)))
		if n < 1 {
			n = 1
		}
		version := fmt.Sprintf("1.%d.%d", 4+i%7, 2+i%5)
		lang := langs[i%len(langs)]
		rsHash := hexID(r, 10)
		for k := 0; k < n; k++ {
			node := r.IntN(nodes)
			nodeName := fmt.Sprintf("ip-10-%d-%d-%d.eu-west-1.compute.internal", node%3, 16+node/250, 10+node%250)
			sv := &m.svcs[i]
			p := podT{svc: i, node: node, res: [][2]string{
				{"service.name", sv.name},
				{"service.namespace", sv.ns},
				{"service.version", version},
				{"service.instance.id", uuid(r)},
				{"telemetry.sdk.language", lang[0]},
				{"telemetry.sdk.name", "opentelemetry"},
				{"telemetry.sdk.version", lang[1]},
				{"host.name", nodeName},
				{"os.type", "linux"},
				{"cloud.provider", "aws"},
				{"cloud.region", "eu-west-1"},
				{"cloud.availability_zone", "eu-west-1" + string(rune('a'+node%3))},
				{"k8s.cluster.name", cluster},
				{"k8s.namespace.name", sv.ns},
				{"k8s.deployment.name", sv.name},
				{"k8s.replicaset.name", sv.name + "-" + rsHash},
				{"k8s.pod.name", sv.name + "-" + rsHash + "-" + hexID(r, 5)},
				{"k8s.pod.uid", uuid(r)},
				{"k8s.node.name", nodeName},
				{"k8s.container.name", sv.name},
				{"container.image.tag", version},
			}}
			sv.pods = append(sv.pods, len(m.pods))
			m.pods = append(m.pods, p)
		}
	}
	return m
}

// ring: the loadbalancing exporter's consistent hash ring, approximately
// (crc32 of "endpoint-j" + vnode, 100 per endpoint, key = service name).
func ring(names []string, n int) map[string]int {
	type pt struct {
		h uint32
		e int
	}
	var pts []pt
	for e := 0; e < n; e++ {
		for v := 0; v < 100; v++ {
			pts = append(pts, pt{crc32.ChecksumIEEE([]byte("publisher-" + strconv.Itoa(e) + ":4317-" + strconv.Itoa(v))), e})
		}
	}
	sort.Slice(pts, func(a, b int) bool { return pts[a].h < pts[b].h })
	out := map[string]int{}
	for _, nm := range names {
		h := crc32.ChecksumIEEE([]byte(nm))
		i := sort.Search(len(pts), func(k int) bool { return pts[k].h >= h })
		if i == len(pts) {
			i = 0
		}
		out[nm] = pts[i].e
	}
	return out
}

type batchStat struct {
	Batch    int            `json:"batch"`
	Rows     int            `json:"rows"`
	WindowMs float64        `json:"window_ms"`
	StartNs  int64          `json:"start_ns"`
	Services map[string]int `json:"services"`
	Pods     int            `json:"pods"`
	Bytes    int            `json:"bytes"`
}

func main() {
	signal := flag.String("signal", "traces", "traces|logs")
	out := flag.String("out", "", "output directory")
	batches := flag.Int("batches", 32, "")
	rows := flag.Int("rows", 10000, "rows per batch")
	nsvc := flag.Int("services", 120, "")
	zipf := flag.Float64("zipf", 1.1, "Zipf exponent of the service shares")
	npods := flag.Int("pods", 3000, "")
	nodes := flag.Int("nodes", 200, "")
	rate := flag.Float64("rate", 30000, "the cluster's rows/s of this signal (all publishers)")
	pubs := flag.Int("publishers", 3, "")
	route := flag.String("route", "none", "none|service")
	pub := flag.Int("pub", 0, "which publisher's stream")
	seed := flag.Uint64("seed", 1, "")
	start := flag.String("start", "2026-09-26T10:00:00Z", "")
	cluster := flag.String("cluster", "prod-eu-west-1", "")
	stats := flag.String("stats", "", "JSONL file of per-batch stats")
	ringOnly := flag.Bool("ring", false, "print {service: share, publisher} for -publishers and exit")
	flag.Parse()

	m := newMix(*nsvc, *zipf, *npods, *nodes, *cluster)
	names := make([]string, len(m.svcs))
	for i, s := range m.svcs {
		names[i] = s.name
	}
	owner := ring(names, *pubs)
	if *ringOnly {
		type e struct {
			Share     float64 `json:"share"`
			Publisher int     `json:"publisher"`
			Pods      int     `json:"pods"`
		}
		o := map[string]e{}
		for _, s := range m.svcs {
			o[s.name] = e{s.share, owner[s.name], len(s.pods)}
		}
		b, _ := json.Marshal(o)
		fmt.Println(string(b))
		return
	}
	// This publisher's services and its rows/s.
	var cand []int
	var cum []float64
	acc := 0.0
	for i, s := range m.svcs {
		if *route == "service" && owner[s.name] != *pub {
			continue
		}
		cand = append(cand, i)
		acc += s.share
		cum = append(cum, acc)
	}
	pubRate := *rate * acc
	if *route != "service" {
		pubRate = *rate / float64(*pubs)
	}
	window := time.Duration(float64(*rows) / pubRate * float64(time.Second))
	t0, err := time.Parse(time.RFC3339, *start)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	var sf *os.File
	if *stats != "" {
		if sf, err = os.Create(*stats); err != nil {
			log.Fatal(err)
		}
		defer sf.Close()
	}
	r := rand.New(rand.NewPCG(*seed, uint64(*pub)*1000+uint64(*pubs)))
	for b := 0; b < *batches; b++ {
		bstart := t0.Add(time.Duration(b) * window)
		// Sample rows: service by share, then a pod of it, then a time.
		type row struct {
			pod int
			ts  time.Time
		}
		perPod := map[int][]time.Time{}
		st := batchStat{Batch: b, Rows: *rows, WindowMs: float64(window) / 1e6, StartNs: bstart.UnixNano(), Services: map[string]int{}}
		for k := 0; k < *rows; k++ {
			u := r.Float64() * acc
			j := sort.SearchFloat64s(cum, u)
			if j >= len(cand) {
				j = len(cand) - 1
			}
			sv := &m.svcs[cand[j]]
			p := sv.pods[r.IntN(len(sv.pods))]
			ts := bstart.Add(time.Duration(r.Float64() * float64(window)))
			perPod[p] = append(perPod[p], ts)
			st.Services[sv.name]++
		}
		podIdx := make([]int, 0, len(perPod))
		for p := range perPod {
			podIdx = append(podIdx, p)
		}
		sort.Ints(podIdx)
		r.Shuffle(len(podIdx), func(a, c int) { podIdx[a], podIdx[c] = podIdx[c], podIdx[a] })
		st.Pods = len(podIdx)
		var body []byte
		if *signal == "logs" {
			ld := plog.NewLogs()
			for _, p := range podIdx {
				tss := perPod[p]
				sort.Slice(tss, func(a, c int) bool { return tss[a].Before(tss[c]) })
				addLogs(ld, m, p, tss, r)
			}
			body, err = plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
		} else {
			td := ptrace.NewTraces()
			for _, p := range podIdx {
				tss := perPod[p]
				sort.Slice(tss, func(a, c int) bool { return tss[a].Before(tss[c]) })
				addSpans(td, m, p, tss, r)
			}
			body, err = ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
		}
		if err != nil {
			log.Fatal(err)
		}
		st.Bytes = len(body)
		if err := os.WriteFile(filepath.Join(*out, fmt.Sprintf("%s-b%04d.pb", *signal, b)), body, 0o644); err != nil {
			log.Fatal(err)
		}
		if sf != nil {
			j, _ := json.Marshal(st)
			fmt.Fprintln(sf, string(j))
		}
	}
	fmt.Printf("%s: %d batches of %d rows, publisher %d/%d route=%s, %d services, rate %.0f rows/s, window %v\n",
		*signal, *batches, *rows, *pub, *pubs, *route, len(cand), pubRate, window)
}

func putRes(a pcommon.Map, res [][2]string) {
	for _, kv := range res {
		a.PutStr(kv[0], kv[1])
	}
}

func tid(r *rand.Rand) (t pcommon.TraceID) {
	for i := range t {
		t[i] = byte(r.Uint32())
	}
	return
}

func sid(r *rand.Rand) (s pcommon.SpanID) {
	for i := range s {
		s[i] = byte(r.Uint32())
	}
	return
}

func addSpans(td ptrace.Traces, m *mix, p int, tss []time.Time, r *rand.Rand) {
	pd := m.pods[p]
	sv := m.svcs[pd.svc]
	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
	putRes(rs.Resource().Attributes(), pd.res)
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")
	ss.Scope().SetVersion("0.63.0")
	for _, ts := range tss {
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(tid(r))
		s.SetSpanID(sid(r))
		if r.IntN(5) != 0 {
			s.SetParentSpanID(sid(r))
		}
		op := sv.ops[r.IntN(len(sv.ops))]
		s.SetName(op)
		dur := time.Duration(math.Exp(13+1.2*r.NormFloat64())) // median ~0.44 ms, long tail
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(ts))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(ts.Add(dur)))
		a := s.Attributes()
		switch {
		case len(op) > 7 && op[:7] == "SELECT ":
			s.SetKind(ptrace.SpanKindClient)
			a.PutStr("db.system.name", "postgresql")
			a.PutStr("db.collection.name", op[7:])
			a.PutStr("db.query.text", "SELECT id, total, status FROM "+op[7:]+" WHERE customer_id = $1 LIMIT 50")
			a.PutStr("server.address", sv.ns+"-db.internal")
			a.PutInt("db.response.returned_rows", int64(r.IntN(50)))
		default:
			s.SetKind(ptrace.SpanKindServer)
			method := "GET"
			if op[0] == 'P' {
				method = "POST"
			}
			code := 200
			switch x := r.IntN(100); {
			case x < 2:
				code = 500
			case x < 5:
				code = 404
			case x < 8:
				code = 201
			}
			a.PutStr("http.request.method", method)
			a.PutStr("http.route", op[len(method)+1:])
			a.PutStr("url.path", op[len(method)+1:]+"/"+strconv.Itoa(r.IntN(100000)))
			a.PutInt("http.response.status_code", int64(code))
			a.PutStr("network.protocol.version", "1.1")
			a.PutStr("user_agent.original", []string{"Mozilla/5.0 (X11; Linux x86_64)", "okhttp/4.12.0", "Go-http-client/1.1", "curl/8.5.0"}[r.IntN(4)])
			a.PutStr("client.address", fmt.Sprintf("10.%d.%d.%d", r.IntN(4), r.IntN(256), r.IntN(256)))
			if code >= 500 {
				s.Status().SetCode(ptrace.StatusCodeError)
				s.Status().SetMessage("upstream error: connection reset by peer")
			}
		}
		if r.IntN(20) == 0 {
			ev := s.Events().AppendEmpty()
			ev.SetName("exception")
			ev.SetTimestamp(pcommon.NewTimestampFromTime(ts.Add(dur / 2)))
			ev.Attributes().PutStr("exception.type", "TimeoutError")
			ev.Attributes().PutStr("exception.message", "upstream timed out after 250ms")
		}
		if r.IntN(50) == 0 {
			l := s.Links().AppendEmpty()
			l.SetTraceID(tid(r))
			l.SetSpanID(sid(r))
			l.Attributes().PutStr("link.kind", "follows_from")
		}
	}
}

var logTemplates = []string{
	"request completed method=%s path=/api/%s/%d status=%d duration_ms=%d",
	"cache miss for key %s:%d, fetching from origin",
	"processed batch of %d messages from topic %s in %d ms",
	"user %d logged in from %s",
	"slow query took %d ms: SELECT * FROM %s WHERE id = %d",
	"retrying call to %s (attempt %d of 3): context deadline exceeded",
	"connection pool stats: active=%d idle=%d waiting=%d",
	"order %d state changed to %s",
}

func addLogs(ld plog.Logs, m *mix, p int, tss []time.Time, r *rand.Rand) {
	pd := m.pods[p]
	sv := m.svcs[pd.svc]
	rl := ld.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
	putRes(rl.Resource().Attributes(), pd.res)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("log/slog")
	sl.Scope().Attributes().PutStr("bridge", "otelslog")
	for _, ts := range tss {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(ts.Add(time.Duration(r.IntN(5000)) * time.Microsecond)))
		if r.IntN(10) < 6 {
			lr.SetTraceID(tid(r))
			lr.SetSpanID(sid(r))
			lr.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
		}
		x := r.IntN(100)
		switch {
		case x < 70:
			lr.SetSeverityNumber(plog.SeverityNumberInfo)
			lr.SetSeverityText("INFO")
		case x < 85:
			lr.SetSeverityNumber(plog.SeverityNumberDebug)
			lr.SetSeverityText("DEBUG")
		case x < 95:
			lr.SetSeverityNumber(plog.SeverityNumberWarn)
			lr.SetSeverityText("WARN")
		default:
			lr.SetSeverityNumber(plog.SeverityNumberError)
			lr.SetSeverityText("ERROR")
		}
		var body string
		switch t := r.IntN(len(logTemplates)); t {
		case 0:
			body = fmt.Sprintf(logTemplates[t], []string{"GET", "POST"}[r.IntN(2)], sv.name, r.IntN(100000), []int{200, 200, 200, 404, 500}[r.IntN(5)], r.IntN(900))
		case 1:
			body = fmt.Sprintf(logTemplates[t], sv.name, r.IntN(1000000))
		case 2:
			body = fmt.Sprintf(logTemplates[t], r.IntN(500), sv.ns+".events", r.IntN(200))
		case 3:
			body = fmt.Sprintf(logTemplates[t], r.IntN(10000000), fmt.Sprintf("10.%d.%d.%d", r.IntN(4), r.IntN(256), r.IntN(256)))
		case 4:
			body = fmt.Sprintf(logTemplates[t], 100+r.IntN(2000), sv.ns+"_orders", r.IntN(1000000))
		case 5:
			body = fmt.Sprintf(logTemplates[t], svcName(r.IntN(40)), 1+r.IntN(3))
		case 6:
			body = fmt.Sprintf(logTemplates[t], r.IntN(50), r.IntN(50), r.IntN(5))
		default:
			body = fmt.Sprintf(logTemplates[t], r.IntN(10000000), []string{"created", "paid", "shipped", "cancelled"}[r.IntN(4)])
		}
		lr.Body().SetStr(body)
		a := lr.Attributes()
		a.PutStr("logger.name", sv.name+"."+[]string{"http", "db", "worker", "cache"}[r.IntN(4)])
		a.PutStr("code.function.name", "main.handle"+strconv.Itoa(r.IntN(12)))
		a.PutInt("code.line.number", int64(20+r.IntN(400)))
		if r.IntN(10) == 0 {
			lr.SetEventName(sv.name + ".audit")
		}
	}
}
