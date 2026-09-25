// Package fleet generates realistic OTel metrics for a Kubernetes fleet: pods
// of services on nodes, each exporting a fixed set of SDK-style metrics every
// Interval. It is the one dataset every layout in this directory is measured
// on (A: the contrib otel_metrics_* tables, B: series-deduplicated, C: the
// TimeSeries engine, D: the Prometheus TSDB).
//
// Shape (defaults): 40 services x 10 pods = 400 pods on 27 nodes, 500 series
// per pod = 200,000 series, 30 s interval, 720 rounds = 6 h, 144 M points.
//
//   - resources: 21 k8s/cloud/SDK resource attributes per pod, as the
//     k8sattributes and resourcedetection processors leave them;
//   - five instrumentation scopes, 45 metric names, 1 to 40 attribute sets per
//     metric with 1 to 5 point attributes;
//   - per pod and round: 260 sum points (monotonic cumulative counters),
//     140 gauges, 80 explicit-bucket histograms (11 to 16 buckets,
//     cumulative), 10 exponential histograms (delta) and 10 summaries;
//   - values evolve like the real thing: counters grow by Poisson increments
//     from heavy-tailed per-series rates with a diurnal swing (many error
//     and rare-route counters stay flat for long stretches), gauges random
//     walk, stick, burst or stay constant, histograms add Poisson bucket
//     increments drawn from a per-series log-normal latency, and their
//     cumulative min/max only move when a new extreme is seen;
//   - timestamps: each pod exports at its own offset within the interval,
//     with jitter, so TimeUnix steps are mostly 30 s and sometimes 29 or 31;
//   - churn: 20% of services roll out once, between 1 h and 5 h in, one pod
//     per round: new pod name, uid, instance id, version and start time, and
//     counters restart from zero. So there are about 8,000 more series than
//     series live at any one time.
//
// Generation is deterministic and sequential: Emit must be called for
// rounds in order and, within a round, for pods in order.
package fleet

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Config sizes the fleet.
type Config struct {
	Services       int
	PodsPerService int
	Namespaces     int
	PodsPerNode    int
	Rounds         int
	Interval       time.Duration
	Start          time.Time
	// RolloutEvery: every RolloutEvery-th service redeploys once (0: none).
	RolloutEvery int
}

// Default is the 200k-series, 6 h fleet.
func Default() Config {
	return Config{Services: 40, PodsPerService: 10, Namespaces: 8, PodsPerNode: 15, Rounds: 720,
		Interval: 30 * time.Second, Start: time.Date(2026, 9, 24, 6, 0, 0, 0, time.UTC), RolloutEvery: 5}
}

// Pods is the number of pod slots.
func (c Config) Pods() int { return c.Services * c.PodsPerService }

// SeriesPerPod is the number of series one pod exports.
func SeriesPerPod() int {
	n := 0
	for _, s := range specs {
		n += len(s.sets)
	}
	return n
}

// PointsPerPod returns the points one pod exports per round, by type
// (gauge, sum, histogram, exponential histogram, summary).
func PointsPerPod() [5]int {
	var n [5]int
	for _, s := range specs {
		n[s.typ] += len(s.sets)
	}
	return n
}

// Metric types, in parquetgo's order.
const (
	tGauge = iota
	tSum
	tHist
	tExp
	tSummary
)

type model int

const (
	mCounter      model = iota // int counter, Poisson increments
	mCounterF                  // double counter (CPU seconds)
	mCounterBytes              // int counter, large increments
	mWalk                      // int gauge, mean-reverting walk, quantised
	mRatio                     // double gauge in [0,1], AR(1)
	mSmall                     // small int gauge, Poisson each round
	mSticky                    // int gauge that rarely changes by +-1..3
	mConst                     // constant int gauge
	mUptime                    // double seconds since pod start
	mHist                      // explicit-bucket histogram, cumulative
	mExp                       // exponential histogram, delta
	mSummary                   // summary, cumulative count/sum
)

type dim struct {
	key  string
	vals []any // string or int64
}

type spec struct {
	scope      int
	typ        int
	name, unit string
	model      model
	mu, sigma  float64   // log-rate (per second) distribution, or level
	bounds     []float64 // histograms
	lat        [2]float64
	sets       [][]kv
}

type kv struct {
	k string
	v any
}

var scopes = [][2]string{
	{"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp", "0.63.0"},
	{"github.com/XSAM/otelsql", "0.39.0"},
	{"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc", "0.63.0"},
	{"go.opentelemetry.io/contrib/instrumentation/runtime", "0.63.0"},
	{"shop/app", "2.3.1"},
}

var (
	semconvBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}
	sdkBounds     = []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000}
	sizeBounds    = []float64{0, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1e6}
	routes        = []any{"/api/cart", "/api/checkout", "/api/products/{id}", "/api/products", "/api/orders/{id}",
		"/api/orders", "/api/users/{id}", "/healthz", "/metrics", "/api/search"}
	peers = []any{"inventory.shop.svc", "pricing.shop.svc", "auth.platform.svc", "api.stripe.com", "search.shop.svc"}
)

func strs(prefix string, n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

// product returns the first n attribute sets of the cartesian product of
// dims, plus constant attributes.
func product(n int, consts []kv, ds ...dim) [][]kv {
	var out [][]kv
	idx := make([]int, len(ds))
	for len(out) < n {
		var set []kv
		for i, d := range ds {
			set = append(set, kv{d.key, d.vals[idx[i]]})
		}
		set = append(set, consts...)
		out = append(out, set)
		for i := len(ds) - 1; i >= 0; i-- {
			idx[i]++
			if idx[i] < len(ds[i].vals) {
				break
			}
			idx[i] = 0
			if i == 0 {
				if len(out) < n {
					panic("product: not enough combinations")
				}
			}
		}
	}
	return out
}

func pairs(k1 string, k2 string, ps ...[2]any) dim {
	// A dimension whose values are (k1, k2) pairs, flattened later.
	vals := make([]any, len(ps))
	for i, p := range ps {
		vals[i] = []kv{{k1, p[0]}, {k2, p[1]}}
	}
	return dim{"", vals}
}

var specs = buildSpecs()

func buildSpecs() []spec {
	httpC := []kv{{"url.scheme", "http"}, {"network.protocol.version", "1.1"}}
	mstat := pairs("http.request.method", "http.response.status_code",
		[2]any{"GET", int64(200)}, [2]any{"POST", int64(201)}, [2]any{"GET", int64(404)}, [2]any{"GET", int64(500)})
	mstat6 := pairs("http.request.method", "http.response.status_code",
		[2]any{"GET", int64(200)}, [2]any{"POST", int64(200)}, [2]any{"GET", int64(404)}, [2]any{"POST", int64(500)},
		[2]any{"GET", int64(503)}, [2]any{"PUT", int64(204)})
	mstat3 := pairs("http.request.method", "http.response.status_code",
		[2]any{"GET", int64(200)}, [2]any{"POST", int64(201)}, [2]any{"GET", int64(500)})
	ops := []any{"SELECT", "INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT"}
	tables := []any{"orders", "carts", "products", "users", "payments"}
	s := []spec{
		// http (scope 0)
		{scope: 0, typ: tSum, name: "http.server.request.count", unit: "{request}", model: mCounter, mu: -1.5, sigma: 1.6,
			sets: product(40, httpC, dim{"http.route", routes}, mstat)},
		{scope: 0, typ: tSum, name: "http.client.request.count", unit: "{request}", model: mCounter, mu: -1.8, sigma: 1.5,
			sets: product(30, nil, dim{"server.address", peers}, mstat6)},
		{scope: 0, typ: tHist, name: "http.server.request.duration", unit: "s", model: mHist, mu: -1.5, sigma: 1.6,
			bounds: semconvBounds, lat: [2]float64{math.Log(0.04), 0.9}, sets: product(30, httpC, dim{"http.route", routes}, mstat3)},
		{scope: 0, typ: tHist, name: "http.client.request.duration", unit: "s", model: mHist, mu: -1.8, sigma: 1.5,
			bounds: semconvBounds, lat: [2]float64{math.Log(0.08), 0.8}, sets: product(15, nil, dim{"server.address", peers}, mstat3)},
		{scope: 0, typ: tHist, name: "http.server.request.body.size", unit: "By", model: mHist, mu: -1.0, sigma: 1.2,
			bounds: sizeBounds, lat: [2]float64{math.Log(2000), 1.5}, sets: product(5, nil, dim{"http.route", routes[:5]})},
		{scope: 0, typ: tGauge, name: "http.server.active_requests", unit: "{request}", model: mSmall, mu: 0.3, sigma: 1,
			sets: product(10, nil, dim{"http.request.method", []any{"GET", "POST"}}, dim{"http.route", routes[:5]})},
		{scope: 0, typ: tGauge, name: "http.server.open_connections", unit: "{connection}", model: mSmall, mu: 3, sigma: 0.5,
			sets: product(1, nil)},
		// db (scope 1)
		{scope: 1, typ: tSum, name: "db.client.operation.count", unit: "{operation}", model: mCounter, mu: -1.2, sigma: 1.4,
			sets: product(30, []kv{{"db.system.name", "postgresql"}}, dim{"db.operation.name", ops}, dim{"db.collection.name", tables})},
		{scope: 1, typ: tHist, name: "db.client.operation.duration", unit: "s", model: mHist, mu: -1.2, sigma: 1.4,
			bounds: semconvBounds, lat: [2]float64{math.Log(0.006), 1.1}, sets: product(20, []kv{{"db.system.name", "postgresql"}},
				dim{"db.operation.name", ops[:4]}, dim{"db.collection.name", tables})},
		{scope: 1, typ: tGauge, name: "db.client.connection.count", unit: "{connection}", model: mSticky, mu: 8, sigma: 0.5,
			sets: product(12, nil, dim{"db.client.connection.pool.name", []any{"primary", "replica", "analytics"}},
				dim{"db.client.connection.state", []any{"idle", "used"}}, dim{"db.system.name", []any{"postgresql", "redis"}})},
		// grpc (scope 2)
		{scope: 2, typ: tSum, name: "rpc.server.requests", unit: "{request}", model: mCounter, mu: -1.0, sigma: 1.5,
			sets: product(30, []kv{{"rpc.system", "grpc"}}, dim{"rpc.service", []any{"shop.Cart", "shop.Catalog", "shop.Checkout"}},
				dim{"rpc.method", []any{"Get", "List", "Create", "Update", "Delete"}}, dim{"rpc.grpc.status_code", []any{int64(0), int64(14)}})},
		{scope: 2, typ: tHist, name: "rpc.server.duration", unit: "ms", model: mHist, mu: -1.0, sigma: 1.5,
			bounds: sdkBounds, lat: [2]float64{math.Log(12), 1.0}, sets: product(10, []kv{{"rpc.system", "grpc"}},
				dim{"rpc.method", []any{"Get", "List", "Create", "Update", "Delete"}}, dim{"rpc.grpc.status_code", []any{int64(0), int64(14)}})},
		// runtime (scope 3)
		{scope: 3, typ: tSum, name: "process.cpu.time", unit: "s", model: mCounterF, mu: -1.5, sigma: 0.8,
			sets: product(2, nil, dim{"cpu.mode", []any{"user", "system"}})},
		{scope: 3, typ: tSum, name: "system.network.io", unit: "By", model: mCounterBytes, mu: 9, sigma: 1.5,
			sets: product(4, nil, dim{"network.io.direction", []any{"receive", "transmit"}}, dim{"network.interface.name", []any{"eth0", "lo"}})},
		{scope: 3, typ: tSum, name: "runtime.gc.collections", unit: "{collection}", model: mCounter, mu: -2.5, sigma: 0.8,
			sets: product(4, nil, dim{"gc.name", []any{"G1 Young Generation", "G1 Old Generation", "G1 Concurrent GC", "go"}})},
		{scope: 3, typ: tGauge, name: "process.memory.usage", unit: "By", model: mWalk, mu: 19.5, sigma: 0.8, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "process.memory.virtual", unit: "By", model: mWalk, mu: 21, sigma: 0.5, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "container.memory.working_set", unit: "By", model: mWalk, mu: 19.8, sigma: 0.8, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "process.cpu.utilization", unit: "1", model: mRatio, mu: -1.8, sigma: 0.7, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "jvm.memory.used", unit: "By", model: mWalk, mu: 17, sigma: 1.2,
			sets: product(8, nil, dim{"jvm.memory.pool.name", []any{"G1 Eden Space", "G1 Old Gen", "G1 Survivor Space", "Metaspace"}},
				dim{"jvm.memory.type", []any{"heap", "non_heap"}})},
		{scope: 3, typ: tGauge, name: "go.goroutine.count", unit: "{goroutine}", model: mWalk, mu: 5, sigma: 0.8, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "process.open_file_descriptors", unit: "{fd}", model: mSticky, mu: 60, sigma: 0.5, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "process.threads", unit: "{thread}", model: mSticky, mu: 40, sigma: 0.5, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "process.uptime", unit: "s", model: mUptime, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "runtime.gc.pause.last", unit: "s", model: mRatio, mu: -6, sigma: 0.8, sets: product(1, nil)},
		{scope: 3, typ: tGauge, name: "thread.pool.active", unit: "{thread}", model: mSmall, mu: 1, sigma: 1, sets: product(20, nil, dim{"thread.pool.name", strs("worker-", 20)})},
		// app (scope 4)
		{scope: 4, typ: tSum, name: "messaging.process.count", unit: "{message}", model: mCounter, mu: -0.5, sigma: 1.8,
			sets: product(20, []kv{{"messaging.system", "kafka"}}, dim{"messaging.destination.name", strs("orders.events.", 10)}, dim{"messaging.operation.outcome", []any{"ok", "error"}})},
		{scope: 4, typ: tSum, name: "app.errors", unit: "{error}", model: mCounter, mu: -6, sigma: 1.5,
			sets: product(30, nil, dim{"error.type", []any{"timeout", "connection_refused", "validation", "not_found", "conflict",
				"internal", "rate_limited", "unauthorized", "canceled", "unavailable"}}, dim{"http.route", routes[:3]})},
		{scope: 4, typ: tSum, name: "cache.requests", unit: "{request}", model: mCounter, mu: 0, sigma: 1.5,
			sets: product(20, nil, dim{"cache.name", strs("cache-", 10)}, dim{"cache.result", []any{"hit", "miss"}})},
		{scope: 4, typ: tSum, name: "http.server.response.bytes", unit: "By", model: mCounterBytes, mu: 6, sigma: 2,
			sets: product(20, nil, dim{"http.route", routes}, dim{"http.response.body.encoding", []any{"gzip", "identity"}})},
		{scope: 4, typ: tSum, name: "log.records", unit: "{record}", model: mCounter, mu: -1, sigma: 2,
			sets: product(10, nil, dim{"log.severity", []any{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}}, dim{"logger.name", []any{"app", "http"}})},
		{scope: 4, typ: tSum, name: "jobs.processed", unit: "{job}", model: mCounter, mu: -3, sigma: 1.5,
			sets: product(20, nil, dim{"job.name", strs("job-", 10)}, dim{"job.outcome", []any{"success", "failure"}})},
		{scope: 4, typ: tGauge, name: "queue.depth", unit: "{message}", model: mSmall, mu: -1, sigma: 1.5, sets: product(20, nil, dim{"queue.name", strs("queue-", 20)})},
		{scope: 4, typ: tGauge, name: "cache.size", unit: "{entry}", model: mWalk, mu: 9, sigma: 1.5, sets: product(10, nil, dim{"cache.name", strs("cache-", 10)})},
		{scope: 4, typ: tGauge, name: "cache.hit_ratio", unit: "1", model: mRatio, mu: 1.5, sigma: 1, sets: product(10, nil, dim{"cache.name", strs("cache-", 10)})},
		{scope: 4, typ: tGauge, name: "app.config.value", unit: "1", model: mConst, mu: 3, sigma: 2, sets: product(40, nil, dim{"config.key", strs("feature.flag.", 40)})},
		{scope: 4, typ: tExp, name: "messaging.process.duration", unit: "s", model: mExp, mu: -0.5, sigma: 1.5,
			lat: [2]float64{math.Log(0.02), 1.2}, sets: product(5, nil, dim{"messaging.destination.name", strs("orders.events.", 5)})},
		{scope: 4, typ: tExp, name: "queue.wait.time", unit: "s", model: mExp, mu: -1, sigma: 1.5,
			lat: [2]float64{math.Log(0.5), 1.5}, sets: product(5, nil, dim{"queue.name", strs("queue-", 5)})},
		{scope: 4, typ: tSummary, name: "legacy.request.latency", unit: "ms", model: mSummary, mu: -1, sigma: 1.5,
			lat: [2]float64{math.Log(30), 0.9}, sets: product(5, nil, dim{"http.route", routes[:5]})},
		{scope: 4, typ: tSummary, name: "legacy.db.latency", unit: "ms", model: mSummary, mu: -1, sigma: 1.5,
			lat: [2]float64{math.Log(4), 1.0}, sets: product(5, nil, dim{"db.operation.name", ops[:5]})},
	}
	// Flatten pair dimensions ([]kv values) into their two attributes.
	for i := range s {
		for j, set := range s[i].sets {
			var flat []kv
			for _, a := range set {
				if p, ok := a.v.([]kv); ok {
					flat = append(flat, p...)
				} else {
					flat = append(flat, a)
				}
			}
			s[i].sets[j] = flat
		}
	}
	return s
}

var svcWords = []string{"frontend", "checkout", "payments", "catalog", "shipping", "cart", "search", "recommend",
	"inventory", "pricing", "auth", "users", "orders", "notify", "email", "ads", "reviews", "media", "gateway", "billing"}

var nsNames = []string{"shop", "platform", "payments", "catalog", "fulfilment", "identity", "marketing", "data"}

var langs = [][2]string{{"go", "1.38.0"}, {"java", "1.54.1"}, {"python", "1.37.0"}, {"dotnet", "1.12.0"}}

// pod is one pod slot; gen counts its rollouts.
type pod struct {
	slot, svc, ns, node int
	gen                 int
	offsetMs            int64 // export offset within the interval
	switchRound         int   // round at which the slot rolls out (-1: never)
	start               time.Time
	res                 []kv
	state               []series
	rng                 *rand.Rand
}

type series struct {
	rate      float64 // per second
	v         float64 // counter total / gauge value
	level     float64 // gauge mean level
	buckets   []uint64
	count     uint64
	sum       float64
	min, max  float64
	probs     []float64 // bucket probabilities
	condMean  []float64 // mean observation per bucket
	lmu, lsig float64
	scale     int32
}

// Fleet is the generator.
type Fleet struct {
	cfg     Config
	pods    []*pod
	round   int
	nextPod int
}

// New builds the fleet at round 0.
func New(cfg Config) *Fleet {
	f := &Fleet{cfg: cfg}
	nodes := (cfg.Pods() + cfg.PodsPerNode - 1) / cfg.PodsPerNode
	for slot := 0; slot < cfg.Pods(); slot++ {
		svc := slot / cfg.PodsPerService
		p := &pod{slot: slot, svc: svc, ns: svc % cfg.Namespaces, node: (slot*7 + svc) % nodes, switchRound: -1}
		r := rand.New(rand.NewPCG(uint64(slot)+1, 0x5eed))
		p.offsetMs = int64(r.IntN(int(cfg.Interval / time.Millisecond)))
		if cfg.RolloutEvery > 0 && svc%cfg.RolloutEvery == 0 {
			rs := rand.New(rand.NewPCG(uint64(svc)+7, 0xabc))
			hour := int(time.Hour / cfg.Interval)
			at := hour + rs.IntN(4*hour)
			p.switchRound = at + slot%cfg.PodsPerService
		}
		f.initPod(p, cfg.Start.Add(-time.Duration(1+r.IntN(72))*time.Hour))
		f.pods = append(f.pods, p)
	}
	// Order pods by node, as an edge (one collector per node) sees them.
	sortPods(f.pods)
	return f
}

func sortPods(ps []*pod) {
	for i := 1; i < len(ps); i++ {
		for j := i; j > 0 && (ps[j].node < ps[j-1].node || ps[j].node == ps[j-1].node && ps[j].slot < ps[j-1].slot); j-- {
			ps[j], ps[j-1] = ps[j-1], ps[j]
		}
	}
}

// Pods returns the number of pods (the unit Emit takes).
func (f *Fleet) Pods() int { return len(f.pods) }

// Round returns the next round Emit will produce.
func (f *Fleet) Round() int { return f.round }

// Nodes returns the node index of every pod, in Emit order.
func (f *Fleet) Nodes() []int {
	out := make([]int, len(f.pods))
	for i, p := range f.pods {
		out[i] = p.node
	}
	return out
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

func (f *Fleet) initPod(p *pod, start time.Time) {
	cfg := f.cfg
	p.rng = rand.New(rand.NewPCG(uint64(p.slot)*0x9e3779b97f4a7c15+uint64(p.gen), 0xfeed))
	r := rand.New(rand.NewPCG(uint64(p.slot)+99, uint64(p.gen)))
	p.start = start
	svcName := svcWords[p.svc%len(svcWords)]
	if p.svc >= len(svcWords) {
		svcName = fmt.Sprintf("%s-%d", svcName, p.svc/len(svcWords)+1)
	}
	ns := nsNames[p.ns%len(nsNames)]
	nodes := (cfg.Pods() + cfg.PodsPerNode - 1) / cfg.PodsPerNode
	node := (p.node + p.gen*3) % nodes
	nodeName := fmt.Sprintf("ip-10-%d-%d-%d.eu-west-1.compute.internal", node%3, 16+node/250, 10+node%250)
	version := fmt.Sprintf("1.%d.%d", 4+p.svc%7, 2+p.gen)
	rsHash := hexID(rand.New(rand.NewPCG(uint64(p.svc), uint64(p.gen))), 10)
	lang := langs[p.svc%len(langs)]
	p.res = []kv{
		{"service.name", svcName},
		{"service.namespace", ns},
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
		{"k8s.cluster.name", "prod-eu-west-1"},
		{"k8s.namespace.name", ns},
		{"k8s.deployment.name", svcName},
		{"k8s.replicaset.name", svcName + "-" + rsHash},
		{"k8s.pod.name", svcName + "-" + rsHash + "-" + hexID(r, 5)},
		{"k8s.pod.uid", uuid(r)},
		{"k8s.node.name", nodeName},
		{"k8s.container.name", svcName},
		{"container.image.tag", version},
	}
	// Per-series state.
	p.state = p.state[:0]
	for _, s := range specs {
		for range s.sets {
			p.state = append(p.state, newSeries(s, r))
		}
	}
}

func newSeries(s spec, r *rand.Rand) series {
	var st series
	st.rate = math.Exp(s.mu + s.sigma*r.NormFloat64())
	switch s.model {
	case mWalk:
		st.level = math.Exp(s.mu + s.sigma*r.NormFloat64())
		st.v = st.level
	case mRatio:
		st.level = 1 / (1 + math.Exp(-(s.mu + s.sigma*r.NormFloat64())))
		st.v = st.level
	case mSticky:
		st.level = math.Round(s.mu * (0.5 + r.Float64()))
		st.v = st.level
	case mConst:
		st.v = math.Round(math.Exp(s.mu + s.sigma*r.NormFloat64()))
	case mSmall:
		st.level = math.Exp(s.mu + s.sigma*r.NormFloat64())
	case mHist, mSummary, mExp:
		st.lmu = s.lat[0] + 0.4*r.NormFloat64()
		st.lsig = s.lat[1] * (0.8 + 0.4*r.Float64())
		st.min, st.max = math.Inf(1), math.Inf(-1)
		if s.model == mHist {
			nb := len(s.bounds) + 1
			st.buckets = make([]uint64, nb)
			st.probs = make([]float64, nb)
			st.condMean = make([]float64, nb)
			cdf := func(x float64) float64 {
				if x <= 0 {
					return 0
				}
				return 0.5 * math.Erfc(-(math.Log(x)-st.lmu)/(st.lsig*math.Sqrt2))
			}
			prev := 0.0
			for i := 0; i < nb; i++ {
				hi := 1.0
				if i < len(s.bounds) {
					hi = cdf(s.bounds[i])
				}
				st.probs[i] = hi - prev
				prev = hi
				lo := 0.0
				if i > 0 {
					lo = s.bounds[i-1]
				}
				up := lo * 2
				if i < len(s.bounds) {
					up = s.bounds[i]
				}
				if up == 0 {
					up = math.Exp(st.lmu) / 10
				}
				st.condMean[i] = (lo + up) / 2
			}
		}
		if s.model == mExp {
			st.scale = int32(2 + r.IntN(2))
		}
	}
	return st
}

func poisson(r *rand.Rand, lambda float64) uint64 {
	if lambda <= 0 {
		return 0
	}
	if lambda < 30 {
		l, k, p := math.Exp(-lambda), uint64(0), 1.0
		for {
			p *= r.Float64()
			if p <= l {
				return k
			}
			k++
		}
	}
	v := math.Round(lambda + math.Sqrt(lambda)*r.NormFloat64())
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// Emit returns the points of round f.Round() for pods [p0, p1) and advances
// past them. A round is complete when p1 == Pods(); the next call starts the
// next round at pod 0.
func (f *Fleet) Emit(p0, p1 int) pmetric.Metrics {
	if p0 != f.nextPod || p1 > len(f.pods) || p1 <= p0 {
		panic(fmt.Sprintf("fleet: Emit(%d,%d) out of order (next %d)", p0, p1, f.nextPod))
	}
	md := pmetric.NewMetrics()
	for _, p := range f.pods[p0:p1] {
		f.emitPod(md, p)
	}
	f.nextPod = p1
	if p1 == len(f.pods) {
		f.nextPod = 0
		f.round++
	}
	return md
}

// EmitRound returns the whole next round.
func (f *Fleet) EmitRound() pmetric.Metrics { return f.Emit(0, len(f.pods)) }

func (f *Fleet) emitPod(md pmetric.Metrics, p *pod) {
	cfg := f.cfg
	round := f.round
	if round == p.switchRound {
		p.gen++
		f.initPod(p, cfg.Start.Add(time.Duration(round)*cfg.Interval))
	}
	r := p.rng
	jitter := int64(r.NormFloat64() * 80)
	t := cfg.Start.Add(time.Duration(round)*cfg.Interval + time.Duration(p.offsetMs+jitter)*time.Millisecond)
	ts := pcommon.NewTimestampFromTime(t)
	start := pcommon.NewTimestampFromTime(p.start)
	dt := cfg.Interval.Seconds()
	// Diurnal swing, phase per service.
	day := 1 + 0.35*math.Sin(2*math.Pi*float64(t.Unix()%86400)/86400+float64(p.svc))

	rm := md.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
	ra := rm.Resource().Attributes()
	ra.EnsureCapacity(len(p.res))
	for _, a := range p.res {
		ra.PutStr(a.k, a.v.(string))
	}
	var sm pmetric.ScopeMetrics
	curScope := -1
	si := 0
	for _, s := range specs {
		if s.scope != curScope {
			curScope = s.scope
			sm = rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName(scopes[s.scope][0])
			sm.Scope().SetVersion(scopes[s.scope][1])
		}
		m := sm.Metrics().AppendEmpty()
		m.SetName(s.name)
		m.SetDescription(describe(s.name))
		m.SetUnit(s.unit)
		switch s.typ {
		case tGauge:
			dps := m.SetEmptyGauge().DataPoints()
			dps.EnsureCapacity(len(s.sets))
			for _, set := range s.sets {
				st := &p.state[si]
				si++
				dp := dps.AppendEmpty()
				putAttrs(dp.Attributes(), set)
				dp.SetTimestamp(ts)
				stepGauge(s, st, r, t, p.start, dp)
			}
		case tSum:
			sum := m.SetEmptySum()
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			sum.SetIsMonotonic(true)
			dps := sum.DataPoints()
			dps.EnsureCapacity(len(s.sets))
			for _, set := range s.sets {
				st := &p.state[si]
				si++
				dp := dps.AppendEmpty()
				putAttrs(dp.Attributes(), set)
				dp.SetStartTimestamp(start)
				dp.SetTimestamp(ts)
				lam := st.rate * dt * day
				switch s.model {
				case mCounterF:
					st.v += lam * (0.8 + 0.4*r.Float64())
					dp.SetDoubleValue(math.Round(st.v*1e6) / 1e6)
				case mCounterBytes:
					st.v += math.Round(math.Max(0, lam*(1+0.25*r.NormFloat64())))
					dp.SetIntValue(int64(st.v))
				default:
					st.v += float64(poisson(r, lam))
					dp.SetIntValue(int64(st.v))
				}
			}
		case tHist:
			h := m.SetEmptyHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			dps := h.DataPoints()
			dps.EnsureCapacity(len(s.sets))
			for _, set := range s.sets {
				st := &p.state[si]
				si++
				dp := dps.AppendEmpty()
				putAttrs(dp.Attributes(), set)
				dp.SetStartTimestamp(start)
				dp.SetTimestamp(ts)
				stepHist(s, st, r, st.rate*dt*day, ts, dp)
			}
		case tExp:
			h := m.SetEmptyExponentialHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			dps := h.DataPoints()
			for _, set := range s.sets {
				st := &p.state[si]
				si++
				dp := dps.AppendEmpty()
				putAttrs(dp.Attributes(), set)
				dp.SetStartTimestamp(pcommon.NewTimestampFromTime(t.Add(-cfg.Interval)))
				dp.SetTimestamp(ts)
				stepExp(st, r, st.rate*dt*day, dp)
			}
		case tSummary:
			dps := m.SetEmptySummary().DataPoints()
			for _, set := range s.sets {
				st := &p.state[si]
				si++
				dp := dps.AppendEmpty()
				putAttrs(dp.Attributes(), set)
				dp.SetStartTimestamp(start)
				dp.SetTimestamp(ts)
				n := poisson(r, st.rate*dt*day)
				st.count += n
				st.sum += float64(n) * math.Exp(st.lmu+st.lsig*st.lsig/2) * (0.9 + 0.2*r.Float64())
				dp.SetCount(st.count)
				dp.SetSum(math.Round(st.sum*1000) / 1000)
				for _, q := range []float64{0.5, 0.9, 0.99} {
					z := map[float64]float64{0.5: 0, 0.9: 1.2816, 0.99: 2.3263}[q]
					v := dp.QuantileValues().AppendEmpty()
					v.SetQuantile(q)
					v.SetValue(math.Round(math.Exp(st.lmu+st.lsig*z)*(0.95+0.1*r.Float64())*10) / 10)
				}
			}
		}
	}
}

func describe(name string) string { return "Measures " + name }

func putAttrs(m pcommon.Map, set []kv) {
	m.EnsureCapacity(len(set))
	for _, a := range set {
		switch v := a.v.(type) {
		case string:
			m.PutStr(a.k, v)
		case int64:
			m.PutInt(a.k, v)
		}
	}
}

func stepGauge(s spec, st *series, r *rand.Rand, t, podStart time.Time, dp pmetric.NumberDataPoint) {
	switch s.model {
	case mWalk:
		// Mean-reverting walk, quantised like real memory/queue counters.
		st.v += 0.05*(st.level-st.v) + st.level*0.01*r.NormFloat64()
		if st.v < 0 {
			st.v = 0
		}
		q := 1.0
		if st.level > 1e6 {
			q = 4096
		}
		dp.SetIntValue(int64(math.Round(st.v/q) * q))
	case mRatio:
		st.v += 0.2*(st.level-st.v) + st.level*0.08*r.NormFloat64()
		st.v = math.Min(1, math.Max(0, st.v))
		dp.SetDoubleValue(st.v)
	case mSmall:
		dp.SetIntValue(int64(poisson(r, st.level)))
	case mSticky:
		if r.IntN(12) == 0 {
			st.v += float64(r.IntN(7) - 3)
			if st.v < 0 {
				st.v = 0
			}
		}
		dp.SetIntValue(int64(st.v))
	case mConst:
		dp.SetIntValue(int64(st.v))
	case mUptime:
		dp.SetDoubleValue(math.Round(t.Sub(podStart).Seconds()*1000) / 1000)
	}
}

func stepHist(s spec, st *series, r *rand.Rand, lam float64, ts pcommon.Timestamp, dp pmetric.HistogramDataPoint) {
	n := poisson(r, lam)
	obsSeen := false
	if n > 0 && n <= 64 {
		for range n {
			x := math.Exp(st.lmu + st.lsig*r.NormFloat64())
			i := 0
			for i < len(s.bounds) && x > s.bounds[i] {
				i++
			}
			st.buckets[i]++
			st.sum += x
			st.min, st.max = math.Min(st.min, x), math.Max(st.max, x)
		}
		obsSeen = true
	} else if n > 64 {
		var tot uint64
		for i, p := range st.probs {
			c := poisson(r, float64(n)*p)
			st.buckets[i] += c
			st.sum += float64(c) * st.condMean[i]
			tot += c
		}
		n = tot
		// Extremes: the tails of n log-normal draws.
		z := math.Sqrt(2 * math.Log(float64(n)+1))
		st.min = math.Min(st.min, math.Exp(st.lmu-st.lsig*z*(0.9+0.2*r.Float64())))
		st.max = math.Max(st.max, math.Exp(st.lmu+st.lsig*z*(0.9+0.2*r.Float64())))
		obsSeen = n > 0
	}
	st.count += n
	dp.ExplicitBounds().FromRaw(s.bounds)
	dp.BucketCounts().FromRaw(st.buckets)
	dp.SetCount(st.count)
	dp.SetSum(st.sum)
	if st.count > 0 {
		dp.SetMin(st.min)
		dp.SetMax(st.max)
	}
	// Exemplars on a few points of the busiest metric, as a trace-sampling
	// SDK would attach them.
	if obsSeen && s.name == "http.server.request.duration" && r.IntN(20) == 0 {
		e := dp.Exemplars().AppendEmpty()
		e.SetTimestamp(ts - pcommon.Timestamp(r.IntN(30e9)))
		e.SetDoubleValue(math.Exp(st.lmu + st.lsig*r.NormFloat64()))
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
}

func stepExp(st *series, r *rand.Rand, lam float64, dp pmetric.ExponentialHistogramDataPoint) {
	n := poisson(r, lam)
	dp.SetScale(st.scale)
	if n == 0 {
		return
	}
	draws, w := n, uint64(1)
	if n > 256 {
		draws, w = 256, n/256
	}
	base := math.Ldexp(1/math.Ln2, int(st.scale)) // 2^scale / ln 2
	lo, hi := int32(math.MaxInt32), int32(math.MinInt32)
	idx := make([]int32, draws)
	var sum, mn, mx float64 = 0, math.Inf(1), math.Inf(-1)
	for i := range idx {
		x := math.Exp(st.lmu + st.lsig*r.NormFloat64())
		k := int32(math.Ceil(math.Log(x)*base)) - 1
		idx[i] = k
		lo, hi = min(lo, k), max(hi, k)
		sum += x
		mn, mx = math.Min(mn, x), math.Max(mx, x)
	}
	counts := make([]uint64, hi-lo+1)
	var c uint64
	for _, k := range idx {
		counts[k-lo] += w
		c += w
	}
	dp.Positive().SetOffset(lo)
	dp.Positive().BucketCounts().FromRaw(counts)
	dp.SetCount(c)
	dp.SetSum(sum * float64(w))
	dp.SetMin(mn)
	dp.SetMax(mx)
}
