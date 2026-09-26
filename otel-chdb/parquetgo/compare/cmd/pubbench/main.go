// pubbench publishes testgen batches as Parquet through either the chdb
// exporter (store_tables: false) or parquetgo, one implementation per
// process so RSS and startup are per implementation, and prints one JSON
// line of measurements.
//
//	pubbench -impl go|chdb -url file:///dir|http://host/bucket/prefix -signal traces|logs|metrics|metrics_gauge|... \
//	         -n 10000 -batches 30 -warmup 3 [-count-s3]
//	pubbench -impl edge [-layout series_table|clickstack_tables] -url http://host/bucket/prefix ...
//
// -impl edge is the manifest-less Go edge (../../edge, the s3pq exporter's
// core): one create-only PUT per object, content keys, lanes. Every batch
// differs from the previous one in one timestamp (for every impl), so the
// edge's lanes never skip a batch as already committed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/casselc/observability/otel-chdb/chdbexporter/testgen"
	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/compare"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var mainStart = time.Now()

type result struct {
	Impl, Signal, Dest     string
	Rows, Batches          int
	ReadyMS, FirstBatchMS  float64
	MedianMS, MinMS, MaxMS float64
	RowsPerSec             float64
	CPUMSPerBatch          float64
	GoAllocsPerBatch       float64
	GoBytesPerBatch        float64
	MaxRSSMB               float64
	RSSAfterStartMB        float64
	ObjectBytes            int64              `json:",omitempty"`
	S3PerBatch             map[string]float64 `json:",omitempty"`
}

func cpu() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func maxRSSMB() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Maxrss) / 1024
}

func rssMB() float64 {
	b, _ := os.ReadFile("/proc/self/statm")
	var size, res int64
	fmt.Sscan(string(b), &size, &res)
	return float64(res*int64(os.Getpagesize())) / (1 << 20)
}

// counter is a reverse proxy in front of the S3 endpoint that counts
// requests by method. It keeps the Host header, which SigV4 signed.
type counter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *counter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := map[string]int{}
	for k, v := range c.n {
		m[k] = v
	}
	return m
}

func startCounter(target string) (string, *counter) {
	u, err := url.Parse(target)
	if err != nil {
		log.Fatal(err)
	}
	c := &counter{n: map[string]int{}}
	rp := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(&url.URL{Scheme: u.Scheme, Host: u.Host})
		r.Out.Host = r.In.Host
		c.mu.Lock()
		c.n[r.In.Method]++
		c.mu.Unlock()
	}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go http.Serve(ln, rp)
	u2 := *u
	u2.Host = ln.Addr().String()
	return u2.String(), c
}

func main() {
	impl := flag.String("impl", "parquet-go", "chdb, arrow or parquet-go")
	par := flag.Int("par", 1, "parquet-go: columns encoded in parallel")
	dest := flag.String("url", "", "file:///dir or S3 prefix URL")
	signal := flag.String("signal", "traces", "traces, logs, metrics (all five types) or one of "+strings.Join(parquetgo.MetricSignals[:], ", "))
	n := flag.Int("n", 10000, "rows per batch")
	batches := flag.Int("batches", 30, "measured batches")
	warmup := flag.Int("warmup", 3, "unmeasured batches first")
	countS3 := flag.Bool("count-s3", false, "route S3 through a counting proxy")
	bloom := flag.Bool("bloom", true, "go: write bloom filters")
	compression := flag.String("compression", "zstd", "go: parquet codec")
	layout := flag.String("layout", "series_table", "edge: metrics layout")
	memprofile := flag.String("memprofile", "", "write an allocation profile of the measured batches")
	cpuprofile := flag.String("cpuprofile", "", "write a CPU profile of the measured batches")
	flag.Parse()

	s3, _ := compare.S3FromEnv()
	var cnt *counter
	url := *dest
	if *countS3 && strings.HasPrefix(url, "http") {
		url, cnt = startCounter(url)
	}
	epoch := fmt.Sprintf("b%d", time.Now().UnixNano())
	producer := "bench-" + *impl
	if *par > 1 {
		producer += fmt.Sprintf("-par%d", *par)
	}

	var push func() error
	var closeFn func() error
	ctx := context.Background()
	td, ld := testgen.Traces(*n), testgen.Logs(*n)
	// Metrics: -n data points of the named type, or of every type.
	md := pmetric.NewMetrics()
	metricSignals := []string{}
	if *signal == "metrics" {
		md = compare.Metrics(*n)
		metricSignals = parquetgo.MetricSignals[:]
	}
	for t, sig := range parquetgo.MetricSignals {
		if *signal == sig {
			md = compare.MetricsBatch(parquetgo.MetricType(t), *n, 0)
			metricSignals = []string{sig}
		}
	}
	// vary makes batch i distinct (one timestamp), the same way for every impl.
	vary := func(i int) {
		switch {
		case len(metricSignals) > 0:
			rm := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			for k := 0; k < rm.Len(); k++ {
				if m := rm.At(k); m.Type() == pmetric.MetricTypeGauge && m.Gauge().DataPoints().Len() > 0 {
					m.Gauge().DataPoints().At(0).SetStartTimestamp(pcommon.Timestamp(i))
					return
				}
			}
			for k := 0; k < rm.Len(); k++ {
				m := rm.At(k)
				switch m.Type() {
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().At(0).SetStartTimestamp(pcommon.Timestamp(i))
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().At(0).SetStartTimestamp(pcommon.Timestamp(i))
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().At(0).SetStartTimestamp(pcommon.Timestamp(i))
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().At(0).SetStartTimestamp(pcommon.Timestamp(i))
				default:
					continue
				}
				return
			}
		case *signal == "traces":
			sp := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
			sp.SetEndTimestamp(sp.StartTimestamp() + pcommon.Timestamp(i))
		default:
			ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetObservedTimestamp(pcommon.Timestamp(i))
		}
	}
	switch *impl {
	case "edge":
		e, err := edge.New(edge.Config{S3: parquetgo.Config{URL: url, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret},
			ProducerID: producer + "-" + epoch, MetricsLayout: *layout})
		if err != nil {
			log.Fatal(err)
		}
		switch {
		case len(metricSignals) > 0:
			push = func() error { return e.PushMetrics(ctx, md) }
		case *signal == "traces":
			push = func() error { return e.PushTraces(ctx, td) }
		default:
			push = func() error { return e.PushLogs(ctx, ld) }
		}
		closeFn = func() error { return nil }
	case "chdb":
		if len(metricSignals) > 0 {
			log.Fatal("the chdb exporter publishes no metrics")
		}
		dir, _ := os.MkdirTemp("", "pubbench-chdb")
		defer os.RemoveAll(dir)
		p, err := compare.NewChdbPublisher(dir, producer, epoch, url, s3)
		if err != nil {
			log.Fatal(err)
		}
		if *signal == "traces" {
			push = func() error { return p.Traces.ConsumeTraces(ctx, td) }
		} else {
			push = func() error { return p.Logs.ConsumeLogs(ctx, ld) }
		}
		closeFn = p.Shutdown
	case "arrow", "parquet-go":
		opts := parquetgo.DefaultOptions()
		opts.BloomFilters = *bloom
		opts.Compression = *compression
		opts.Parallelism = *par
		p, err := parquetgo.New(parquetgo.Config{URL: url, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret,
			ProducerID: producer, Region: "cmp", SchemaVersion: 1, Epoch: epoch, Parquet: opts, Engine: *impl})
		if err != nil {
			log.Fatal(err)
		}
		switch {
		case len(metricSignals) > 0:
			push = func() error { return p.PushMetrics(ctx, md) }
		case *signal == "traces":
			push = func() error { return p.PushTraces(ctx, td) }
		default:
			push = func() error { return p.PushLogs(ctx, ld) }
		}
		closeFn = func() error { return p.Close(ctx) }
	default:
		log.Fatalf("impl %q", *impl)
	}
	name := *impl
	if *impl == "edge" && len(metricSignals) > 0 {
		name += "-" + *layout
	}
	if *par > 1 {
		name += fmt.Sprintf("-par%d", *par)
	}
	if !*bloom {
		name += "-nobloom"
	}
	inner, round := push, 0
	push = func() error { round++; vary(round); return inner() }
	r := result{Impl: name, Signal: *signal, Rows: *n, Batches: *batches}
	r.Dest = "local"
	if strings.HasPrefix(*dest, "http") {
		r.Dest = "s3"
	}
	r.ReadyMS = ms(time.Since(mainStart))
	r.RSSAfterStartMB = rssMB()
	if err := push(); err != nil {
		log.Fatal(err)
	}
	r.FirstBatchMS = ms(time.Since(mainStart))
	for i := 1; i < *warmup; i++ {
		if err := push(); err != nil {
			log.Fatal(err)
		}
	}
	var c0 map[string]int
	if cnt != nil {
		c0 = cnt.snapshot()
	}
	if *memprofile != "" {
		runtime.MemProfileRate = 64 << 10
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		_ = pprof.StartCPUProfile(f)
	}
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	cpu0 := cpu()
	lat := make([]float64, 0, *batches)
	t0 := time.Now()
	for i := 0; i < *batches; i++ {
		s := time.Now()
		if err := push(); err != nil {
			log.Fatal(err)
		}
		lat = append(lat, ms(time.Since(s)))
	}
	el := time.Since(t0)
	cpu1 := cpu()
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		if f, err := os.Create(*memprofile); err == nil {
			_ = pprof.Lookup("allocs").WriteTo(f, 0)
			f.Close()
		}
	}
	runtime.ReadMemStats(&ms1)
	sort.Float64s(lat)
	r.MedianMS, r.MinMS, r.MaxMS = lat[len(lat)/2], lat[0], lat[len(lat)-1]
	r.RowsPerSec = float64(*n**batches) / el.Seconds()
	r.CPUMSPerBatch = ms(cpu1-cpu0) / float64(*batches)
	r.GoAllocsPerBatch = float64(ms1.Mallocs-ms0.Mallocs) / float64(*batches)
	r.GoBytesPerBatch = float64(ms1.TotalAlloc-ms0.TotalAlloc) / float64(*batches)
	if cnt != nil {
		c1 := cnt.snapshot()
		r.S3PerBatch = map[string]float64{}
		for k, v := range c1 {
			r.S3PerBatch[k] = float64(v-c0[k]) / float64(*batches)
		}
	}
	if err := closeFn(); err != nil {
		log.Fatal(err)
	}
	r.MaxRSSMB = maxRSSMB()
	if strings.HasPrefix(*dest, "file://") {
		sigs := metricSignals
		if len(sigs) == 0 {
			sigs = []string{*signal}
		}
		for _, sig := range sigs {
			matches, _ := filepath.Glob(filepath.Join(strings.TrimPrefix(*dest, "file://"), "cmp", sig, "v1", producer, epoch, "g*", "*.parquet"))
			if len(matches) > 0 {
				if st, err := os.Stat(matches[0]); err == nil {
					r.ObjectBytes += st.Size()
				}
			}
		}
	}
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
