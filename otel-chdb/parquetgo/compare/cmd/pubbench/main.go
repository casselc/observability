// pubbench publishes testgen batches as Parquet through either the chdb
// exporter (store_tables: false) or parquetgo, one implementation per
// process so RSS and startup are per implementation, and prints one JSON
// line of measurements.
//
//	pubbench -impl go|chdb -url file:///dir|http://host/bucket/prefix -signal traces|logs \
//	         -n 10000 -batches 30 -warmup 3 [-count-s3]
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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare"
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
	impl := flag.String("impl", "go", "go or chdb")
	dest := flag.String("url", "", "file:///dir or S3 prefix URL")
	signal := flag.String("signal", "traces", "traces or logs")
	n := flag.Int("n", 10000, "rows per batch")
	batches := flag.Int("batches", 30, "measured batches")
	warmup := flag.Int("warmup", 3, "unmeasured batches first")
	countS3 := flag.Bool("count-s3", false, "route S3 through a counting proxy")
	bloom := flag.Bool("bloom", true, "go: write bloom filters")
	compression := flag.String("compression", "zstd", "go: parquet codec")
	flag.Parse()

	s3, _ := compare.S3FromEnv()
	var cnt *counter
	url := *dest
	if *countS3 && strings.HasPrefix(url, "http") {
		url, cnt = startCounter(url)
	}
	epoch := fmt.Sprintf("b%d", time.Now().UnixNano())
	producer := "bench-" + *impl

	var push func() error
	var closeFn func() error
	ctx := context.Background()
	td, ld := testgen.Traces(*n), testgen.Logs(*n)
	switch *impl {
	case "chdb":
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
	case "go":
		opts := parquetgo.DefaultOptions()
		opts.BloomFilters = *bloom
		opts.Compression = *compression
		p, err := parquetgo.New(parquetgo.Config{URL: url, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret,
			ProducerID: producer, Region: "cmp", SchemaVersion: 1, Epoch: epoch, Parquet: opts})
		if err != nil {
			log.Fatal(err)
		}
		if *signal == "traces" {
			push = func() error { return p.PushTraces(ctx, td) }
		} else {
			push = func() error { return p.PushLogs(ctx, ld) }
		}
		closeFn = func() error { return p.Close(ctx) }
	default:
		log.Fatalf("impl %q", *impl)
	}
	r := result{Impl: *impl, Signal: *signal, Rows: *n, Batches: *batches}
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
		matches, _ := filepath.Glob(filepath.Join(strings.TrimPrefix(*dest, "file://"), "cmp", *signal, "v1", producer, epoch, "g*", "*.parquet"))
		if len(matches) > 0 {
			if st, err := os.Stat(matches[0]); err == nil {
				r.ObjectBytes = st.Size()
			}
		}
	}
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
