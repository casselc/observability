// otapbench publishes testgen batches through one variant per process and
// prints one JSON line of measurements, like ../../parquetgo/compare/cmd/pubbench
// (same batch size, warm-up and timed-batch counts, same CPU/RSS/alloc
// accounting), so the numbers line up with parquetgo's.
//
//	otapbench -variant ref|star|flat-parquet|flat-arrow|via-pdata|bar|encode|decode|flatten|startables \
//	          -url file:///dir|http://host/bucket/prefix -signal traces|logs -n 10000 -batches 30 -warmup 3 [-count-s3]
//
// The OTAP variants start from a BatchArrowRecords built once before timing:
// an OTAP edge receives OTAP, it does not build it. "encode" times that
// pdata -> OTAP step alone (what an edge receiving OTLP would add).
// "decode", "flatten" and "startables" stop after that stage and write nothing.
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
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/otap"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
)

var mainStart = time.Now()

type result struct {
	Variant, Signal, Dest  string
	Rows, Batches          int
	ReadyMS                float64
	MedianMS, MinMS, MaxMS float64
	RowsPerSec             float64
	CPUMSPerBatch          float64
	GoAllocsPerBatch       float64
	GoBytesPerBatch        float64
	MaxRSSMB               float64
	ObjectBytes            int                `json:",omitempty"`
	Objects                int                `json:",omitempty"`
	BARBytes               int                `json:",omitempty"`
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

// startCounter is a reverse proxy in front of the S3 endpoint that counts
// requests by method (it keeps the Host header, which SigV4 signed).
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
	variant := flag.String("variant", "star", "ref, star, flat-parquet, flat-arrow, via-pdata, bar, encode, decode, flatten, startables")
	dest := flag.String("url", "", "file:///dir or S3 prefix URL")
	signal := flag.String("signal", "traces", "traces or logs")
	n := flag.Int("n", 10000, "rows per batch")
	batches := flag.Int("batches", 30, "measured batches")
	warmup := flag.Int("warmup", 3, "unmeasured batches first")
	countS3 := flag.Bool("count-s3", false, "route S3 through a counting proxy")
	bloom := flag.Bool("bloom", true, "write bloom filters")
	compression := flag.String("compression", "zstd", "zstd or none (Arrow IPC: zstd, lz4 or none)")
	flag.Parse()

	key, secret := os.Getenv("OTAP_TEST_S3_KEY"), os.Getenv("OTAP_TEST_S3_SECRET")
	u := *dest
	var cnt *counter
	if *countS3 && strings.HasPrefix(u, "http") {
		u, cnt = startCounter(u)
	}
	opts := parquetgo.DefaultOptions()
	opts.BloomFilters = *bloom
	opts.Compression = *compression
	var sink *otap.Sink
	if u != "" {
		var err error
		if sink, err = otap.NewSink(u, key, secret, nil); err != nil {
			log.Fatal(err)
		}
	}
	epoch := fmt.Sprintf("b%d", time.Now().UnixNano())
	p := &otap.Publisher{Sink: sink, Producer: "bench-" + *variant, Epoch: epoch, SchemaVersion: 1, Parquet: opts}

	td, ld := testgen.Traces(*n), testgen.Logs(*n)
	var bar *pb.BatchArrowRecords
	var err error
	if *signal == "traces" {
		bar, err = otap.EncodeTraces(td)
	} else {
		bar, err = otap.EncodeLogs(ld)
	}
	if err != nil {
		log.Fatal(err)
	}
	barBytes := 0
	for _, pl := range bar.ArrowPayloads {
		barBytes += len(pl.Record)
	}

	ctx := context.Background()
	var last otap.Manifest
	env := &parquetgo.Envelope{Producer: "b", Epoch: epoch}
	push := func() error {
		switch *variant {
		case "ref":
			var err error
			if *signal == "traces" {
				last, err = p.PublishRef(ctx, &td, nil)
			} else {
				last, err = p.PublishRef(ctx, nil, &ld)
			}
			return err
		case "encode":
			if *signal == "traces" {
				_, err = otap.EncodeTraces(td)
			} else {
				_, err = otap.EncodeLogs(ld)
			}
			return err
		case "decode", "flatten", "startables":
			b, err := otap.Decode(bar)
			if err != nil {
				return err
			}
			defer b.Release()
			if *variant == "flatten" {
				otap.Flatten(b, env, nil).Release()
			} else if *variant == "startables" {
				tabs, err := otap.StarTables(b, env, nil)
				if err != nil {
					return err
				}
				for _, r := range tabs {
					r.Release()
				}
			}
			return nil
		default:
			var err error
			last, err = p.PublishBAR(ctx, *variant, bar)
			return err
		}
	}

	r := result{Variant: *variant, Signal: *signal, Rows: *n, Batches: *batches, BARBytes: barBytes, Dest: "none"}
	if strings.HasPrefix(*dest, "http") {
		r.Dest = "s3"
	} else if strings.HasPrefix(*dest, "file") {
		r.Dest = "local"
	}
	if !*bloom {
		r.Variant += "-nobloom"
	}
	r.ReadyMS = float64(time.Since(mainStart).Microseconds()) / 1000
	for i := 0; i < *warmup; i++ {
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
		lat = append(lat, float64(time.Since(s).Microseconds())/1000)
	}
	el := time.Since(t0)
	cpu1 := cpu()
	runtime.ReadMemStats(&ms1)
	sort.Float64s(lat)
	r.MedianMS, r.MinMS, r.MaxMS = lat[len(lat)/2], lat[0], lat[len(lat)-1]
	r.RowsPerSec = float64(*n**batches) / el.Seconds()
	r.CPUMSPerBatch = float64((cpu1 - cpu0).Microseconds()) / 1000 / float64(*batches)
	r.GoAllocsPerBatch = float64(ms1.Mallocs-ms0.Mallocs) / float64(*batches)
	r.GoBytesPerBatch = float64(ms1.TotalAlloc-ms0.TotalAlloc) / float64(*batches)
	if cnt != nil {
		c1 := cnt.snapshot()
		r.S3PerBatch = map[string]float64{}
		for k, v := range c1 {
			r.S3PerBatch[k] = float64(v-c0[k]) / float64(*batches)
		}
	}
	for _, o := range last.Objects {
		r.ObjectBytes += o.Bytes
	}
	r.Objects = len(last.Objects)
	r.MaxRSSMB = maxRSSMB()
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}
