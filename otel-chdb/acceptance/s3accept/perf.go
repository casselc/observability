package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type PerfStat struct {
	Op       string  `json:"op"`
	Size     int     `json:"size_bytes"`
	N        int     `json:"n"`
	Errors   int     `json:"errors"`
	Conc     int     `json:"concurrency"`
	WallS    float64 `json:"wall_s"`
	OpsPerS  float64 `json:"ops_per_s"`
	MBPerS   float64 `json:"mb_per_s,omitempty"`
	P50ms    float64 `json:"p50_ms"`
	P90ms    float64 `json:"p90_ms"`
	P99ms    float64 `json:"p99_ms"`
	MaxMs    float64 `json:"max_ms"`
	FirstErr string  `json:"first_error,omitempty"`
}

// runOps runs op(i) for i in [0,n) on conc goroutines and summarises latencies.
func runOps(ctx context.Context, name string, size, n, conc int, op func(context.Context, int) error) PerfStat {
	lat := make([]time.Duration, n)
	errs := make([]error, n)
	jobs := make(chan int)
	var wg sync.WaitGroup
	t0 := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				s := time.Now()
				errs[i] = op(ctx, i)
				lat[i] = time.Since(s)
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	wall := time.Since(t0)
	st := PerfStat{Op: name, Size: size, N: n, Conc: conc, WallS: wall.Seconds()}
	var ok []time.Duration
	for i, err := range errs {
		if err != nil {
			st.Errors++
			if st.FirstErr == "" {
				st.FirstErr = describe(err)
			}
			continue
		}
		ok = append(ok, lat[i])
	}
	sort.Slice(ok, func(a, b int) bool { return ok[a] < ok[b] })
	pct := func(p float64) float64 {
		if len(ok) == 0 {
			return 0
		}
		i := int(p*float64(len(ok))+0.5) - 1
		i = max(0, min(i, len(ok)-1))
		return float64(ok[i].Microseconds()) / 1000
	}
	st.P50ms, st.P90ms, st.P99ms = pct(0.50), pct(0.90), pct(0.99)
	if len(ok) > 0 {
		st.MaxMs = float64(ok[len(ok)-1].Microseconds()) / 1000
	}
	st.OpsPerS = float64(len(ok)) / wall.Seconds()
	if size > 0 {
		st.MBPerS = st.OpsPerS * float64(size) / 1e6
	}
	return st
}

func fmtSize(b int) string {
	switch {
	case b >= 1<<20 && b%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", b>>20)
	case b >= 1000000 && b%1000000 == 0:
		return fmt.Sprintf("%dMB", b/1000000)
	case b >= 1000:
		return fmt.Sprintf("%dKB", b/1000)
	}
	return fmt.Sprintf("%dB", b)
}

func checkPerf(ctx context.Context, e *Env, r *Result) {
	p := e.Params
	var stats []PerfStat
	add := func(s PerfStat) {
		stats = append(stats, s)
		r.Log("%-9s %-7s n=%-4d c=%-3d err=%-3d %7.1f op/s %7.1f MB/s  p50 %7.1f  p90 %7.1f  p99 %7.1f  max %7.1f ms %s",
			s.Op, fmtSize(s.Size), s.N, s.Conc, s.Errors, s.OpsPerS, s.MBPerS, s.P50ms, s.P90ms, s.P99ms, s.MaxMs, s.FirstErr)
	}
	for _, size := range p.PerfSizes {
		payload := make([]byte, size)
		_, _ = rand.Read(payload) // incompressible, like zstd'd Parquet
		dir := e.Key("perf", fmtSize(size))
		key := func(i int) string { return fmt.Sprintf("%s/%020d.parquet", dir, i) }
		add(runOps(ctx, "PUT-INM", size, p.PerfOps, p.PerfConc, func(ctx context.Context, i int) error {
			b := append([]byte(fmt.Sprintf("%020d", i)), payload[20:]...)
			_, err := e.put(ctx, putReq{Key: key(i), Bytes: b, INM: "*", CT: "application/vnd.apache.parquet"})
			return err
		}))
		add(runOps(ctx, "GET", size, p.PerfOps, p.PerfConc, func(ctx context.Context, i int) error {
			cctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
			defer cancel()
			out, err := e.S3.GetObject(cctx, &s3.GetObjectInput{Bucket: &e.O.Bucket, Key: aws.String(key(i))})
			if err != nil {
				return err
			}
			defer out.Body.Close()
			_, err = io.Copy(io.Discard, out.Body)
			return err
		}))
		add(runOps(ctx, "HEAD", 0, p.PerfOps, p.PerfConc, func(ctx context.Context, i int) error {
			_, err := e.head(ctx, key(i))
			return err
		}))
		if size == p.PerfSizes[0] {
			add(runOps(ctx, "LIST", 0, max(10, p.PerfOps/4), p.PerfConc, func(ctx context.Context, i int) error {
				_, err := e.list(ctx, dir+"/", key((i*7)%p.PerfOps), "", 100, nil)
				return err
			}))
		}
	}
	// The control plane's small requests: a log slot, a CAS'd checkpoint, a free-slot probe.
	slots := e.Key("perf", "slots")
	add(runOps(ctx, "PUT-slot", 200, p.PerfOps, p.PerfConc, func(ctx context.Context, i int) error {
		_, err := e.put(ctx, putReq{Key: fmt.Sprintf("%s/%020d.json", slots, i), Bytes: make([]byte, 200), INM: "*"})
		return err
	}))
	etags := make([]string, p.PerfConc)
	locks := make([]sync.Mutex, p.PerfConc)
	add(runOps(ctx, "CAS", 200, p.PerfOps, p.PerfConc, func(ctx context.Context, i int) error {
		w := i % p.PerfConc // one checkpoint per lane, one CAS at a time per lane: latency, not contention
		locks[w].Lock()
		defer locks[w].Unlock()
		k := e.Key("perf", "ckpt", fmt.Sprint(w))
		et := etags[w]
		body := []byte(fmt.Sprintf(`{"next":%d,"pad":"%0180d"}`, i, 0))
		var ne string
		var err error
		if et == "" {
			ne, err = e.put(ctx, putReq{Key: k, Bytes: body})
		} else {
			ne, err = e.put(ctx, putReq{Key: k, Bytes: body, IM: et})
		}
		if err == nil {
			etags[w] = ne
		}
		return err
	}))
	add(runOps(ctx, "HEAD-404", 0, p.PerfOps, p.PerfConc, func(ctx context.Context, i int) error {
		_, err := e.head(ctx, fmt.Sprintf("%s/free/%020d.json", slots, i))
		if o, _, _ := classify(err); o == NotFound {
			return nil
		}
		if err == nil {
			return fmt.Errorf("free slot exists")
		}
		return err
	}))
	r.Set("perf", stats)
	r.Worsen(INFO, "", "")
	var worst []string
	for _, s := range stats {
		if s.Errors > 0 {
			worst = append(worst, fmt.Sprintf("%s %s: %d errors (%s)", s.Op, fmtSize(s.Size), s.Errors, s.FirstErr))
		} else if time.Duration(s.P99ms*float64(time.Millisecond)) > p.PerfWarnP99 {
			worst = append(worst, fmt.Sprintf("%s %s p99 %.0f ms", s.Op, fmtSize(s.Size), s.P99ms))
		}
	}
	sum := ""
	for _, s := range stats {
		if s.Op == "PUT-INM" || s.Op == "PUT-slot" || s.Op == "CAS" {
			sum += fmt.Sprintf("%s %s p50/p99 %.0f/%.0f ms; ", s.Op, fmtSize(s.Size), s.P50ms, s.P99ms)
		}
	}
	r.Summary = sum + "see evidence/JSON for GET/HEAD/LIST"
	if len(worst) > 0 {
		r.Worsen(WARN, "", fmt.Sprintf("Over the p99 budget (%s) or erroring: %v. The edge acks a batch after the data PUT "+
			"and the slot PUT, so their p99 sets the exporter's timeout and queue depth; the consumer's visibility "+
			"budget adds HEAD, LIST and CAS. Re-run once to rule out a cold start (AWS partitions a new prefix "+
			"under load: 503 SlowDown).", p.PerfWarnP99, worst))
	}
}
