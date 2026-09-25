// promtsdb is layout D: the fleet dataset appended straight into the
// Prometheus TSDB (github.com/prometheus/prometheus/tsdb, v0.303.0 =
// Prometheus 3.3) through its Go API, as a Prometheus server's head does,
// then persisted to one block. It reports:
//
//   - ingest CPU per sample and per OTel point: conversion to labels
//     (promconv) plus Append/Commit, and the conversion alone;
//   - the head-to-block compaction CPU;
//   - block bytes (chunks + index) per sample and per OTel point.
//
// -native stores histograms as native histograms (explicit buckets as
// custom-bucket NHCB, exponential as native exponential gauge histograms)
// instead of classic _bucket series.
//
//	promtsdb -services 4 -rounds 720 -dir /scratch/prom [-native]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/promconv"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/zeebo/xxh3"
)

func cpu() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// sink appends into a head appender with a series-ref cache keyed by a hash
// of the label set (what the scrape loop's cache does with the raw line).
type sink struct {
	app   storage.Appender
	refs  map[uint64]storage.SeriesRef
	sb    labels.ScratchBuilder
	hb    []byte
	count int
	err   error
}

func (s *sink) key(l []promconv.Label) uint64 {
	s.hb = s.hb[:0]
	for _, x := range l {
		s.hb = append(s.hb, x.Name...)
		s.hb = append(s.hb, 0xff)
		s.hb = append(s.hb, x.Value...)
		s.hb = append(s.hb, 0xff)
	}
	return xxh3.Hash(s.hb)
}

func (s *sink) lset(l []promconv.Label) labels.Labels {
	s.sb.Reset()
	for _, x := range l {
		s.sb.Add(x.Name, x.Value)
	}
	return s.sb.Labels()
}

func (s *sink) Float(l []promconv.Label, t int64, v float64) {
	k := s.key(l)
	ref := s.refs[k]
	var ls labels.Labels
	if ref == 0 {
		ls = s.lset(l)
	}
	r, err := s.app.Append(ref, ls, t, v)
	if err != nil && s.err == nil {
		s.err = err
	}
	s.refs[k] = r
	s.count++
}

func (s *sink) Hist(l []promconv.Label, t int64, h *promconv.Hist) {
	k := s.key(l)
	ref := s.refs[k]
	var ls labels.Labels
	if ref == 0 {
		ls = s.lset(l)
	}
	ph := &histogram.Histogram{Schema: h.Schema, Count: h.Count, Sum: h.Sum, ZeroCount: h.ZeroCount}
	if h.Delta {
		ph.CounterResetHint = histogram.GaugeType
	}
	n := len(h.Counts)
	if n > 0 {
		off := int32(0)
		if h.Schema != -53 {
			off = h.Offset + 1 // OTel bucket i is Prometheus bucket i+1
		}
		ph.PositiveSpans = []histogram.Span{{Offset: off, Length: uint32(n)}}
		ph.PositiveBuckets = make([]int64, n)
		prev := int64(0)
		for i, c := range h.Counts {
			ph.PositiveBuckets[i] = int64(c) - prev
			prev = int64(c)
		}
	}
	if h.Schema == -53 {
		ph.CustomValues = h.Bounds
		if n == 0 {
			ph.PositiveSpans = nil
		}
	}
	r, err := s.app.AppendHistogram(ref, ls, t, ph, nil)
	if err != nil && s.err == nil {
		s.err = err
	}
	s.refs[k] = r
	s.count++
}

type nop struct{}

func (nop) Float([]promconv.Label, int64, float64)       {}
func (nop) Hist([]promconv.Label, int64, *promconv.Hist) {}

func dirSize(dir string) (chunks, index, total int64) {
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		total += fi.Size()
		if filepath.Base(filepath.Dir(p)) == "chunks" {
			chunks += fi.Size()
		}
		if fi.Name() == "index" {
			index += fi.Size()
		}
		return nil
	})
	return
}

func main() {
	services := flag.Int("services", 4, "services (10 pods each)")
	rounds := flag.Int("rounds", 720, "rounds")
	dir := flag.String("dir", "", "tsdb directory (emptied)")
	native := flag.Bool("native", false, "native histograms")
	flag.Parse()
	os.RemoveAll(*dir)
	opts := tsdb.DefaultOptions()
	opts.EnableNativeHistograms = true
	opts.MinBlockDuration = int64(24 * time.Hour / time.Millisecond) // one block for the whole run
	opts.MaxBlockDuration = opts.MinBlockDuration
	opts.RetentionDuration = 0
	db, err := tsdb.Open(*dir, slog.New(slog.DiscardHandler), nil, opts, nil)
	if err != nil {
		log.Fatal(err)
	}
	db.DisableCompactions()
	cfg := fleet.Default()
	cfg.Services = *services
	f := fleet.New(cfg)
	conv := promconv.New(*native)
	conv2 := promconv.New(*native)
	s := &sink{refs: map[uint64]storage.SeriesRef{}}
	var ingest, convOnly, gen time.Duration
	points, floats, hists := 0, 0, 0
	for r := 0; r < *rounds; r++ {
		c0 := cpu()
		md := f.EmitRound()
		c1 := cpu()
		gen += c1 - c0
		points += md.DataPointCount()
		// Conversion alone (a separate converter, same work).
		conv2.Walk(md, nop{})
		c2 := cpu()
		convOnly += c2 - c1
		s.app = db.Appender(context.Background())
		fl, hs := conv.Walk(md, s)
		if err := s.app.Commit(); err != nil {
			log.Fatal(err)
		}
		if s.err != nil {
			log.Fatal(s.err)
		}
		ingest += cpu() - c2
		floats += fl
		hists += hs
		if r%120 == 0 {
			log.Printf("round %d, %d series", r, db.Head().NumSeries())
		}
	}
	head := db.Head()
	numSeries := head.NumSeries()
	c0 := cpu()
	if err := db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime())); err != nil {
		log.Fatal(err)
	}
	compact := cpu() - c0
	var chunks, index, total int64
	blocks := db.Blocks()
	var bs []tsdb.BlockStats
	for _, b := range blocks {
		c, i, t := dirSize(b.Dir())
		chunks, index, total = chunks+c, index+i, total+t
		bs = append(bs, b.Meta().Stats)
	}
	samples := floats + hists
	out := map[string]any{
		"native": *native, "services": *services, "rounds": *rounds, "otelPoints": points,
		"floatSamples": floats, "histSamples": hists, "headSeries": numSeries, "blockStats": bs,
		"samplesPerPoint":      float64(samples) / float64(points),
		"ingestCPUusPerSample": float64(ingest.Nanoseconds()) / 1e3 / float64(samples),
		"ingestCPUusPerPoint":  float64(ingest.Nanoseconds()) / 1e3 / float64(points),
		"convertCPUusPerPoint": float64(convOnly.Nanoseconds()) / 1e3 / float64(points),
		"appendCPUusPerPoint":  float64((ingest - convOnly).Nanoseconds()) / 1e3 / float64(points),
		"compactCPUusPerPoint": float64(compact.Nanoseconds()) / 1e3 / float64(points),
		"blockBytesPerSample":  float64(total) / float64(samples),
		"chunkBytesPerSample":  float64(chunks) / float64(samples),
		"indexBytesPerSample":  float64(index) / float64(samples),
		"blockBytesPerPoint":   float64(total) / float64(points),
		"chunkBytesPerPoint":   float64(chunks) / float64(points),
		"indexBytesPerPoint":   float64(index) / float64(points),
		"indexBytesPerSeries":  float64(index) / float64(numSeries),
		"genCPUs":              gen.Seconds(),
	}
	out["queries"] = runQueries(db, *native)
	_ = json.NewEncoder(os.Stdout).Encode(out)
	db.Close()
}

// runQueries runs the dashboard queries (sql/queries.sql's Q1-Q4 in PromQL)
// over the persisted block, for the last hour (1m step) and the whole run
// (5m step): median wall and process CPU of 5 runs after a warm-up.
func runQueries(db *tsdb.DB, native bool) []map[string]any {
	eng := promql.NewEngine(promql.EngineOpts{MaxSamples: 500_000_000, Timeout: 5 * time.Minute, LookbackDelta: 5 * time.Minute})
	start := fleet.Default().Start
	type w struct {
		name     string
		from, to time.Time
		step     time.Duration
		rng      string
	}
	ws := []w{{"1h", start.Add(5 * time.Hour), start.Add(6 * time.Hour), time.Minute, "1m"}, {"6h", start, start.Add(6*time.Hour + time.Minute), 5 * time.Minute, "5m"}}
	var out []map[string]any
	for _, win := range ws {
		q2 := `histogram_quantile(0.99, sum by (le) (rate(http_server_request_duration_bucket{service_name="checkout"}[` + win.rng + `])))`
		if native {
			q2 = `histogram_quantile(0.99, sum(rate(http_server_request_duration{service_name="checkout"}[` + win.rng + `])))`
		}
		qs := []struct {
			id, q   string
			instant bool
		}{
			{"Q1", `sum by (k8s_pod_name) (rate(http_server_request_count_total{service_name="checkout"}[` + win.rng + `]))`, false},
			{"Q2", q2, false},
			{"Q3", `topk(10, avg by (k8s_pod_name) (avg_over_time(process_memory_usage[` + fmt.Sprint(int(win.to.Sub(win.from).Seconds())) + `s])))`, true},
			{"Q4", `sum by (service_name) (rate(http_server_request_count_total{k8s_namespace_name="payments"}[` + win.rng + `]))`, false},
		}
		for _, q := range qs {
			var walls, cpus []float64
			var n int
			var errs string
			for i := 0; i < 6; i++ {
				c0, t0 := cpu(), time.Now()
				var qry promql.Query
				var err error
				if q.instant {
					qry, err = eng.NewInstantQuery(context.Background(), db, nil, q.q, win.to)
				} else {
					qry, err = eng.NewRangeQuery(context.Background(), db, nil, q.q, win.from.Add(win.step), win.to, win.step)
				}
				if err != nil {
					errs = err.Error()
					break
				}
				res := qry.Exec(context.Background())
				if res.Err != nil {
					errs = res.Err.Error()
					break
				}
				switch v := res.Value.(type) {
				case promql.Matrix:
					n = len(v)
				case promql.Vector:
					n = len(v)
				}
				qry.Close()
				if i > 0 {
					walls = append(walls, float64(time.Since(t0).Microseconds())/1e3)
					cpus = append(cpus, float64((cpu()-c0).Microseconds())/1e3)
				}
			}
			m := map[string]any{"query": q.id, "window": win.name, "promql": q.q, "resultSeries": n}
			if errs != "" {
				m["err"] = errs
			} else {
				sort.Float64s(walls)
				sort.Float64s(cpus)
				m["wallMs"], m["cpuMs"] = walls[len(walls)/2], cpus[len(cpus)/2]
			}
			out = append(out, m)
		}
	}
	return out
}
