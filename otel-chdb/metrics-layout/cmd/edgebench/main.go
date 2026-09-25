// edgebench measures the edge encode for layouts A (parquetgo, contrib
// schema) and B (seriesenc, narrow points + series object) on the same fleet
// batches: process CPU (getrusage, user+sys, GC included) around each
// encoder call only, generation excluded, and Parquet bytes per point by
// type. Encode only: the PUT is the same for both and was measured in
// ../parquetgo (pubbench, +0.2–0.4 µs/point).
//
// The run crosses a cache-window boundary (hourly), so B's cost includes
// re-announcing every series once per hour, plus the initial announce.
//
//	edgebench -services 8 -rounds 130 -pods-per-batch 40
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"syscall"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/seriesenc"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

func cpu() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

type result struct {
	Services, PodsPerBatch, Rounds, Warmup int
	Points                                 int
	PointsPerBatch                         float64
	AusPerPoint, BusPerPoint               float64
	AusPerPointBatches, BusPerPointBatches [3]float64 // min, median, max over 10-round windows
	ABytesPerPoint, BBytesPerPoint         map[string]float64
	BSeriesBytesPerPoint                   float64
	BSeriesRows                            int
	AAllocMBPerMPoint, BAllocMBPerMPoint   float64
}

var names = []string{"gauge", "sum", "histogram", "exponential_histogram", "summary"}

func main() {
	services := flag.Int("services", 8, "services (10 pods each)")
	rounds := flag.Int("rounds", 130, "rounds measured")
	warm := flag.Int("warmup", 3, "rounds not measured")
	ppb := flag.Int("pods-per-batch", 40, "pods per batch")
	flag.Parse()
	cfg := fleet.Default()
	cfg.Services = *services
	f := fleet.New(cfg)
	pg := parquetgo.NewPGEncoder(parquetgo.DefaultOptions())
	se := seriesenc.New()
	var abuf [parquetgo.NumMetricTypes]bytes.Buffer
	var bbuf seriesenc.Buffers
	res := result{Services: *services, PodsPerBatch: *ppb, Rounds: *rounds, Warmup: *warm,
		ABytesPerPoint: map[string]float64{}, BBytesPerPoint: map[string]float64{}}
	var aCPU, bCPU time.Duration
	var aAlloc, bAlloc uint64
	var aWin, bWin []float64
	var wa, wb time.Duration
	wpts := 0
	var typePts [5]int
	var ms runtime.MemStats
	for r := 0; r < *warm+*rounds; r++ {
		measured := r >= *warm
		for p0 := 0; p0 < f.Pods(); p0 += *ppb {
			md := f.Emit(p0, min(p0+*ppb, f.Pods()))
			n := md.DataPointCount()
			env := [parquetgo.NumMetricTypes]*parquetgo.Envelope{}
			for t := range env {
				env[t] = &parquetgo.Envelope{Producer: "edge", Epoch: "e", Batch: uint64(r), Received: 1, Schema: 1}
				abuf[t].Reset()
			}
			runtime.ReadMemStats(&ms)
			m0 := ms.TotalAlloc
			c0 := cpu()
			rows, err := pg.Metrics(func(t parquetgo.MetricType) io.Writer { return &abuf[t] }, md, &env)
			if err != nil {
				log.Fatal(err)
			}
			c1 := cpu()
			runtime.ReadMemStats(&ms)
			m1 := ms.TotalAlloc
			c2 := cpu()
			se.Encode(md, &seriesenc.Envelope{ProducerID: "edge", Epoch: "e", BatchID: uint64(r), ReceivedAtNs: 1, SchemaVersion: 1})
			if err := se.Flush(bbuf.Dst()); err != nil {
				log.Fatal(err)
			}
			se.Announced()
			c3 := cpu()
			runtime.ReadMemStats(&ms)
			if !measured {
				continue
			}
			aCPU += c1 - c0
			bCPU += c3 - c2
			wa += c1 - c0
			wb += c3 - c2
			aAlloc += m1 - m0
			bAlloc += ms.TotalAlloc - m1
			res.Points += n
			wpts += n
			for t := 0; t < 5; t++ {
				typePts[t] += rows[t]
				res.ABytesPerPoint[names[t]] += float64(abuf[t].Len())
				res.BBytesPerPoint[names[t]] += float64(bbuf[t].Len())
			}
			res.BSeriesBytesPerPoint += float64(bbuf[5].Len())
			res.BSeriesRows += len(se.New)
		}
		if measured && (r-*warm+1)%10 == 0 {
			aWin = append(aWin, float64(wa.Nanoseconds())/1e3/float64(wpts))
			bWin = append(bWin, float64(wb.Nanoseconds())/1e3/float64(wpts))
			wa, wb, wpts = 0, 0, 0
		}
	}
	var aTot, bTot float64
	for t := 0; t < 5; t++ {
		aTot += res.ABytesPerPoint[names[t]]
		bTot += res.BBytesPerPoint[names[t]]
		res.ABytesPerPoint[names[t]] /= float64(typePts[t])
		res.BBytesPerPoint[names[t]] /= float64(typePts[t])
	}
	bTot += res.BSeriesBytesPerPoint
	res.ABytesPerPoint["all"] = aTot / float64(res.Points)
	res.BBytesPerPoint["all (incl. series)"] = bTot / float64(res.Points)
	res.BSeriesBytesPerPoint /= float64(res.Points)
	res.PointsPerBatch = float64(res.Points) / float64(*rounds*((f.Pods()+*ppb-1) / *ppb))
	res.AusPerPoint = float64(aCPU.Nanoseconds()) / 1e3 / float64(res.Points)
	res.BusPerPoint = float64(bCPU.Nanoseconds()) / 1e3 / float64(res.Points)
	mmm := func(v []float64) [3]float64 {
		sort.Float64s(v)
		return [3]float64{v[0], v[len(v)/2], v[len(v)-1]}
	}
	res.AusPerPointBatches, res.BusPerPointBatches = mmm(aWin), mmm(bWin)
	res.AAllocMBPerMPoint = float64(aAlloc) / 1e6 / (float64(res.Points) / 1e6)
	res.BAllocMBPerMPoint = float64(bAlloc) / 1e6 / (float64(res.Points) / 1e6)
	_ = json.NewEncoder(os.Stdout).Encode(res)
}
