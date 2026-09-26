// mgen writes the metrics pool the merge benchmark inserts from: the
// metrics-layout fleet (k8s-shaped, 500 series per pod) encoded as the edge
// encodes it, and PUT to SeaweedFS. Nothing is inserted here; the driver
// (../merges.py) reads these objects with INSERT … SELECT FROM s3(), as the
// consumer does, shifting time per cycle so the pool can be replayed.
//
// Layout B (seriesenc): one object per points type per edge batch, plus a
// series object when the batch announces new series. Layout A (parquetgo's
// contrib-schema encoder) with -a.
//
//	mgen -prefix merges/pool/small -rounds 24 -pods-per-batch 25
//	mgen -prefix merges/pool/big   -rounds 24 -pods-per-batch 200 -a
//	mgen -prefix merges/pool/big50 -rounds 50 -pods-per-batch 200 -only exponential_histogram,summary
//	mgen -clean merges/                      # remove the pools (and the TTL runs' S3 disk)
//
// Keys: {prefix}/{B|A}/{type}/{round:04d}-{batch:02d}.parquet.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/central"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/seriesenc"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

func main() {
	prefix := flag.String("prefix", "merges/pool/small", "S3 key prefix in the otel bucket")
	rounds := flag.Int("rounds", 24, "rounds (30 s each)")
	ppb := flag.Int("pods-per-batch", 25, "pods per edge batch")
	services := flag.Int("services", 20, "services (10 pods each; 20 = the README's 100k series)")
	doA := flag.Bool("a", false, "also write layout A (contrib schema) objects")
	only := flag.String("only", "", "comma-separated B types to write (default: all, series included)")
	clean := flag.String("clean", "", "delete every object under this key prefix (e.g. merges/) and exit")
	flag.Parse()
	ctx := context.Background()
	if *clean != "" {
		fmt.Println("deleted", central.DeletePrefix(ctx, central.S3(), *clean), "objects under", *clean)
		return
	}

	cfg := fleet.Default()
	cfg.Services = *services
	cfg.Rounds = *rounds
	cfg.RolloutEvery = 0 // the pool is replayed in cycles; keep the series set fixed
	f := fleet.New(cfg)
	s3 := central.S3()
	se := seriesenc.New()
	pg := parquetgo.NewPGEncoder(parquetgo.DefaultOptions())
	var bbuf seriesenc.Buffers
	var abuf [parquetgo.NumMetricTypes]bytes.Buffer
	names := append(append([]string{}, central.Types...), "series")
	var objs, bytesOut int
	var rowsByType [6]int
	put := func(layout string, t, r, b int, data []byte) {
		if *only != "" && layout == "B" && !strings.Contains(","+*only+",", ","+names[t]+",") {
			return
		}
		key := fmt.Sprintf("%s/%s/%s/%04d-%02d.parquet", *prefix, layout, names[t], r, b)
		if err := central.Put(ctx, s3, key, data); err != nil {
			log.Fatal(err)
		}
		objs++
		bytesOut += len(data)
	}
	t0 := time.Now()
	for r := 0; r < *rounds; r++ {
		b := 0
		for p0 := 0; p0 < f.Pods(); p0 += *ppb {
			p1 := min(p0+*ppb, f.Pods())
			md := f.Emit(p0, p1)
			recv := cfg.Start.Add(time.Duration(r+1) * cfg.Interval).UnixNano()
			env := &seriesenc.Envelope{ProducerID: "edge", Epoch: "pool", BatchID: uint64(r*1000 + b), ReceivedAtNs: recv, SchemaVersion: 1}
			rows := se.Encode(md, env)
			if err := se.Flush(bbuf.Dst()); err != nil {
				log.Fatal(err)
			}
			for t, n := range rows {
				if n > 0 {
					put("B", t, r, b, bytes.Clone(bbuf[t].Bytes()))
					rowsByType[t] += n
				}
			}
			if len(se.New) > 0 {
				put("B", 5, r, b, bytes.Clone(bbuf[5].Bytes()))
				rowsByType[5] += len(se.New)
			}
			se.Announced()
			if *doA {
				var envs [parquetgo.NumMetricTypes]*parquetgo.Envelope
				for t := range envs {
					envs[t] = &parquetgo.Envelope{Producer: "edge", Epoch: "pool", Batch: uint64(r*1000 + b), Received: uint64(recv), Schema: 1}
					abuf[t].Reset()
				}
				arows, err := pg.Metrics(func(t parquetgo.MetricType) io.Writer { return &abuf[t] }, md, &envs)
				if err != nil {
					log.Fatal(err)
				}
				for t, n := range arows {
					if n > 0 {
						put("A", t, r, b, bytes.Clone(abuf[t].Bytes()))
					}
				}
			}
			b++
		}
	}
	log.Printf("%s: %d rounds, %d pods, %d pods/batch: %d objects, %.1f MB, B rows by type %v, %.1fs",
		*prefix, *rounds, f.Pods(), *ppb, objs, float64(bytesOut)/1e6, rowsByType, time.Since(t0).Seconds())
}
