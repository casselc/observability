// load runs the whole pipeline for layouts A and B on the fleet dataset:
// generate a batch, encode it with parquetgo (A: contrib schema, one object
// per type) and with seriesenc (B: narrow points objects plus a series
// object), PUT every object to SeaweedFS, and have the ClickHouse server
// ingest each object with the importer's INSERT…SELECT FROM s3() (single-
// block settings, a dedup token, a query_id per object). Objects are deleted
// once ingested, so disk holds only the tables.
//
// Per-insert server CPU is read back from system.query_log (ProfileEvents
// OSCPUVirtualTimeMicroseconds and User+SystemTimeMicroseconds of each
// INSERT), which excludes merges and other people's queries. Results are
// JSON lines on stdout: one per (layout, type) plus a run summary.
//
//	load -db ml_main -rounds 720 -pods-per-batch 0 -workers 2
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/central"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/seriesenc"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"github.com/minio/minio-go/v7"
)

type obj struct {
	layout string // A or B
	typ    int    // 0..4, 5 = B series
	key    string
	data   []byte
	rows   int
	size   int
}

type job struct {
	batch int
	objs  []obj
}

type stat struct {
	Layout, Type      string
	Objects, Rows     int
	ParquetBytes      int64
	ServerCPUus       float64 // OSCPUVirtualTimeMicroseconds, summed over inserts
	ServerUserSysus   float64
	WallMs            float64
	CPUusPerPoint     float64
	CPUmsPerObject    float64
	ParquetBytesPerPt float64
	RowsPerObject     float64
}

func cpu() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func main() {
	db := flag.String("db", "ml_main", "database")
	create := flag.Bool("create", true, "create the database and tables")
	layouts := flag.String("layouts", "A,B", "layouts to load")
	rounds := flag.Int("rounds", 720, "rounds (30 s each)")
	ppb := flag.Int("pods-per-batch", 0, "pods per edge batch (0: whole round)")
	workers := flag.Int("workers", 2, "concurrent insert workers")
	run := flag.String("run", fmt.Sprintf("r%d", time.Now().Unix()), "run id (query_id and S3 prefix)")
	valueCodec := flag.String("value-codec", central.DefaultCodecs.Value, "B Value/Sum codec")
	bucketCodec := flag.String("bucket-codec", central.DefaultCodecs.Bucket, "B bucket array codec")
	services := flag.Int("services", 0, "override fleet services (0: default)")
	keep := flag.Bool("keep", false, "keep the objects in S3 (for cmd/fixedcost)")
	flag.Parse()
	ctx := context.Background()
	doA, doB := strings.Contains(*layouts, "A"), strings.Contains(*layouts, "B")
	// Bc: B tables fed centrally from the A objects (the edge unchanged).
	doBc := strings.Contains(*layouts, "Bc")
	if doBc {
		doB = strings.Contains(strings.ReplaceAll(*layouts, "Bc", ""), "B")
	}

	if *create {
		central.MustQ("CREATE DATABASE IF NOT EXISTS " + *db)
		for t := range central.Types {
			if doA {
				central.MustQ(central.ADDL(*db, t))
			}
		}
		if doB || doBc {
			for _, s := range central.BDDL(*db, central.Codecs{Value: *valueCodec, Bucket: *bucketCodec}) {
				central.MustQ(s)
			}
		}
		if doBc {
			for _, s := range central.BcDDL(*db) {
				central.MustQ(s)
			}
		}
		// Disk on this box is tight: drop merged-away parts after 30 s
		// instead of 8 min. Storage only; no effect on what is measured.
		for _, t := range strings.Fields(central.MustQ("SELECT name FROM system.tables WHERE database = '" + *db + "' AND engine LIKE '%MergeTree' FORMAT TSV")) {
			central.MustQ("ALTER TABLE " + *db + "." + t + " MODIFY SETTING old_parts_lifetime = 30")
		}
	}
	cfg := fleet.Default()
	if *services > 0 {
		cfg.Services = *services
	}
	cfg.Rounds = *rounds
	f := fleet.New(cfg)
	if *ppb <= 0 {
		*ppb = f.Pods()
	}
	s3 := central.S3()
	prefix := central.Prefix + "/load/" + *run + "/"
	log.Printf("run %s: db %s, %d pods, %d rounds, %d pods per batch, objects under %s", *run, *db, f.Pods(), *rounds, *ppb, prefix)

	jobs := make(chan job, 3)
	var edgeCPU [2]time.Duration
	var genCPU time.Duration
	// Producer: generate and encode.
	go func() {
		defer close(jobs)
		pg := parquetgo.NewPGEncoder(parquetgo.DefaultOptions())
		se := seriesenc.New()
		var abuf [parquetgo.NumMetricTypes]bytes.Buffer
		var bbuf seriesenc.Buffers
		batch := 0
		for r := 0; r < *rounds; r++ {
			for p0 := 0; p0 < f.Pods(); p0 += *ppb {
				p1 := min(p0+*ppb, f.Pods())
				c0 := cpu()
				md := f.Emit(p0, p1)
				c1 := cpu()
				genCPU += c1 - c0
				recv := uint64(fleet.Default().Start.Add(time.Duration(r+1) * cfg.Interval).UnixNano())
				j := job{batch: batch}
				if doA {
					var envs [parquetgo.NumMetricTypes]*parquetgo.Envelope
					for t := range envs {
						envs[t] = &parquetgo.Envelope{Producer: "edge", Epoch: *run, Batch: uint64(batch), Received: recv, Schema: 1}
						abuf[t].Reset()
					}
					rows, err := pg.Metrics(func(t parquetgo.MetricType) io.Writer { return &abuf[t] }, md, &envs)
					if err != nil {
						log.Fatal(err)
					}
					for t, n := range rows {
						if n > 0 {
							j.objs = append(j.objs, obj{"A", t, fmt.Sprintf("%sA/%s/%08d.parquet", prefix, central.Types[t], batch), bytes.Clone(abuf[t].Bytes()), n, 0})
						}
					}
				}
				c2 := cpu()
				edgeCPU[0] += c2 - c1
				if doB {
					env := &seriesenc.Envelope{ProducerID: "edge", Epoch: *run, BatchID: uint64(batch), ReceivedAtNs: int64(recv), SchemaVersion: 1}
					rows := se.Encode(md, env)
					if err := se.Flush(bbuf.Dst()); err != nil {
						log.Fatal(err)
					}
					if len(se.New) > 0 {
						j.objs = append(j.objs, obj{"B", 5, fmt.Sprintf("%sB/series/%08d.parquet", prefix, batch), bytes.Clone(bbuf[5].Bytes()), len(se.New), 0})
					}
					for t, n := range rows {
						if n > 0 {
							j.objs = append(j.objs, obj{"B", t, fmt.Sprintf("%sB/%s/%08d.parquet", prefix, central.Types[t], batch), bytes.Clone(bbuf[t].Bytes()), n, 0})
						}
					}
					se.Announced() // the loader commits series before points
				}
				edgeCPU[1] += cpu() - c2
				jobs <- j
				batch++
			}
			if r%20 == 0 {
				log.Printf("round %d/%d generated", r, *rounds)
			}
		}
	}()

	type rec struct {
		o    obj
		cost central.Cost
	}
	var mu sync.Mutex
	var recs []rec
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := central.Dial(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			for j := range jobs {
				for i := range j.objs {
					j.objs[i].size = len(j.objs[i].data)
				}
				for _, o := range j.objs {
					if err := central.Put(ctx, s3, o.key, o.data); err != nil {
						log.Fatal(err)
					}
				}
				for _, o := range j.objs {
					var sql string
					switch {
					case o.layout == "A" && doBc:
						sql = central.BcInsert(*db, o.typ, central.ObjURL(o.key))
						o.layout = "Bc"
					case o.layout == "A":
						sql = central.AInsert(*db, o.typ, central.ObjURL(o.key))
					default:
						sql = central.BInsert(*db, o.typ, central.ObjURL(o.key))
					}
					sql = strings.Replace(sql, ") SELECT ", ") SETTINGS "+central.OneBlock+", insert_deduplication_token = '"+o.key+"' SELECT ", 1)
					var cost central.Cost
					for attempt := 0; ; attempt++ {
						cost, err = conn.Exec(ctx, sql, fmt.Sprintf("ml-%s-%s-%d-%d-%d", *run, o.layout, o.typ, j.batch, attempt), nil)
						if err == nil {
							break
						}
						if attempt == 2 {
							log.Fatalf("insert %s: %v", o.key, err)
						}
						log.Printf("insert %s: %v (retrying)", o.key, err)
						conn.Close()
						time.Sleep(2 * time.Second)
						if conn, err = central.Dial(ctx); err != nil {
							log.Fatal(err)
						}
					}
					o.data = nil
					mu.Lock()
					recs = append(recs, rec{o, cost})
					mu.Unlock()
				}
				for _, o := range j.objs {
					if !*keep {
						_ = s3.RemoveObject(ctx, central.Bucket, o.key, minio.RemoveObjectOptions{})
					}
				}
			}
		}()
	}
	wg.Wait()
	log.Printf("ingest done")
	stats := map[string]*stat{}
	for _, r := range recs {
		tn := "series"
		if r.o.typ < 5 {
			tn = central.Types[r.o.typ]
		}
		k := r.o.layout + "/" + tn
		s := stats[k]
		if s == nil {
			s = &stat{Layout: r.o.layout, Type: tn}
			stats[k] = s
		}
		s.Objects++
		s.Rows += r.o.rows
		s.ParquetBytes += int64(r.o.size)
		s.ServerCPUus += r.cost.CPUus
		s.ServerUserSysus += r.cost.UserSysus
		s.WallMs += float64(r.cost.Wall.Microseconds()) / 1e3
	}
	_ = s3
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	enc := json.NewEncoder(os.Stdout)
	for _, k := range keys {
		s := stats[k]
		s.CPUusPerPoint = s.ServerCPUus / float64(s.Rows)
		s.CPUmsPerObject = s.ServerCPUus / 1e3 / float64(s.Objects)
		s.RowsPerObject = float64(s.Rows) / float64(s.Objects)
		s.ParquetBytesPerPt = float64(s.ParquetBytes) / float64(s.Rows)
		_ = enc.Encode(s)
	}
	_ = enc.Encode(map[string]any{"run": *run, "db": *db, "rounds": *rounds, "podsPerBatch": *ppb,
		"genCPUs": genCPU.Seconds(), "edgeCPUsA": edgeCPU[0].Seconds(), "edgeCPUsB": edgeCPU[1].Seconds()})
}
