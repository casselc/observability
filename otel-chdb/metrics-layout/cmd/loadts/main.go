// loadts is layout C: ClickHouse's TimeSeries table engine (experimental in
// 26.10: allow_experimental_time_series_table). The fleet's points are
// converted to Prometheus samples (promconv, classic histograms), written at
// the edge as one Parquet object per batch with one row per sample
// (metric_name, tag keys, tag values, timestamp, value), PUT to SeaweedFS
// and ingested with
//
//	INSERT INTO ts (metric_name, tags, samples)
//	SELECT metric_name, mapFromArrays(k, v), [(timestamp, value)] FROM s3(...)
//
// over the native protocol, so each insert's server CPU comes from its
// ProfileEvents. Prints one JSON line with the per-sample and per-point
// costs; stored bytes are read from the inner tables afterwards (README).
//
//	loadts -db ml_ts -services 4 -rounds 720
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/central"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/promconv"
	"github.com/minio/minio-go/v7"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// SampleRow is one Prometheus sample as the C edge object carries it.
type SampleRow struct {
	MetricName string   `parquet:"metric_name,dict"`
	Keys       []string `parquet:"k,list"`
	Vals       []string `parquet:"v,list"`
	TimeMs     int64    `parquet:"timestamp,timestamp(millisecond)"`
	Value      float64  `parquet:"value"`
}

type sink struct{ rows []SampleRow }

func (s *sink) Float(l []promconv.Label, t int64, v float64) {
	r := SampleRow{TimeMs: t, Value: v, Keys: make([]string, 0, len(l)-1), Vals: make([]string, 0, len(l)-1)}
	for _, x := range l {
		if x.Name == "__name__" {
			r.MetricName = x.Value
			continue
		}
		r.Keys = append(r.Keys, x.Name)
		r.Vals = append(r.Vals, x.Value)
	}
	s.rows = append(s.rows, r)
}

func (s *sink) Hist([]promconv.Label, int64, *promconv.Hist) { panic("classic only") }

func cpu() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func main() {
	db := flag.String("db", "ml_ts", "database")
	services := flag.Int("services", 4, "services (10 pods each)")
	rounds := flag.Int("rounds", 720, "rounds")
	run := flag.String("run", fmt.Sprintf("ts%d", time.Now().Unix()), "run id")
	flag.Parse()
	ctx := context.Background()
	central.MustQ("CREATE DATABASE IF NOT EXISTS " + *db)
	central.MustQ("CREATE TABLE "+*db+".ts ENGINE = TimeSeries", "allow_experimental_time_series_table", "1")
	cfg := fleet.Default()
	cfg.Services = *services
	f := fleet.New(cfg)
	conv := promconv.New(false)
	s3 := central.S3()
	conn, err := central.Dial(ctx)
	if err != nil {
		log.Fatal(err)
	}
	w := parquet.NewGenericWriter[SampleRow](&bytes.Buffer{}, parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}))
	var buf bytes.Buffer
	var edge time.Duration
	var serverCPU float64
	var wall time.Duration
	points, samples, objBytes := 0, 0, 0
	structure := "metric_name String, k Array(String), v Array(String), timestamp DateTime64(3), value Float64"
	for r := 0; r < *rounds; r++ {
		md := f.EmitRound()
		points += md.DataPointCount()
		c0 := cpu()
		sk := &sink{}
		conv.Walk(md, sk)
		buf.Reset()
		w.Reset(&buf)
		if _, err := w.Write(sk.rows); err != nil {
			log.Fatal(err)
		}
		if err := w.Close(); err != nil {
			log.Fatal(err)
		}
		edge += cpu() - c0
		samples += len(sk.rows)
		objBytes += buf.Len()
		key := fmt.Sprintf("%s/ts/%s/%06d.parquet", central.Prefix, *run, r)
		if err := central.Put(ctx, s3, key, buf.Bytes()); err != nil {
			log.Fatal(err)
		}
		sql := fmt.Sprintf("INSERT INTO %s.ts (metric_name, tags, samples) SETTINGS %s, insert_deduplication_token = '%s' "+
			"SELECT metric_name, mapFromArrays(k, v), [(timestamp, value)] FROM s3('%s', '%s', '%s', 'Parquet', '%s')",
			*db, central.OneBlock, key, central.ObjURL(key), central.S3Key, central.S3Secret, structure)
		cost, err := conn.Exec(ctx, sql, fmt.Sprintf("ml-%s-%d", *run, r), nil)
		if err != nil {
			log.Fatal(err)
		}
		serverCPU += cost.CPUus
		wall += cost.Wall
		_ = s3.RemoveObject(ctx, central.Bucket, key, minio.RemoveObjectOptions{})
		if r%120 == 0 {
			log.Printf("round %d", r)
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"layout": "C", "db": *db, "services": *services, "rounds": *rounds, "otelPoints": points, "samples": samples,
		"samplesPerPoint":      float64(samples) / float64(points),
		"serverCPUusPerSample": serverCPU / float64(samples),
		"serverCPUusPerPoint":  serverCPU / float64(points),
		"insertWallMsPerRound": float64(wall.Milliseconds()) / float64(*rounds),
		"edgeCPUusPerPoint":    float64(edge.Nanoseconds()) / 1e3 / float64(points),
		"parquetBytesPerPoint": float64(objBytes) / float64(points),
	})
}
