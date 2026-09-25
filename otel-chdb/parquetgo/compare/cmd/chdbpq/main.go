// chdbpq publishes testgen batches through the chdb exporter's Parquet-only
// path, for inspecting what ClickHouse's Parquet writer produces.
//
//	chdbpq -url file:///tmp/out -spans 10000 -logs 10000
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare"
)

func main() {
	url := flag.String("url", "", "file:///dir or S3 prefix")
	spans := flag.Int("spans", 10000, "spans per traces batch")
	logs := flag.Int("logs", 10000, "records per logs batch")
	batches := flag.Int("batches", 1, "batches of each")
	epoch := flag.String("epoch", "e1", "producer epoch")
	flag.Parse()
	s3, _ := compare.S3FromEnv()
	dir, _ := os.MkdirTemp("", "chdbpq")
	defer os.RemoveAll(dir)
	p, err := compare.NewChdbPublisher(dir, "chdb", *epoch, *url, s3)
	if err != nil {
		log.Fatal(err)
	}
	td, ld := testgen.Traces(*spans), testgen.Logs(*logs)
	for i := 0; i < *batches; i++ {
		if err := p.Traces.ConsumeTraces(context.Background(), td); err != nil {
			log.Fatal(err)
		}
		if err := p.Logs.ConsumeLogs(context.Background(), ld); err != nil {
			log.Fatal(err)
		}
	}
	if err := p.Shutdown(); err != nil {
		log.Fatal(err)
	}
}
