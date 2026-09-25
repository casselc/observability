// seriesref is the Go reference for the Rust series-table metrics encoder
// (src/series.rs): it runs ../../metrics-layout/seriesenc, the spike's
// prototype, over OTLP metrics requests and writes its objects, so the Rust
// test (tests/series.rs) can compare rows, Parquet schemas and series ids.
//
//	seriesref -out DIR a.pb b.pb ...   one encoder (one epoch) over the files in order:
//	                                   DIR/NNNN.<type>.parquet and DIR/NNNN.series.parquet,
//	                                   Announced() after each request, as if every
//	                                   series object committed
//	seriesref -rm otap-rs-edge/        deletes every object under otel/<prefix> on the local
//	                                   SeaweedFS (cleanup after a run)
//	seriesref -fleet DIR -services 2 -rounds 130 -pods-per-batch 20
//	                                   writes the spike's fleet batches as OTLP requests
//	                                   (DIR/fleet-NNNN.pb): realistic series reuse
//
// Every object's envelope is producer "p", epoch "e", batch_id = the file's
// index, received_at 1700000000000000000, schema_version 1.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/central"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/seriesenc"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func main() {
	out := flag.String("out", "", "encode the files into DIR")
	fleetDir := flag.String("fleet", "", "write fleet batches into DIR")
	services := flag.Int("services", 2, "fleet services (10 pods each)")
	rounds := flag.Int("rounds", 130, "fleet rounds (30 s each)")
	ppb := flag.Int("pods-per-batch", 20, "fleet pods per batch")
	window := flag.Int64("window", 3600, "series cache window, seconds")
	rm := flag.String("rm", "", "delete every object under otel/PREFIX")
	flag.Parse()
	switch {
	case *rm != "":
		if len(*rm) < 4 {
			log.Fatal("refusing a short prefix")
		}
		fmt.Println("deleted", central.DeletePrefix(context.Background(), central.S3(), *rm), "objects under otel/"+*rm)
	case *fleetDir != "":
		cfg := fleet.Default()
		cfg.Services = *services
		f := fleet.New(cfg)
		if err := os.MkdirAll(*fleetDir, 0o755); err != nil {
			log.Fatal(err)
		}
		n := 0
		for r := 0; r < *rounds; r++ {
			for p0 := 0; p0 < f.Pods(); p0 += *ppb {
				md := f.Emit(p0, min(p0+*ppb, f.Pods()))
				b, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
				if err != nil {
					log.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(*fleetDir, fmt.Sprintf("fleet-%04d.pb", n)), b, 0o644); err != nil {
					log.Fatal(err)
				}
				n++
			}
		}
		fmt.Printf("%d batches\n", n)
	case *out != "":
		if err := os.MkdirAll(*out, 0o755); err != nil {
			log.Fatal(err)
		}
		e := seriesenc.New()
		e.WindowSeconds = *window
		var bufs seriesenc.Buffers
		names := append(seriesenc.TypeNames[:], "series")
		for i, f := range flag.Args() {
			b, err := os.ReadFile(f)
			if err != nil {
				log.Fatal(err)
			}
			md, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(b)
			if err != nil {
				log.Fatalf("%s: %v", f, err)
			}
			env := &seriesenc.Envelope{ProducerID: "p", Epoch: "e", BatchID: uint64(i), ReceivedAtNs: 1700000000000000000, SchemaVersion: 1}
			rows := e.Encode(md, env)
			if err := e.Flush(bufs.Dst()); err != nil {
				log.Fatal(err)
			}
			for t := range bufs {
				if bufs[t].Len() == 0 {
					continue
				}
				p := filepath.Join(*out, fmt.Sprintf("%04d.%s.parquet", i, names[t]))
				if err := os.WriteFile(p, bytes.Clone(bufs[t].Bytes()), 0o644); err != nil {
					log.Fatal(err)
				}
			}
			fmt.Printf("%s\trows %v\tnew series %d\n", f, rows, len(e.New))
			e.Announced()
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}
