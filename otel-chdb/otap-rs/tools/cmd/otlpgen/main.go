// otlpgen writes the Go benchmarks' datasets as OTLP protobuf requests
// (ExportTraceServiceRequest / ExportLogsServiceRequest bytes), for the Rust
// pipeline's sender and in-process benchmark, and optionally publishes the
// same data through parquetgo as the reference for the correctness check.
//
//	otlpgen -out DIR                      # testgen 10k/3k and nasty 700, both signals
//	otlpgen -out DIR -ref http://127.0.0.1:18333/otel/otap-rs/ref -key otel -secret otelsecret -epoch R1
//	otlpgen -metrics -out DIR [-variants N] [-ref ...]   # metrics only (see metrics.go)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type dataset struct {
	name string
	td   ptrace.Traces
	ld   plog.Logs
}

func main() {
	out := flag.String("out", "", "directory for the .pb files")
	ref := flag.String("ref", "", "S3 prefix URL to publish the parquetgo reference to (optional)")
	key := flag.String("key", "otel", "")
	secret := flag.String("secret", "otelsecret", "")
	epoch := flag.String("epoch", "", "reference epoch (default: a timestamp)")
	variants := flag.Int("variants", 0, "also write N distinct 10k-row batches per signal (timestamps shifted by i s), bench-v{i}.pb")
	metrics := flag.Bool("metrics", false, "write (and with -ref publish) the metrics datasets instead")
	flag.Parse()
	if *metrics {
		if *out != "" {
			if err := os.MkdirAll(*out, 0o755); err != nil {
				log.Fatal(err)
			}
			writeMetrics(*out, *variants)
		}
		if *ref != "" {
			if *epoch == "" {
				*epoch = fmt.Sprintf("r%d", time.Now().Unix())
			}
			publishMetricsRef(*ref, *key, *secret, *epoch)
		}
		return
	}
	data := []dataset{
		{"testgen-10000", testgen.Traces(10000), testgen.Logs(10000)},
		{"testgen-3000", testgen.Traces(3000), testgen.Logs(3000)},
		{"nasty-700", compare.NastyTraces(700), compare.NastyLogs(700)},
	}
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			log.Fatal(err)
		}
		for _, d := range data {
			tb, err := ptraceotlp.NewExportRequestFromTraces(d.td).MarshalProto()
			if err != nil {
				log.Fatal(err)
			}
			lb, err := plogotlp.NewExportRequestFromLogs(d.ld).MarshalProto()
			if err != nil {
				log.Fatal(err)
			}
			for sig, b := range map[string][]byte{"traces": tb, "logs": lb} {
				p := filepath.Join(*out, fmt.Sprintf("%s-%s.pb", sig, d.name))
				if err := os.WriteFile(p, b, 0o644); err != nil {
					log.Fatal(err)
				}
				fmt.Printf("%s\t%d bytes\n", p, len(b))
			}
		}
	}
	for v := 0; v < *variants; v++ {
		td, ld := testgen.Traces(10000), testgen.Logs(10000)
		shift := pcommon.Timestamp(uint64(v) * 1e9)
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				sp := sss.At(j).Spans()
				for k := 0; k < sp.Len(); k++ {
					s := sp.At(k)
					s.SetStartTimestamp(s.StartTimestamp() + shift)
					s.SetEndTimestamp(s.EndTimestamp() + shift)
					for e := 0; e < s.Events().Len(); e++ {
						s.Events().At(e).SetTimestamp(s.Events().At(e).Timestamp() + shift)
					}
				}
			}
		}
		rls := ld.ResourceLogs()
		for i := 0; i < rls.Len(); i++ {
			sls := rls.At(i).ScopeLogs()
			for j := 0; j < sls.Len(); j++ {
				rs := sls.At(j).LogRecords()
				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)
					r.SetTimestamp(r.Timestamp() + shift)
					r.SetObservedTimestamp(r.ObservedTimestamp() + shift)
				}
			}
		}
		tb, _ := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
		lb, _ := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
		for sig, b := range map[string][]byte{"traces": tb, "logs": lb} {
			if err := os.WriteFile(filepath.Join(*out, fmt.Sprintf("%s-bench-v%02d.pb", sig, v)), b, 0o644); err != nil {
				log.Fatal(err)
			}
		}
	}
	if *ref != "" {
		if *epoch == "" {
			*epoch = fmt.Sprintf("r%d", time.Now().Unix())
		}
		// One publisher per dataset, so each dataset is its own producer
		// prefix: {ref}/cmp/{signal}/v1/{dataset}/{epoch}/g*/{batch}.parquet
		for _, d := range data[1:] {
			p, err := parquetgo.New(parquetgo.Config{URL: *ref, AccessKeyID: *key, SecretAccessKey: *secret,
				ProducerID: d.name, Region: "cmp", SchemaVersion: 1, Epoch: *epoch,
				Parquet: parquetgo.DefaultOptions(), Engine: "parquet-go"})
			if err != nil {
				log.Fatal(err)
			}
			ctx := context.Background()
			if err := p.PushTraces(ctx, d.td); err != nil {
				log.Fatal(err)
			}
			if err := p.PushLogs(ctx, d.ld); err != nil {
				log.Fatal(err)
			}
			if err := p.Close(ctx); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("reference %s: %s/cmp/{traces,logs}/v1/%s/%s/\n", d.name, *ref, d.name, *epoch)
		}
	}
}
