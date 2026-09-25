// otapsend is an OTAP (OTel Arrow) sender: it reads OTLP requests (.pb
// files), turns each into a BatchArrowRecords with otel-arrow's Go producer
// (the encoder the collector's otelarrowexporter uses), and sends them on one
// gRPC Arrow stream per signal (ArrowTracesService / ArrowLogsService /
// ArrowMetricsService), one batch at a time, waiting for each BatchStatus.
// The producer is kept for the whole stream, as the exporter's is, so later
// batches carry dictionary deltas against earlier ones.
//
//	otapsend -addr 127.0.0.1:14419 -signal traces -file a.pb,b.pb -n 30 [-quiet]
//
// Output: one JSON line per batch (seq, file, status, ack_ms) unless -quiet,
// then a summary line with the median / min / max ack latency and the
// encode time. The exit status is 1 if any batch was not OK.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stream interface {
	Send(*arrowpb.BatchArrowRecords) error
	Recv() (*arrowpb.BatchStatus, error)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:4317", "OTAP gRPC address")
	signal := flag.String("signal", "traces", "traces, logs or metrics")
	files := flag.String("file", "", "comma-separated OTLP .pb files, sent round-robin")
	n := flag.Int("n", 1, "batches to send")
	quiet := flag.Bool("quiet", false, "only the summary")
	warmup := flag.Int("warmup", 0, "after this many batches print {\"warm\":true}, pause, and restart the statistics")
	pause := flag.Duration("pause", time.Second, "the pause after the warm-up")
	flag.Parse()
	var bodies [][]byte
	names := strings.Split(*files, ",")
	for _, f := range names {
		b, err := os.ReadFile(f)
		if err != nil {
			log.Fatal(err)
		}
		bodies = append(bodies, b)
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	var st stream
	switch *signal {
	case "traces":
		st, err = arrowpb.NewArrowTracesServiceClient(conn).ArrowTraces(ctx)
	case "logs":
		st, err = arrowpb.NewArrowLogsServiceClient(conn).ArrowLogs(ctx)
	case "metrics":
		st, err = arrowpb.NewArrowMetricsServiceClient(conn).ArrowMetrics(ctx)
	default:
		log.Fatalf("unknown -signal %s", *signal)
	}
	if err != nil {
		log.Fatal(err)
	}
	p := arrow_record.NewProducer()
	defer p.Close()
	var lat []float64
	var encode time.Duration
	failed := 0
	start := time.Now()
	for i := 0; i < *n; i++ {
		if *warmup > 0 && i == *warmup {
			fmt.Println(`{"warm":true}`)
			time.Sleep(*pause)
			lat, encode, start = nil, 0, time.Now()
		}
		b := bodies[i%len(bodies)]
		t0 := time.Now()
		var bar *arrowpb.BatchArrowRecords
		switch *signal {
		case "traces":
			td, e := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b)
			if e != nil {
				log.Fatal(e)
			}
			bar, err = p.BatchArrowRecordsFromTraces(td)
		case "logs":
			ld, e := (&plog.ProtoUnmarshaler{}).UnmarshalLogs(b)
			if e != nil {
				log.Fatal(e)
			}
			bar, err = p.BatchArrowRecordsFromLogs(ld)
		case "metrics":
			md, e := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(b)
			if e != nil {
				log.Fatal(e)
			}
			bar, err = p.BatchArrowRecordsFromMetrics(md)
		}
		if err != nil {
			log.Fatalf("batch %d (%s): encode: %v", i, names[i%len(names)], err)
		}
		t1 := time.Now()
		encode += t1.Sub(t0)
		if err := st.Send(bar); err != nil {
			log.Fatalf("batch %d: send: %v", i, err)
		}
		s, err := st.Recv()
		if err != nil {
			log.Fatalf("batch %d: recv: %v", i, err)
		}
		ms := float64(time.Since(t1).Microseconds()) / 1000
		lat = append(lat, ms)
		if s.GetStatusCode() != arrowpb.StatusCode_OK {
			failed++
		}
		if !*quiet {
			j, _ := json.Marshal(map[string]any{"seq": i, "file": names[i%len(names)], "batch_id": s.GetBatchId(),
				"status": s.GetStatusCode().String(), "message": s.GetStatusMessage(), "ack_ms": ms})
			fmt.Println(string(j))
		}
	}
	_ = st.(grpc.ClientStream).CloseSend()
	sort.Float64s(lat)
	timed := *n - *warmup
	j, _ := json.Marshal(map[string]any{"summary": true, "batches": timed, "failed": failed,
		"median_ack_ms": lat[len(lat)/2], "min_ack_ms": lat[0], "max_ack_ms": lat[len(lat)-1],
		"encode_ms_per_batch": float64(encode.Microseconds()) / 1000 / float64(timed), "elapsed_s": time.Since(start).Seconds()})
	fmt.Println(string(j))
	if failed > 0 {
		os.Exit(1)
	}
}
