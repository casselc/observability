// metricsref writes the reference rows for the metrics correctness check:
// it feeds OTLP metrics requests (.pb files, ExportMetricsServiceRequest) to
// the contrib clickhouseexporter v0.161.0, built through its factory with
// create_schema on and the queue and retries off, so each ConsumeMetrics
// returns once the rows are in ClickHouse. The tables are the exporter's own
// otel_metrics_* in database -db.
//
//	metricsref -db otaprs_mref -native 127.0.0.1:19000 metrics-testgen-3000.pb metrics-nasty-700.pb ...
//
// With -db-per-file, each file goes to its own database (-db + "_" + index).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func newExporter(ctx context.Context, native, db string) exporter.Metrics {
	f := clickhouseexporter.NewFactory()
	cfg := f.CreateDefaultConfig().(*clickhouseexporter.Config)
	cfg.Endpoint = "tcp://" + native + "?dial_timeout=10s"
	cfg.Database = db
	cfg.CreateSchema = true
	cfg.AsyncInsert = false
	cfg.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.BackOffConfig.Enabled = false
	cfg.TimeoutSettings.Timeout = 5 * time.Minute
	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}
	e, err := f.CreateMetrics(ctx, exportertest.NewNopSettings(component.MustNewType("clickhouse")), cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := e.Start(ctx, componenttest.NewNopHost()); err != nil {
		log.Fatal(err)
	}
	return e
}

func main() {
	db := flag.String("db", "otaprs_mref", "database for the exporter's tables")
	native := flag.String("native", "127.0.0.1:19000", "ClickHouse native-protocol address")
	perFile := flag.Bool("db-per-file", false, "one database per file: DB_0, DB_1, ...")
	flag.Parse()
	ctx := context.Background()
	var e exporter.Metrics
	for i, f := range flag.Args() {
		b, err := os.ReadFile(f)
		if err != nil {
			log.Fatal(err)
		}
		md, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(b)
		if err != nil {
			log.Fatalf("%s: %v", f, err)
		}
		if e == nil || *perFile {
			if e != nil {
				_ = e.Shutdown(ctx)
			}
			d := *db
			if *perFile {
				d = fmt.Sprintf("%s_%d", *db, i)
			}
			e = newExporter(ctx, *native, d)
		}
		t0 := time.Now()
		if err := e.ConsumeMetrics(ctx, md); err != nil {
			fmt.Printf("%s: rejected: %v\n", f, err)
			continue
		}
		fmt.Printf("%s: %d points in %v\n", f, md.DataPointCount(), time.Since(t0))
	}
	if e != nil {
		if err := e.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}
}
