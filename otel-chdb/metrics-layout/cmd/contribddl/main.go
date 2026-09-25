// contribddl has the contrib clickhouseexporter v0.161.0 create its own
// otel_metrics_* tables (create_schema) in a scratch database on the server,
// and writes each table's SHOW CREATE TABLE to sql/contrib/<table>.sql. The
// A layout is exactly these tables plus the envelope (see ../../README.md).
// Same construction as ../../parquetgo/compare/refexporter.go.
package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func q(sql string) string {
	resp, err := http.Post("http://127.0.0.1:18123/", "text/plain", strings.NewReader(sql))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		log.Fatalf("%s: %s", sql, b)
	}
	return string(b)
}

func main() {
	ctx := context.Background()
	db := "ml_contribddl"
	f := clickhouseexporter.NewFactory()
	cfg := f.CreateDefaultConfig().(*clickhouseexporter.Config)
	cfg.Endpoint = "tcp://127.0.0.1:19000?dial_timeout=10s"
	cfg.Database = db
	cfg.CreateSchema = true
	cfg.AsyncInsert = false
	cfg.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.BackOffConfig.Enabled = false
	cfg.TimeoutSettings.Timeout = time.Minute
	e, err := f.CreateMetrics(ctx, exportertest.NewNopSettings(component.MustNewType("clickhouse")), cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := e.Start(ctx, componenttest.NewNopHost()); err != nil {
		log.Fatal(err)
	}
	_ = e.Shutdown(ctx)
	out := os.Args[1]
	for _, t := range []string{"gauge", "sum", "histogram", "exponential_histogram", "summary"} {
		ddl := q("SHOW CREATE TABLE " + db + ".otel_metrics_" + t + " FORMAT TSVRaw")
		ddl = strings.Replace(ddl, "CREATE TABLE "+db+".", "CREATE TABLE {db}.", 1)
		if err := os.WriteFile(filepath.Join(out, "otel_metrics_"+t+".sql"), []byte(ddl), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	q("DROP DATABASE " + db)
}
