package compare

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// NativeAddr is the ClickHouse server's native-protocol address, which the
// contrib exporter (clickhouse-go) talks to: CHDB_TEST_CLICKHOUSE_NATIVE, or
// CHDB_TEST_CLICKHOUSE's host on port 19000.
func NativeAddr() string {
	if a := os.Getenv("CHDB_TEST_CLICKHOUSE_NATIVE"); a != "" {
		return a
	}
	u, _ := url.Parse(os.Getenv("CHDB_TEST_CLICKHOUSE"))
	return u.Hostname() + ":19000"
}

// NewRefExporter starts the contrib clickhouseexporter v0.161.0 as the
// metrics reference, built through its factory: create_schema on (so it
// creates database db and its otel_metrics_* tables with its own DDL),
// synchronous inserts, queue and retries off, so ConsumeMetrics returns once
// the rows are in. Shut it down when done.
func NewRefExporter(ctx context.Context, db string) (exporter.Metrics, error) {
	f := clickhouseexporter.NewFactory()
	cfg := f.CreateDefaultConfig().(*clickhouseexporter.Config)
	cfg.Endpoint = "tcp://" + NativeAddr() + "?dial_timeout=10s"
	cfg.Database = db
	cfg.CreateSchema = true
	cfg.AsyncInsert = false
	cfg.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.BackOffConfig.Enabled = false
	cfg.TimeoutSettings.Timeout = 5 * time.Minute
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e, err := f.CreateMetrics(ctx, exportertest.NewNopSettings(component.MustNewType("clickhouse")), cfg)
	if err != nil {
		return nil, err
	}
	if err := e.Start(ctx, componenttest.NewNopHost()); err != nil {
		return nil, fmt.Errorf("start clickhouseexporter: %w", err)
	}
	return e, nil
}
