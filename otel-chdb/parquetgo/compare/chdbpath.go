// Package compare drives the chdb exporter's Parquet-only publishing path and
// the Go-native parquetgo path side by side, for correctness checks and
// benchmarks. It is its own module so parquetgo itself never links chdb-go.
package compare

import (
	"context"
	"fmt"
	"os"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// S3 is where the S3 variants write: a path-style endpoint with bucket.
type S3 struct{ Endpoint, Key, Secret string }

// S3FromEnv reads CHDB_TEST_S3, CHDB_TEST_S3_KEY and CHDB_TEST_S3_SECRET.
func S3FromEnv() (S3, bool) {
	s := S3{os.Getenv("CHDB_TEST_S3"), os.Getenv("CHDB_TEST_S3_KEY"), os.Getenv("CHDB_TEST_S3_SECRET")}
	return s, s.Endpoint != ""
}

// ChdbPublisher is the chdb exporter configured as a pure Parquet publisher
// (store_tables: false), for both signals, queue off: each Consume call is one
// synchronous batch.
type ChdbPublisher struct {
	Traces exporter.Traces
	Logs   exporter.Logs
}

// NewChdbPublisher starts the exporter. url is file:///dir or an S3 prefix.
func NewChdbPublisher(path, producer, epoch, url string, s3 S3) (*ChdbPublisher, error) {
	f := chdbexporter.NewFactory()
	cfg := f.CreateDefaultConfig().(*chdbexporter.Config)
	cfg.Path = path
	cfg.Database = "pq"
	cfg.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.StoreTables = false
	cfg.Producer.ID, cfg.Producer.Region, cfg.Producer.Epoch = producer, "cmp", epoch
	cfg.Parquet.URL = url
	cfg.Parquet.AccessKeyID, cfg.Parquet.SecretAccessKey = s3.Key, configopaque.String(s3.Secret)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	set := exportertest.NewNopSettings(component.MustNewType("chdb"))
	ctx := context.Background()
	t, err := f.CreateTraces(ctx, set, cfg)
	if err != nil {
		return nil, err
	}
	l, err := f.CreateLogs(ctx, set, cfg)
	if err != nil {
		return nil, err
	}
	for _, c := range []component.Component{t, l} {
		if err := c.Start(ctx, componenttest.NewNopHost()); err != nil {
			return nil, fmt.Errorf("start chdb exporter: %w", err)
		}
	}
	return &ChdbPublisher{Traces: t, Logs: l}, nil
}

func (p *ChdbPublisher) Shutdown() error {
	ctx := context.Background()
	return errorsJoin(p.Traces.Shutdown(ctx), p.Logs.Shutdown(ctx))
}

func errorsJoin(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
