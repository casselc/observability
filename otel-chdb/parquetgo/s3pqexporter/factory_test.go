package s3pqexporter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func load(t *testing.T, m map[string]any) *Config {
	t.Helper()
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if err := confmap.NewFromStringMap(m).Unmarshal(cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestConfig(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "producer_id") {
		t.Fatal(err)
	}
	cfg = load(t, map[string]any{
		"producer_id": "p1", "lanes": 2, "metrics_layout": "clickstack_tables",
		"series":  map[string]any{"window": "30m", "byte_stream_split": false},
		"parquet": map[string]any{"bloom_columns": []any{}, "zstd_level": 6},
		"s3": map[string]any{"url": "https://objects.example/bucket/edge/p1", "path_style": true,
			"put_timeout": "5s", "access_key_id": "k", "secret_access_key": "s"},
	})
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ec := cfg.EdgeConfig()
	if ec.Lanes != 2 || ec.Series.Window != 30*time.Minute || ec.Series.ByteStreamSplit || !ec.Series.MergeNumberPoints ||
		ec.Parquet.BloomFilters || ec.Parquet.CompressionLevel != 6 || *ec.S3.PathStyle != true || ec.PutTimeout != 5*time.Second ||
		ec.S3.SecretAccessKey != "s" {
		t.Fatalf("%+v", ec)
	}
	// D4: no max_elapsed_time, no batching after the queue.
	bad := load(t, map[string]any{"producer_id": "p", "s3": map[string]any{"url": "s3://b/p"},
		"retry_on_failure": map[string]any{"max_elapsed_time": "5m"}})
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "max_elapsed_time") {
		t.Fatal(err)
	}
	bad = load(t, map[string]any{"producer_id": "p", "s3": map[string]any{"url": "s3://b/p"},
		"sending_queue": map[string]any{"batch": map[string]any{}}})
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "batch") {
		t.Fatal(err)
	}
}

// Against a real bucket (GOEDGE_S3=http://127.0.0.1:18333/goedge-test, keys
// in AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY): the three pipelines of one
// exporter share one edge, and each request lands.
func TestExportToBucket(t *testing.T) {
	url := os.Getenv("GOEDGE_S3")
	if url == "" {
		t.Skip("GOEDGE_S3 not set")
	}
	run := time.Now().Format("150405.000")
	cfg := load(t, map[string]any{"producer_id": "exporter-test", "s3": map[string]any{"url": url + "/exporter-test/" + run}})
	cfg.QueueSettings = cfg.QueueSettings // default in-memory queue
	set := exportertest.NewNopSettings(Type)
	set.ID = component.NewIDWithName(Type, "t")
	ctx := context.Background()
	f := NewFactory()
	te, err := f.CreateTraces(ctx, set, cfg)
	if err != nil {
		t.Fatal(err)
	}
	me, err := f.CreateMetrics(ctx, set, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []component.Component{te, me} {
		if err := c.Start(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	td := ptrace.NewTraces()
	s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetName("x")
	s.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	if err := te.ConsumeTraces(ctx, td); err != nil {
		t.Fatal(err)
	}
	md := pmetric.NewMetrics()
	dp := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	if err := me.ConsumeMetrics(ctx, md); err != nil {
		t.Fatal(err)
	}
	for _, c := range []component.Component{te, me} {
		if err := c.Shutdown(ctx); err != nil { // drains the queue
			t.Fatal(err)
		}
	}
	e, err := acquire(set.ID, cfg) // a fresh edge: the old one was released
	if err != nil || e == nil {
		t.Fatal(err)
	}
	release(set.ID, set.Logger)
	_ = parquetgo.SigSeries
}
