// Package s3pqexporter is the Go edge's publisher as a collector exporter,
// `s3pq`: the same objects, keys, metadata, content keys, lanes and
// request-level acknowledgement as the Rust edge's `urn:otel:exporter:s3pq`
// (../../otap-rs), so one Rust consumer ingests both. The work is done by
// ../edge; this package is configuration and the exporterhelper wiring
// (persistent queue, retry, timeout).
package s3pqexporter

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config mirrors the Rust exporter's configuration (otap-rs/src/exporter.rs
// `Config`), plus the collector's queue, retry and timeout.
type Config struct {
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueSettings                configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	BackOffConfig                configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`

	// ProducerID names this publisher (the envelope's producer_id and
	// x-amz-meta-oscope-producer). It must be unique in the fleet and
	// stable per persistent queue.
	ProducerID string `mapstructure:"producer_id"`
	// Lanes per namespace (default 1): a request goes to lane
	// hash(content) mod lanes, so a retry meets its own unresolved slot.
	Lanes int `mapstructure:"lanes"`
	// MetricsLayout: series_table (layout B, the default) or clickstack_tables.
	MetricsLayout string                  `mapstructure:"metrics_layout"`
	Series        parquetgo.SeriesOptions `mapstructure:"series"`
	Parquet       ParquetConfig           `mapstructure:"parquet"`
	S3            S3Config                `mapstructure:"s3"`
}

// ParquetConfig tunes the trace, log and layout-A metric objects (layout B
// has its own encoding, `series`).
type ParquetConfig struct {
	// ZstdLevel: 0 means the codec's default (level 3).
	ZstdLevel int `mapstructure:"zstd_level"`
	// BloomColumns: leaf paths with a bloom filter (default [TraceId]);
	// empty list: none.
	BloomColumns []string `mapstructure:"bloom_columns"`
	// Statistics: "page" (default: chunk and page statistics, page index)
	// or "none".
	Statistics string `mapstructure:"statistics"`
}

// S3Config is the bucket and the credentials (parquetgo's S3 client, DECISIONS.md D18).
type S3Config struct {
	// URL: s3://bucket/prefix (AWS) or http(s)://host[:port]/bucket/prefix
	// (a custom endpoint, path-style). Objects go under
	// {prefix}/{namespace}/{epoch}/{seq:020d}.parquet: put the producer in
	// the prefix ({root}/{producer}, the consumer's --depth 2).
	URL    string `mapstructure:"url"`
	Region string `mapstructure:"region"`
	// Static keys (Nutanix Objects, SeaweedFS, MinIO); without them the AWS
	// default chain (IRSA, Pod Identity, credential_process, IMDS, ...).
	AccessKeyID     string              `mapstructure:"access_key_id"`
	SecretAccessKey configopaque.String `mapstructure:"secret_access_key"`
	SessionToken    configopaque.String `mapstructure:"session_token"`
	Profile         string              `mapstructure:"profile"`
	RoleARN         string              `mapstructure:"role_arn"`
	// CABundle: PEM with extra roots (a private CA); AWS_CA_BUNDLE otherwise.
	CABundle string `mapstructure:"ca_bundle"`
	// PathStyle overrides the addressing (default: path-style for a custom
	// endpoint, virtual-hosted for s3://).
	PathStyle   *bool         `mapstructure:"path_style"`
	PutTimeout  time.Duration `mapstructure:"put_timeout"`
	HeadTimeout time.Duration `mapstructure:"head_timeout"`
}

func (c *Config) Validate() error {
	var errs []error
	if c.ProducerID == "" {
		errs = append(errs, errors.New("producer_id is required"))
	}
	if c.S3.URL == "" {
		errs = append(errs, errors.New("s3.url is required"))
	} else if !strings.HasPrefix(c.S3.URL, "s3://") && !strings.HasPrefix(c.S3.URL, "http://") && !strings.HasPrefix(c.S3.URL, "https://") {
		errs = append(errs, fmt.Errorf("s3.url %q: want s3:// or http(s)://", c.S3.URL))
	}
	switch c.MetricsLayout {
	case "", edge.SeriesTable, edge.ClickstackTables:
	default:
		errs = append(errs, fmt.Errorf("metrics_layout %q: want %s or %s", c.MetricsLayout, edge.SeriesTable, edge.ClickstackTables))
	}
	switch c.Series.Statistics {
	case "", "none", "page":
	default:
		errs = append(errs, fmt.Errorf("series.statistics %q: want none or page", c.Series.Statistics))
	}
	switch c.Parquet.Statistics {
	case "", "none", "page":
	default:
		errs = append(errs, fmt.Errorf("parquet.statistics %q: want none or page", c.Parquet.Statistics))
	}
	if c.Lanes < 0 || c.Lanes > 64 {
		errs = append(errs, fmt.Errorf("lanes %d: want 1..64", c.Lanes))
	}
	// DECISIONS.md D4: a request must never be dropped from the queue for
	// age, and must not be re-batched after it (its content key would change).
	if c.BackOffConfig.Enabled && c.BackOffConfig.MaxElapsedTime != 0 {
		errs = append(errs, errors.New("retry_on_failure.max_elapsed_time must be 0: an item is deleted from the queue after it (DECISIONS.md D4)"))
	}
	if q := c.QueueSettings.Get(); q != nil && q.Batch.HasValue() {
		errs = append(errs, errors.New("sending_queue.batch re-cuts requests after the queue, which changes their content keys across restarts (DECISIONS.md risk #10): batch before the queue"))
	}
	return errors.Join(errs...)
}

// EdgeConfig maps the collector configuration onto ../edge's.
func (c *Config) EdgeConfig() edge.Config {
	p := edge.EdgeParquet()
	if c.Parquet.ZstdLevel != 0 {
		p.CompressionLevel = c.Parquet.ZstdLevel
	}
	if c.Parquet.BloomColumns != nil {
		p.BloomColumns = c.Parquet.BloomColumns
		p.BloomFilters = len(p.BloomColumns) > 0
	}
	if c.Parquet.Statistics == "none" {
		p.Statistics, p.PageIndex, p.NoBounds = false, false, true
	}
	s3 := parquetgo.Config{URL: c.S3.URL, S3Region: c.S3.Region, AccessKeyID: c.S3.AccessKeyID,
		SecretAccessKey: string(c.S3.SecretAccessKey), SessionToken: string(c.S3.SessionToken),
		Profile: c.S3.Profile, RoleARN: c.S3.RoleARN, CABundle: c.S3.CABundle}
	s3.PathStyle = c.S3.PathStyle
	series := c.Series
	d := parquetgo.DefaultSeriesOptions()
	if series.Window == 0 {
		series.Window = d.Window
	}
	if series.Statistics == "" {
		series.Statistics = d.Statistics
	}
	return edge.Config{S3: s3, ProducerID: c.ProducerID, Lanes: c.Lanes, MetricsLayout: c.MetricsLayout,
		Series: series, Parquet: p, PutTimeout: c.S3.PutTimeout, HeadTimeout: c.S3.HeadTimeout}
}
