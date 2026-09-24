package chdbexporter

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
)

// ProducerConfig identifies this exporter as a source of published batches.
// Every published row carries it (the envelope), and it names the object
// storage namespace: {region}/{signal}/v{schema_version}/{id}/{epoch}/.
type ProducerConfig struct {
	// ID is a stable identity for this source. Defaults to the hostname.
	ID string `mapstructure:"id"`
	// Region is the first path segment of every namespace.
	Region string `mapstructure:"region"`
	// SchemaVersion is stamped on every row and names the namespace, so a
	// schema change starts new tables instead of altering published ones.
	SchemaVersion uint16 `mapstructure:"schema_version"`
	// Epoch distinguishes incarnations of one producer. Empty (the default)
	// generates a new one at start, so a restarted process never writes into
	// tables a previous incarnation owned: no fencing needed.
	Epoch string `mapstructure:"epoch"`
}

// S3Config locates a bucket prefix. Endpoint is a path-style URL including
// the bucket, e.g. http://127.0.0.1:8333/otel or
// https://bucket.s3.eu-west-1.amazonaws.com/prefix.
type S3Config struct {
	Endpoint        string              `mapstructure:"endpoint"`
	AccessKeyID     string              `mapstructure:"access_key_id"`
	SecretAccessKey configopaque.String `mapstructure:"secret_access_key"`
}

// ObjectStorageConfig puts the MergeTree tables on s3_plain_rewritable
// disks: one writer (this exporter) per table, and any number of read-only
// readers attaching the same endpoint with refresh_parts_interval.
type ObjectStorageConfig struct {
	S3Config `mapstructure:",squash"`
	// Generation is how long one set of tables takes writes before the
	// exporter seals it and starts the next. Default 1h.
	Generation time.Duration `mapstructure:"generation"`
	// LocalRetention is how long a sealed generation stays attached in this
	// process (queryable locally) before it is detached. Detaching never
	// deletes objects: reclaiming them is the central consumer's job, once
	// it has acknowledged the generation. Default 6h; must be under 72h.
	// Without object storage (local tables beside Parquet export) sealed
	// generations are dropped after it instead.
	LocalRetention time.Duration `mapstructure:"local_retention"`
	// SealOptimize runs OPTIMIZE TABLE ... FINAL before sealing, so a sealed
	// generation is one part per partition and readers list fewer objects.
	SealOptimize bool `mapstructure:"seal_optimize"`
	// CompactParts keeps every part compact (all columns in one data file)
	// however large it grows. MergeTree switches a part to the wide format,
	// a file per column, past 10 MB; on s3_plain_rewritable a file is an
	// object, and a merged wide part of the traces table is ~150 PUTs against
	// ~20 for a compact one. Default true.
	CompactParts bool `mapstructure:"compact_parts"`
	// OldPartsLifetime is how long a part replaced by a merge keeps its
	// objects before the writer deletes them. chDB's default is 0: a reader
	// in the middle of a query over the old part would find its objects
	// gone. Readers see a merged part as covering the ones it replaced, so
	// keeping them costs storage, not correctness. Default 10m; it bounds
	// the longest safe reader query on an active generation.
	OldPartsLifetime time.Duration `mapstructure:"old_parts_lifetime"`
}

// ParquetConfig writes every batch as one Parquet object as well.
type ParquetConfig struct {
	// URL is an S3 endpoint (http:// or https://, with bucket) or a local
	// directory (file:///abs/path).
	URL             string              `mapstructure:"url"`
	AccessKeyID     string              `mapstructure:"access_key_id"`
	SecretAccessKey configopaque.String `mapstructure:"secret_access_key"`
	// Compression is the Parquet codec: zstd (default), lz4, snappy, gzip or none.
	Compression string `mapstructure:"compression"`
}

func (c *Config) objectStorage() bool { return c.ObjectStorage.Endpoint != "" }
func (c *Config) parquet() bool       { return c.Parquet.URL != "" }

// publishing reports whether batches leave this process: the envelope, batch
// ids, generations and manifests exist only then.
func (c *Config) publishing() bool { return c.objectStorage() || c.parquet() }

// generation is the rotation period; object storage's when set, else an hour
// (Parquet objects are still grouped by generation).
func (c *Config) generation() time.Duration {
	if c.ObjectStorage.Generation > 0 {
		return c.ObjectStorage.Generation
	}
	return time.Hour
}

var segmentRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func (c *Config) validatePublish() error {
	var err error
	if !c.publishing() {
		if !c.StoreTables {
			err = errors.Join(err, errors.New("store_tables: false needs parquet, or there is nowhere for data to go"))
		}
		return err
	}
	for name, v := range map[string]string{"producer.id": c.Producer.ID, "producer.region": c.Producer.Region} {
		if !segmentRe.MatchString(v) {
			err = errors.Join(err, fmt.Errorf("%s %q must match %s (it is a path segment)", name, v, segmentRe))
		}
	}
	if c.Producer.Epoch != "" && !segmentRe.MatchString(c.Producer.Epoch) {
		err = errors.Join(err, fmt.Errorf("producer.epoch %q must match %s", c.Producer.Epoch, segmentRe))
	}
	if c.InsertFormat != FormatRowBinary {
		err = errors.Join(err, errors.New("object_storage and parquet need insert_format rowbinary"))
	}
	if c.objectStorage() {
		if !isHTTP(c.ObjectStorage.Endpoint) {
			err = errors.Join(err, errors.New("object_storage.endpoint must be an http(s) URL including the bucket"))
		}
		if !c.StoreTables {
			err = errors.Join(err, errors.New("object_storage needs store_tables"))
		}

		if g := c.ObjectStorage.Generation; g != 0 && g < time.Second {
			err = errors.Join(err, errors.New("object_storage.generation must be at least 1s"))
		}
	}
	if r := c.ObjectStorage.LocalRetention; r < 0 || r >= 72*time.Hour {
		err = errors.Join(err, errors.New("object_storage.local_retention must be in [0, 72h)"))
	}
	if c.parquet() {
		u := c.Parquet.URL
		if !isHTTP(u) && !(strings.HasPrefix(u, "file:///")) {
			err = errors.Join(err, errors.New("parquet.url must be http(s):// (S3) or file:///absolute/dir"))
		}
		switch c.Parquet.Compression {
		case "zstd", "lz4", "snappy", "gzip", "none":
		default:
			err = errors.Join(err, fmt.Errorf("parquet.compression %q must be zstd, lz4, snappy, gzip or none", c.Parquet.Compression))
		}
	}
	return err
}

func isHTTP(u string) bool {
	return strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://")
}

func defaultProducerID() string {
	h, err := os.Hostname()
	if err != nil {
		return "producer"
	}
	h = regexp.MustCompile(`[^A-Za-z0-9._-]`).ReplaceAllString(h, "-")
	if !segmentRe.MatchString(h) {
		return "producer"
	}
	return h
}
