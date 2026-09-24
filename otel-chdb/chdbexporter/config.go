// Package chdbexporter writes traces and logs into an in-process chDB,
// using the same otel_traces / otel_logs schema as the contrib clickhouse
// exporter, so anything that queries that schema (ClickStack, HyperDX,
// Grafana's ClickHouse plugin, oscope) reads what this writes.
package chdbexporter

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Insert formats. See README.md for what each costs.
const (
	// FormatRowBinary encodes rows as RowBinary and hands the bytes to
	// chDB through chdb_stream_insert_n. Needs the chdb-go fork.
	FormatRowBinary = "rowbinary"
	// FormatJSON encodes rows as JSONEachRow and inserts them as the tail of
	// an INSERT statement through Session.Query. Works with stock chdb-go.
	FormatJSON = "json"
	// FormatFile encodes RowBinary into a file and inserts it with
	// INSERT ... SELECT FROM file(). Works with stock chdb-go; pays a write
	// and a read of the batch through the page cache instead of a text
	// encoding.
	FormatFile = "file"
)

type Config struct {
	TimeoutSettings exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	BackOffConfig   configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	QueueSettings   configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// Path is the chDB data directory. Empty means a temporary directory
	// that is removed at shutdown. chDB binds one path per process: every
	// chdb exporter in a collector must name the same path.
	Path string `mapstructure:"path"`
	// Database, table names and TTL mean what they mean in the clickhouse
	// exporter.
	Database        string        `mapstructure:"database"`
	TracesTableName string        `mapstructure:"traces_table_name"`
	LogsTableName   string        `mapstructure:"logs_table_name"`
	TTL             time.Duration `mapstructure:"ttl"`
	CreateSchema    bool          `mapstructure:"create_schema"`

	// InsertFormat is rowbinary (default), json or file.
	InsertFormat string `mapstructure:"insert_format"`
	// Connections is how many native connections inserts are spread over.
	// A connection runs one statement at a time, so with a sending queue of
	// N consumers, N connections let N batches insert in parallel.
	Connections int `mapstructure:"connections"`
	// BufferSeconds > 0 puts a Buffer table in front of each table, which
	// turns many small inserts into few large parts. Rows in the buffer are
	// lost if the process dies before it flushes; Shutdown flushes it.
	BufferSeconds int `mapstructure:"buffer_seconds"`
	// StagingTables routes inserts through a Null-engine table with plain
	// String types and a materialized view into the real table, which halves
	// parse cost for the LowCardinality and Map columns. On by default.
	StagingTables bool `mapstructure:"staging_tables"`
	// FileDir is where FormatFile stages batches. Defaults to the OS temp
	// directory.
	FileDir string `mapstructure:"file_dir"`
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (c *Config) Validate() error {
	var err error
	for name, v := range map[string]string{
		"database": c.Database, "traces_table_name": c.TracesTableName, "logs_table_name": c.LogsTableName,
	} {
		if !identRe.MatchString(v) {
			err = errors.Join(err, fmt.Errorf("%s %q must match %s", name, v, identRe))
		}
	}
	switch c.InsertFormat {
	case FormatRowBinary, FormatJSON, FormatFile:
	default:
		err = errors.Join(err, fmt.Errorf("insert_format %q must be rowbinary, json or file", c.InsertFormat))
	}
	if c.Connections < 1 {
		err = errors.Join(err, errors.New("connections must be at least 1"))
	}
	if c.BufferSeconds < 0 {
		err = errors.Join(err, errors.New("buffer_seconds must not be negative"))
	}
	return err
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:   configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		Database:        "otel",
		TracesTableName: "otel_traces",
		LogsTableName:   "otel_logs",
		CreateSchema:    true,
		InsertFormat:    FormatRowBinary,
		Connections:     1,
		StagingTables:   true,
	}
}
