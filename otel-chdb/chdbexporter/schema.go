package chdbexporter

import (
	"fmt"
	"strings"
	"time"
)

// The table definitions are the contrib clickhouse exporter's
// (exporter/clickhouseexporter/internal/sqltemplates, v0.161.0, Apache-2.0)
// with cluster and engine templating removed: chDB is one process, and the
// engine is always MergeTree. Keeping the schema identical is the point: a
// reader written for that exporter's tables reads these.

const tracesTable = `CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    Events Nested (
        Timestamp DateTime64(9),
        Name LowCardinality(String),
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    Links Nested (
        TraceId String,
        SpanId String,
        TraceState String,
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),%[4]s
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
%[3]s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1%[5]s`

const tracesIDTsTable = `CREATE TABLE IF NOT EXISTS %[1]s.%[2]s_trace_id_ts (
    TraceId String CODEC(ZSTD(1)),
    Start DateTime CODEC(Delta, ZSTD(1)),
    End DateTime CODEC(Delta, ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Start)
ORDER BY (TraceId, Start)
%[3]s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1%[4]s`

const tracesIDTsView = `CREATE MATERIALIZED VIEW IF NOT EXISTS %[1]s.%[2]s_trace_id_ts_mv
TO %[1]s.%[2]s_trace_id_ts
AS SELECT TraceId, min(Timestamp) as Start, max(Timestamp) as End
FROM %[1]s.%[2]s
WHERE TraceId != ''
GROUP BY TraceId`

const logsTable = `CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    TraceFlags UInt8,
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName String CODEC(ZSTD(1)),%[4]s
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_lower_body lower(Body) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)
%[3]s
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1%[5]s`

// Column lists, in the order the encoders write them.
var (
	traceColumns = []string{
		"Timestamp", "TraceId", "SpanId", "ParentSpanId", "TraceState", "SpanName", "SpanKind",
		"ServiceName", "ResourceAttributes", "ScopeName", "ScopeVersion", "SpanAttributes",
		"Duration", "StatusCode", "StatusMessage",
		"Events.Timestamp", "Events.Name", "Events.Attributes",
		"Links.TraceId", "Links.SpanId", "Links.TraceState", "Links.Attributes",
	}
	traceStructure = "Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, " +
		"SpanName String, SpanKind String, ServiceName String, ResourceAttributes Map(String, String), " +
		"ScopeName String, ScopeVersion String, SpanAttributes Map(String, String), Duration UInt64, " +
		"StatusCode String, StatusMessage String, `Events.Timestamp` Array(DateTime64(9)), " +
		"`Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), " +
		"`Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), " +
		"`Links.Attributes` Array(Map(String, String))"

	logColumns = []string{
		"Timestamp", "TraceId", "SpanId", "TraceFlags", "SeverityText", "SeverityNumber", "ServiceName",
		"Body", "ResourceSchemaUrl", "ResourceAttributes", "ScopeSchemaUrl", "ScopeName", "ScopeVersion",
		"ScopeAttributes", "LogAttributes", "EventName",
	}
	logStructure = "Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText String, " +
		"SeverityNumber UInt8, ServiceName String, Body String, ResourceSchemaUrl String, " +
		"ResourceAttributes Map(String, String), ScopeSchemaUrl String, ScopeName String, ScopeVersion String, " +
		"ScopeAttributes Map(String, String), LogAttributes Map(String, String), EventName String"
)

func ttlExpr(ttl time.Duration, field string) string {
	if ttl <= 0 {
		return ""
	}
	switch {
	case ttl%(24*time.Hour) == 0:
		return fmt.Sprintf("TTL %s + toIntervalDay(%d)", field, ttl/(24*time.Hour))
	case ttl%time.Hour == 0:
		return fmt.Sprintf("TTL %s + toIntervalHour(%d)", field, ttl/time.Hour)
	case ttl%time.Minute == 0:
		return fmt.Sprintf("TTL %s + toIntervalMinute(%d)", field, ttl/time.Minute)
	}
	return fmt.Sprintf("TTL %s + toIntervalSecond(%d)", field, ttl/time.Second)
}

func quoteColumns(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = "`" + c + "`"
	}
	return strings.Join(q, ", ")
}

// The envelope: columns that identify where and in which batch a row
// entered, added to every table when the exporter publishes (object storage
// or Parquet). A central reader checkpoints on (producer_id, producer_epoch,
// batch_id), never on event time or part names, which merges change.
const envelopeDDL = `
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(Delta, ZSTD(1)),
    schema_version UInt16,`

var envelopeColumns = []string{"producer_id", "producer_epoch", "batch_id", "row_ordinal", "received_at", "schema_version"}

const envelopeStructure = ", producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, " +
	"received_at DateTime64(9), schema_version UInt16"

const (
	signalTraces = "traces"
	signalLogs   = "logs"
)

// columns and structure return a signal's insert column list and its
// plain-typed structure, with the envelope when env is set.
func columns(signal string, env bool) []string {
	c := traceColumns
	if signal == signalLogs {
		c = logColumns
	}
	if env {
		c = append(append([]string(nil), c...), envelopeColumns...)
	}
	return c
}

func structure(signal string, env bool) string {
	s := traceStructure
	if signal == signalLogs {
		s = logStructure
	}
	if env {
		s += envelopeStructure
	}
	return s
}

// tableSet is everything one signal writes to in one generation.
type tableSet struct {
	signal string
	// base is the MergeTree table's name: the configured name, plus a
	// generation suffix when tables rotate.
	base string
	// target is the fully qualified table inserts name, and insert the
	// INSERT statement (no FORMAT, no data) naming it and its columns.
	target, insert string
	ddl            []string
	// stored are the MergeTree tables holding data. On object storage they
	// are only ever detached, never dropped: DROP would delete the objects a
	// central reader may not have consumed yet.
	stored []string
	// local are the helper objects (views, staging table, Buffer) that
	// exist only in this process's catalog and are dropped at seal, in
	// order: views before the tables they read or write.
	local []string
}

// diskFunc returns the SETTINGS suffix that puts a table on its own disk, or
// "" for the default local disk.
type diskFunc func(table string) string

func localDisk(string) string { return "" }

// buildTableSet returns the DDL for one signal's tables. gen is "" for the
// unrotated local layout.
func buildTableSet(c *Config, signal, gen string, env bool, disk diskFunc) tableSet {
	db := c.Database
	name := c.TracesTableName
	if signal == signalLogs {
		name = c.LogsTableName
	}
	base := name
	if gen != "" {
		base = name + "_" + gen
	}
	extra := ""
	if env {
		extra = envelopeDDL
	}
	ts := tableSet{signal: signal, base: base}
	fq := db + "." + base
	if signal == signalTraces {
		ts.ddl = append(ts.ddl,
			fmt.Sprintf(tracesTable, db, base, ttlExpr(c.TTL, "toDateTime(Timestamp)"), extra, disk(base)),
			fmt.Sprintf(tracesIDTsTable, db, base, ttlExpr(c.TTL, "toDateTime(Start)"), disk(base+"_trace_id_ts")),
			fmt.Sprintf(tracesIDTsView, db, base))
		ts.stored = []string{fq, fq + "_trace_id_ts"}
		ts.local = []string{fq + "_trace_id_ts_mv"}
	} else {
		ts.ddl = append(ts.ddl, fmt.Sprintf(logsTable, db, base, ttlExpr(c.TTL, "toDateTime(Timestamp)"), extra, disk(base)))
		ts.stored = []string{fq}
	}
	into := fq
	var buffer []string
	if c.BufferSeconds > 0 {
		ts.ddl = append(ts.ddl, bufferDDL(db, base, c.BufferSeconds))
		into = fq + "_buf"
		buffer = []string{into}
	}
	ts.target = into
	if c.StagingTables {
		ts.ddl = append(ts.ddl, stagingDDL(db, base, structure(signal, env)), stagingViewDDL(db, base, into))
		ts.target = fq + "_in"
		ts.local = append([]string{fq + "_in_mv", fq + "_in"}, ts.local...)
	}
	ts.local = append(ts.local, buffer...)
	ts.insert = fmt.Sprintf("INSERT INTO %s (%s)", ts.target, quoteColumns(columns(signal, env)))
	return ts
}

// schemaDDL returns the statements that create the database and both
// signals' tables in the local, unrotated layout.
func schemaDDL(c *Config) []string {
	ddl := []string{fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.Database)}
	for _, sig := range []string{signalTraces, signalLogs} {
		ddl = append(ddl, buildTableSet(c, sig, "", false, localDisk).ddl...)
	}
	return ddl
}

// Staging tables. Parsing RowBinary (or JSON) straight into
// LowCardinality(String) and Map(LowCardinality(String), String) columns
// costs about twice as much as parsing into String and Map(String, String)
// and converting the whole block afterwards (see README, "Why staging
// tables"). chDB's streaming insert refuses INSERT ... SELECT FROM input(),
// which would be the usual way to parse with other types, so inserts go to
// a Null-engine table with plain types, and a materialized view converts
// each block into the real table.
func stagingDDL(db, table, structure string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s_in (%s) ENGINE = Null", db, table, structure)
}

func stagingViewDDL(db, table, target string) string {
	return fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %[1]s.%[2]s_in_mv TO %[3]s AS SELECT * FROM %[1]s.%[2]s_in",
		db, table, target)
}

// bufferDDL puts a single-layer Buffer in front of table: flushed after
// secs, or sooner at a million rows or 100 MiB.
func bufferDDL(db, table string, secs int) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s.%[2]s_buf AS %[1]s.%[2]s "+
		"ENGINE = Buffer(%[1]s, %[2]s, 1, %[3]d, %[3]d, 10000, 1000000, 10485760, 104857600)", db, table, secs)
}
