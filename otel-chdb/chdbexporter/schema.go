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
    ) CODEC(ZSTD(1)),
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
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1`

const tracesIDTsTable = `CREATE TABLE IF NOT EXISTS %[1]s.%[2]s_trace_id_ts (
    TraceId String CODEC(ZSTD(1)),
    Start DateTime CODEC(Delta, ZSTD(1)),
    End DateTime CODEC(Delta, ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Start)
ORDER BY (TraceId, Start)
%[3]s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1`

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
    EventName String CODEC(ZSTD(1)),
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
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`

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

// schemaDDL returns the statements that create the database, the tables, and
// when buffer_seconds is set, the Buffer tables inserts go through.
func schemaDDL(c *Config) []string {
	db := c.Database
	ddl := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db),
		fmt.Sprintf(tracesTable, db, c.TracesTableName, ttlExpr(c.TTL, "toDateTime(Timestamp)")),
		fmt.Sprintf(tracesIDTsTable, db, c.TracesTableName, ttlExpr(c.TTL, "toDateTime(Start)")),
		fmt.Sprintf(tracesIDTsView, db, c.TracesTableName),
		fmt.Sprintf(logsTable, db, c.LogsTableName, ttlExpr(c.TTL, "toDateTime(Timestamp)")),
	}
	if c.BufferSeconds > 0 {
		for _, t := range []string{c.TracesTableName, c.LogsTableName} {
			ddl = append(ddl, bufferDDL(db, t, c.BufferSeconds))
		}
	}
	if c.StagingTables {
		ddl = append(ddl,
			stagingDDL(db, c.TracesTableName, traceStructure),
			stagingViewDDL(db, c.TracesTableName, bufferedTable(c, c.TracesTableName)),
			stagingDDL(db, c.LogsTableName, logStructure),
			stagingViewDDL(db, c.LogsTableName, bufferedTable(c, c.LogsTableName)),
		)
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

// bufferedTable is where rows are written to be stored: the Buffer when
// there is one, otherwise the MergeTree table.
func bufferedTable(c *Config, table string) string {
	if c.BufferSeconds > 0 {
		return c.Database + "." + table + "_buf"
	}
	return c.Database + "." + table
}

// insertTarget is the table inserts name: the staging table when there is
// one, else bufferedTable.
func insertTarget(c *Config, table string) string {
	if c.StagingTables {
		return c.Database + "." + table + "_in"
	}
	return bufferedTable(c, table)
}
