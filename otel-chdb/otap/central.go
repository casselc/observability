package otap

import (
	"fmt"
	"strings"
)

// Central-side SQL: what a ClickHouse consumer runs to move published
// batches into otel_traces / otel_logs.

// FlatTraceStructure / FlatLogStructure are parquetgo's plain-typed
// structures (compare/correctness_test.go), which read the flat Parquet and
// Arrow objects too.
const (
	envStructure       = ", producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
	FlatTraceStructure = "Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, SpanName String, SpanKind String, ServiceName String, ResourceAttributes Map(String, String), ScopeName String, ScopeVersion String, SpanAttributes Map(String, String), Duration UInt64, StatusCode String, StatusMessage String, `Events.Timestamp` Array(DateTime64(9)), `Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), `Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), `Links.Attributes` Array(Map(String, String))" + envStructure
	FlatLogStructure   = "Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText String, SeverityNumber UInt8, ServiceName String, Body String, ResourceSchemaUrl String, ResourceAttributes Map(String, String), ScopeSchemaUrl String, ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), LogAttributes Map(String, String), EventName String" + envStructure

	TraceCols = "Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes, Duration, StatusCode, StatusMessage, `Events.Timestamp`, `Events.Name`, `Events.Attributes`, `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`, producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version"
	LogCols   = "Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, ServiceName, Body, ResourceSchemaUrl, ResourceAttributes, ScopeSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, LogAttributes, EventName, producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version"
)

// Star table structures: every column a star table can have (the OTAP
// columns are optional, so a given object may lack some; ClickHouse fills
// them with NULL, input_format_parquet_allow_missing_columns).
const (
	starEnv      = ", producer_id String, producer_epoch String, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
	starResScope = "resource_id UInt16, resource_schema_url Nullable(String), resource_dropped_attributes_count Nullable(UInt32), scope_id UInt16, scope_name Nullable(String), scope_version Nullable(String), scope_dropped_attributes_count Nullable(UInt32), schema_url Nullable(String)"
	StarAttrs    = "batch_id UInt64, parent_id UInt32, key String, type UInt8, str Nullable(String), int Nullable(Int64), double Nullable(Float64), bool Nullable(Bool), bytes Nullable(String), ser_json Nullable(String)"
	StarSpans    = "batch_id UInt64, id Nullable(UInt32), " + starResScope + ", start_time_unix_nano DateTime64(9), duration_time_unix_nano Int64, trace_id FixedString(16), span_id FixedString(8), trace_state Nullable(String), parent_span_id Nullable(FixedString(8)), name String, kind Nullable(Int32), dropped_attributes_count Nullable(UInt32), dropped_events_count Nullable(UInt32), dropped_links_count Nullable(UInt32), status_code Nullable(Int32), status_status_message Nullable(String), flags Nullable(UInt32)" + starEnv
	StarEvents   = "batch_id UInt64, id Nullable(UInt32), parent_id UInt32, time_unix_nano Nullable(DateTime64(9)), name String, dropped_attributes_count Nullable(UInt32)"
	StarLinks    = "batch_id UInt64, id Nullable(UInt32), parent_id UInt32, trace_id FixedString(16), span_id FixedString(8), trace_state Nullable(String), dropped_attributes_count Nullable(UInt32), flags Nullable(UInt32)"
	StarLogs     = "batch_id UInt64, id Nullable(UInt32), " + starResScope + ", time_unix_nano Nullable(DateTime64(9)), observed_time_unix_nano Nullable(DateTime64(9)), trace_id Nullable(FixedString(16)), span_id Nullable(FixedString(8)), severity_number Nullable(Int32), severity_text Nullable(String), event_name Nullable(String), body_type Nullable(UInt8), body_str Nullable(String), body_int Nullable(Int64), body_double Nullable(Float64), body_bool Nullable(Bool), body_bytes Nullable(String), body_ser_json Nullable(String), dropped_attributes_count Nullable(UInt32), flags Nullable(UInt32)" + starEnv
)

// StarStructure is the s3() structure for a star table.
func StarStructure(table string) string {
	switch table {
	case "spans":
		return StarSpans
	case "logs":
		return StarLogs
	case "span_events":
		return StarEvents
	case "span_links":
		return StarLinks
	}
	return StarAttrs
}

// render is pcommon.Value.AsString in SQL, for a row with type/str/int/...
// columns under the given prefix ("" for attribute tables, "body_" for the
// log body). Doubles: Go prints 'f' format in [1e-6, 1e21) (as ClickHouse
// does) and 'e' with a sign outside it, and NaN / Infinity / -Infinity.
func renderSQL(p string) string {
	d := p + "double"
	dbl := fmt.Sprintf("multiIf(isNaN(%[1]s), 'NaN', isInfinite(%[1]s), if(%[1]s > 0, 'Infinity', '-Infinity'), "+
		"abs(%[1]s) >= 1e21 OR (%[1]s != 0 AND abs(%[1]s) < 1e-6), replaceRegexpOne(toString(%[1]s), 'e([0-9])', 'e+\\\\1'), toString(%[1]s))", d)
	return fmt.Sprintf("multiIf(%[1]stype = 1, ifNull(%[1]sstr, ''), %[1]stype = 2, toString(%[1]sint), %[1]stype = 3, %[2]s, "+
		"%[1]stype = 4, if(%[1]sbool, 'true', 'false'), %[1]stype = 7, base64Encode(ifNull(%[1]sbytes, '')), "+
		"%[1]stype IN (5, 6), ifNull(%[1]sser_json, ''), '')", p, dbl)
}

// hexOrEmpty is traceutil.TraceIDToHexOrEmptyString.
func hexOrEmpty(x string) string {
	return fmt.Sprintf("if(%[1]s IS NULL OR replaceAll(hex(%[1]s), '0', '') = '', '', lower(hex(%[1]s)))", x)
}

// attrAgg groups an attribute table into one Map per (batch_id, parent_id).
// groupArray keeps input order only when the query runs single-threaded
// (max_threads = 1); otherwise key order inside a Map depends on scheduling.
func attrAgg(src string) string {
	return fmt.Sprintf("(SELECT batch_id, parent_id, mapFromArrays(groupArray(key), groupArray(v)) AS m FROM "+
		"(SELECT batch_id, parent_id, key, %s AS v FROM %s) GROUP BY batch_id, parent_id)", renderSQL(""), src)
}

// StarTracesSelect rebuilds otel_traces rows from the star tables. src maps
// a table name to a FROM source (an s3() call, or null('structure') when
// the batch had no such table).
func StarTracesSelect(src func(table string) string) string {
	return strings.NewReplacer("\n\t", " ").Replace(fmt.Sprintf(`WITH
	ra AS %s,
	sa AS %s,
	ea AS %s,
	la AS %s,
	ev AS (SELECT e.batch_id AS batch_id, e.parent_id AS parent_id, groupArray(ifNull(e.time_unix_nano, toDateTime64(0, 9))) AS ts, groupArray(e.name) AS nm, groupArray(ea.m) AS at
	  FROM %s AS e LEFT JOIN ea ON ea.batch_id = e.batch_id AND ea.parent_id = e.id GROUP BY e.batch_id, e.parent_id),
	lk AS (SELECT l.batch_id AS batch_id, l.parent_id AS parent_id, groupArray(%s) AS tid, groupArray(%s) AS sid, groupArray(ifNull(l.trace_state, '')) AS st, groupArray(la.m) AS at
	  FROM %s AS l LEFT JOIN la ON la.batch_id = l.batch_id AND la.parent_id = l.id GROUP BY l.batch_id, l.parent_id)
	SELECT s.start_time_unix_nano AS Timestamp, %s AS TraceId, %s AS SpanId, %s AS ParentSpanId,
	  ifNull(s.trace_state, '') AS TraceState, s.name AS SpanName,
	  transform(ifNull(s.kind, 0), [0, 1, 2, 3, 4, 5], ['Unspecified', 'Internal', 'Server', 'Client', 'Producer', 'Consumer'], '') AS SpanKind,
	  ra.m['service.name'] AS ServiceName, ra.m AS ResourceAttributes,
	  ifNull(s.scope_name, '') AS ScopeName, ifNull(s.scope_version, '') AS ScopeVersion, sa.m AS SpanAttributes,
	  toUInt64(s.duration_time_unix_nano) AS Duration,
	  transform(ifNull(s.status_code, 0), [0, 1, 2], ['Unset', 'Ok', 'Error'], '') AS StatusCode,
	  ifNull(s.status_status_message, '') AS StatusMessage,
	  ev.ts AS `+"`Events.Timestamp`"+`, ev.nm AS `+"`Events.Name`"+`, ev.at AS `+"`Events.Attributes`"+`,
	  lk.tid AS `+"`Links.TraceId`"+`, lk.sid AS `+"`Links.SpanId`"+`, lk.st AS `+"`Links.TraceState`"+`, lk.at AS `+"`Links.Attributes`"+`,
	  s.producer_id AS producer_id, s.producer_epoch AS producer_epoch, s.batch_id AS batch_id, s.row_ordinal AS row_ordinal,
	  s.received_at AS received_at, s.schema_version AS schema_version
	FROM %s AS s
	LEFT JOIN ra ON ra.batch_id = s.batch_id AND ra.parent_id = s.resource_id
	LEFT JOIN sa ON sa.batch_id = s.batch_id AND sa.parent_id = s.id
	LEFT JOIN ev ON ev.batch_id = s.batch_id AND ev.parent_id = s.id
	LEFT JOIN lk ON lk.batch_id = s.batch_id AND lk.parent_id = s.id`,
		attrAgg(src("resource_attrs")), attrAgg(src("span_attrs")), attrAgg(src("span_event_attrs")), attrAgg(src("span_link_attrs")),
		src("span_events"), hexOrEmpty("l.trace_id"), hexOrEmpty("l.span_id"), src("span_links"),
		hexOrEmpty("s.trace_id"), hexOrEmpty("s.span_id"), hexOrEmpty("s.parent_span_id"), src("spans")))
}

// StarLogsSelect rebuilds otel_logs rows from the star tables.
func StarLogsSelect(src func(table string) string) string {
	return strings.NewReplacer("\n\t", " ").Replace(fmt.Sprintf(`WITH
	ra AS %s,
	sc AS %s,
	la AS %s
	SELECT if(toUnixTimestamp64Nano(ifNull(g.time_unix_nano, toDateTime64(0, 9))) = 0, ifNull(g.observed_time_unix_nano, toDateTime64(0, 9)), g.time_unix_nano) AS Timestamp,
	  %s AS TraceId, %s AS SpanId, toUInt8(ifNull(g.flags, 0) %% 256) AS TraceFlags,
	  ifNull(g.severity_text, '') AS SeverityText, toUInt8(ifNull(g.severity_number, 0) %% 256) AS SeverityNumber,
	  ra.m['service.name'] AS ServiceName, if(g.body_type IS NULL, '', %s) AS Body,
	  ifNull(g.resource_schema_url, '') AS ResourceSchemaUrl, ra.m AS ResourceAttributes,
	  ifNull(g.schema_url, '') AS ScopeSchemaUrl, ifNull(g.scope_name, '') AS ScopeName, ifNull(g.scope_version, '') AS ScopeVersion,
	  sc.m AS ScopeAttributes, la.m AS LogAttributes, ifNull(g.event_name, '') AS EventName,
	  g.producer_id AS producer_id, g.producer_epoch AS producer_epoch, g.batch_id AS batch_id, g.row_ordinal AS row_ordinal,
	  g.received_at AS received_at, g.schema_version AS schema_version
	FROM %s AS g
	LEFT JOIN ra ON ra.batch_id = g.batch_id AND ra.parent_id = g.resource_id
	LEFT JOIN sc ON sc.batch_id = g.batch_id AND sc.parent_id = g.scope_id
	LEFT JOIN la ON la.batch_id = g.batch_id AND la.parent_id = g.id`,
		attrAgg(src("resource_attrs")), attrAgg(src("scope_attrs")), attrAgg(src("log_attrs")),
		hexOrEmpty("g.trace_id"), hexOrEmpty("g.span_id"), renderSQL("g.body_"), src("logs")))
}
