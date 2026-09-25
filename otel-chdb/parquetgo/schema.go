// Package parquetgo publishes OpenTelemetry traces and logs as Parquet
// without chDB: pdata is walked straight into Arrow builders and written with
// arrow-go's Parquet writer, then uploaded with an S3 client. The schema, the
// envelope columns, the object layout and the manifests are the chdb
// exporter's (../chdbexporter, publish.go), so a consumer cannot tell the two
// producers apart except by the Parquet footer's created_by.
package parquetgo

import "github.com/apache/arrow-go/v18/arrow"

// The Arrow schemas mirror what ClickHouse's Parquet writer produces for the
// exporter's plain-typed structure (schema.go traceStructure/logStructure plus
// envelopeStructure): every column required, DateTime64(9) as
// TIMESTAMP(NANOS, UTC), Map(String, String) as a MAP of required key/value,
// Nested columns as separate required LISTs, UInt8/16/32/64 as unsigned INTs.

var (
	tsType  = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	strType = arrow.BinaryTypes.String
)

func attrMap() *arrow.MapType {
	m := arrow.MapOf(strType, strType)
	m.SetItemNullable(false)
	return m
}

func col(name string, t arrow.DataType) arrow.Field {
	return arrow.Field{Name: name, Type: t, Nullable: false}
}

func list(t arrow.DataType) arrow.DataType { return arrow.ListOfNonNullable(t) }

var envelopeFields = []arrow.Field{
	col("producer_id", strType),
	col("producer_epoch", strType),
	col("batch_id", arrow.PrimitiveTypes.Uint64),
	col("row_ordinal", arrow.PrimitiveTypes.Uint32),
	col("received_at", tsType),
	col("schema_version", arrow.PrimitiveTypes.Uint16),
}

// TracesSchema is the published traces schema, envelope included.
var TracesSchema = arrow.NewSchema(append([]arrow.Field{
	col("Timestamp", tsType),
	col("TraceId", strType),
	col("SpanId", strType),
	col("ParentSpanId", strType),
	col("TraceState", strType),
	col("SpanName", strType),
	col("SpanKind", strType),
	col("ServiceName", strType),
	col("ResourceAttributes", attrMap()),
	col("ScopeName", strType),
	col("ScopeVersion", strType),
	col("SpanAttributes", attrMap()),
	col("Duration", arrow.PrimitiveTypes.Uint64),
	col("StatusCode", strType),
	col("StatusMessage", strType),
	col("Events.Timestamp", list(tsType)),
	col("Events.Name", list(strType)),
	col("Events.Attributes", list(attrMap())),
	col("Links.TraceId", list(strType)),
	col("Links.SpanId", list(strType)),
	col("Links.TraceState", list(strType)),
	col("Links.Attributes", list(attrMap())),
}, envelopeFields...), nil)

// LogsSchema is the published logs schema, envelope included.
var LogsSchema = arrow.NewSchema(append([]arrow.Field{
	col("Timestamp", tsType),
	col("TraceId", strType),
	col("SpanId", strType),
	col("TraceFlags", arrow.PrimitiveTypes.Uint8),
	col("SeverityText", strType),
	col("SeverityNumber", arrow.PrimitiveTypes.Uint8),
	col("ServiceName", strType),
	col("Body", strType),
	col("ResourceSchemaUrl", strType),
	col("ResourceAttributes", attrMap()),
	col("ScopeSchemaUrl", strType),
	col("ScopeName", strType),
	col("ScopeVersion", strType),
	col("ScopeAttributes", attrMap()),
	col("LogAttributes", attrMap()),
	col("EventName", strType),
}, envelopeFields...), nil)
