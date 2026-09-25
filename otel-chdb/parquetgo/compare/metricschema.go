package compare

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

// The s3() structures of the metric objects (METRICS_SCHEMA.md), without
// and with the envelope, and the contrib columns every comparison hashes.
const (
	mCommon    = "ResourceAttributes Map(String, String), ResourceSchemaUrl String, ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), ScopeDroppedAttrCount UInt32, ScopeSchemaUrl String, ServiceName String, MetricName String, MetricDescription String, MetricUnit String, Attributes Map(String, String), StartTimeUnix DateTime, TimeUnix DateTime"
	mExemplars = "`Exemplars.FilteredAttributes` Array(Map(String, String)), `Exemplars.TimeUnix` Array(DateTime), `Exemplars.Value` Array(Float64), `Exemplars.SpanId` Array(String), `Exemplars.TraceId` Array(String)"
	mEnvelope  = "producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
)

// MetricStructures are the s3() structure strings of the metric objects.
var MetricStructures = [parquetgo.NumMetricTypes]string{
	parquetgo.MetricGauge: mCommon + ", Value Float64, Flags UInt32, " + mExemplars + ", " + mEnvelope,
	parquetgo.MetricSum:   mCommon + ", Value Float64, Flags UInt32, " + mExemplars + ", AggregationTemporality Int32, IsMonotonic Bool, " + mEnvelope,
	parquetgo.MetricHistogram: mCommon + ", Count UInt64, Sum Float64, BucketCounts Array(UInt64), ExplicitBounds Array(Float64), " + mExemplars +
		", Flags UInt32, Min Float64, Max Float64, AggregationTemporality Int32, " + mEnvelope,
	parquetgo.MetricExponentialHistogram: mCommon + ", Count UInt64, Sum Float64, Scale Int32, ZeroCount UInt64, PositiveOffset Int32, PositiveBucketCounts Array(UInt64), NegativeOffset Int32, NegativeBucketCounts Array(UInt64), " + mExemplars +
		", Flags UInt32, Min Float64, Max Float64, AggregationTemporality Int32, " + mEnvelope,
	parquetgo.MetricSummary: mCommon + ", Count UInt64, Sum Float64, `ValueAtQuantiles.Quantile` Array(Float64), `ValueAtQuantiles.Value` Array(Float64), Flags UInt32, " + mEnvelope,
}

// EnvelopeCols are the envelope columns every object ends with.
var EnvelopeCols = []string{"producer_id", "producer_epoch", "batch_id", "row_ordinal", "received_at", "schema_version"}

// ContribCols is the column list of type t's table (no envelope), quoted.
func ContribCols(t parquetgo.MetricType) string {
	cols := parquetgo.MetricColumns(t)
	cols = cols[:len(cols)-len(EnvelopeCols)]
	for i, c := range cols {
		cols[i] = "`" + c + "`"
	}
	return strings.Join(cols, ", ")
}

// OneBlock is the importer's single-block setting set, as
// ../../model/FASTPATH.md and fastpath/fp.py (ONE_BLOCK) specify it.
const OneBlock = "max_threads = 1, max_insert_threads = 1, max_block_size = 1048576, max_insert_block_size = 1048576, " +
	"min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, input_format_parquet_max_block_size = 1048576, " +
	"input_format_parquet_prefer_block_bytes = 17179869184"

// CentralDDL turns the exporter's CREATE TABLE (SHOW CREATE TABLE output)
// into the central target: renamed to table, with the envelope columns
// after the exporter's, partitioned by the per-batch-constant
// toDate(received_at) instead of toDate(TimeUnix) (FASTPATH.md), and with a
// dedup window for the importer's token. withEnvelope false keeps the
// exporter's table as it is, renamed.
func CentralDDL(exporterDDL, table string, withEnvelope bool) (string, error) {
	ddl := regexp.MustCompile(`^CREATE TABLE \S+`).ReplaceAllString(exporterDDL, "CREATE TABLE "+table)
	if !withEnvelope {
		return ddl, nil
	}
	for _, r := range [][2]string{
		{"    INDEX idx_res_attr_key", "    producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16,\n    INDEX idx_res_attr_key"},
		{"PARTITION BY toDate(TimeUnix)", "PARTITION BY toDate(received_at)"},
		{"SETTINGS index_granularity = 8192", "SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192"},
	} {
		if !strings.Contains(ddl, r[0]) {
			return "", fmt.Errorf("exporter DDL lacks %q:\n%s", r[0], ddl)
		}
		ddl = strings.Replace(ddl, r[0], r[1], 1)
	}
	return ddl, nil
}
