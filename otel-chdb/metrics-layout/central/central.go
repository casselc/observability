// Package central holds the ClickHouse side of the layouts: DDL, the s3()
// structures of every object, the importer's INSERT…SELECT statements, a
// small HTTP client, and the S3 client the loaders publish with.
package central

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Environment defaults: the shared server and SeaweedFS of this spike.
var (
	CH       = env("ML_CLICKHOUSE", "http://127.0.0.1:18123")
	S3Host   = env("ML_S3_HOST", "127.0.0.1:18333")
	S3Key    = env("ML_S3_KEY", "otel")
	S3Secret = env("ML_S3_SECRET", "otelsecret")
	Bucket   = "otel"
	Prefix   = "metrics-layout"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// Dir is this module's root (for sql/).
func Dir() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(f))
}

// Summary is X-ClickHouse-Summary.
type Summary map[string]string

// Q runs one statement with extra URL parameters (settings, query_id).
func Q(sql string, params ...string) (string, Summary, error) {
	u := CH + "/"
	if len(params) > 0 {
		v := url.Values{}
		for i := 0; i+1 < len(params); i += 2 {
			v.Set(params[i], params[i+1])
		}
		u += "?" + v.Encode()
	}
	resp, err := http.Post(u, "text/plain", strings.NewReader(sql))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", nil, fmt.Errorf("clickhouse %d: %s\n%.400s", resp.StatusCode, strings.TrimSpace(string(b)), sql)
	}
	var s Summary
	_ = json.Unmarshal([]byte(resp.Header.Get("X-ClickHouse-Summary")), &s)
	return strings.TrimSpace(string(b)), s, nil
}

// MustQ is Q that panics.
func MustQ(sql string, params ...string) string {
	s, _, err := Q(sql, params...)
	if err != nil {
		panic(err)
	}
	return s
}

// Float parses a number result.
func Float(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v
}

// Types are the five metric types, table-name order.
var Types = []string{"gauge", "sum", "histogram", "exponential_histogram", "summary"}

// ---- A: the contrib tables plus the envelope (parquetgo/compare/metricschema.go) ----

const (
	mCommon    = "ResourceAttributes Map(String, String), ResourceSchemaUrl String, ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), ScopeDroppedAttrCount UInt32, ScopeSchemaUrl String, ServiceName String, MetricName String, MetricDescription String, MetricUnit String, Attributes Map(String, String), StartTimeUnix DateTime, TimeUnix DateTime"
	mExemplars = "`Exemplars.FilteredAttributes` Array(Map(String, String)), `Exemplars.TimeUnix` Array(DateTime), `Exemplars.Value` Array(Float64), `Exemplars.SpanId` Array(String), `Exemplars.TraceId` Array(String)"
	mEnvelope  = "producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
)

// AStructure is the s3() structure of an A object, by type index.
var AStructure = []string{
	mCommon + ", Value Float64, Flags UInt32, " + mExemplars + ", " + mEnvelope,
	mCommon + ", Value Float64, Flags UInt32, " + mExemplars + ", AggregationTemporality Int32, IsMonotonic Bool, " + mEnvelope,
	mCommon + ", Count UInt64, Sum Float64, BucketCounts Array(UInt64), ExplicitBounds Array(Float64), " + mExemplars +
		", Flags UInt32, Min Float64, Max Float64, AggregationTemporality Int32, " + mEnvelope,
	mCommon + ", Count UInt64, Sum Float64, Scale Int32, ZeroCount UInt64, PositiveOffset Int32, PositiveBucketCounts Array(UInt64), NegativeOffset Int32, NegativeBucketCounts Array(UInt64), " + mExemplars +
		", Flags UInt32, Min Float64, Max Float64, AggregationTemporality Int32, " + mEnvelope,
	mCommon + ", Count UInt64, Sum Float64, `ValueAtQuantiles.Quantile` Array(Float64), `ValueAtQuantiles.Value` Array(Float64), Flags UInt32, " + mEnvelope,
}

// OneBlock is the importer's single-block settings (FASTPATH.md).
const OneBlock = "max_threads = 1, max_insert_threads = 1, max_block_size = 1048576, max_insert_block_size = 1048576, " +
	"min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, input_format_parquet_max_block_size = 1048576, " +
	"input_format_parquet_prefer_block_bytes = 17179869184"

// ADDL returns the contrib table of type t (sql/contrib) turned into the
// central target as parquetgo's compare.CentralDDL does: envelope columns,
// PARTITION BY toDate(received_at), a dedup window.
func ADDL(db string, t int) string {
	b, err := os.ReadFile(filepath.Join(Dir(), "sql", "contrib", "otel_metrics_"+Types[t]+".sql"))
	if err != nil {
		panic(err)
	}
	ddl := strings.ReplaceAll(string(b), "{db}", db)
	for _, r := range [][2]string{
		{"    INDEX idx_res_attr_key", "    producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16,\n    INDEX idx_res_attr_key"},
		{"PARTITION BY toDate(TimeUnix)", "PARTITION BY toDate(received_at)"},
		{"SETTINGS index_granularity = 8192", "SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192"},
	} {
		if !strings.Contains(ddl, r[0]) {
			panic("contrib DDL lacks " + r[0])
		}
		ddl = strings.Replace(ddl, r[0], r[1], 1)
	}
	return ddl
}

// AColumns is the insert column list of an A table (structure order).
func AColumns(t int) string { return columnsOf(AStructure[t]) }

func columnsOf(structure string) string {
	var cols []string
	depth := 0
	start := 0
	for i := 0; i <= len(structure); i++ {
		if i == len(structure) || structure[i] == ',' && depth == 0 {
			f := strings.TrimSpace(structure[start:i])
			name := f[:strings.IndexByte(f, ' ')]
			cols = append(cols, name)
			start = i + 1
			continue
		}
		switch structure[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
	}
	return strings.Join(cols, ", ")
}

// AInsert is the importer's statement for an A object.
func AInsert(db string, t int, objURL string) string {
	cols := AColumns(t)
	return fmt.Sprintf("INSERT INTO %s.otel_metrics_%s (%s) SELECT %s FROM s3('%s', '%s', '%s', 'Parquet', '%s')",
		db, Types[t], cols, cols, objURL, S3Key, S3Secret, AStructure[t])
}

// ---- B: series table + narrow points tables ----

const bEnv = mEnvelope
const bEx = "`Exemplars.TimeUnix` Array(DateTime), `Exemplars.Value` Array(Float64), `Exemplars.SpanId` Array(String), `Exemplars.TraceId` Array(String)"
const bHead = "MetricName String, ServiceName String, series_id UInt64, StartTimeUnix DateTime, TimeUnix DateTime"

// BStructure is the s3() structure of a B points object by type; index 5
// is the series object.
var BStructure = []string{
	bHead + ", Value Float64, Flags UInt32, " + bEx + ", " + bEnv,
	bHead + ", Value Float64, Flags UInt32, " + bEx + ", " + bEnv,
	bHead + ", Count UInt64, Sum Float64, BucketCounts Array(UInt64), Min Float64, Max Float64, Flags UInt32, " + bEx + ", " + bEnv,
	bHead + ", Count UInt64, Sum Float64, Scale Int32, ZeroCount UInt64, PositiveOffset Int32, PositiveBucketCounts Array(UInt64), NegativeOffset Int32, NegativeBucketCounts Array(UInt64), Min Float64, Max Float64, Flags UInt32, " + bEx + ", " + bEnv,
	bHead + ", Count UInt64, Sum Float64, `ValueAtQuantiles.Quantile` Array(Float64), `ValueAtQuantiles.Value` Array(Float64), Flags UInt32, " + bEnv,
	"series_id UInt64, MetricType UInt8, MetricName String, MetricDescription String, MetricUnit String, ServiceName String, " +
		"ResourceAttributesKeys Array(String), ResourceAttributesValues Array(String), ResourceSchemaUrl String, ScopeName String, ScopeVersion String, " +
		"ScopeAttributesKeys Array(String), ScopeAttributesValues Array(String), ScopeDroppedAttrCount UInt32, ScopeSchemaUrl String, " +
		"AttributesKeys Array(String), AttributesValues Array(String), AggregationTemporality Int32, IsMonotonic Bool, ExplicitBounds Array(Float64), FirstSeen DateTime, " + bEnv,
}

// Codecs are B's value/bucket codec choices (the defaults were picked by
// cmd/codecs; see README).
type Codecs struct{ Value, Bucket string }

// DefaultCodecs is what the README's numbers use.
var DefaultCodecs = Codecs{Value: "ZSTD(1)", Bucket: "ZSTD(1)"}

// BDDL returns B's statements.
func BDDL(db string, c Codecs) []string {
	b, err := os.ReadFile(filepath.Join(Dir(), "sql", "b_tables.sql"))
	if err != nil {
		panic(err)
	}
	s := strings.NewReplacer("{db}", db, "{valuecodec}", c.Value, "{bucketcodec}", c.Bucket).Replace(string(b))
	return statements(s)
}

func statements(s string) []string {
	var out []string
	var cur []string
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "--") {
			continue
		}
		cur = append(cur, l)
		if strings.HasSuffix(strings.TrimSpace(l), ";") {
			st := strings.TrimSuffix(strings.TrimSpace(strings.Join(cur, "\n")), ";")
			if st != "" {
				out = append(out, st)
			}
			cur = nil
		}
	}
	return out
}

// BInsert is the importer's statement for a B object (t 0..4 points, 5 series).
func BInsert(db string, t int, objURL string) string {
	src := fmt.Sprintf("s3('%s', '%s', '%s', 'Parquet', '%s')", objURL, S3Key, S3Secret, BStructure[t])
	if t == 5 {
		return fmt.Sprintf("INSERT INTO %s.otel_metrics_series (series_id, MetricType, MetricName, MetricDescription, MetricUnit, ServiceName, "+
			"ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, Attributes, "+
			"AggregationTemporality, IsMonotonic, ExplicitBounds, FirstSeen, LastSeen) "+
			"SELECT series_id, MetricType, MetricName, MetricDescription, MetricUnit, ServiceName, "+
			"mapFromArrays(ResourceAttributesKeys, ResourceAttributesValues), ResourceSchemaUrl, ScopeName, ScopeVersion, "+
			"mapFromArrays(ScopeAttributesKeys, ScopeAttributesValues), ScopeDroppedAttrCount, ScopeSchemaUrl, "+
			"mapFromArrays(AttributesKeys, AttributesValues), AggregationTemporality, IsMonotonic, ExplicitBounds, FirstSeen, FirstSeen FROM %s", db, src)
	}
	cols := columnsOf(BStructure[t])
	return fmt.Sprintf("INSERT INTO %s.otel_metrics_%s_points (%s) SELECT %s FROM %s", db, Types[t], cols, cols, src)
}

// ---- S3 ----

// S3 is a minio client for the spike's SeaweedFS.
func S3() *minio.Client {
	c, err := minio.New(S3Host, &minio.Options{Creds: credentials.NewStaticV4(S3Key, S3Secret, ""), Secure: false})
	if err != nil {
		panic(err)
	}
	return c
}

// ObjURL is the path-style URL of key, as ClickHouse's s3() reads it.
func ObjURL(key string) string { return "http://" + S3Host + "/" + Bucket + "/" + key }

// Put uploads b to key.
func Put(ctx context.Context, c *minio.Client, key string, b []byte) error {
	_, err := c.PutObject(ctx, Bucket, key, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

// DeletePrefix removes every object under prefix.
func DeletePrefix(ctx context.Context, c *minio.Client, prefix string) int {
	n := 0
	objs := c.ListObjects(ctx, Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	ch := make(chan minio.ObjectInfo)
	go func() {
		defer close(ch)
		for o := range objs {
			if o.Err == nil {
				ch <- o
				n++
			}
		}
	}()
	for range c.RemoveObjects(ctx, Bucket, ch, minio.RemoveObjectsOptions{}) {
	}
	return n
}

// ---- B-central: B's tables fed from unchanged A objects ----
//
// The edge keeps publishing the contrib-schema objects; central computes the
// series id itself. Each A object is inserted into a Null table with the A
// structure, and two materialized views split it into the points table and
// the series table (one series row per point; the AggregatingMergeTree
// collapses them). This is B without an edge change.

var bcPointCols = []string{
	"Value, Flags, `Exemplars.TimeUnix`, `Exemplars.Value`, `Exemplars.SpanId`, `Exemplars.TraceId`",
	"Value, Flags, `Exemplars.TimeUnix`, `Exemplars.Value`, `Exemplars.SpanId`, `Exemplars.TraceId`",
	"Count, Sum, BucketCounts, Min, Max, Flags, `Exemplars.TimeUnix`, `Exemplars.Value`, `Exemplars.SpanId`, `Exemplars.TraceId`",
	"Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts, NegativeOffset, NegativeBucketCounts, Min, Max, Flags, `Exemplars.TimeUnix`, `Exemplars.Value`, `Exemplars.SpanId`, `Exemplars.TraceId`",
	"Count, Sum, `ValueAtQuantiles.Quantile`, `ValueAtQuantiles.Value`, Flags",
}

var bcIdentity = []string{
	"", ", AggregationTemporality, IsMonotonic", ", AggregationTemporality, ExplicitBounds", ", AggregationTemporality", "",
}

// BcDDL returns the Null input tables and the two MVs per type (B's tables
// must exist in db already).
func BcDDL(db string) []string {
	var out []string
	for t, name := range Types {
		in := fmt.Sprintf("%s.otel_metrics_%s_in", db, name)
		out = append(out, fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = Null", in, AStructure[t]))
		id := "cityHash64(ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, MetricName, MetricDescription, MetricUnit, Attributes" + bcIdentity[t] + ")"
		out = append(out, fmt.Sprintf("CREATE MATERIALIZED VIEW %s.mv_points_%s TO %s.otel_metrics_%s_points AS SELECT MetricName, ServiceName, %s AS series_id, StartTimeUnix, TimeUnix, %s, %s FROM %s",
			db, name, db, name, id, bcPointCols[t], strings.Join(strings.Split(mEnvelopeCols, ","), ","), in))
		temp, mono, bounds := "0", "false", "CAST([], 'Array(Float64)')"
		switch t {
		case 1:
			temp, mono = "AggregationTemporality", "IsMonotonic"
		case 2:
			temp, bounds = "AggregationTemporality", "ExplicitBounds"
		case 3:
			temp = "AggregationTemporality"
		}
		out = append(out, fmt.Sprintf("CREATE MATERIALIZED VIEW %s.mv_series_%s TO %s.otel_metrics_series AS SELECT %s AS series_id, %d AS MetricType, MetricName, MetricDescription, MetricUnit, ServiceName, "+
			"ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, Attributes, "+
			"%s AS AggregationTemporality, %s AS IsMonotonic, %s AS ExplicitBounds, TimeUnix AS FirstSeen, TimeUnix AS LastSeen FROM %s",
			db, name, db, id, t, temp, mono, bounds, in))
	}
	return out
}

const mEnvelopeCols = "producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version"

// BcInsert is the importer's statement for an A object into B-central.
func BcInsert(db string, t int, objURL string) string {
	cols := AColumns(t)
	return fmt.Sprintf("INSERT INTO %s.otel_metrics_%s_in (%s) SELECT %s FROM s3('%s', '%s', '%s', 'Parquet', '%s')",
		db, Types[t], cols, cols, objURL, S3Key, S3Secret, AStructure[t])
}
