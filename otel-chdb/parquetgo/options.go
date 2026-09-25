package parquetgo

// Options tune the Parquet written. The zero value is not useful; start from
// DefaultOptions, which matches what ClickHouse 26.x writes by default
// (output_format_parquet_*): zstd, dictionary encoding, statistics, page
// indexes and bloom filters on every column, 1 MiB pages, one row group per
// batch.
type Options struct {
	// Compression: zstd, snappy, lz4, gzip, brotli or none.
	Compression string
	// CompressionLevel is codec-specific; 0 means the codec's default.
	CompressionLevel int
	Dictionary       bool
	Statistics       bool
	PageIndex        bool
	// BloomFilters writes a split-block bloom filter per column chunk, sized
	// for the batch's row count at BloomFPP.
	BloomFilters bool
	BloomFPP     float64
	DataPageSize int64
	// MaxRowGroupRows splits big batches into several row groups.
	MaxRowGroupRows int64
	// PlainFor lists leaf column paths written without a dictionary: the
	// ones that are nearly unique per row, where a dictionary only costs
	// hashing (ClickHouse tries a dictionary everywhere and falls back at
	// 1 MiB).
	PlainFor []string
	// Parallelism > 1 encodes and compresses columns on that many
	// goroutines (parquet-go engine only). ClickHouse does the same by
	// default (output_format_parquet_parallel_encoding).
	Parallelism int
	// DataPageV2 writes V2 data pages (arrow engine; parquet-go always
	// does). arrow-go v18.7 with V1 pages can end a page in the middle of a
	// row of a repeated column, and with a page index present ClickHouse's
	// reader then rejects the file (compare/pagesplit_test.go). V2 pages
	// always end at row boundaries. On by default.
	DataPageV2 bool
}

// HighCardinality are the columns that are unique, or nearly, per row.
var HighCardinality = []string{"Timestamp", "TraceId", "SpanId", "ParentSpanId", "row_ordinal",
	"Events.Timestamp.list.element"}

// DefaultOptions matches ClickHouse's Parquet output defaults.
func DefaultOptions() Options {
	return Options{Compression: "zstd", Dictionary: true, Statistics: true, PageIndex: true,
		BloomFilters: true, BloomFPP: 0.005, DataPageSize: 1 << 20, MaxRowGroupRows: 1_000_000,
		PlainFor: HighCardinality, DataPageV2: true}
}
