package otap

import (
	"io"
	"math"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

// WriteParquet writes rec as one Parquet file (one row group) with
// parquetgo's options: the same writer properties parquetgo's arrow engine
// uses (copied from parquetgo/encode.go, where they are unexported), so
// object sizes compare like for like. o.PlainFor names columns of the flat
// schema; they are simply absent from star tables.
func WriteParquet(dst io.Writer, rec arrow.Record, o parquetgo.Options, mem memory.Allocator) error {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	opts := []parquet.WriterProperty{
		parquet.WithAllocator(mem),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCreatedBy("otap spike (arrow-go v18)"),
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(compress.DefaultCompressionLevel),
		parquet.WithDictionaryDefault(o.Dictionary),
		parquet.WithStats(o.Statistics),
		parquet.WithPageIndexEnabled(o.PageIndex),
		parquet.WithDataPageSize(o.DataPageSize),
		parquet.WithMaxRowGroupLength(max(o.MaxRowGroupRows, rec.NumRows()+1)),
		parquet.WithDataPageVersion(parquet.DataPageV2),
	}
	if o.Compression == "none" {
		opts = append(opts, parquet.WithCompression(compress.Codecs.Uncompressed))
	}
	for _, p := range o.PlainFor {
		opts = append(opts, parquet.WithDictionaryFor(p, false))
	}
	if o.BloomFilters {
		ndv := float64(max(rec.NumRows(), 1))
		maxBytes := int64(-ndv * math.Log(o.BloomFPP) / (math.Ln2 * math.Ln2) / 8)
		candidates := max(bits.Len64(uint64(maxBytes))-5, 1)
		opts = append(opts, parquet.WithBloomFilterEnabled(true), parquet.WithBloomFilterFPP(o.BloomFPP),
			parquet.WithAdaptiveBloomFilterEnabled(true), parquet.WithMaxBloomFilterBytes(maxBytes),
			parquet.WithBloomFilterCandidates(candidates))
	}
	fw, err := pqarrow.NewFileWriter(rec.Schema(), dst, parquet.NewWriterProperties(opts...),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	if err != nil {
		return err
	}
	if err := fw.Write(rec); err != nil {
		fw.Close()
		return err
	}
	return fw.Close()
}

// WriteArrowFile writes rec as an Arrow IPC *file* (random-access format,
// what ClickHouse's `Arrow` input format reads), with zstd-compressed
// buffers unless compression is "none".
func WriteArrowFile(dst io.Writer, rec arrow.Record, compression string, mem memory.Allocator) error {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	opts := []ipc.Option{ipc.WithSchema(rec.Schema()), ipc.WithAllocator(mem)}
	switch compression {
	case "zstd", "":
		opts = append(opts, ipc.WithZstd())
	case "lz4":
		opts = append(opts, ipc.WithLZ4())
	}
	w, err := ipc.NewFileWriter(dst, opts...)
	if err != nil {
		return err
	}
	if err := w.Write(rec); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}
