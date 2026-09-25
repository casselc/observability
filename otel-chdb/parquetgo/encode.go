//go:build !noarrow

package parquetgo

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func codec(name string) (compress.Compression, error) {
	switch name {
	case "zstd", "":
		return compress.Codecs.Zstd, nil
	case "snappy":
		return compress.Codecs.Snappy, nil
	case "lz4":
		return compress.Codecs.Lz4Raw, nil
	case "gzip":
		return compress.Codecs.Gzip, nil
	case "brotli":
		return compress.Codecs.Brotli, nil
	case "none":
		return compress.Codecs.Uncompressed, nil
	}
	return 0, fmt.Errorf("unknown parquet compression %q", name)
}

func (o Options) props(rows int, mem memory.Allocator) (*parquet.WriterProperties, error) {
	c, err := codec(o.Compression)
	if err != nil {
		return nil, err
	}
	level := compress.DefaultCompressionLevel
	if o.CompressionLevel != 0 {
		level = o.CompressionLevel
	}
	opts := []parquet.WriterProperty{
		parquet.WithAllocator(mem),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCreatedBy("parquetgo (arrow-go v18)"),
		parquet.WithCompression(c),
		parquet.WithCompressionLevel(level),
		parquet.WithDictionaryDefault(o.Dictionary),
		parquet.WithStats(o.Statistics),
		parquet.WithPageIndexEnabled(o.PageIndex),
		parquet.WithDataPageSize(o.DataPageSize),
		parquet.WithMaxRowGroupLength(o.MaxRowGroupRows),
	}
	if o.DataPageV2 {
		opts = append(opts, parquet.WithDataPageVersion(parquet.DataPageV2))
	}
	for _, p := range o.PlainFor {
		opts = append(opts, parquet.WithDictionaryFor(p, false))
	}
	if o.BloomFilters {
		// Adaptive: candidate filters from the size a row group of unique
		// values needs at BloomFPP down by halves to 32 bytes; the smallest
		// that still meets the FPP for the distinct values seen is written.
		// ClickHouse sizes its filters from the distinct count the same way
		// (output_format_parquet_bloom_filter_bits_per_value).
		ndv := float64(min(int64(max(rows, 1)), o.MaxRowGroupRows))
		maxBytes := int64(-ndv * math.Log(o.BloomFPP) / (math.Ln2 * math.Ln2) / 8)
		candidates := max(bits.Len64(uint64(maxBytes))-5, 1)
		opts = append(opts, parquet.WithBloomFilterEnabled(true), parquet.WithBloomFilterFPP(o.BloomFPP),
			parquet.WithAdaptiveBloomFilterEnabled(true), parquet.WithMaxBloomFilterBytes(maxBytes),
			parquet.WithBloomFilterCandidates(candidates))
	}
	return parquet.NewWriterProperties(opts...), nil
}

func init() {
	newArrowEncoder = func(o Options) BatchEncoder { return NewEncoder(o, nil) }
}

// Encoder turns pdata into Parquet. It keeps its Arrow builders between
// batches; it is not safe for concurrent use (pool them).
type Encoder struct {
	opts   Options
	mem    memory.Allocator
	traces *arrowRows
	logs   *arrowRows
}

// NewEncoder returns an encoder. mem nil means a pooling allocator that
// reuses the encoder's buffers from batch to batch.
func NewEncoder(opts Options, mem memory.Allocator) *Encoder {
	if mem == nil {
		mem = &poolAllocator{}
	}
	return &Encoder{opts: opts, mem: mem}
}

// Traces writes td as one Parquet file to dst and returns the row count.
// env may be nil (no envelope values are then written, so it must not be:
// the schema always carries the envelope).
func (e *Encoder) Traces(dst io.Writer, td ptrace.Traces, env *Envelope) (int, error) {
	if e.traces == nil {
		e.traces = newArrowRows(TracesSchema, e.mem)
	}
	e.traces.reserve(td.SpanCount())
	n := writeTraces(e.traces, td, env)
	return n, e.flush(dst, e.traces, n)
}

// Logs writes ld as one Parquet file to dst and returns the row count.
func (e *Encoder) Logs(dst io.Writer, ld plog.Logs, env *Envelope) (int, error) {
	if e.logs == nil {
		e.logs = newArrowRows(LogsSchema, e.mem)
	}
	e.logs.reserve(ld.LogRecordCount())
	n := writeLogs(e.logs, ld, env)
	return n, e.flush(dst, e.logs, n)
}

func (e *Encoder) flush(dst io.Writer, w *arrowRows, rows int) error {
	rec := w.rb.NewRecordBatch()
	defer rec.Release()
	props, err := e.opts.props(rows, e.mem)
	if err != nil {
		return err
	}
	fw, err := pqarrow.NewFileWriter(rec.Schema(), dst, props, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(e.mem)))
	if err != nil {
		return err
	}
	if err := fw.Write(rec); err != nil {
		fw.Close()
		return err
	}
	return fw.Close()
}

// arrowRows is a rowWriter over a RecordBuilder: each call appends to the
// next top-level column, or, between arr() and end(), to the open list's
// value builder.
type arrowRows struct {
	rb      *array.RecordBuilder
	b       []array.Builder
	col     int
	list    array.Builder
	scratch []byte
	hex     [32]byte
}

func newArrowRows(s *arrow.Schema, mem memory.Allocator) *arrowRows {
	rb := array.NewRecordBuilder(mem, s)
	return &arrowRows{rb: rb, b: rb.Fields()}
}

func (w *arrowRows) reserve(n int) { w.rb.Reserve(n) }

func (w *arrowRows) next() array.Builder {
	if w.list != nil {
		return w.list
	}
	b := w.b[w.col]
	w.col++
	return b
}

func (w *arrowRows) row()    { w.col = 0 }
func (w *arrowRows) endRow() {}

func (w *arrowRows) arr(n int) {
	lb := w.b[w.col].(*array.ListBuilder)
	w.col++
	lb.Append(true)
	w.list = lb.ValueBuilder()
}

func (w *arrowRows) end() { w.list = nil }

func (w *arrowRows) ts(nanos uint64) {
	w.next().(*array.TimestampBuilder).Append(arrow.Timestamp(int64(nanos)))
}
func (w *arrowRows) str(s string) { w.next().(*array.StringBuilder).Append(s) }
func (w *arrowRows) u8(v uint8)   { w.next().(*array.Uint8Builder).Append(v) }
func (w *arrowRows) u16(v uint16) { w.next().(*array.Uint16Builder).Append(v) }
func (w *arrowRows) u32(v uint32) { w.next().(*array.Uint32Builder).Append(v) }
func (w *arrowRows) u64(v uint64) { w.next().(*array.Uint64Builder).Append(v) }

// hexID writes id as lowercase hex, or "" when every byte is zero, matching
// traceutil.TraceIDToHexOrEmptyString.
func (w *arrowRows) hexID(id []byte) {
	sb := w.next().(*array.StringBuilder)
	for _, b := range id {
		if b != 0 {
			n := hex.Encode(w.hex[:], id)
			sb.BinaryBuilder.Append(w.hex[:n])
			return
		}
	}
	sb.BinaryBuilder.Append(nil)
}

func (w *arrowRows) traceID(id pcommon.TraceID) { w.hexID(id[:]) }
func (w *arrowRows) spanID(id pcommon.SpanID)   { w.hexID(id[:]) }

func (w *arrowRows) attrs(m pcommon.Map) {
	mb := w.next().(*array.MapBuilder)
	mb.Append(true)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.StringBuilder)
	m.Range(func(k string, v pcommon.Value) bool {
		kb.Append(k)
		if v.Type() == pcommon.ValueTypeStr {
			ib.Append(v.Str())
			return true
		}
		w.scratch = valueString(w.scratch[:0], v)
		ib.BinaryBuilder.Append(w.scratch)
		return true
	})
}
