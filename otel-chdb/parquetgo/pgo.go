package parquetgo

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// PGEncoder is the same encoder on parquet-go/parquet-go instead of
// arrow-go. The walker writes level-annotated values straight into one
// buffer per Parquet leaf column (no Arrow arrays in between), and each
// column is handed to parquet-go's column writer a page's worth of rows at
// a time. Same schema, same Options.
type PGEncoder struct {
	opts    Options
	traces  *pgSignal
	logs    *pgSignal
	metrics [NumMetricTypes]*pgSignal
}

func NewPGEncoder(opts Options) *PGEncoder { return &PGEncoder{opts: opts} }

func (e *PGEncoder) Traces(dst io.Writer, td ptrace.Traces, env *Envelope) (int, error) {
	if e.traces == nil {
		s, err := newPGSignal(pgTraceCols, e.opts)
		if err != nil {
			return 0, err
		}
		e.traces = s
	}
	e.traces.reset()
	n := writeTraces(e.traces, td, env)
	return n, e.traces.flush(dst)
}

func (e *PGEncoder) Logs(dst io.Writer, ld plog.Logs, env *Envelope) (int, error) {
	if e.logs == nil {
		s, err := newPGSignal(pgLogCols, e.opts)
		if err != nil {
			return 0, err
		}
		e.logs = s
	}
	e.logs.reset()
	n := writeLogs(e.logs, ld, env)
	return n, e.logs.flush(dst)
}

type pgKind int

const (
	kStr pgKind = iota
	kTS
	kU8
	kU16
	kU32
	kU64
	kMap
	kListStr
	kListTS
	kListMap
	// metrics only
	kF64
	kI32
	kBool
	kDT // DateTime: TIMESTAMP(MILLIS) holding whole seconds
	kListU64
	kListF64
	kListDT
)

type pgCol struct {
	name string
	kind pgKind
}

var pgEnvelope = []pgCol{
	{"producer_id", kStr}, {"producer_epoch", kStr}, {"batch_id", kU64}, {"row_ordinal", kU32},
	{"received_at", kTS}, {"schema_version", kU16},
}

var pgTraceCols = append([]pgCol{
	{"Timestamp", kTS}, {"TraceId", kStr}, {"SpanId", kStr}, {"ParentSpanId", kStr}, {"TraceState", kStr},
	{"SpanName", kStr}, {"SpanKind", kStr}, {"ServiceName", kStr}, {"ResourceAttributes", kMap},
	{"ScopeName", kStr}, {"ScopeVersion", kStr}, {"SpanAttributes", kMap}, {"Duration", kU64},
	{"StatusCode", kStr}, {"StatusMessage", kStr},
	{"Events.Timestamp", kListTS}, {"Events.Name", kListStr}, {"Events.Attributes", kListMap},
	{"Links.TraceId", kListStr}, {"Links.SpanId", kListStr}, {"Links.TraceState", kListStr}, {"Links.Attributes", kListMap},
}, pgEnvelope...)

var pgLogCols = append([]pgCol{
	{"Timestamp", kTS}, {"TraceId", kStr}, {"SpanId", kStr}, {"TraceFlags", kU8}, {"SeverityText", kStr},
	{"SeverityNumber", kU8}, {"ServiceName", kStr}, {"Body", kStr}, {"ResourceSchemaUrl", kStr},
	{"ResourceAttributes", kMap}, {"ScopeSchemaUrl", kStr}, {"ScopeName", kStr}, {"ScopeVersion", kStr},
	{"ScopeAttributes", kMap}, {"LogAttributes", kMap}, {"EventName", kStr},
}, pgEnvelope...)

func pgCodec(name string, level int) (compress.Codec, error) {
	switch name {
	case "zstd", "":
		c := &zstd.Codec{Level: zstd.DefaultLevel}
		if level != 0 {
			c.Level = zstd.Level(level)
		}
		return c, nil
	case "snappy":
		return &snappy.Codec{}, nil
	case "lz4":
		return &lz4.Codec{}, nil
	case "gzip":
		return &gzip.Codec{}, nil
	case "brotli":
		return &brotli.Codec{}, nil
	case "none":
		return &uncompressed.Codec{}, nil
	}
	return nil, fmt.Errorf("unknown parquet compression %q", name)
}

// pgSignal holds one signal's schema, writer and per-leaf value buffers.
type pgSignal struct {
	cols     []pgCol
	leaf     [][2]int // walker column -> leaf column index (key, value for maps)
	vals     [][]parquet.Value
	pages    []int // row boundaries, as per-leaf value offsets, every pageRows rows
	offs     [][]int
	rows     int
	schema   *parquet.Schema
	w        *parquet.Writer
	parallel int
	opts     []parquet.WriterOption

	col   int
	inArr bool
	elem  int // element index inside the open list

	scratch []byte
	kvs     []attrKV // sortedAttrs' buffer
	arena   []byte   // hex ids and rendered values, referenced by vals until flush
	hex     [32]byte
}

const pgPageRows = 1024

func newPGSignal(cols []pgCol, o Options) (*pgSignal, error) {
	plain := map[string]bool{}
	for _, p := range o.PlainFor {
		plain[p] = true
	}
	leafNode := func(path string, n parquet.Node) parquet.Node {
		if o.Dictionary && !plain[path] {
			n = parquet.Encoded(n, &parquet.RLEDictionary)
		}
		return parquet.Required(n)
	}
	str := func(path string) parquet.Node { return leafNode(path, parquet.String()) }
	ts := func(path string) parquet.Node { return leafNode(path, parquet.Timestamp(parquet.Nanosecond)) }
	dt := func(path string) parquet.Node { return leafNode(path, parquet.Timestamp(parquet.Millisecond)) }
	f64 := func(path string) parquet.Node { return leafNode(path, parquet.Leaf(parquet.DoubleType)) }
	u64 := func(path string) parquet.Node { return leafNode(path, parquet.Uint(64)) }
	g := parquet.Group{}
	var blooms []parquet.BloomFilterColumn
	bloom := func(path ...string) {
		if o.BloomFilters {
			// ClickHouse's default, output_format_parquet_bloom_filter_bits_per_value.
			blooms = append(blooms, parquet.SplitBlockFilter(11, path...))
		}
	}
	for _, c := range cols {
		switch c.kind {
		case kStr:
			g[c.name] = str(c.name)
		case kTS:
			g[c.name] = ts(c.name)
		case kU8:
			g[c.name] = leafNode(c.name, parquet.Uint(8))
		case kU16:
			g[c.name] = leafNode(c.name, parquet.Uint(16))
		case kU32:
			g[c.name] = leafNode(c.name, parquet.Uint(32))
		case kU64:
			g[c.name] = leafNode(c.name, parquet.Uint(64))
		case kMap:
			g[c.name] = parquet.Required(parquet.Map(str(c.name+".key_value.key"), str(c.name+".key_value.value")))
		case kListStr:
			g[c.name] = parquet.Required(parquet.List(str(c.name + ".list.element")))
		case kListTS:
			g[c.name] = parquet.Required(parquet.List(ts(c.name + ".list.element")))
		case kListMap:
			p := c.name + ".list.element.key_value."
			g[c.name] = parquet.Required(parquet.List(parquet.Required(parquet.Map(str(p+"key"), str(p+"value")))))
		case kF64:
			g[c.name] = f64(c.name)
		case kI32:
			g[c.name] = leafNode(c.name, parquet.Leaf(parquet.Int32Type))
		case kBool:
			// No dictionary: RLE booleans are already a bit per value.
			g[c.name] = parquet.Required(parquet.Leaf(parquet.BooleanType))
		case kDT:
			g[c.name] = dt(c.name)
		case kListU64:
			g[c.name] = parquet.Required(parquet.List(u64(c.name + ".list.element")))
		case kListF64:
			g[c.name] = parquet.Required(parquet.List(f64(c.name + ".list.element")))
		case kListDT:
			g[c.name] = parquet.Required(parquet.List(dt(c.name + ".list.element")))
		}
		switch c.kind {
		case kMap:
			bloom(c.name, "key_value", "key")
			bloom(c.name, "key_value", "value")
		case kBool:
		case kListStr, kListTS, kListU64, kListF64, kListDT:
			bloom(c.name, "list", "element")
		case kListMap:
			bloom(c.name, "list", "element", "key_value", "key")
			bloom(c.name, "list", "element", "key_value", "value")
		default:
			bloom(c.name)
		}
	}
	order := make([]string, len(cols))
	for i, c := range cols {
		order[i] = c.name
	}
	schema := parquet.NewSchema("schema", orderedGroup{g, order})
	s := &pgSignal{cols: cols, schema: schema, leaf: make([][2]int, len(cols)), parallel: o.Parallelism}
	for i, c := range cols {
		var paths [][]string
		switch c.kind {
		case kMap:
			paths = [][]string{{c.name, "key_value", "key"}, {c.name, "key_value", "value"}}
		case kListStr, kListTS, kListU64, kListF64, kListDT:
			paths = [][]string{{c.name, "list", "element"}}
		case kListMap:
			paths = [][]string{{c.name, "list", "element", "key_value", "key"}, {c.name, "list", "element", "key_value", "value"}}
		default:
			paths = [][]string{{c.name}}
		}
		for j, p := range paths {
			l, ok := schema.Lookup(p...)
			if !ok {
				return nil, fmt.Errorf("no leaf %v", p)
			}
			s.leaf[i][j] = l.ColumnIndex
		}
	}
	s.vals = make([][]parquet.Value, len(schema.Columns()))
	codec, err := pgCodec(o.Compression, o.CompressionLevel)
	if err != nil {
		return nil, err
	}
	s.opts = []parquet.WriterOption{schema, parquet.Compression(codec),
		parquet.CreatedBy("parquetgo (parquet-go)", "v0.32.0", ""),
		parquet.DataPageStatistics(o.Statistics),
		parquet.PageBufferSize(int(o.DataPageSize)),
		parquet.MaxRowsPerRowGroup(o.MaxRowGroupRows),
	}
	if !o.PageIndex {
		s.opts = append(s.opts, parquet.ColumnIndexSizeLimit(func([]string) int { return 0 }))
	}
	if len(blooms) > 0 {
		s.opts = append(s.opts, parquet.BloomFilters(blooms...))
	}
	return s, nil
}

func (s *pgSignal) reset() {
	for i := range s.vals {
		s.vals[i] = s.vals[i][:0]
	}
	s.offs = s.offs[:0]
	s.rows = 0
	s.arena = s.arena[:0]
}

func (s *pgSignal) flush(dst io.Writer) error {
	// A new writer per file, not Writer.Reset: in parquet-go v0.32.0 Reset
	// clears the column paths the writer keeps (format.ColumnMetaData.Reset
	// does clear(PathInSchema) on a slice that aliases them), so every file
	// after the first would carry empty path_in_schema entries and differ
	// from a fresh writer's bytes for the same batch.
	s.w = parquet.NewWriter(dst, s.opts...)
	cws := s.w.ColumnWriters()
	// Page-sized runs of whole rows per column; the writer cuts a page when
	// its buffer passes DataPageSize.
	bounds := append(s.offs, nil)
	writeCol := func(leaf int) error {
		cw, start := cws[leaf], 0
		for _, b := range bounds {
			end := len(s.vals[leaf])
			if b != nil {
				end = b[leaf]
			}
			if end > start {
				if _, err := cw.WriteRowValues(s.vals[leaf][start:end]); err != nil {
					return err
				}
			}
			start = end
		}
		return nil
	}
	if s.parallel <= 1 {
		for leaf := range cws {
			if err := writeCol(leaf); err != nil {
				return err
			}
		}
		return s.w.Close()
	}
	// Columns are independent until the row group is flushed, so encode
	// and compress them on several goroutines (as ClickHouse's
	// output_format_parquet_parallel_encoding does), then close serially.
	var next atomic.Int64
	errs := make([]error, s.parallel)
	var wg sync.WaitGroup
	for g := 0; g < s.parallel; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				leaf := int(next.Add(1) - 1)
				if leaf >= len(cws) {
					return
				}
				if err := writeCol(leaf); err != nil {
					errs[g] = err
					return
				}
				// Flush the column's last partial page here too, so the
				// compression happens in parallel rather than in Close.
				if err := cws[leaf].Flush(); err != nil {
					errs[g] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	return s.w.Close()
}

func bytesOf(str string) []byte { return unsafe.Slice(unsafe.StringData(str), len(str)) }

// put appends v at the next position: a top-level scalar column, or the
// next element of the open list.
func (s *pgSignal) put(v parquet.Value) {
	if s.inArr {
		rep := 1
		if s.elem == 0 {
			rep = 0
		}
		s.elem++
		leaf := s.leaf[s.col][0]
		s.vals[leaf] = append(s.vals[leaf], v.Level(rep, 1, leaf))
		return
	}
	leaf := s.leaf[s.col][0]
	s.vals[leaf] = append(s.vals[leaf], v.Level(0, 0, leaf))
	s.col++
}

func (s *pgSignal) row() {
	if s.rows > 0 && s.rows%pgPageRows == 0 {
		b := make([]int, len(s.vals))
		for i := range s.vals {
			b[i] = len(s.vals[i])
		}
		s.offs = append(s.offs, b)
	}
	s.col = 0
}
func (s *pgSignal) endRow() { s.rows++ }

func (s *pgSignal) arr(n int) {
	if n == 0 {
		// An empty list: one null at definition level 0 in each leaf.
		for j, leaf := range s.leaf[s.col] {
			if j == 1 && s.cols[s.col].kind != kListMap {
				break
			}
			s.vals[leaf] = append(s.vals[leaf], parquet.Value{}.Level(0, 0, leaf))
		}
		s.col++
		s.inArr = false
		s.elem = -1 // swallow: no values follow
		return
	}
	s.inArr = true
	s.elem = 0
}

func (s *pgSignal) end() {
	if s.elem != -1 {
		s.col++
	}
	s.inArr = false
	s.elem = 0
}

func (s *pgSignal) ts(n uint64) { s.put(parquet.Int64Value(int64(n))) }
func (s *pgSignal) str(v string) {
	s.put(parquet.ByteArrayValue(bytesOf(v)))
}
func (s *pgSignal) u8(v uint8)   { s.put(parquet.Int32Value(int32(v))) }
func (s *pgSignal) u16(v uint16) { s.put(parquet.Int32Value(int32(v))) }
func (s *pgSignal) u32(v uint32) { s.put(parquet.Int32Value(int32(v))) }
func (s *pgSignal) u64(v uint64) { s.put(parquet.Int64Value(int64(v))) }

// stash copies b into the arena and returns the copy. The arena may be
// reallocated as it grows; values already appended keep pointing into the
// old backing array, which stays alive until they are dropped.
func (s *pgSignal) stash(b []byte) []byte {
	if cap(s.arena)-len(s.arena) < len(b) {
		s.arena = make([]byte, 0, max(2*cap(s.arena), len(b), 64<<10))
	}
	o := len(s.arena)
	s.arena = append(s.arena, b...)
	return s.arena[o:len(s.arena):len(s.arena)]
}

func (s *pgSignal) hexID(id []byte) {
	if slices.ContainsFunc(id, func(b byte) bool { return b != 0 }) {
		n := hex.Encode(s.hex[:], id)
		s.put(parquet.ByteArrayValue(s.stash(s.hex[:n])))
		return
	}
	s.put(parquet.ByteArrayValue(nil))
}

func (s *pgSignal) traceID(id pcommon.TraceID) { s.hexID(id[:]) }
func (s *pgSignal) spanID(id pcommon.SpanID)   { s.hexID(id[:]) }

func (s *pgSignal) attrs(m pcommon.Map) {
	mp, ok := s.mapStart(m.Len())
	if !ok {
		return
	}
	m.Range(func(k string, v pcommon.Value) bool {
		s.mapEntry(&mp, k, v)
		return true
	})
}

// mapPos is where one map value's entries go: a top-level map column or the
// next element of a list of maps.
type mapPos struct {
	kl, vl, rep, def int
}

// mapStart opens a map of n entries at the current position; for n == 0 it
// writes the empty map and returns false.
func (s *pgSignal) mapStart(n int) (mapPos, bool) {
	mp := mapPos{kl: s.leaf[s.col][0], vl: s.leaf[s.col][1]}
	// Levels: a top-level map has max rep 1 / def 1; a map inside a list
	// element has max rep 2 / def 2.
	if s.inArr {
		mp.def = 2
		if s.elem > 0 {
			mp.rep = 1
		}
		s.elem++
	} else {
		mp.def = 1
		s.col++
	}
	if n == 0 {
		s.vals[mp.kl] = append(s.vals[mp.kl], parquet.Value{}.Level(mp.rep, mp.def-1, mp.kl))
		s.vals[mp.vl] = append(s.vals[mp.vl], parquet.Value{}.Level(mp.rep, mp.def-1, mp.vl))
		return mp, false
	}
	return mp, true
}

// mapEntry appends one entry; after the first, entries repeat at the map's
// own level.
func (s *pgSignal) mapEntry(mp *mapPos, k string, v pcommon.Value) {
	s.vals[mp.kl] = append(s.vals[mp.kl], parquet.ByteArrayValue(bytesOf(k)).Level(mp.rep, mp.def, mp.kl))
	var vb []byte
	if v.Type() == pcommon.ValueTypeStr {
		vb = bytesOf(v.Str())
	} else {
		s.scratch = valueString(s.scratch[:0], v)
		vb = s.stash(s.scratch)
	}
	s.vals[mp.vl] = append(s.vals[mp.vl], parquet.ByteArrayValue(vb).Level(mp.rep, mp.def, mp.vl))
	mp.rep = mp.def
}

// orderedGroup is a parquet.Group whose fields keep the given order.
// parquet.Group sorts its fields by name, which would put the columns in a
// different order from the chdb exporter's files; ClickHouse and Spark read
// by name, but SELECT * and positional INSERTs would see the difference.
type orderedGroup struct {
	parquet.Group
	order []string
}

func (g orderedGroup) Fields() []parquet.Field {
	f := make([]parquet.Field, len(g.order))
	for i, name := range g.order {
		f[i] = &orderedField{Node: g.Group[name], name: name}
	}
	return f
}

type orderedField struct {
	parquet.Node
	name string
}

func (f *orderedField) Name() string { return f.name }

// Value maps a Go value onto the field; unused, since rows are written
// column by column as parquet.Values.
func (f *orderedField) Value(base reflect.Value) reflect.Value { return reflect.Value{} }
