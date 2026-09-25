package otap

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Payload types, shortened.
const (
	ResourceAttrs  = pb.ArrowPayloadType_RESOURCE_ATTRS
	ScopeAttrs     = pb.ArrowPayloadType_SCOPE_ATTRS
	Logs           = pb.ArrowPayloadType_LOGS
	LogAttrs       = pb.ArrowPayloadType_LOG_ATTRS
	Spans          = pb.ArrowPayloadType_SPANS
	SpanAttrs      = pb.ArrowPayloadType_SPAN_ATTRS
	SpanEvents     = pb.ArrowPayloadType_SPAN_EVENTS
	SpanLinks      = pb.ArrowPayloadType_SPAN_LINKS
	SpanEventAttrs = pb.ArrowPayloadType_SPAN_EVENT_ATTRS
	SpanLinkAttrs  = pb.ArrowPayloadType_SPAN_LINK_ATTRS
)

// TracePayloads and LogPayloads are the tables of each signal, root first.
var (
	TracePayloads = []pb.ArrowPayloadType{Spans, ResourceAttrs, ScopeAttrs, SpanAttrs, SpanEvents, SpanEventAttrs, SpanLinks, SpanLinkAttrs}
	LogPayloads   = []pb.ArrowPayloadType{Logs, ResourceAttrs, ScopeAttrs, LogAttrs}
)

// TableName is the lower-case payload type, as the Rust parquet exporter
// names its directories.
func TableName(t pb.ArrowPayloadType) string { return lower(t.String()) }

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

// Encode turns pdata into one self-contained OTAP BatchArrowRecords with a
// fresh producer, so the IPC streams inside it carry their schemas and
// dictionaries: what one object on S3 needs. (A long-lived gRPC stream
// would reuse the producer and send schemas once.)
func EncodeTraces(td ptrace.Traces) (*pb.BatchArrowRecords, error) {
	p := arrow_record.NewProducer()
	defer p.Close()
	return p.BatchArrowRecordsFromTraces(td)
}

func EncodeLogs(ld plog.Logs) (*pb.BatchArrowRecords, error) {
	p := arrow_record.NewProducer()
	defer p.Close()
	return p.BatchArrowRecordsFromLogs(ld)
}

// Batch is one OTAP batch as decoded Arrow tables, with its transport
// optimised ids (delta and quasi-delta) resolved to absolute, batch-local
// ids. The tables themselves are left as the producer built them.
type Batch struct {
	Traces bool
	T      map[pb.ArrowPayloadType]arrow.Record
	Root   arrow.Record

	RootID  []int64  // root row's id; -1 when null (no children)
	ResID   []uint16 // root row's resource.id
	ScopeID []uint16 // root row's scope.id
	// Parent holds, per child table, each row's absolute parent id.
	Parent map[pb.ArrowPayloadType][]uint32
	// ChildID holds the absolute id of event and link rows (-1 when null).
	ChildID map[pb.ArrowPayloadType][]int64

	release func()
}

func (b *Batch) Release() {
	if b.release != nil {
		b.release()
	}
}

// Decode reads a BatchArrowRecords with a fresh consumer (each object is a
// self-contained set of IPC streams) and resolves the ids.
func Decode(bar *pb.BatchArrowRecords) (*Batch, error) {
	c := arrow_record.NewConsumer()
	recs, err := c.Consume(bar)
	if err != nil {
		return nil, err
	}
	b := &Batch{T: map[pb.ArrowPayloadType]arrow.Record{}, Parent: map[pb.ArrowPayloadType][]uint32{},
		ChildID: map[pb.ArrowPayloadType][]int64{}}
	b.release = func() {
		for _, r := range recs {
			r.Record().Release()
		}
		c.Close()
	}
	for _, r := range recs {
		b.T[r.PayloadType()] = r.Record()
	}
	switch {
	case b.T[Spans] != nil:
		b.Traces, b.Root = true, b.T[Spans]
	case b.T[Logs] != nil:
		b.Root = b.T[Logs]
	default:
		b.Release()
		return nil, fmt.Errorf("otap: batch has neither SPANS nor LOGS")
	}
	b.decodeRoot()
	for _, t := range []pb.ArrowPayloadType{ResourceAttrs, ScopeAttrs, LogAttrs, SpanAttrs, SpanEventAttrs, SpanLinkAttrs} {
		if r := b.T[t]; r != nil {
			b.Parent[t] = decodeAttrParents(r)
		}
	}
	if r := b.T[SpanEvents]; r != nil {
		b.Parent[SpanEvents], b.ChildID[SpanEvents] = decodeChildren(r, func(i int) (string, bool) { return strAt(col(r, "name"), i) })
	}
	if r := b.T[SpanLinks]; r != nil {
		tid := col(r, "trace_id")
		b.Parent[SpanLinks], b.ChildID[SpanLinks] = decodeChildren(r, func(i int) (string, bool) {
			v, ok := bytesAt(tid, i)
			return string(v), ok
		})
	}
	return b, nil
}

// decodeRoot resolves the root table's id (delta over non-null values), and
// resource.id and scope.id (delta, a null counting as 0), as the Go
// consumer does (traces/otlp/traces.go, logs/otlp/logs.go).
func (b *Batch) decodeRoot() {
	r := b.Root
	n := rows(r)
	id, res, scope := col(r, "id"), child(col(r, "resource"), "id"), child(col(r, "scope"), "id")
	b.RootID, b.ResID, b.ScopeID = make([]int64, n), make([]uint16, n), make([]uint16, n)
	var cur, rid, sid uint64
	for i := 0; i < n; i++ {
		if v, ok := uintAt(id, i); ok {
			cur += v
			b.RootID[i] = int64(uint16(cur))
		} else {
			b.RootID[i] = -1
		}
		d, _ := uintAt(res, i)
		rid += d
		b.ResID[i] = uint16(rid)
		d, _ = uintAt(scope, i)
		sid += d
		b.ScopeID[i] = uint16(sid)
	}
}

// decodeChildren resolves events and links: id is delta over non-null
// values; parent_id is quasi-delta, a delta while the equality column
// (event name, link trace id) repeats, absolute otherwise.
func decodeChildren(r arrow.Record, eq func(int) (string, bool)) ([]uint32, []int64) {
	n := rows(r)
	id, pid := col(r, "id"), col(r, "parent_id")
	parents, ids := make([]uint32, n), make([]int64, n)
	var cur uint64
	var prevParent uint32
	var prevKey string
	havePrev := false
	for i := 0; i < n; i++ {
		if v, ok := uintAt(id, i); ok {
			cur += v
			ids[i] = int64(uint32(cur))
		} else {
			ids[i] = -1
		}
		p, _ := uintAt(pid, i)
		k, _ := eq(i)
		if havePrev && k == prevKey {
			prevParent += uint32(p)
		} else {
			prevParent = uint32(p)
		}
		prevKey, havePrev = k, true
		parents[i] = prevParent
	}
	return parents, ids
}

// decodeAttrParents resolves an attribute table's parent_id: a delta when
// the key and the value (type included) equal the previous row's, absolute
// otherwise. Empty, map and slice values are never equal, matching
// otlp.AttrsParentIDDecoder and arrow.Equal in the Go library.
func decodeAttrParents(r arrow.Record) []uint32 {
	n := rows(r)
	pid, key, typ := col(r, "parent_id"), col(r, "key"), col(r, "type")
	vals := attrValueCols(r)
	out := make([]uint32, n)
	var prev uint32
	for i := 0; i < n; i++ {
		p, _ := uintAt(pid, i)
		if i > 0 && attrEqual(key, typ, vals, i-1, i) {
			prev += uint32(p)
		} else {
			prev = uint32(p)
		}
		out[i] = prev
	}
	return out
}

type attrCols struct{ str, i64, f64, boolean, bin, ser arrow.Array }

func attrValueCols(r arrow.Record) attrCols {
	return attrCols{col(r, "str"), col(r, "int"), col(r, "double"), col(r, "bool"), col(r, "bytes"), col(r, "ser")}
}

// Attribute value types (pcommon.ValueType numbering, as OTAP uses).
const (
	tEmpty  = 0
	tStr    = 1
	tInt    = 2
	tDouble = 3
	tBool   = 4
	tMap    = 5
	tSlice  = 6
	tBytes  = 7
)

func attrEqual(key, typ arrow.Array, v attrCols, a, b int) bool {
	ka, _ := strAt(key, a)
	kb, _ := strAt(key, b)
	if ka != kb {
		return false
	}
	ta, _ := uintAt(typ, a)
	tb, _ := uintAt(typ, b)
	if ta != tb {
		return false
	}
	switch ta {
	case tStr:
		x, okx := strAt(v.str, a)
		y, oky := strAt(v.str, b)
		return okx == oky && x == y
	case tInt:
		x, okx := intAt(v.i64, a)
		y, oky := intAt(v.i64, b)
		return okx == oky && x == y
	case tDouble:
		x, _ := f64At(v.f64, a)
		y, _ := f64At(v.f64, b)
		return x == y
	case tBool:
		x, _ := boolAt(v.boolean, a)
		y, _ := boolAt(v.boolean, b)
		return x == y
	case tBytes:
		x, _ := bytesAt(v.bin, a)
		y, _ := bytesAt(v.bin, b)
		return bytes.Equal(x, y)
	}
	return false
}
