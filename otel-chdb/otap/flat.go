package otap

import (
	"encoding/base64"
	"encoding/hex"
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Flatten denormalises an OTAP batch into the ClickStack row shape: the
// exact Arrow schema parquetgo publishes (parquetgo.TracesSchema /
// LogsSchema, the clickhouse exporter's otel_traces / otel_logs columns
// plus the envelope). It works on the Arrow tables directly: attribute rows
// are bucketed by parent id and emitted as Map entries, events and links as
// the Nested arrays. This is the shape the Rust otap-dataflow ClickHouse
// exporter builds before its ArrowStream insert
// (contrib-nodes/src/exporters/clickhouse_exporter/transform), here in Go.
//
// Rows come out in OTAP row order, and Map entries in attribute-table order
// (the producer sorts attribute rows by type, key, value), so both differ
// from the pdata order the Go exporters use; the values do not.
func Flatten(b *Batch, env *parquetgo.Envelope, mem memory.Allocator) arrow.Record {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	f := &flattener{b: b, env: env}
	if b.Traces {
		return f.traces(mem)
	}
	return f.logs(mem)
}

type flattener struct {
	b       *Batch
	env     *parquetgo.Envelope
	scratch []byte
	hexbuf  [32]byte
}

// groups buckets a child table's rows by parent id, keeping table order
// inside each bucket (a counting sort).
type groups struct {
	off  []int32
	rows []int32
}

func newGroups(parents []uint32) groups {
	var max uint32
	for _, p := range parents {
		if p > max {
			max = p
		}
	}
	g := groups{off: make([]int32, int(max)+2), rows: make([]int32, len(parents))}
	if len(parents) == 0 {
		g.off = g.off[:1]
		return g
	}
	for _, p := range parents {
		g.off[p+1]++
	}
	for i := 1; i < len(g.off); i++ {
		g.off[i] += g.off[i-1]
	}
	cur := append([]int32(nil), g.off[:len(g.off)-1]...)
	for i, p := range parents {
		g.rows[cur[p]] = int32(i)
		cur[p]++
	}
	return g
}

func (g groups) of(id int64) []int32 {
	if id < 0 || id+1 >= int64(len(g.off)) {
		return nil
	}
	return g.rows[g.off[id]:g.off[id+1]]
}

// attrTable is one attribute payload with its rows bucketed by parent.
type attrTable struct {
	key, typ arrow.Array
	v        attrCols
	g        groups
}

func (f *flattener) attrTable(t pb.ArrowPayloadType) *attrTable {
	r := f.b.T[t]
	if r == nil {
		return &attrTable{g: newGroups(nil)}
	}
	return &attrTable{key: col(r, "key"), typ: col(r, "type"), v: attrValueCols(r), g: newGroups(f.b.Parent[t])}
}

// render appends attribute row i's value as pcommon.Value.AsString renders
// it (what the clickhouse exporter stores), matching parquetgo's valueString.
func render(dst []byte, typ arrow.Array, v attrCols, i int) []byte {
	t, _ := uintAt(typ, i)
	switch t {
	case tStr:
		s, _ := strAt(v.str, i)
		return append(dst, s...)
	case tInt:
		n, _ := intAt(v.i64, i)
		return strconv.AppendInt(dst, n, 10)
	case tDouble:
		x, _ := f64At(v.f64, i)
		if a := math.Abs(x); a == 0 || (a >= 1e-6 && a < 1e21) {
			return strconv.AppendFloat(dst, x, 'f', -1, 64)
		}
		return append(dst, pcommon.NewValueDouble(x).AsString()...)
	case tBool:
		x, _ := boolAt(v.boolean, i)
		return strconv.AppendBool(dst, x)
	case tBytes:
		x, _ := bytesAt(v.bin, i)
		return base64.StdEncoding.AppendEncode(dst, x)
	case tMap, tSlice:
		x, _ := bytesAt(v.ser, i)
		pv := pcommon.NewValueEmpty()
		if decodeCBOR(x, pv) != nil {
			return dst
		}
		return append(dst, pv.AsString()...)
	}
	return dst
}

func (f *flattener) mapOf(mb *array.MapBuilder, a *attrTable, id int64) {
	mb.Append(true)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	vb := mb.ItemBuilder().(*array.StringBuilder)
	for _, r := range a.g.of(id) {
		k, _ := strAt(a.key, int(r))
		kb.Append(k)
		f.scratch = render(f.scratch[:0], a.typ, a.v, int(r))
		vb.BinaryBuilder.Append(f.scratch)
	}
}

func (f *flattener) serviceName(a *attrTable, id int64) string {
	for _, r := range a.g.of(id) {
		if k, _ := strAt(a.key, int(r)); k == "service.name" {
			return string(render(nil, a.typ, a.v, int(r)))
		}
	}
	return ""
}

// hexID appends lowercase hex, or "" for a missing or all-zero id
// (traceutil.TraceIDToHexOrEmptyString).
func (f *flattener) hexID(sb *array.StringBuilder, id []byte) {
	for _, c := range id {
		if c != 0 {
			n := hex.Encode(f.hexbuf[:], id)
			sb.BinaryBuilder.Append(f.hexbuf[:n])
			return
		}
	}
	sb.BinaryBuilder.Append(nil)
}

func (f *flattener) envelope(bs []array.Builder, row int, ts uint64) {
	e := f.env
	if e.MinTS == 0 || ts < e.MinTS {
		e.MinTS = ts
	}
	if ts > e.MaxTS {
		e.MaxTS = ts
	}
	bs[0].(*array.StringBuilder).Append(e.Producer)
	bs[1].(*array.StringBuilder).Append(e.Epoch)
	bs[2].(*array.Uint64Builder).Append(e.Batch)
	bs[3].(*array.Uint32Builder).Append(uint32(row))
	bs[4].(*array.TimestampBuilder).Append(arrow.Timestamp(e.Received))
	bs[5].(*array.Uint16Builder).Append(e.Schema)
}

func (f *flattener) traces(mem memory.Allocator) arrow.Record {
	b, r := f.b, f.b.Root
	n := rows(r)
	rb := array.NewRecordBuilder(mem, parquetgo.TracesSchema)
	defer rb.Release()
	rb.Reserve(n)
	fb := rb.Fields()
	str := func(i int) *array.StringBuilder { return fb[i].(*array.StringBuilder) }

	res, spanAttrs := f.attrTable(ResourceAttrs), f.attrTable(SpanAttrs)
	evAttrs, lkAttrs := f.attrTable(SpanEventAttrs), f.attrTable(SpanLinkAttrs)
	ev, lk := b.T[SpanEvents], b.T[SpanLinks]
	evG, lkG := newGroups(b.Parent[SpanEvents]), newGroups(b.Parent[SpanLinks])
	evTime, evName := col(ev, "time_unix_nano"), col(ev, "name")
	lkTrace, lkSpan, lkState := col(lk, "trace_id"), col(lk, "span_id"), col(lk, "trace_state")

	start, dur := col(r, "start_time_unix_nano"), col(r, "duration_time_unix_nano")
	traceID, spanID, parentID := col(r, "trace_id"), col(r, "span_id"), col(r, "parent_span_id")
	state, name, kind := col(r, "trace_state"), col(r, "name"), col(r, "kind")
	scopeCol := col(r, "scope")
	scopeName, scopeVersion := child(scopeCol, "name"), child(scopeCol, "version")
	status := col(r, "status")
	code, msg := child(status, "code"), child(status, "status_message")

	svcCache := map[uint16]string{}
	for i := 0; i < n; i++ {
		ts, _ := uintAt(start, i)
		fb[0].(*array.TimestampBuilder).Append(arrow.Timestamp(ts))
		v, _ := bytesAt(traceID, i)
		f.hexID(str(1), v)
		v, _ = bytesAt(spanID, i)
		f.hexID(str(2), v)
		v, _ = bytesAt(parentID, i)
		f.hexID(str(3), v)
		s, _ := strAt(state, i)
		str(4).Append(s)
		s, _ = strAt(name, i)
		str(5).Append(s)
		k, _ := intAt(kind, i)
		str(6).Append(ptrace.SpanKind(k).String())
		rid := b.ResID[i]
		svc, ok := svcCache[rid]
		if !ok {
			svc = f.serviceName(res, int64(rid))
			svcCache[rid] = svc
		}
		str(7).Append(svc)
		f.mapOf(fb[8].(*array.MapBuilder), res, int64(rid))
		s, _ = strAt(scopeName, i)
		str(9).Append(s)
		s, _ = strAt(scopeVersion, i)
		str(10).Append(s)
		f.mapOf(fb[11].(*array.MapBuilder), spanAttrs, b.RootID[i])
		d, _ := uintAt(dur, i)
		fb[12].(*array.Uint64Builder).Append(d)
		c, _ := intAt(code, i)
		str(13).Append(ptrace.StatusCode(c).String())
		s, _ = strAt(msg, i)
		str(14).Append(s)

		evs := evG.of(b.RootID[i])
		f.list(fb[15], evs, func(lb array.Builder, e int) {
			t, _ := uintAt(evTime, e)
			lb.(*array.TimestampBuilder).Append(arrow.Timestamp(t))
		})
		f.list(fb[16], evs, func(lb array.Builder, e int) {
			s, _ := strAt(evName, e)
			lb.(*array.StringBuilder).Append(s)
		})
		f.list(fb[17], evs, func(lb array.Builder, e int) {
			f.mapOf(lb.(*array.MapBuilder), evAttrs, b.ChildID[SpanEvents][e])
		})
		lks := lkG.of(b.RootID[i])
		f.list(fb[18], lks, func(lb array.Builder, l int) {
			v, _ := bytesAt(lkTrace, l)
			f.hexID(lb.(*array.StringBuilder), v)
		})
		f.list(fb[19], lks, func(lb array.Builder, l int) {
			v, _ := bytesAt(lkSpan, l)
			f.hexID(lb.(*array.StringBuilder), v)
		})
		f.list(fb[20], lks, func(lb array.Builder, l int) {
			s, _ := strAt(lkState, l)
			lb.(*array.StringBuilder).Append(s)
		})
		f.list(fb[21], lks, func(lb array.Builder, l int) {
			f.mapOf(lb.(*array.MapBuilder), lkAttrs, b.ChildID[SpanLinks][l])
		})
		f.envelope(fb[22:], i, ts)
	}
	return rb.NewRecordBatch()
}

func (f *flattener) list(b array.Builder, idx []int32, each func(array.Builder, int)) {
	lb := b.(*array.ListBuilder)
	lb.Append(true)
	vb := lb.ValueBuilder()
	for _, e := range idx {
		each(vb, int(e))
	}
}

func (f *flattener) logs(mem memory.Allocator) arrow.Record {
	b, r := f.b, f.b.Root
	n := rows(r)
	rb := array.NewRecordBuilder(mem, parquetgo.LogsSchema)
	defer rb.Release()
	rb.Reserve(n)
	fb := rb.Fields()
	str := func(i int) *array.StringBuilder { return fb[i].(*array.StringBuilder) }

	res, scope, logAttrs := f.attrTable(ResourceAttrs), f.attrTable(ScopeAttrs), f.attrTable(LogAttrs)
	tsCol, obs := col(r, "time_unix_nano"), col(r, "observed_time_unix_nano")
	traceID, spanID, flags := col(r, "trace_id"), col(r, "span_id"), col(r, "flags")
	sevText, sevNum, event := col(r, "severity_text"), col(r, "severity_number"), col(r, "event_name")
	resCol, scopeCol := col(r, "resource"), col(r, "scope")
	resURL := child(resCol, "schema_url")
	scopeName, scopeVersion := child(scopeCol, "name"), child(scopeCol, "version")
	scopeURL := col(r, "schema_url")
	body := col(r, "body")
	bt := child(body, "type")
	bodyVals := attrCols{child(body, "str"), child(body, "int"), child(body, "double"), child(body, "bool"),
		child(body, "bytes"), child(body, "ser")}

	svcCache := map[uint16]string{}
	for i := 0; i < n; i++ {
		ts, _ := uintAt(tsCol, i)
		if ts == 0 {
			ts, _ = uintAt(obs, i)
		}
		fb[0].(*array.TimestampBuilder).Append(arrow.Timestamp(ts))
		v, _ := bytesAt(traceID, i)
		f.hexID(str(1), v)
		v, _ = bytesAt(spanID, i)
		f.hexID(str(2), v)
		fl, _ := uintAt(flags, i)
		fb[3].(*array.Uint8Builder).Append(uint8(fl))
		s, _ := strAt(sevText, i)
		str(4).Append(s)
		sn, _ := intAt(sevNum, i)
		fb[5].(*array.Uint8Builder).Append(uint8(sn))
		rid := b.ResID[i]
		svc, ok := svcCache[rid]
		if !ok {
			svc = f.serviceName(res, int64(rid))
			svcCache[rid] = svc
		}
		str(6).Append(svc)
		if body != nil && body.IsValid(i) {
			f.scratch = render(f.scratch[:0], bt, bodyVals, i)
			str(7).BinaryBuilder.Append(f.scratch)
		} else {
			str(7).Append("")
		}
		s, _ = strAt(resURL, i)
		str(8).Append(s)
		f.mapOf(fb[9].(*array.MapBuilder), res, int64(rid))
		s, _ = strAt(scopeURL, i)
		str(10).Append(s)
		s, _ = strAt(scopeName, i)
		str(11).Append(s)
		s, _ = strAt(scopeVersion, i)
		str(12).Append(s)
		f.mapOf(fb[13].(*array.MapBuilder), scope, int64(b.ScopeID[i]))
		f.mapOf(fb[14].(*array.MapBuilder), logAttrs, b.RootID[i])
		s, _ = strAt(event, i)
		str(15).Append(s)
		f.envelope(fb[16:], i, ts)
	}
	return rb.NewRecordBatch()
}
