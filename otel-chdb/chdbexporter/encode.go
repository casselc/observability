package chdbexporter

import (
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// rowWriter is one wire format. The walkers below visit pdata once, in column
// order, and call it; RowBinary and JSONEachRow differ only in these methods,
// so a benchmark between them measures the format and nothing else.
//
// Arrays (the Nested columns) are opened with arr(n), filled with n values,
// and closed with end().
type rowWriter interface {
	row()
	endRow()
	ts(nanos uint64)
	str(s string)
	traceID(id pcommon.TraceID)
	spanID(id pcommon.SpanID)
	u8(v uint8)
	u16(v uint16)
	u32(v uint32)
	u64(v uint64)
	attrs(m pcommon.Map)
	arr(n int)
	end()
}

// envelope is the batch identity appended to every row when publishing.
// The walkers fill in the event-time range as they go, for the manifest.
type envelope struct {
	producer, epoch string
	batch           uint64
	received        uint64 // ns
	schema          uint16
	minTS, maxTS    uint64
}

// write appends the envelope columns for the row'th row of the batch.
func (e *envelope) write(w rowWriter, row int, ts uint64) {
	if e.minTS == 0 || ts < e.minTS {
		e.minTS = ts
	}
	if ts > e.maxTS {
		e.maxTS = ts
	}
	w.str(e.producer)
	w.str(e.epoch)
	w.u64(e.batch)
	w.u32(uint32(row))
	w.ts(e.received)
	w.u16(e.schema)
}

func serviceName(res pcommon.Map) string {
	if v, ok := res.Get("service.name"); ok {
		if v.Type() == pcommon.ValueTypeStr {
			return v.Str()
		}
		return v.AsString()
	}
	return ""
}

// valueString appends v as the string the clickhouse exporter would store for
// it (pcommon.Value.AsString), without allocating for the common types.
func valueString(dst []byte, v pcommon.Value) []byte {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return append(dst, v.Str()...)
	case pcommon.ValueTypeInt:
		return strconv.AppendInt(dst, v.Int(), 10)
	case pcommon.ValueTypeBool:
		return strconv.AppendBool(dst, v.Bool())
	case pcommon.ValueTypeDouble:
		// AsString's own rule (ES6 number formatting) for the range where it
		// is plain 'f'; NaN, infinities and exponents fall through to it.
		f := v.Double()
		if abs := math.Abs(f); abs == 0 || (abs >= 1e-6 && abs < 1e21) {
			return strconv.AppendFloat(dst, f, 'f', -1, 64)
		}
	}
	return append(dst, v.AsString()...)
}

// writeTraces encodes every span in td and returns the row count. env is nil
// unless publishing.
func writeTraces(w rowWriter, td ptrace.Traces, env *envelope) int {
	n := 0
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		res := rs.Resource().Attributes()
		svc := serviceName(res)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scope := ss.Scope()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				s := spans.At(k)
				w.row()
				w.ts(uint64(s.StartTimestamp()))
				w.traceID(s.TraceID())
				w.spanID(s.SpanID())
				w.spanID(s.ParentSpanID())
				w.str(s.TraceState().AsRaw())
				w.str(s.Name())
				w.str(s.Kind().String())
				w.str(svc)
				w.attrs(res)
				w.str(scope.Name())
				w.str(scope.Version())
				w.attrs(s.Attributes())
				w.u64(uint64(s.EndTimestamp() - s.StartTimestamp()))
				w.str(s.Status().Code().String())
				w.str(s.Status().Message())

				ev := s.Events()
				w.arr(ev.Len())
				for e := 0; e < ev.Len(); e++ {
					w.ts(uint64(ev.At(e).Timestamp()))
				}
				w.end()
				w.arr(ev.Len())
				for e := 0; e < ev.Len(); e++ {
					w.str(ev.At(e).Name())
				}
				w.end()
				w.arr(ev.Len())
				for e := 0; e < ev.Len(); e++ {
					w.attrs(ev.At(e).Attributes())
				}
				w.end()

				ls := s.Links()
				w.arr(ls.Len())
				for l := 0; l < ls.Len(); l++ {
					w.traceID(ls.At(l).TraceID())
				}
				w.end()
				w.arr(ls.Len())
				for l := 0; l < ls.Len(); l++ {
					w.spanID(ls.At(l).SpanID())
				}
				w.end()
				w.arr(ls.Len())
				for l := 0; l < ls.Len(); l++ {
					w.str(ls.At(l).TraceState().AsRaw())
				}
				w.end()
				w.arr(ls.Len())
				for l := 0; l < ls.Len(); l++ {
					w.attrs(ls.At(l).Attributes())
				}
				w.end()
				if env != nil {
					env.write(w, n, uint64(s.StartTimestamp()))
				}
				w.endRow()
				n++
			}
		}
	}
	return n
}

// writeLogs encodes every log record in ld and returns the row count. env is
// nil unless publishing.
func writeLogs(w rowWriter, ld plog.Logs, env *envelope) int {
	n := 0
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		res := rl.Resource().Attributes()
		svc := serviceName(res)
		resURL := rl.SchemaUrl()
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scope := sl.Scope()
			scopeURL := sl.SchemaUrl()
			recs := sl.LogRecords()
			for k := 0; k < recs.Len(); k++ {
				r := recs.At(k)
				ts := r.Timestamp()
				if ts == 0 {
					ts = r.ObservedTimestamp()
				}
				w.row()
				w.ts(uint64(ts))
				w.traceID(r.TraceID())
				w.spanID(r.SpanID())
				w.u8(uint8(r.Flags()))
				w.str(r.SeverityText())
				w.u8(uint8(r.SeverityNumber()))
				w.str(svc)
				body := r.Body()
				if body.Type() == pcommon.ValueTypeStr {
					w.str(body.Str())
				} else {
					w.str(body.AsString())
				}
				w.str(resURL)
				w.attrs(res)
				w.str(scopeURL)
				w.str(scope.Name())
				w.str(scope.Version())
				w.attrs(scope.Attributes())
				w.attrs(r.Attributes())
				w.str(r.EventName())
				if env != nil {
					env.write(w, n, uint64(ts))
				}
				w.endRow()
				n++
			}
		}
	}
	return n
}
