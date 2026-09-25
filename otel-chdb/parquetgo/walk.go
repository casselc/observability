package parquetgo

import (
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The walkers, the envelope and valueString are copied from
// ../chdbexporter/encode.go unchanged except for naming, so both producers
// visit pdata in the same order and render values identically. They are
// copied rather than imported to keep chdb-go out of this module's graph; a
// real split would move them into a shared package.

// rowWriter is one output: here, Arrow column builders. Arrays (the Nested
// columns) are opened with arr(n), filled with n values, and closed with end().
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

// Envelope is the batch identity appended to every row. The walkers fill in
// the event-time range as they go, for the manifest.
type Envelope struct {
	Producer, Epoch string
	Batch           uint64
	Received        uint64 // ns
	Schema          uint16
	MinTS, MaxTS    uint64
}

func (e *Envelope) write(w rowWriter, row int, ts uint64) {
	if e.MinTS == 0 || ts < e.MinTS {
		e.MinTS = ts
	}
	if ts > e.MaxTS {
		e.MaxTS = ts
	}
	w.str(e.Producer)
	w.str(e.Epoch)
	w.u64(e.Batch)
	w.u32(uint32(row))
	w.ts(e.Received)
	w.u16(e.Schema)
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
		f := v.Double()
		if abs := math.Abs(f); abs == 0 || (abs >= 1e-6 && abs < 1e21) {
			return strconv.AppendFloat(dst, f, 'f', -1, 64)
		}
	}
	return append(dst, v.AsString()...)
}

func writeTraces(w rowWriter, td ptrace.Traces, env *Envelope) int {
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

func writeLogs(w rowWriter, ld plog.Logs, env *Envelope) int {
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
