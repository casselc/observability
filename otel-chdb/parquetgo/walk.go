package parquetgo

import (
	"encoding/hex"
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
	pairs(p [][2]string)
	arr(n int)
	end()
}

// Envelope is the batch identity appended to every row. The walkers fill in
// the event-time range as they go, for the manifest.
//
// Traces and logs: Announce says which resources this object announces
// (nil: every one); the walkers list them in Announced, in walk order.
type Envelope struct {
	Producer, Epoch string
	Batch           uint64
	Received        uint64 // ns
	Schema          uint16
	MinTS, MaxTS    uint64
	Announce        func(id uint64) bool
	Announced       []uint64
	// Keep, when set, selects the rows written by their event time (the
	// late split, DECISIONS.md D31): a row it rejects is skipped entirely,
	// and row_ordinal counts the rows written.
	Keep func(ts uint64) bool
	// Offload (traces, logs): the object's payload offloader (offload.go);
	// nil: values stay inline, payload_refs and payloads stay empty. The
	// walkers reset it. Carry says which payloads the object carries (nil:
	// every one); Carried lists them, in walk order.
	Offload *Offloader
	Carry   func(h [16]byte) bool
	Carried [][16]byte
	res     Resources
	carried int // Offload.Payloads written so far
}

// offloader is env's offloader, or nil.
func (e *Envelope) offloader() *Offloader {
	if e == nil {
		return nil
	}
	return e.Offload
}

// start resets env's per-walk state.
func (e *Envelope) start() {
	e.res.reset()
	e.Announced = e.Announced[:0]
	e.Carried = e.Carried[:0]
	e.carried = 0
	if e.Offload != nil {
		e.Offload.reset()
	}
}

// PayloadRefs is how many distinct payloads the object's rows reference
// (oscope-payload-refs).
func (e *Envelope) PayloadRefs() int {
	if e.Offload == nil {
		return 0
	}
	return len(e.Offload.Payloads)
}

// attrsOff writes a span, span event or log attribute map through the
// offloader (otap-rs flatten.rs push_attrs_off): a value may become a
// reference document; the markers of this map, and any pending before it (a
// log body's), follow its entries. A map without a candidate value and with
// no markers pending is written as it is.
func attrsOff(w rowWriter, env *Envelope, m pcommon.Map, row int) {
	o := env.offloader()
	if o == nil || (!o.Pending() && !o.anyCandidate(m)) {
		w.attrs(m)
		return
	}
	pairs := make([][2]string, 0, m.Len()+4)
	m.Range(func(k string, v pcommon.Value) bool {
		var s string
		if v.Type() == pcommon.ValueTypeStr {
			s = v.Str()
		} else {
			s = string(valueString(nil, v))
		}
		if o.Candidate(k, len(s)) {
			if out, ok := o.Value(k, bytesOf(s), uint32(row)); ok {
				s = string(out)
			}
		}
		pairs = append(pairs, [2]string{k, s})
		return true
	})
	pairs = append(pairs, o.Markers...)
	o.Markers = o.Markers[:0]
	w.pairs(pairs)
}

// bodyOff is a log body through the offloader (key BodyKey); its markers
// wait for the log's attributes.
func bodyOff(env *Envelope, body string, row int) string {
	o := env.offloader()
	if o == nil || !o.Candidate(BodyKey, len(body)) {
		return body
	}
	if out, ok := o.Value(BodyKey, bytesOf(body), uint32(row)); ok {
		return string(out)
	}
	return body
}

// payloadCols writes a row's payload_refs (its distinct references, hex)
// and, after resource_announce, payloads: the payloads first referenced by
// this row that the object carries.
func payloadRefs(w rowWriter, env *Envelope) {
	o := env.offloader()
	if o == nil {
		w.arr(0)
		w.end()
		return
	}
	refs := o.EndRow()
	w.arr(len(refs))
	for _, h := range refs {
		w.str(hex.EncodeToString(h[:]))
	}
	w.end()
}

func payloadsCol(w rowWriter, env *Envelope) {
	o := env.offloader()
	if o == nil || env.carried == len(o.Payloads) {
		w.pairs(nil)
		return
	}
	var pairs [][2]string
	for _, p := range o.Payloads[env.carried:] {
		if env.Carry == nil || env.Carry(p.Hash) {
			pairs = append(pairs, [2]string{hex.EncodeToString(p.Hash[:]), string(p.Content)})
			env.Carried = append(env.Carried, p.Hash)
		}
	}
	env.carried = len(o.Payloads)
	w.pairs(pairs)
}

// skip reports whether env's Keep rejects a row with event time ts.
func (e *Envelope) skip(ts uint64) bool { return e != nil && e.Keep != nil && !e.Keep(ts) }

// resourceCols writes a row's resource_id, payload_refs,
// resource_announce (the covered set on the first row of a resource the
// object announces) and payloads.
func resourceCols(w rowWriter, env *Envelope, id uint64) {
	w.u64(id)
	payloadRefs(w, env)
	announceCol(w, env, id)
	payloadsCol(w, env)
}

func announceCol(w rowWriter, env *Envelope, id uint64) {
	if env == nil {
		w.pairs(nil)
		return
	}
	r := &env.res
	i, ok := r.index[id]
	first := ok && r.Entries[i].FirstRow < 0
	r.row(id)
	if first && (env.Announce == nil || env.Announce(id)) {
		env.Announced = append(env.Announced, id)
		w.pairs(r.Entries[i].Pairs)
		return
	}
	w.pairs(nil)
}

// resourceOf registers a resource with the object's collector and sets the
// offloader's tenant: the resource's covered k8s.namespace.name.
func resourceOf(env *Envelope, res pcommon.Map) uint64 {
	c := CoveredOf(res)
	if env == nil {
		return c.ID
	}
	if o := env.Offload; o != nil {
		ns := ""
		for _, p := range c.Pairs {
			if p[0] == "k8s.namespace.name" {
				ns = p[1]
				break
			}
		}
		o.Tenant(ns)
	}
	return env.res.resource(c)
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
		return AttrString(v)
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
	return AppendAttrJSON(dst, v) // AsString; maps and slices pinned (attrjson.go)
}

func writeTraces(w rowWriter, td ptrace.Traces, env *Envelope) int {
	n := 0
	if env != nil {
		env.start()
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		res := rs.Resource().Attributes()
		svc := serviceName(res)
		rid := resourceOf(env, res)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scope := ss.Scope()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				s := spans.At(k)
				if env.skip(uint64(s.StartTimestamp())) {
					continue
				}
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
				attrsOff(w, env, s.Attributes(), n)
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
					attrsOff(w, env, ev.At(e).Attributes(), n)
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
				resourceCols(w, env, rid)
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
	if env != nil {
		env.start()
	}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		res := rl.Resource().Attributes()
		svc := serviceName(res)
		rid := resourceOf(env, res)
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
				if env.skip(uint64(ts)) {
					continue
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
					w.str(bodyOff(env, body.Str(), n))
				} else {
					w.str(bodyOff(env, AttrString(body), n))
				}
				w.str(resURL)
				w.attrs(res)
				w.str(scopeURL)
				w.str(scope.Name())
				w.str(scope.Version())
				w.attrs(scope.Attributes())
				attrsOff(w, env, r.Attributes(), n)
				w.str(r.EventName())
				resourceCols(w, env, rid)
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
