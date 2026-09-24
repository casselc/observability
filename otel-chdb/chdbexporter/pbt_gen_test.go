package chdbexporter

import (
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"hegel.dev/go/hegel"
)

// Hegel generators for pdata. They aim at what an encoder can get wrong:
// strings with NULs, control characters, quotes, backslashes, non-BMP
// characters and invalid UTF-8; every attribute type, nested maps and slices,
// bytes; doubles including NaN, ±Inf, -0, subnormals and the AsString
// formatting thresholds; all-zero and arbitrary ids; timestamps anywhere in
// a range; empty everything. Sizes are small so a counterexample shrinks to
// something readable.

// trickyStrings are hand-picked values that random text rarely hits.
var trickyStrings = []string{
	"", "\x00", "a\x00b", "\n", "\r\n", "\t", "\\", "\"", "'", "\\'", "\x1f", "\x7f",
	" ", "﻿", "é", "déclinée", "𝄞", "\xff", "\xc3", "\xed\xa0\x80", "{}", "[]", "null",
	"\\u0000", "%s", "?", "\x00\x00\x00",
}

// genString draws valid Unicode text (any codepoint but surrogates), one of
// trickyStrings, or arbitrary bytes, which are usually invalid UTF-8.
// validUTF8 restricts it to the first two.
func genString(maxLen int, validUTF8 bool) hegel.Generator[string] {
	gs := []hegel.Generator[string]{hegel.Text().MaxSize(maxLen), hegel.SampledFrom(trickyStrings)}
	if validUTF8 {
		gs[1] = hegel.SampledFrom(validTricky())
	} else {
		gs = append(gs, hegel.Map(hegel.Binary(0, maxLen), func(b []byte) string { return string(b) }))
	}
	return hegel.OneOf(gs...)
}

func validTricky() []string {
	var out []string
	for _, s := range trickyStrings {
		if isValidUTF8(s) {
			out = append(out, s)
		}
	}
	return out
}

// genDouble favours the values valueString treats specially.
func genDouble() hegel.Generator[float64] {
	return hegel.OneOf(
		hegel.Floats[float64](),
		hegel.SampledFrom([]float64{0, math.Copysign(0, -1), math.NaN(), math.Inf(1), math.Inf(-1),
			1e-6, math.Nextafter(1e-6, 0), 1e21, math.Nextafter(1e21, 0), -1e21, 5e-324, math.MaxFloat64,
			0.1, 1.25, 123456789.125, 1e20, 9.99e20, -1e-7}),
	)
}

// pdataGen controls the shapes drawn; the zero value is the full range.
type pdataGen struct {
	validUTF8 bool // strings are valid UTF-8 (what OTLP/protobuf guarantees)
	// maxTS bounds timestamps; 0 means any uint64.
	maxTS uint64
}

// fillValue sets v to a drawn attribute value, nesting maps and slices up to
// depth levels.
func (g pdataGen) fillValue(tc hegel.TestCase, v pcommon.Value, depth int) {
	kinds := 6
	if depth > 0 {
		kinds = 8
	}
	switch hegel.Draw(tc, hegel.Integers(0, kinds-1)) {
	case 0:
		v.SetStr(hegel.Draw(tc, genString(12, g.validUTF8)))
	case 1:
		v.SetInt(hegel.Draw(tc, hegel.Integers[int64](math.MinInt64, math.MaxInt64)))
	case 2:
		v.SetBool(hegel.Draw(tc, hegel.Booleans()))
	case 3:
		v.SetDouble(hegel.Draw(tc, genDouble()))
	case 4:
		v.SetEmptyBytes().FromRaw(hegel.Draw(tc, hegel.Binary(0, 8)))
	case 5:
		// Empty: leave v as it is (ValueTypeEmpty).
	case 6:
		g.fillMap(tc, v.SetEmptyMap(), depth-1, 3)
	case 7:
		s := v.SetEmptySlice()
		n := hegel.Draw(tc, hegel.Integers(0, 3))
		for i := 0; i < n; i++ {
			g.fillValue(tc, s.AppendEmpty(), depth-1)
		}
	}
}

// fillMap puts up to max drawn attributes into m. Keys may repeat in the
// draw; pcommon.Map keeps the first, like the collector does.
func (g pdataGen) fillMap(tc hegel.TestCase, m pcommon.Map, depth, max int) {
	n := hegel.Draw(tc, hegel.Integers(0, max))
	for i := 0; i < n; i++ {
		k := hegel.Draw(tc, hegel.OneOf(hegel.SampledFrom([]string{"service.name", "k", "http.method", ""}), genString(8, g.validUTF8)))
		if _, ok := m.Get(k); ok {
			continue
		}
		g.fillValue(tc, m.PutEmpty(k), depth)
	}
}

func (g pdataGen) ts(tc hegel.TestCase) uint64 {
	max := g.maxTS
	if max == 0 {
		max = math.MaxUint64
	}
	return hegel.Draw(tc, hegel.OneOf(
		hegel.Integers[uint64](0, max),
		hegel.SampledFrom([]uint64{0, 1, 1_000_000_000, 1_790_000_000_123_456_789, max}),
	))
}

func (g pdataGen) traceID(tc hegel.TestCase) pcommon.TraceID {
	var id pcommon.TraceID
	if hegel.Draw(tc, hegel.Booleans()) {
		copy(id[:], hegel.Draw(tc, hegel.Binary(16, 16)))
	}
	return id
}

func (g pdataGen) spanID(tc hegel.TestCase) pcommon.SpanID {
	var id pcommon.SpanID
	if hegel.Draw(tc, hegel.Booleans()) {
		copy(id[:], hegel.Draw(tc, hegel.Binary(8, 8)))
	}
	return id
}

// traces draws a small ptrace.Traces: up to 2 resources × 2 scopes × 3
// spans, each with up to 2 events and 2 links.
func (g pdataGen) traces() hegel.Generator[ptrace.Traces] {
	return hegel.Composite(func(tc hegel.TestCase) ptrace.Traces {
		td := ptrace.NewTraces()
		for range hegel.Draw(tc, hegel.Integers(0, 2)) {
			rs := td.ResourceSpans().AppendEmpty()
			g.fillMap(tc, rs.Resource().Attributes(), 1, 3)
			if hegel.Draw(tc, hegel.Booleans()) {
				// service.name of any type: ServiceName renders it with AsString.
				g.fillValue(tc, rs.Resource().Attributes().PutEmpty("service.name"), 1)
			}
			for range hegel.Draw(tc, hegel.Integers(0, 2)) {
				ss := rs.ScopeSpans().AppendEmpty()
				ss.Scope().SetName(hegel.Draw(tc, genString(8, g.validUTF8)))
				ss.Scope().SetVersion(hegel.Draw(tc, genString(4, g.validUTF8)))
				for range hegel.Draw(tc, hegel.Integers(0, 3)) {
					s := ss.Spans().AppendEmpty()
					s.SetTraceID(g.traceID(tc))
					s.SetSpanID(g.spanID(tc))
					s.SetParentSpanID(g.spanID(tc))
					s.TraceState().FromRaw(hegel.Draw(tc, genString(6, g.validUTF8)))
					s.SetName(hegel.Draw(tc, genString(10, g.validUTF8)))
					s.SetKind(ptrace.SpanKind(hegel.Draw(tc, hegel.Integers[int32](0, 6))))
					s.SetStartTimestamp(pcommon.Timestamp(g.ts(tc)))
					s.SetEndTimestamp(pcommon.Timestamp(g.ts(tc)))
					s.Status().SetCode(ptrace.StatusCode(hegel.Draw(tc, hegel.Integers[int32](0, 3))))
					s.Status().SetMessage(hegel.Draw(tc, genString(8, g.validUTF8)))
					g.fillMap(tc, s.Attributes(), 2, 4)
					for range hegel.Draw(tc, hegel.Integers(0, 2)) {
						e := s.Events().AppendEmpty()
						e.SetTimestamp(pcommon.Timestamp(g.ts(tc)))
						e.SetName(hegel.Draw(tc, genString(6, g.validUTF8)))
						g.fillMap(tc, e.Attributes(), 1, 2)
					}
					for range hegel.Draw(tc, hegel.Integers(0, 2)) {
						l := s.Links().AppendEmpty()
						l.SetTraceID(g.traceID(tc))
						l.SetSpanID(g.spanID(tc))
						l.TraceState().FromRaw(hegel.Draw(tc, genString(4, g.validUTF8)))
						g.fillMap(tc, l.Attributes(), 1, 2)
					}
				}
			}
		}
		return td
	})
}

// logs draws a small plog.Logs: up to 2 resources × 2 scopes × 3 records.
func (g pdataGen) logs() hegel.Generator[plog.Logs] {
	return hegel.Composite(func(tc hegel.TestCase) plog.Logs {
		ld := plog.NewLogs()
		for range hegel.Draw(tc, hegel.Integers(0, 2)) {
			rl := ld.ResourceLogs().AppendEmpty()
			rl.SetSchemaUrl(hegel.Draw(tc, genString(6, g.validUTF8)))
			g.fillMap(tc, rl.Resource().Attributes(), 1, 3)
			for range hegel.Draw(tc, hegel.Integers(0, 2)) {
				sl := rl.ScopeLogs().AppendEmpty()
				sl.SetSchemaUrl(hegel.Draw(tc, genString(6, g.validUTF8)))
				sl.Scope().SetName(hegel.Draw(tc, genString(8, g.validUTF8)))
				sl.Scope().SetVersion(hegel.Draw(tc, genString(4, g.validUTF8)))
				g.fillMap(tc, sl.Scope().Attributes(), 1, 2)
				for range hegel.Draw(tc, hegel.Integers(0, 3)) {
					r := sl.LogRecords().AppendEmpty()
					if hegel.Draw(tc, hegel.Booleans()) {
						r.SetTimestamp(pcommon.Timestamp(g.ts(tc)))
					}
					r.SetObservedTimestamp(pcommon.Timestamp(g.ts(tc)))
					r.SetTraceID(g.traceID(tc))
					r.SetSpanID(g.spanID(tc))
					r.SetFlags(plog.LogRecordFlags(hegel.Draw(tc, hegel.Integers[uint32](0, math.MaxUint32))))
					r.SetSeverityText(hegel.Draw(tc, genString(6, g.validUTF8)))
					r.SetSeverityNumber(plog.SeverityNumber(hegel.Draw(tc, hegel.Integers[int32](0, 24))))
					g.fillValue(tc, r.Body(), 2)
					g.fillMap(tc, r.Attributes(), 2, 4)
					r.SetEventName(hegel.Draw(tc, genString(6, g.validUTF8)))
				}
			}
		}
		return ld
	})
}

// genValue draws a standalone pcommon.Value.
func (g pdataGen) value() hegel.Generator[pcommon.Value] {
	return hegel.Composite(func(tc hegel.TestCase) pcommon.Value {
		v := pcommon.NewValueEmpty()
		g.fillValue(tc, v, 3)
		return v
	})
}
