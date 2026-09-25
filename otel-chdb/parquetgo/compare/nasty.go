package compare

import (
	"fmt"
	"math"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Nasty values for the correctness comparison: NULs, control characters,
// unicode (and invalid UTF-8), extreme numbers, every pcommon value type,
// empty and large maps, many events and links, zero ids, zero and extreme
// timestamps.
var nastyStrings = []string{
	"", "plain", "\x00", "a\x00b", "\x00\x00lead", "tab\there\nnewline\r\n",
	"日本語テキスト", "🚀🔥 emoji", "RTL \u202e txet", "combining e\u0301", "\xef\xbb\xbfbom",
	"quote ' \" ` \\ backslash", "invalid \xff\xfe utf8", "\x7f\x01\x02 ctrl",
	strings.Repeat("long-", 20000), "{\"looks\":\"json\"}", "NaN", "null",
}

var nastyDoubles = []float64{0, math.Copysign(0, -1), 1, -1, 0.1 + 0.2, 1e21, 1e-7, 1e-6, 5e-324,
	math.MaxFloat64, -math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1), 123456789.123456789}

var nastyInts = []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1 << 53}

func putNastyAttrs(m pcommon.Map, i, n int) {
	for j := 0; j < n; j++ {
		k := fmt.Sprintf("k%d.%s", j, nastyStrings[(i+j)%len(nastyStrings)])
		if len(k) > 200 {
			k = k[:200]
		}
		switch (i + j) % 10 {
		case 0:
			m.PutStr(k, nastyStrings[(i*7+j)%len(nastyStrings)])
		case 1:
			m.PutInt(k, nastyInts[(i+j)%len(nastyInts)])
		case 2:
			m.PutDouble(k, nastyDoubles[(i+j)%len(nastyDoubles)])
		case 3:
			m.PutBool(k, (i+j)%2 == 0)
		case 4:
			m.PutEmptyBytes(k).FromRaw([]byte{0, 1, 2, 0xff, byte(i)})
		case 5:
			s := m.PutEmptySlice(k)
			s.AppendEmpty().SetStr("x\x00y")
			s.AppendEmpty().SetInt(int64(i))
			s.AppendEmpty().SetDouble(nastyDoubles[i%len(nastyDoubles)])
		case 6:
			mm := m.PutEmptyMap(k)
			mm.PutStr("inner", nastyStrings[i%len(nastyStrings)])
			mm.PutEmptyMap("deeper").PutBool("b", true)
		case 7:
			m.PutEmpty(k) // Empty value: AsString ""
		case 8:
			m.PutStr(k, "")
		case 9:
			m.PutStr("\x00key with nul "+fmt.Sprint(j), "v")
		}
	}
}

// NastyTraces builds n spans over several resources and scopes.
func NastyTraces(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	for i := 0; i < n; {
		rs := td.ResourceSpans().AppendEmpty()
		ra := rs.Resource().Attributes()
		switch i % 3 {
		case 0:
			ra.PutStr("service.name", nastyStrings[i%len(nastyStrings)])
		case 1:
			ra.PutInt("service.name", int64(i)) // non-string service name
		}
		putNastyAttrs(ra, i, i%5)
		for sc := 0; sc < 2 && i < n; sc++ {
			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName(nastyStrings[(i+sc)%len(nastyStrings)])
			if sc == 1 {
				ss.Scope().SetVersion("v\x001")
			}
			for k := 0; k < 7 && i < n; k++ {
				s := ss.Spans().AppendEmpty()
				if i%13 != 0 {
					s.SetTraceID(pcommon.TraceID{byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
					s.SetSpanID(pcommon.SpanID{byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 7})
				}
				if i%3 == 0 {
					s.SetParentSpanID(pcommon.SpanID{0xff, byte(i), 0, 0, 0, 0, 0, 0})
				}
				s.TraceState().FromRaw([]string{"", "a=1,b=2", "vendor=\x00x"}[i%3])
				s.SetName(nastyStrings[(i*3)%len(nastyStrings)])
				s.SetKind(ptrace.SpanKind(i % 6))
				var start uint64
				switch i % 17 {
				case 0:
					start = 0
				case 1:
					start = math.MaxInt64
				case 2:
					start = 1 // 1 ns after the epoch
				default:
					start = 1790000000000000000 + uint64(i)*1_000_003
				}
				s.SetStartTimestamp(pcommon.Timestamp(start))
				end := start + uint64(i%1000)*1000
				if i%19 == 0 {
					end = start - 5 // negative duration wraps as the table exporter's does
				}
				s.SetEndTimestamp(pcommon.Timestamp(end))
				putNastyAttrs(s.Attributes(), i, []int{0, 1, 3, 10, 40}[i%5])
				s.Status().SetCode(ptrace.StatusCode(i % 3))
				s.Status().SetMessage(nastyStrings[(i+5)%len(nastyStrings)])
				for e := 0; e < []int{0, 1, 2, 50}[i%4]; e++ {
					ev := s.Events().AppendEmpty()
					ev.SetName(nastyStrings[(i+e)%len(nastyStrings)])
					ev.SetTimestamp(pcommon.Timestamp(start + uint64(e)))
					putNastyAttrs(ev.Attributes(), i+e, e%3)
				}
				for l := 0; l < []int{0, 1, 3}[i%3]; l++ {
					lk := s.Links().AppendEmpty()
					if l != 1 {
						lk.SetTraceID(pcommon.TraceID{9, byte(l), byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})
						lk.SetSpanID(pcommon.SpanID{9, byte(l), byte(i), 0, 0, 0, 0, 3})
					}
					lk.TraceState().FromRaw([]string{"", "k=v"}[l%2])
					putNastyAttrs(lk.Attributes(), i+l, l%2)
				}
				i++
			}
		}
	}
	return td
}

// NastyLogs builds n log records with every body type.
func NastyLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	for i := 0; i < n; {
		rl := ld.ResourceLogs().AppendEmpty()
		if i%2 == 0 {
			rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
		}
		ra := rl.Resource().Attributes()
		if i%4 != 3 {
			ra.PutStr("service.name", nastyStrings[i%len(nastyStrings)])
		}
		putNastyAttrs(ra, i, i%4)
		sl := rl.ScopeLogs().AppendEmpty()
		sl.SetSchemaUrl([]string{"", "scope://x\x00"}[i%2])
		sl.Scope().SetName(nastyStrings[(i+1)%len(nastyStrings)])
		sl.Scope().SetVersion(nastyStrings[(i+2)%len(nastyStrings)])
		putNastyAttrs(sl.Scope().Attributes(), i, i%3)
		for k := 0; k < 9 && i < n; k++ {
			r := sl.LogRecords().AppendEmpty()
			switch i % 5 {
			case 0: // no timestamp: the observed one is used
				r.SetObservedTimestamp(pcommon.Timestamp(1790000000000000000 + uint64(i)))
			case 1: // neither
			case 2:
				r.SetTimestamp(math.MaxInt64)
			default:
				r.SetTimestamp(pcommon.Timestamp(1790000000000000000 + uint64(i)*997))
			}
			if i%2 == 0 {
				r.SetTraceID(pcommon.TraceID{1, byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
				r.SetSpanID(pcommon.SpanID{2, byte(i), 0, 0, 0, 0, 0, 0})
			}
			r.SetFlags(plog.LogRecordFlags(uint32(i * 37)))
			r.SetSeverityNumber(plog.SeverityNumber(i % 25))
			r.SetSeverityText(nastyStrings[(i+3)%len(nastyStrings)])
			b := r.Body()
			switch i % 8 {
			case 0:
				b.SetStr(nastyStrings[i%len(nastyStrings)])
			case 1:
				b.SetInt(nastyInts[i%len(nastyInts)])
			case 2:
				b.SetDouble(nastyDoubles[i%len(nastyDoubles)])
			case 3:
				b.SetBool(true)
			case 4:
				m := b.SetEmptyMap()
				m.PutStr("msg", "structured\x00")
				m.PutDouble("d", nastyDoubles[i%len(nastyDoubles)])
			case 5:
				s := b.SetEmptySlice()
				s.AppendEmpty().SetStr("a")
				s.AppendEmpty().SetEmptyMap().PutInt("n", 1)
			case 6:
				b.SetEmptyBytes().FromRaw([]byte{0xde, 0xad, 0, 0xbe, 0xef})
			case 7: // empty body
			}
			putNastyAttrs(r.Attributes(), i, []int{0, 2, 5, 25}[i%4])
			r.SetEventName([]string{"", "evt", "e\x00v"}[i%3])
			i++
		}
	}
	return ld
}
