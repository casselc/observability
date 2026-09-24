// Package testgen builds deterministic traces and logs for the chdb exporter's
// tests and for benchmarks that compare it with other exporters.
package testgen

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Epoch is the timestamp of the first span and log record.
var Epoch = time.Date(2026, 9, 24, 12, 0, 0, 123456789, time.UTC)

// Traces builds n spans shaped like instrumented HTTP + DB traffic: three
// services, a handful of attributes of mixed types per span, an event on
// every 4th span and a link on every 8th. Deterministic, so every format sees
// the same input.
func Traces(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	services := []string{"frontend", "checkout", "payments"}
	per := (n + len(services) - 1) / len(services)
	i := 0
	for si, svc := range services {
		rs := td.ResourceSpans().AppendEmpty()
		ra := rs.Resource().Attributes()
		ra.PutStr("service.name", svc)
		ra.PutStr("service.version", "1.4.2")
		ra.PutStr("host.name", fmt.Sprintf("host-%d", si))
		ra.PutStr("deployment.environment.name", "local")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")
		ss.Scope().SetVersion("0.63.0")
		for k := 0; k < per && i < n; k++ {
			s := ss.Spans().AppendEmpty()
			s.SetTraceID(pcommon.TraceID{0x4b, 0xf9, byte(i >> 16), byte(i >> 8), byte(i), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
			s.SetSpanID(pcommon.SpanID{0, 0xf0, byte(i >> 16), byte(i >> 8), byte(i), 0, 0, 1})
			if i%5 != 0 {
				s.SetParentSpanID(pcommon.SpanID{0, 0xe0, byte(i >> 16), byte(i >> 8), byte(i), 0, 0, 2})
			}
			start := Epoch.Add(time.Duration(i) * time.Millisecond)
			s.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
			s.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Duration(1+i%97) * 317 * time.Microsecond)))
			a := s.Attributes()
			if i%3 == 0 {
				s.SetName("SELECT orders")
				s.SetKind(ptrace.SpanKindClient)
				a.PutStr("db.system.name", "postgresql")
				a.PutStr("db.query.text", "SELECT id, total FROM orders WHERE customer_id = $1 AND note <> 'it''s'")
				a.PutInt("db.response.returned_rows", int64(i%50))
			} else {
				s.SetName("POST /api/checkout")
				s.SetKind(ptrace.SpanKindServer)
				a.PutStr("http.request.method", "POST")
				a.PutStr("url.path", "/api/checkout")
				a.PutInt("http.response.status_code", int64(200+(i%7)*50))
				a.PutStr("user_agent.original", "Mozilla/5.0 (X11; Linux x86_64) \"quoted\"")
				a.PutBool("feature.new_checkout", i%2 == 0)
				a.PutDouble("app.cart.total", float64(i)*1.25)
			}
			if i%11 == 0 {
				s.Status().SetCode(ptrace.StatusCodeError)
				s.Status().SetMessage("payment declined:\n\tcard\x00expired")
			} else {
				s.Status().SetCode(ptrace.StatusCodeOk)
			}
			if i%4 == 0 {
				ev := s.Events().AppendEmpty()
				ev.SetName("exception")
				ev.SetTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Microsecond)))
				ev.Attributes().PutStr("exception.type", "TimeoutError")
				ev.Attributes().PutStr("exception.message", "upstream timed out after 250ms")
			}
			if i%8 == 0 {
				l := s.Links().AppendEmpty()
				l.SetTraceID(pcommon.TraceID{9, 9, byte(i), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
				l.SetSpanID(pcommon.SpanID{9, 9, byte(i), 1, 2, 3, 4, 5})
				l.TraceState().FromRaw("vendor=1")
				l.Attributes().PutStr("link.kind", "follows_from")
			}
			i++
		}
	}
	return td
}

// Logs builds n log records across three services, with bodies that need
// escaping in JSON and a NUL that would end a C string.
func Logs(n int) plog.Logs {
	ld := plog.NewLogs()
	services := []string{"frontend", "checkout", "payments"}
	per := (n + len(services) - 1) / len(services)
	i := 0
	for _, svc := range services {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
		rl.Resource().Attributes().PutStr("service.name", svc)
		rl.Resource().Attributes().PutStr("host.name", "devbox")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("log/slog")
		sl.Scope().Attributes().PutStr("bridge", "otelslog")
		for k := 0; k < per && i < n; k++ {
			r := sl.LogRecords().AppendEmpty()
			r.SetTimestamp(pcommon.NewTimestampFromTime(Epoch.Add(time.Duration(i) * time.Millisecond)))
			r.SetObservedTimestamp(pcommon.NewTimestampFromTime(Epoch.Add(time.Duration(i)*time.Millisecond + time.Microsecond)))
			if i%2 == 0 {
				r.SetTraceID(pcommon.TraceID{0x4b, 0xf9, byte(i >> 16), byte(i >> 8), byte(i), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
				r.SetSpanID(pcommon.SpanID{0, 0xf0, byte(i >> 16), byte(i >> 8), byte(i), 0, 0, 1})
				r.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
			}
			switch i % 4 {
			case 0:
				r.SetSeverityNumber(plog.SeverityNumberInfo)
				r.SetSeverityText("INFO")
				r.Body().SetStr(fmt.Sprintf("order %d accepted for \"customer\" 42", i))
			case 1:
				r.SetSeverityNumber(plog.SeverityNumberWarn)
				r.SetSeverityText("WARN")
				r.Body().SetStr("slow query: 812ms\nSELECT * FROM orders\\items")
			case 2:
				r.SetSeverityNumber(plog.SeverityNumberError)
				r.SetSeverityText("ERROR")
				r.Body().SetStr("card\x00expired — déclinée")
			case 3:
				r.SetSeverityNumber(plog.SeverityNumberDebug)
				r.SetSeverityText("DEBUG")
				m := r.Body().SetEmptyMap()
				m.PutStr("msg", "structured body")
				m.PutInt("attempt", 3)
			}
			if i%10 == 0 {
				r.SetEventName("checkout.completed")
			}
			a := r.Attributes()
			a.PutStr("code.function.name", "main.handleCheckout")
			a.PutInt("code.line.number", int64(100+i%40))
			a.PutStr("customer.tier", []string{"free", "pro", "enterprise"}[i%3])
			i++
		}
	}
	return ld
}
