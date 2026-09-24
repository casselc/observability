package qobs

import (
	"context"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// OTelSink emits each step as a short span "quint <action>" carrying one
// span event "quint.step" with the schema attributes (see qtrace). The span
// is a child of the span in ctx, so steps show up inside the application's
// own traces.
//
// Give it a TracerProvider whose sampler keeps every span: a sampled-out
// step is a hole in the trace, which the reconstructor reports (quint.seq is
// dense) but cannot fill.
type OTelSink struct{ Tracer trace.Tracer }

// NewOTelSink returns a sink on tp's "quintgo" tracer.
func NewOTelSink(tp trace.TracerProvider) *OTelSink {
	return &OTelSink{Tracer: tp.Tracer("github.com/chucklehead-dev/oscope/spike/quintgo/qobs")}
}

func (o *OTelSink) Record(ctx context.Context, s *qtrace.Step) {
	_, span := o.Tracer.Start(ctx, qtrace.SpanName+s.Action, trace.WithTimestamp(s.Time))
	span.AddEvent(qtrace.EventName, trace.WithTimestamp(s.Time), trace.WithAttributes(Attributes(s)...))
	span.End(trace.WithTimestamp(s.Time))
}

// Attributes encodes a step in the schema.
func Attributes(s *qtrace.Step) []attribute.KeyValue {
	kv := []attribute.KeyValue{
		attribute.String(qtrace.KeyAction, s.Action),
		attribute.Int64(qtrace.KeySeq, int64(s.Seq)),
		attribute.String(qtrace.KeyRecorder, s.Recorder),
		attribute.String(qtrace.KeyOutcome, s.Outcome),
	}
	if s.Actor != "" {
		kv = append(kv, attribute.String(qtrace.KeyActor, s.Actor))
	}
	if s.Process != "" {
		kv = append(kv, attribute.String(qtrace.KeyProcess, s.Process))
	}
	if s.Thread != "" {
		kv = append(kv, attribute.String(qtrace.KeyThread, s.Thread))
	}
	if s.Error != "" {
		kv = append(kv, attribute.String(qtrace.KeyError, s.Error))
	}
	for k, v := range s.Args {
		kv = append(kv, attr(qtrace.PrefixArg+k, v))
	}
	for k, v := range s.Obs {
		kv = append(kv, attr(qtrace.PrefixObs+k, v))
	}
	for k, v := range s.Order {
		kv = append(kv, attribute.Int64(qtrace.PrefixOrder+k, v))
	}
	return kv
}

func attr(k string, v any) attribute.KeyValue {
	switch x := v.(type) {
	case int64:
		return attribute.Int64(k, x)
	case bool:
		return attribute.Bool(k, x)
	case float64:
		return attribute.Float64(k, x)
	case string:
		return attribute.String(k, x)
	}
	return attribute.String(k, "")
}
