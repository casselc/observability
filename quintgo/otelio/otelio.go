// Package otelio moves quint steps in and out of OpenTelemetry: a span
// exporter that writes OTLP/JSON lines (the format of the collector's file
// exporter), a reader for such files, and a converter from in-memory SDK
// spans (e.g. tracetest.SpanRecorder) for tests.
package otelio

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// OTLP/JSON, only the parts quintgo reads and writes.
type (
	TracesData struct {
		ResourceSpans []ResourceSpans `json:"resourceSpans"`
	}
	ResourceSpans struct {
		Resource   Resource     `json:"resource"`
		ScopeSpans []ScopeSpans `json:"scopeSpans"`
	}
	Resource struct {
		Attributes []KeyValue `json:"attributes,omitempty"`
	}
	ScopeSpans struct {
		Scope Scope  `json:"scope"`
		Spans []Span `json:"spans"`
	}
	Scope struct {
		Name string `json:"name,omitempty"`
	}
	Span struct {
		TraceID           string     `json:"traceId"`
		SpanID            string     `json:"spanId"`
		ParentSpanID      string     `json:"parentSpanId,omitempty"`
		Name              string     `json:"name"`
		StartTimeUnixNano U64        `json:"startTimeUnixNano"`
		EndTimeUnixNano   U64        `json:"endTimeUnixNano"`
		Attributes        []KeyValue `json:"attributes,omitempty"`
		Events            []Event    `json:"events,omitempty"`
	}
	Event struct {
		TimeUnixNano U64        `json:"timeUnixNano"`
		Name         string     `json:"name"`
		Attributes   []KeyValue `json:"attributes,omitempty"`
	}
	KeyValue struct {
		Key   string   `json:"key"`
		Value AnyValue `json:"value"`
	}
	AnyValue struct {
		StringValue *string  `json:"stringValue,omitempty"`
		IntValue    *U64     `json:"intValue,omitempty"`
		BoolValue   *bool    `json:"boolValue,omitempty"`
		DoubleValue *float64 `json:"doubleValue,omitempty"`
	}
)

// U64 is a proto3-JSON 64-bit integer: written as a string, read from either.
type U64 int64

func (u U64) MarshalJSON() ([]byte, error) { return json.Marshal(strconv.FormatInt(int64(u), 10)) }
func (u *U64) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		n, err := strconv.ParseInt(s, 10, 64)
		*u = U64(n)
		return err
	}
	var n int64
	err := json.Unmarshal(b, &n)
	*u = U64(n)
	return err
}

func (v AnyValue) Go() any {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return int64(*v.IntValue)
	case v.BoolValue != nil:
		return *v.BoolValue
	case v.DoubleValue != nil:
		return *v.DoubleValue
	}
	return nil
}

func attrMap(kvs []KeyValue) map[string]any {
	m := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value.Go()
	}
	return m
}

// StepsFromTracesData extracts every quint.step event.
func StepsFromTracesData(td *TracesData) ([]qtrace.Step, error) {
	var out []qtrace.Step
	for _, rs := range td.ResourceSpans {
		res := attrMap(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				var spanAttrs map[string]any
				for _, ev := range sp.Events {
					if ev.Name != qtrace.EventName {
						continue
					}
					if spanAttrs == nil {
						spanAttrs = attrMap(sp.Attributes)
					}
					s, err := qtrace.FromAttributes(time.Unix(0, int64(ev.TimeUnixNano)), attrMap(ev.Attributes), spanAttrs, res)
					if err != nil {
						return nil, fmt.Errorf("span %s: %w", sp.SpanID, err)
					}
					s.Source = "span " + sp.SpanID
					out = append(out, s)
				}
			}
		}
	}
	return out, nil
}

// ReadSteps reads OTLP/JSON: one TracesData (or ExportTraceServiceRequest,
// the same shape) per line, as the collector's file exporter writes, or a
// single JSON document.
func ReadSteps(r io.Reader) ([]qtrace.Step, error) {
	dec := json.NewDecoder(bufio.NewReader(r))
	var out []qtrace.Step
	for n := 1; ; n++ {
		var td TracesData
		if err := dec.Decode(&td); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, fmt.Errorf("record %d: %w", n, err)
		}
		steps, err := StepsFromTracesData(&td)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", n, err)
		}
		out = append(out, steps...)
	}
}

// StepsFromSpans extracts steps from SDK spans, e.g. those captured by
// go.opentelemetry.io/otel/sdk/trace/tracetest.SpanRecorder.
func StepsFromSpans(spans []sdktrace.ReadOnlySpan) ([]qtrace.Step, error) {
	td := toTracesData(spans)
	return StepsFromTracesData(&td)
}

func kvs(attrs []attribute.KeyValue) []KeyValue {
	out := make([]KeyValue, 0, len(attrs))
	for _, a := range attrs {
		var v AnyValue
		switch a.Value.Type() {
		case attribute.INT64:
			n := U64(a.Value.AsInt64())
			v.IntValue = &n
		case attribute.BOOL:
			b := a.Value.AsBool()
			v.BoolValue = &b
		case attribute.FLOAT64:
			f := a.Value.AsFloat64()
			v.DoubleValue = &f
		default:
			s := a.Value.Emit()
			v.StringValue = &s
		}
		out = append(out, KeyValue{Key: string(a.Key), Value: v})
	}
	return out
}

func toTracesData(spans []sdktrace.ReadOnlySpan) TracesData {
	var td TracesData
	idx := map[string]int{}
	for _, s := range spans {
		rk := ""
		var rattrs []attribute.KeyValue
		if r := s.Resource(); r != nil {
			rk = r.Encoded(attribute.DefaultEncoder())
			rattrs = r.Attributes()
		}
		i, ok := idx[rk]
		if !ok {
			i = len(td.ResourceSpans)
			idx[rk] = i
			td.ResourceSpans = append(td.ResourceSpans, ResourceSpans{Resource: Resource{Attributes: kvs(rattrs)},
				ScopeSpans: []ScopeSpans{{Scope: Scope{Name: s.InstrumentationScope().Name}}}})
		}
		sc := s.SpanContext()
		sp := Span{
			TraceID: sc.TraceID().String(), SpanID: sc.SpanID().String(),
			Name: s.Name(), StartTimeUnixNano: U64(s.StartTime().UnixNano()), EndTimeUnixNano: U64(s.EndTime().UnixNano()),
			Attributes: kvs(s.Attributes()),
		}
		if p := s.Parent(); p.IsValid() {
			sp.ParentSpanID = p.SpanID().String()
		}
		for _, e := range s.Events() {
			sp.Events = append(sp.Events, Event{TimeUnixNano: U64(e.Time.UnixNano()), Name: e.Name, Attributes: kvs(e.Attributes)})
		}
		td.ResourceSpans[i].ScopeSpans[0].Spans = append(td.ResourceSpans[i].ScopeSpans[0].Spans, sp)
	}
	return td
}

// FileExporter is an sdktrace.SpanExporter writing one OTLP/JSON TracesData
// per export batch, one per line.
type FileExporter struct {
	mu sync.Mutex
	w  io.Writer
}

func NewFileExporter(w io.Writer) *FileExporter { return &FileExporter{w: w} }

func (f *FileExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	td := toTracesData(spans)
	b, err := json.Marshal(td)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err = f.w.Write(append(b, '\n'))
	return err
}

func (f *FileExporter) Shutdown(context.Context) error { return nil }
