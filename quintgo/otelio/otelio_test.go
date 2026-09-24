package otelio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type ambiguous struct{}

func (ambiguous) Error() string        { return "timeout" }
func (ambiguous) QuintOutcome() string { return "ambiguous" }

func record(r *qobs.Recorder) {
	ctx := qobs.Thread(context.Background())
	r.Record(ctx, "pushStart", nil, "payload", "req-1", "batch", uint64(7), "order.batch", uint64(7), "process", "e1", "actor", "w/traces")
	r.Record(ctx, "writeTable", ambiguous{}, "batch", 7, "obs.rows", 3)
	r.Record(ctx, "writeParquet", errors.New("boom"), "when", "ok", "batch", 7) // skipped
}

func TestInMemorySpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	record(qobs.NewRecorder(qobs.NewOTelSink(tp)))
	steps, err := StepsFromSpans(sr.Ended())
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Fatalf("want 2 steps, got %d", len(steps))
	}
	a, b := steps[0], steps[1]
	if a.Action != "pushStart" || a.Args["batch"] != int64(7) || a.Args["payload"] != "req-1" || a.Order["batch"] != 7 ||
		a.Process != "e1" || a.Actor != "w/traces" || a.Seq != 1 || a.Thread == "" {
		t.Fatalf("step 1: %+v", a)
	}
	if b.Outcome != "ambiguous" || b.Error != "timeout" || b.Obs["rows"] != int64(3) || b.Thread != a.Thread || b.Seq != 2 {
		t.Fatalf("step 2: %+v", b)
	}
}

func TestOTLPJSONFile(t *testing.T) {
	var buf bytes.Buffer
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(NewFileExporter(&buf)))
	record(qobs.NewRecorder(qobs.NewOTelSink(tp)))
	_ = tp.Shutdown(context.Background())
	steps, err := ReadSteps(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[1].Outcome != "ambiguous" || steps[0].Args["batch"] != int64(7) {
		t.Fatalf("steps: %+v", steps)
	}
}

// Overhead of one recorded step, by sink.
func BenchmarkRecordDisabled(b *testing.B) {
	qobs.SetGlobal(nil)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		qobs.Record(ctx, "writeTable", nil, "payload", "req-1", "batch", uint64(i), "gen", "g20260924T100000")
	}
}

func BenchmarkRecordMemory(b *testing.B) {
	r := qobs.NewRecorder(&qobs.MemorySink{})
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Record(ctx, "writeTable", nil, "payload", "req-1", "batch", uint64(i), "gen", "g20260924T100000")
	}
}

func BenchmarkRecordOTelBatchToFile(b *testing.B) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(NewFileExporter(io.Discard)))
	defer tp.Shutdown(context.Background())
	r := qobs.NewRecorder(qobs.NewOTelSink(tp))
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Record(ctx, "writeTable", nil, "payload", "req-1", "batch", uint64(i), "gen", "g20260924T100000")
	}
}

func BenchmarkRecordStepLog(b *testing.B) {
	r := qobs.NewRecorder(qobs.NewJSONLSink(io.Discard))
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Record(ctx, "writeTable", nil, "payload", "req-1", "batch", uint64(i), "gen", "g20260924T100000")
	}
}
