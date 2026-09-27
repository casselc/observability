package s3pqexporter

import (
	"context"
	"errors"
	"go.opentelemetry.io/collector/component"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"
)

func spans(names ...string) ptrace.Traces {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	for _, n := range names {
		ss.AppendEmpty().SetName(n)
	}
	return td
}

func names(td ptrace.Traces) []string {
	var out []string
	for i := range td.ResourceSpans().Len() {
		for j := range td.ResourceSpans().At(i).ScopeSpans().Len() {
			ss := td.ResourceSpans().At(i).ScopeSpans().At(j).Spans()
			for k := range ss.Len() {
				out = append(out, ss.At(k).Name())
			}
		}
	}
	return out
}

// The queue stand-in: records what was handed on, optionally blocking until
// released or failing.
type sink struct {
	mu      sync.Mutex
	batches [][]string
	ctxs    []context.Context
	gate    chan struct{}
	err     error
}

func (s *sink) consume(ctx context.Context, td ptrace.Traces) error {
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, names(td))
	s.ctxs = append(s.ctxs, ctx)
	return s.err
}

func (s *sink) snapshot() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.batches...)
}

// Requests are merged up to max_size (the request that overflows is split,
// deterministically, the rest opening the next batch), a full batch is handed
// on at once, the remainder after flush_timeout, and every caller returns
// only once every batch holding its items has been handed on.
func TestBatchMergesSplitsAndWaits(t *testing.T) {
	s := &sink{}
	b := newBatcher(BatchConfig{Enabled: true, FlushTimeout: 200 * time.Millisecond, MinSize: 5, MaxSize: 5}, tracesKind, s.consume)
	start := time.Now()
	var wg sync.WaitGroup
	returned := make([]time.Duration, 2)
	for i, req := range [][]string{{"a1", "a2", "a3"}, {"b1", "b2", "b3", "b4"}} {
		if i == 1 {
			time.Sleep(20 * time.Millisecond) // a first, then b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.consume(context.Background(), spans(req...)); err != nil {
				t.Error(err)
			}
			returned[i] = time.Since(start)
		}()
	}
	wg.Wait()
	got := s.snapshot()
	if len(got) != 2 || strings.Join(got[0], ",") != "a1,a2,a3,b1,b2" || strings.Join(got[1], ",") != "b3,b4" {
		t.Fatalf("batches %v", got)
	}
	// a's items were all in the first (full) batch: it returns at once; b
	// waits for the timeout flush of the second.
	if returned[0] > 150*time.Millisecond || returned[1] < 200*time.Millisecond {
		t.Fatalf("returned %v", returned)
	}
}

// A request above max_size is cut into max_size pieces, in order; with
// max_size 0 it is merged whole.
func TestBatchSplitsOversized(t *testing.T) {
	s := &sink{}
	b := newBatcher(BatchConfig{Enabled: true, FlushTimeout: time.Hour, MinSize: 2, MaxSize: 2}, tracesKind, s.consume)
	// 5 items: two full pieces at once, the fifth waits: shut down flushes it.
	done := make(chan error)
	go func() { done <- b.consume(context.Background(), spans("1", "2", "3", "4", "5")) }()
	time.Sleep(50 * time.Millisecond)
	if got := s.snapshot(); len(got) != 2 {
		t.Fatalf("before shutdown %v", got)
	}
	b.shutdown()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := s.snapshot(); len(got) != 3 || strings.Join(got[2], ",") != "5" {
		t.Fatalf("batches %v", got)
	}
	// After shutdown: straight through.
	if err := b.consume(context.Background(), spans("x", "y", "z")); err != nil {
		t.Fatal(err)
	}
	s2 := &sink{}
	b2 := newBatcher(BatchConfig{Enabled: true, FlushTimeout: time.Hour, MinSize: 2}, tracesKind, s2.consume)
	if err := b2.consume(context.Background(), spans("1", "2", "3", "4", "5")); err != nil {
		t.Fatal(err)
	}
	if got := s2.snapshot(); len(got) != 1 || len(got[0]) != 5 {
		t.Fatalf("no max: %v", got)
	}
}

// No caller is answered before the merged request is in the queue, and each
// gets the queue's answer (a full queue: the senders resend).
func TestBatchAnswersAfterTheQueue(t *testing.T) {
	s := &sink{gate: make(chan struct{}), err: errors.New("sending queue is full")}
	b := newBatcher(BatchConfig{Enabled: true, FlushTimeout: 10 * time.Millisecond, MinSize: 100}, tracesKind, s.consume)
	var answered atomic.Int32
	errs := make(chan error, 2)
	for _, n := range []string{"a", "b"} {
		go func() { errs <- b.consume(context.Background(), spans(n)); answered.Add(1) }()
	}
	time.Sleep(100 * time.Millisecond) // well past flush_timeout: the flush is blocked in the queue
	if answered.Load() != 0 {
		t.Fatal("answered before the queue took the batch")
	}
	close(s.gate)
	for range 2 {
		if err := <-errs; err == nil || !strings.Contains(err.Error(), "full") {
			t.Fatalf("want the queue's error, got %v", err)
		}
	}
}

// received_at of a merged request: the stamp is taken as the merged request
// is handed to the queue, after every member arrived, from its own context
// (no member's deadline or client metadata), and it is what the queue
// persists.
func TestBatchReceivedIsTheEnqueueTime(t *testing.T) {
	var clock atomic.Int64
	clock.Store(100)
	c := &captureTraces{}
	st := stampTraces{c, func() time.Time { return time.Unix(0, clock.Load()) }, custodyFor(component.MustNewID("batchtest"))}
	bt := batchTraces{st, newBatcher(BatchConfig{Enabled: true, FlushTimeout: time.Hour, MinSize: 3}, tracesKind, st.ConsumeTraces)}

	ctxA, cancel := context.WithCancel(client.NewContext(context.Background(),
		client.Info{Metadata: client.NewMetadata(map[string][]string{"x-tenant": {"a"}})}))
	done := make(chan error, 2)
	go func() { done <- bt.ConsumeTraces(ctxA, spans("a1", "a2")) }() // arrives at 100
	time.Sleep(20 * time.Millisecond)
	cancel() // the first sender gives up: must not cancel the others' enqueue
	clock.Store(200)
	go func() { done <- bt.ConsumeTraces(context.Background(), spans("b1")) }() // arrives at 200, fills the batch
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if c.ctx.Err() != nil {
		t.Fatal("the merged request's context is a member's")
	}
	b, err := pdatareq.MarshalTraces(c.ctx, spans("a1", "a2", "b1"))
	if err != nil {
		t.Fatal(err)
	}
	out, _, err := pdatareq.UnmarshalTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	md := client.FromContext(out).Metadata
	if got := md.Get(ReceivedKey); len(got) != 1 || got[0] != strconv.Itoa(200) {
		t.Fatalf("received_at %v, want the enqueue time 200 (>= every member's arrival)", got)
	}
	if got := md.Get("x-tenant"); len(got) != 0 {
		t.Fatalf("a member's client metadata leaked into the merged request: %v", got)
	}
}

// Logs and metrics merge the same way; a metrics request with no points is
// passed through (the edge decides whether it is a permanent error).
func TestBatchLogsMetrics(t *testing.T) {
	var got []int
	var mu sync.Mutex
	bl := newBatcher(BatchConfig{Enabled: true, FlushTimeout: time.Hour, MinSize: 3, MaxSize: 3}, logsKind,
		func(_ context.Context, ld plog.Logs) error {
			mu.Lock()
			got = append(got, ld.LogRecordCount())
			mu.Unlock()
			return nil
		})
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for range 3 {
		lr.AppendEmpty()
	}
	if err := bl.consume(context.Background(), ld); err != nil || len(got) != 1 || got[0] != 3 {
		t.Fatalf("%v %v", err, got)
	}
	var mgot []int
	bm := newBatcher(BatchConfig{Enabled: true, FlushTimeout: time.Hour, MinSize: 4, MaxSize: 4}, metricsKind,
		func(_ context.Context, md pmetric.Metrics) error {
			mu.Lock()
			mgot = append(mgot, md.DataPointCount())
			mu.Unlock()
			return nil
		})
	md := pmetric.NewMetrics()
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("g")
	g.SetEmptyGauge()
	for range 6 {
		g.Gauge().DataPoints().AppendEmpty()
	}
	go func() { _ = bm.consume(context.Background(), md) }()
	time.Sleep(50 * time.Millisecond)
	bm.shutdown()
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(mgot) != 2 || mgot[0] != 4 || mgot[1] != 2 {
		t.Fatalf("metrics %v", mgot)
	}
	empty := pmetric.NewMetrics()
	empty.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("typeless")
	bm2 := newBatcher(BatchConfig{Enabled: true, FlushTimeout: time.Hour, MinSize: 4}, metricsKind,
		func(context.Context, pmetric.Metrics) error { return errors.New("passed through") })
	if err := bm2.consume(context.Background(), empty); err == nil {
		t.Fatal("a pointless metrics request must reach the edge at once")
	}
}

func TestBatchConfig(t *testing.T) {
	cfg := load(t, map[string]any{"cluster": "c1", "producer_id": "p", "s3": map[string]any{"url": "s3://b/p"},
		"batch": map[string]any{"enabled": true, "flush_timeout": "500ms", "min_size": 8000, "max_size": 10000}})
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Batch.FlushTimeout != 500*time.Millisecond || cfg.Batch.MinSize != 8000 || cfg.Batch.MaxSize != 10000 {
		t.Fatalf("%+v", cfg.Batch)
	}
	def := NewFactory().CreateDefaultConfig().(*Config).Batch
	if def.Enabled || def.MinSize != 10000 || def.MaxSize != 10000 || def.FlushTimeout != time.Second {
		t.Fatalf("default %+v", def)
	}
	for _, bad := range []map[string]any{
		{"enabled": true, "flush_timeout": "0s"},
		{"enabled": true, "min_size": 0},
		{"enabled": true, "min_size": 100, "max_size": 50},
	} {
		c := load(t, map[string]any{"cluster": "c1", "producer_id": "p", "s3": map[string]any{"url": "s3://b/p"}, "batch": bad})
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "batch.") {
			t.Fatalf("%v: %v", bad, err)
		}
	}
}
