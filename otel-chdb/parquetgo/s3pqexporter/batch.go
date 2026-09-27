package s3pqexporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// BatchConfig is the batch step BEFORE the sending queue (DECISIONS.md D4):
// the exporter merges incoming requests and hands the merged request to the
// queue as one item, and every caller whose items are in it gets its answer
// only once that item is in the queue (file_storage: written, as the ack
// means). So:
//
//   - nothing is acknowledged before it is persisted: a SIGKILL loses only
//     requests nobody was told were accepted (the sender resends them). The
//     collector's `batch` processor answers as soon as it has taken a
//     request, which is what lost acknowledged data on kind
//     (deploy/results/k8s-sim.md §8);
//   - the queue item is the merged request, so a replay after a restart has
//     the same bytes and content key (what `sending_queue.batch` breaks: it
//     re-cuts after the queue, differently after a restart, risk #10);
//   - received_at is stamped as the merged request is handed to the queue
//     (received.go): the moment every merged request entered custody, and
//     no earlier one. Not the min or max of the members' arrival times:
//     until the merged request is written none of them is in custody (none
//     was acknowledged), and a received_at from before that would be older
//     than the custody start of a request not yet held, which the
//     completeness watermark (model/completeness.qnt, oscope-low) and the
//     consumer's toDate(received_at) partition rule both rule out.
//
// Sizes are items (spans, log records, metric data points), as the agents'
// `batch` processor sized them: a merged request is flushed once it holds
// MinSize items or FlushTimeout after its first item arrived; a request that
// would take it over MaxSize is split deterministically (the batch
// processor's split), the rest starting the next batch. What it gives up (as
// the Rust publisher's batch step, deploy/README.md §Duplicates): a sender's
// resend of a request already written but whose answer was lost lands in a
// different merged request, so central keeps both copies.
type BatchConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// FlushTimeout bounds how long a request waits for companions. Keep it
	// well under the senders' timeout: it adds to every request's latency.
	FlushTimeout time.Duration `mapstructure:"flush_timeout"`
	// MinSize: flush once a merged request holds this many items.
	MinSize int `mapstructure:"min_size"`
	// MaxSize: no merged request above it (0: no limit, no split).
	MaxSize int `mapstructure:"max_size"`
}

func defaultBatchConfig() BatchConfig {
	return BatchConfig{Enabled: false, FlushTimeout: time.Second, MinSize: 10000, MaxSize: 10000}
}

func (b BatchConfig) validate() error {
	if !b.Enabled {
		return nil
	}
	var errs []error
	if b.FlushTimeout <= 0 {
		errs = append(errs, errors.New("batch.flush_timeout must be > 0"))
	}
	if b.MinSize <= 0 {
		errs = append(errs, errors.New("batch.min_size must be > 0"))
	}
	if b.MaxSize < 0 || (b.MaxSize > 0 && b.MaxSize < b.MinSize) {
		errs = append(errs, fmt.Errorf("batch.max_size %d: want 0 or >= min_size %d", b.MaxSize, b.MinSize))
	}
	return errors.Join(errs...)
}

// kind is what the batcher needs of one pdata type.
type kind[T any] struct {
	count func(T) int
	// split removes n items from src and returns them (src.count > n).
	split func(n int, src T) T
	// moveTo appends src's contents to dst, emptying src.
	moveTo func(src, dst T)
	empty  func() T
}

var (
	tracesKind = kind[ptrace.Traces]{
		count: ptrace.Traces.SpanCount, split: splitTraces, empty: ptrace.NewTraces,
		moveTo: func(src, dst ptrace.Traces) { src.ResourceSpans().MoveAndAppendTo(dst.ResourceSpans()) },
	}
	logsKind = kind[plog.Logs]{
		count: plog.Logs.LogRecordCount, split: splitLogs, empty: plog.NewLogs,
		moveTo: func(src, dst plog.Logs) { src.ResourceLogs().MoveAndAppendTo(dst.ResourceLogs()) },
	}
	metricsKind = kind[pmetric.Metrics]{
		count: pmetric.Metrics.DataPointCount, split: splitMetrics, empty: pmetric.NewMetrics,
		moveTo: func(src, dst pmetric.Metrics) { src.ResourceMetrics().MoveAndAppendTo(dst.ResourceMetrics()) },
	}
)

// pending is a merged request being filled; done closes once it has been
// handed on, with err its answer.
type pending[T any] struct {
	data  T
	n     int
	timer *time.Timer
	done  chan struct{}
	err   error
}

// batcher merges requests in front of next (the stamp and the queue).
type batcher[T any] struct {
	cfg  BatchConfig
	k    kind[T]
	next func(context.Context, T) error

	mu     sync.Mutex
	cur    *pending[T]
	closed bool
}

func newBatcher[T any](cfg BatchConfig, k kind[T], next func(context.Context, T) error) *batcher[T] {
	return &batcher[T]{cfg: cfg, k: k, next: next}
}

// consume merges data (taking its contents) and returns once every merged
// request holding any of its items has been handed on, with the first error.
func (b *batcher[T]) consume(ctx context.Context, data T) error {
	n := b.k.count(data)
	b.mu.Lock()
	if b.closed || n == 0 {
		// Nothing to merge (an empty request, or one the edge must still
		// look at: a metric without points can be a permanent error).
		b.mu.Unlock()
		return b.next(ctx, data)
	}
	var waits, full []*pending[T]
	for n > 0 {
		if b.cur == nil {
			p := &pending[T]{data: b.k.empty(), done: make(chan struct{})}
			p.timer = time.AfterFunc(b.cfg.FlushTimeout, func() { b.flushIf(p) })
			b.cur = p
		}
		p := b.cur
		piece := data
		if room := b.cfg.MaxSize - p.n; b.cfg.MaxSize > 0 && n > room {
			piece = b.k.split(room, data)
		}
		m := b.k.count(piece)
		b.k.moveTo(piece, p.data)
		p.n += m
		n -= m
		waits = append(waits, p)
		if p.n >= b.cfg.MinSize || (b.cfg.MaxSize > 0 && p.n >= b.cfg.MaxSize) {
			p.timer.Stop()
			b.cur = nil
			full = append(full, p)
		}
	}
	b.mu.Unlock()
	for _, p := range full {
		b.flush(p)
	}
	var errs []error
	for _, p := range waits {
		<-p.done
		if p.err != nil {
			errs = append(errs, p.err)
		}
	}
	return errors.Join(errs...)
}

// flushIf flushes p if it is still the batch being filled (its timer).
func (b *batcher[T]) flushIf(p *pending[T]) {
	b.mu.Lock()
	if b.cur != p {
		b.mu.Unlock()
		return
	}
	b.cur = nil
	b.mu.Unlock()
	b.flush(p)
}

// flush hands p on with a context of its own: none of the callers' contexts
// (their deadlines must not cancel the others' enqueue, and their client
// metadata belongs to one sender each); the stamp adds received_at.
func (b *batcher[T]) flush(p *pending[T]) {
	p.err = b.next(context.Background(), p.data)
	close(p.done)
}

// shutdown flushes what is pending; later requests pass straight through.
func (b *batcher[T]) shutdown() {
	b.mu.Lock()
	b.closed = true
	p := b.cur
	b.cur = nil
	b.mu.Unlock()
	if p != nil {
		p.timer.Stop()
		b.flush(p)
	}
}

// The exporters with the batch step in front: merged requests go to the
// stamp (received.go), then exporterhelper's queue. MutatesData: the batcher
// takes the request's contents (the pipeline clones for other consumers).

type batchTraces struct {
	exporter.Traces
	b *batcher[ptrace.Traces]
}

func (x batchTraces) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return x.b.consume(ctx, td)
}
func (x batchTraces) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
func (x batchTraces) Shutdown(ctx context.Context) error {
	x.b.shutdown()
	return x.Traces.Shutdown(ctx)
}

type batchLogs struct {
	exporter.Logs
	b *batcher[plog.Logs]
}

func (x batchLogs) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return x.b.consume(ctx, ld)
}
func (x batchLogs) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
func (x batchLogs) Shutdown(ctx context.Context) error {
	x.b.shutdown()
	return x.Logs.Shutdown(ctx)
}

type batchMetrics struct {
	exporter.Metrics
	b *batcher[pmetric.Metrics]
}

func (x batchMetrics) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return x.b.consume(ctx, md)
}
func (x batchMetrics) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
func (x batchMetrics) Shutdown(ctx context.Context) error {
	x.b.shutdown()
	return x.Metrics.Shutdown(ctx)
}

var _ component.Component = batchTraces{}
