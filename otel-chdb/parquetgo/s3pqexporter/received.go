package s3pqexporter

import (
	"context"
	"strconv"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ReceivedKey is the client-metadata key that carries a request's
// received_at (decimal ns since the Unix epoch) from the moment the exporter
// takes the request to the moment it publishes it.
//
// received_at is when the request entered the edge's durable custody. The
// exporter stamps it as it takes the request, before its sending queue, and
// the queue keeps it: the persistent queue (file_storage) stores a request
// with its context's client metadata (pdata/xpdata/request), and the memory
// queue and the retry sender pass the context on. So a retry, and a replay
// after a restart of a request whose commit landed but whose removal from the
// queue did not, carry the first value; a replay then lands in the original's
// toDate(received_at) partition, where the consumer's count check finds the
// original however long the edge was down. With the queue disabled the stamp
// is the receive time, as before.
//
// The stamp replaces any value a client sent under this key (receivers with
// include_metadata copy request headers into client metadata). Nothing but
// the exporter reads it: the request's bytes, and so the content key and the
// rows, are unchanged.
const ReceivedKey = "x-s3pq-received-at"

// stampReceived returns ctx with ReceivedKey set to now, keeping the rest of
// the client info.
func stampReceived(ctx context.Context, now time.Time) context.Context {
	info := client.FromContext(ctx)
	md := map[string][]string{}
	for k := range info.Metadata.Keys() {
		md[k] = info.Metadata.Get(k)
	}
	md[ReceivedKey] = []string{strconv.FormatInt(now.UnixNano(), 10)}
	info.Metadata = client.NewMetadata(md)
	return client.NewContext(ctx, info)
}

// withReceived hands the stamp to the edge (edge.WithReceived). A request
// without one (queued by an older version, or stamped by nobody) is stamped
// by the edge's clock.
func withReceived(ctx context.Context) context.Context {
	v := client.FromContext(ctx).Metadata.Get(ReceivedKey)
	if len(v) != 1 {
		return ctx
	}
	ns, err := strconv.ParseUint(v[0], 10, 64)
	if err != nil || ns == 0 {
		return ctx
	}
	return edge.WithReceived(ctx, ns)
}

// The exporters exporterhelper builds, with the stamp applied as each
// request is handed to them (before the queue), the request entered in the
// custody ledger (custody.go), and, once started, their queue's probe.
type stampTraces struct {
	exporter.Traces
	now func() time.Time
	c   *custody
}

func (s stampTraces) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	ctx, id := s.c.take(ctx, s.now())
	err := s.Traces.ConsumeTraces(ctx, td)
	if err != nil {
		s.c.done(id) // refused: the sender keeps it
	}
	return err
}

func (s stampTraces) Start(ctx context.Context, host component.Host) error {
	if err := s.Traces.Start(ctx, host); err != nil {
		return err
	}
	return s.Traces.ConsumeTraces(probeCtx(ctx), probeTraces())
}

type stampLogs struct {
	exporter.Logs
	now func() time.Time
	c   *custody
}

func (s stampLogs) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	ctx, id := s.c.take(ctx, s.now())
	err := s.Logs.ConsumeLogs(ctx, ld)
	if err != nil {
		s.c.done(id)
	}
	return err
}

func (s stampLogs) Start(ctx context.Context, host component.Host) error {
	if err := s.Logs.Start(ctx, host); err != nil {
		return err
	}
	return s.Logs.ConsumeLogs(probeCtx(ctx), probeLogs())
}

type stampMetrics struct {
	exporter.Metrics
	now func() time.Time
	c   *custody
}

func (s stampMetrics) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	ctx, id := s.c.take(ctx, s.now())
	err := s.Metrics.ConsumeMetrics(ctx, md)
	if err != nil {
		s.c.done(id)
	}
	return err
}

func (s stampMetrics) Start(ctx context.Context, host component.Host) error {
	if err := s.Metrics.Start(ctx, host); err != nil {
		return err
	}
	return s.Metrics.ConsumeMetrics(probeCtx(ctx), probeMetrics())
}
