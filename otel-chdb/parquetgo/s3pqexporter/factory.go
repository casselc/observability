package s3pqexporter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Type is the component type: `s3pq`, as the Rust exporter's URN.
var Type = component.MustNewType("s3pq")

const stability = component.StabilityLevelAlpha

// NewFactory returns the exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(Type, createDefaultConfig,
		exporter.WithTraces(createTraces, stability),
		exporter.WithLogs(createLogs, stability),
		exporter.WithMetrics(createMetrics, stability))
}

func createDefaultConfig() component.Config {
	backoff := configretry.NewDefaultBackOffConfig()
	backoff.MaxElapsedTime = 0 // never drop a request (DECISIONS.md D4)
	return &Config{
		TimeoutConfig: exporterhelper.TimeoutConfig{Timeout: 30 * time.Second},
		QueueSettings: configoptional.Default(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig: backoff,
		Batch:         defaultBatchConfig(),
		Lanes:         1,
		MetricsLayout: edge.SeriesTable,
		Series:        parquetgo.DefaultSeriesOptions(),
		S3:            S3Config{Region: "us-east-1", PutTimeout: 10 * time.Second, HeadTimeout: 2 * time.Second},
		Heartbeat:     HeartbeatConfig{Interval: 30 * time.Second, BirthTimeout: 30 * time.Second},
		Resources:     ResourcesConfig{Announce: true, Window: time.Hour, CacheSize: 65536},
		// DECISIONS.md D36: on, 2 KiB, 8 MiB, the GenAI content keys.
		Offload: defaultOffloadConfig(),
		// DECISIONS.md D31: above a 5-minute clock skew with margin.
		LateSplitAfter: 15 * time.Minute,
	}
}

// storeForTests replaces the S3 store of every edge acquired while set
// (tests only; custody_test.go).
var storeForTests commit.Store

// One edge per exporter configuration: the traces, logs and metrics
// pipelines of one `s3pq` share its lanes and series cache, as the Rust
// exporter node does.
var (
	edgesMu sync.Mutex
	edges   = map[component.ID]*shared{}
)

type shared struct {
	once sync.Once
	e    *edge.Edge
	err  error
	refs int
	reg  metric.Registration // s3pq_commit_outcomes (telemetry.go)
	stop context.CancelFunc  // the heartbeats
	done chan struct{}       // closed once the heartbeat loop has returned
}

// heartbeats runs the edge's heartbeat loop (edge.Heartbeats: births, then
// a heartbeat per idle lane) and closes done when it has returned.
func heartbeats(ctx context.Context, e *edge.Edge, hb HeartbeatConfig, log *zap.Logger, done chan struct{}) {
	var warn func(string, error)
	if log != nil {
		warn = func(ns string, err error) { log.Warn("s3pq heartbeat", zap.String("lane", ns), zap.Error(err)) }
	}
	born, loop := e.Heartbeats(ctx, hb.Interval, hb.BirthTimeout, warn)
	if log != nil {
		log.Info("s3pq births", zap.Int("registered", born), zap.Int("lanes", len(e.Registered())))
	}
	go func() {
		<-loop
		close(done)
	}()
}

func acquire(id component.ID, cfg *Config, mp metric.MeterProvider, log *zap.Logger) (*edge.Edge, error) {
	edgesMu.Lock()
	s := edges[id]
	if s == nil {
		s = &shared{}
		edges[id] = s
	}
	s.refs++
	edgesMu.Unlock()
	s.once.Do(func() {
		ec := cfg.EdgeConfig()
		if storeForTests != nil {
			ec.Store, ec.Prefix = storeForTests, "root"
		}
		ec.Custody = custodyFor(id).lowNow
		if s.e, s.err = edge.New(ec); s.err == nil && mp != nil {
			s.reg, s.err = registerOutcomes(mp, s.e.Stats(), s.e.OffloadStats)
		}
		if s.err == nil && cfg.Heartbeat.Interval > 0 {
			var ctx context.Context
			ctx, s.stop = context.WithCancel(context.Background())
			s.done = make(chan struct{})
			heartbeats(ctx, s.e, cfg.Heartbeat, log, s.done)
		}
	})
	return s.e, s.err
}

// release drops one pipeline's reference; the last one (every pipeline's
// sending queue already shut down: exporterhelper shuts the queue before
// this) stops the heartbeats and, if the custody is empty, commits the
// orderly close (../../FORMAT.md §3.1, DECISIONS.md D35) within ctx.
func release(ctx context.Context, id component.ID, log *zap.Logger) {
	edgesMu.Lock()
	defer edgesMu.Unlock()
	s := edges[id]
	if s == nil {
		return
	}
	if s.refs--; s.refs == 0 {
		if s.stop != nil {
			s.stop()
			// A heartbeat in flight must not land after the close.
			select {
			case <-s.done:
			case <-ctx.Done():
			}
		}
		if s.e != nil {
			closeEdge(ctx, s.e, custodyFor(id), s.stop != nil && s.done != nil && isClosed(s.done), log)
		}
		if s.e != nil {
			st := s.e.Stats()
			log.Info("s3pq stop",
				zap.Int64("committed", st.Committed.Load()), zap.Int64("resolved_own", st.ResolvedOwn.Load()),
				zap.Int64("resent", st.Resent.Load()), zap.Int64("learned_other", st.LearnedOther.Load()),
				zap.Int64("halted", st.Halted.Load()), zap.Int64("known_skipped", st.KnownSkipped.Load()),
				zap.Int64("unresolved", st.Unresolved.Load()), zap.Int64("inconsistent", st.Inconsistent.Load()),
				zap.Int64("encodes", st.Encodes.Load()), zap.Int64("puts", st.Puts.Load()), zap.Int64("heads", st.Heads.Load()))
		}
		if s.reg != nil {
			_ = s.reg.Unregister()
		}
		delete(edges, id)
	}
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// closer is what closeEdge needs of the edge (a test double in factory_test.go).
type closer interface {
	Close(ctx context.Context) (int, error)
}

// closeEdge commits the orderly close when it may: heartbeats were on (the
// lanes were born) and have stopped, and the custody ledger is empty (every
// request taken was committed or refused, every queue's probe came out).
// Otherwise, or past ctx's deadline, no close: the lanes stay stale, which
// is safe (../../FORMAT.md §3.1).
func closeEdge(ctx context.Context, e closer, c *custody, beatsStopped bool, log *zap.Logger) (int, bool) {
	if !beatsStopped || !c.empty() {
		if log != nil {
			log.Info("s3pq close: none", zap.Bool("heartbeats_stopped", beatsStopped), zap.Bool("custody_empty", c.empty()))
		}
		return 0, false
	}
	n, err := e.Close(ctx)
	if log != nil {
		if err != nil {
			log.Warn("s3pq close: some lanes not closed (they stay open: safe)", zap.Int("closed", n), zap.Error(err))
		} else {
			log.Info("s3pq close: custody empty, lanes closed", zap.Int("closed", n))
		}
	}
	return n, err == nil
}

// exp is one pipeline's view of the shared edge.
type exp struct {
	id  component.ID
	mp  metric.MeterProvider
	cfg *Config
	log *zap.Logger
	e   *edge.Edge
	c   *custody
}

func (x *exp) start(context.Context, component.Host) error {
	e, err := acquire(x.id, x.cfg, x.mp, x.log)
	x.e = e
	return err
}

func (x *exp) shutdown(ctx context.Context) error {
	if x.e != nil {
		release(ctx, x.id, x.log)
	}
	return nil
}

// verdict maps the edge's answer onto the collector's: nil once every
// object committed; permanent for data that can never be encoded (the
// request is dropped, logged); anything else is retried with the same
// request, which resolves the unresolved slot first.
func verdict(err error) error {
	if err == nil {
		return nil
	}
	if edge.IsPermanent(err) {
		return consumererror.NewPermanent(err)
	}
	return err
}

func (x *exp) traces(ctx context.Context, td ptrace.Traces) error {
	return x.c.push(ctx, "traces", func(ctx context.Context) error { return verdict(x.e.PushTraces(ctx, td)) })
}
func (x *exp) logs(ctx context.Context, ld plog.Logs) error {
	return x.c.push(ctx, "logs", func(ctx context.Context) error { return verdict(x.e.PushLogs(ctx, ld)) })
}
func (x *exp) metrics(ctx context.Context, md pmetric.Metrics) error {
	return x.c.push(ctx, "metrics", func(ctx context.Context) error { return verdict(x.e.PushMetrics(ctx, md)) })
}

func newExp(set exporter.Settings, cfg component.Config) (*exp, *Config, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, nil, errors.New("s3pq: unexpected config type")
	}
	return &exp{id: set.ID, mp: set.MeterProvider, cfg: c, log: set.Logger, c: custodyFor(set.ID)}, c, nil
}

func options(x *exp, c *Config) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithStart(x.start),
		exporterhelper.WithShutdown(x.shutdown),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	}
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	x, c, err := newExp(set, cfg)
	if err != nil {
		return nil, err
	}
	e, err := exporterhelper.NewTraces(ctx, set, cfg, x.traces, options(x, c)...)
	if err != nil {
		return nil, err
	}
	x.c.expect("traces")
	var out exporter.Traces = stampTraces{e, time.Now, x.c}
	if c.Batch.Enabled {
		out = batchTraces{out, newBatcher(c.Batch, tracesKind, out.ConsumeTraces)}
	}
	return out, nil
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	x, c, err := newExp(set, cfg)
	if err != nil {
		return nil, err
	}
	e, err := exporterhelper.NewLogs(ctx, set, cfg, x.logs, options(x, c)...)
	if err != nil {
		return nil, err
	}
	x.c.expect("logs")
	var out exporter.Logs = stampLogs{e, time.Now, x.c}
	if c.Batch.Enabled {
		out = batchLogs{out, newBatcher(c.Batch, logsKind, out.ConsumeLogs)}
	}
	return out, nil
}

func createMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	x, c, err := newExp(set, cfg)
	if err != nil {
		return nil, err
	}
	e, err := exporterhelper.NewMetrics(ctx, set, cfg, x.metrics, options(x, c)...)
	if err != nil {
		return nil, err
	}
	x.c.expect("metrics")
	var out exporter.Metrics = stampMetrics{e, time.Now, x.c}
	if c.Batch.Enabled {
		out = batchMetrics{out, newBatcher(c.Batch, metricsKind, out.ConsumeMetrics)}
	}
	return out, nil
}
