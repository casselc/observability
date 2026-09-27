package s3pqexporter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
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
	}
}

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
}

func acquire(id component.ID, cfg *Config, mp metric.MeterProvider) (*edge.Edge, error) {
	edgesMu.Lock()
	s := edges[id]
	if s == nil {
		s = &shared{}
		edges[id] = s
	}
	s.refs++
	edgesMu.Unlock()
	s.once.Do(func() {
		if s.e, s.err = edge.New(cfg.EdgeConfig()); s.err == nil && mp != nil {
			s.reg, s.err = registerOutcomes(mp, s.e.Stats())
		}
	})
	return s.e, s.err
}

func release(id component.ID, log *zap.Logger) {
	edgesMu.Lock()
	defer edgesMu.Unlock()
	s := edges[id]
	if s == nil {
		return
	}
	if s.refs--; s.refs == 0 {
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

// exp is one pipeline's view of the shared edge.
type exp struct {
	id  component.ID
	mp  metric.MeterProvider
	cfg *Config
	log *zap.Logger
	e   *edge.Edge
}

func (x *exp) start(context.Context, component.Host) error {
	e, err := acquire(x.id, x.cfg, x.mp)
	x.e = e
	return err
}

func (x *exp) shutdown(context.Context) error {
	if x.e != nil {
		release(x.id, x.log)
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
	return verdict(x.e.PushTraces(withReceived(ctx), td))
}
func (x *exp) logs(ctx context.Context, ld plog.Logs) error {
	return verdict(x.e.PushLogs(withReceived(ctx), ld))
}
func (x *exp) metrics(ctx context.Context, md pmetric.Metrics) error {
	return verdict(x.e.PushMetrics(withReceived(ctx), md))
}

func newExp(set exporter.Settings, cfg component.Config) (*exp, *Config, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, nil, errors.New("s3pq: unexpected config type")
	}
	return &exp{id: set.ID, mp: set.MeterProvider, cfg: c, log: set.Logger}, c, nil
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
	var out exporter.Traces = stampTraces{e, time.Now}
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
	var out exporter.Logs = stampLogs{e, time.Now}
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
	var out exporter.Metrics = stampMetrics{e, time.Now}
	if c.Batch.Enabled {
		out = batchMetrics{out, newBatcher(c.Batch, metricsKind, out.ConsumeMetrics)}
	}
	return out, nil
}
