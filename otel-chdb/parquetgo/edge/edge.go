// Package edge is the Go edge publisher: ClickStack-shaped Parquet (traces,
// logs) and metrics layout B (or the ClickStack metrics tables), committed
// to S3 manifest-less (../commit), exactly as the Rust edge (otap-rs,
// `urn:otel:exporter:s3pq`) does, so the one Rust consumer ingests both.
//
// Per request:
//
//   - content key: BLAKE3("{namespace}\0" + the OTLP request's protobuf);
//   - traces and logs: one object; metrics: one per non-empty namespace
//     (layout B: metrics_number_points, _histogram_points,
//     _exponential_histogram_points, _summary_points, plus metrics_series for
//     series not yet announced), each appended to its namespace's lane
//     `hash(content) mod lanes`, concurrently;
//   - the request succeeds only when every object has committed
//     (commit.RequestVerdict); an unresolved object makes the whole request
//     retryable, and the retry finds the committed parts in their lanes'
//     known sets; an undecodable request is a permanent error;
//   - layout B: a series counts as announced only once its series object has
//     committed, in the series lane's epoch.
//
// It knows nothing of the collector: ../s3pqexporter wraps it as the `s3pq`
// exporter, and the benchmarks and model checks drive it directly.
package edge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// SchemaVersion is the envelope's schema_version.
const SchemaVersion = 1

// Metrics layouts.
const (
	SeriesTable      = "series_table"      // layout B (default)
	ClickstackTables = "clickstack_tables" // the contrib exporter's five tables
)

// Config is the edge's configuration.
type Config struct {
	// S3 is the bucket URL and credentials (parquetgo.Config's S3 fields:
	// URL, keys or the default chain, Profile, RoleARN, S3Region,
	// PathStyle, CABundle, Transport). The URL's path is the data root:
	// objects go under
	// {root}/{Cluster}/{ProducerID}/{namespace}/{epoch}/{seq:020d}.parquet
	// (format v2, ../../FORMAT.md).
	S3 parquetgo.Config
	// Cluster is the first key segment, which write access is scoped by
	// (DECISIONS.md D18). Required.
	Cluster string
	// ProducerID is unique in the cluster and stable per persistent queue.
	ProducerID string
	// Lanes per namespace (default 1).
	Lanes int
	// MetricsLayout: SeriesTable (default) or ClickstackTables.
	MetricsLayout string
	Series        parquetgo.SeriesOptions
	// Parquet: traces, logs and layout-A options (default: EdgeParquet).
	Parquet     parquetgo.Options
	PutTimeout  time.Duration // default 10s
	HeadTimeout time.Duration // default 2s

	// Store replaces the S3 store (tests, the model checks); Prefix is then
	// the key prefix (with S3, the URL's path).
	Store    commit.Store
	Prefix   string
	Observer commit.Observer
	Mutation commit.Mutation
	// NewEpoch names epochs (default commit.NewEpoch).
	NewEpoch func() string
	// Now is the clock for received_at (default time.Now).
	Now func() time.Time
}

// EdgeParquet is the Rust edge's Parquet for traces and logs (parquet-rs
// defaults as otap-rs sets them): zstd, dictionaries except on the
// near-unique columns, chunk statistics and the page index with min/max cut
// to 64 bytes, no statistics in the page headers, a bloom filter on
// TraceId only.
func EdgeParquet() parquetgo.Options {
	o := parquetgo.DefaultOptions()
	o.BloomColumns = []string{"TraceId"}
	// otap-rs schema.rs HIGH_CARDINALITY: no dictionary on the near-unique leaves.
	o.PlainFor = append(o.PlainFor, "Value", "Sum", "Exemplars.TimeUnix.list.element", "Exemplars.Value.list.element",
		"Exemplars.SpanId.list.element", "Exemplars.TraceId.list.element")
	o.Statistics = false // page-header statistics; chunk statistics stay
	o.ColumnIndexLimit, o.TruncateStatistics = 64, 64
	return o
}

// Edge publishes requests. Safe for concurrent use.
type Edge struct {
	cfg      Config
	store    commit.Store
	prefix   string
	lanes    map[string][]*commit.Lane
	stats    *commit.Stats
	series   *parquetgo.SeriesEncoder
	encs     *parquetgo.FreeList[*parquetgo.PGEncoder]
}

// Namespaces are every lane namespace, in the Rust edge's order.
var Namespaces = []string{"traces", "logs",
	"metrics_gauge", "metrics_sum", "metrics_histogram", "metrics_exponential_histogram", "metrics_summary",
	parquetgo.SigNumberPoints, parquetgo.SigGaugePoints, parquetgo.SigSumPoints, parquetgo.SigHistogramPoints,
	parquetgo.SigExpHistogramPoints, parquetgo.SigSummaryPoints, parquetgo.SigSeries}

// New validates cfg and builds the lanes (no request is made).
func New(cfg Config) (*Edge, error) {
	if cfg.ProducerID == "" || cfg.Cluster == "" {
		return nil, errors.New("cluster and producer_id are required")
	}
	if !commit.ValidName(cfg.Cluster) || !commit.ValidName(cfg.ProducerID) {
		return nil, fmt.Errorf("cluster %q, producer_id %q: want [a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])? (a key segment, ../../FORMAT.md)", cfg.Cluster, cfg.ProducerID)
	}
	if cfg.Lanes <= 0 {
		cfg.Lanes = 1
	}
	switch cfg.MetricsLayout {
	case "":
		cfg.MetricsLayout = SeriesTable
	case SeriesTable, ClickstackTables:
	default:
		return nil, fmt.Errorf("metrics_layout %q: want %s or %s", cfg.MetricsLayout, SeriesTable, ClickstackTables)
	}
	if cfg.Series == (parquetgo.SeriesOptions{}) {
		cfg.Series = parquetgo.DefaultSeriesOptions()
	}
	if cfg.Parquet.Compression == "" && cfg.Parquet.DataPageSize == 0 {
		cfg.Parquet = EdgeParquet()
	}
	if cfg.PutTimeout <= 0 {
		cfg.PutTimeout = 10 * time.Second
	}
	if cfg.HeadTimeout <= 0 {
		cfg.HeadTimeout = 2 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	e := &Edge{cfg: cfg, store: cfg.Store, lanes: map[string][]*commit.Lane{}, stats: &commit.Stats{}}
	if e.store == nil {
		client, bucket, prefix, err := parquetgo.NewS3Client(cfg.S3)
		if err != nil {
			return nil, err
		}
		e.store, e.prefix = &commit.S3Store{Client: client, Bucket: bucket}, prefix
	} else {
		e.prefix = strings.Trim(cfg.Prefix, "/")
	}
	for _, ns := range Namespaces {
		p := commit.LanePrefix(e.prefix, cfg.Cluster, cfg.ProducerID, ns)
		for i := range cfg.Lanes {
			e.lanes[ns] = append(e.lanes[ns], &commit.Lane{
				Name: ns + "/" + strconv.Itoa(i), Prefix: p, Producer: cfg.ProducerID, Store: e.store,
				Timeouts: commit.Timeouts{Put: cfg.PutTimeout, Head: cfg.HeadTimeout},
				Stats:    e.stats, Observer: cfg.Observer, Mutation: cfg.Mutation, NewEpoch: cfg.NewEpoch,
			})
		}
	}
	if cfg.MetricsLayout == SeriesTable {
		e.series = parquetgo.NewSeriesEncoder(cfg.Series, cfg.Parquet)
	}
	e.encs = parquetgo.NewFreeList(64, func() *parquetgo.PGEncoder { return parquetgo.NewPGEncoder(cfg.Parquet) })
	return e, nil
}

// Stats are the lanes' counters (shared by every lane).
func (e *Edge) Stats() *commit.Stats { return e.stats }

// Lane returns a namespace's lanes (tests, observers).
func (e *Edge) Lane(ns string) []*commit.Lane { return e.lanes[ns] }

// SeriesCacheLen is layout B's cache size.
func (e *Edge) SeriesCacheLen() int {
	if e.series == nil {
		return 0
	}
	return e.series.CacheLen()
}

func (e *Edge) lane(ns, content string) *commit.Lane {
	ls := e.lanes[ns]
	if len(ls) == 1 {
		return ls[0]
	}
	v, _ := strconv.ParseUint(content[:16], 16, 64)
	return ls[v%uint64(len(ls))]
}

// PermanentError marks a request that can never be published (undecodable,
// a metric without a type): retrying can't help.
type PermanentError struct{ Err error }

func (p *PermanentError) Error() string { return p.Err.Error() }
func (p *PermanentError) Unwrap() error { return p.Err }

// IsPermanent reports whether err is permanent.
func IsPermanent(err error) bool {
	var p *PermanentError
	var ee *commit.ErrEncode
	return errors.As(err, &p) || errors.As(err, &ee)
}

// description is an object's S3 metadata (the lane adds kind, epoch, seq,
// content and producer); footer is the same plus those four.
func (e *Edge) description(ns string, rows int, minTS, maxTS, received uint64) map[string]string {
	return map[string]string{
		commit.MetaFormat:   strconv.Itoa(commit.FormatVersion),
		commit.MetaCluster:  e.cfg.Cluster,
		commit.MetaProducer: e.cfg.ProducerID,
		commit.MetaSignal:   ns,
		commit.MetaSchema:   strconv.Itoa(SchemaVersion),
		commit.MetaRows:     strconv.Itoa(rows),
		commit.MetaMinTime:  strconv.FormatUint(minTS, 10),
		commit.MetaMaxTime:  strconv.FormatUint(maxTS, 10),
		commit.MetaReceived: strconv.FormatUint(received, 10),
	}
}

func footerOf(meta map[string]string, r commit.Ref, content string) map[string]string {
	f := make(map[string]string, len(meta)+4)
	for k, v := range meta {
		// Where and when it was published is S3 metadata only, as the
		// Rust edge writes it (../../FORMAT.md §2).
		if k == commit.MetaFormat || k == commit.MetaCluster || k == commit.MetaLow {
			continue
		}
		f[k] = v
	}
	f[commit.MetaKind] = commit.KindData
	f[commit.MetaEpoch] = r.Epoch
	f[commit.MetaSeq] = strconv.FormatUint(r.Seq, 10)
	f[commit.MetaContent] = content
	return f
}

func (e *Edge) env(r commit.Ref, received uint64) *parquetgo.Envelope {
	return &parquetgo.Envelope{Producer: e.cfg.ProducerID, Epoch: r.Epoch, Batch: r.Seq, Received: received, Schema: SchemaVersion}
}

// pgObject encodes one object with the PGEncoder: walk (via enc) with the
// slot's envelope, the description into the footer once the rows are known.
func (e *Edge) pgObject(ns, content string, r commit.Ref, received uint64,
	walk func(*parquetgo.PGEncoder, *bytes.Buffer, *parquetgo.Envelope) (int, error)) (commit.Object, error) {
	enc := e.encs.Get()
	defer e.encs.Put(enc)
	var meta map[string]string
	enc.Footer = func(rows int, env *parquetgo.Envelope) map[string]string {
		meta = e.description(ns, rows, env.MinTS, env.MaxTS, received)
		return footerOf(meta, r, content)
	}
	buf := new(bytes.Buffer)
	buf.Grow(256 << 10)
	if _, err := walk(enc, buf, e.env(r, received)); err != nil {
		return commit.Object{}, err
	}
	body := buf.Bytes()
	if n := e.cfg.Parquet.TruncateStatistics; n > 0 {
		var err error
		if body, err = parquetgo.TruncateStatistics(body, n); err != nil {
			return commit.Object{}, err
		}
	}
	return commit.Object{Body: body, ContentType: commit.ParquetContentType, Meta: meta}, nil
}

func (e *Edge) now() uint64 { return uint64(e.cfg.Now().UnixNano()) }

type receivedKey struct{}

// WithReceived returns ctx carrying the request's received_at (ns since the
// Unix epoch): when the request entered the edge's durable custody. Behind a
// persistent queue that is the enqueue time, which the queue keeps with the
// request (../s3pqexporter stamps it and reads it back), so a retry and a
// replay after a restart carry the first value, and a replay lands in the
// original's toDate(received_at) partition, where the consumer's count
// check finds the original. Without it the edge stamps its own clock
// (Config.Now) when it is handed the request: the custody time when nothing
// holds the request before the edge. The value is beside the request, never
// in it: the content key and the rows are unchanged, and every object of the
// request, and every row, carries it.
func WithReceived(ctx context.Context, ns uint64) context.Context {
	return context.WithValue(ctx, receivedKey{}, ns)
}

// received is the request's received_at: WithReceived's, else now.
func (e *Edge) received(ctx context.Context) uint64 {
	if ns, ok := ctx.Value(receivedKey{}).(uint64); ok && ns > 0 {
		return ns
	}
	return e.now()
}

// PushTraces publishes td as one object.
func (e *Edge) PushTraces(ctx context.Context, td ptrace.Traces) error {
	if td.SpanCount() == 0 {
		return nil
	}
	received := e.received(ctx)
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		return &PermanentError{err}
	}
	content := commit.ContentHash("traces", b)
	_, err = e.lane("traces", content).Append(ctx, content, func(r commit.Ref) (commit.Object, error) {
		return e.pgObject("traces", content, r, received, func(enc *parquetgo.PGEncoder, buf *bytes.Buffer, env *parquetgo.Envelope) (int, error) {
			return enc.Traces(buf, td, env)
		})
	})
	return err
}

// PushLogs publishes ld as one object.
func (e *Edge) PushLogs(ctx context.Context, ld plog.Logs) error {
	if ld.LogRecordCount() == 0 {
		return nil
	}
	received := e.received(ctx)
	b, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		return &PermanentError{err}
	}
	content := commit.ContentHash("logs", b)
	_, err = e.lane("logs", content).Append(ctx, content, func(r commit.Ref) (commit.Object, error) {
		return e.pgObject("logs", content, r, received, func(enc *parquetgo.PGEncoder, buf *bytes.Buffer, env *parquetgo.Envelope) (int, error) {
			return enc.Logs(buf, ld, env)
		})
	})
	return err
}

// part is one object of a metrics request.
type part struct {
	ns, content string
	encode      commit.Encoder
	onCommit    func(commit.Ref)
}

// PushMetrics publishes md as one object per non-empty namespace, and
// succeeds only when all of them have committed.
func (e *Edge) PushMetrics(ctx context.Context, md pmetric.Metrics) error {
	if md.DataPointCount() == 0 {
		if _, err := parquetgo.MetricPoints(md); err != nil {
			return &PermanentError{err}
		}
		return nil
	}
	received := e.received(ctx)
	b, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		return &PermanentError{err}
	}
	var parts []part
	if e.series != nil {
		batch, err := e.series.Walk(md)
		if err != nil {
			return &PermanentError{err}
		}
		defer batch.Release()
		for _, o := range batch.Objects {
			var content string
			var onCommit func(commit.Ref)
			if o.Signal == parquetgo.SigSeries {
				// Keyed by its own rows: what a request announces depends on
				// the cache, so a retry may carry another series object.
				h := commit.NewRowsHasher(o.Signal)
				o.HashRows(h.Write)
				content = h.Sum()
				news := batch.New
				onCommit = func(r commit.Ref) { e.series.Announced(news, r.Epoch) }
			} else {
				content = commit.ContentHash(o.Signal, b)
			}
			parts = append(parts, part{ns: o.Signal, content: content, onCommit: onCommit,
				encode: func(r commit.Ref) (commit.Object, error) {
					meta := e.description(o.Signal, o.Rows, o.MinTS, o.MaxTS, received)
					buf := new(bytes.Buffer)
					buf.Grow(128 << 10)
					if err := o.Encode(buf, e.env(r, received), footerOf(meta, r, content)); err != nil {
						return commit.Object{}, err
					}
					return commit.Object{Body: buf.Bytes(), ContentType: commit.ParquetContentType, Meta: meta}, nil
				}})
		}
	} else {
		n, err := parquetgo.MetricPoints(md)
		if err != nil {
			return &PermanentError{err}
		}
		for t := range parquetgo.NumMetricTypes {
			if n[t] == 0 {
				continue
			}
			ns := parquetgo.MetricSignals[t]
			content := commit.ContentHash(ns, b)
			parts = append(parts, part{ns: ns, content: content, encode: func(r commit.Ref) (commit.Object, error) {
				return e.pgObject(ns, content, r, received, func(enc *parquetgo.PGEncoder, buf *bytes.Buffer, env *parquetgo.Envelope) (int, error) {
					return enc.MetricsOf(buf, md, t, env)
				})
			}})
		}
	}
	outs := make([]commit.PartOutcome, len(parts))
	var wg sync.WaitGroup
	for i, p := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.lane(p.ns, p.content).Append(ctx, p.content, p.encode)
			if err == nil && p.onCommit != nil {
				p.onCommit(r)
			}
			outs[i] = commit.PartOutcome{Signal: p.ns, Ref: r, Err: err}
		}()
	}
	wg.Wait()
	switch v, err := commit.RequestVerdict(outs, e.cfg.Mutation); v {
	case commit.Ack:
		return nil
	case commit.Reject:
		return &PermanentError{err}
	default:
		return err
	}
}
