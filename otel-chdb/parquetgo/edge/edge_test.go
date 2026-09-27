package edge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func traces(n int, seed int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := range n {
		s := ss.Spans().AppendEmpty()
		s.SetName(fmt.Sprintf("op-%d-%d", seed, i))
		s.SetTraceID(pcommon.TraceID{byte(seed), byte(i), 1})
		s.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(i)))
		s.SetEndTimestamp(pcommon.Timestamp(1_700_000_000_000_000_100 + int64(i)))
	}
	return td
}

func metrics(seed int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	ts := pcommon.Timestamp(1_700_000_000_000_000_000 + int64(seed)*1e9)
	g := sm.Metrics().AppendEmpty()
	g.SetName("g")
	gd := g.SetEmptyGauge()
	for i := range 3 {
		dp := gd.DataPoints().AppendEmpty()
		dp.Attributes().PutInt("i", int64(i))
		dp.SetTimestamp(ts)
		dp.SetDoubleValue(float64(seed))
	}
	s := sm.Metrics().AppendEmpty()
	s.SetName("s")
	dp := s.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(int64(seed))
	h := sm.Metrics().AppendEmpty()
	h.SetName("h")
	hp := h.SetEmptyHistogram().DataPoints().AppendEmpty()
	hp.SetTimestamp(ts)
	hp.ExplicitBounds().FromRaw([]float64{1})
	hp.BucketCounts().FromRaw([]uint64{1, uint64(seed)})
	return md
}

func newEdge(t *testing.T, st commit.Store, layout string) *Edge {
	t.Helper()
	var n atomic.Int64
	e, err := New(Config{Store: st, Prefix: "root", Cluster: "c1", ProducerID: "p1", MetricsLayout: layout,
		PutTimeout: time.Second, HeadTimeout: time.Second,
		NewEpoch: func() string { return fmt.Sprintf("20260926T000000.000Z-%08x", n.Add(1)) },
		Now:      func() time.Time { return time.Unix(0, 1_790_000_000_123_456_789) }})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func footer(t *testing.T, b []byte) map[string]string {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, kv := range f.Metadata().KeyValueMetadata {
		out[kv.Key] = kv.Value
	}
	return out
}

// The object's key, metadata and footer are the Rust edge's; received_at is
// the metadata's value in every row (the consumer asserts it).
func TestTracesObject(t *testing.T) {
	st := commit.NewMemStore()
	e := newEdge(t, st, "")
	td := traces(5, 1)
	if err := e.PushTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	keys := st.Keys("root/c1/p1/traces/")
	if len(keys) != 1 || keys[0] != "root/c1/p1/traces/20260926T000000.000Z-00000001/00000000000000000000.parquet" {
		t.Fatal(keys)
	}
	o, _ := st.Get(keys[0])
	b, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	want := map[string]string{
		"oscope-format": "2", "oscope-cluster": "c1", "oscope-kind": "data", "oscope-producer": "p1", "oscope-epoch": "20260926T000000.000Z-00000001", "oscope-seq": "0",
		"oscope-content": commit.ContentHash("traces", b), "oscope-signal": "traces", "oscope-schema": "1", "oscope-rows": "5",
		"oscope-min-time": "1700000000000000000", "oscope-max-time": "1700000000000000004", "oscope-received": "1790000000123456789",
	}
	for k, v := range want {
		if o.Meta[k] != v {
			t.Errorf("meta %s = %q, want %q", k, o.Meta[k], v)
		}
	}
	// oscope-low: at most the request's own received_at (it is in the edge's hands).
	if l, err := strconv.ParseUint(o.Meta[commit.MetaLow], 10, 64); err != nil || l > 1790000000123456789 {
		t.Errorf("oscope-low %q", o.Meta[commit.MetaLow])
	}
	if len(o.Meta) != len(want)+1 {
		t.Errorf("meta %v", o.Meta)
	}
	f := footer(t, o.Body)
	delete(want, commit.MetaFormat)
	delete(want, commit.MetaCluster)
	for k, v := range want {
		if f[k] != v {
			t.Errorf("footer %s = %q, want %q", k, f[k], v)
		}
	}
	rows, err := parquet.Read[struct {
		ReceivedAt int64  `parquet:"received_at"`
		Epoch      string `parquet:"producer_epoch"`
		Batch      uint64 `parquet:"batch_id"`
	}](bytes.NewReader(o.Body), int64(len(o.Body)))
	if err != nil || len(rows) != 5 {
		t.Fatal(err, len(rows))
	}
	for _, r := range rows {
		if strconv.FormatInt(r.ReceivedAt, 10) != want["oscope-received"] || r.Epoch != want["oscope-epoch"] || r.Batch != 0 {
			t.Fatal(r)
		}
	}
	// A retry of the committed request: no request to S3.
	puts := st.Puts
	if err := e.PushTraces(context.Background(), td); err != nil || st.Puts != puts {
		t.Fatal(err, st.Puts, puts)
	}
	// Another request, ambiguous PUT: resolved as ours.
	st.Inject(commit.ApplyLoseAnswer)
	if err := e.PushTraces(context.Background(), traces(3, 2)); err != nil {
		t.Fatal(err)
	}
	if n := len(st.Keys("root/c1/p1/traces/")); n != 2 || e.Stats().ResolvedOwn.Load() != 1 {
		t.Fatal(n)
	}
}

func TestLogsAndEmpty(t *testing.T) {
	st := commit.NewMemStore()
	e := newEdge(t, st, "")
	ld := plog.NewLogs()
	if err := e.PushLogs(context.Background(), ld); err != nil || len(st.Keys("")) != 0 {
		t.Fatal(err)
	}
	r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	r.Body().SetStr("hello")
	if err := e.PushLogs(context.Background(), ld); err != nil || len(st.Keys("root/c1/p1/logs/")) != 1 {
		t.Fatal(err, st.Keys(""))
	}
}

// Layout B: the points objects and a series object; the series object's
// series count as announced only once it commits; a request is acked only
// when every object committed, and its retry skips the parts that did.
func TestMetricsSeriesLayout(t *testing.T) {
	st := commit.NewMemStore()
	e := newEdge(t, st, SeriesTable)
	ctx := context.Background()
	md := metrics(1)
	if err := e.PushMetrics(ctx, md); err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{parquetgo.SigNumberPoints, parquetgo.SigHistogramPoints, parquetgo.SigSeries} {
		if n := len(st.Keys("root/c1/p1/" + ns + "/")); n != 1 {
			t.Fatalf("%s: %d objects", ns, n)
		}
	}
	if n := e.SeriesCacheLen(); n != 5 {
		t.Fatal("announced", n)
	}
	// The same series again: no series object.
	if err := e.PushMetrics(ctx, metrics(2)); err != nil {
		t.Fatal(err)
	}
	if n := len(st.Keys("root/c1/p1/" + parquetgo.SigSeries + "/")); n != 1 {
		t.Fatal("re-announced", n)
	}
	// New series whose series object stays unresolved: the request fails,
	// the cache doesn't learn them, and the retry announces them again.
	md3 := metrics(3)
	md3.ResourceMetrics().At(0).Resource().Attributes().PutStr("pod", "b")
	lanes := e.Lane(parquetgo.SigSeries)
	st.Inject(commit.Hold) // hits whichever part PUTs first; hold them all
	st.Inject(commit.Hold)
	st.Inject(commit.Hold)
	st.Inject(commit.HeadFail)
	st.Inject(commit.HeadFail)
	st.Inject(commit.HeadFail)
	err := e.PushMetrics(ctx, md3)
	var un *commit.ErrUnresolved
	if !errors.As(err, &un) || IsPermanent(err) {
		t.Fatal("want unresolved, got", err)
	}
	if n := e.SeriesCacheLen(); n != 5 {
		t.Fatal("announced before commit", n)
	}
	if _, _, p := lanes[0].State(); p != commit.Unresolved {
		t.Fatal(p)
	}
	st.ReleaseHeld() // the held PUTs land late
	if err := e.PushMetrics(ctx, md3); err != nil {
		t.Fatal(err)
	}
	if n := e.SeriesCacheLen(); n != 10 {
		t.Fatal("announced after commit", n)
	}
	for _, k := range st.Keys("root/c1/p1/") {
		if !strings.HasSuffix(k, ".parquet") {
			t.Fatal(k)
		}
	}
	// A metric without a type: permanent.
	bad := pmetric.NewMetrics()
	bad.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("x")
	if err := e.PushMetrics(ctx, bad); !IsPermanent(err) {
		t.Fatal(err)
	}
}

func TestMetricsClickstackLayout(t *testing.T) {
	st := commit.NewMemStore()
	e := newEdge(t, st, ClickstackTables)
	if err := e.PushMetrics(context.Background(), metrics(1)); err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"metrics_gauge", "metrics_sum", "metrics_histogram"} {
		ks := st.Keys("root/c1/p1/" + ns + "/")
		if len(ks) != 1 {
			t.Fatalf("%s: %v", ns, ks)
		}
		o, _ := st.Get(ks[0])
		if f := footer(t, o.Body); f["oscope-signal"] != ns || f["oscope-rows"] != o.Meta["oscope-rows"] {
			t.Fatal(f)
		}
	}
}

// A tombstone in the next slot: the lane halts and continues in a new epoch.
func TestHaltOnTombstone(t *testing.T) {
	st := commit.NewMemStore()
	e := newEdge(t, st, "")
	ctx := context.Background()
	if err := e.PushTraces(ctx, traces(1, 1)); err != nil {
		t.Fatal(err)
	}
	ep := e.Lane("traces")[0].Epoch()
	st.Tomb(commit.SlotKey("root/c1/p1/traces", ep, 1))
	if err := e.PushTraces(ctx, traces(1, 2)); err != nil {
		t.Fatal(err)
	}
	if e.Lane("traces")[0].Epoch() == ep || e.Stats().Halted.Load() != 1 {
		t.Fatal("no halt")
	}
}

// received_at is the custody time the caller hands over (WithReceived), not
// the edge's clock: a replay by a later incarnation, days later, carries the
// original's value into every object and row; the content key is unchanged.
func TestReplayKeepsReceived(t *testing.T) {
	const t0 = uint64(1_780_000_000_000_000_001)
	st := commit.NewMemStore()
	var n atomic.Int64
	incarnation := func() *Edge {
		e, err := New(Config{Store: st, Prefix: "root", Cluster: "c1", ProducerID: "p1", PutTimeout: time.Second, HeadTimeout: time.Second,
			NewEpoch: func() string { return fmt.Sprintf("20260926T000000.000Z-%08x", n.Add(1)) },
			// four days after the request was queued
			Now: func() time.Time { return time.Unix(0, int64(t0)).Add(96 * time.Hour) }})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	ctx := WithReceived(context.Background(), t0)
	td, md := traces(4, 7), metrics(7)
	for range 2 { // the original, then a replay by a new incarnation (new epoch)
		e := incarnation()
		if err := e.PushTraces(ctx, td); err != nil {
			t.Fatal(err)
		}
		if err := e.PushMetrics(ctx, md); err != nil {
			t.Fatal(err)
		}
	}
	keys := st.Keys("root/c1/p1/")
	contents := map[string]map[string]bool{} // namespace -> content keys
	epochs := map[string]bool{}
	for _, k := range keys {
		o, _ := st.Get(k)
		if o.Meta["oscope-received"] != strconv.FormatUint(t0, 10) {
			t.Errorf("%s: received %s, want %d", k, o.Meta["oscope-received"], t0)
		}
		if footer(t, o.Body)["oscope-received"] != o.Meta["oscope-received"] {
			t.Errorf("%s: footer and metadata differ", k)
		}
		rows, err := parquet.Read[struct {
			ReceivedAt int64 `parquet:"received_at"`
		}](bytes.NewReader(o.Body), int64(len(o.Body)))
		if err != nil || len(rows) == 0 {
			t.Fatal(k, err)
		}
		for _, r := range rows {
			if uint64(r.ReceivedAt) != t0 {
				t.Fatalf("%s: row received_at %d", k, r.ReceivedAt)
			}
		}
		ns := o.Meta["oscope-signal"]
		if contents[ns] == nil {
			contents[ns] = map[string]bool{}
		}
		contents[ns][o.Meta["oscope-content"]] = true
		epochs[o.Meta["oscope-epoch"]] = true
	}
	// Each namespace's two copies share one content key; the series object
	// is announced again in the new epoch (its own rows' key, also equal).
	for ns, c := range contents {
		if len(c) != 1 {
			t.Errorf("%s: %d content keys", ns, len(c))
		}
	}
	if len(keys) < 4 || len(epochs) < 2 {
		t.Fatalf("keys %v", keys)
	}
	// No custody time handed over: the edge's clock.
	st2 := commit.NewMemStore()
	e := newEdge(t, st2, "")
	if err := e.PushTraces(context.Background(), traces(1, 9)); err != nil {
		t.Fatal(err)
	}
	o, _ := st2.Get(st2.Keys("")[0])
	if o.Meta["oscope-received"] != "1790000000123456789" {
		t.Fatal(o.Meta)
	}
}
