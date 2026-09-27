package s3pqexporter

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"
)

type captureTraces struct {
	exporter.Traces
	ctx context.Context
}

func (c *captureTraces) ConsumeTraces(ctx context.Context, _ ptrace.Traces) error {
	c.ctx = ctx
	return nil
}

// The stamp is applied as the request is handed to the exporter, survives
// the persistent queue's encoding of the request (what a restart reads
// back), replaces a client's value under the key, keeps the other client
// metadata, and becomes the object's received_at however much later the
// edge publishes it.
func TestReceivedSurvivesTheQueue(t *testing.T) {
	enq := time.Unix(0, 1_780_000_000_000_000_042)
	in := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{
		"x-tenant": {"a"}, ReceivedKey: {"1"}, // a client's header under our key is overwritten
	})})
	c := &captureTraces{}
	td := ptrace.NewTraces()
	s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetName("x")
	s.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
	if err := (stampTraces{c, func() time.Time { return enq }}).ConsumeTraces(in, td); err != nil {
		t.Fatal(err)
	}
	// The persistent queue's encoding (queuebatch tracesEncoding): request + context.
	b, err := pdatareq.MarshalTraces(c.ctx, td)
	if err != nil {
		t.Fatal(err)
	}
	out, td2, err := pdatareq.UnmarshalTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	md := client.FromContext(out).Metadata
	if got := md.Get(ReceivedKey); len(got) != 1 || got[0] != strconv.FormatInt(enq.UnixNano(), 10) {
		t.Fatalf("stamp %v", got)
	}
	if got := md.Get("x-tenant"); len(got) != 1 || got[0] != "a" {
		t.Fatalf("tenant %v", got)
	}
	// Published four days later (a replay after a long outage).
	st := commit.NewMemStore()
	e, err := edge.New(edge.Config{Store: st, Prefix: "p", Cluster: "c", ProducerID: "p", PutTimeout: time.Second, HeadTimeout: time.Second,
		NewEpoch: func() string { return "E2" }, Now: func() time.Time { return enq.Add(96 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.PushTraces(withReceived(out), td2); err != nil {
		t.Fatal(err)
	}
	o, _ := st.Get(st.Keys("")[0])
	if o.Meta[commit.MetaReceived] != fmt.Sprint(enq.UnixNano()) {
		t.Fatalf("received %s", o.Meta[commit.MetaReceived])
	}
	// A request queued without a stamp (an older version's queue): the edge's clock.
	if withReceived(context.Background()) != context.Background() {
		t.Fatal("unstamped context changed")
	}
}

// The stamp's cost per request: stamping as the request is taken, reading
// it back before publishing, and the bytes it adds to the queue's record.
func BenchmarkReceivedStamp(b *testing.B) {
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("x")
	base := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-tenant": {"a"}})})
	plain, _ := pdatareq.MarshalTraces(base, td)
	stamped, _ := pdatareq.MarshalTraces(stampReceived(base, time.Now()), td)
	now := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		_ = withReceived(stampReceived(base, now))
	}
	b.ReportMetric(float64(len(stamped)-len(plain)), "queue-bytes/req")
}
