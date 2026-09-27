package s3pqexporter

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func oneSpan(name string) ptrace.Traces {
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName(name)
	return td
}

func lowOf(t *testing.T, st *commit.MemStore, key string) uint64 {
	t.Helper()
	o, ok := st.Get(key)
	if !ok {
		t.Fatalf("no object %s", key)
	}
	l, err := strconv.ParseUint(o.Meta[commit.MetaLow], 10, 64)
	if err != nil {
		t.Fatalf("%s: oscope-low %q", key, o.Meta[commit.MetaLow])
	}
	return l
}

// A backlog in the queue drained out of order: an object's low is the
// oldest received_at still in custody when it was encoded (not its own),
// and once the queue is empty a heartbeat's low is its send time. Before
// the queue's probe came out (requests from before a restart may be in it)
// every low is 0.
func TestOscopeLowFollowsCustody(t *testing.T) {
	c := custodyFor(component.MustNewIDWithName("s3pq", "lowtest"))
	c.expect("traces")
	clock := time.Unix(0, 1_000_000_000_000)
	st := commit.NewMemStore()
	e, err := edge.New(edge.Config{Store: st, Prefix: "r", Cluster: "c1", ProducerID: "p1", PutTimeout: time.Second,
		HeadTimeout: time.Second, NewEpoch: func() string { return "E1" }, Now: func() time.Time { return clock },
		Custody: func() uint64 { return c.low(uint64(clock.UnixNano())) }})
	if err != nil {
		t.Fatal(err)
	}
	key := func(seq uint64) string { return commit.SlotKey("r/c1/p1/traces", "E1", seq) }
	// Birth, before the probe: low 0.
	if err := e.Beat(context.Background(), "traces"); err != nil || lowOf(t, st, key(0)) != 0 {
		t.Fatalf("birth: %v low %d", err, lowOf(t, st, key(0)))
	}
	// Three requests enter custody at t, t+1s, t+2s (the stamp, before the queue).
	var ctxs []context.Context
	for i := range 3 {
		ctx, _ := c.take(context.Background(), clock.Add(time.Duration(i)*time.Second))
		ctxs = append(ctxs, ctx)
	}
	t0 := uint64(clock.UnixNano())
	clock = clock.Add(5 * time.Second)
	// The probe comes out of the queue: drained.
	if err := c.push(probeCtx(context.Background()), "traces", nil); err != nil {
		t.Fatal(err)
	}
	// The newest first: its low is the oldest still held.
	push := func(i int) error {
		return c.push(ctxs[i], "traces", func(ctx context.Context) error { return e.PushTraces(ctx, oneSpan(strconv.Itoa(i))) })
	}
	if err := push(2); err != nil || lowOf(t, st, key(1)) != t0 {
		t.Fatalf("out of order: low %d, want %d", lowOf(t, st, key(1)), t0)
	}
	if err := push(0); err != nil || lowOf(t, st, key(2)) != t0 {
		t.Fatalf("the oldest itself: low %d, want %d (its own received_at bounds it)", lowOf(t, st, key(2)), t0)
	}
	if err := push(1); err != nil || lowOf(t, st, key(3)) != t0+1e9 {
		t.Fatalf("drained to the last: low %d", lowOf(t, st, key(3)))
	}
	// Custody empty: a heartbeat carries its send time.
	if err := e.Beat(context.Background(), "traces"); err != nil || lowOf(t, st, key(4)) != uint64(clock.UnixNano()) {
		t.Fatalf("idle heartbeat: low %d, want %d", lowOf(t, st, key(4)), clock.UnixNano())
	}
	o, _ := st.Get(key(4))
	if o.Meta[commit.MetaKind] != commit.KindBeat || len(o.Body) != 0 {
		t.Fatalf("heartbeat %v", o.Meta)
	}
}

// The ledger: a retryable failure keeps a request in custody, a permanent
// one lets it go; a request replayed from before the restart is held while
// in flight; an earlier incarnation's probe is dropped and drains nothing.
func TestCustodyLedger(t *testing.T) {
	c := custodyFor(component.MustNewIDWithName("s3pq", "ledger"))
	c.expect("logs")
	if c.low(100) != 0 {
		t.Fatal("undrained: 0")
	}
	old := setMeta(context.Background(), probeKey, "an-earlier-incarnation")
	if err := c.push(old, "logs", nil); err != nil || c.low(100) != 0 {
		t.Fatal("an old probe drained the queue")
	}
	_ = c.push(probeCtx(context.Background()), "logs", nil)
	if c.low(100) != 100 {
		t.Fatalf("drained and empty: %d", c.low(100))
	}
	ctx, _ := c.take(context.Background(), time.Unix(0, 40))
	retry := errors.New("unresolved")
	if c.push(ctx, "logs", func(context.Context) error { return retry }) != retry || c.low(100) != 40 {
		t.Fatal("a retryable error let the request go")
	}
	if c.push(ctx, "logs", func(context.Context) error { return consumererror.NewPermanent(retry) }) == nil || c.low(100) != 100 {
		t.Fatal("a permanent error kept it")
	}
	replay := setMeta(setMeta(context.Background(), custodyIDKey, "earlier-7"), ReceivedKey, "30")
	_ = c.push(replay, "logs", func(context.Context) error {
		if c.low(100) != 30 {
			t.Errorf("a replay in flight: %d", c.low(100))
		}
		return nil
	})
	if c.low(100) != 100 {
		t.Fatal("the replay was not let go")
	}
}
