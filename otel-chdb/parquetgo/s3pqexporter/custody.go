package s3pqexporter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Custody: what waits for the edge in the sending queue, for oscope-low
// (../../FORMAT.md §2, ../../model/completeness.qnt).
//
// A request is in the publisher's custody from the moment the exporter
// takes it (the stamp, before the queue) until the queue drops it: a commit,
// or a permanent error. The ledger holds each such request's received_at by
// a custody id carried in its client metadata (persisted with it by
// file_storage, like the received_at stamp). What it cannot know is what a
// persistent queue held before this process started: those requests were
// stamped by an earlier incarnation. So at start each pipeline's queue gets
// a probe, an empty request tagged with this incarnation; a queue is FIFO,
// so once the probe comes out every request persisted before it has been
// handed to the exporter (and is in the ledger while it is in flight or
// retried). Until then the floor is 0: every object's oscope-low is 0,
// which is sound and only holds the consumer's watermark where it is.
const (
	custodyIDKey = "x-s3pq-custody-id"
	probeKey     = "x-s3pq-probe"
)

// incarnation names this process's probes and custody ids.
var incarnation = func() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}()

var custodySeq atomic.Uint64

type custody struct {
	mu        sync.Mutex
	held      map[string]uint64 // custody id -> received_at (ns)
	undrained map[string]bool   // pipeline kind -> its queue may hold an earlier incarnation's requests
}

var (
	custodiesMu sync.Mutex
	custodies   = map[component.ID]*custody{}
)

func custodyFor(id component.ID) *custody {
	custodiesMu.Lock()
	defer custodiesMu.Unlock()
	c := custodies[id]
	if c == nil {
		c = &custody{held: map[string]uint64{}, undrained: map[string]bool{}}
		custodies[id] = c
	}
	return c
}

func (c *custody) expect(kind string) {
	c.mu.Lock()
	c.undrained[kind] = true
	c.mu.Unlock()
}

func (c *custody) drained(kind string) {
	c.mu.Lock()
	delete(c.undrained, kind)
	c.mu.Unlock()
}

func (c *custody) add(id string, r uint64) {
	c.mu.Lock()
	c.held[id] = r
	c.mu.Unlock()
}

func (c *custody) done(id string) {
	c.mu.Lock()
	delete(c.held, id)
	c.mu.Unlock()
}

// low is the custody floor at now (ns).
func (c *custody) low(now uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.undrained) > 0 {
		return 0
	}
	l := now
	for _, r := range c.held {
		l = min(l, r)
	}
	return l
}

func (c *custody) lowNow() uint64 { return c.low(uint64(time.Now().UnixNano())) }

// empty reports whether nothing is in custody: no request held (taken and
// not committed or refused), and every queue's probe out (nothing an
// earlier incarnation persisted is left in a queue). The orderly close
// (../../FORMAT.md §3.1) is committed only then.
func (c *custody) empty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.held) == 0 && len(c.undrained) == 0
}

func setMeta(ctx context.Context, k, v string) context.Context {
	info := client.FromContext(ctx)
	md := map[string][]string{}
	for key := range info.Metadata.Keys() {
		md[key] = info.Metadata.Get(key)
	}
	md[k] = []string{v}
	info.Metadata = client.NewMetadata(md)
	return client.NewContext(ctx, info)
}

func metaOf(ctx context.Context, k string) string {
	if v := client.FromContext(ctx).Metadata.Get(k); len(v) == 1 {
		return v[0]
	}
	return ""
}

// take stamps a request as it enters custody and records it: the
// received_at stamp and a custody id. Returns the context and the id.
func (c *custody) take(ctx context.Context, now time.Time) (context.Context, string) {
	ctx = stampReceived(ctx, now)
	id := incarnation + "-" + strconv.FormatUint(custodySeq.Add(1), 10)
	c.add(id, uint64(now.UnixNano()))
	return setMeta(ctx, custodyIDKey, id), id
}

// push runs the exporter's push under the ledger: a probe of this
// incarnation marks its queue drained (an earlier incarnation's is
// dropped); a request is held while in flight (a replay from before the
// restart included) and let go on a commit or a permanent error.
func (c *custody) push(ctx context.Context, kind string, f func(context.Context) error) error {
	if p := metaOf(ctx, probeKey); p != "" {
		if p == incarnation {
			c.drained(kind)
		}
		return nil
	}
	id := metaOf(ctx, custodyIDKey)
	if id != "" {
		r, _ := strconv.ParseUint(metaOf(ctx, ReceivedKey), 10, 64)
		c.add(id, r)
	}
	err := f(withReceived(ctx))
	if id != "" && (err == nil || consumererror.IsPermanent(err)) {
		c.done(id)
	}
	return err
}

func probeCtx(ctx context.Context) context.Context { return setMeta(ctx, probeKey, incarnation) }

// The probes: one empty resource each (not zero bytes: a persistent queue
// stores them like any request).
func probeTraces() ptrace.Traces {
	t := ptrace.NewTraces()
	t.ResourceSpans().AppendEmpty()
	return t
}

func probeLogs() plog.Logs {
	l := plog.NewLogs()
	l.ResourceLogs().AppendEmpty()
	return l
}

func probeMetrics() pmetric.Metrics {
	m := pmetric.NewMetrics()
	m.ResourceMetrics().AppendEmpty()
	return m
}
