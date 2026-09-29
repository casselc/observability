package ingress

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Lanes is what the lifecycle needs of the edge (*edge.Edge; a double in
// tests): the registered lanes, their idleness, a heartbeat, the close.
type Lanes interface {
	Registered() []string
	IdleFor(ns string) time.Duration
	Beat(ctx context.Context, ns string) error
	Close(ctx context.Context) (int, error)
}

// The ingress holds no buffer: a request is in its custody only while a
// handler runs, and the answer (200 committed, or 503 "not committed;
// retry") hands it back. After a 503 this replica never publishes that
// request again: the lane resolves its slot in order before anything else
// (commit.Lane, Unresolved), so an object that did land is below any later
// slot, including a close. Custody is therefore empty exactly when no
// handler is running and none can start, which is what Drain establishes
// before the D35 close (edge.Close's precondition).

// ErrDraining refuses requests once Drain has begun (HTTP 503: the sender
// retries, through the load balancer, to another replica).
var ErrDraining = errors.New("draining")

type gate struct {
	mu       sync.Mutex
	draining bool
	inflight sync.WaitGroup
}

// enter admits one handler unless draining.
func (g *gate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return false
	}
	g.inflight.Add(1)
	return true
}

func (g *gate) leave() { g.inflight.Done() }

// Heartbeats commits a birth heartbeat on every lane, then a heartbeat on
// each lane idle for interval, until ctx ends (FORMAT.md §2; the same loop
// as s3pqexporter's). It returns once the loop has stopped.
func Heartbeats(ctx context.Context, l Lanes, interval time.Duration, logf func(string, ...any)) {
	for _, ns := range l.Registered() {
		if err := l.Beat(ctx, ns); err != nil && ctx.Err() == nil {
			logf("ingress birth heartbeat %s: %v", ns, err)
		}
	}
	t := time.NewTicker(interval / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, ns := range l.Registered() {
			if l.IdleFor(ns) >= interval {
				if err := l.Beat(ctx, ns); err != nil && ctx.Err() == nil {
					logf("ingress heartbeat %s: %v", ns, err)
				}
			}
		}
	}
}

// Drain stops admitting requests, waits for the running ones to answer,
// calls stopBeats (which must return only once the heartbeat loop has
// returned: no Append may run beside the close), and commits the orderly
// close (D35) so the consumer can retire this replica's lanes instead of
// holding complete_through for them. Past ctx's deadline it writes no
// close: the lanes stay stale and are paged, which is safe (FORMAT.md
// §3.1). It reports how many lanes it closed.
func (s *Server) Drain(ctx context.Context, l Lanes, stopBeats func()) (int, error) {
	s.gate.mu.Lock()
	s.gate.draining = true
	s.gate.mu.Unlock()
	done := make(chan struct{})
	go func() { s.gate.inflight.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		s.Logf("ingress close: none (requests still running at the deadline)")
		return 0, ctx.Err()
	}
	if stopBeats != nil {
		stopBeats()
	}
	n, err := l.Close(ctx)
	if err != nil {
		s.Logf("ingress close: %d lanes closed, others stay open (safe): %v", n, err)
	} else {
		s.Logf("ingress close: custody empty, %d lanes closed", n)
	}
	return n, err
}
