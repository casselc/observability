package edge

import (
	"context"
	"time"
)

// Heartbeats registers every lane the edge can write (a birth heartbeat per
// namespace, retried every half second for up to birthTimeout: exporters
// start before receivers, so nothing is taken into custody before) and then
// keeps each idle lane alive: a heartbeat for every namespace that has
// committed nothing for interval (../../FORMAT.md §2), checked every
// interval/2. It returns how many namespaces were born, and a channel
// closed once the keep-alive loop has returned (ctx done). warn, if set,
// hears every failed keep-alive heartbeat while ctx is live.
//
// The collector exporter (s3pqexporter) runs it per edge; the edge DST
// (../dst) runs the same loop under faults, so the heartbeat path is
// simulated with the data path it shares lanes with.
func (e *Edge) Heartbeats(ctx context.Context, interval, birthTimeout time.Duration, warn func(ns string, err error)) (int, <-chan struct{}) {
	lanes := e.Registered()
	born := map[string]bool{}
	until := time.Now().Add(birthTimeout)
	for len(born) < len(lanes) && time.Now().Before(until) && ctx.Err() == nil {
		for _, ns := range lanes {
			if !born[ns] && e.Beat(ctx, ns) == nil {
				born[ns] = true
			}
		}
		if len(born) < len(lanes) {
			time.Sleep(500 * time.Millisecond)
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(interval / 2)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			for _, ns := range lanes {
				if e.IdleFor(ns) >= interval {
					if err := e.Beat(ctx, ns); err != nil && warn != nil && ctx.Err() == nil {
						warn(ns, err)
					}
				}
			}
		}
	}()
	return len(born), done
}
