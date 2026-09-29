package ingress

// Drain's deadline and the heartbeat ticker in fake time
// (testing/synctest): handlers that finish after 5, 20 and 45 s; a drain
// with a 30 s deadline gives up at exactly 30 s and writes no close, one
// with 60 s closes the lanes at exactly 45 s, after the heartbeat loop has
// stopped (research/go-verification.md §5).

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
)

func TestDrainDeadlineInFakeTime(t *testing.T) {
	for _, c := range []struct {
		deadline  time.Duration
		wantAfter time.Duration
		closed    bool
	}{
		{30 * time.Second, 30 * time.Second, false},
		{60 * time.Second, 45 * time.Second, true},
	} {
		t.Run(fmt.Sprint(c.deadline), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := commit.NewMemStore()
				var n atomic.Int64
				e, err := edge.New(edge.Config{Store: st, Prefix: "root", Cluster: "devtools", ProducerID: "ingress-0",
					NewEpoch: func() string { return fmt.Sprintf("20000101T000000.000Z-%08x", n.Add(1)) }})
				if err != nil {
					t.Fatal(err)
				}
				s := &Server{Logf: func(string, ...any) {}, counts: map[string]int64{}}
				for _, d := range []time.Duration{5 * time.Second, 20 * time.Second, 45 * time.Second} {
					if !s.gate.enter() {
						t.Fatal("gate closed")
					}
					go func() { time.Sleep(d); s.gate.leave() }()
				}
				beatCtx, stopBeats := context.WithCancel(context.Background())
				beatsDone := make(chan struct{})
				var beatsStoppedAt time.Time
				go func() { Heartbeats(beatCtx, e, time.Minute, s.Logf); beatsStoppedAt = time.Now(); close(beatsDone) }()
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
				defer cancel()
				closed, err := s.Drain(ctx, e, func() { stopBeats(); <-beatsDone })
				took := time.Since(start)
				if took != c.wantAfter {
					t.Fatalf("drain returned after %v, want %v", took, c.wantAfter)
				}
				if s.gate.enter() {
					t.Fatal("a request was admitted during the drain")
				}
				closes := 0
				for _, k := range st.Keys("root/devtools/") {
					if o, _ := st.Get(k); o.Meta[commit.MetaKind] == commit.KindClose {
						closes++
					}
				}
				if c.closed != (err == nil && closed > 0 && closes == closed) {
					t.Fatalf("closed %d lanes (%d close slots), err %v", closed, closes, err)
				}
				if c.closed && beatsStoppedAt.After(start.Add(took)) {
					t.Fatal("the heartbeat loop outlived the close")
				}
				if !c.closed {
					stopBeats()
					<-beatsDone
				}
				time.Sleep(time.Minute) // the last handler leaves; Drain's waiter exits
				synctest.Wait()
			})
		})
	}
}
