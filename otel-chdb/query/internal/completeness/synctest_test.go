package completeness

// The Reader on the real clock inside a testing/synctest bubble: no
// SetClock. A consumer goroutine publishes the watermark document every
// 30 s; readers poll it. The TTL, the max-age staleness flip when the
// consumer stops, and the error flip when the store fails are checked at
// their exact fake-time boundaries (research/go-verification.md §5).

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// published is a store the fake consumer writes to.
type published struct {
	mu    sync.Mutex
	body  []byte
	err   error
	reads int
}

func (p *published) get(context.Context, string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads++
	return p.body, p.err
}

func (p *published) publish() {
	now := time.Now()
	b, _ := json.Marshal(Doc{Format: 2, Version: 3, CompleteThroughNs: uint64(now.Add(-40 * time.Second).UnixNano()),
		WallMs: uint64(now.UnixMilli())})
	p.mu.Lock()
	p.body = b
	p.mu.Unlock()
}

func TestReaderInFakeTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ttl, maxAge, every = 15 * time.Second, 5 * time.Minute, 30 * time.Second
		st := &published{}
		ctx, stop := context.WithCancel(context.Background())
		consumerDone := make(chan struct{})
		st.publish()
		go func() { // the consumer
			defer close(consumerDone)
			tk := time.NewTicker(every)
			defer tk.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
					st.publish()
				}
			}
		}()
		r := NewReader(st.get, "otel/_consumer/watermark.json", ttl, maxAge)

		// TTL: one read per 15 s window, however often Get is called
		for range 60 { // 60 s, a Get every second
			if s := r.Get(context.Background()); s.Status != StatusOK {
				t.Fatalf("status %s while the consumer runs", s.Status)
			}
			time.Sleep(time.Second)
		}
		if st.reads != 4 {
			t.Fatalf("%d store reads in 60 s with a 15 s TTL", st.reads)
		}

		// the consumer stops: stale exactly when its last document is older
		// than maxAge, not before (the document keeps its value)
		stop()
		<-consumerDone
		var last struct{ wall time.Time }
		{
			var d Doc
			_ = json.Unmarshal(st.body, &d)
			last.wall = time.UnixMilli(int64(d.WallMs))
		}
		flip := time.Time{}
		for time.Since(last.wall) < maxAge+time.Minute {
			s := r.Get(context.Background())
			if s.Status == StatusStale && flip.IsZero() {
				flip = time.Now()
				if s.Doc == nil {
					t.Fatal("stale without the last value")
				}
			}
			time.Sleep(time.Second)
		}
		if d := flip.Sub(last.wall); d <= maxAge || d > maxAge+time.Second {
			t.Fatalf("stale %v after the last publication; want just past maxAge (%v)", d, maxAge)
		}

		// the store fails: the last good copy serves (stale here, as the
		// consumer is gone) until maxAge after its last successful read,
		// then the status is error
		st.publish() // the consumer is back once
		time.Sleep(ttl)
		if s := r.Get(context.Background()); s.Status != StatusOK {
			t.Fatalf("status %s after a fresh publication", s.Status)
		}
		fetched := time.Now()
		st.mu.Lock()
		st.body, st.err = nil, errors.New("503 Slow Down")
		st.mu.Unlock()
		flip = time.Time{}
		for time.Since(fetched) < maxAge+time.Minute {
			if s := r.Get(context.Background()); s.Status == StatusError && flip.IsZero() {
				flip = time.Now()
			}
			time.Sleep(time.Second)
		}
		if d := flip.Sub(fetched); d <= maxAge || d > maxAge+time.Second {
			t.Fatalf("error %v after the last good read; want just past maxAge (%v)", d, maxAge)
		}
	})
}
