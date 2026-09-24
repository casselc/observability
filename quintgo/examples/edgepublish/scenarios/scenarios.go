// Package scenarios drives the stand-in publisher through the demo
// scenarios: conforming ones, and mutants the validator must reject. The
// demo binary and the qtest-based tests both run them.
package scenarios

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/chucklehead-dev/oscope/spike/quintgo/examples/edgepublish/publisher"
)

// clock is a manually advanced clock, so generation rotation is deterministic.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Names in presentation order.
var Names = []string{"happy", "rotation-race", "ambiguous-seal", "manifest-first", "no-lock"}

// Describe says what each scenario does.
var Describe = map[string]string{
	"happy":          "concurrent pushes, a failed Parquet write and its retry, a rotation and seal, a crash and restart, close",
	"rotation-race":  "a slow push while the clock moves on: rotation must wait for it",
	"no-lock":        "MUTANT: the same race with the rotation lock dropped, so a batch commits into a sealed generation",
	"manifest-first": "MUTANT: the manifest is written before the table insert",
	"ambiguous-seal": "a manifest write lands but reports failure; the retry commits again; the seal undercounts",
}

// deliver is the collector's persistent queue: it retries a request until an
// export succeeds.
func deliver(w io.Writer, p *publisher.Publisher, id string) {
	ctx := publisher.WithPayload(context.Background(), id)
	for attempt := 1; attempt <= 5; attempt++ {
		err := p.Push(ctx, []byte("rows of "+id))
		if err == nil {
			fmt.Fprintf(w, "  %s: exported (attempt %d)\n", id, attempt)
			return
		}
		fmt.Fprintf(w, "  %s: attempt %d failed: %v\n", id, attempt, err)
	}
}

// Run drives one scenario. Progress goes to w (io.Discard for quiet).
func Run(name string, w io.Writer) {
	clk := &clock{t: time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)}
	store := publisher.NewMemStore()
	cfg := publisher.Config{Producer: "edge-1", Region: "eu", Signal: "traces", Epoch: "epoch-A",
		Generation: time.Minute, Now: clk.Now}

	switch name {
	case "happy":
		var failed sync.Once
		store.Hook = func(_ context.Context, op publisher.Op) publisher.Fault {
			// Some latency per write, so the concurrent pushes interleave.
			time.Sleep(time.Duration(1+len(op.Key)%3) * time.Millisecond)
			// The first Parquet PUT of batch 2 fails.
			if strings.HasSuffix(op.Key, "00000000000000000002.parquet") && strings.Contains(op.Key, "epoch-A") {
				fault := publisher.Apply
				failed.Do(func() { fault = publisher.Fail })
				return fault
			}
			return publisher.Apply
		}
		p := publisher.New(cfg, store)
		var wg sync.WaitGroup
		for _, id := range []string{"req-1", "req-2", "req-3"} {
			wg.Add(1)
			go func() { defer wg.Done(); deliver(w, p, id) }()
		}
		wg.Wait()
		clk.Advance(time.Minute) // the next push rotates and seals the first generation
		deliver(w, p, "req-4")
		time.Sleep(50 * time.Millisecond) // let the background seal of the first generation finish
		fmt.Fprintln(w, "  crash: epoch-A is gone without closing; epoch-B starts")
		cfg.Epoch = "epoch-B"
		p2 := publisher.New(cfg, store)
		deliver(w, p2, "req-5")
		if err := p2.Close(context.Background()); err != nil {
			log.Fatal(err)
		}

	case "rotation-race", "no-lock":
		if name == "no-lock" {
			cfg.Mutation = publisher.NoRotationLock
		}
		sealed := make(chan struct{})
		var once sync.Once
		store.Hook = func(_ context.Context, op publisher.Op) publisher.Fault {
			if strings.HasSuffix(op.Key, "_sealed.json") {
				once.Do(func() { close(sealed) })
			}
			// Hold req-1's table insert until the first generation is
			// sealed, or 300ms (with the lock, rotation waits for req-1, so
			// the seal cannot come first).
			if op.Kind == "table" && op.Batch == 1 {
				select {
				case <-sealed:
				case <-time.After(300 * time.Millisecond):
				}
			}
			return publisher.Apply
		}
		p := publisher.New(cfg, store)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); deliver(w, p, "req-1") }()
		time.Sleep(50 * time.Millisecond) // req-1 is now inside its table insert
		clk.Advance(time.Minute)
		deliver(w, p, "req-2") // rotates
		wg.Wait()
		if err := p.Close(context.Background()); err != nil {
			log.Fatal(err)
		}

	case "manifest-first":
		cfg.Mutation = publisher.ManifestBeforeTable
		p := publisher.New(cfg, store)
		deliver(w, p, "req-1")
		if err := p.Close(context.Background()); err != nil {
			log.Fatal(err)
		}

	case "ambiguous-seal":
		var once sync.Once
		store.Hook = func(_ context.Context, op publisher.Op) publisher.Fault {
			if strings.Contains(op.Key, "/manifests/") && strings.HasSuffix(op.Key, "00000000000000000001.json") {
				fault := publisher.Apply
				once.Do(func() { fault = publisher.Ambiguous })
				return fault
			}
			return publisher.Apply
		}
		p := publisher.New(cfg, store)
		deliver(w, p, "req-1") // committed as batch 1 (unbeknownst to the writer), then as batch 2
		clk.Advance(time.Minute)
		deliver(w, p, "req-2")
		if err := p.Close(context.Background()); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Fprintln(w, "  store objects:")
	for _, k := range store.Keys() {
		if strings.Contains(k, "manifests") {
			b, _ := store.Object(k)
			fmt.Fprintf(w, "    %s %s\n", k[strings.Index(k, "manifests"):], strings.TrimSpace(string(b)))
		}
	}
}
