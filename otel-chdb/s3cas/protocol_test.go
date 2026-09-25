package s3cas

// The S3-native log (../model/s3Native.qnt) run against a real endpoint:
// fencing a zombie writer, idempotent retries across incarnations, and a
// zombie racing its successor.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func readLog(t *testing.T, c *Client, prefix string) []Entry {
	t.Helper()
	var out []Entry
	for i := 0; ; i++ {
		b, _, err := c.Get(context.Background(), SlotKey(prefix, i))
		if err != nil {
			if o, _, _ := Classify(err); o == NotFound {
				return out
			}
			t.Fatalf("slot %d: %v", i, err)
		}
		var e Entry
		if err := json.Unmarshal(b, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
}

// checkLog asserts the model's log invariants on what S3 holds: epochs never
// decrease along the log (noWriteFromFencedWriter), each content hash is
// committed once (logNoDuplicatePayload), and nothing is committed into a
// generation after its seal or after a later epoch's fence (sealMatchesManifests).
func checkLog(t *testing.T, log []Entry) {
	t.Helper()
	seen := map[string]int{}
	for i, e := range log {
		if i > 0 && e.Epoch < log[i-1].Epoch {
			t.Errorf("slot %d: epoch %d after epoch %d: a fenced writer wrote", i, e.Epoch, log[i-1].Epoch)
		}
		if e.Kind == "commit" {
			if j, dup := seen[e.Content]; dup {
				t.Errorf("content %s committed at slots %d and %d", e.Content, j, i)
			}
			seen[e.Content] = i
			for _, x := range log[:i] {
				if (x.Kind == "seal" && x.Epoch == e.Epoch && x.Gen == e.Gen) || (x.Kind == "fence" && x.Epoch > e.Epoch) {
					t.Errorf("slot %d: commit into generation (%d,%d) after it was closed", i, e.Epoch, e.Gen)
				}
			}
		}
	}
}

func TestFencingAndReplay(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	w1 := &Writer{C: c, Prefix: p, Epoch: 1}
	for _, h := range []string{"h1", "h2"} {
		if _, _, err := w1.Commit(ctx, h, 1); err != nil {
			t.Fatal(err)
		}
	}
	// Epoch 2 starts while epoch 1 still runs (a zombie).
	w2, fence, err := Start(ctx, c, p, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("epoch 2 fenced the log at slot %d and replayed %d committed hashes", fence, len(w2.Known))
	// The queue retries h2 (committed by epoch 1, never acknowledged): a no-op.
	if _, already, err := w2.Commit(ctx, "h2", 1); err != nil || !already {
		t.Fatalf("retry of h2 in epoch 2: already=%v err=%v", already, err)
	}
	// The zombie tries to commit h3: its slot is taken, it reads the fence, halts.
	if _, _, err := w1.Commit(ctx, "h3", 1); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie commit: %v, want ErrFenced", err)
	}
	if _, _, err := w2.Commit(ctx, "h3", 1); err != nil {
		t.Fatal(err)
	}
	log := readLog(t, c, p)
	t.Logf("log: %+v", log)
	checkLog(t, log)
}

// A zombie and its successor append concurrently, for many rounds; the
// successor starts at a random point of the zombie's run.
func TestZombieRace(t *testing.T) {
	c, _ := client(t)
	ctx := context.Background()
	halted := 0
	for r := 0; r < 20; r++ {
		p := fmt.Sprintf("s3cas-probe/zombie-race/%d/%d", time.Now().UnixNano(), r)
		w1 := &Writer{C: c, Prefix: p, Epoch: 1}
		var wg sync.WaitGroup
		var zErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				if _, _, err := w1.Commit(ctx, fmt.Sprintf("h%d", i), 1); err != nil {
					zErr = err
					return
				}
			}
		}()
		w2, _, err := Start(ctx, c, p, 2)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 12; i++ { // the queue retries everything the zombie may not have acked
			if _, _, err := w2.Commit(ctx, fmt.Sprintf("h%d", i), 1); err != nil {
				t.Fatal(err)
			}
		}
		wg.Wait()
		if errors.Is(zErr, ErrFenced) {
			halted++
		} else if zErr != nil {
			t.Fatal(zErr)
		}
		log := readLog(t, c, p)
		checkLog(t, log)
		n := 0
		for _, e := range log {
			if e.Kind == "commit" {
				n++
			}
		}
		if n != 12 {
			t.Errorf("round %d: %d commits, want 12", r, n)
		}
	}
	t.Logf("20 rounds, zombie racing its successor: the zombie was fenced in %d; every log had 12 distinct commits, epoch-monotone", halted)
}
