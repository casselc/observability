package commit

import (
	"context"
	"errors"
	"testing"
)

// A store that fails every PUT, whose HEADs find the slot free, or a caller
// past its deadline (every PUT then fails at once): Append gives up after
// MaxResends resends instead of spinning on the slot while holding the lane
// (found in the D31 integration run: an edge at 40% CPU for 11 minutes, its
// heartbeats blocked, while SeaweedFS refused writes). The lane stays at
// the slot: the next call commits there. The Rust runner's
// `a_store_that_fails_every_put_is_not_resent_forever`.
func TestAppendResendLimit(t *testing.T) {
	s := NewMemStore()
	l := lane(s)
	for range 3 * MaxResends {
		s.Inject(Drop)
	}
	_, err := l.Append(context.Background(), "a", enc)
	var u *ErrUnresolved
	if !errors.As(err, &u) || s.Puts != MaxResends+1 || l.Stats.Resent.Load() != MaxResends || l.Stats.Unresolved.Load() != 1 {
		t.Fatalf("err %v, %d PUTs, %d resent", err, s.Puts, l.Stats.Resent.Load())
	}
	s.ClearFaults()
	if r, err := l.Append(context.Background(), "a", enc); err != nil || r.Seq != 0 || r.Epoch != "E1" {
		t.Fatal(r, err)
	}
	// a caller whose deadline has passed: bounded too
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	puts := s.Puts
	if _, err := l.Append(ctx, "b", enc); !errors.As(err, &u) || s.Puts-puts != MaxResends+1 {
		t.Fatalf("cancelled: err %v, %d PUTs", err, s.Puts-puts)
	}
	if r, err := l.Append(context.Background(), "b", enc); err != nil || r.Seq != 1 {
		t.Fatal(r, err)
	}
}
