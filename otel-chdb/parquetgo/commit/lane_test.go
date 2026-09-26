package commit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func enc(r Ref) (Object, error) {
	return Object{Body: []byte(fmt.Sprintf("%s/%d", r.Epoch, r.Seq)), ContentType: "application/octet-stream"}, nil
}

func lane(s Store) *Lane {
	n := 0
	return &Lane{Name: "t/0", Prefix: "p", Producer: "prod", Store: s,
		Timeouts: Timeouts{Put: time.Second, Head: time.Second},
		NewEpoch: func() string { n++; return fmt.Sprintf("E%d", n) }}
}

func TestKeysAndEpochs(t *testing.T) {
	k := SlotKey("p/traces/", "E1", 7)
	if k != "p/traces/E1/00000000000000000007.parquet" {
		t.Fatal(k)
	}
	if e, s, ok := ParseSlotKey("p/traces", k); !ok || e != "E1" || s != 7 {
		t.Fatal(e, s, ok)
	}
	e := EpochAt(time.Date(2026, 9, 25, 3, 15, 0, 123e6, time.UTC))
	if len(e) != len("20260925T031500.123Z-")+8 || e[:21] != "20260925T031500.123Z-" {
		t.Fatal(e)
	}
	// The Rust edge's content key for the same bytes (otap-rs batch.rs
	// content_hash_otlp: BLAKE3("traces\0" + b), first 16 bytes hex).
	if h := ContentHash("traces", []byte("abc")); len(h) != 32 {
		t.Fatal(h)
	}
}

// The Rust runner's fault test (otap-rs/src/runner.rs), step for step.
func TestFaultsCommitOnceWithoutGaps(t *testing.T) {
	s := NewMemStore()
	l := lane(s)
	ctx := context.Background()
	push := func(c string) Ref {
		t.Helper()
		r, err := l.Append(ctx, c, enc)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := push("a"); r.Seq != 0 || r.Epoch != "E1" {
		t.Fatal(r)
	}
	s.Inject(ApplyLoseAnswer) // applied, answer lost: resolved as ours by HEAD
	if r := push("b"); r.Seq != 1 {
		t.Fatal(r)
	}
	s.Inject(Drop) // HEAD finds it free: resend
	if r := push("c"); r.Seq != 2 {
		t.Fatal(r)
	}
	s.Inject(Hold) // late: the resend wins, the late copy gets 412 when released
	if r := push("d"); r.Seq != 3 {
		t.Fatal(r)
	}
	if n := s.ReleaseHeld(); n != 0 {
		t.Fatal("late copy landed", n)
	}
	puts := s.Puts
	if r := push("b"); r.Seq != 1 || s.Puts != puts { // a retry of a committed batch: no request
		t.Fatal(r, s.Puts, puts)
	}
	if n := len(s.Keys("p/")); n != 4 {
		t.Fatal(n)
	}
	st := l.Stats
	if st.ResolvedOwn.Load() != 1 || st.Resent.Load() != 2 || st.KnownSkipped.Load() != 1 {
		t.Fatal(st.ResolvedOwn.Load(), st.Resent.Load(), st.KnownSkipped.Load())
	}
}

func TestEpochNamedAtFirstWrite(t *testing.T) {
	l := lane(NewMemStore())
	if e := l.Epoch(); e != "" {
		t.Fatal(e)
	}
	r, _ := l.Append(context.Background(), "a", enc)
	r2, _ := l.Append(context.Background(), "b", enc)
	if r.Epoch != "E1" || r2 != (Ref{"E1", 1}) {
		t.Fatal(r, r2)
	}
}

func TestTombstoneHaltsIntoNewEpoch(t *testing.T) {
	s := NewMemStore()
	l := lane(s)
	ctx := context.Background()
	if _, err := l.Append(ctx, "a", enc); err != nil {
		t.Fatal(err)
	}
	s.Tomb(SlotKey("p", "E1", 1))
	r, err := l.Append(ctx, "b", enc)
	if err != nil || r.Epoch != "E2" || r.Seq != 0 || l.Stats.Halted.Load() != 1 {
		t.Fatal(r, err)
	}
	// The object carries its new slot: re-encoded for E2/0.
	o, _ := s.Get(SlotKey("p", "E2", 0))
	if string(o.Body) != "E2/0" || o.Meta[MetaEpoch] != "E2" || o.Meta[MetaSeq] != "0" || o.Meta[MetaContent] != "b" {
		t.Fatal(string(o.Body), o.Meta)
	}
}

func TestSwitchedSlotLearnsOther(t *testing.T) {
	s := NewMemStore()
	l := lane(s)
	ctx := context.Background()
	s.Inject(Hold)
	s.Inject(HeadFail)
	if _, err := l.Append(ctx, "h1", enc); !errors.As(err, new(*ErrUnresolved)) {
		t.Fatal(err)
	}
	s.ReleaseHeld() // h1 lands late in slot 0
	r, err := l.Append(ctx, "h2", enc)
	if err != nil || r.Seq != 1 || l.Stats.LearnedOther.Load() != 1 {
		t.Fatal(r, err)
	}
	if k, ok := l.Known("h1"); !ok || k.Seq != 0 {
		t.Fatal(k, ok)
	}
}

func TestInconsistentStoreStaysUnresolved(t *testing.T) {
	l := lane(inconsistent{})
	_, err := l.Append(context.Background(), "h", enc)
	var u *ErrUnresolved
	if !errors.As(err, &u) {
		t.Fatal(err)
	}
	if _, _, p := l.State(); p != Unresolved {
		t.Fatal(p)
	}
}

type inconsistent struct{}

func (inconsistent) PutCreate(context.Context, string, []byte, string, map[string]string) PutOutcome {
	return PutExists
}
func (inconsistent) Head(context.Context, string) (map[string]string, bool, error) {
	return nil, false, nil
}

func TestResendIsByteIdentical(t *testing.T) {
	s := NewMemStore()
	l := lane(s)
	calls := 0
	e := func(r Ref) (Object, error) {
		calls++
		return Object{Body: []byte(fmt.Sprintf("%s/%d/%d", r.Epoch, r.Seq, calls))}, nil
	}
	s.Inject(Drop)
	if _, err := l.Append(context.Background(), "a", e); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("re-encoded for the same slot", calls)
	}
}

func TestVerdict(t *testing.T) {
	ok := PartOutcome{Signal: "a"}
	un := PartOutcome{Signal: "b", Err: &ErrUnresolved{"t"}}
	bad := PartOutcome{Signal: "c", Err: &ErrEncode{errors.New("x")}}
	if v, _ := RequestVerdict(nil, NoMutation); v != Ack {
		t.Fatal(v)
	}
	if v, _ := RequestVerdict([]PartOutcome{ok, un}, NoMutation); v != Retry {
		t.Fatal(v)
	}
	if v, _ := RequestVerdict([]PartOutcome{ok, un}, AckOnAny); v != Ack {
		t.Fatal(v)
	}
	if v, _ := RequestVerdict([]PartOutcome{un, bad, ok}, NoMutation); v != Reject {
		t.Fatal(v)
	}
}
