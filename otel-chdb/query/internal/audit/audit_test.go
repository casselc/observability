package audit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type shipper struct {
	mu    sync.Mutex
	got   []Record
	fails int
}

func (s *shipper) Ship(_ context.Context, b []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fails > 0 {
		s.fails--
		return errors.New("sink down")
	}
	s.got = append(s.got, b...)
	return nil
}

func TestFileAppendsAndShips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	f, err := OpenFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	sh := &shipper{fails: 1}
	f.StartShipping(context.Background(), sh, 2, 20*time.Millisecond, nil)
	for i := 0; i < 3; i++ {
		if err := f.Write(Record{RequestID: "r", Event: "decision", Decision: "deny", Reason: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		sh.mu.Lock()
		n := len(sh.got)
		sh.mu.Unlock()
		if n == 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Write(Record{}); err == nil {
		t.Fatal("write after close")
	}
	recs, err := ReadFrom(path)
	if err != nil || len(recs) != 3 || recs[0].Time.IsZero() {
		t.Fatalf("%v %+v", err, recs)
	}
	if len(sh.got) != 3 {
		t.Fatalf("shipped %d after a failed batch", len(sh.got))
	}
	st, _ := os.Stat(path)
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", st.Mode())
	}
	// reopening appends
	f2, _ := OpenFile(path, false)
	_ = f2.Write(Record{Event: "outcome"})
	_ = f2.Close()
	if recs, _ := ReadFrom(path); len(recs) != 4 {
		t.Fatalf("%d records after reopening", len(recs))
	}
}
