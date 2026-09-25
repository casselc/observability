package seriesenc

import (
	"bytes"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
	"github.com/parquet-go/parquet-go"
)

func small() fleet.Config {
	c := fleet.Default()
	c.Services, c.PodsPerService = 4, 3
	return c
}

// Same data, same bytes; a second round of the same window announces
// nothing; a fresh encoder (new epoch) announces everything again; the next
// window re-announces.
func TestCacheAndDeterminism(t *testing.T) {
	f := fleet.New(small())
	md := f.EmitRound()
	env := &Envelope{ProducerID: "p", Epoch: "e", BatchID: 1, ReceivedAtNs: 1}
	var b1, b2 Buffers
	e1, e2 := New(), New()
	rows := e1.Encode(md, env)
	if err := e1.Flush(b1.Dst()); err != nil {
		t.Fatal(err)
	}
	e2.Encode(md, env)
	if err := e2.Flush(b2.Dst()); err != nil {
		t.Fatal(err)
	}
	for i := range b1 {
		if !bytes.Equal(b1[i].Bytes(), b2[i].Bytes()) {
			t.Fatalf("object %d differs", i)
		}
	}
	n := 0
	for _, r := range rows {
		n += r
	}
	if len(e1.New) != n {
		t.Fatalf("announced %d, want %d (every series once)", len(e1.New), n)
	}
	// Series ids unique and read back.
	sr, err := parquet.Read[SeriesRow](bytes.NewReader(b1[NumTypes].Bytes()), int64(b1[NumTypes].Len()))
	if err != nil {
		t.Fatal(err)
	}
	ids := map[uint64]bool{}
	for _, r := range sr {
		if ids[r.SeriesID] {
			t.Fatal("duplicate series id")
		}
		ids[r.SeriesID] = true
	}
	e1.Announced()
	md2 := f.EmitRound()
	e1.Encode(md2, env)
	if len(e1.New) != 0 {
		t.Fatalf("round 2 announced %d", len(e1.New))
	}
	// Ids are stable across rounds.
	e1.Flush(b1.Dst())
	pr, _ := parquet.Read[NumberRow](bytes.NewReader(b1[Sum].Bytes()), int64(b1[Sum].Len()))
	for _, r := range pr {
		if !ids[r.SeriesID] {
			t.Fatal("round 2 id not seen in round 1")
		}
	}
	// Next window: everything again.
	for f.Round() < int(time.Hour/(30*time.Second))+1 {
		f.EmitRound()
	}
	e1.Encode(f.EmitRound(), env)
	if len(e1.New) != n {
		t.Fatalf("new window announced %d, want %d", len(e1.New), n)
	}
}
