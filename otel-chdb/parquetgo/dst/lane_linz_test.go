package dst

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/casselc/observability/otel-chdb/casreg"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
)

// linzStore records a commit.Store's calls as a casreg history: PutCreate
// is a create-only write of the slot's (kind, epoch, content), Head a read
// of it.
type linzStore struct {
	commit.Store
	rec    *casreg.Recorder
	client int
}

func slotValue(m map[string]string) string {
	return m[commit.MetaKind] + "|" + m[commit.MetaEpoch] + "|" + m[commit.MetaContent]
}

func (s linzStore) PutCreate(ctx context.Context, key string, body []byte, ct string, meta map[string]string) commit.PutOutcome {
	c := s.rec.Begin(s.client, casreg.Input{Key: key, Op: casreg.Create, Value: slotValue(meta)})
	o := s.Store.PutCreate(ctx, key, body, ct, meta)
	c.End(casreg.Output{Result: map[commit.PutOutcome]casreg.Result{
		commit.PutOK: casreg.OK, commit.PutExists: casreg.Failed, commit.PutUnknown: casreg.Unknown}[o]})
	return o
}

func (s linzStore) Head(ctx context.Context, key string) (map[string]string, bool, error) {
	c := s.rec.Begin(s.client, casreg.Input{Key: key, Op: casreg.Read})
	m, found, err := s.Store.Head(ctx, key)
	switch {
	case err != nil:
		c.End(casreg.Output{Result: casreg.Unknown})
	case !found:
		c.End(casreg.Output{Result: casreg.OK})
	default:
		c.End(casreg.Output{Result: casreg.OK, Found: true, Value: slotValue(m)})
	}
	return m, found, err
}

func encode(r commit.Ref) (commit.Object, error) {
	return commit.Object{Body: []byte(fmt.Sprintf("%s/%d", r.Epoch, r.Seq)), ContentType: "application/octet-stream"}, nil
}

// Random fault schedules over one lane (answers lost after applying,
// requests lost, requests held and released late, HEADs failing, the
// consumer's tombstones), 200 seeds: the store's history as the lane saw
// it is linearizable as one create-only register per slot, and every batch
// the lane ACKed is in the slot it was ACKed at.
func TestLaneHistoryIsLinearizable(t *testing.T) {
	for seed := range uint64(200) {
		rng := rand.New(rand.NewPCG(seed, 9))
		ms := commit.NewMemStore()
		rec := &casreg.Recorder{}
		n := 0
		l := &commit.Lane{Name: "t/0", Prefix: "p", Producer: "prod", Store: linzStore{ms, rec, 0},
			Timeouts: commit.Timeouts{Put: time.Second, Head: time.Second},
			NewEpoch: func() string { n++; return fmt.Sprintf("E%d", n) }}
		ctx := context.Background()
		acked := map[string]commit.Ref{}
		for i := range 30 {
			switch rng.IntN(8) {
			case 0:
				ms.Inject(commit.ApplyLoseAnswer)
			case 1:
				ms.Inject(commit.Drop)
			case 2:
				ms.Inject(commit.Hold)
			case 3:
				ms.Inject(commit.HeadFail)
			case 4:
				ms.ReleaseHeld() // late copies land (or lose) outside any call
			case 5:
				if ep, next, _ := l.State(); ep != "" {
					key := commit.SlotKey("p", ep, next)
					c := rec.Begin(1, casreg.Input{Key: key, Op: casreg.Create, Value: commit.KindTomb + "||"})
					ok := ms.Tomb(key)
					c.End(casreg.Output{Result: map[bool]casreg.Result{true: casreg.OK, false: casreg.Failed}[ok]})
				}
			}
			content := fmt.Sprintf("batch-%d", i)
			for range 4 {
				if r, err := l.Append(ctx, content, encode); err == nil {
					acked[content] = r
					break
				}
			}
		}
		ms.ClearFaults()
		ms.ReleaseHeld()
		if res, _ := rec.Check(0); res != porcupine.Ok {
			t.Fatalf("seed %d: history of %d operations (%d pending) is %s", seed, rec.Len(), rec.Pending(), res)
		}
		for c, r := range acked {
			o, ok := ms.Get(commit.SlotKey("p", r.Epoch, r.Seq))
			if !ok || o.Meta[commit.MetaContent] != c {
				t.Fatalf("seed %d: %s ACKed at %v, slot holds %v", seed, c, r, o.Meta)
			}
		}
	}
}
