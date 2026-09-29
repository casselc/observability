package runner

// The state store's history, as the replicas saw it, checked for
// linearizability as one If-Match / If-None-Match register per key
// (../../../casreg, porcupine). The simulation's own check ("every window
// once, in order") reads the committed history from inside the store; this
// one asks whether the answers the replicas got, lost and delayed ones
// included, fit any single order of the writes.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
	"github.com/casselc/observability/otel-chdb/casreg"
)

// linz records a store's operations; one per replica (its client id).
type linz struct {
	store.Store
	rec    *casreg.Recorder
	etags  *casreg.Etags
	client int
}

func value(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:12])
}

func (l linz) Get(ctx context.Context, key string) ([]byte, string, error) {
	c := l.rec.Begin(l.client, casreg.Input{Key: key, Op: casreg.Read})
	b, e, err := l.Store.Get(ctx, key)
	switch {
	case errors.Is(err, store.ErrNotFound):
		c.End(casreg.Output{Result: casreg.OK})
	case err != nil:
		c.End(casreg.Output{Result: casreg.Unknown})
	default:
		l.etags.Learn(e, value(b))
		c.End(casreg.Output{Result: casreg.OK, Found: true, Value: value(b)})
	}
	return b, e, err
}

func (l linz) write(key string, in casreg.Input, body []byte, put func() (string, error)) (string, error) {
	in.Key, in.Value = key, value(body)
	c := l.rec.Begin(l.client, in)
	e, err := put()
	switch {
	case err == nil:
		l.etags.Learn(e, in.Value)
		c.End(casreg.Output{Result: casreg.OK})
	case errors.Is(err, store.ErrAmbiguous):
		c.End(casreg.Output{Result: casreg.Unknown})
	default: // 412, 404, a definite refusal: not applied
		c.End(casreg.Output{Result: casreg.Failed})
	}
	return e, err
}

func (l linz) PutIfMatch(ctx context.Context, key, etag string, body []byte) (string, error) {
	exp, ok := l.etags.Value(etag)
	if !ok {
		exp = "unseen-etag:" + etag
	}
	return l.write(key, casreg.Input{Op: casreg.Swap, Expect: exp}, body, func() (string, error) {
		return l.Store.PutIfMatch(ctx, key, etag, body)
	})
}

func (l linz) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	return l.write(key, casreg.Input{Op: casreg.Create}, body, func() (string, error) {
		return l.Store.PutIfAbsent(ctx, key, body)
	})
}

// ignoresIfMatch is the mutant store: an If-Match PUT replaces whatever is
// there.
type ignoresIfMatch struct{ store.Store }

func (m ignoresIfMatch) PutIfMatch(ctx context.Context, key, _ string, body []byte) (string, error) {
	_, cur, err := m.Store.Get(ctx, key)
	if err != nil {
		return "", err
	}
	return m.Store.PutIfMatch(ctx, key, cur, body)
}

type doc struct {
	N      int    `json:"n"`
	Writer string `json:"writer"`
	ID     string `json:"id"`
}

func (d *doc) SetWrite(w, id string) { d.Writer, d.ID = w, id }

// Two writers read the same version and both commit: with If-Match the
// second loses; with the mutant store both "win", which the replicas' own
// bookkeeping cannot see but the history check does.
func TestLinearizabilityCatchesAStoreIgnoringIfMatch(t *testing.T) {
	for _, mutant := range []bool{false, true} {
		var base store.Store = store.NewMem()
		if mutant {
			base = ignoresIfMatch{base}
		}
		rec, et := &casreg.Recorder{}, &casreg.Etags{}
		a, b := linz{base, rec, et, 0}, linz{base, rec, et, 1}
		ctx := context.Background()
		if _, o, err := store.Commit(ctx, a, "k", "", "a", &doc{N: 0}, nil); !o.OK() {
			t.Fatal(o, err)
		}
		_, ea, _ := a.Get(ctx, "k")
		_, eb, _ := b.Get(ctx, "k")
		_, oa, _ := store.Commit(ctx, a, "k", ea, "a", &doc{N: 1}, nil)
		_, ob, _ := store.Commit(ctx, b, "k", eb, "b", &doc{N: 1}, nil)
		res, _ := rec.Check(0)
		t.Logf("mutant=%v: commits %s, %s; history %s", mutant, oa, ob, res)
		if want := map[bool]porcupine.CheckResult{false: porcupine.Ok, true: porcupine.Illegal}[mutant]; res != want {
			t.Fatalf("mutant=%v: %s, want %s", mutant, res, want)
		}
	}
}
