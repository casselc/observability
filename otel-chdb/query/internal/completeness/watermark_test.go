package completeness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type fakeStore struct {
	body  []byte
	err   error
	reads int
}

func (f *fakeStore) get(context.Context, string) ([]byte, error) {
	f.reads++
	return f.body, f.err
}

func doc(ctNs uint64, wall time.Time) []byte {
	b, _ := json.Marshal(Doc{Format: 2, Version: 3, CompleteThroughNs: ctNs, WallMs: uint64(wall.UnixMilli()),
		Holding: []LaneWm{{Lane: "prod-a/p0/logs", WmNs: ctNs}, {Lane: "prod-b/p0/logs", WmNs: ctNs + 1}},
		Stale:   []LaneWm{{Lane: "prod-b/p1/traces", WmNs: 1}}})
	return b
}

var t0 = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func reader(f *fakeStore, now *time.Time) *Reader {
	r := NewReader(f.get, "otel/_consumer/watermark.json", 15*time.Second, 5*time.Minute)
	r.SetClock(func() time.Time { return *now })
	return r
}

func onlyA(c string) bool { return c == "prod-a" }

func TestLabels(t *testing.T) {
	now := t0
	ct := t0.Add(-40 * time.Second)
	f := &fakeStore{body: doc(uint64(ct.UnixNano()), t0.Add(-10*time.Second))}
	r := reader(f, &now)
	s := r.Get(context.Background())
	if s.Status != StatusOK {
		t.Fatalf("status %s", s.Status)
	}
	// a window that ends before complete_through is complete
	w := &Window{FromNs: ct.Add(-time.Hour).UnixNano(), ToNs: ct.Add(-time.Minute).UnixNano()}
	l := MakeLabel("central", s, w, now, r.Key(), onlyA, 0)
	if l.Completeness != "complete" || l.Partial || l.IncompleteFrom != nil {
		t.Fatalf("closed window: %+v", l)
	}
	if len(l.Watermark.Holding) != 1 || l.Watermark.Holding[0].Lane != "prod-a/p0/logs" || len(l.Watermark.Stale) != 0 {
		t.Fatalf("another cluster's lanes leaked: %+v", l.Watermark)
	}
	// one that extends past it is partial from complete_through on
	w = &Window{FromNs: ct.Add(-time.Hour).UnixNano(), ToNs: now.UnixNano()}
	l = MakeLabel("central", s, w, now, r.Key(), onlyA, 0)
	if l.Completeness != "partial" || !l.Partial || *l.IncompleteFromNs != ct.UnixNano() {
		t.Fatalf("open window: %+v", l)
	}
	// a window entirely after complete_through is incomplete from its start
	w = &Window{FromNs: ct.Add(time.Second).UnixNano(), ToNs: now.UnixNano()}
	l = MakeLabel("central", s, w, now, r.Key(), nil, 0)
	if *l.IncompleteFromNs != w.FromNs {
		t.Fatalf("incomplete_from %d", *l.IncompleteFromNs)
	}
	// no window: up to now, so partial
	l = MakeLabel("central", s, nil, now, r.Key(), nil, time.Minute)
	if !l.Partial || l.Completeness != "partial" {
		t.Fatalf("unbounded: %+v", l)
	}
}

func TestCaching(t *testing.T) {
	now := t0
	f := &fakeStore{body: doc(uint64(t0.UnixNano()), t0)}
	r := reader(f, &now)
	r.Get(context.Background())
	now = now.Add(10 * time.Second)
	r.Get(context.Background())
	if f.reads != 1 {
		t.Fatalf("read %d times inside the cache window", f.reads)
	}
	now = now.Add(10 * time.Second)
	r.Get(context.Background())
	if f.reads != 2 {
		t.Fatalf("not re-read after the cache window: %d", f.reads)
	}
}

func TestMissingStaleAndErrorAreNeverComplete(t *testing.T) {
	closed := func(ct time.Time) *Window {
		return &Window{FromNs: ct.Add(-2 * time.Hour).UnixNano(), ToNs: ct.Add(-time.Hour).UnixNano()}
	}
	cases := []struct {
		name    string
		f       *fakeStore
		advance time.Duration
		status  string
		hasCT   bool
	}{
		{"missing", &fakeStore{}, 0, StatusMissing, false},
		{"unreadable", &fakeStore{err: errors.New("503 Slow Down")}, 0, StatusError, false},
		{"garbage", &fakeStore{body: []byte("{not json")}, 0, StatusError, false},
		// the consumer last published 6 minutes ago: stale, value kept
		{"stale", &fakeStore{body: doc(uint64(t0.Add(-time.Hour).UnixNano()), t0.Add(-6*time.Minute))}, 0, StatusStale, true},
	}
	for _, c := range cases {
		now := t0.Add(c.advance)
		r := reader(c.f, &now)
		s := r.Get(context.Background())
		if s.Status != c.status {
			t.Errorf("%s: status %s", c.name, s.Status)
		}
		l := MakeLabel("central", s, closed(t0.Add(-3*time.Hour)), now, r.Key(), nil, 0)
		if l.Completeness != "unknown" || !l.Partial {
			t.Errorf("%s: a result must not read as complete: %+v", c.name, l)
		}
		if (l.CompleteThrough != nil) != c.hasCT {
			t.Errorf("%s: complete_through %v", c.name, l.CompleteThrough)
		}
	}
}

func TestAGoodCopyAgesOutWhenTheStoreFails(t *testing.T) {
	now := t0
	f := &fakeStore{body: doc(uint64(t0.Add(-time.Minute).UnixNano()), t0)}
	r := reader(f, &now)
	if s := r.Get(context.Background()); s.Status != StatusOK {
		t.Fatal(s.Status)
	}
	f.body, f.err = nil, errors.New("connection refused")
	now = now.Add(time.Minute)
	s := r.Get(context.Background())
	if s.Doc == nil || s.Status != StatusOK || s.Err == "" {
		t.Fatalf("a recent good copy should serve while the store fails: %+v", s)
	}
	now = now.Add(5 * time.Minute)
	s = r.Get(context.Background())
	if s.Status == StatusOK {
		t.Fatalf("a copy older than max age must not be ok: %+v", s)
	}
	if l := MakeLabel("central", s, &Window{FromNs: 0, ToNs: 1}, now, r.Key(), nil, 0); l.Completeness != "unknown" {
		t.Fatalf("%+v", l)
	}
}
