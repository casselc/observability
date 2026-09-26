package commit

import (
	"context"
	"errors"
	"maps"
	"sort"
	"strings"
	"sync"
)

// Fault is what MemStore does to the next request of its kind.
type Fault int

const (
	// FaultNone answers normally.
	FaultNone Fault = iota
	// ApplyLoseAnswer applies the PUT and answers "no answer" (PutUnknown).
	ApplyLoseAnswer
	// Drop never applies the PUT and answers PutUnknown (a lost request).
	Drop
	// Hold keeps the PUT in flight and answers PutUnknown; ReleaseHeld
	// applies it later (a late request: create-only, so it may lose).
	Hold
	// HeadFail makes the next HEAD fail (no answer).
	HeadFail
)

// MemStore is an in-memory bucket with create-only semantics and fault
// injection, for the lane's tests and the model checks.
type MemStore struct {
	mu      sync.Mutex
	objects map[string]MemObject
	faults  []Fault // for PUTs, in order
	hfaults []Fault // for HEADs
	held    []heldPut
	Puts    int
	Heads   int
	// OnApply sees every PUT the store decides (applied or not), for the
	// model recorder: late=true for a released held PUT.
	OnApply func(key string, applied, late bool, meta map[string]string)
}

type MemObject struct {
	Body []byte
	Meta map[string]string
}

type heldPut struct {
	key  string
	body []byte
	meta map[string]string
}

func NewMemStore() *MemStore { return &MemStore{objects: map[string]MemObject{}} }

// Inject queues a fault for the next PUT (or HEAD, for HeadFail).
func (s *MemStore) Inject(f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f == HeadFail {
		s.hfaults = append(s.hfaults, f)
		return
	}
	s.faults = append(s.faults, f)
}

// createLocked applies a create-only write; false if the key is taken.
func (s *MemStore) createLocked(key string, body []byte, meta map[string]string, late bool) bool {
	_, taken := s.objects[key]
	if !taken {
		s.objects[key] = MemObject{Body: append([]byte(nil), body...), Meta: maps.Clone(meta)}
	}
	if s.OnApply != nil {
		s.OnApply(key, !taken, late, meta)
	}
	return !taken
}

func (s *MemStore) PutCreate(ctx context.Context, key string, body []byte, _ string, meta map[string]string) PutOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Puts++
	f := FaultNone
	if len(s.faults) > 0 {
		f, s.faults = s.faults[0], s.faults[1:]
	}
	switch f {
	case ApplyLoseAnswer:
		s.createLocked(key, body, meta, false)
		return PutUnknown
	case Drop:
		return PutUnknown
	case Hold:
		s.held = append(s.held, heldPut{key, append([]byte(nil), body...), maps.Clone(meta)})
		return PutUnknown
	}
	if ctx.Err() != nil {
		return PutUnknown
	}
	if s.createLocked(key, body, meta, false) {
		return PutOK
	}
	return PutExists
}

func (s *MemStore) Head(ctx context.Context, key string) (map[string]string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Heads++
	if len(s.hfaults) > 0 {
		s.hfaults = s.hfaults[1:]
		return nil, false, errors.New("injected: HEAD timed out")
	}
	o, ok := s.objects[key]
	if !ok {
		return nil, false, nil
	}
	return maps.Clone(o.Meta), true, nil
}

// ReleaseHeld applies every held PUT (create-only) and returns how many landed.
func (s *MemStore) ReleaseHeld() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, h := range s.held {
		if s.createLocked(h.key, h.body, h.meta, true) {
			n++
		}
	}
	s.held = nil
	return n
}

// Tomb puts a consumer's tombstone at key (create-only); false if taken.
func (s *MemStore) Tomb(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createLocked(key, nil, map[string]string{MetaKind: KindTomb}, false)
}

// Get returns an object.
func (s *MemStore) Get(key string) (MemObject, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	return o, ok
}

// Keys lists the keys under prefix, sorted.
func (s *MemStore) Keys(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
