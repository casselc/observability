package publisher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Op is one write reaching the store.
type Op struct {
	Kind  string // "table" or "object"
	Key   string // table name or object key
	Batch uint64 // for tables
}

// Fault is what an injected hook does to a write.
type Fault int

const (
	Apply     Fault = iota // write lands, success reported
	Fail                   // write does not land, error reported
	Ambiguous              // write lands, error reported (a timeout after the PUT was applied)
)

// ErrFailed and the ambiguous error classify themselves for quintgo
// (qobs.Outcomer), so the recorded outcome says whether the write landed.
// That is ground truth only a test double has; see the README on
// observability.
var ErrFailed = errors.New("injected: write failed")

type ambiguousError struct{ op Op }

func (e ambiguousError) Error() string {
	return fmt.Sprintf("injected: timeout writing %s (it landed)", e.op.Key)
}
func (e ambiguousError) QuintOutcome() string { return "ambiguous" }

type failedError struct{ op Op }

func (e failedError) Error() string        { return fmt.Sprintf("injected: %s write failed", e.op.Key) }
func (e failedError) QuintOutcome() string { return "error" }
func (e failedError) Unwrap() error        { return ErrFailed }

// MemStore is an in-memory S3 plus table store with a fault-injection hook.
type MemStore struct {
	mu      sync.Mutex
	tables  map[string][]uint64 // table -> batch ids inserted
	objects map[string][]byte
	// Hook, if set, runs before each write (outside the store's lock, so it
	// may block to hold a write back) and decides its fate.
	Hook func(ctx context.Context, op Op) Fault
}

func NewMemStore() *MemStore {
	return &MemStore{tables: map[string][]uint64{}, objects: map[string][]byte{}}
}

func (m *MemStore) fate(ctx context.Context, op Op) Fault {
	if m.Hook == nil {
		return Apply
	}
	return m.Hook(ctx, op)
}

func (m *MemStore) InsertTable(ctx context.Context, table string, batch uint64, rows []byte) error {
	op := Op{Kind: "table", Key: table, Batch: batch}
	f := m.fate(ctx, op)
	if f != Fail {
		m.mu.Lock()
		m.tables[table] = append(m.tables[table], batch)
		m.mu.Unlock()
	}
	return errFor(f, op)
}

func (m *MemStore) PutObject(ctx context.Context, key string, data []byte) error {
	op := Op{Kind: "object", Key: key}
	f := m.fate(ctx, op)
	if f != Fail {
		m.mu.Lock()
		m.objects[key] = append([]byte(nil), data...)
		m.mu.Unlock()
	}
	return errFor(f, op)
}

func errFor(f Fault, op Op) error {
	switch f {
	case Fail:
		return failedError{op}
	case Ambiguous:
		return ambiguousError{op}
	}
	return nil
}

// Keys lists stored objects.
func (m *MemStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ks []string
	for k := range m.objects {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// Tables returns, per table, the batch ids inserted into it.
func (m *MemStore) Tables() map[string][]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string][]uint64{}
	for t, bs := range m.tables {
		out[t] = append([]uint64(nil), bs...)
	}
	return out
}

// Object returns a stored object.
func (m *MemStore) Object(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	return b, ok
}
