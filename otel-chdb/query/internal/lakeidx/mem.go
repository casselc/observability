package lakeidx

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// Mem is an in-memory bucket for tests: store.Store, store.RangeGetter and
// store.Writer, with ETags (MD5, as S3 single-part PUTs), request counts and
// injectable faults.
type Mem struct {
	mu   sync.Mutex
	objs map[string]memObj
	Now  func() time.Time
	// Fault hooks, consulted per call (key given); nil: none.
	// LosePut: the PUT is not applied and fails (a lost request).
	LosePut func(key string) bool
	// LoseAnswer: the PUT is applied, then fails (a lost answer).
	LoseAnswer func(key string) bool
	// FailGet fails reads of matching keys.
	FailGet                             func(key string) bool
	Gets, RangeGets, Puts, Lists, Heads int
	BytesRead                           int64
}

type memObj struct {
	body []byte
	meta map[string]string
	lm   time.Time
	etag string
}

// NewMem returns an empty bucket.
func NewMem() *Mem { return &Mem{objs: map[string]memObj{}, Now: time.Now} }

// Put stores an object unconditionally (test setup).
func (m *Mem) Put(key string, body []byte, meta map[string]string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = memObj{body: body, meta: meta, lm: at, etag: etagOf(body)}
}

// Delete removes an object (test setup).
func (m *Mem) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
}

// Body returns an object's bytes (tests).
func (m *Mem) Body(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.objs[key].body
}

// Keys lists every key with prefix (tests).
func (m *Mem) Keys(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func etagOf(b []byte) string {
	s := md5.Sum(b)
	return `"` + hex.EncodeToString(s[:]) + `"`
}

var errNotFound = errors.New("not found")

// Get implements store.Store.
func (m *Mem) Get(_ context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Gets++
	if m.FailGet != nil && m.FailGet(key) {
		return nil, "", fmt.Errorf("GET %s: injected", key)
	}
	o, ok := m.objs[key]
	if !ok {
		return nil, "", nil
	}
	m.BytesRead += int64(len(o.body))
	return bytes.Clone(o.body), o.etag, nil
}

// GetRange implements store.RangeGetter.
func (m *Mem) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RangeGets++
	if m.FailGet != nil && m.FailGet(key) {
		return nil, fmt.Errorf("GET %s: injected", key)
	}
	o, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("GET %s: %w", key, errNotFound)
	}
	if n < 0 {
		n = int64(len(o.body)) - off
	}
	if off < 0 || off+n > int64(len(o.body)) {
		return nil, fmt.Errorf("GET %s: range %d+%d of %d", key, off, n, len(o.body))
	}
	m.BytesRead += n
	return bytes.Clone(o.body[off : off+n]), nil
}

// Dirs implements store.Store.
func (m *Mem) Dirs(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Lists++
	seen := map[string]bool{}
	for k := range m.objs {
		if rest, ok := strings.CutPrefix(k, prefix); ok {
			if i := strings.IndexByte(rest, '/'); i > 0 {
				seen[rest[:i]] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

// List implements store.Store.
func (m *Mem) List(_ context.Context, prefix string, max int) ([]store.Object, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Lists++
	var out []store.Object
	for k, o := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, store.Object{Key: k, Size: int64(len(o.body)), LastModified: o.lm, ETag: o.etag})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > max {
		return out[:max], true, nil
	}
	return out, false, nil
}

// Head implements store.Store.
func (m *Mem) Head(_ context.Context, key string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Heads++
	o, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("HEAD %s: %w", key, errNotFound)
	}
	return o.meta, nil
}

// Presign implements store.Store.
func (m *Mem) Presign(_ context.Context, key string, ttl time.Duration) (string, error) {
	return fmt.Sprintf("https://mem.invalid/%s?X-Amz-Expires=%d", key, int(ttl.Seconds())), nil
}

func (m *Mem) put(key string, body []byte, meta map[string]string, cond func(o memObj, ok bool) bool) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Puts++
	if m.LosePut != nil && m.LosePut(key) {
		return false, fmt.Errorf("PUT %s: injected lost request", key)
	}
	o, ok := m.objs[key]
	if !cond(o, ok) {
		return false, nil
	}
	m.objs[key] = memObj{body: bytes.Clone(body), meta: meta, lm: m.Now(), etag: etagOf(body)}
	if m.LoseAnswer != nil && m.LoseAnswer(key) {
		return false, fmt.Errorf("PUT %s: injected lost answer", key)
	}
	return true, nil
}

// PutCreate implements store.Writer.
func (m *Mem) PutCreate(_ context.Context, key string, body []byte, meta map[string]string) (bool, error) {
	return m.put(key, body, meta, func(_ memObj, ok bool) bool { return !ok })
}

// PutIfMatch implements store.Writer.
func (m *Mem) PutIfMatch(_ context.Context, key string, body []byte, etag string) (bool, error) {
	return m.put(key, body, nil, func(o memObj, ok bool) bool {
		if etag == "" {
			return !ok
		}
		return ok && o.etag == etag
	})
}
