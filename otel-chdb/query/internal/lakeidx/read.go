package lakeidx

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// TailFetch is the first tail read of a segment (the header is usually
// well under it).
const TailFetch = 64 << 10

// ReadRange reads [off, off+n) of key through st, ranged when st can.
func ReadRange(ctx context.Context, st store.Store, key string, off, n int64) ([]byte, error) {
	if rg, ok := st.(store.RangeGetter); ok {
		return rg.GetRange(ctx, key, off, n)
	}
	body, _, err := st.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if body == nil || off < 0 || off+n > int64(len(body)) {
		return nil, corrupt("%s: short object", key)
	}
	return body[off : off+n], nil
}

// ReadHeader reads a segment's header: one tail GET, a second only when the
// header is longer than TailFetch. requests counts the GETs.
func ReadHeader(ctx context.Context, st store.Store, key string, size int64) (*Header, int, int64, error) {
	n := min(size, TailFetch)
	tail, err := ReadRange(ctx, st, key, size-n, n)
	if err != nil {
		return nil, 1, 0, err
	}
	h, need, err := ParseTail(tail, size)
	if err == nil && need > 0 {
		if need > size {
			return nil, 1, n, corrupt("%s: header longer than the object", key)
		}
		tail, err = ReadRange(ctx, st, key, size-need, need)
		if err != nil {
			return nil, 2, n, err
		}
		h, _, err = ParseTail(tail, size)
		return h, 2, n + need, err
	}
	return h, 1, n, err
}

// HeaderCache caches headers by key and size (segments are create-only and
// content-addressed: a key's bytes never change).
type HeaderCache struct {
	mu    sync.Mutex
	limit int
	m     map[string]*Header
	order []string
}

// NewHeaderCache holds up to limit headers.
func NewHeaderCache(limit int) *HeaderCache {
	return &HeaderCache{limit: limit, m: map[string]*Header{}}
}

func cacheKey(key string, size int64) string { return key + "\x00" + itoa(size) }

func itoa(v int64) string {
	var b [20]byte
	i := len(b)
	if v == 0 {
		return "0"
	}
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

// Get returns a cached header.
func (c *HeaderCache) Get(key string, size int64) *Header {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[cacheKey(key, size)]
}

// Put caches a header.
func (c *HeaderCache) Put(key string, size int64, h *Header) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey(key, size)
	if _, ok := c.m[k]; ok {
		return
	}
	if len(c.order) >= c.limit {
		delete(c.m, c.order[0])
		c.order = c.order[1:]
	}
	c.m[k] = h
	c.order = append(c.order, k)
}

// SegRef is a listed segment and its header.
type SegRef struct {
	Key    string
	Size   int64
	Header *Header
}

// Covering picks, for every object key the segments cover, the segment that
// answers for it: segments with more objects first (a merged hour before
// its pieces), then higher level, then key. An object is covered only
// when size (and ETag, when both sides know it) match.
func Covering(segs []SegRef) map[string]CoverRef {
	order := append([]SegRef(nil), segs...)
	sort.SliceStable(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if len(a.Header.Objects) != len(b.Header.Objects) {
			return len(a.Header.Objects) > len(b.Header.Objects)
		}
		if a.Header.Level != b.Header.Level {
			return a.Header.Level > b.Header.Level
		}
		return a.Key < b.Key
	})
	out := map[string]CoverRef{}
	for si := range order {
		for oi, o := range order[si].Header.Objects {
			if _, ok := out[o.Key]; !ok {
				out[o.Key] = CoverRef{Seg: &order[si], Obj: oi}
			}
		}
	}
	return out
}

// CoverRef is an object's covering segment.
type CoverRef struct {
	Seg *SegRef
	Obj int
}

// Matches says whether the covered object is the listed one.
func (c CoverRef) Matches(o store.Object) bool {
	so := c.Seg.Header.Objects[c.Obj]
	if so.Size != o.Size {
		return false
	}
	return so.ETag == "" || o.ETag == "" || strings.Trim(so.ETag, `"`) == strings.Trim(o.ETag, `"`)
}

// BucketOf is the hour an object's LastModified puts it in.
func BucketOf(t time.Time) string { return t.UTC().Format("20060102T15") }

// BucketStart parses a bucket name.
func BucketStart(b string) (time.Time, bool) {
	t, err := time.Parse("20060102T15", b)
	return t, err == nil
}
