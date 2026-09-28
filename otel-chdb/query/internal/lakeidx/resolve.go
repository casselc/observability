package lakeidx

// Resolution on the query service: given a plan's candidate objects and a
// filter, which row groups of which objects may hold a match. Every object
// no verified segment covers is "scan"; an object whose covering segment
// says no row group matches is "none" (the planner drops it); otherwise
// "hit" with the row groups.

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// Filter is what a plan narrows by. Both set: both must hold.
type Filter struct {
	TraceID string   // 32 hex digits
	Terms   []string // each a case-insensitive substring of the text column
}

// Empty says whether f narrows nothing.
func (f Filter) Empty() bool { return f.TraceID == "" && len(f.Terms) == 0 }

// Constraints of every term, in order.
func (f Filter) constraints() []Constraint {
	var out []Constraint
	for _, t := range f.Terms {
		out = append(out, Constraints(t)...)
	}
	return out
}

// Status of one object under the index.
const (
	Hit  = "hit"  // covered; RowGroups may match
	Scan = "scan" // not covered (or the index could not be read): read it all
	None = "none" // covered; nothing can match
)

// ObjResult is one object's answer.
type ObjResult struct {
	Status    string
	RowGroups []int
}

// Report says what resolution did, for the plan and the audit.
type Report struct {
	TraceID     string   `json:"trace_id,omitempty"`
	Terms       []string `json:"terms,omitempty"`
	Constraints []string `json:"constraints,omitempty"`
	Segments    int      `json:"segments"`
	Requests    int      `json:"requests"`
	Bytes       int64    `json:"bytes"`
	CacheHits   int      `json:"cache_hits"`
	Covered     int      `json:"covered"`
	Scan        int      `json:"scan"`
	Pruned      int      `json:"pruned"`
	Errors      []string `json:"errors,omitempty"`
	Note        string   `json:"note,omitempty"`
	ElapsedMs   int64    `json:"elapsed_ms"`
}

// ResolverConfig tunes resolution.
type ResolverConfig struct {
	Root string
	// MaxBytesPerPlan bounds index bytes read for one plan (cache misses);
	// segments past it leave their objects "scan".
	MaxBytesPerPlan int64
	// CacheBytes bounds the block cache; HeaderCacheN the header cache.
	CacheBytes   int64
	HeaderCacheN int
	Concurrency  int
}

// Resolver resolves filters against the index, caching headers and blocks.
type Resolver struct {
	cfg    ResolverConfig
	st     store.Store
	hdrs   *HeaderCache
	blocks *blockCache
}

// NewResolver returns a resolver reading through st.
func NewResolver(cfg ResolverConfig, st store.Store) *Resolver {
	if cfg.MaxBytesPerPlan <= 0 {
		cfg.MaxBytesPerPlan = 64 << 20
	}
	if cfg.CacheBytes <= 0 {
		cfg.CacheBytes = 128 << 20
	}
	if cfg.HeaderCacheN <= 0 {
		cfg.HeaderCacheN = 10_000
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	return &Resolver{cfg: cfg, st: st, hdrs: NewHeaderCache(cfg.HeaderCacheN), blocks: newBlockCache(cfg.CacheBytes)}
}

type budget struct {
	mu        sync.Mutex
	left      int64
	requests  int
	bytes     int64
	cacheHits int
}

func (b *budget) take(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.left {
		return false
	}
	b.left -= n
	b.requests++
	b.bytes += n
	return true
}

func (b *budget) hit() {
	b.mu.Lock()
	b.cacheHits++
	b.mu.Unlock()
}

// Resolve answers f for the objects (one cluster's, of one signal).
func (r *Resolver) Resolve(ctx context.Context, cluster, signal string, objs []store.Object, f Filter) (map[string]ObjResult, Report) {
	t0 := time.Now()
	rep := Report{TraceID: f.TraceID, Terms: f.Terms}
	cons := f.constraints()
	for _, c := range cons {
		rep.Constraints = append(rep.Constraints, c.Match.String()+":"+c.Text)
	}
	out := map[string]ObjResult{}
	for _, o := range objs {
		out[o.Key] = ObjResult{Status: Scan}
	}
	if f.TraceID == "" && len(cons) == 0 {
		rep.Note = "the filter constrains no token: nothing to look up"
		rep.Scan = len(objs)
		return out, rep
	}
	bud := &budget{left: r.cfg.MaxBytesPerPlan}
	byBucket := map[string][]store.Object{}
	for _, o := range objs {
		b := BucketOf(o.LastModified)
		byBucket[b] = append(byBucket[b], o)
	}
	type job struct {
		seg  *SegRef
		objs []int // indexes into the segment's objects
		keys []string
	}
	var jobs []job
	var mu sync.Mutex
	fail := func(format string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		if len(rep.Errors) < 20 {
			rep.Errors = append(rep.Errors, fmt.Sprintf(format, a...))
		}
	}
	for _, bk := range sortedKeys(byBucket) {
		segs, err := r.segments(ctx, cluster, signal, bk, bud, fail)
		if err != nil {
			fail("%s: %v", bk, err)
			continue
		}
		cov := Covering(segs)
		per := map[*SegRef]*job{}
		for _, o := range byBucket[bk] {
			c, ok := cov[o.Key]
			if !ok || !c.Matches(o) {
				continue
			}
			j := per[c.Seg]
			if j == nil {
				j = &job{seg: c.Seg}
				per[c.Seg] = j
			}
			j.objs = append(j.objs, c.Obj)
			j.keys = append(j.keys, o.Key)
		}
		start := len(jobs)
		for _, j := range per {
			jobs = append(jobs, *j)
		}
		sort.Slice(jobs[start:], func(a, b int) bool { return jobs[start+a].seg.Key < jobs[start+b].seg.Key })
	}
	sem := make(chan struct{}, r.cfg.Concurrency)
	var wg sync.WaitGroup
	for i := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j job) {
			defer wg.Done()
			defer func() { <-sem }()
			rgs, err := r.evalSegment(ctx, j.seg, f.TraceID, cons, bud)
			if err != nil {
				fail("%s: %v", j.seg.Key, err)
				return
			}
			h := j.seg.Header
			mu.Lock()
			defer mu.Unlock()
			for n, oi := range j.objs {
				o := h.Objects[oi]
				var mine []int
				for _, g := range rgs {
					if g >= o.Base && g < o.Base+uint32(len(o.RowGroups)) {
						mine = append(mine, int(g-o.Base))
					}
				}
				if len(mine) == 0 {
					out[j.keys[n]] = ObjResult{Status: None}
				} else {
					out[j.keys[n]] = ObjResult{Status: Hit, RowGroups: mine}
				}
			}
		}(jobs[i])
	}
	wg.Wait()
	rep.Segments = len(jobs)
	for _, o := range objs {
		switch out[o.Key].Status {
		case Scan:
			rep.Scan++
		case None:
			rep.Covered++
			rep.Pruned++
		default:
			rep.Covered++
		}
	}
	rep.Requests, rep.Bytes, rep.CacheHits = bud.requests, bud.bytes, bud.cacheHits
	rep.ElapsedMs = time.Since(t0).Milliseconds()
	return out, rep
}

// segments lists and reads one hour's segment headers; unreadable ones are
// left out (their objects scan) and reported.
func (r *Resolver) segments(ctx context.Context, cluster, signal, bucket string, bud *budget, fail func(string, ...any)) ([]SegRef, error) {
	objs, _, err := r.st.List(ctx, BucketPrefix(r.cfg.Root, cluster, signal, bucket), 100_000)
	if err != nil {
		return nil, err
	}
	bud.mu.Lock()
	bud.requests++
	bud.mu.Unlock()
	var out []SegRef
	for _, o := range objs {
		if len(o.Key) < 5 || o.Key[len(o.Key)-5:] != ".osix" {
			continue
		}
		h := r.hdrs.Get(o.Key, o.Size)
		if h == nil {
			if !bud.take(min(o.Size, TailFetch)) {
				fail("%s: index byte budget spent", o.Key)
				continue
			}
			var n int
			var nb int64
			h, n, nb, err = ReadHeader(ctx, r.st, o.Key, o.Size)
			if n > 1 {
				bud.take(nb - min(o.Size, TailFetch))
			}
			if err != nil {
				fail("%s: %v", o.Key, err)
				continue
			}
			if h.Cluster != cluster || h.Signal != signal || h.Bucket != bucket {
				fail("%s: header names %s/%s/%s", o.Key, h.Cluster, h.Signal, h.Bucket)
				continue
			}
			r.hdrs.Put(o.Key, o.Size, h)
		} else {
			bud.hit()
		}
		out = append(out, SegRef{Key: o.Key, Size: o.Size, Header: h})
	}
	return out, nil
}

// evalSegment returns the segment's global row groups that may match.
func (r *Resolver) evalSegment(ctx context.Context, seg *SegRef, traceID string, cons []Constraint, bud *budget) ([]uint32, error) {
	h := seg.Header
	var set []uint32 // nil: every row group so far
	all := true
	and := func(rgs []uint32) {
		if all {
			set, all = rgs, false
			return
		}
		set = intersect(set, rgs)
	}
	if traceID != "" {
		if h.Trace == nil {
			return nil, fmt.Errorf("no trace section")
		}
		fp := FP(traceID)
		bi := h.TraceBlockFor(fp)
		var rgs []uint32
		if bi >= 0 {
			b := h.Trace.Blocks[bi]
			blk, err := r.block(ctx, seg.Key, b.Off, b.Len, bud)
			if err != nil {
				return nil, err
			}
			if rgs, err = LookupTrace(blk, b, fp); err != nil {
				return nil, err
			}
		}
		and(rgs)
	}
	if len(cons) > 0 {
		if h.Terms == nil {
			return nil, fmt.Errorf("no term section")
		}
		bs := h.Terms.Blocks
		for _, c := range cons {
			if !all && len(set) == 0 {
				break
			}
			lo, hi, withFirst := h.TermBlocksFor(c)
			need := []int{}
			if withFirst {
				need = append(need, 0)
			}
			for i := max(lo, 0); i < hi; i++ {
				need = append(need, i)
			}
			if err := r.prefetch(ctx, seg.Key, bs, need, bud); err != nil {
				return nil, err
			}
			var u []uint32
			every := false
			for _, i := range need {
				blk, err := r.block(ctx, seg.Key, bs[i].Off, bs[i].Len, bud)
				if err != nil {
					return nil, err
				}
				err = ScanTermBlock(blk, bs[i], func(term string, a bool, rgs []uint32) bool {
					if Selects(c, term, h.Terms.MaxTerm) {
						if a {
							every = true
							return false
						}
						u = append(u, rgs...)
					}
					return true
				})
				if err != nil {
					return nil, err
				}
				if every {
					break
				}
			}
			if every {
				continue // a frequent term: this constraint narrows nothing
			}
			and(uniq(u))
		}
	}
	if all {
		out := make([]uint32, h.RowGroups)
		for i := range out {
			out[i] = uint32(i)
		}
		return out, nil
	}
	return set, nil
}

func intersect(a, b []uint32) []uint32 {
	var out []uint32
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

func uniq(u []uint32) []uint32 {
	sort.Slice(u, func(i, j int) bool { return u[i] < u[j] })
	out := u[:0]
	for i, v := range u {
		if i == 0 || v != u[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// prefetch reads the uncached blocks among need with as few range GETs as
// contiguous runs allow (term blocks are adjacent in the file).
func (r *Resolver) prefetch(ctx context.Context, key string, bs []TermBlock, need []int, bud *budget) error {
	var run []int
	flush := func() error {
		if len(run) == 0 {
			return nil
		}
		first, last := bs[run[0]], bs[run[len(run)-1]]
		n := last.Off + last.Len - first.Off
		if !bud.take(n) {
			return fmt.Errorf("index byte budget spent")
		}
		buf, err := ReadRange(ctx, r.st, key, first.Off, n)
		if err != nil {
			return err
		}
		for _, i := range run {
			b := bs[i]
			r.blocks.put(key, b.Off, buf[b.Off-first.Off:b.Off-first.Off+b.Len])
		}
		run = run[:0]
		return nil
	}
	for _, i := range need {
		if r.blocks.get(key, bs[i].Off) != nil {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if len(run) > 0 && bs[run[len(run)-1]].Off+bs[run[len(run)-1]].Len != bs[i].Off {
			if err := flush(); err != nil {
				return err
			}
		}
		run = append(run, i)
	}
	return flush()
}

// block returns [off, off+n) of a segment, from the cache or one range GET.
func (r *Resolver) block(ctx context.Context, key string, off, n int64, bud *budget) ([]byte, error) {
	if b := r.blocks.get(key, off); b != nil {
		bud.hit()
		return b, nil
	}
	if !bud.take(n) {
		return nil, fmt.Errorf("index byte budget spent")
	}
	b, err := ReadRange(ctx, r.st, key, off, n)
	if err != nil {
		return nil, err
	}
	r.blocks.put(key, off, b)
	return b, nil
}

// blockCache is an LRU of segment blocks by (key, offset), bounded in bytes.
type blockCache struct {
	mu    sync.Mutex
	limit int64
	size  int64
	m     map[string]*cacheEnt
	head  *cacheEnt // most recent
	tail  *cacheEnt
}

type cacheEnt struct {
	k          string
	b          []byte
	prev, next *cacheEnt
}

func newBlockCache(limit int64) *blockCache {
	return &blockCache{limit: limit, m: map[string]*cacheEnt{}}
}

func (c *blockCache) unlink(e *cacheEnt) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (c *blockCache) front(e *cacheEnt) {
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *blockCache) get(key string, off int64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.m[cacheKey(key, off)]
	if e == nil {
		return nil
	}
	c.unlink(e)
	c.front(e)
	return e.b
}

func (c *blockCache) put(key string, off int64, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey(key, off)
	if _, ok := c.m[k]; ok || int64(len(b)) > c.limit {
		return
	}
	e := &cacheEnt{k: k, b: append([]byte(nil), b...)}
	c.m[k] = e
	c.front(e)
	c.size += int64(len(b))
	for c.size > c.limit && c.tail != nil {
		t := c.tail
		c.unlink(t)
		delete(c.m, t.k)
		c.size -= int64(len(t.b))
	}
}
