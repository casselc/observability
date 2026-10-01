package lakeidx

// The indexer: follows a cluster's lanes of one signal and writes segments
// for the data objects no segment covers yet.
//
// Restart- and race-safety comes from three facts, not from the progress
// document:
//   - what is indexed is exactly what the segments' headers list (a reader
//     never trusts anything else);
//   - segments are create-only and content-addressed, so a crashed pass
//     that re-runs, or two indexers building the same bytes, meet at one key
//     and the second write is a 412, i.e. already done;
//   - before building, a pass subtracts what the hour's segments already
//     cover (one LIST and cached headers).
// The progress document (CAS by ETag) only saves the LIST-and-header work
// for lanes' old slots and publishes lag; losing it costs time, not data.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// Bucket is what the indexer needs of the store.
type Bucket interface {
	store.Store
	store.RangeGetter
	store.Writer
}

// Config tunes the indexer.
type Config struct {
	Root     string   `json:"root"`
	Clusters []string `json:"clusters"` // empty: every cluster under root
	Signals  []string `json:"signals"`  // default traces, logs
	// MaxSegmentObjects / MaxSegmentBytes bound one L0 segment's sources.
	MaxSegmentObjects int   `json:"max_segment_objects"`
	MaxSegmentBytes   int64 `json:"max_segment_bytes"`
	// MergeAfterS: an hour's L0 segments are merged into one L1 this long
	// after the hour ends; MergeLookbackH: hours older than this are left.
	MergeAfterS    int `json:"merge_after_s"`
	MergeLookbackH int `json:"merge_lookback_h"`
	// SkewS: indexed_through stays this far behind the LIST (advisory).
	SkewS   int         `json:"skew_s"`
	ListMax int         `json:"list_max"`
	Build   BuildConfig `json:"build"`
	// The per-day trace-id level (day.go): built MergeAfterS after a day
	// ends, for days at most DayLookbackD old (default 3); shards of at
	// most DayShardBytes (default 8 MiB), at least 2^DayMinBits of them
	// (default 2: 4 shards). NoDays turns it off.
	NoDays        bool  `json:"no_days"`
	DayLookbackD  int   `json:"day_lookback_d"`
	DayShardBytes int64 `json:"day_shard_bytes"`
	DayMinBits    *int  `json:"day_min_bits"`
}

func (c *Config) defaults() {
	c.Root = strings.Trim(c.Root, "/")
	if len(c.Signals) == 0 {
		c.Signals = []string{"traces", "logs"}
	}
	if c.MaxSegmentObjects <= 0 {
		c.MaxSegmentObjects = 256
	}
	if c.MaxSegmentBytes <= 0 {
		c.MaxSegmentBytes = 256 << 20
	}
	if c.MergeAfterS <= 0 {
		c.MergeAfterS = 600
	}
	if c.MergeLookbackH <= 0 {
		c.MergeLookbackH = 48
	}
	if c.SkewS <= 0 {
		c.SkewS = 60
	}
	if c.ListMax <= 0 {
		c.ListMax = 100_000
	}
	if c.DayLookbackD <= 0 {
		c.DayLookbackD = 3
	}
	if c.DayShardBytes <= 0 {
		c.DayShardBytes = 8 << 20
	}
	if c.DayMinBits == nil {
		two := 2
		c.DayMinBits = &two
	}
	if c.Build.Builder == "" {
		c.Build.Builder = "lakeindex/1"
	}
}

// Columns indexed per signal: the trace-id column, and the text column.
func Columns(signal string) (trace, text string, ok bool) {
	switch signal {
	case "traces":
		return "TraceId", "", true
	case "logs":
		return "TraceId", "Body", true
	}
	return "", "", false
}

// Indexer indexes.
type Indexer struct {
	cfg   Config
	b     Bucket
	hdrs  *HeaderCache
	bad   map[string]bool // segment keys whose bytes did not verify
	Now   func() time.Time
	Stats Stats
}

// Stats are cumulative counters (exported as metrics by the command).
type Stats struct {
	Passes, Segments, Merges, ObjectsIndexed, BytesRead, Unindexable, PutConflicts, Errors int64
	Rows                                                                                   int64
	BuildNs                                                                                int64
	SegmentBytes                                                                           int64
	Days                                                                                   int64 // day manifests written
}

// New returns an indexer over b.
func New(cfg Config, b Bucket) *Indexer {
	cfg.defaults()
	return &Indexer{cfg: cfg, b: b, hdrs: NewHeaderCache(10_000), bad: map[string]bool{}, Now: time.Now}
}

// Progress is {root}/{cluster}/_index/v1/{signal}.progress.json.
type Progress struct {
	Format  int    `json:"format"`
	Cluster string `json:"cluster"`
	Signal  string `json:"signal"`
	// Lanes: lane ("{producer}") → epoch → next: every slot below next is
	// covered by a segment, empty (a heartbeat or tombstone) or
	// unindexable. A hint: readers never use it.
	Lanes map[string]map[string]uint64 `json:"lanes"`
	// IndexedThroughMs: every data object with LastModified below it was
	// covered at WallMs (LIST time minus SkewS; advisory, for lag).
	IndexedThroughMs int64 `json:"indexed_through_ms"`
	WallMs           int64 `json:"wall_ms"`
	// Unindexable: objects that could not be decoded (bounded); readers
	// scan them like any uncovered object.
	Unindexable []string `json:"unindexable,omitempty"`
}

var (
	epochRE = regexp.MustCompile(`^\d{8}T\d{6}\.\d{3}Z-[0-9a-f]{8}$`)
	seqRE   = regexp.MustCompile(`^(\d{20})\.parquet$`)
	nameRE  = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)
)

// PassReport is one pass's outcome for (cluster, signal).
type PassReport struct {
	Cluster, Signal  string
	Listed, Pending  int
	Indexed          int
	Segments         []string
	Merged           []string
	Days             []string // day manifests written (day.go)
	Unindexable      []string
	IndexedThroughMs int64
}

// RunOnce makes one pass over every configured cluster and signal. An
// error in one (cluster, signal) does not stop the others; the first is
// returned.
func (ix *Indexer) RunOnce(ctx context.Context) ([]PassReport, error) {
	clusters := ix.cfg.Clusters
	if len(clusters) == 0 {
		prefix := ""
		if ix.cfg.Root != "" {
			prefix = ix.cfg.Root + "/"
		}
		dirs, err := ix.b.Dirs(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, d := range dirs {
			if nameRE.MatchString(d) {
				clusters = append(clusters, d)
			}
		}
	}
	var reps []PassReport
	var first error
	for _, cl := range clusters {
		for _, sig := range ix.cfg.Signals {
			r, err := ix.Pass(ctx, cl, sig)
			if err != nil {
				ix.Stats.Errors++
				if first == nil {
					first = fmt.Errorf("%s/%s: %w", cl, sig, err)
				}
			}
			reps = append(reps, r)
		}
	}
	ix.Stats.Passes++
	return reps, first
}

type slot struct {
	obj      store.Object
	producer string
	epoch    string
	seq      uint64
}

// Pass indexes one cluster's signal: new objects into L0 segments, then
// merges of finished hours.
func (ix *Indexer) Pass(ctx context.Context, cluster, signal string) (PassReport, error) {
	rep := PassReport{Cluster: cluster, Signal: signal}
	traceCol, textCol, ok := Columns(signal)
	if !ok {
		return rep, fmt.Errorf("signal %q is not indexed", signal)
	}
	listAt := ix.Now()
	prog, etag, err := ix.readProgress(ctx, cluster, signal)
	if err != nil {
		return rep, err
	}
	producers, err := ix.b.Dirs(ctx, join(ix.cfg.Root, cluster)+"/")
	if err != nil {
		return rep, err
	}
	var slots []slot
	for _, prod := range producers {
		if !nameRE.MatchString(prod) {
			continue // _index and other control prefixes
		}
		prefix := join(ix.cfg.Root, cluster+"/"+prod+"/"+signal) + "/"
		objs, trunc, err := ix.b.List(ctx, prefix, ix.cfg.ListMax)
		if err != nil {
			return rep, err
		}
		if trunc {
			return rep, fmt.Errorf("LIST %s: more than %d objects", prefix, ix.cfg.ListMax)
		}
		for _, o := range objs {
			ep, name, ok := strings.Cut(strings.TrimPrefix(o.Key, prefix), "/")
			if !ok || !epochRE.MatchString(ep) {
				continue
			}
			m := seqRE.FindStringSubmatch(name)
			if m == nil {
				continue
			}
			seq, _ := strconv.ParseUint(m[1], 10, 64)
			slots = append(slots, slot{obj: o, producer: prod, epoch: ep, seq: seq})
		}
	}
	rep.Listed = len(slots)
	unindexable := map[string]bool{}
	for _, k := range prog.Unindexable {
		unindexable[k] = true
	}
	// candidates: data slots at or past their epoch's next
	byBucket := map[string][]slot{}
	for _, s := range slots {
		if s.obj.Size == 0 || s.seq < prog.Lanes[s.producer][s.epoch] || unindexable[s.obj.Key] {
			continue
		}
		b := BucketOf(s.obj.LastModified)
		byBucket[b] = append(byBucket[b], s)
	}
	covered := map[string]bool{}
	buckets := sortedKeys(byBucket)
	for _, bk := range buckets {
		segs, err := ix.segments(ctx, cluster, signal, bk)
		if err != nil {
			return rep, err
		}
		cov := Covering(segs)
		var todo []slot
		for _, s := range byBucket[bk] {
			if c, ok := cov[s.obj.Key]; ok && c.Matches(s.obj) {
				covered[s.obj.Key] = true
				continue
			}
			todo = append(todo, s)
		}
		rep.Pending += len(todo)
		sort.Slice(todo, func(i, j int) bool { return todo[i].obj.Key < todo[j].obj.Key })
		for len(todo) > 0 {
			n, bytes := 0, int64(0)
			for n < len(todo) && n < ix.cfg.MaxSegmentObjects && (n == 0 || bytes+todo[n].obj.Size <= ix.cfg.MaxSegmentBytes) {
				bytes += todo[n].obj.Size
				n++
			}
			key, done, bad, err := ix.buildL0(ctx, cluster, signal, bk, traceCol, textCol, todo[:n])
			for _, k := range bad {
				unindexable[k] = true
				rep.Unindexable = append(rep.Unindexable, k)
			}
			if err != nil {
				return rep, err
			}
			for _, k := range done {
				covered[k] = true
			}
			if key != "" {
				rep.Segments = append(rep.Segments, key)
			}
			rep.Indexed += len(done)
			todo = todo[n:]
		}
	}
	// progress: advance each epoch over contiguous passed slots
	next := map[string]map[string]uint64{}
	for p, eps := range prog.Lanes {
		next[p] = map[string]uint64{}
		for e, n := range eps {
			next[p][e] = n
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].obj.Key < slots[j].obj.Key })
	throughMs := listAt.Add(-time.Duration(ix.cfg.SkewS) * time.Second).UnixMilli()
	for _, s := range slots {
		passed := s.obj.Size == 0 || covered[s.obj.Key] || unindexable[s.obj.Key] || s.seq < next[s.producer][s.epoch]
		if !passed {
			if lm := s.obj.LastModified.UnixMilli(); lm < throughMs {
				throughMs = lm
			}
			continue
		}
		if next[s.producer] == nil {
			next[s.producer] = map[string]uint64{}
		}
		if s.seq == next[s.producer][s.epoch] {
			next[s.producer][s.epoch] = s.seq + 1
		}
	}
	rep.IndexedThroughMs = throughMs
	bad := sortedKeys(unindexable)
	if len(bad) > 1000 {
		bad = bad[len(bad)-1000:]
	}
	np := Progress{Format: FormatVersion, Cluster: cluster, Signal: signal, Lanes: next, IndexedThroughMs: throughMs,
		WallMs: ix.Now().UnixMilli(), Unindexable: bad}
	if err := ix.writeProgress(ctx, cluster, signal, np, prog, etag); err != nil {
		return rep, err
	}
	merged, err := ix.mergeHours(ctx, cluster, signal, traceCol, textCol)
	rep.Merged = merged
	if err != nil {
		return rep, err
	}
	rep.Days, err = ix.buildDays(ctx, cluster, signal, traceCol)
	return rep, err
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// segments lists one hour's segments with their headers. A segment whose
// header cannot be read is left out (its objects are indexed again: a
// fresh segment for them is correct, a missing one would not be).
func (ix *Indexer) segments(ctx context.Context, cluster, signal, bucket string) ([]SegRef, error) {
	objs, _, err := ix.b.List(ctx, BucketPrefix(ix.cfg.Root, cluster, signal, bucket), ix.cfg.ListMax)
	if err != nil {
		return nil, err
	}
	var out []SegRef
	for _, o := range objs {
		if !strings.HasSuffix(o.Key, ".osix") {
			continue
		}
		h := ix.hdrs.Get(o.Key, o.Size)
		if h == nil {
			h, _, _, err = ReadHeader(ctx, ix.b, o.Key, o.Size)
			if err != nil {
				if errors.Is(err, ErrCorrupt) {
					ix.Stats.Errors++
					ix.bad[o.Key] = true
					continue
				}
				return nil, err
			}
			if h.Cluster != cluster || h.Signal != signal || h.Bucket != bucket {
				ix.Stats.Errors++
				ix.bad[o.Key] = true
				continue // a segment that names another place: not ours to trust
			}
			ix.hdrs.Put(o.Key, o.Size, h)
		}
		out = append(out, SegRef{Key: o.Key, Size: o.Size, Header: h})
	}
	return out, nil
}

// buildL0 reads the objects, builds one segment and writes it create-only.
// done lists the objects it covers; bad those that could not be decoded.
func (ix *Indexer) buildL0(ctx context.Context, cluster, signal, bucket, traceCol, textCol string, objs []slot) (key string, done, bad []string, err error) {
	t0 := time.Now()
	bld := NewBuilder(ix.cfg.Build, traceCol, textCol)
	cols := []string{traceCol}
	if textCol != "" {
		cols = append(cols, textCol)
	}
	for _, s := range objs {
		body, etag, err := ix.b.Get(ctx, s.obj.Key)
		if err != nil {
			return "", nil, bad, err
		}
		if body == nil {
			continue // deleted since the LIST (GC): nothing to cover
		}
		ix.Stats.BytesRead += int64(len(body))
		if int64(len(body)) != s.obj.Size {
			return "", nil, bad, fmt.Errorf("GET %s: %d bytes, LIST said %d", s.obj.Key, len(body), s.obj.Size)
		}
		type val struct {
			rg  int
			col int
			v   string
		}
		var rgRows []int64
		var vals []val
		err = RowGroupValues(body, cols, func(rg int, n int64) { rgRows = append(rgRows, n) },
			func(rg, col int, v []byte) { vals = append(vals, val{rg, col, string(v)}) })
		if err != nil {
			bad = append(bad, s.obj.Key)
			continue
		}
		if etag == "" {
			etag = s.obj.ETag
		}
		base := bld.AddObject(SegObject{Key: s.obj.Key, Size: s.obj.Size, ETag: strings.Trim(etag, `"`),
			LastModifiedMs: s.obj.LastModified.UnixMilli(), RowGroups: rgRows})
		for _, v := range vals {
			if v.col == 0 {
				bld.AddTraceID(base+uint32(v.rg), v.v)
			} else {
				bld.AddText(base+uint32(v.rg), v.v)
			}
		}
		for _, n := range rgRows {
			ix.Stats.Rows += n
		}
		done = append(done, s.obj.Key)
	}
	if len(done) == 0 {
		return "", nil, bad, nil
	}
	body, _, err := bld.Build(cluster, signal, bucket, 0)
	if err != nil {
		return "", nil, bad, err
	}
	ix.Stats.BuildNs += time.Since(t0).Nanoseconds()
	key, err = ix.put(ctx, Key(ix.cfg.Root, cluster, signal, bucket, 0, body), body)
	if err != nil {
		return "", nil, bad, err
	}
	ix.Stats.ObjectsIndexed += int64(len(done))
	return key, done, bad, nil
}

// put writes a segment create-only. A key already there whose bytes did
// not verify (storage corruption under a content-addressed name) is
// written beside, as {key}-r{n}.osix, so a rebuild is not stuck behind it.
func (ix *Indexer) put(ctx context.Context, key string, body []byte) (string, error) {
	for n := 1; ix.bad[key] && n < 10; n++ {
		key = strings.TrimSuffix(strings.TrimSuffix(key, ".osix"), fmt.Sprintf("-r%d", n-1)) + fmt.Sprintf("-r%d.osix", n)
	}
	created, err := ix.b.PutCreate(ctx, key, body, map[string]string{"oscope-kind": "index"})
	if err != nil {
		return "", err // unknown: the next pass finds the segment by LIST, or rebuilds the same key
	}
	if !created {
		ix.Stats.PutConflicts++ // the same bytes are there already (content-addressed key)
	}
	ix.Stats.Segments++
	ix.Stats.SegmentBytes += int64(len(body))
	return key, nil
}

func (ix *Indexer) readProgress(ctx context.Context, cluster, signal string) (Progress, string, error) {
	body, etag, err := ix.b.Get(ctx, ProgressKey(ix.cfg.Root, cluster, signal))
	if err != nil {
		return Progress{}, "", err
	}
	p := Progress{Lanes: map[string]map[string]uint64{}}
	if body == nil {
		return p, "", nil
	}
	if err := json.Unmarshal(body, &p); err != nil || p.Format != FormatVersion {
		// an unreadable hint: start from nothing (segments say what is indexed)
		return Progress{Lanes: map[string]map[string]uint64{}}, etag, nil
	}
	if p.Lanes == nil {
		p.Lanes = map[string]map[string]uint64{}
	}
	return p, etag, nil
}

// writeProgress CASes np over what is there, merging with a concurrent
// indexer's document (per-epoch max) on a conflict.
func (ix *Indexer) writeProgress(ctx context.Context, cluster, signal string, np, _ Progress, etag string) error {
	key := ProgressKey(ix.cfg.Root, cluster, signal)
	for attempt := 0; attempt < 4; attempt++ {
		body, _ := json.Marshal(np)
		ok, err := ix.b.PutIfMatch(ctx, key, body, etag)
		if err != nil {
			return err // unknown: the next pass re-reads
		}
		if ok {
			return nil
		}
		cur, e2, err := ix.readProgress(ctx, cluster, signal)
		if err != nil {
			return err
		}
		for p, eps := range cur.Lanes {
			if np.Lanes[p] == nil {
				np.Lanes[p] = map[string]uint64{}
			}
			for e, n := range eps {
				if n > np.Lanes[p][e] {
					np.Lanes[p][e] = n
				}
			}
		}
		np.IndexedThroughMs = max(np.IndexedThroughMs, cur.IndexedThroughMs)
		etag = e2
	}
	return errors.New("progress: CAS lost 4 times")
}

// mergeHours merges each finished hour's segments into one L1 when no
// single segment already covers them all.
func (ix *Indexer) mergeHours(ctx context.Context, cluster, signal, traceCol, textCol string) ([]string, error) {
	dirs, err := ix.b.Dirs(ctx, join(ix.cfg.Root, fmt.Sprintf("%s/%s/%s/", cluster, IndexDir, signal)))
	if err != nil {
		return nil, err
	}
	now := ix.Now()
	var out []string
	for _, bk := range dirs {
		start, ok := BucketStart(bk)
		if !ok {
			continue
		}
		end := start.Add(time.Hour)
		if now.Before(end.Add(time.Duration(ix.cfg.MergeAfterS)*time.Second)) || now.Sub(end) > time.Duration(ix.cfg.MergeLookbackH)*time.Hour {
			continue
		}
		segs, err := ix.segments(ctx, cluster, signal, bk)
		if err != nil {
			return out, err
		}
		if len(segs) < 2 {
			continue
		}
		cov := Covering(segs)
		best := 0
		for _, s := range segs {
			best = max(best, len(s.Header.Objects))
		}
		if best == len(cov) {
			continue // one segment already answers for the whole hour
		}
		key, err := ix.merge(ctx, cluster, signal, bk, traceCol, textCol, segs, 1)
		if err != nil {
			return out, err
		}
		out = append(out, key)
		ix.Stats.Merges++
	}
	return out, nil
}

// merge writes one segment covering the union of segs (each object once,
// from the segment Covering picks for it).
func (ix *Indexer) merge(ctx context.Context, cluster, signal, bucket, traceCol, textCol string, segs []SegRef, level int) (string, error) {
	cov := Covering(segs)
	bld := NewBuilder(ix.cfg.Build, traceCol, textCol)
	// objects in key order, each from its covering segment
	keys := sortedKeys(cov)
	decoded := map[string]*Segment{}
	remap := map[string]map[uint32]uint32{} // segment key → old global rg → new
	for _, k := range keys {
		c := cov[k]
		if decoded[c.Seg.Key] == nil {
			body, _, err := ix.b.Get(ctx, c.Seg.Key)
			if err != nil {
				return "", err
			}
			if body == nil {
				return "", fmt.Errorf("segment %s vanished", c.Seg.Key)
			}
			s, err := Decode(body)
			if err != nil {
				return "", fmt.Errorf("segment %s: %w", c.Seg.Key, err)
			}
			decoded[c.Seg.Key] = s
			remap[c.Seg.Key] = map[uint32]uint32{}
		}
		so := c.Seg.Header.Objects[c.Obj]
		nb := bld.AddObject(SegObject{Key: so.Key, Size: so.Size, ETag: so.ETag, LastModifiedMs: so.LastModifiedMs, RowGroups: so.RowGroups})
		for i := range so.RowGroups {
			remap[c.Seg.Key][so.Base+uint32(i)] = nb + uint32(i)
		}
	}
	for _, sk := range sortedKeys(decoded) {
		s, rm := decoded[sk], remap[sk]
		for _, e := range s.FPs {
			if n, ok := rm[e.rg]; ok {
				bld.AddFP(n, e.fp)
			}
		}
		if textCol == "" {
			continue
		}
		mapList := func(rgs []uint32) []uint32 {
			var out []uint32
			for _, r := range rgs {
				if n, ok := rm[r]; ok {
					out = append(out, n)
				}
			}
			sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
			return out
		}
		all := make([]uint32, 0, s.Header.RowGroups)
		for r := uint32(0); r < uint32(s.Header.RowGroups); r++ {
			all = append(all, r)
		}
		bld.addPosting("", mapList(s.Long))
		for t, rgs := range s.Terms {
			if rgs == nil { // frequent in that segment: every row group
				rgs = all
			}
			bld.addPosting(t, mapList(rgs))
		}
	}
	body, _, err := bld.Build(cluster, signal, bucket, level)
	if err != nil {
		return "", err
	}
	return ix.put(ctx, Key(ix.cfg.Root, cluster, signal, bucket, level, body), body)
}
