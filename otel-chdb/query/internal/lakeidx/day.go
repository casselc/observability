package lakeidx

// The per-day trace-id level (DECISIONS.md D27, amendment 2026-10-01;
// FORMAT.md §7.6). A trace lookup over hours reads, per hour, a LIST, the
// segments' headers and one block: 720 of each for 30 days. Once a day has
// ended (and its hours have merged), the indexer writes, per (cluster,
// signal, day):
//
//	{root}/{cluster}/_index/v1/{signal}/{YYYYMMDD}/S{pp}-{h}.osix   2^bits shards
//	{root}/{cluster}/_index/v1/{signal}/{YYYYMMDD}/M-{h}.osix       the manifest
//
// Each shard is a segment (level 2) holding only the trace fingerprints
// whose top `bits` bits are its prefix, with row-group ordinals across the
// whole day; it lists no objects. The manifest (level 2) lists the day's
// covered objects (those ordinals) and every shard by prefix. Shards are
// written first, the manifest last: a manifest names shards that exist.
// Everything is content-addressed and create-only, like the hourly
// segments: a rebuild of the same day meets the same keys (412 = done).
//
// A trace lookup for a day: LIST the day, read the best manifest's header
// (most objects; cached), the one shard's header (cached) for the id's
// fingerprint prefix, and one block. Objects the manifest does not cover
// (stragglers written after it, a size or ETag mismatch) go through the
// hourly path, and anything the day level cannot read falls back to it too:
// the day level only ever removes hourly reads, never coverage.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// DayLevel is the level of per-day manifests and shards.
const DayLevel = 2

// MaxDayBits bounds the shard count (2^bits).
const MaxDayBits = 8

// DayInfo is a manifest's account of its shards.
type DayInfo struct {
	Bits   int        `json:"bits"`
	Shards []DayShard `json:"shards"`
}

// DayShard names one shard of a manifest.
type DayShard struct {
	Prefix uint32 `json:"prefix"`
	Key    string `json:"key"`
	Size   int64  `json:"size"`
}

// ShardInfo is a shard's own header field.
type ShardInfo struct {
	Bits   int    `json:"bits"`
	Prefix uint32 `json:"prefix"`
}

// ShardOf is the shard a fingerprint belongs to: its top bits bits.
func ShardOf(fp uint32, bits int) uint32 {
	if bits <= 0 {
		return 0
	}
	return fp >> (32 - bits)
}

func (h *Header) checkShard() error {
	s := h.Shard
	if len(h.Objects) != 0 || h.Terms != nil || h.Trace == nil || h.Day != nil || h.Level != DayLevel {
		return corrupt("shard with objects, terms, a day or no trace section")
	}
	if s.Bits < 0 || s.Bits > MaxDayBits || (s.Bits < 32 && s.Prefix >= 1<<s.Bits) {
		return corrupt("shard prefix %d of %d bits", s.Prefix, s.Bits)
	}
	for i, b := range h.Trace.Blocks {
		if ShardOf(b.First, s.Bits) != s.Prefix {
			return corrupt("trace block %d outside the shard", i)
		}
	}
	return nil
}

func (h *Header) checkDay() error {
	d := h.Day
	if h.Shard != nil || h.Trace != nil || h.Terms != nil || h.Level != DayLevel {
		return corrupt("a day manifest with sections")
	}
	if d.Bits < 0 || d.Bits > MaxDayBits || len(d.Shards) != 1<<d.Bits {
		return corrupt("day of %d bits with %d shards", d.Bits, len(d.Shards))
	}
	for i, s := range d.Shards {
		if s.Prefix != uint32(i) || s.Key == "" || s.Size <= 0 {
			return corrupt("day shard %d", i)
		}
	}
	return nil
}

// DayOf is the day an hour bucket (or a time) falls in.
func DayOf(t time.Time) string { return t.UTC().Format("20060102") }

// DayStart parses a day name.
func DayStart(d string) (time.Time, bool) {
	t, err := time.Parse("20060102", d)
	return t, err == nil
}

// DayPrefix is the prefix of one day's manifests and shards.
func DayPrefix(root, cluster, signal, day string) string {
	return BucketPrefix(root, cluster, signal, day)
}

// DayKey is a manifest's ("M") or shard's ("S{pp}") content-addressed key.
func DayKey(root, cluster, signal, day, kind string, body []byte) string {
	sum := sha256.Sum256(body)
	return join(root, fmt.Sprintf("%s/%s/%s/%s/%s-%s.osix", cluster, IndexDir, signal, day, kind, hex.EncodeToString(sum[:16])))
}

// dayBits picks the shard count: the smallest bits >= min whose shards
// hold at most target bytes (an entry costs about 3 bytes), at most
// MaxDayBits.
func dayBits(entries int, minBits int, target int64) int {
	bits := max(0, min(minBits, MaxDayBits))
	for bits < MaxDayBits && int64(entries)*3>>bits > target {
		bits++
	}
	return bits
}

// buildDays writes the day level of every finished day within the
// lookback whose hours' covered objects no manifest already covers.
func (ix *Indexer) buildDays(ctx context.Context, cluster, signal, traceCol string) ([]string, error) {
	if ix.cfg.NoDays || traceCol == "" {
		return nil, nil
	}
	dirs, err := ix.b.Dirs(ctx, join(ix.cfg.Root, fmt.Sprintf("%s/%s/%s/", cluster, IndexDir, signal)))
	if err != nil {
		return nil, err
	}
	hours := map[string][]string{}
	for _, bk := range dirs {
		if t, ok := BucketStart(bk); ok {
			d := DayOf(t)
			hours[d] = append(hours[d], bk)
		}
	}
	now := ix.Now()
	var out []string
	for _, d := range sortedKeys(hours) {
		start, _ := DayStart(d)
		end := start.Add(24 * time.Hour)
		if now.Before(end.Add(time.Duration(ix.cfg.MergeAfterS)*time.Second)) || now.Sub(end) > time.Duration(ix.cfg.DayLookbackD)*24*time.Hour {
			continue
		}
		key, err := ix.buildDay(ctx, cluster, signal, traceCol, d, hours[d])
		if err != nil {
			return out, err
		}
		if key != "" {
			out = append(out, key)
		}
	}
	return out, nil
}

// buildDay writes one day's shards and manifest, unless a manifest already
// covers every object the day's hourly segments cover.
func (ix *Indexer) buildDay(ctx context.Context, cluster, signal, traceCol, day string, buckets []string) (string, error) {
	type src struct {
		seg *SegRef
		obj int
	}
	objs := map[string]src{}
	for _, bk := range buckets {
		segs, err := ix.segments(ctx, cluster, signal, bk)
		if err != nil {
			return "", err
		}
		for k, c := range Covering(segs) {
			objs[k] = src{c.Seg, c.Obj}
		}
	}
	if len(objs) == 0 {
		return "", nil
	}
	mans, err := ix.dayManifests(ctx, cluster, signal, day)
	if err != nil {
		return "", err
	}
	for _, m := range mans {
		have := map[string]SegObject{}
		for _, o := range m.Header.Objects {
			have[o.Key] = o
		}
		all := true
		for k, s := range objs {
			so := s.seg.Header.Objects[s.obj]
			if h, ok := have[k]; !ok || h.Size != so.Size || h.ETag != so.ETag {
				all = false
				break
			}
		}
		if all && ix.shardsOK(ctx, cluster, signal, day, m) {
			return "", nil // the day is answered by a manifest already
		}
	}
	// the objects in key order, each from its covering hourly segment
	keys := sortedKeys(objs)
	man := NewBuilder(ix.cfg.Build, "", "")
	decoded := map[string]*Segment{}
	remap := map[string]map[uint32]uint32{}
	for _, k := range keys {
		s := objs[k]
		if decoded[s.seg.Key] == nil {
			body, _, err := ix.b.Get(ctx, s.seg.Key)
			if err != nil {
				return "", err
			}
			if body == nil {
				return "", fmt.Errorf("segment %s vanished", s.seg.Key)
			}
			seg, err := Decode(body)
			if err != nil {
				return "", fmt.Errorf("segment %s: %w", s.seg.Key, err)
			}
			decoded[s.seg.Key], remap[s.seg.Key] = seg, map[uint32]uint32{}
		}
		so := s.seg.Header.Objects[s.obj]
		nb := man.AddObject(SegObject{Key: so.Key, Size: so.Size, ETag: so.ETag, LastModifiedMs: so.LastModifiedMs, RowGroups: so.RowGroups})
		for i := range so.RowGroups {
			remap[s.seg.Key][so.Base+uint32(i)] = nb + uint32(i)
		}
	}
	var fps []fpEntry
	for _, sk := range sortedKeys(decoded) {
		rm := remap[sk]
		for _, e := range decoded[sk].FPs {
			if n, ok := rm[e.rg]; ok {
				fps = append(fps, fpEntry{e.fp, n})
			}
		}
	}
	srcHash := SourcesHash(man.Objects())
	bits := dayBits(len(fps), *ix.cfg.DayMinBits, ix.cfg.DayShardBytes)
	byShard := make([][]fpEntry, 1<<bits)
	for _, e := range fps {
		p := ShardOf(e.fp, bits)
		byShard[p] = append(byShard[p], e)
	}
	info := &DayInfo{Bits: bits}
	for p := range byShard {
		sb := NewBuilder(ix.cfg.Build, traceCol, "")
		sb.shard, sb.shardSources, sb.shardRGs = &ShardInfo{Bits: bits, Prefix: uint32(p)}, srcHash, man.rgs
		for _, e := range byShard[p] {
			sb.AddFP(e.rg, e.fp)
		}
		body, _, err := sb.Build(cluster, signal, day, DayLevel)
		if err != nil {
			return "", err
		}
		key, err := ix.put(ctx, DayKey(ix.cfg.Root, cluster, signal, day, fmt.Sprintf("S%02x", p), body), body)
		if err != nil {
			return "", err // the manifest is not written: these shards are not used
		}
		info.Shards = append(info.Shards, DayShard{Prefix: uint32(p), Key: key, Size: int64(len(body))})
	}
	man.day = info
	body, _, err := man.Build(cluster, signal, day, DayLevel)
	if err != nil {
		return "", err
	}
	key, err := ix.put(ctx, DayKey(ix.cfg.Root, cluster, signal, day, "M", body), body)
	if err == nil {
		ix.Stats.Days++
	}
	return key, err
}

// shardsOK: every shard of manifest m reads and is m's. A shard that does
// not verify is marked bad, so the rebuild writes it beside
// (`…-r{n}.osix`); a missing one is written again under its own key.
func (ix *Indexer) shardsOK(ctx context.Context, cluster, signal, day string, m SegRef) bool {
	for _, sd := range m.Header.Day.Shards {
		h := ix.hdrs.Get(sd.Key, sd.Size)
		if h == nil {
			var err error
			h, _, _, err = ReadHeader(ctx, ix.b, sd.Key, sd.Size)
			if err != nil {
				if errors.Is(err, ErrCorrupt) {
					ix.bad[sd.Key] = true
				}
				ix.Stats.Errors++
				return false
			}
			ix.hdrs.Put(sd.Key, sd.Size, h)
		}
		if h.Shard == nil || h.Shard.Prefix != sd.Prefix || h.Shard.Bits != m.Header.Day.Bits || h.SourcesHash != m.Header.SourcesHash ||
			h.Cluster != cluster || h.Signal != signal || h.Bucket != day {
			return false
		}
	}
	return true
}

// dayManifests lists one day's manifests whose headers verify, best first.
func (ix *Indexer) dayManifests(ctx context.Context, cluster, signal, day string) ([]SegRef, error) {
	objs, _, err := ix.b.List(ctx, DayPrefix(ix.cfg.Root, cluster, signal, day), ix.cfg.ListMax)
	if err != nil {
		return nil, err
	}
	var out []SegRef
	for _, o := range objs {
		if !isManifest(o.Key) {
			continue
		}
		h := ix.hdrs.Get(o.Key, o.Size)
		if h == nil {
			h, _, _, err = ReadHeader(ctx, ix.b, o.Key, o.Size)
			if err != nil {
				ix.Stats.Errors++
				continue // unreadable: the day is built again beside it
			}
			ix.hdrs.Put(o.Key, o.Size, h)
		}
		if h.Day == nil || h.Cluster != cluster || h.Signal != signal || h.Bucket != day {
			continue
		}
		out = append(out, SegRef{Key: o.Key, Size: o.Size, Header: h})
	}
	sortManifests(out)
	return out, nil
}

func isManifest(key string) bool {
	i := strings.LastIndexByte(key, '/')
	return strings.HasPrefix(key[i+1:], "M-") && strings.HasSuffix(key, ".osix")
}

// sortManifests: most objects first, then key.
func sortManifests(ms []SegRef) {
	sort.SliceStable(ms, func(i, j int) bool {
		if len(ms[i].Header.Objects) != len(ms[j].Header.Objects) {
			return len(ms[i].Header.Objects) > len(ms[j].Header.Objects)
		}
		return ms[i].Key < ms[j].Key
	})
}

// Deliberate bugs of the day path (tests only; Resolver.mut).
const (
	mutDayLowBits  = 1 // the shard from the fingerprint's low bits
	mutDayNoMatch  = 2 // a manifest covers an object by key alone (no size/ETag check)
	mutDayAnyShard = 3 // a shard is used without checking it is its manifest's
)

// resolveDays answers a trace-only lookup from the day level for the
// objects a day manifest covers; it returns those objects' results (the
// rest go through the hourly path) and how many shards it read.
func (r *Resolver) resolveDays(ctx context.Context, cluster, signal string, objs []store.Object, traceID string, bud *budget,
	fail func(string, ...any)) (map[string]ObjResult, int) {
	out := map[string]ObjResult{}
	byDay := map[string][]store.Object{}
	for _, o := range objs {
		byDay[DayOf(o.LastModified)] = append(byDay[DayOf(o.LastModified)], o)
	}
	fp := FP(traceID)
	shards := 0
	for _, d := range sortedKeys(byDay) {
		mans, err := r.dayManifests(ctx, cluster, signal, d, bud, fail)
		if err != nil {
			fail("%s: %v", d, err)
			continue
		}
		// the best manifest whose shard for this id reads and is its own
		for mi := range mans {
			man := &mans[mi]
			h := man.Header
			idx := map[string]int{}
			for i, o := range h.Objects {
				idx[o.Key] = i
			}
			var mine []store.Object
			for _, o := range byDay[d] {
				i, ok := idx[o.Key]
				if !ok {
					continue
				}
				if c := (CoverRef{Seg: man, Obj: i}); r.mut != mutDayNoMatch && !c.Matches(o) {
					continue
				}
				mine = append(mine, o)
			}
			if len(mine) == 0 {
				continue
			}
			p := ShardOf(fp, h.Day.Bits)
			if r.mut == mutDayLowBits && h.Day.Bits > 0 {
				p = fp & (1<<h.Day.Bits - 1)
			}
			sd := h.Day.Shards[p]
			sh, err := r.header(ctx, sd.Key, sd.Size, bud)
			if err != nil {
				fail("%s: %v", sd.Key, err)
				continue
			}
			if r.mut != mutDayAnyShard && (sh.Shard == nil || sh.Shard.Bits != h.Day.Bits || sh.Shard.Prefix != p || sh.SourcesHash != h.SourcesHash ||
				sh.RowGroups != h.RowGroups || sh.Cluster != cluster || sh.Signal != signal || sh.Bucket != d) {
				fail("%s: not a shard of %s", sd.Key, man.Key)
				continue
			}
			rgs, err := r.evalSegment(ctx, &SegRef{Key: sd.Key, Size: sd.Size, Header: sh}, traceID, nil, bud)
			if err != nil {
				fail("%s: %v", sd.Key, err)
				continue
			}
			shards++
			for _, o := range mine {
				so := h.Objects[idx[o.Key]]
				var hit []int
				for _, g := range rgs {
					if g < uint32(h.RowGroups) && g >= so.Base && g < so.Base+uint32(len(so.RowGroups)) {
						hit = append(hit, int(g-so.Base))
					}
				}
				if len(hit) == 0 {
					out[o.Key] = ObjResult{Status: None}
				} else {
					out[o.Key] = ObjResult{Status: Hit, RowGroups: hit}
				}
			}
			break
		}
	}
	return out, shards
}

// dayManifests are a day's verified manifests, best first.
func (r *Resolver) dayManifests(ctx context.Context, cluster, signal, day string, bud *budget, fail func(string, ...any)) ([]SegRef, error) {
	objs, _, err := r.st.List(ctx, DayPrefix(r.cfg.Root, cluster, signal, day), 100_000)
	if err != nil {
		return nil, err
	}
	bud.mu.Lock()
	bud.requests++
	bud.mu.Unlock()
	var ms []SegRef
	for _, o := range objs {
		if !isManifest(o.Key) {
			continue
		}
		h, err := r.header(ctx, o.Key, o.Size, bud)
		if err != nil {
			fail("%s: %v", o.Key, err)
			continue
		}
		if h.Day == nil || h.Cluster != cluster || h.Signal != signal || h.Bucket != day {
			fail("%s: not a manifest of %s/%s/%s", o.Key, cluster, signal, day)
			continue
		}
		ms = append(ms, SegRef{Key: o.Key, Size: o.Size, Header: h})
	}
	sortManifests(ms)
	return ms, nil
}

// header reads (or takes from the cache) a segment's header within budget.
func (r *Resolver) header(ctx context.Context, key string, size int64, bud *budget) (*Header, error) {
	if h := r.hdrs.Get(key, size); h != nil {
		bud.hit()
		return h, nil
	}
	if !bud.take(min(size, TailFetch)) {
		return nil, fmt.Errorf("index byte budget spent")
	}
	h, n, nb, err := ReadHeader(ctx, r.st, key, size)
	if n > 1 {
		bud.take(nb - min(size, TailFetch))
	}
	if err != nil {
		return nil, err
	}
	r.hdrs.Put(key, size, h)
	return h, nil
}
