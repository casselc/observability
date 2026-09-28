package parquetgo

import (
	"slices"
	"sort"
	"strings"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// resource_id at the edge (../entities/README.md §3.1, §6.3), byte for byte
// the entity controller's rid.ID(rid.Split(...)) and otap-rs
// src/resource.rs; ../entities/testdata/resource_id_vectors.json holds the
// vectors the three are tested against.
//
//	resource_id = xxh3_64("res.v1\0" ‖ for (k, v) in covered, sorted by k bytewise: k ‖ "\0" ‖ v ‖ "\0")
//
// The covered set is the FIRST occurrence of each key, kept when the key is
// in CoveredKeys or is k8s.pod.label. plus a non-empty rest without NUL, and
// the value is a non-empty string without NUL. Everything else is residual.

// CoveredKeys (sorted): the controller's rid.CoveredKeys.
var CoveredKeys = []string{
	"cloud.account.id", "cloud.availability.zone", "cloud.platform", "cloud.provider", "cloud.region",
	"container.image.name", "container.image.tag", "deployment.environment.name",
	"host.id", "host.name", "host.type",
	"k8s.cluster.name", "k8s.cluster.uid", "k8s.container.name", "k8s.cronjob.name", "k8s.daemonset.name",
	"k8s.deployment.name", "k8s.job.name", "k8s.namespace.name", "k8s.node.name", "k8s.node.uid",
	"k8s.pod.name", "k8s.pod.start_time", "k8s.pod.uid", "k8s.replicaset.name", "k8s.statefulset.name",
	"service.name",
}

// LabelPrefix: every k8s.pod.label.<l> is covered.
const LabelPrefix = "k8s.pod.label."

const resDomain = "res.v1\x00"

// IsCoveredKey reports whether a key is covered, by name.
func IsCoveredKey(k string) bool {
	if len(k) > len(LabelPrefix) && strings.HasPrefix(k, LabelPrefix) {
		return !strings.ContainsRune(k, 0)
	}
	_, ok := slices.BinarySearch(CoveredKeys, k)
	return ok
}

// Covered is a resource's covered set, sorted by key, and its id.
type Covered struct {
	ID    uint64
	Pairs [][2]string
}

// ResourceKV is one attribute: Str is false for a non-string value.
type ResourceKV struct {
	Key, Value string
	Str        bool
}

// SplitCovered is the covered set of attributes given in their order.
func SplitCovered(attrs []ResourceKV) Covered {
	var seen []string
	var pairs [][2]string
	for _, a := range attrs {
		if !IsCoveredKey(a.Key) || slices.Contains(seen, a.Key) {
			continue
		}
		seen = append(seen, a.Key)
		if a.Str && a.Value != "" && !strings.ContainsRune(a.Value, 0) {
			pairs = append(pairs, [2]string{a.Key, a.Value})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	return Covered{ID: ResourceID(pairs), Pairs: pairs}
}

// CoveredOf is SplitCovered over a pdata resource's attributes.
func CoveredOf(m pcommon.Map) Covered {
	kvs := make([]ResourceKV, 0, m.Len())
	for k, v := range m.All() {
		if v.Type() == pcommon.ValueTypeStr {
			kvs = append(kvs, ResourceKV{Key: k, Value: v.Str(), Str: true})
		} else {
			kvs = append(kvs, ResourceKV{Key: k})
		}
	}
	return SplitCovered(kvs)
}

// ResourceID hashes a covered set sorted by key with no empty value.
func ResourceID(pairs [][2]string) uint64 {
	n := len(resDomain)
	for _, p := range pairs {
		n += len(p[0]) + len(p[1]) + 2
	}
	b := make([]byte, 0, n)
	b = append(b, resDomain...)
	for _, p := range pairs {
		b = append(b, p[0]...)
		b = append(b, 0)
		b = append(b, p[1]...)
		b = append(b, 0)
	}
	return xxh3.Hash(b)
}

// Resources are one object's distinct resources in walk order, each with
// the first row that uses it (the row an announcement rides on).
type Resources struct {
	Entries []ResourceEntry
	index   map[uint64]int
	rows    int
}

// ResourceEntry is one distinct, non-empty resource of an object.
type ResourceEntry struct {
	Covered
	FirstRow int
}

func (r *Resources) reset() {
	r.Entries = r.Entries[:0]
	clear(r.index)
	r.rows = 0
}

// resource registers a new resource and returns its id.
func (r *Resources) resource(c Covered) uint64 {
	if r.index == nil {
		r.index = map[uint64]int{}
	}
	if len(c.Pairs) > 0 {
		if _, ok := r.index[c.ID]; !ok {
			r.index[c.ID] = len(r.Entries)
			r.Entries = append(r.Entries, ResourceEntry{Covered: c, FirstRow: -1})
		}
	}
	return c.ID
}

// row notes the next row's resource.
func (r *Resources) row(id uint64) {
	if i, ok := r.index[id]; ok && r.Entries[i].FirstRow < 0 {
		r.Entries[i].FirstRow = r.rows
	}
	r.rows++
}

// AnnounceCache is one lane's announced resources in its current epoch: a
// resource is announced once per window per lane epoch, and counts as
// announced only once an object carrying it has committed (Announced). A
// commit in another epoch empties it; over its size the least recently
// announced eighth is forgotten (and announced again when next seen). The
// same policy as otap-rs resource.rs AnnounceCache.
type AnnounceCache struct {
	epoch   string
	m       map[uint64][2]uint64 // id -> window, last use
	tick    uint64
	Evicted uint64
}

// Wants reports whether an object in epoch, window w, must announce id.
func (c *AnnounceCache) Wants(epoch string, id uint64, w int64) bool {
	if c.epoch != epoch {
		return true
	}
	e, ok := c.m[id]
	return !ok || int64(e[0]) != w
}

// Announced marks ids announced in epoch and window w.
func (c *AnnounceCache) Announced(epoch string, ids []uint64, w int64, size int) {
	if c.epoch != epoch || c.m == nil {
		c.m = map[uint64][2]uint64{}
		c.epoch = epoch
	}
	for _, id := range ids {
		c.tick++
		c.m[id] = [2]uint64{uint64(w), c.tick}
	}
	size = max(size, 1)
	if len(c.m) > size {
		keep := size - size/8
		type u struct{ tick, id uint64 }
		v := make([]u, 0, len(c.m))
		for id, e := range c.m {
			v = append(v, u{e[1], id})
		}
		sort.Slice(v, func(i, j int) bool { return v[i].tick < v[j].tick || (v[i].tick == v[j].tick && v[i].id < v[j].id) })
		for _, x := range v[:len(v)-keep] {
			delete(c.m, x.id)
			c.Evicted++
		}
	}
}

// Len is the number of resources remembered.
func (c *AnnounceCache) Len() int { return len(c.m) }
