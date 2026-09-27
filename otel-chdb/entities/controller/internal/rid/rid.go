// Package rid is the resource_id definition of ../../README.md §3.1, and the
// covered-attribute spec both sides of the agreement check read.
//
//	resource_id = xxh3_64("res.v1\0" ‖ for (k, v) in covered, v != "", sorted by k: k ‖ "\0" ‖ v ‖ "\0")
//
// The same bytes as the spike's ClickHouse expression (sql/resources.sql) and
// the edges' Rust / Go implementations: XXH3-64, seed 0.
package rid

import (
	"sort"
	"strings"

	"github.com/zeebo/xxh3"
)

// ID hashes a covered attribute set. Empty values are dropped, as in the SQL.
func ID(attrs map[string]string) uint64 {
	keys := make([]string, 0, len(attrs))
	n := len("res.v1\x00")
	for k, v := range attrs {
		if v == "" {
			continue
		}
		keys = append(keys, k)
		n += len(k) + len(v) + 2
	}
	sort.Strings(keys) // bytewise, like ClickHouse's arraySort on String
	var b strings.Builder
	b.Grow(n)
	b.WriteString("res.v1\x00")
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(attrs[k])
		b.WriteByte(0)
	}
	return xxh3.HashString(b.String())
}

// Key is a stable 64-bit key for an entity or a version: xxh3 over the parts
// joined by NUL (the catalog's *_key columns).
func Key(parts ...string) uint64 {
	return xxh3.HashString(strings.Join(parts, "\x00"))
}

// Merge flattens levels into one map (later levels win), like the SQL's mapConcat.
func Merge(levels ...map[string]string) map[string]string {
	n := 0
	for _, l := range levels {
		n += len(l)
	}
	out := make(map[string]string, n)
	for _, l := range levels {
		for k, v := range l {
			out[k] = v
		}
	}
	return out
}

// AttrsKey hashes a map deterministically (for version keys).
func AttrsKey(prefix string, attrs map[string]string) uint64 {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(prefix)
	b.WriteByte(0)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(attrs[k])
		b.WriteByte(0)
	}
	return xxh3.HashString(b.String())
}
