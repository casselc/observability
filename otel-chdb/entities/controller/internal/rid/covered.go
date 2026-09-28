package rid

import (
	"sort"
	"strings"
)

// The covered set (../../README.md §3.1): the resource attributes the catalog
// reproduces, i.e. what k8sattributes and resourcedetection put on a
// resource and this controller derives for it. Everything else a resource
// carries is the residual and is never hashed. The edges (otap-rs
// src/resource.rs, parquetgo resource.go) hold the same list; the shared
// vectors in ../../testdata/resource_id_vectors.json make the three agree.
var CoveredKeys = []string{
	"cloud.account.id",
	"cloud.availability.zone",
	"cloud.platform",
	"cloud.provider",
	"cloud.region",
	"container.image.name",
	"container.image.tag",
	"deployment.environment.name",
	"host.id",
	"host.name",
	"host.type",
	"k8s.cluster.name",
	"k8s.cluster.uid",
	"k8s.container.name",
	"k8s.cronjob.name",
	"k8s.daemonset.name",
	"k8s.deployment.name",
	"k8s.job.name",
	"k8s.namespace.name",
	"k8s.node.name",
	"k8s.node.uid",
	"k8s.pod.name",
	"k8s.pod.start_time",
	"k8s.pod.uid",
	"k8s.replicaset.name",
	"k8s.statefulset.name",
	"service.name",
}

// LabelPrefix: every k8s.pod.label.<l> is covered (the agent extracts only
// the configured labels, the same list as the controller's --labels).
const LabelPrefix = "k8s.pod.label."

var coveredSet = func() map[string]bool {
	m := make(map[string]bool, len(CoveredKeys))
	for _, k := range CoveredKeys {
		m[k] = true
	}
	return m
}()

// IsCoveredKey: whether a key belongs to the covered set (by name only).
func IsCoveredKey(k string) bool {
	if coveredSet[k] {
		return true
	}
	return len(k) > len(LabelPrefix) && strings.HasPrefix(k, LabelPrefix) && !strings.ContainsRune(k, 0)
}

// KV is one resource attribute as an edge sees it, in the resource's order.
// Str is false for a non-string value (int, bool, double, bytes, array,
// map): such a value is never covered, whatever its key.
type KV struct {
	Key   string
	Value string
	Str   bool
}

// Split returns the covered part of a resource's attributes, the map ID
// hashes. The rules, in the edges' order of evaluation:
//   - the FIRST occurrence of a key decides (pcommon Map.Get), duplicates
//     after it are ignored;
//   - a key is covered by name (IsCoveredKey), and only with a string value
//     that is non-empty (ID drops empty values anyway) and contains no NUL
//     (the hash's separator; a NUL-bearing value would make two maps hash
//     alike). Anything else stays in the residual.
func Split(attrs []KV) map[string]string {
	out := map[string]string{}
	seen := map[string]bool{}
	for _, a := range attrs {
		if seen[a.Key] {
			continue
		}
		seen[a.Key] = true
		if a.Str && a.Value != "" && !strings.ContainsRune(a.Value, 0) && IsCoveredKey(a.Key) {
			out[a.Key] = a.Value
		}
	}
	return out
}

// Covered reports whether every entry of attrs would be kept by Split: the
// controller's derived sets must hash to what the edges compute.
func Covered(attrs map[string]string) (bad []string) {
	for k, v := range attrs {
		if v != "" && (!IsCoveredKey(k) || strings.ContainsRune(v, 0)) {
			bad = append(bad, k)
		}
	}
	sort.Strings(bad)
	return bad
}
