// Package lane holds the entity record (one versioned catalog row) and the
// S3 entity lane: create-only, sequenced, gzip NDJSON objects.
//
// Lane layout (one lane per controller incarnation):
//
//	{prefix}/{cluster}/{epochMs}-{instance}/{seq:012d}.delta.ndjson.gz
//	{prefix}/{cluster}/{epochMs}-{instance}/{seq:012d}.sync.{syncAtMs}.ndjson.gz
//
// A delta object carries the versions that opened or closed since the last
// flush; a sync object carries every version open at syncAt (the controller's
// whole live view), so the aggregator can close what the controller no longer
// sees (deletions missed while it was down). Records are idempotent and merge
// commutatively (min valid_from, the latest observation decides open/closed),
// so a lane may be replayed, and two controllers may write the same cluster.
package lane

import (
	"time"
)

// Levels of the catalog (../../sql/catalog.sql).
const (
	LCluster   = "cluster"
	LNode      = "node"
	LNamespace = "namespace"
	LWorkload  = "workload"
	LPod       = "pod"
	LResource  = "resource"
	// LGap is not a catalog level: a window [ValidFrom, ClosedAt] in which
	// the informer Kind names saw no events (a relist), so what changed in
	// it is uncertain (AMBIGUITY.md X1).
	LGap = "gap"
)

// Record is one version of one entity, as observed at ObservedAt.
// Key identifies the version (for a resource it is the resource_id),
// Entity the thing that has versions. ClosedAt zero means open.
type Record struct {
	Level      string            `json:"level"`
	Key        uint64            `json:"key"`
	Entity     uint64            `json:"entity"`
	ClusterKey uint64            `json:"cluster_key"`
	NodeKey    uint64            `json:"node_key,omitempty"`
	NsKey      uint64            `json:"ns_key,omitempty"`
	WlKey      uint64            `json:"wl_key,omitempty"`
	PodKey     uint64            `json:"pod_key,omitempty"`
	Kind       string            `json:"kind,omitempty"`
	Name       string            `json:"name,omitempty"`
	PodUID     string            `json:"pod_uid,omitempty"`
	Container  string            `json:"container,omitempty"`
	Attrs      map[string]string `json:"attrs"`
	ValidFrom  Time              `json:"valid_from"`
	ClosedAt   Time              `json:"closed_at"`
	ObservedAt Time              `json:"observed_at"`
	EventAt    Time              `json:"event_at"`
	Writer     string            `json:"writer"`
}

// Time is milliseconds since the epoch, written the way ClickHouse's
// DateTime64(3, 'UTC') parses it in JSONEachRow.
type Time int64

func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, 26)
	b = append(b, '"')
	b = time.UnixMilli(int64(t)).UTC().AppendFormat(b, "2006-01-02 15:04:05.000")
	return append(b, '"'), nil
}

func Now() Time { return Time(time.Now().UnixMilli()) }

func FromTime(t time.Time) Time {
	if t.IsZero() {
		return 0
	}
	return Time(t.UnixMilli())
}
