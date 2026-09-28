package bitemp

import "sort"

// The mapping from today's catalog inputs to events: the controller's lane
// objects (SCD2 open/close records, full-state syncs, gap records), and the
// edges' announcements. Every event's SystemFrom is when the aggregator took
// its object in (ingest_log.put_at); Seq is the order of mapping.

// LaneRecord is the part of a controller record (../controller/internal/lane)
// the mapping reads. Key is the version (a resource_id, a pod version key),
// Entity what has versions; ClosedAt 0 while open. A gap record has Level
// "gap", Kind the informer's resource ("pods") and [ValidFrom, ClosedAt] its
// window.
type LaneRecord struct {
	Level      string
	Key        uint64
	Entity     uint64
	Kind       string
	ValidFrom  Time
	ClosedAt   Time
	ObservedAt Time
}

// LaneObject is one object of one controller lane (an incarnation), in the
// order the aggregator ingested it.
type LaneObject struct {
	Lane    string // {cluster}/{epochMs}-{instance}: a new lane is a new incarnation
	PutAt   Time
	Sync    bool
	SyncAt  Time
	Records []LaneRecord
}

// GapLevels says which catalog levels an informer's gap record makes
// unknown.
var GapLevels = map[string][]string{
	"pods":       {"pod", "resource"},
	"nodes":      {"node"},
	"namespaces": {"namespace"},
	"restart":    {"cluster", "node", "namespace", "workload", "pod", "resource"}, // a controller restart (no informer watched)
}

// MapOptions: RestartGaps adds what today's catalog lacks
// (../README.md §6.2: "a controller restart is not yet a gap record"). When
// a cluster's lane changes (a new incarnation), every entity alive at the
// previous lane's last object is Unknown from then to the new lane's first
// sync; and the new incarnation's first open of an entity is dated honestly:
// a version asserted before keeps the valid_from it had, a version that
// changed while no controller watched counts from when it was observed. (The
// controller writes valid_from = the pod's creation for every pod it has
// not seen yet, which rewrites a relabelled pod's history on every restart:
// fleetreplay's finding, README.md here.)
type MapOptions struct {
	RestartGaps bool
}

// FromLanes maps the objects of one cluster, for one catalog level, to
// events. Within an object, a sync's retracts first, then the records in the
// order the controller wrote them (higher Seq wins at equal SystemFrom):
//
//   - a close of version K of entity E at t: Assert(E, [vf, t), K), then
//     Retract(E, [t, Inf)) (a relabel's open follows it and wins from its
//     valid_from; an earlier attempt skipped the retract whenever the object
//     also opened E, which left every pod opened and closed within one
//     object alive until the next sync: the replay's flush of 5 minutes
//     showed it, 2,074 false pod-hours);
//   - a gap record over [from, to]: Unknown(E, [from, to]) for every entity
//     E the events so far assert at `from` (a restart's gap record comes
//     after the first sync has retracted what vanished: what is alive before
//     the object would miss exactly those); it follows the closes the relist
//     revealed, so it hides their bounded assertions in the window. An
//     entity created inside the window is not made unknown before its
//     creation. Not Unknown(All):
//     that would make every entity ever deleted unknown in the window too
//     (a deleted uid never returns); entities born and gone inside the
//     window have no authority event at all, and the gap window itself
//     remains the record that the catalog may miss some;
//   - an open: Assert(E, [vf, Inf), K);
//   - a sync object at syncAt: Retract(E, [syncAt, Inf)) first for every
//     entity E alive before it and not in it (what the aggregator's sync
//     close does), then its records as opens. Not Retract(All, [syncAt,
//     Inf)): a snapshot says nothing about entities born after syncAt, and
//     that would hide the announcements of pods born and gone before the
//     next sync (the first replay did exactly that).
func FromLanes(objs []LaneObject, level string, opt MapOptions, seq *uint64) []Event {
	var out []Event
	byEnt := map[uint64][]int{} // entity -> its events in out
	add := func(e Event) {
		*seq++
		e.Seq = *seq
		byEnt[e.Entity] = append(byEnt[e.Entity], len(out))
		out = append(out, e)
	}
	// aliveAt: the entities the events so far assert at valid time t
	aliveAt := func(t Time) []uint64 {
		var es []uint64
		var evs []Event
		for e, idx := range byEnt {
			evs = evs[:0]
			for _, i := range idx {
				evs = append(evs, out[i])
			}
			if Resolve(evs, e, t, Inf, Policy{}).State == Asserted {
				es = append(es, e)
			}
		}
		sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
		return es
	}
	gapFor := func(kind string) bool {
		for _, l := range GapLevels[kind] {
			if l == level {
				return true
			}
		}
		return false
	}
	alive := map[uint64]bool{}         // entity -> asserted and not retracted, as mapped so far
	lastVersion := map[uint64]uint64{} // entity -> the version last asserted (any incarnation)
	versionVF := map[uint64]Time{}     // version -> the valid_from it was first asserted with
	type fix struct {
		key uint64
		vf  Time
	}
	seen := map[uint64]fix{} // entities opened by the current incarnation: the version and the valid_from it was given
	curLane, prevEnd := "", Time(0)
	restartFrom := Time(0) // > 0: this incarnation follows one whose last object was at restartFrom
	var aliveAtRestart []uint64
	sortedAlive := func() []uint64 {
		var es []uint64
		for e, a := range alive {
			if a {
				es = append(es, e)
			}
		}
		sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
		return es
	}
	for _, o := range objs {
		st := o.PutAt
		if o.Lane != curLane {
			if curLane != "" {
				restartFrom, aliveAtRestart = prevEnd, sortedAlive()
			}
			curLane, seen = o.Lane, map[uint64]fix{}
		}
		prevEnd = o.PutAt
		if opt.RestartGaps && restartFrom > 0 && aliveAtRestart != nil && o.Sync {
			for _, e := range aliveAtRestart {
				add(Event{Entity: e, ValidFrom: restartFrom, ValidTo: o.SyncAt, SystemFrom: st, Source: Controller, Kind: Unknown})
			}
			aliveAtRestart = nil
		}
		before := sortedAlive()
		opens := map[uint64]bool{}
		for _, r := range o.Records {
			if r.Level == level && r.ClosedAt == 0 {
				opens[r.Entity] = true
			}
		}
		if o.Sync { // what was alive and is not in the sync ended by syncAt (the aggregator's sync close)
			for _, e := range before {
				if !opens[e] {
					add(Event{Entity: e, ValidFrom: o.SyncAt, ValidTo: Inf, SystemFrom: st, Source: Controller, Kind: Retract})
					alive[e] = false
				}
			}
		}
		// the records in the order the controller wrote them: a relabel is a
		// close then an open (the open, later, wins from its valid_from); a
		// pod opened and closed within one object ends retracted
		for _, r := range o.Records {
			switch {
			case r.Level == "gap" && gapFor(r.Kind) && r.ValidFrom < r.ClosedAt:
				for _, e := range aliveAt(r.ValidFrom) {
					add(Event{Entity: e, ValidFrom: r.ValidFrom, ValidTo: r.ClosedAt + 1, SystemFrom: st, Source: Controller, Kind: Unknown})
				}
			case r.Level != level:
			case r.ClosedAt != 0:
				vf := r.ValidFrom
				if f, ok := seen[r.Entity]; ok && f.key == r.Key {
					vf = f.vf // as the open was dated
				}
				if vf < r.ClosedAt {
					add(Event{Entity: r.Entity, ValidFrom: vf, ValidTo: r.ClosedAt, SystemFrom: st, Source: Controller, Kind: Assert, Version: r.Key})
				}
				add(Event{Entity: r.Entity, ValidFrom: r.ClosedAt, ValidTo: Inf, SystemFrom: st, Source: Controller, Kind: Retract})
				alive[r.Entity] = false
			default:
				vf := r.ValidFrom
				if f, ok := seen[r.Entity]; ok && f.key == r.Key {
					vf = f.vf // the same version again (a sync): as first given
				} else if opt.RestartGaps && restartFrom > 0 && !ok && vf < restartFrom {
					if v, known := versionVF[r.Key]; known {
						vf = v // asserted before: continuity
					} else if _, known := lastVersion[r.Entity]; known {
						vf = r.ObservedAt // changed while no controller watched: from when it was seen
					}
				}
				seen[r.Entity] = fix{r.Key, vf}
				lastVersion[r.Entity] = r.Key
				if _, ok := versionVF[r.Key]; !ok {
					versionVF[r.Key] = vf
				}
				alive[r.Entity] = true
				add(Event{Entity: r.Entity, ValidFrom: vf, ValidTo: Inf, SystemFrom: st, Source: Controller, Kind: Assert, Version: r.Key})
			}
		}
	}
	return out
}

// Announcement is one announcement of a resource by an edge, as the
// consumer's otel_resources holds it: seen at SeenAt (the object's
// received_at), ingested at IngestedAt.
type Announcement struct {
	Entity     uint64 // the resource's entity (pod uid, container), from its covered set
	Version    uint64 // its resource_id
	SeenAt     Time
	IngestedAt Time
}

// FromAnnouncements maps announcements to evidence events: each asserts the
// resource from its first sighting to this one (+ tail), so a later
// announcement extends an earlier one. tail is how long after its last
// sighting the resource is taken to live (0: only what was seen; the edges
// re-announce once per window, so window is an upper bound).
func FromAnnouncements(anns []Announcement, tail Time, seq *uint64) []Event {
	sorted := append([]Announcement(nil), anns...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].IngestedAt < sorted[j].IngestedAt })
	first := map[uint64]Time{}
	var out []Event
	for _, a := range sorted {
		f, ok := first[a.Version]
		if !ok || a.SeenAt < f {
			f = a.SeenAt
			first[a.Version] = f
		}
		*seq++
		out = append(out, Event{Entity: a.Entity, ValidFrom: f, ValidTo: max(a.SeenAt, f) + 1 + tail, SystemFrom: a.IngestedAt, Seq: *seq,
			Source: Announce, Kind: Assert, Version: a.Version})
	}
	return out
}
