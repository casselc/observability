package bitemp

import "testing"

// The lane mapping on the cases entities/README.md §6.2 names. One pod
// (entity 1), versions 11 and 12; times in seconds for readability.
func TestFromLanes(t *testing.T) {
	const s = Time(1000)
	open := func(e, k uint64, vf, obs Time) LaneRecord {
		return LaneRecord{Level: "pod", Key: k, Entity: e, ValidFrom: vf * s, ObservedAt: obs * s}
	}
	closed := func(e, k uint64, vf, at Time) LaneRecord {
		return LaneRecord{Level: "pod", Key: k, Entity: e, ValidFrom: vf * s, ClosedAt: at * s, ObservedAt: at * s}
	}
	gap := func(from, to Time) LaneRecord {
		return LaneRecord{Level: "gap", Kind: "pods", ValidFrom: from * s, ClosedAt: to * s}
	}
	obj := func(lane string, put Time, recs ...LaneRecord) LaneObject {
		return LaneObject{Lane: lane, PutAt: put * s, Records: recs}
	}
	sync := func(lane string, at Time, recs ...LaneRecord) LaneObject {
		return LaneObject{Lane: lane, PutAt: (at + 1) * s, Sync: true, SyncAt: at * s, Records: recs}
	}
	p := Policy{TrustWindow: 1200 * s}
	type want struct {
		vt    Time
		state State
		ver   uint64
	}
	for _, c := range []struct {
		name string
		opt  MapOptions
		objs []LaneObject
		ent  uint64
		want []want
	}{
		{"relabel then delete", MapOptions{}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9)),
			obj("a", 110, closed(1, 11, 5, 100), open(1, 12, 100, 100)),
			obj("a", 210, closed(1, 12, 100, 200)),
		}, 1, []want{{4, Absent, 0}, {5, Asserted, 11}, {99, Asserted, 11}, {100, Asserted, 12}, {199, Asserted, 12}, {200, Retracted, 0}}},
		{"a pod opened and closed within one object ends retracted", MapOptions{}, []LaneObject{
			obj("a", 400, open(1, 11, 100, 101), closed(1, 11, 100, 300)),
		}, 1, []want{{99, Absent, 0}, {100, Asserted, 11}, {299, Asserted, 11}, {300, Retracted, 0}}},
		{"a relabel within one object", MapOptions{}, []LaneObject{
			obj("a", 400, open(1, 11, 100, 101), closed(1, 11, 100, 300), open(1, 12, 300, 300)),
		}, 1, []want{{299, Asserted, 11}, {300, Asserted, 12}}},
		{"sync closes what it lacks", MapOptions{}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9), open(2, 21, 5, 9)),
			sync("a", 300, open(2, 21, 5, 300)),
		}, 1, []want{{299, Asserted, 11}, {300, Retracted, 0}}},
		{"a sync does not retract entities born after it", MapOptions{}, []LaneObject{
			sync("a", 300, open(2, 21, 5, 300)),
			obj("a", 400, open(1, 11, 350, 351)),
		}, 1, []want{{349, Absent, 0}, {350, Asserted, 11}}},
		{"relist gap: unknown for what was alive; a deletion found by the relist", MapOptions{}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9)),
			obj("a", 130, closed(1, 11, 5, 121), gap(100, 120)),
		}, 1, []want{{99, Asserted, 11}, {100, Unknowable, 0}, {120, Unknowable, 0}, {121, Retracted, 0}}},
		{"relist gap: a pod not alive then stays retracted", MapOptions{}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9)),
			obj("a", 60, closed(1, 11, 5, 50)),
			obj("a", 130, gap(100, 120)),
		}, 1, []want{{110, Retracted, 0}}},
		{"restart, the fixed controller: its restart gap record comes after the first sync", MapOptions{}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9), open(2, 21, 5, 9)),
			obj("a", 110, closed(1, 11, 5, 100), open(1, 12, 100, 100)),
			obj("b", 500, open(1, 12, 110, 490)), // valid_from = the previous lane's end (ctrl.Config.Since)
			sync("b", 495, open(1, 12, 110, 495)),
			obj("b", 510, LaneRecord{Level: "gap", Kind: "restart", ValidFrom: 110 * s, ClosedAt: 496 * s}),
		}, 2, []want{{109, Asserted, 21}, {110, Unknowable, 0}, {496, Unknowable, 0}, {497, Retracted, 0}}},
		{"restart, today's records: the relabelled pod's new version claims its whole life", MapOptions{}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9)),
			obj("a", 110, closed(1, 11, 5, 100), open(1, 12, 100, 100)),
			obj("b", 500, open(1, 12, 5, 490)), // new incarnation: vf = the pod's creation
			sync("b", 495, open(1, 12, 5, 495)),
		}, 1, []want{{50, Asserted, 12}, {150, Asserted, 12}}},
		{"restart with RestartGaps: history kept, the outage unknown for a pod that died in it", MapOptions{RestartGaps: true}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9), open(2, 21, 5, 9)),
			obj("a", 110, closed(1, 11, 5, 100), open(1, 12, 100, 100)),
			obj("b", 500, open(1, 12, 5, 490)),
			sync("b", 495, open(1, 12, 5, 495)),
		}, 1, []want{{50, Asserted, 11}, {150, Asserted, 12}, {496, Asserted, 12}}},
		{"restart with RestartGaps: the pod that died in the outage", MapOptions{RestartGaps: true}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9), open(2, 21, 5, 9)),
			obj("a", 110, closed(1, 11, 5, 100), open(1, 12, 100, 100)),
			obj("b", 500, open(1, 12, 5, 490)),
			sync("b", 495, open(1, 12, 5, 495)),
		}, 2, []want{{50, Asserted, 21}, {109, Asserted, 21}, {110, Unknowable, 0}, {494, Unknowable, 0}, {495, Retracted, 0}}},
		{"restart with RestartGaps: relabelled while down counts from when it was seen", MapOptions{RestartGaps: true}, []LaneObject{
			obj("a", 10, open(1, 11, 5, 9)),
			obj("b", 500, open(1, 12, 5, 490)),
			sync("b", 495, open(1, 12, 5, 495)),
		}, 1, []want{{9, Asserted, 11}, {10, Unknowable, 0}, {489, Unknowable, 0}, {490, Asserted, 12}}},
	} {
		var seq uint64
		evs := FromLanes(c.objs, "pod", c.opt, &seq)
		for _, w := range c.want {
			r := Resolve(evs, c.ent, w.vt*s, Inf, p)
			if r.State != w.state || r.Version != w.ver {
				t.Errorf("%s: at %d s: %v %d, want %v %d\n%+v", c.name, w.vt, r.State, r.Version, w.state, w.ver, evs)
			}
		}
	}
}

// Announcements extend each other; an authority assert or retract wins over
// them; they fill an authority's unknown.
func TestFromAnnouncements(t *testing.T) {
	var seq uint64
	evs := FromAnnouncements([]Announcement{
		{Entity: 1, Version: 7, SeenAt: 100, IngestedAt: 110},
		{Entity: 1, Version: 7, SeenAt: 200, IngestedAt: 210},
	}, 0, &seq)
	p := Policy{}
	for _, c := range []struct {
		vt    Time
		state State
	}{{99, Absent}, {100, Asserted}, {150, Asserted}, {200, Asserted}, {201, Absent}} {
		if r := Resolve(evs, 1, c.vt, Inf, p); r.State != c.state || (r.State == Asserted && (r.Source != Announce || r.Uncertain)) {
			t.Errorf("at %d: %+v", c.vt, r)
		}
	}
	evs = append(evs, Event{Entity: 1, ValidFrom: 150, ValidTo: Inf, SystemFrom: 300, Seq: 90, Source: Controller, Kind: Retract},
		Event{Entity: 1, ValidFrom: 120, ValidTo: 160, SystemFrom: 300, Seq: 91, Source: Controller, Kind: Unknown})
	if r := Resolve(evs, 1, 130, Inf, p); r.State != Asserted || !r.Uncertain {
		t.Errorf("fills unknown: %+v", r)
	}
	if r := Resolve(evs, 1, 170, Inf, p); r.State != Retracted {
		t.Errorf("retract wins: %+v", r)
	}
}
