// fleetreplay replays a slice of the synthetic fleet (../../../scripts/fleet.py,
// its pods table joined with the workloads' containers, as TSV) through
//
//  1. a simulated per-cluster controller with faults (restarts, informer
//     relist gaps), written as the controller's lane objects (open/close
//     records, full-state syncs, gap records), and edges announcing every
//     resource they see;
//  2. today's aggregator: its SQL (../../../controller/sql/aggregator.sql,
//     announced.sql, the per-object statements of cmd/aggregator) in
//     ClickHouse, read back as versions_final, pods and resources;
//  3. the mapping of the same inputs to events (bitemp.FromLanes,
//     FromAnnouncements) and the reference resolver;
//
// and compares both with the truth over valid time, in pod-hours per
// category, with the current view's size and the resolver's cost.
//
//	clickhouse client -q "SELECT p.pod_key, p.pod_uid, toUnixTimestamp64Milli(p.valid_from), toUnixTimestamp64Milli(p.valid_to),
//	  toUnixTimestamp64Milli(p.pod_start), toUnixTimestamp64Milli(p.pod_end), arrayStringConcat(arrayMap(c -> c.1, w.containers), ',')
//	  FROM db.pods p LEFT JOIN db.workloads w ON p.wl_key = w.wl_key ORDER BY p.valid_from FORMAT TSV" > pods.tsv
//	go run ./cmd/fleetreplay -pods pods.tsv -ch http://127.0.0.1:18123 -db btc_agg -json out.json
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	bt "github.com/casselc/observability/otel-chdb/entities/bitemp"
)

type Time = bt.Time

const (
	sec  = Time(1000)
	min_ = 60 * sec
	hour = 60 * min_
	open = Time(4102444800000) // fleet.py's OPEN (2100-01-01)
)

var (
	podsPath = flag.String("pods", "", "fleet pods TSV (see the package comment)")
	chURL    = flag.String("ch", "http://127.0.0.1:18123", "ClickHouse HTTP")
	db       = flag.String("db", "btc_agg", "catalog database to create (dropped first)")
	annDB    = flag.String("anndb", "btc_ann", "announcements database to create (dropped first)")
	sqlDir   = flag.String("sql", "../controller/sql", "the aggregator's SQL")
	resSQL   = flag.String("ressql", "../../otap-rs/sql/otel_resources.sql", "the consumer's announcements DDL")
	outages  = flag.String("outages", "1d2h:20m,3d9h:45m,5d14h:2h", "controller outages: offset from the start:duration,...")
	relists  = flag.String("gaps", "0d18h:3m,2d4h:1m,4d1h:5m,6d7h:2m", "informer relist gaps (pods): offset:duration,...")
	flushF   = flag.Duration("flush", 30*time.Second, "delta flush interval")
	resync   = flag.Duration("resync", time.Hour, "full-state sync interval")
	window   = flag.Duration("window", time.Hour, "edges re-announce a live resource once per window")
	wTrust   = flag.Duration("trust", 2*time.Hour, "trust window (2 x resync)")
	jsonOut  = flag.String("json", "", "write the results here")
	skipCH   = flag.Bool("skip-ch", false, "skip ClickHouse (resolver only)")
	fixed    = flag.Bool("fixed", false, "simulate the controller with the restart fix (ctrl.Config.Since and the restart gap record)")
)

func h64(parts ...string) uint64 {
	f := fnv.New64a()
	for _, p := range parts {
		f.Write([]byte(p))
		f.Write([]byte{0})
	}
	return f.Sum64()
}

// ---- the truth ------------------------------------------------------------

type podVer struct {
	key        uint64
	uid        string
	vf, vt     Time
	ps         Time
	containers []string
}

type pod struct {
	uid  string
	ent  uint64
	vers []*podVer // by vf
}

func podEnt(uid string) uint64             { return h64("pod", uid) }
func resEnt(uid, c string) uint64          { return h64("res", uid, c) }
func resID(podKey uint64, c string) uint64 { return h64("rid", strconv.FormatUint(podKey, 10), c) }

func readPods(path string) (map[string]*pod, Time, Time) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	pods := map[string]*pod{}
	seen := map[uint64]bool{}
	t0, tEnd := Time(1<<62), Time(0)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		c := strings.Split(sc.Text(), "\t")
		k, _ := strconv.ParseUint(c[0], 10, 64)
		if seen[k] { // the generator's duplicate pod versions (entities/README.md §3.6)
			continue
		}
		seen[k] = true
		n := func(i int) Time { v, _ := strconv.ParseInt(c[i], 10, 64); return Time(v) }
		v := &podVer{key: k, uid: c[1], vf: n(2), vt: n(3), ps: n(4), containers: strings.Split(c[6], ",")}
		if v.vt >= open {
			v.vt = bt.Inf
		}
		p := pods[v.uid]
		if p == nil {
			p = &pod{uid: v.uid, ent: podEnt(v.uid)}
			pods[v.uid] = p
		}
		p.vers = append(p.vers, v)
		t0 = min(t0, v.vf)
		if v.vf > tEnd {
			tEnd = v.vf
		}
	}
	for _, p := range pods {
		sort.Slice(p.vers, func(i, j int) bool { return p.vers[i].vf < p.vers[j].vf })
	}
	// the fleet's end: the day after the last version starts, rounded
	tEnd = (tEnd/(24*hour) + 1) * 24 * hour
	return pods, t0, tEnd
}

func (p *pod) at(t Time) *podVer {
	for _, v := range p.vers {
		if v.vf <= t && t < v.vt {
			return v
		}
	}
	return nil
}

// ---- the simulated controller ------------------------------------------------

type rec struct {
	Level      string            `json:"level"`
	Key        uint64            `json:"key"`
	Entity     uint64            `json:"entity"`
	ClusterKey uint64            `json:"cluster_key"`
	PodKey     uint64            `json:"pod_key,omitempty"`
	Kind       string            `json:"kind,omitempty"`
	Name       string            `json:"name,omitempty"`
	PodUID     string            `json:"pod_uid,omitempty"`
	Container  string            `json:"container,omitempty"`
	Attrs      map[string]string `json:"attrs"`
	ValidFrom  jt                `json:"valid_from"`
	ClosedAt   jt                `json:"closed_at"`
	ObservedAt jt                `json:"observed_at"`
	EventAt    jt                `json:"event_at"`
	Writer     string            `json:"writer"`
}

// jt marshals like lane.Time.
type jt Time

func (t jt) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.UnixMilli(int64(t)).UTC().Format("2006-01-02 15:04:05.000") + `"`), nil
}

type object struct {
	key    string
	lane   string
	putAt  Time
	sync   bool
	syncAt Time
	recs   []rec
}

const clusterUID = "btc-cluster-0"

var clusterKey = h64("cluster", clusterUID)

type ctrlState struct {
	key uint64
	vf  Time
	ver *podVer
}

type window_ struct{ from, to Time }

func parseWindows(s string, t0 Time) []window_ {
	var out []window_
	for _, w := range strings.Split(s, ",") {
		if w == "" {
			continue
		}
		p := strings.SplitN(w, ":", 2)
		off := p[0]
		d := Time(0)
		if i := strings.Index(off, "d"); i >= 0 {
			n, _ := strconv.Atoi(off[:i])
			d = Time(n) * 24 * hour
			off = off[i+1:]
		}
		od, err := time.ParseDuration(off)
		if err != nil {
			log.Fatalf("window %q: %v", w, err)
		}
		dur, err := time.ParseDuration(p[1])
		if err != nil {
			log.Fatalf("window %q: %v", w, err)
		}
		from := t0 + d + Time(od.Milliseconds())
		out = append(out, window_{from, from + Time(dur.Milliseconds())})
	}
	return out
}

func in(ws []window_, t Time) (window_, bool) {
	for _, w := range ws {
		if w.from <= t && t < w.to {
			return w, true
		}
	}
	return window_{}, false
}

type change struct {
	t    Time
	p    *pod
	ver  *podVer // nil: the pod ends
	prev *podVer
}

func podRecs(p *pod, v *podVer, vf, closed, obs Time, writer string) []rec {
	out := []rec{{Level: "pod", Key: v.key, Entity: p.ent, ClusterKey: clusterKey, PodUID: p.uid, Attrs: map[string]string{},
		ValidFrom: jt(vf), ClosedAt: jt(closed), ObservedAt: jt(obs), EventAt: jt(obs), Writer: writer}}
	for _, c := range v.containers {
		out = append(out, rec{Level: "resource", Key: resID(v.key, c), Entity: resEnt(p.uid, c), ClusterKey: clusterKey, PodKey: v.key,
			PodUID: p.uid, Container: c, Attrs: map[string]string{}, ValidFrom: jt(vf), ClosedAt: jt(closed), ObservedAt: jt(obs), EventAt: jt(obs), Writer: writer})
	}
	return out
}

// simulate runs the controller over [t0, tEnd) and returns its objects in
// the aggregator's ingest order.
func simulate(pods map[string]*pod, t0, tEnd Time, outs, gaps []window_, rnd *rand.Rand) []object {
	var chs []change
	for _, p := range pods {
		for i, v := range p.vers {
			var prev *podVer
			if i > 0 {
				prev = p.vers[i-1]
				if prev.vt != v.vf { // a gap between versions: the pod ends and another starts (not in fleet.py)
					prev = nil
				}
			}
			chs = append(chs, change{t: v.vf, p: p, ver: v, prev: prev})
			if v.vt < tEnd && (i == len(p.vers)-1 || p.vers[i+1].vf != v.vt) {
				chs = append(chs, change{t: v.vt, p: p, prev: v})
			}
		}
	}
	sort.Slice(chs, func(i, j int) bool { return chs[i].t < chs[j].t })
	F, R := Time(flushF.Milliseconds()), Time(resync.Milliseconds())
	lag := func() Time { return sec + Time(rnd.Intn(2000)) }
	ingest := func() Time { return 5*sec + Time(rnd.Intn(5000)) }

	var objs []object
	var since, lastPut Time // the previous incarnation's last object (the fix); the last object's PUT
	restartGapDue := false
	state := map[string]*ctrlState{}
	writer, lane, seq := "", "", 0
	var buf []rec
	var nextFlush, nextSync Time
	newLane := func(at Time) {
		writer = fmt.Sprintf("%d-sim", at)
		lane = clusterUID + "/" + writer
		seq = 0
		state = map[string]*ctrlState{}
		nextFlush, nextSync = at+F, at+5*sec
	}
	flush := func(at Time) {
		if len(buf) > 0 {
			seq++
			objs = append(objs, object{key: fmt.Sprintf("%s/%012d.delta.ndjson.gz", lane, seq), lane: lane, putAt: at + ingest(), recs: buf})
			lastPut = at
			buf = nil
		}
	}
	doSync := func(at Time) {
		flush(at)
		var recs []rec
		for _, st := range state {
			recs = append(recs, podRecs(st.ver.p(), st.ver, st.vf, 0, at, writer)...)
		}
		sort.Slice(recs, func(i, j int) bool { return recs[i].Level < recs[j].Level })
		seq++
		objs = append(objs, object{key: fmt.Sprintf("%s/%012d.sync.%d.ndjson.gz", lane, seq, at), lane: lane, putAt: at + ingest(), sync: true, syncAt: at, recs: recs})
		lastPut = at
	}
	// observe: the controller's handler for a pod at time obs (state vs what it sees)
	observe := func(p *pod, v *podVer, obs Time) {
		st := state[p.uid]
		if v == nil { // gone
			if st != nil {
				buf = append(buf, podRecs(p, st.ver, st.vf, obs, obs, writer)...)
				delete(state, p.uid)
			}
			return
		}
		if st != nil && st.key == v.key {
			return
		}
		vf := v.ps // created (the controller's `vf := created` for a pod it has not seen)
		if *fixed && since > 0 && vf < since {
			vf = since // the fix (ctrl.Config.Since): from the previous incarnation's last object
		}
		if st != nil {
			vf = obs
			buf = append(buf, podRecs(p, st.ver, st.vf, obs, obs, writer)...)
		}
		state[p.uid] = &ctrlState{key: v.key, vf: vf, ver: v}
		buf = append(buf, podRecs(p, v, vf, 0, obs, writer)...)
	}
	// relist / restart: reconcile the state with the truth at t
	reconcile := func(t Time) {
		for uid, st := range state {
			if pods[uid].at(t) == nil {
				observe(pods[uid], nil, t)
			} else {
				_ = st
			}
		}
		uids := make([]string, 0, len(pods))
		for uid := range pods {
			uids = append(uids, uid)
		}
		sort.Strings(uids)
		for _, uid := range uids {
			if v := pods[uid].at(t); v != nil {
				observe(pods[uid], v, t)
			}
		}
	}
	podOf = map[*podVer]*pod{}
	for _, p := range pods {
		for _, v := range p.vers {
			podOf[v] = p
		}
	}

	newLane(t0 - min_)
	reconcile(t0 - min_ + 2*sec)
	i := 0
	type gapRec struct{ from, to, at Time }
	down := false
	var curGap *window_
	for t := t0 - min_; t < tEnd; {
		// the next thing: a change, a flush, a sync, an outage or gap edge
		next := min(nextFlush, nextSync)
		if i < len(chs) {
			next = min(next, chs[i].t)
		}
		for _, w := range append(append([]window_{}, outs...), gaps...) {
			if w.from > t && w.from < next {
				next = w.from
			}
			if w.to > t && w.to < next {
				next = w.to
			}
		}
		if next >= tEnd {
			break
		}
		t = next
		if w, ok := in(outs, t); ok && !down {
			flush(w.from) // the old incarnation's last object
			down = true
		}
		if down {
			if _, ok := in(outs, t); !ok { // restart: a new incarnation
				down = false
				since = lastPut
				newLane(t)
				reconcile(t + 2*sec)
				restartGapDue = *fixed
			} else {
				for i < len(chs) && chs[i].t <= t {
					i++ // unobserved
				}
				nextFlush, nextSync = t+F, t+R
				continue
			}
		}
		if w, ok := in(gaps, t); ok {
			if curGap == nil {
				curGap = &w
			}
			for i < len(chs) && chs[i].t <= t {
				i++ // not delivered: the watch is broken
			}
		} else if curGap != nil { // the relist completes
			g := *curGap
			curGap = nil
			reconcile(t + sec)
			buf = append(buf, rec{Level: "gap", Key: h64("gap", strconv.FormatInt(int64(g.from), 10)), Entity: h64("gap", "pods"), ClusterKey: clusterKey,
				Kind: "pods", Name: "relist", Attrs: map[string]string{}, ValidFrom: jt(g.from), ClosedAt: jt(t + sec), ObservedAt: jt(t + sec), EventAt: jt(t), Writer: writer})
		}
		for i < len(chs) && chs[i].t <= t {
			c := chs[i]
			i++
			if c.ver == nil {
				observe(c.p, nil, c.t+lag())
			} else {
				observe(c.p, c.ver, c.t+lag())
			}
		}
		if t >= nextFlush {
			flush(t)
			nextFlush = t + F
		}
		if t >= nextSync {
			doSync(t)
			if restartGapDue { // the fix: the window as a gap record, after the first sync
				restartGapDue = false
				buf = append(buf, rec{Level: "gap", Key: h64("gap", "restart", strconv.FormatInt(int64(since), 10)), Entity: h64("gap", "restart"), ClusterKey: clusterKey,
					Kind: "restart", Name: "restart", Attrs: map[string]string{}, ValidFrom: jt(since), ClosedAt: jt(t + sec), ObservedAt: jt(t + sec), EventAt: jt(since), Writer: writer})
			}
			nextSync = t + R
		}
	}
	flush(tEnd)
	sort.SliceStable(objs, func(i, j int) bool { return objs[i].putAt < objs[j].putAt })
	return objs
}

var podOf map[*podVer]*pod

func (v *podVer) p() *pod { return podOf[v] }

// ---- announcements (edges) ----------------------------------------------------

type ann struct {
	rid, ent   uint64
	uid, c     string
	seen, ingd Time
}

func announcements(pods map[string]*pod, tEnd Time, rnd *rand.Rand) []ann {
	W := Time(window.Milliseconds())
	var out []ann
	for _, p := range pods {
		for _, v := range p.vers {
			for _, c := range v.containers {
				end := min(v.vt, tEnd)
				for s := v.vf + sec; s < end; s += W {
					out = append(out, ann{rid: resID(v.key, c), ent: resEnt(p.uid, c), uid: p.uid, c: c, seen: s, ingd: s + 5*sec + Time(rnd.Intn(25000))})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ingd < out[j].ingd })
	return out
}

// ---- ClickHouse: today's aggregator ------------------------------------------------

func chExec(q string, body io.Reader, settings map[string]string) (string, error) {
	u, _ := url.Parse(*chURL)
	v := url.Values{}
	v.Set("query", q)
	for k, s := range settings {
		v.Set(k, s)
	}
	u.RawQuery = v.Encode()
	if body == nil {
		body = strings.NewReader("")
	}
	resp, err := http.Post(u.String(), "text/plain", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("%s: %s", q[:min(len(q), 120)], b)
	}
	return string(b), nil
}

func must(s string, err error) string {
	if err != nil {
		log.Fatal(err)
	}
	return s
}

func applySQL(path string, repl map[string]string) {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			lines = append(lines, l)
		}
	}
	body := strings.Join(lines, "\n")
	for k, v := range repl {
		body = strings.ReplaceAll(body, k, v)
	}
	for _, st := range strings.Split(body, ";\n") {
		if st = strings.TrimSpace(st); st != "" {
			must(chExec(strings.TrimSuffix(st, ";"), nil, nil))
		}
	}
}

func chTime(t Time) string { return time.UnixMilli(int64(t)).UTC().Format("2006-01-02 15:04:05.000") }

// ingestObject does what cmd/aggregator's ingest does for one object (copied
// from ../../../controller/cmd/aggregator/main.go: the records INSERT, the
// sync close with its margin, markGaps).
func ingestObject(o object) {
	var body bytes.Buffer
	for _, r := range o.recs {
		b, _ := json.Marshal(r)
		body.Write(b)
		body.WriteByte('\n')
	}
	set := map[string]string{"insert_deduplication_token": o.key, "deduplicate_blocks_in_dependent_materialized_views": "1",
		"max_insert_block_size": "10000000", "min_insert_block_size_rows": "0", "min_insert_block_size_bytes": "0"}
	must(chExec(fmt.Sprintf("INSERT INTO %s.records (level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from, closed_at, observed_at, event_at, writer) FORMAT JSONEachRow", *db),
		&body, set))
	if o.sync {
		cut := chTime(o.syncAt - 60*sec)
		at := chTime(o.syncAt)
		q := fmt.Sprintf(`INSERT INTO %[1]s.records (level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from, closed_at, observed_at, event_at, writer)
SELECT level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from,
       toDateTime64('%[2]s', 3, 'UTC'), toDateTime64('%[2]s', 3, 'UTC'), toDateTime64('%[2]s', 3, 'UTC'), 'sync-close'
FROM %[1]s.versions_final
WHERE cluster_key = %[3]d AND closed_at = toDateTime64(0, 3, 'UTC') AND last_observed < toDateTime64('%[4]s', 3, 'UTC')`, *db, at, clusterKey, cut)
		must(chExec(q, nil, map[string]string{"insert_deduplication_token": o.key + "#close", "deduplicate_blocks_in_dependent_materialized_views": "1"}))
	}
	n := 0
	for _, g := range o.recs {
		if g.Level != "gap" {
			continue
		}
		q := fmt.Sprintf(`INSERT INTO %[1]s.records (level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from, closed_at, observed_at, event_at, writer, uncertain)
SELECT level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from, closed_at, last_observed, first_event, 'gap-mark', 1
FROM %[1]s.versions_final
WHERE cluster_key = %[2]d AND level != 'gap' AND uncertain = 0
  AND (closed_at BETWEEN toDateTime64('%[3]s', 3, 'UTC') AND toDateTime64('%[4]s', 3, 'UTC')
       OR valid_from BETWEEN toDateTime64('%[3]s', 3, 'UTC') AND toDateTime64('%[4]s', 3, 'UTC'))`, *db, g.ClusterKey, chTime(Time(g.ValidFrom)), chTime(Time(g.ClosedAt)))
		must(chExec(q, nil, map[string]string{"insert_deduplication_token": fmt.Sprintf("%s#gap%d", o.key, n), "deduplicate_blocks_in_dependent_materialized_views": "1"}))
		n++
	}
}

type scdRow struct {
	key, ent  uint64
	vf, vt    Time
	uncertain bool
	source    string
}

func readSCD(q string, entOf func(cols []string) uint64) map[uint64][]scdRow {
	out := map[uint64][]scdRow{}
	s := must(chExec(q+" FORMAT TSV", nil, nil))
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		if l == "" {
			continue
		}
		c := strings.Split(l, "\t")
		k, _ := strconv.ParseUint(c[0], 10, 64)
		vf, _ := strconv.ParseInt(c[1], 10, 64)
		vt, _ := strconv.ParseInt(c[2], 10, 64)
		r := scdRow{key: k, vf: Time(vf), vt: Time(vt), uncertain: c[3] == "1", source: c[4]}
		if r.vt >= open {
			r.vt = bt.Inf
		}
		e := entOf(c[5:])
		r.ent = e
		out[e] = append(out[e], r)
	}
	return out
}

// ---- comparison --------------------------------------------------------------------

type truthIv struct {
	vf, vt Time
	ver    uint64
}

type tally map[string]float64 // category -> hours

func (t tally) add(k string, d Time) { t[k] += float64(d) / float64(hour) }

func catSCD(rows []scdRow, t Time, truth uint64) string {
	var hit []scdRow
	for _, r := range rows {
		if r.vf <= t && t < r.vt {
			hit = append(hit, r)
		}
	}
	switch {
	case len(hit) == 0:
		if truth == 0 {
			return "none(ok)"
		}
		return "MISSING"
	case len(hit) > 1:
		return fmt.Sprintf("OVERLAP(%d)", len(hit))
	}
	r := hit[0]
	u := ""
	if r.uncertain {
		u = ",uncertain"
	}
	if r.source == "announce" {
		u += ",announce"
	}
	switch {
	case truth == 0:
		return "FALSE-ALIVE" + u
	case r.key != truth:
		return "WRONG-VERSION" + u
	}
	return "exact" + u
}

func catBT(r bt.Row, truth uint64) string {
	switch r.State {
	case bt.Asserted:
		u := ""
		if r.Source == bt.Announce {
			u = ",announce"
			if r.Uncertain {
				u += "(fills-unknown)"
			}
		}
		if truth == 0 {
			return "FALSE-ALIVE" + u
		}
		if r.Version != truth {
			return "WRONG-VERSION" + u
		}
		return "exact" + u
	case bt.Retracted, bt.Absent:
		if truth == 0 {
			return "none(ok)"
		}
		return "MISSING(" + r.State.String() + ")"
	}
	if truth == 0 {
		return "unknown(dead)"
	}
	return "unknown(alive)"
}

var debugged int

func minDbg() int64 { v, _ := strconv.ParseInt(os.Getenv("BTC_DEBUG_MIN"), 10, 64); return v }

func compare(truth map[uint64][]truthIv, scd map[uint64][]scdRow, idx *index, t0, tEnd Time, p bt.Policy) (tally, tally, map[string]int) {
	ts, tb := tally{}, tally{}
	ents := map[string]int{}
	for e, tv := range truth {
		rows := bt.Rows(idx.of(e), e, t0, tEnd, bt.Inf, p)
		bounds := []Time{t0, tEnd}
		for _, x := range tv {
			bounds = append(bounds, x.vf, x.vt)
		}
		for _, x := range scd[e] {
			bounds = append(bounds, x.vf, x.vt)
		}
		for _, x := range rows {
			bounds = append(bounds, x.From, x.To)
		}
		sort.Slice(bounds, func(i, j int) bool { return bounds[i] < bounds[j] })
		seen := map[string]bool{}
		for i := 0; i+1 < len(bounds); i++ {
			a, b := bounds[i], bounds[i+1]
			if a >= b || a < t0 || b > tEnd {
				continue
			}
			var tr uint64
			for _, x := range tv {
				if x.vf <= a && a < x.vt {
					tr = x.ver
				}
			}
			var br bt.Row
			for _, x := range rows {
				if x.From <= a && a < x.To {
					br = x
				}
			}
			alive := "dead"
			if tr != 0 {
				alive = "alive"
			}
			cs, cb := catSCD(scd[e], a, tr), catBT(br, tr)
			if dbg := os.Getenv("BTC_DEBUG"); dbg != "" && strings.HasPrefix(cb, dbg) && debugged < 3 && b-a > Time(minDbg()) {
				debugged++
				fmt.Printf("DEBUG entity %d segment [%s, %s) truth %d: %+v\n truth %+v\n rows %+v\n", e, chTime(a), chTime(b), tr, br, tv, rows)
				for _, x := range idx.by[e] {
					fmt.Printf("   ev %+v vf=%s st=%s\n", x, chTime(x.ValidFrom), chTime(x.SystemFrom))
				}
			}
			ts.add(alive+" | "+cs, b-a)
			tb.add(alive+" | "+cb, b-a)
			for _, k := range []string{"scd2 " + alive + " | " + cs, "bitemp " + alive + " | " + cb} {
				if !seen[k] {
					seen[k] = true
					ents[k]++
				}
			}
		}
	}
	return ts, tb, ents
}

// index: events by entity, with the All events every entity sees.
type index struct {
	by  map[uint64][]bt.Event
	all []bt.Event
}

func newIndex(evs []bt.Event) *index {
	x := &index{by: map[uint64][]bt.Event{}}
	for _, e := range evs {
		if e.Entity == bt.All {
			x.all = append(x.all, e)
		} else {
			x.by[e.Entity] = append(x.by[e.Entity], e)
		}
	}
	return x
}

func (x *index) of(e uint64) []bt.Event {
	out := make([]bt.Event, 0, len(x.by[e])+len(x.all))
	return append(append(out, x.by[e]...), x.all...)
}

// ---- main --------------------------------------------------------------------------

func toLane(objs []object) []bt.LaneObject {
	out := make([]bt.LaneObject, len(objs))
	for i, o := range objs {
		lo := bt.LaneObject{Lane: o.lane, PutAt: o.putAt, Sync: o.sync, SyncAt: o.syncAt}
		for _, r := range o.recs {
			lo.Records = append(lo.Records, bt.LaneRecord{Level: r.Level, Key: r.Key, Entity: r.Entity, Kind: r.Kind,
				ValidFrom: Time(r.ValidFrom), ClosedAt: Time(r.ClosedAt), ObservedAt: Time(r.ObservedAt)})
		}
		out[i] = lo
	}
	return out
}

func printTally(name string, t tally) {
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("  %s\n", name)
	for _, k := range keys {
		fmt.Printf("    %-52s %12.2f h\n", k, t[k])
	}
}

func main() {
	flag.Parse()
	rnd := rand.New(rand.NewSource(7))
	pods, t0, tEnd := readPods(*podsPath)
	outs, gaps := parseWindows(*outages, t0), parseWindows(*relists, t0)
	objs := simulate(pods, t0, tEnd, outs, gaps, rnd)
	anns := announcements(pods, tEnd, rnd)
	nrec, nsync := 0, 0
	for _, o := range objs {
		nrec += len(o.recs)
		if o.sync {
			nsync += len(o.recs)
		}
	}
	log.Printf("fleet: %d pods, [%s, %s); controller: %d objects, %d records (%d in syncs); %d announcements", len(pods), chTime(t0), chTime(tEnd), len(objs), nrec, nsync, len(anns))
	res := map[string]any{"pods": len(pods), "t0": chTime(t0), "t_end": chTime(tEnd), "objects": len(objs), "records": nrec, "sync_records": nsync, "announcements": len(anns),
		"fixed_controller": *fixed, "outages": *outages, "gaps": *relists, "resync": resync.String(), "flush": flushF.String(), "trust_window": wTrust.String()}

	// truth, per level
	truthPod, truthRes := map[uint64][]truthIv{}, map[uint64][]truthIv{}
	for _, p := range pods {
		for _, v := range p.vers {
			truthPod[p.ent] = append(truthPod[p.ent], truthIv{v.vf, min(v.vt, bt.Inf), v.key})
			for _, c := range v.containers {
				e := resEnt(p.uid, c)
				truthRes[e] = append(truthRes[e], truthIv{v.vf, v.vt, resID(v.key, c)})
			}
		}
	}

	// today's aggregator
	scdPod, scdRes := map[uint64][]scdRow{}, map[uint64][]scdRow{}
	if !*skipCH {
		t := time.Now()
		must(chExec("DROP DATABASE IF EXISTS "+*db, nil, nil))
		must(chExec("DROP DATABASE IF EXISTS "+*annDB, nil, nil))
		must(chExec("CREATE DATABASE "+*db, nil, nil))
		must(chExec("CREATE DATABASE "+*annDB, nil, nil))
		applySQL(filepath.Join(*sqlDir, "aggregator.sql"), map[string]string{"{db}": *db})
		applySQL(*resSQL, map[string]string{"{table}": *annDB + ".otel_resources"})
		applySQL(filepath.Join(*sqlDir, "announced.sql"), map[string]string{"{db}": *db, "{ann}": *annDB + ".otel_resources"})
		// Consecutive delta objects go in as one INSERT (records merge
		// commutatively; the sync close and markGaps see every earlier
		// record, as they do object by object): one part per object costs
		// ~100 KB of disk each until merges catch up, which a first run
		// with 10,786 objects turned into 1.7 GB.
		var batch object
		flushBatch := func() {
			if len(batch.recs) > 0 {
				ingestObject(batch)
			}
			batch = object{}
		}
		for i, o := range objs {
			gap := false
			for _, r := range o.recs {
				gap = gap || r.Level == "gap"
			}
			if o.sync || gap {
				flushBatch()
				ingestObject(o)
			} else {
				if batch.key == "" {
					batch.key, batch.lane = o.key+"+", o.lane
				}
				batch.recs = append(batch.recs, o.recs...)
			}
			if i%500 == 0 {
				log.Printf("aggregator: %d/%d objects", i, len(objs))
			}
		}
		flushBatch()
		var ab bytes.Buffer
		for i, a := range anns {
			b, _ := json.Marshal(map[string]any{"resource_id": a.rid, "ResourceAttributes": map[string]string{"k8s.cluster.uid": clusterUID, "k8s.pod.uid": a.uid, "k8s.container.name": a.c},
				"signal": "logs", "producer_id": "edge", "producer_epoch": "1", "batch_id": i, "seen_at": chTime(a.seen), "ingested_at": chTime(a.ingd)})
			ab.Write(b)
			ab.WriteByte('\n')
		}
		must(chExec("INSERT INTO "+*annDB+".otel_resources FORMAT JSONEachRow", &ab, nil))
		res["aggregator_s"] = time.Since(t).Seconds()
		scdPod = readSCD(fmt.Sprintf("SELECT pod_key, toUnixTimestamp64Milli(valid_from), toUnixTimestamp64Milli(valid_to), uncertain, 'controller', pod_uid FROM %s.pods", *db),
			func(c []string) uint64 { return podEnt(c[0]) })
		scdRes = readSCD(fmt.Sprintf("SELECT resource_id, toUnixTimestamp64Milli(valid_from), toUnixTimestamp64Milli(valid_to), uncertain, source, pod_uid, container FROM %s.resources", *db),
			func(c []string) uint64 { return resEnt(c[0], c[1]) })
		// the X1 marks
		res["uncertain_versions"] = strings.TrimSpace(must(chExec(fmt.Sprintf("SELECT level, count(), countIf(uncertain = 1) FROM %s.versions_final GROUP BY level ORDER BY level FORMAT TSV", *db), nil, nil)))
		res["current_scd2_resources"] = strings.TrimSpace(must(chExec(fmt.Sprintf("SELECT count() FROM %s.resources WHERE valid_to > toDateTime64('%s', 3, 'UTC') FORMAT TSV", *db, chTime(tEnd)), nil, nil)))
	}

	p := bt.Policy{TrustWindow: Time(wTrust.Milliseconds())}
	lanes := toLane(objs)
	for _, variant := range []struct {
		name string
		opt  bt.MapOptions
		tail Time
	}{
		{"today's inputs", bt.MapOptions{}, 0},
		{"+ restart gaps", bt.MapOptions{RestartGaps: true}, 0},
		{"+ restart gaps, announcement tail = window", bt.MapOptions{RestartGaps: true}, Time(window.Milliseconds())},
	} {
		var seq uint64
		podEvs := bt.FromLanes(lanes, "pod", variant.opt, &seq)
		seq = 0
		resEvs := bt.FromLanes(lanes, "resource", variant.opt, &seq)
		var bas []bt.Announcement
		for _, a := range anns {
			bas = append(bas, bt.Announcement{Entity: a.ent, Version: a.rid, SeenAt: a.seen, IngestedAt: a.ingd})
		}
		resEvs = append(resEvs, bt.FromAnnouncements(bas, variant.tail, &seq)...)
		pi, ri := newIndex(podEvs), newIndex(resEvs)
		debugged = 0
		if os.Getenv("BTC_DEBUG_VARIANT") != "" && os.Getenv("BTC_DEBUG_VARIANT") != variant.name {
			debugged = 1 << 30
		}
		fmt.Printf("== %s: %d pod events, %d resource events (%d All)\n", variant.name, len(podEvs), len(resEvs), len(ri.all))
		out := map[string]any{"pod_events": len(podEvs), "resource_events": len(resEvs), "all_events": len(ri.all)}
		sp, bp, ep := compare(truthPod, scdPod, pi, t0, tEnd, p)
		sr, br, er := compare(truthRes, scdRes, ri, t0, tEnd, p)
		if !*skipCH && variant.name == "today's inputs" {
			printTally("pods view (SCD2), pod-hours by truth | view", sp)
			printTally("resources view (SCD2 + announced), resource-hours", sr)
			out["scd2_pods"], out["scd2_resources"] = sp, sr
		}
		printTally("resolver, pods", bp)
		printTally("resolver, resources", br)
		out["bitemp_pods"], out["bitemp_resources"], out["entities_pods"], out["entities_resources"] = bp, br, ep, er

		// the current view: built as events arrive, pruned; at the end
		all := append([]bt.Event(nil), resEvs...)
		sort.Slice(all, func(i, j int) bool {
			if all[i].SystemFrom != all[j].SystemFrom {
				return all[i].SystemFrom < all[j].SystemFrom
			}
			return all[i].Seq < all[j].Seq
		})
		tc := time.Now()
		cur := bt.NewCurrent(p, t0)
		for i, e := range all {
			cur.Advance(e.SystemFrom)
			cur.Add(e)
			if i%200000 == 0 {
				cur.Compact()
			}
		}
		cur.Advance(tEnd)
		cur.Compact()
		build := time.Since(tc)
		live, wrong, liveTruth, tomb := 0, 0, 0, 0
		for e, tv := range truthRes {
			var want uint64
			for _, x := range tv {
				if x.vf <= tEnd && tEnd < x.vt {
					want = x.ver
				}
			}
			if want != 0 {
				liveTruth++
			}
			r := cur.Get(e)
			if r.State == bt.Asserted {
				live++
			}
			if r.State == bt.Retracted {
				tomb++
			}
			if (r.State == bt.Asserted) != (want != 0) || (want != 0 && r.Version != want) {
				wrong++
			}
			if w := bt.Resolve(ri.of(e), e, tEnd, bt.Inf, p); w != r && !(w.State == r.State && w.Version == r.Version && w.Source == r.Source && w.Uncertain == r.Uncertain) {
				log.Fatalf("current view != resolve for %d: %+v vs %+v", e, r, w)
			}
		}
		fmt.Printf("  current view at the end: %d events kept of %d (%.2f%%), %d entities; asserted %d, retracted (tombstones) %d, truth live %d, disagreeing with truth %d; built in %s\n",
			cur.Len(), len(resEvs), 100*float64(cur.Len())/float64(len(resEvs)), cur.Entities(), live, tomb, liveTruth, wrong, build.Round(time.Millisecond))
		out["current"] = map[string]any{"kept": cur.Len(), "events": len(resEvs), "entities": cur.Entities(), "asserted": live, "tombstones": tomb, "truth_live": liveTruth, "wrong": wrong, "build_ms": build.Milliseconds()}

		// cost per lookup
		ents := make([]uint64, 0, len(truthRes))
		for e := range truthRes {
			ents = append(ents, e)
		}
		sort.Slice(ents, func(i, j int) bool { return ents[i] < ents[j] })
		lists := make([][]bt.Event, len(ents))
		evn := 0
		for i, e := range ents {
			lists[i] = ri.of(e)
			evn += len(lists[i])
		}
		r2 := rand.New(rand.NewSource(1))
		const N = 200000
		runtime.GC()
		t1 := time.Now()
		for k := 0; k < N; k++ {
			i := r2.Intn(len(ents))
			bt.Resolve(lists[i], ents[i], t0+Time(r2.Int63n(int64(tEnd-t0))), bt.Inf, p)
		}
		point := time.Since(t1) / N
		t2 := time.Now()
		for k := 0; k < N/10; k++ {
			i := r2.Intn(len(ents))
			bt.ResolveRange(lists[i], ents[i], t0, tEnd, bt.Inf, p, func(bt.Row) bool { return true })
		}
		rng := time.Since(t2) / (N / 10)
		t3 := time.Now()
		for k := 0; k < N; k++ {
			cur.Get(ents[r2.Intn(len(ents))])
		}
		cget := time.Since(t3) / N
		fmt.Printf("  cost: %.0f events per entity lookup (own + All); Resolve (point, history) %s; ResolveRange (whole range) %s; Current.Get %s\n",
			float64(evn)/float64(len(ents)), point, rng, cget)
		out["cost"] = map[string]any{"events_per_lookup": float64(evn) / float64(len(ents)), "resolve_point_ns": point.Nanoseconds(), "resolve_range_ns": rng.Nanoseconds(), "current_get_ns": cget.Nanoseconds()}
		res[variant.name] = out
	}
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(res, "", " ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			log.Fatal(err)
		}
	}
}
