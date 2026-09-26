// Package modelcheck validates real Go edge runs against the Quint models of
// the manifest-less commit (../../model/s3Inline.qnt for one lane,
// s3InlineMetrics.qnt for a request split over two lanes), the way
// ../../PBT.md does for chdbexporter: the run is recorded as model steps and
// quintgo replays them against the model (`quint test`): every step must be
// a transition the model can take from the state reached, the lane's state
// after it must be the model writer's (phase, next slot), and the model's
// invariants must hold in every state.
//
// What is recorded, from three sources, in one order:
//   - the lanes (commit.Observer): start → startPush / switchPayload, the
//     PUT → send, its answer → receive (200, or 412 with the HEAD that
//     reads the slot) or timeout, the HEAD after a timeout → resolve, a halt
//     → newIncarnation + startPush in the new epoch;
//   - the store (commit.MemStore hooks): a PUT applied, now or late → apply,
//     a PUT dropped → lose;
//   - the harness: crashes and zombies (newIncarnation / crash), the
//     request acknowledgements (ackRequest), and a consumer that follows the
//     model's (cCheck, cInsert, cAdvance, cSeeTomb, cTomb, cTombReceive),
//     whose tombstones the lanes then meet.
//
// One thing the model cannot express is dropped: it keeps in-flight
// requests in a set, so two identical requests (a resend of a request that
// is still in flight, both seen with the slot free) are one; the second of
// them to reach the store is not recorded (it can only get a 412 there).
package modelcheck

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/quintgo/qtrace"
)

type entry struct {
	kind  string // Free, Data, Tomb
	epoch int
	p     int
}

func (x entry) code() int64 {
	switch x.kind {
	case "Data":
		return 1
	case "Tomb":
		return 2
	}
	return 0
}

func (x entry) q() string {
	return fmt.Sprintf("{ kind: %s, epoch: %d, payload: %d }", x.kind, x.epoch, x.p)
}

type req struct {
	e, slot int
	x       entry
	sawFree bool
}

func (r req) q() string {
	return fmt.Sprintf("{ epoch: %d, slot: %d, entry: %s, sawFree: %v }", r.e, r.slot, r.x.q(), r.sawFree)
}

// args: the binding's rq(qe, qs, qk, qp, qf) (the entry's epoch is the slot's).
func (r req) args() []any {
	return []any{"qe", int64(r.e), "qs", int64(r.slot), "qk", r.x.code(), "qp", int64(r.x.p), "qf", r.sawFree}
}

type resp struct {
	e, slot int
	x       entry
	ok      bool
}

func (r resp) q() string {
	return fmt.Sprintf("{ epoch: %d, slot: %d, entry: %s, ok: %v }", r.e, r.slot, r.x.q(), r.ok)
}

// args: the binding's rs(re, rs, rk, rp, rok).
func (r resp) args() []any {
	return []any{"re", int64(r.e), "rs", int64(r.slot), "rk", r.x.code(), "rp", int64(r.x.p), "rok", r.ok}
}

type laneState struct {
	e          int
	p          int
	alive      bool
	unresolved bool
	pending412 *resp
	last       int // index of the lane's last step, for its post-state expect
}

// instance is one s3Inline log (one namespace's lane): the model's view of it.
type instance struct {
	ns, prefix string // namespace; action prefix ("" or "g"/"s"); vars prefix ("" or "G::"/"S::")
	vars       string
	lease      int
	epochs     map[string]int // Go epoch name -> model epoch
	names      map[int]string
	log        map[[2]int]entry
	inflight   []req
	responses  []resp
	lanes      map[string]*laneState // lane key -> state
	queue      map[int]bool          // the model's queue: payloads not yet found committed
	payloads   map[string]int        // content key -> payload
	keyPrefix  string                // object keys of this namespace start with it
}

// Translator turns a run into quintgo steps. Safe for concurrent use: every
// event is translated under one lock, which fixes the order.
type Translator struct {
	mu    sync.Mutex
	steps []qtrace.Step
	insts map[string]*instance // namespace -> instance
	seq   uint64
	Notes []string // what was dropped, and why
}

// NewTranslator: namespaces maps each observed namespace to its model
// instance prefix ("" for s3Inline itself; "g" / "s" for s3InlineMetrics'
// G and S); keyPrefix is the edge's key prefix.
func NewTranslator(keyPrefix string, namespaces map[string]string) *Translator {
	t := &Translator{insts: map[string]*instance{}}
	for ns, pre := range namespaces {
		vars := ""
		if pre != "" {
			vars = strings.ToUpper(pre) + "::"
		}
		t.insts[ns] = &instance{ns: ns, prefix: pre, vars: vars, lease: 1, epochs: map[string]int{}, names: map[int]string{},
			log: map[[2]int]entry{}, lanes: map[string]*laneState{}, payloads: map[string]int{}, queue: map[int]bool{},
			keyPrefix: strings.Trim(keyPrefix+"/"+ns, "/") + "/"}
	}
	return t
}

// Payload registers content key c of namespace ns as request p.
func (t *Translator) Payload(ns, c string, p int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.insts[ns].payloads[c] = p
	t.insts[ns].queue[p] = true
}

// Queued reports whether the model's queue of namespace ns still holds p.
// The model's queue is told a payload is exported as soon as any lane finds
// it committed (its own commit, or another batch found in a slot); the
// collector's queue only when that request's push returns. The harness
// follows the model (a request found committed is not pushed again), which
// only removes duplicates the real edge would make and the consumer skip.
func (t *Translator) Queued(ns string, p int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.insts[ns].queue[p]
}

// SetQueue is the model's crash: the queue holds the pending requests again.
func (t *Translator) setQueue(in *instance, pending []int) {
	clear(in.queue)
	for _, p := range pending {
		in.queue[p] = true
	}
}

// Steps returns the recorded steps.
func (t *Translator) Steps() []qtrace.Step {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]qtrace.Step(nil), t.steps...)
}

func (t *Translator) add(action string, args ...any) int {
	a := map[string]any{"ee": int64(0), "ph": int64(0), "nx": int64(0)}
	for i := 0; i+1 < len(args); i += 2 {
		a[args[i].(string)] = args[i+1]
	}
	t.seq++
	t.steps = append(t.steps, qtrace.Step{Action: action, Seq: t.seq, Actor: "edge", Process: "run", Outcome: "ok",
		Args: a, Obs: map[string]any{}, Source: fmt.Sprintf("step %d", t.seq)})
	return len(t.steps) - 1
}

func (in *instance) act(name string) string {
	if in.prefix == "" {
		return name
	}
	return in.prefix + strings.ToUpper(name[:1]) + name[1:]
}

func (t *Translator) inst(key string) *instance {
	for _, in := range t.insts {
		if strings.HasPrefix(key, in.keyPrefix) {
			return in
		}
	}
	return nil
}

// ---- the harness --------------------------------------------------------------

// Bind makes lane (a *commit.Lane's name plus an incarnation tag) the writer
// of the instance's current incarnation.
func (t *Translator) Bind(ns, lane string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	in := t.insts[ns]
	in.lanes[lane] = &laneState{e: in.lease, alive: true, last: -1}
}

// NewIncarnation records a restart (zombie=false: every earlier writer of
// the instance is dead) or a zombie start (the earlier ones keep running),
// and binds lane to the new incarnation.
func (t *Translator) NewIncarnation(ns, lane string, zombie bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	in := t.insts[ns]
	t.newIncarnation(in, zombie)
	in.lanes[lane] = &laneState{e: in.lease, alive: true, last: -1}
}

func (t *Translator) newIncarnation(in *instance, zombie bool) {
	in.lease++
	if !zombie {
		for _, l := range in.lanes {
			l.alive = false
		}
	}
	t.add(in.act("newIncarnation"), "zombie", zombie)
}

// Crash records s3InlineMetrics' crash: every instance takes a new epoch,
// the sender resends the pending requests; lanes maps each namespace to its
// new lane key.
func (t *Translator) Crash(pending []int, lanes map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sort.Ints(pending)
	in1, in2 := false, false
	for _, p := range pending {
		in1, in2 = in1 || p == 1, in2 || p == 2
	}
	t.add("crash", "p1", in1, "p2", in2)
	for ns, lane := range lanes {
		in := t.insts[ns]
		t.setQueue(in, pending)
		in.lease++
		for _, l := range in.lanes {
			l.alive = false
		}
		in.lanes[lane] = &laneState{e: in.lease, alive: true, last: -1}
	}
}

// AckRequest records s3InlineMetrics' ackRequest(p): the edge answered 2xx.
func (t *Translator) AckRequest(p int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.add("ackRequest", "p", int64(p))
}

// ---- the lanes -----------------------------------------------------------------

// phaseCode numbers the lane's phase as the binding's phaseOf does.
func phaseCode(p commit.Phase) int64 {
	return map[commit.Phase]int64{commit.Idle: 0, commit.Ready: 1, commit.Waiting: 2, commit.Unresolved: 3, commit.Halted: 4}[p]
}

// expect sets the post-state check of the lane's last step: the model
// writer's phase and next slot are the lane's.
func (t *Translator) expect(in *instance, l *laneState, ev commit.Event) {
	if l.last < 0 {
		return
	}
	a := t.steps[l.last].Args
	a["ee"], a["ph"], a["nx"] = int64(l.e), phaseCode(ev.Phase), int64(ev.Next)
}

// Tagged is the Observer an edge instance gets: its lanes' events carry
// "{lane}#{tag}", the key the harness binds.
func (t *Translator) Tagged(tag string) commit.Observer { return tagged{t, tag} }

type tagged struct {
	t   *Translator
	tag string
}

func (o tagged) Observe(ev commit.Event) {
	ev.Lane += "#" + o.tag
	o.t.Observe(ev)
}

// Observe translates one lane event.
func (t *Translator) Observe(ev commit.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ns := ev.Lane[:strings.LastIndex(ev.Lane, "/")]
	in := t.insts[ns]
	if in == nil {
		return
	}
	l := in.lanes[ev.Lane]
	if l == nil {
		t.Notes = append(t.Notes, "event of an unbound lane: "+ev.Kind)
		return
	}
	e := l.e
	switch ev.Kind {
	case "start":
		l.p = in.payloads[ev.Content]
		if l.unresolved {
			l.last = t.add(in.act("switchPayload"), "e", int64(e), "p", int64(l.p))
		} else {
			l.last = t.add(in.act("startPush"), "e", int64(e), "p", int64(l.p))
		}
		l.unresolved = false
	case "put":
		if _, ok := in.epochs[ev.Ref.Epoch]; !ok {
			in.epochs[ev.Ref.Epoch], in.names[e] = e, ev.Ref.Epoch
		}
		slot := int(ev.Ref.Seq)
		r := req{e: e, slot: slot, x: entry{"Data", e, l.p}, sawFree: in.log[[2]int{e, slot}].kind == ""}
		if !containsReq(in.inflight, r) {
			in.inflight = append(in.inflight, r)
		}
		l.last = t.add(in.act("send"), "e", int64(e), "slot", int64(slot))
	case "putResult":
		slot := int(ev.Ref.Seq)
		switch ev.Outcome {
		case commit.PutOK:
			r := resp{e, slot, entry{"Data", e, l.p}, true}
			in.responses = removeResp(in.responses, r)
			l.last = t.add(in.act("receive"), append([]any{"e", int64(e)}, r.args()...)...)
		case commit.PutExists:
			r := resp{e, slot, entry{"Data", e, l.p}, false}
			l.pending412 = &r
		default:
			l.last = t.add(in.act("timeout"), "e", int64(e))
			l.unresolved = true
		}
	case "head":
		if l.pending412 != nil {
			r := *l.pending412
			l.pending412 = nil
			in.responses = removeResp(in.responses, r)
			l.last = t.add(in.act("receive"), append([]any{"e", int64(e)}, r.args()...)...)
		} else {
			l.last = t.add(in.act("resolve"), "e", int64(e))
		}
		l.unresolved = false
	case "headError":
		if l.pending412 != nil {
			// 412, but the HEAD that reads the slot got no answer: to the
			// model the answer never came (the 412 response stays unread).
			l.pending412 = nil
			l.last = t.add(in.act("timeout"), "e", int64(e))
		}
		l.unresolved = true
	case "committed", "resolvedOwn":
		delete(in.queue, l.p)
		t.expect(in, l, ev)
	case "learnedOther":
		delete(in.queue, in.payloads[ev.Content])
		t.expect(in, l, ev)
	case "resend":
		t.expect(in, l, ev)
	case "halted":
		t.expect(in, l, ev)
		zombie := false
		for k, o := range in.lanes {
			if o != l && o.alive && k != "" {
				zombie = true
			}
		}
		t.newIncarnation(in, zombie)
		l.e, l.alive = in.lease, true
		in.epochs[ev.NewEpoch], in.names[l.e] = l.e, ev.NewEpoch
		l.last = t.add(in.act("startPush"), "e", int64(l.e), "p", int64(l.p))
	}
}

// ---- the store --------------------------------------------------------------------

func (t *Translator) entryOf(in *instance, e int, meta map[string]string) entry {
	if meta[commit.MetaKind] == commit.KindTomb {
		return entry{"Tomb", e, 0}
	}
	return entry{"Data", e, in.payloads[meta[commit.MetaContent]]}
}

// OnApply is commit.MemStore's hook: the store decided a PUT.
func (t *Translator) OnApply(key string, applied, late bool, meta map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	in := t.inst(key)
	if in == nil {
		return
	}
	epoch, seq, _ := commit.ParseSlotKey(strings.TrimSuffix(in.keyPrefix, "/"), key)
	e, ok := in.epochs[epoch]
	if !ok {
		t.Notes = append(t.Notes, "apply to an unknown epoch: "+key)
		return
	}
	x := t.entryOf(in, e, meta)
	var r *req
	for i := range in.inflight {
		if q := in.inflight[i]; q.e == e && q.slot == int(seq) && q.x == x {
			r = &in.inflight[i]
			break
		}
	}
	if r == nil {
		t.Notes = append(t.Notes, fmt.Sprintf("apply of %s (%s): the model's in-flight set already consumed an identical request", key, x.q()))
		return
	}
	q := *r
	in.inflight = removeReq(in.inflight, q)
	k := [2]int{e, int(seq)}
	wins := in.log[k].kind == ""
	if wins != applied {
		t.Notes = append(t.Notes, fmt.Sprintf("store and model disagree on %s: applied=%v", key, applied))
	}
	if wins {
		in.log[k] = x
	}
	rs := resp{e, int(seq), x, wins}
	if !containsResp(in.responses, rs) {
		in.responses = append(in.responses, rs)
	}
	t.add(in.act("apply"), append(q.args(), "slot", int64(seq))...)
}

// OnLose is commit.MemStore's hook: a PUT was lost.
func (t *Translator) OnLose(key string, meta map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	in := t.inst(key)
	if in == nil {
		return
	}
	epoch, seq, _ := commit.ParseSlotKey(strings.TrimSuffix(in.keyPrefix, "/"), key)
	e := in.epochs[epoch]
	x := t.entryOf(in, e, meta)
	for _, q := range in.inflight {
		if q.e == e && q.slot == int(seq) && q.x == x {
			in.inflight = removeReq(in.inflight, q)
			t.add(in.act("lose"), q.args()...)
			return
		}
	}
}

// ---- the consumer (s3Inline's, driven by the harness) ------------------------------

// Consumer is the model's consumer over the store: per epoch a checkpoint
// and a closed flag, central counts per payload.
type Consumer struct {
	T       *Translator
	NS      string
	Store   *commit.MemStore
	ckpt    map[int]int
	closed  map[int]bool
	central map[int]int
}

func (c *Consumer) init() {
	if c.ckpt == nil {
		c.ckpt, c.closed, c.central = map[int]int{}, map[int]bool{}, map[int]int{}
	}
}

// Candidates are the epochs where a consumer step is enabled.
func (c *Consumer) Candidates() []int {
	c.init()
	t := c.T
	t.mu.Lock()
	defer t.mu.Unlock()
	in := t.insts[c.NS]
	var out []int
	for e := 1; e <= in.lease; e++ {
		if c.closed[e] {
			continue
		}
		x := in.log[[2]int{e, c.ckpt[e]}]
		if x.kind != "" || (e < in.lease && in.names[e] != "") {
			out = append(out, e)
		}
	}
	return out
}

// Step takes the consumer's next step in epoch e: ingest the slot at the
// checkpoint, see a tombstone there, or race a tombstone into it.
func (c *Consumer) Step(e int) {
	c.init()
	t := c.T
	t.mu.Lock()
	in := t.insts[c.NS]
	s := c.ckpt[e]
	x := in.log[[2]int{e, s}]
	switch x.kind {
	case "Data":
		present := c.central[x.p] > 0
		t.add(in.act("cCheck"), "e", int64(e))
		t.add(in.act("cInsert"))
		t.add(in.act("cAdvance"))
		if !present {
			c.central[x.p]++
		}
		c.ckpt[e] = s + 1
		t.mu.Unlock()
	case "Tomb":
		t.add(in.act("cSeeTomb"), "e", int64(e))
		c.closed[e] = true
		t.mu.Unlock()
	default:
		name := in.names[e]
		q := req{e: e, slot: s, x: entry{"Tomb", e, 0}, sawFree: true}
		in.inflight = append(in.inflight, q)
		t.add(in.act("cTomb"), "e", int64(e), "slot", int64(s))
		t.mu.Unlock()
		won := c.Store.Tomb(commit.SlotKey(strings.TrimSuffix(in.keyPrefix, "/"), name, uint64(s)))
		t.mu.Lock()
		r := resp{e, s, entry{"Tomb", e, 0}, won}
		in.responses = removeResp(in.responses, r)
		t.add(in.act("cTombReceive"), r.args()...)
		if won {
			c.closed[e] = true
		}
		t.mu.Unlock()
	}
}

func containsReq(v []req, r req) bool {
	for _, x := range v {
		if x == r {
			return true
		}
	}
	return false
}

func removeReq(v []req, r req) []req {
	for i, x := range v {
		if x == r {
			return append(v[:i:i], v[i+1:]...)
		}
	}
	return v
}

func containsResp(v []resp, r resp) bool {
	for _, x := range v {
		if x == r {
			return true
		}
	}
	return false
}

func removeResp(v []resp, r resp) []resp {
	for i, x := range v {
		if x == r {
			return append(v[:i:i], v[i+1:]...)
		}
	}
	return v
}
