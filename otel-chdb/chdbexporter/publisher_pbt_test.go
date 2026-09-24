package chdbexporter

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"hegel.dev/go/hegel"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

// A state-machine property test of the publishing protocol (publish.go),
// run against the real publisher over fake sessions (fakesession_test.go).
//
// Hegel drives command sequences: pushes of new payloads and retries of
// failed ones (as the collector's queue does), pushes during which one
// statement fails or fails ambiguously (applied, then an error), clock moves
// within and across generations (the publisher's injectable now), retention
// sweeps, shutdowns and crashes with restarts under a new epoch. Every rule
// is a separate method so hegel's swarm testing switches whole kinds of
// command off per test case: some runs have no faults, some only manifest
// faults, some never rotate. After every command the invariants compare the
// world (S3 objects, tables and their rows) with a small reference model
// kept here: which pushes returned success, when each generation was sealed.
//
// The default test checks what the design promises. The TestPBTFinding*
// variants each switch on one stricter property or one environment the
// design does not handle, and pass when hegel finds and shrinks a
// counterexample: the seal undercount (F3) and orphan rows (F2) that the
// Quint model found, and three the model cannot express.

type pubOpts struct {
	faults    []string // ops that may fail: ddl, insert, parquet, manifest, seal, drop, optimize, detach
	ambiguous bool     // a fault may apply the write and then fail
	clockBack bool     // the clock may step backwards
	// persistentPath: a restart keeps the local chDB catalog, as a configured
	// path (otelcol/config.edge.yaml: path: ./data/chdb) does. Otherwise the
	// catalog is fresh, as with the default temporary path.
	persistentPath bool
	strictSeal     bool // _sealed.json must match the manifests (F3)
	noOrphans      bool // no table rows without a manifest (F2)
	housekeeping   bool // seals and detaches are not lost when their statement fails
	steps          int
	record         bool // record quint steps
}

var pubRunSeq atomic.Uint64

type attempt struct {
	epoch, signal, payload, gen string
	batch                       uint64 // 0: none allocated
	rows                        int
	err                         error
}

type genKey struct{ ns, gen string }

type pubMachine struct {
	o       pubOpts
	w       *fakeWorld
	cfg     *Config
	e       *chdbExporter
	p       *publisher
	clock   time.Time
	run     uint64
	incs    int // incarnations
	signals []string

	attempts []attempt
	queue    map[string]string // unacknowledged payload -> signal
	payloads int
	// sealedAt: generations the reference model knows were rotated or
	// closed, and when; sealFault: those whose seal statements had a fault.
	sealedAt  map[genKey]time.Time
	sealFault map[genKey]bool
	// detachDue: stored tables that must be detached by now.
	detachDue map[string]bool
	detachErr map[string]bool
	log       []string
}

// lastPubMachine is the machine of the most recent test case: after a failed
// hegel.Run it is the replay of the shrunk counterexample.
var lastPubMachine atomic.Pointer[pubMachine]

func newPubMachine(tc hegel.TestCase, o pubOpts) *pubMachine {
	m := &pubMachine{o: o, run: pubRunSeq.Add(1), queue: map[string]string{}, sealedAt: map[genKey]time.Time{},
		sealFault: map[genKey]bool{}, detachDue: map[string]bool{}, detachErr: map[string]bool{}}
	cfg := createDefaultConfig().(*Config)
	cfg.Path = "unused"
	cfg.Database = "pbt"
	cfg.Producer.ID, cfg.Producer.Region = "edge", "r1"
	cfg.ObjectStorage.Generation = time.Hour
	cfg.ObjectStorage.LocalRetention = 3 * time.Hour
	cfg.ObjectStorage.SealOptimize = hegel.Draw(tc, hegel.Booleans())
	cfg.StagingTables = hegel.Draw(tc, hegel.Booleans())
	cfg.Connections = hegel.Draw(tc, hegel.Integers(1, 3))
	switch hegel.Draw(tc, hegel.SampledFrom([]string{"tables+parquet", "tables", "parquet"})) {
	case "tables+parquet":
		cfg.ObjectStorage.Endpoint = "http://s3.fake/otel"
		cfg.Parquet.URL = "http://s3.fake/parquet"
	case "tables":
		cfg.ObjectStorage.Endpoint = "http://s3.fake/otel"
	case "parquet":
		cfg.StoreTables = false
		cfg.Parquet.URL = "http://s3.fake/parquet"
	}
	// Which signals this run pushes: one most of the time, so that pushes
	// meet in the same generations.
	m.signals = hegel.Draw(tc, hegel.SampledFrom([][]string{{signalLogs}, {signalTraces}, {signalLogs, signalTraces}}))
	m.cfg = cfg
	m.w = newFakeWorld(cfg)
	if o.record {
		m.w.rec = newStepRecorder()
	}
	m.clock = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC).Add(time.Duration(hegel.Draw(tc, hegel.Integers(0, 59))) * time.Minute)
	m.logf("config: %s, staging %v, optimize %v, %d connections, clock %s", map[bool]string{true: "tables", false: "no tables"}[cfg.StoreTables]+
		map[bool]string{true: "+parquet", false: ""}[cfg.parquet()], cfg.StagingTables, cfg.ObjectStorage.SealOptimize, cfg.Connections, m.clock.Format("15:04"))
	if err := m.start(); err != nil {
		tc.Errorf("start: %v", err)
	}
	return m
}

func (m *pubMachine) logf(format string, args ...any) {
	m.log = append(m.log, fmt.Sprintf(format, args...))
}

func (m *pubMachine) epoch() string { return fmt.Sprintf("r%de%d", m.run, m.incs) }

// start brings up a new incarnation: a new epoch, the real newPublisher
// over fake sessions, its retention loop stopped (the machine sweeps).
func (m *pubMachine) start() error {
	m.incs++
	cfg := *m.cfg
	cfg.Producer.Epoch = m.epoch()
	if err := cfg.Validate(); err != nil {
		return err
	}
	e := newExporter(zap.NewNop(), &cfg, "")
	e.conns = make(chan session, cfg.Connections)
	for i := 0; i < cfg.Connections; i++ {
		s := fakeSession{m.w}
		e.all = append(e.all, s)
		e.conns <- s
	}
	p, err := newPublisher(e)
	if err != nil {
		return err
	}
	if cfg.StoreTables {
		close(p.stop)
		<-p.done
		p.stop = make(chan struct{})
		p.done = make(chan struct{})
		close(p.done)
	}
	p.now = func() time.Time { return m.clock }
	e.pub = p
	m.e, m.p = e, p
	m.w.disarm()
	return nil
}

func (m *pubMachine) ns(signal string) string { return m.p.namespace(signal) }

func (m *pubMachine) peekBatch(signal string) uint64 {
	if v, ok := batchSeq.Load(m.ns(signal)); ok {
		return v.(*atomic.Uint64).Load()
	}
	return 0
}

// ---- commands -----------------------------------------------------------------

// pushOnce exports one payload (a new one, or a retry of a queued one) of
// 1-3 rows, with an optional fault armed, and updates the reference model.
func pushOnce(m *pubMachine, tc hegel.TestCase, retry bool, faultOp, faultMode string) {
	var payload, signal string
	if retry {
		keys := make([]string, 0, len(m.queue))
		for k := range m.queue {
			keys = append(keys, k)
		}
		tc.Assume(len(keys) > 0)
		sort.Strings(keys)
		payload = hegel.Draw(tc, hegel.SampledFrom(keys))
		signal = m.queue[payload]
	} else {
		m.payloads++
		payload = fmt.Sprintf("p%d", m.payloads)
		signal = hegel.Draw(tc, hegel.SampledFrom(m.signals))
		m.queue[payload] = signal
	}
	n := hegel.Draw(tc, hegel.Integers(1, 3))
	if faultOp != "" {
		m.w.arm(faultOp, faultMode)
	}
	gen, _ := genID(m.clock, m.cfg.generation())
	cur := m.p.signals[signal].cur
	before := m.peekBatch(signal)
	m.w.payload = payload
	var err error
	if signal == signalLogs {
		ld := testgen.Logs(n)
		err = m.p.push(signal, func(w rowWriter, env *envelope) int { return writeLogs(w, ld, env) })
	} else {
		td := testgen.Traces(n)
		err = m.p.push(signal, func(w rowWriter, env *envelope) int { return writeTraces(w, td, env) })
	}
	m.p.wg.Wait() // a rotation's background seal
	fired := m.w.disarm()
	a := attempt{epoch: m.p.epoch, signal: signal, payload: payload, gen: gen, rows: n, err: err}
	if after := m.peekBatch(signal); after != before {
		a.batch = after
	}
	m.attempts = append(m.attempts, a)
	if err == nil {
		delete(m.queue, payload)
	}
	if cur != nil && cur.id != gen && (m.p.signals[signal].cur == nil || m.p.signals[signal].cur.id != cur.id) {
		m.rotated(signal, cur.id, fired)
	}
	res := "ok"
	if err != nil {
		res = "error: " + err.Error()
	}
	m.logf("push %s %s (%d rows) at %s -> batch %d in %s; faults %v; %s", payload, signal, n, m.clock.Format("15:04"), a.batch, gen, fired, res)
	tc.Note(m.log[len(m.log)-1])
}

// rotated records that a generation was rotated away (and so sealed).
func (m *pubMachine) rotated(signal, gen string, fired []fault) {
	k := genKey{m.ns(signal), gen}
	m.sealedAt[k] = m.clock
	for _, f := range fired {
		if f.op == "seal" || f.op == "drop" || f.op == "optimize" {
			m.sealFault[k] = true
		}
	}
}

func drawFault(m *pubMachine, tc hegel.TestCase, op string) (string, string) {
	tc.Assume(slices.Contains(m.o.faults, op))
	mode := "fail"
	if m.o.ambiguous && hegel.Draw(tc, hegel.Booleans()) {
		mode = "ambiguous"
	}
	return op, mode
}

func (m *pubMachine) RulePush(tc hegel.TestCase)  { pushOnce(m, tc, false, "", "") }
func (m *pubMachine) RuleRetry(tc hegel.TestCase) { pushOnce(m, tc, true, "", "") }

func (m *pubMachine) RuleTableFault(tc hegel.TestCase) {
	op, mode := drawFault(m, tc, "insert")
	pushOnce(m, tc, hegel.Draw(tc, hegel.Booleans()) && len(m.queue) > 0, op, mode)
}

func (m *pubMachine) RuleParquetFault(tc hegel.TestCase) {
	tc.Assume(m.cfg.parquet())
	op, mode := drawFault(m, tc, "parquet")
	pushOnce(m, tc, hegel.Draw(tc, hegel.Booleans()) && len(m.queue) > 0, op, mode)
}

func (m *pubMachine) RuleManifestFault(tc hegel.TestCase) {
	op, mode := drawFault(m, tc, "manifest")
	pushOnce(m, tc, hegel.Draw(tc, hegel.Booleans()) && len(m.queue) > 0, op, mode)
}

// RuleRotateWithFault moves into the next generation and pushes, with a
// fault in the new generation's DDL or in the old one's seal.
func (m *pubMachine) RuleRotateWithFault(tc hegel.TestCase) {
	op := hegel.Draw(tc, hegel.SampledFrom([]string{"ddl", "seal", "drop", "optimize"}))
	tc.Assume(m.cfg.StoreTables || op == "seal")
	op, mode := drawFault(m, tc, op)
	m.clock = m.clock.Add(m.cfg.generation())
	pushOnce(m, tc, false, op, mode)
}

func (m *pubMachine) RuleTick(tc hegel.TestCase) {
	d := time.Duration(hegel.Draw(tc, hegel.Integers(1, 40))) * time.Minute
	m.clock = m.clock.Add(d)
	m.logf("tick +%v -> %s", d, m.clock.Format("15:04"))
	tc.Note(m.log[len(m.log)-1])
}

func (m *pubMachine) RuleNextGeneration(tc hegel.TestCase) {
	n := hegel.Draw(tc, hegel.Integers(1, 3))
	m.clock = m.clock.Add(time.Duration(n) * m.cfg.generation())
	m.logf("clock +%d generation(s) -> %s", n, m.clock.Format("15:04"))
	tc.Note(m.log[len(m.log)-1])
}

// RuleClockBack steps the wall clock back (NTP, a VM migration), or
// equivalently: a push that read the clock just before a generation boundary
// takes the rotation lock after one that read it just after.
func (m *pubMachine) RuleClockBack(tc hegel.TestCase) {
	tc.Assume(m.o.clockBack)
	d := time.Duration(hegel.Draw(tc, hegel.Integers(1, 2)))*m.cfg.generation() - time.Duration(hegel.Draw(tc, hegel.Integers(0, 59)))*time.Minute
	m.clock = m.clock.Add(-d)
	m.logf("clock -%v -> %s", d, m.clock.Format("15:04"))
	tc.Note(m.log[len(m.log)-1])
}

// RuleRetention lets local_retention pass for some sealed generations and
// runs the sweep the retention loop would run, maybe with a failing DETACH.
func (m *pubMachine) RuleRetention(tc hegel.TestCase) {
	tc.Assume(m.cfg.StoreTables)
	m.clock = m.clock.Add(time.Duration(hegel.Draw(tc, hegel.Integers(0, 7))) * time.Hour / 2)
	faulty := len(m.o.faults) > 0 && slices.Contains(m.o.faults, "detach") && hegel.Draw(tc, hegel.Booleans())
	if faulty {
		m.w.arm("detach", "fail")
	}
	m.p.detachExpired()
	fired := m.w.disarm()
	for k, at := range m.sealedAt {
		if m.clock.Sub(at) < m.cfg.ObjectStorage.LocalRetention || !strings.HasSuffix(k.ns, "/"+m.p.epoch) {
			continue
		}
		for _, t := range m.storedTables(k) {
			if !m.detachDue[t] {
				m.detachDue[t] = true
				m.detachErr[t] = len(fired) > 0
			}
		}
	}
	m.logf("retention sweep at %s; faults %v", m.clock.Format("15:04"), fired)
	tc.Note(m.log[len(m.log)-1])
}

// storedTables are a generation's MergeTree tables in the world.
func (m *pubMachine) storedTables(k genKey) []string {
	var out []string
	for name, t := range m.w.tables {
		if t.stored && t.gen == k.gen && m.w.namespaceOfEndpoint(t.endpoint) == k.ns {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// RuleShutdown closes the publisher (sealing the open generations) and
// starts the next incarnation.
func (m *pubMachine) RuleShutdown(tc hegel.TestCase) {
	faulty := slices.Contains(m.o.faults, "seal") && hegel.Draw(tc, hegel.Booleans())
	if faulty {
		m.w.arm("seal", "fail")
	}
	var open []string
	for sig, st := range m.p.signals {
		if st.cur != nil {
			open = append(open, sig+":"+st.cur.id)
		}
	}
	err := m.p.close()
	fired := m.w.disarm()
	for _, o := range open {
		sig, gen, _ := strings.Cut(o, ":")
		m.rotated(sig, gen, fired)
	}
	m.logf("shutdown (seals %v; faults %v; err %v)", open, fired, err)
	tc.Note(m.log[len(m.log)-1])
	restart(m, tc)
}

// RuleCrash abandons the publisher without sealing anything, as a killed
// process does, and starts the next incarnation.
func (m *pubMachine) RuleCrash(tc hegel.TestCase) {
	m.p.release() // the process is gone, and its claims with it
	m.logf("crash")
	tc.Note(m.log[len(m.log)-1])
	restart(m, tc)
}

func restartWorld(m *pubMachine) {
	if !m.o.persistentPath {
		// A fresh catalog attaches nothing: there is nothing left to detach.
		m.w.crashLocal()
		m.detachDue, m.detachErr = map[string]bool{}, map[string]bool{}
	}
}

func restart(m *pubMachine, tc hegel.TestCase) {
	restartWorld(m)
	// Retries are exported again by the next incarnation: the queue is
	// persistent (file_storage).
	if err := m.start(); err != nil {
		tc.Errorf("restart: %v", err)
	}
	m.logf("start epoch %s", m.p.epoch)
}

// ---- invariants -----------------------------------------------------------------

func failf(m *pubMachine, tc hegel.TestCase, format string, args ...any) {
	tc.Errorf("%s\n\nhistory:\n  %s", fmt.Sprintf(format, args...), strings.Join(m.log, "\n  "))
}

// InvariantNothingForbidden: the world saw no DROP of an object-storage
// table, no overwritten batch manifest or Parquet object, no write into a
// sealed generation, no statement it does not understand.
func (m *pubMachine) InvariantNothingForbidden(tc hegel.TestCase) {
	if len(m.w.violations) > 0 {
		failf(m, tc, "forbidden: %s", m.w.violations[0])
	}
}

// InvariantCommitImpliesData: every batch manifest's rows are where it says:
// in its table (on the endpoint it names) and in its Parquet object.
func (m *pubMachine) InvariantCommitImpliesData(tc hegel.TestCase) {
	ms, _ := m.w.manifests()
	for _, mf := range ms {
		if m.cfg.StoreTables {
			if len(mf.Tables) == 0 {
				failf(m, tc, "manifest %s names no tables", mf.key)
				return
			}
			ep, n := mf.Tables[0].Endpoint, 0
			for _, t := range m.w.storedTables() {
				if t.endpoint == ep {
					for _, r := range t.rows {
						if r.epoch == mf.ProducerEpoch && r.batch == mf.BatchID {
							n++
						}
					}
				}
			}
			if n != mf.Rows {
				failf(m, tc, "manifest %s says %d rows at %s; that table holds %d of them", mf.key, mf.Rows, ep, n)
				return
			}
		}
		if m.cfg.parquet() {
			o := m.w.objects[mf.Parquet]
			if o == nil || len(o.rows) != mf.Rows {
				failf(m, tc, "manifest %s names Parquet %s, which is missing or not %d rows", mf.key, mf.Parquet, mf.Rows)
				return
			}
		}
	}
}

// InvariantSuccessIsCommitted: a push that returned nil has its manifest,
// in the generation of the clock at the push.
func (m *pubMachine) InvariantSuccessIsCommitted(tc hegel.TestCase) {
	ms, _ := m.w.manifests()
	have := map[string]batchManifest{}
	for _, mf := range ms {
		have[fmt.Sprintf("%s|%s|%d", mf.ProducerEpoch, mf.Signal, mf.BatchID)] = mf.batchManifest
	}
	for _, a := range m.attempts {
		if a.err != nil {
			continue
		}
		mf, ok := have[fmt.Sprintf("%s|%s|%d", a.epoch, a.signal, a.batch)]
		if !ok || mf.Generation != a.gen || mf.Rows != a.rows {
			failf(m, tc, "push of %s (%s batch %d, %d rows in %s) returned success; manifest %+v", a.payload, a.epoch, a.batch, a.rows, a.gen, mf)
			return
		}
	}
}

// InvariantBatchIDsUnique: a batch id names one push attempt in its
// namespace, and every manifest is for a batch some push allocated.
func (m *pubMachine) InvariantBatchIDsUnique(tc hegel.TestCase) {
	seen := map[string]string{}
	for _, a := range m.attempts {
		if a.batch == 0 {
			continue
		}
		k := fmt.Sprintf("%s|%s|%d", a.epoch, a.signal, a.batch)
		if p, dup := seen[k]; dup {
			failf(m, tc, "batch %s allocated to %s and %s", k, p, a.payload)
			return
		}
		seen[k] = a.payload
	}
	ms, _ := m.w.manifests()
	for _, mf := range ms {
		if _, ok := seen[fmt.Sprintf("%s|%s|%d", mf.ProducerEpoch, mf.Signal, mf.BatchID)]; !ok {
			failf(m, tc, "manifest %s is for no push attempt", mf.key)
			return
		}
	}
}

// InvariantSealsCount: _sealed.json states the batches and rows of its
// generation. By default: of the pushes that returned success (what the
// in-memory counters count, the design's intent). With strictSeal: of the
// generation's manifests (what a consumer reconciles against).
func (m *pubMachine) InvariantSealsCount(tc hegel.TestCase) {
	ms, ss := m.w.manifests()
	for _, s := range ss {
		var batches, rows uint64
		var what string
		if m.o.strictSeal {
			what = "manifests"
			for _, mf := range ms {
				if mf.ProducerEpoch == s.ProducerEpoch && mf.Signal == s.Signal && mf.Generation == s.Generation {
					batches++
					rows += uint64(mf.Rows)
				}
			}
		} else {
			what = "successful pushes"
			for _, a := range m.attempts {
				if a.err == nil && a.epoch == s.ProducerEpoch && a.signal == s.Signal && a.gen == s.Generation {
					batches++
					rows += uint64(a.rows)
				}
			}
		}
		if s.Batches != batches || s.Rows != rows {
			failf(m, tc, "%s says %d batches / %d rows; its generation's %s: %d / %d", s.key, s.Batches, s.Rows, what, batches, rows)
			return
		}
	}
}

// InvariantNoOrphanRows: rows in the published tables belong to committed
// batches. By default only a push that returned an error may leave rows
// behind (its retry is a new batch; consumers select manifested ids). With
// noOrphans: none at all, i.e. a reader attached live sees only committed
// batches.
func (m *pubMachine) InvariantNoOrphanRows(tc hegel.TestCase) {
	ms, _ := m.w.manifests()
	committed := map[string]bool{}
	for _, mf := range ms {
		committed[fmt.Sprintf("%s|%d", mf.ProducerEpoch, mf.BatchID)] = true
	}
	failed := map[string]bool{}
	for _, a := range m.attempts {
		if a.err != nil {
			failed[fmt.Sprintf("%s|%d", a.epoch, a.batch)] = true
		}
	}
	for _, t := range m.w.storedTables() {
		n := t.name
		for _, r := range t.rows {
			k := fmt.Sprintf("%s|%d", r.epoch, r.batch)
			if committed[k] || (!m.o.noOrphans && failed[k]) {
				continue
			}
			failf(m, tc, "%s holds rows of %s batch %d, which has no manifest", n, r.epoch, r.batch)
			return
		}
	}
}

// InvariantHousekeeping: every generation rotated or closed has its
// _sealed.json, and every sealed generation past local_retention is
// detached (never dropped). Where a seal or DETACH statement itself failed
// the default test excuses it (it is logged and not retried); with
// housekeeping set it does not.
func (m *pubMachine) InvariantHousekeeping(tc hegel.TestCase) {
	for k := range m.sealedAt {
		if !m.w.sealed(k.ns, k.gen) && (m.o.housekeeping || !m.sealFault[k]) {
			failf(m, tc, "generation %s of %s was rotated or closed but has no _sealed.json", k.gen, k.ns)
			return
		}
	}
	for name := range m.detachDue {
		t := m.w.tables[name]
		if t == nil || (t.state != "detached" && (m.o.housekeeping || !m.detachErr[name])) {
			state := "gone"
			if t != nil {
				state = t.state
			}
			failf(m, tc, "%s is past local_retention but %s", name, state)
			return
		}
	}
}

var pubInvariants = []string{"InvariantNothingForbidden", "InvariantCommitImpliesData", "InvariantSuccessIsCommitted",
	"InvariantBatchIDsUnique", "InvariantSealsCount", "InvariantNoOrphanRows", "InvariantHousekeeping"}

// runPublisherMachine is one test case.
func runPublisherMachine(tc hegel.TestCase, o pubOpts) {
	m := newPubMachine(tc, o)
	lastPubMachine.Store(m)
	// A test case ends like a crash: nothing is sealed, and the process-wide
	// claims on each signal go.
	defer func() {
		if m.p != nil {
			m.p.wg.Wait()
			m.p.release()
		}
	}()
	steps := o.steps
	if steps == 0 {
		steps = 30
	}
	hegel.RunStateful(tc, m, hegel.WithStatefulStepCount(steps), hegel.WithAlwaysCheckInvariants(pubInvariants...))
}

var allFaults = []string{"ddl", "insert", "parquet", "manifest", "seal", "drop", "optimize", "detach"}

// TestPBTPublisherStateMachine: the publishing protocol keeps its promises
// under arbitrary pushes, retries, faults (failing and ambiguous) in every
// statement it issues, clock moves, retention sweeps, shutdowns and crashes,
// with a fresh local catalog per incarnation: commit implies data, success
// implies commit, batch ids are unique, seals count the successful pushes,
// only failed pushes leave rows behind, nothing is dropped from object
// storage or written after its seal, and generations are sealed and
// detached unless the statement doing it failed.
func TestPBTPublisherStateMachine(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		runPublisherMachine(ht, pubOpts{faults: allFaults, ambiguous: true})
	}, pbtOpts(300)...)
}

// expectPublisherFinding runs the machine with o and expects a
// counterexample; it logs the shrunk command sequence and returns the
// machine that replayed it.
func expectPublisherFinding(t *testing.T, what string, o pubOpts, n int) *pubMachine {
	t.Helper()
	o.record = true
	expectFinding(t, what, func(tc hegel.TestCase) { runPublisherMachine(tc, o) }, pbtOpts(n)...)
	m := lastPubMachine.Load()
	if m.w.rec != nil {
		t.Logf("as model steps:\n%s", renderSteps(m.w.rec.steps))
	}
	return m
}

// TestPBTFindingSealUndercount (the model's F3): _sealed.json is built from
// in-memory counters that count pushes that returned success. A manifest
// write that lands but reports an error (ambiguous) is a committed batch the
// seal does not count.
func TestPBTFindingSealUndercount(t *testing.T) {
	m := expectPublisherFinding(t, "a seal that disagrees with its generation's manifests",
		pubOpts{faults: []string{"manifest"}, ambiguous: true, strictSeal: true}, 500)
	checkModelAgrees(t, m, "sealMatchesManifests")
}

// TestPBTFindingOrphanRows (the model's F2): a push whose table insert
// succeeded but whose Parquet or manifest write failed leaves rows in the
// published table with no manifest. A reader attached live (ReaderDDL,
// refresh_parts_interval) sees them unless it filters on manifested batch
// ids; the retry adds the same rows again under a new batch id.
func TestPBTFindingOrphanRows(t *testing.T) {
	expectPublisherFinding(t, "table rows without a manifest",
		pubOpts{faults: []string{"parquet", "manifest"}, noOrphans: true}, 500)
}

// TestPBTFindingClockRegressionReopensSealedGeneration: acquire computes the
// generation from p.now() before taking the lock, and rotates to whatever
// generation that is, backwards included. A wall clock that steps back
// across a boundary (or, without any clock step, a push that read the clock
// just before the boundary and reached the lock after a push that read it
// just after) reopens the previous generation: it writes into tables and a
// manifest directory that already have _sealed.json, re-seals it later
// with only the new batches, and seals the newer generation early. The
// Quint model's generation is a counter that only goes up, so it cannot
// produce this.
func TestPBTFindingClockRegressionReopensSealedGeneration(t *testing.T) {
	m := expectPublisherFinding(t, "a write into a sealed generation after the clock went back",
		pubOpts{clockBack: true}, 500)
	checkModelRejects(t, m)
}

// TestPBTFindingRestartReusesPreviousEpochTables: table names carry the
// generation but not the epoch, and openGeneration creates them with
// CREATE TABLE IF NOT EXISTS. With a persistent chDB path (the edge config
// sets path: ./data/chdb), a restart within the same generation finds the
// previous incarnation's table, still attached, and its DDL is a no-op: the
// new epoch's rows go to the OLD epoch's endpoint, into a generation that
// shutdown already sealed, while its manifests name the new epoch's endpoint,
// where nothing is. The previous incarnation's generations are also never
// detached. The model gives every epoch fresh tables by construction.
func TestPBTFindingRestartReusesPreviousEpochTables(t *testing.T) {
	m := expectPublisherFinding(t, "a restarted incarnation writing where its manifests do not point",
		pubOpts{persistentPath: true}, 500)
	checkModelAgrees(t, m, "")
}

// TestPBTFindingFailedSealIsNotRetried: a seal whose _sealed.json write
// fails is logged and dropped; the generation is still put on the detach
// list and nothing writes its seal later. A consumer sees a live epoch with
// an unsealed generation that will never be sealed. The same holds for a
// failed DETACH at retention: the generation stays attached for good. The
// model's sealGen cannot fail, so it has nothing to say about this.
func TestPBTFindingFailedSealIsNotRetried(t *testing.T) {
	expectPublisherFinding(t, "a rotated generation left without _sealed.json",
		pubOpts{faults: []string{"seal"}, housekeeping: true}, 500)
}

// checkModelAgrees validates the shrunk run's steps against edgePublish.qnt
// with quintgo: they must be a behaviour of the model (conform), and the
// model must itself violate invariant (if one is named) on them.
func checkModelAgrees(t *testing.T, m *pubMachine, invariant string) {
	t.Helper()
	rep, ok := quintgoValidate(t, m.w.rec.steps, t.TempDir())
	t.Logf("quintgo:\n%s", rep)
	if !strings.Contains(rep, "conformance: PASS") {
		t.Fatalf("the shrunk run is not a behaviour of the model")
	}
	if invariant == "" {
		if !ok {
			t.Errorf("the model finds a violation in a run it should accept")
		}
		return
	}
	if ok || !strings.Contains(rep, "invariant "+invariant+": FAIL") {
		t.Errorf("the model does not see %s violated", invariant)
	}
}

// checkModelRejects validates the shrunk run and expects it NOT to conform:
// the implementation did something the model cannot do.
func checkModelRejects(t *testing.T, m *pubMachine) {
	t.Helper()
	rep, _ := quintgoValidate(t, m.w.rec.steps, t.TempDir())
	t.Logf("quintgo:\n%s", rep)
	if !strings.Contains(rep, "conformance: FAIL") {
		t.Errorf("expected the model to reject the run")
	}
}

// TestPBTPublisherConformsToModel: runs of the default machine, recorded
// as model steps, are behaviours of edgePublish.qnt (currentDesign's flags)
// and satisfy commitImpliesData; quintgo replays each through quint.
// Needs PBT_QUINT=1, quint, and ../../quintgo; each validation is one
// `quint test` run of a few seconds.
func TestPBTPublisherConformsToModel(t *testing.T) {
	if os.Getenv("PBT_QUINT") == "" {
		t.Skip("set PBT_QUINT=1 to validate traces with quintgo (needs quint and ../../quintgo)")
	}
	var mu sync.Mutex
	var runs [][]qStep
	hegel.Test(t, func(ht *hegel.T) {
		// Faults that the model has (writes that fail or land ambiguously),
		// no seal faults (the model's seal cannot fail).
		runPublisherMachine(ht, pubOpts{faults: []string{"insert", "parquet", "manifest"}, ambiguous: true, record: true, steps: 12})
		mu.Lock()
		runs = append(runs, lastPubMachine.Load().w.rec.steps)
		mu.Unlock()
	}, pbtOpts(8, hegel.WithPhases(hegel.PhaseGenerate))...)
	for i, steps := range runs {
		rep, ok := quintgoValidate(t, steps, t.TempDir())
		conforms := strings.Contains(rep, "conformance: PASS")
		t.Logf("run %d: %d steps, conforms %v, invariants ok %v", i, len(steps), conforms, ok)
		if !conforms {
			t.Errorf("run %d is not a behaviour of the model:\n%s\n%s", i, renderSteps(steps), rep)
		}
	}
}
