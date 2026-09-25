package chdbexporter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

// Deterministic reproductions of what the publisher state machine found,
// run against real chDB and S3 (SeaweedFS) where the finding involves them,
// so that none of it rests on the fake sessions alike. A TestFinding* test
// PASSES while its defect is present and says so in its log; once a defect
// is fixed its test asserts the fixed behaviour instead (TestFixed*).

func randSuffix() string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// s3Publisher starts an exporter publishing logs to S3 tables (no Parquet)
// under the given database, producer and epoch.
func s3Publisher(t *testing.T, db, producer, epoch string) *chdbExporter {
	root, key, secret := s3Env(t)
	cfg := publishConfig(t, db, producer)
	cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.AccessKeyID, cfg.ObjectStorage.SecretAccessKey = root, key, configSecret(secret)
	cfg.ObjectStorage.SealOptimize = false
	cfg.Producer.Epoch = epoch
	return startExporter(t, cfg)
}

func s3Seal(t *testing.T, ns, gen string) sealManifest {
	root, key, secret := s3Env(t)
	raw := query(t, fmt.Sprintf("SELECT json FROM s3('%s/%s/manifests/%s/_sealed.json', '%s', '%s', 'JSONAsString') FORMAT TSVRaw", root, ns, gen, key, secret))
	var s sealManifest
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	return s
}

// TestFixedRestartWritesIntoItsOwnEpochTables: PBT finding 1, fixed. Two
// incarnations share a chDB path (as with path: ./data/chdb) and restart
// within one generation. Before the fix, table names carried only the
// generation, so the second one's CREATE TABLE IF NOT EXISTS found the
// first one's table: its rows went to the first epoch's endpoint, into a
// generation the first epoch had sealed, while its manifest pointed at an
// endpoint that held nothing. Table names now carry the epoch
// (tableSuffix).
func TestFixedRestartWritesIntoItsOwnEpochTables(t *testing.T) {
	sfx := randSuffix()
	ctx := context.Background()
	e1 := s3Publisher(t, "os_restart", "os-restart", "e1-"+sfx)
	if err := e1.pushLogs(ctx, testgen.Logs(10)); err != nil {
		t.Fatal(err)
	}
	g1 := e1.pub.signals[signalLogs].cur
	gen1, ns1, table1 := g1.id, e1.pub.namespace(signalLogs), g1.ts.stored[0]
	if err := e1.shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	e2 := s3Publisher(t, "os_restart", "os-restart", "e2-"+sfx)
	if err := e2.pushLogs(ctx, testgen.Logs(20)); err != nil {
		t.Fatal(err)
	}
	g2 := e2.pub.signals[signalLogs].cur
	gen2, ns2, table2 := g2.id, e2.pub.namespace(signalLogs), g2.ts.stored[0]
	if err := e2.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if gen1 != gen2 {
		t.Skipf("the restart crossed a generation boundary (%s, %s); run again", gen1, gen2)
	}

	root, key, secret := s3Env(t)
	ms := s3Manifests(t, root, ns2, key, secret)
	if len(ms) != 1 {
		t.Fatalf("second epoch manifests: %+v", ms)
	}
	ep := ms[0].Tables[0].Endpoint
	objects := query(t, fmt.Sprintf("SELECT count() FROM s3('%s**', '%s', '%s', 'One') SETTINGS s3_throw_on_zero_files_match = 0", ep, key, secret))
	byEpoch := func(table string) string {
		return query(t, fmt.Sprintf("SELECT groupArray((producer_epoch, c)) FROM (SELECT producer_epoch, count() c FROM %s GROUP BY 1 ORDER BY 1)", table))
	}
	in1, in2 := byEpoch(table1), byEpoch(table2)
	seal1 := s3Seal(t, ns1, gen1)
	t.Logf("tables %s and %s; the second epoch's manifest names %s: %s objects there", table1, table2, ep, objects)
	t.Logf("%s holds %s; %s holds %s; the first epoch's seal says %d batches, %d rows", table1, in1, table2, in2, seal1.Batches, seal1.Rows)
	if table1 == table2 {
		t.Fatalf("both incarnations use table %s", table1)
	}
	if objects == "0" || in1 != "[('e1-"+sfx+"',10)]" || in2 != "[('e2-"+sfx+"',20)]" || seal1.Batches != 1 || seal1.Rows != 10 {
		t.Fatalf("an incarnation wrote outside its own tables")
	}
}

// ambiguousSession applies one statement matching match and then reports an
// error, like an S3 PUT whose response is lost.
type ambiguousSession struct {
	session
	mu    *sync.Mutex
	match func(q string) bool
	left  *int
}

func (s ambiguousSession) Insert(q, format string, data []byte) error {
	err := s.session.Insert(q, format, data)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil && *s.left > 0 && s.match(q) {
		*s.left--
		return errors.New("injected: applied, response lost")
	}
	return err
}

func wrapSessions(e *chdbExporter, wrap func(session) session) {
	n := len(e.all)
	for i := 0; i < n; i++ {
		<-e.conns
	}
	for i, s := range e.all {
		e.all[i] = wrap(s)
		e.conns <- e.all[i]
	}
}

// TestFindingSealUndercountOnS3 reproduces the model's F3 (and
// TestPBTFindingSealUndercount) on chDB and S3: a batch manifest write that
// lands but reports an error is a committed batch that _sealed.json, built
// from in-memory counters, does not count.
func TestFindingSealUndercountOnS3(t *testing.T) {
	ctx := context.Background()
	e := s3Publisher(t, "os_undercount", "os-undercount", "e-"+randSuffix())
	left := 1
	wrapSessions(e, func(s session) session {
		return ambiguousSession{session: s, mu: &sync.Mutex{}, left: &left, match: func(q string) bool {
			return strings.Contains(q, "/manifests/") && strings.Contains(q, "'RawBLOB'") && !strings.Contains(q, "_sealed.json")
		}}
	})
	if err := e.pushLogs(ctx, testgen.Logs(10)); err == nil {
		t.Fatal("the push should have reported the injected error")
	}
	gen, ns := e.pub.signals[signalLogs].cur.id, e.pub.namespace(signalLogs)
	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	root, key, secret := s3Env(t)
	ms := s3Manifests(t, root, ns, key, secret)
	seal := s3Seal(t, ns, gen)
	t.Logf("%d batch manifest(s) in %s; _sealed.json says %d batches, %d rows", len(ms), gen, seal.Batches, seal.Rows)
	if len(ms) != 1 || seal.Batches != 0 {
		t.Fatalf("the defect did not reproduce")
	}
}

// TestFixedClockRegressionOnS3: PBT finding 2, fixed, on chDB and S3.
// Before the fix a clock step back reopened the sealed generation, and its
// second seal counted only the batches written after the reopening. Now
// generations only move forward: after the step back, pushes stay in the
// current generation, and g10's seal is left as it was.
func TestFixedClockRegressionOnS3(t *testing.T) {
	ctx := context.Background()
	e := s3Publisher(t, "os_clockback", "os-clockback", "e-"+randSuffix())
	clock := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	e.pub.now = func() time.Time { return clock }
	push := func() {
		t.Helper()
		if err := e.pushLogs(ctx, testgen.Logs(5)); err != nil {
			t.Fatal(err)
		}
		e.pub.wg.Wait()
	}
	push() // batch 1 in g10
	table10 := e.pub.signals[signalLogs].cur.ts.stored[0]
	clock = clock.Add(time.Hour)
	push() // batch 2 in g11; g10 rotated and sealed
	ns := e.pub.namespace(signalLogs)
	first := s3Seal(t, ns, "g20260924T100000")
	clock = clock.Add(-time.Hour)
	push() // batch 3: the clock says g10, but g11 is current and stays so
	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	root, key, secret := s3Env(t)
	perGen := map[string][]uint64{}
	for _, m := range s3Manifests(t, root, ns, key, secret) {
		perGen[m.Generation] = append(perGen[m.Generation], m.BatchID)
	}
	last := s3Seal(t, ns, "g20260924T100000")
	seal11 := s3Seal(t, ns, "g20260924T110000")
	rows := query(t, "SELECT count() FROM "+table10)
	t.Logf("g10: first seal %d batches [%d..%d], final seal %d batches [%d..%d], table rows %s; g11 seal %d batches; manifests %v",
		first.Batches, first.FirstBatch, first.LastBatch, last.Batches, last.FirstBatch, last.LastBatch, rows, seal11.Batches, perGen)
	if len(perGen["g20260924T100000"]) != 1 || len(perGen["g20260924T110000"]) != 2 || last.Batches != 1 || last.FirstBatch != 1 ||
		last.SealedAt != first.SealedAt || seal11.Batches != 2 || rows != "5" {
		t.Fatalf("the clock step back moved a generation backwards")
	}
}

// TestFixedStaleClockReadReopensGeneration: PBT finding 2's race, fixed.
// Before the fix: the same reopening with a clock that never goes back. acquire reads p.now() before it takes the
// lock, so a push that read the time just before a generation boundary can
// reach the lock after another push that read it just after and rotated.
// The late push then "rotates" back to the old generation, and, since
// acquire re-reads the clock when it retries, forward again at once: the old
// generation is sealed a second time from a new, empty generation struct,
// so its _sealed.json is overwritten with zero batches, and the current one
// is sealed while current, which drops its staging table under the pushes
// still writing to it (a race; when the DROP lands after the re-CREATE,
// every push fails with "table ..._in does not exist" until the next
// rotation). Here the
// interleaving is forced by blocking the first push inside its clock read;
// in production it needs a descheduling between two instructions near the
// boundary, rare per push but not per fleet-year. Runs on the fake sessions.
// Now the clock is read under the lock and a generation never goes back, so
// the late push lands in g11 and g10's seal matches its manifests.
func TestFixedStaleClockReadReopensGeneration(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Database, cfg.Producer.ID, cfg.Producer.Region, cfg.Producer.Epoch = "race", "edge", "r1", "race-"+randSuffix()
	cfg.ObjectStorage.Endpoint = "http://s3.fake/otel"
	cfg.Connections = 2
	w := newFakeWorld(cfg)
	e := newExporter(zap.NewNop(), cfg, signalLogs)
	e.conns = make(chan session, 2)
	for i := 0; i < 2; i++ {
		e.all = append(e.all, fakeSession{w})
		e.conns <- fakeSession{w}
	}
	p, err := newPublisher(e)
	if err != nil {
		t.Fatal(err)
	}
	e.pub = p
	defer p.close()

	before := time.Date(2026, 9, 24, 10, 59, 59, 999_000_000, time.UTC)
	after := before.Add(2 * time.Millisecond)
	var mu sync.Mutex
	reading, release := make(chan struct{}), make(chan struct{})
	stale := false // the next clock read is the one that stalls
	push := func() error {
		ld := testgen.Logs(1)
		return p.push(signalLogs, func(w rowWriter, env *envelope) int { return writeLogs(w, ld, env) })
	}
	// g10 is open.
	p.now = func() time.Time { return before }
	if err := push(); err != nil {
		t.Fatal(err)
	}
	p.now = func() time.Time {
		mu.Lock()
		s := stale
		stale = false
		mu.Unlock()
		if s {
			close(reading)
			<-release
			return before
		}
		return after
	}
	// B reads the clock (10:59:59.999). Before the fix it read it before
	// taking the lock; now it reads it holding the lock shared, so A, which
	// reads 11:00:00.001, cannot rotate until B has written into g10.
	mu.Lock()
	stale = true
	mu.Unlock()
	doneB, doneA := make(chan error), make(chan error)
	go func() { doneB <- push() }()
	<-reading
	go func() { doneA <- push() }()
	time.Sleep(50 * time.Millisecond) // let A reach the lock
	close(release)
	errB, errA := <-doneB, <-doneA
	p.wg.Wait()
	if errA != nil {
		t.Fatal(errA)
	}
	ms, ss := w.manifests()
	perGen := map[string]int{}
	for _, m := range ms {
		perGen[m.Generation]++
	}
	var g10 *fakeSeal
	for i := range ss {
		t.Logf("%s: %d batches; manifests in it: %d", ss[i].Generation, ss[i].Batches, perGen[ss[i].Generation])
		if ss[i].Generation == "g20260924T100000" {
			g10 = &ss[i]
		}
	}
	t.Logf("B's push: %v", errB)
	if errB != nil || g10 == nil || int(g10.Batches) != perGen["g20260924T100000"] || perGen["g20260924T100000"] != 2 ||
		perGen["g20260924T110000"] != 1 || len(ss) != 1 {
		t.Fatalf("a push reopened or re-sealed a generation")
	}
}
