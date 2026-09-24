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
// so that none of it rests on the fake sessions alike. Each test PASSES
// while the defect is present and says so in its log; a fix makes it fail,
// and it should then assert the fixed behaviour instead.

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

// TestFindingRestartWritesIntoPreviousEpochTable reproduces
// TestPBTFindingRestartReusesPreviousEpochTables on chDB and S3. Two
// incarnations share a chDB path (as with path: ./data/chdb) and restart
// within one generation. The second one's CREATE TABLE IF NOT EXISTS finds
// the first one's table, so its rows go to the first epoch's endpoint, into
// a generation the first epoch has sealed, while its manifest points at an
// endpoint that holds nothing.
func TestFindingRestartWritesIntoPreviousEpochTable(t *testing.T) {
	sfx := randSuffix()
	ctx := context.Background()
	e1 := s3Publisher(t, "os_restart", "os-restart", "e1-"+sfx)
	if err := e1.pushLogs(ctx, testgen.Logs(10)); err != nil {
		t.Fatal(err)
	}
	gen1, ns1 := e1.pub.signals[signalLogs].cur.id, e1.pub.namespace(signalLogs)
	if err := e1.shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	e2 := s3Publisher(t, "os_restart", "os-restart", "e2-"+sfx)
	if err := e2.pushLogs(ctx, testgen.Logs(20)); err != nil {
		t.Fatal(err)
	}
	gen2, ns2 := e2.pub.signals[signalLogs].cur.id, e2.pub.namespace(signalLogs)
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
	byEpoch := query(t, fmt.Sprintf("SELECT groupArray((producer_epoch, c)) FROM (SELECT producer_epoch, count() c FROM os_restart.%s_%s GROUP BY 1 ORDER BY 1)",
		"otel_logs", gen1))
	seal1 := s3Seal(t, ns1, gen1)
	t.Logf("second epoch's manifest names %s: %s objects there", ep, objects)
	t.Logf("table os_restart.otel_logs_%s (created by the first epoch, on its endpoint) holds %s", gen1, byEpoch)
	t.Logf("the first epoch's _sealed.json for %s says %d batches, %d rows", gen1, seal1.Batches, seal1.Rows)
	if objects != "0" || !strings.Contains(byEpoch, "'e2-"+sfx+"',20") {
		t.Fatalf("the defect did not reproduce: the second epoch wrote where its manifest points")
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

// TestFindingClockRegressionOnS3 reproduces
// TestPBTFindingClockRegressionReopensSealedGeneration on chDB and S3, and
// shows its worst consequence: the reopened generation is sealed a second
// time, and the new _sealed.json counts only the batches written after the
// reopening, so the seal now disowns the generation's first batch.
func TestFindingClockRegressionOnS3(t *testing.T) {
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
	clock = clock.Add(time.Hour)
	push() // batch 2 in g11; g10 rotated and sealed
	ns := e.pub.namespace(signalLogs)
	first := s3Seal(t, ns, "g20260924T100000")
	clock = clock.Add(-time.Hour)
	push() // batch 3: back into g10, which is sealed; g11 is sealed early
	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	root, key, secret := s3Env(t)
	var in10 []uint64
	for _, m := range s3Manifests(t, root, ns, key, secret) {
		if m.Generation == "g20260924T100000" {
			in10 = append(in10, m.BatchID)
		}
	}
	last := s3Seal(t, ns, "g20260924T100000")
	rows := query(t, "SELECT count() FROM os_clockback.otel_logs_g20260924T100000")
	t.Logf("g10: first seal %d batches [%d..%d]; batch manifests %v; final seal %d batches [%d..%d]; table rows %s",
		first.Batches, first.FirstBatch, first.LastBatch, in10, last.Batches, last.FirstBatch, last.LastBatch, rows)
	if len(in10) != 2 || last.Batches != 1 || last.FirstBatch != 3 {
		t.Fatalf("the defect did not reproduce")
	}
}

// TestFindingStaleClockReadReopensGeneration: the same reopening with a
// clock that never goes back. acquire reads p.now() before it takes the
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
func TestFindingStaleClockReadReopensGeneration(t *testing.T) {
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
	// B reads the clock (10:59:59.999) and stalls before taking the lock.
	mu.Lock()
	stale = true
	mu.Unlock()
	done := make(chan error)
	go func() { done <- push() }()
	<-reading
	// A reads 11:00:00.001, rotates to g11, seals g10, commits.
	if err := push(); err != nil {
		t.Fatal(err)
	}
	p.wg.Wait()
	close(release)
	errB := <-done
	p.wg.Wait()
	// B rotated g11 -> g10 (sealing g11 early), then, re-reading the clock
	// in acquire's recursive call, g10 -> g11 again (sealing g10 a second
	// time from a fresh, empty generation struct).
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
	if g10 == nil || int(g10.Batches) == perGen["g20260924T100000"] {
		t.Fatalf("the defect did not reproduce")
	}
}
