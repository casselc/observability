// Package publisher is a stand-in for the chdb exporter's publishing path
// (spike/otel-chdb/chdbexporter/publish.go) with the same control flow:
// acquire the current generation under a shared lock (rotating under the
// exclusive lock when the clock has moved on), allocate a batch id, write
// table rows, then a Parquet object, then the manifest that commits the batch,
// and seal a rotated generation in the background from in-memory counters.
//
// What differs from publish.go, and why:
//   - the writes go through a Store interface instead of chDB sessions, so a
//     test can inject failures (including ambiguous ones) and hold a write
//     to force an interleaving;
//   - the three writes, the batch start and the rotation are small functions,
//     because //quint:action annotates functions (see ../README.md);
//   - rotation records itself before the background seal starts (publish.go
//     starts the seal goroutine, then sets st.cur; both under the exclusive
//     lock, so nothing observable differs);
//   - one signal per publisher; no table DDL, retention or detach;
//   - Tick, which publish.go does not have (see Tick);
//   - Mutation switches on deliberate protocol bugs for the demo. The zero
//     value is the faithful behaviour.
//
// The //quint:action and //quint:thread comments do nothing in a plain
// `go build`. Under `orchestrion go build` they record model steps.
package publisher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Store is the effect seam: where table rows, Parquet objects and manifests go.
type Store interface {
	InsertTable(ctx context.Context, table string, batch uint64, rows []byte) error
	PutObject(ctx context.Context, key string, data []byte) error
}

// Mutation is a deliberate protocol bug, for showing that the validator
// rejects a non-conforming implementation.
type Mutation int

const (
	Faithful            Mutation = iota
	ManifestBeforeTable          // commit first, then write the data
	NoRotationLock               // drop the shared lock before the writes, so rotation does not wait for pushes
)

type Config struct {
	Producer, Region, Signal string
	Epoch                    string        // producer incarnation; default: time + random
	Generation               time.Duration // generation length
	Now                      func() time.Time
	Mutation                 Mutation
}

type generation struct {
	id            string
	start         time.Time
	batches, rows atomic.Uint64
	first, last   atomic.Uint64
}

type signalState struct {
	mu     sync.RWMutex // pushes hold it shared for the whole write; rotation exclusively
	cur    *generation
	sealed []*generation
}

// Publisher publishes one signal for one producer incarnation.
type Publisher struct {
	cfg   Config
	epoch string
	store Store
	st    signalState
	seq   atomic.Uint64 // batch ids for this namespace (publish.go: batchSeq)
	wg    sync.WaitGroup
}

func New(cfg Config, store Store) *Publisher {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Generation == 0 {
		cfg.Generation = time.Minute
	}
	ep := cfg.Epoch
	if ep == "" {
		ep = time.Now().UTC().Format("20060102T150405Z")
	}
	return &Publisher{cfg: cfg, epoch: ep, store: store}
}

// Actor names this publisher's model instance: one writer per producer and signal.
func (p *Publisher) actor() string { return p.cfg.Producer + "/" + p.cfg.Signal }

func (p *Publisher) namespace() string {
	return fmt.Sprintf("%s/%s/%s/%s", p.cfg.Region, p.cfg.Signal, p.cfg.Producer, p.epoch)
}

func genID(t time.Time, d time.Duration) (string, time.Time) {
	start := t.UTC().Truncate(d)
	return "g" + start.Format("20060102T150405"), start
}

// acquire returns the current generation with its lock held shared,
// rotating first if the clock has moved into a new generation.
func (p *Publisher) acquire(ctx context.Context) (*generation, func(), error) {
	st := &p.st
	id, start := genID(p.cfg.Now(), p.cfg.Generation)
	st.mu.RLock()
	if st.cur != nil && st.cur.id == id {
		return st.cur, st.mu.RUnlock, nil
	}
	st.mu.RUnlock()

	st.mu.Lock()
	if st.cur == nil || st.cur.id != id {
		g := &generation{id: id, start: start}
		if old := st.cur; old != nil {
			p.rotate(ctx, old, g)
			p.sealInBackground(old)
		} else {
			st.cur = g
		}
	}
	st.mu.Unlock()
	return p.acquire(ctx)
}

// rotate makes next current; old takes no more batches. Called with the
// exclusive lock held (by acquire, or by Close with next == nil).
//
//quint:action action:rotateGen actor:p.actor() process:p.epoch gen:old.id
func (p *Publisher) rotate(ctx context.Context, old, next *generation) {
	p.st.cur = next
}

func (p *Publisher) sealInBackground(g *generation) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		_ = p.seal(context.Background(), g)
		p.st.mu.Lock()
		p.st.sealed = append(p.st.sealed, g)
		p.st.mu.Unlock()
	}()
}

type batch struct {
	id      uint64
	gen     *generation
	payload string
}

type payloadKey struct{}

// WithPayload names the request being exported (the collector queue item a
// retry re-sends). The model tracks requests across retries by this name.
func WithPayload(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, payloadKey{}, id)
}

// PayloadOf returns the request name set by WithPayload.
func PayloadOf(ctx context.Context) string {
	s, _ := ctx.Value(payloadKey{}).(string)
	return s
}

// Push writes one batch: table insert, then Parquet object, then manifest.
//
//quint:thread
func (p *Publisher) Push(ctx context.Context, rows []byte) error {
	b, unlock, err := p.begin(ctx)
	if err != nil {
		return err
	}
	if p.cfg.Mutation == NoRotationLock {
		unlock() // BUG: rotation no longer waits for this push
	} else {
		defer unlock()
	}
	if p.cfg.Mutation == ManifestBeforeTable {
		// BUG: the batch is announced before its data exists.
		if err := p.commit(ctx, b, rows); err != nil {
			return err
		}
		if err := p.insertTable(ctx, b, rows); err != nil {
			return err
		}
		if err := p.writeParquet(ctx, b, rows); err != nil {
			return err
		}
	} else {
		if err := p.insertTable(ctx, b, rows); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		if err := p.writeParquet(ctx, b, rows); err != nil {
			return fmt.Errorf("parquet: %w", err)
		}
		if err := p.commit(ctx, b, rows); err != nil {
			return fmt.Errorf("manifest: %w", err)
		}
	}
	g := b.gen
	g.batches.Add(1)
	g.rows.Add(uint64(len(rows)))
	for {
		f := g.first.Load()
		if (f != 0 && f <= b.id) || g.first.CompareAndSwap(f, b.id) {
			break
		}
	}
	for {
		l := g.last.Load()
		if b.id <= l || g.last.CompareAndSwap(l, b.id) {
			break
		}
	}
	return nil
}

// begin acquires the generation and allocates the batch id. Batch ids come
// from an atomic counter, so their order is the order pushes started, even
// when the steps are recorded in another order: order.batch says so.
//
//quint:action action:pushStart when:ok actor:p.actor() process:p.epoch payload:PayloadOf(ctx) batch:b.id gen:b.gen.id order.batch:b.id
func (p *Publisher) begin(ctx context.Context) (b *batch, unlock func(), err error) {
	g, unlock, err := p.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	return &batch{id: p.seq.Add(1), gen: g, payload: PayloadOf(ctx)}, unlock, nil
}

//quint:action action:writeTable actor:p.actor() process:p.epoch payload:b.payload batch:b.id gen:b.gen.id
func (p *Publisher) insertTable(ctx context.Context, b *batch, rows []byte) error {
	return p.store.InsertTable(ctx, p.namespace()+"/"+b.gen.id+"/"+p.cfg.Signal, b.id, rows)
}

//quint:action action:writeParquet actor:p.actor() process:p.epoch payload:b.payload batch:b.id gen:b.gen.id
func (p *Publisher) writeParquet(ctx context.Context, b *batch, rows []byte) error {
	return p.store.PutObject(ctx, fmt.Sprintf("%s/%s/%020d.parquet", p.namespace(), b.gen.id, b.id), rows)
}

//quint:action action:writeManifest actor:p.actor() process:p.epoch payload:b.payload batch:b.id gen:b.gen.id
func (p *Publisher) commit(ctx context.Context, b *batch, rows []byte) error {
	m := fmt.Sprintf(`{"generation":%q,"batch_id":%d,"rows":%d}`, b.gen.id, b.id, len(rows))
	return p.store.PutObject(ctx, fmt.Sprintf("%s/manifests/%s/%020d.json", p.namespace(), b.gen.id, b.id), []byte(m))
}

// seal writes _sealed.json from the in-memory counters (as publish.go does).
//
//quint:action action:sealGen actor:p.actor() process:p.epoch gen:g.id obs.batches:g.batches.Load()
func (p *Publisher) seal(ctx context.Context, g *generation) error {
	m := fmt.Sprintf(`{"generation":%q,"batches":%d,"rows":%d,"first_batch":%d,"last_batch":%d}`,
		g.id, g.batches.Load(), g.rows.Load(), g.first.Load(), g.last.Load())
	return p.store.PutObject(ctx, fmt.Sprintf("%s/manifests/%s/_sealed.json", p.namespace(), g.id), []byte(m))
}

// Tick opens the first generation, or rotates if the clock has moved into a
// new one. publish.go rotates only when a push arrives, so a generation with
// no later traffic stays open until Close; the model's rotateGen needs no
// push, and a model-based test drives it through Tick.
func (p *Publisher) Tick(ctx context.Context) error {
	_, unlock, err := p.acquire(ctx)
	if err == nil {
		unlock()
	}
	return err
}

// Drain waits for background seals (test support: a model-based driver
// simulating a crash lets the dead incarnation's work finish first).
func (p *Publisher) Drain() { p.wg.Wait() }

// Close seals the open generation and waits for background seals.
func (p *Publisher) Close(ctx context.Context) error {
	p.st.mu.Lock()
	g := p.st.cur
	if g != nil {
		p.rotate(ctx, g, nil)
	}
	p.st.mu.Unlock()
	var err error
	if g != nil {
		err = p.seal(ctx, g)
	}
	p.wg.Wait()
	return err
}
