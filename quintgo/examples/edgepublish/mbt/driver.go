// Package mbt drives the stand-in publisher from edgePublish.qnt traces
// (model-based testing: capability 1). It is the per-project half of
// quintgo/connect, the Go analogue of a quint-connect Driver:
//
//   - every store write blocks in a gate until the driver decides its fate,
//     so the model's interleaving of pushes, S3 outcomes, rotations and
//     seals is reproduced exactly;
//   - after each step the store's contents (table rows, Parquet objects,
//     manifests, seals) are compared with the model's variables.
//
// Consumer actions (claim, insert, ...) have no implementation here and are
// skipped; the model's writer state does not depend on them.
package mbt

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chucklehead-dev/oscope/spike/quintgo/connect"
	"github.com/chucklehead-dev/oscope/spike/quintgo/examples/edgepublish/publisher"
	"github.com/chucklehead-dev/oscope/spike/quintgo/itf"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
)

const wait = 2 * time.Second // how long the implementation may take to reach its next write

type gate struct {
	op      publisher.Op
	release chan publisher.Fault
}

type pushKey struct{ epoch, batch int64 }
type genKey struct{ epoch, gen int64 }

type inflight struct {
	gate *gate // where it is blocked now
	done chan error
}

// Driver is the edgePublish writer driver.
type Driver struct {
	Mutation publisher.Mutation
	// Recorder, if set, is the global qobs recorder: a crash turns it off
	// while the dead incarnation's in-flight work drains, since a crashed
	// process reports nothing.
	Recorder *qobs.Recorder

	mu       sync.Mutex
	now      time.Time
	store    *publisher.MemStore
	pub      *publisher.Publisher
	epoch    int64
	arrivals chan *gate
	pushes   map[pushKey]*inflight
	seals    map[genKey]*gate
	gens     map[string]genKey // implementation generation id (with epoch) -> model generation
}

func (d *Driver) clock() time.Time { d.mu.Lock(); defer d.mu.Unlock(); return d.now }

// Connect returns the generic driver bound to this implementation.
func (d *Driver) Connect() *connect.Driver {
	return &connect.Driver{
		Init: d.init,
		Actions: map[string]connect.Handler{
			"pushStart":     d.pushStart,
			"writeTable":    func(s *connect.Step) error { return d.write(s, "table") },
			"writeParquet":  func(s *connect.Step) error { return d.write(s, "parquet") },
			"writeManifest": func(s *connect.Step) error { return d.write(s, "manifest") },
			"rotateGen":     d.rotateGen,
			"sealGen":       d.sealGen,
			"crash":         d.crash,
		},
		Skip:  map[string]bool{"claim": true, "skipDuplicate": true, "insert": true, "ack": true, "workerCrash": true, "otherInsert": true},
		Check: d.check,
	}
}

func (d *Driver) init(map[string]itf.Value) error {
	d.now = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	d.store = publisher.NewMemStore()
	d.arrivals = make(chan *gate, 16)
	d.store.Hook = func(_ context.Context, op publisher.Op) publisher.Fault {
		g := &gate{op: op, release: make(chan publisher.Fault)}
		d.arrivals <- g
		return <-g.release
	}
	d.pushes = map[pushKey]*inflight{}
	d.seals = map[genKey]*gate{}
	d.gens = map[string]genKey{}
	d.epoch = 0
	return d.start()
}

// start a new writer incarnation and open its first generation (the model's
// writer starts in generation 1).
func (d *Driver) start() error {
	d.epoch++
	d.pub = publisher.New(publisher.Config{Producer: "edge-1", Region: "eu", Signal: "traces",
		Epoch: fmt.Sprintf("e%d", d.epoch), Generation: time.Minute, Now: d.clock, Mutation: d.Mutation}, d.store)
	return d.pub.Tick(context.Background())
}

var (
	reNS    = regexp.MustCompile(`^eu/traces/edge-1/e(\d+)/`)
	reBatch = regexp.MustCompile(`/(g[0-9T]+)/(\d{20})\.(parquet|json)$`)
	reSeal  = regexp.MustCompile(`/manifests/(g[0-9T]+)/_sealed\.json$`)
	reTable = regexp.MustCompile(`/(g[0-9T]+)/traces$`)
)

// classify says what a write is: table, parquet, manifest or seal, and for
// which epoch, implementation generation and batch.
func classify(op publisher.Op) (kind string, epoch int64, gen string, batch int64) {
	if m := reNS.FindStringSubmatch(op.Key); m != nil {
		epoch, _ = strconv.ParseInt(m[1], 10, 64)
	}
	switch {
	case op.Kind == "table":
		if m := reTable.FindStringSubmatch(op.Key); m != nil {
			gen = m[1]
		}
		return "table", epoch, gen, int64(op.Batch)
	case reSeal.MatchString(op.Key):
		return "seal", epoch, reSeal.FindStringSubmatch(op.Key)[1], 0
	}
	if m := reBatch.FindStringSubmatch(op.Key); m != nil {
		b, _ := strconv.ParseInt(m[2], 10, 64)
		if m[3] == "parquet" {
			return "parquet", epoch, m[1], b
		}
		return "manifest", epoch, m[1], b
	}
	return "unknown", epoch, "", 0
}

func (d *Driver) nextArrival() (*gate, error) {
	select {
	case g := <-d.arrivals:
		return g, nil
	case <-time.After(wait):
		return nil, fmt.Errorf("the implementation made no write within %v (blocked?)", wait)
	}
}

// learnGen records which model generation an implementation generation is.
func (d *Driver) learnGen(epoch int64, implGen string, model genKey) error {
	k := fmt.Sprintf("e%d/%s", epoch, implGen)
	if have, ok := d.gens[k]; ok && have != model {
		return fmt.Errorf("implementation generation %s is model generation %v, but was %v", k, model, have)
	}
	d.gens[k] = model
	return nil
}

func push(v itf.Value) (p, epoch, batch, gen int64, phase string) {
	return v.Get("payload").Int64(), v.Get("bk").Get("epoch").Int64(), v.Get("bk").Get("batch").Int64(),
		v.Get("gk").Get("gen").Int64(), v.Get("phase").Tag
}

func (d *Driver) pushStart(s *connect.Step) error {
	// The model chose the batch id and generation; find the new push.
	before := map[string]bool{}
	for _, x := range s.Before["writer"].Get("pushes").Elems {
		before[x.String()] = true
	}
	var x itf.Value
	for _, y := range s.After["writer"].Get("pushes").Elems {
		if !before[y.String()] {
			x = y
		}
	}
	p, epoch, batch, gen, _ := push(x)
	fl := &inflight{done: make(chan error, 1)}
	ctx := publisher.WithPayload(context.Background(), strconv.FormatInt(p, 10))
	go func() { fl.done <- d.pub.Push(ctx, []byte(fmt.Sprintf("rows of %d", p))) }()
	g, err := d.nextArrival()
	if err != nil {
		return err
	}
	kind, ie, igen, ib := classify(g.op)
	if kind != "table" || ie != epoch || ib != batch {
		g.release <- publisher.Fail
		return fmt.Errorf("model started batch %d of epoch %d, expecting its table insert; the implementation wrote %s %s (epoch %d, batch %d)", batch, epoch, kind, g.op.Key, ie, ib)
	}
	if err := d.learnGen(epoch, igen, genKey{epoch, gen}); err != nil {
		return err
	}
	fl.gate = g
	d.pushes[pushKey{epoch, batch}] = fl
	return nil
}

var faults = map[string]publisher.Fault{"Ok": publisher.Apply, "Fail": publisher.Fail, "Ambiguous": publisher.Ambiguous}

func (d *Driver) write(s *connect.Step, want string) error {
	_, epoch, batch, _, _ := push(s.Pick("x"))
	o := s.Pick("o").Tag
	fl := d.pushes[pushKey{epoch, batch}]
	if fl == nil || fl.gate == nil {
		return fmt.Errorf("no in-flight push for batch %d of epoch %d", batch, epoch)
	}
	if kind, _, _, _ := classify(fl.gate.op); kind != want {
		return fmt.Errorf("model writes the %s of batch %d; the implementation is writing its %s (%s)", want, batch, kind, fl.gate.op.Key)
	}
	fl.gate.release <- faults[o]
	fl.gate = nil
	if o == "Ok" && want != "manifest" {
		g, err := d.nextArrival()
		if err != nil {
			return err
		}
		kind, ie, _, ib := classify(g.op)
		if ie != epoch || ib != batch {
			g.release <- publisher.Fail
			return fmt.Errorf("after the %s of batch %d the implementation wrote %s for batch %d", want, batch, g.op.Key, ib)
		}
		_ = kind // checked by the next write step
		fl.gate = g
		return nil
	}
	select {
	case err := <-fl.done:
		delete(d.pushes, pushKey{epoch, batch})
		if o == "Ok" && err != nil {
			return fmt.Errorf("the model commits batch %d; the implementation's push failed: %v", batch, err)
		}
		if o != "Ok" && err == nil {
			return fmt.Errorf("the write failed (%s) but the push reported success", o)
		}
		return nil
	case g := <-d.arrivals:
		g.release <- publisher.Fail
		return fmt.Errorf("the model's push of batch %d is over (%s %s); the implementation went on to write %s", batch, want, o, g.op.Key)
	case <-time.After(wait):
		return fmt.Errorf("push of batch %d did not return", batch)
	}
}

func (d *Driver) rotateGen(s *connect.Step) error {
	w := s.Before["writer"]
	old := genKey{w.Get("epoch").Int64(), w.Get("gen").Int64()}
	d.mu.Lock()
	d.now = d.now.Add(time.Minute)
	d.mu.Unlock()
	tick := make(chan error, 1)
	go func() { tick <- d.pub.Tick(context.Background()) }()
	select {
	case err := <-tick:
		if err != nil {
			return err
		}
	case <-time.After(wait):
		return fmt.Errorf("rotation did not complete (waiting for an in-flight push?)")
	}
	g, err := d.nextArrival() // the background seal of the old generation
	if err != nil {
		return err
	}
	kind, ie, igen, _ := classify(g.op)
	if kind != "seal" || ie != old.epoch {
		g.release <- publisher.Fail
		return fmt.Errorf("expected the seal of generation %v, got a write of %s", old, g.op.Key)
	}
	if err := d.learnGen(ie, igen, old); err != nil {
		return err
	}
	d.seals[old] = g
	return nil
}

func (d *Driver) sealGen(s *connect.Step) error {
	gk := s.Pick("gk")
	k := genKey{gk.Get("epoch").Int64(), gk.Get("gen").Int64()}
	g := d.seals[k]
	if g == nil {
		return fmt.Errorf("no seal pending for generation %v", k)
	}
	delete(d.seals, k)
	g.release <- publisher.Apply
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if _, ok := d.store.Object(g.op.Key); ok {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("seal %s was not written", g.op.Key)
}

func (d *Driver) crash(*connect.Step) error {
	// The process is gone: nothing it had in flight lands or reports.
	if d.Recorder != nil {
		qobs.SetGlobal(nil)
		defer qobs.SetGlobal(d.Recorder)
	}
	for k, fl := range d.pushes {
		if fl.gate != nil {
			fl.gate.release <- publisher.Fail
		}
		<-fl.done
		delete(d.pushes, k)
	}
	for k, g := range d.seals {
		g.release <- publisher.Fail
		delete(d.seals, k)
	}
	d.pub.Drain()
	return d.start()
}

// check compares the store with the model's S3 state: table rows, Parquet
// objects, manifests and seals, as sets of (epoch, generation, batch).
func (d *Driver) check(s *connect.Step) error {
	model := func(v itf.Value, src bool) []string {
		var out []string
		for _, e := range v.Elems {
			k := e
			if src {
				k = e.Get("src")
			}
			out = append(out, fmt.Sprintf("e%d/g%d/b%d", k.Get("bk").Get("epoch").Int64(), k.Get("gk").Get("gen").Int64(), k.Get("bk").Get("batch").Int64()))
		}
		sort.Strings(out)
		return out
	}
	var tables, parquet, manifests, seals []string
	gen := func(epoch int64, implGen string) (int64, bool) {
		k, ok := d.gens[fmt.Sprintf("e%d/%s", epoch, implGen)]
		return k.gen, ok
	}
	for t, bs := range d.store.Tables() {
		kind, e, ig, _ := classify(publisher.Op{Kind: "table", Key: t})
		g, ok := gen(e, ig)
		if !ok || kind != "table" {
			return fmt.Errorf("table %s: unknown generation", t)
		}
		for _, b := range bs {
			tables = append(tables, fmt.Sprintf("e%d/g%d/b%d", e, g, b))
		}
	}
	for _, key := range d.store.Keys() {
		kind, e, ig, b := classify(publisher.Op{Kind: "object", Key: key})
		g, ok := gen(e, ig)
		if !ok {
			return fmt.Errorf("object %s: unknown generation", key)
		}
		switch kind {
		case "parquet":
			parquet = append(parquet, fmt.Sprintf("e%d/g%d/b%d", e, g, b))
		case "manifest":
			manifests = append(manifests, fmt.Sprintf("e%d/g%d/b%d", e, g, b))
		case "seal":
			data, _ := d.store.Object(key)
			var sm struct{ Batches int }
			_ = json.Unmarshal(data, &sm)
			seals = append(seals, fmt.Sprintf("e%d/g%d:%d", e, g, sm.Batches))
		}
	}
	var mseals []string
	for _, sv := range s.After["seals"].Elems {
		mseals = append(mseals, fmt.Sprintf("e%d/g%d:%d", sv.Get("gk").Get("epoch").Int64(), sv.Get("gk").Get("gen").Int64(), len(sv.Get("batches").Elems)))
	}
	sort.Strings(tables)
	sort.Strings(parquet)
	sort.Strings(manifests)
	sort.Strings(seals)
	sort.Strings(mseals)
	for _, c := range []struct {
		name       string
		impl, spec []string
	}{
		{"table rows", tables, model(s.After["tableRows"], true)},
		{"parquet objects", parquet, model(s.After["parquet"], false)},
		{"manifests", manifests, model(s.After["manifests"], false)},
		{"seals (generation:batches)", seals, mseals},
	} {
		if strings.Join(c.impl, " ") != strings.Join(c.spec, " ") {
			return fmt.Errorf("%s: implementation %v, model %v", c.name, c.impl, c.spec)
		}
	}
	return nil
}
