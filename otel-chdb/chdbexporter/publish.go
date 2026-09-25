package chdbexporter

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Publishing turns the exporter into a short-lived edge buffer: every batch
// is written somewhere a central consumer can read it after this process is
// gone, and announced by a manifest once it is durable.
//
//	{root}/{region}/{signal}/v{schema}/{producer}/{epoch}/
//	    {generation}/{table}/...                  s3_plain_rewritable table data (object_storage)
//	    {generation}/{batch_id}.parquet           one object per batch (parquet)
//	    manifests/{generation}/{batch_id}.json    written last: the batch's commit record
//	    manifests/{generation}/_sealed.json       written when the generation takes no more batches
//
// A batch is committed when its manifest exists. Rows or objects without one
// (an insert that succeeded before a crash, a retry's first attempt) are not
// committed, and a consumer that selects by the batch ids in manifests never
// sees them. That is what makes retries safe without deduplication here: a
// retried push is a new batch with a new id.

// processEpoch is shared by every exporter in the process: it names this
// incarnation of the producer.
var processEpoch = sync.OnceValue(func() string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
})

// batchSeq numbers batches per namespace, so ids start at 1 in every
// {region}/{signal}/v{schema}/{producer}/{epoch}. A failed push leaves a gap
// (its retry takes the next id); manifests, not contiguity, say which
// batches exist.
var batchSeq sync.Map // namespace -> *atomic.Uint64

func nextBatch(namespace string) uint64 {
	v, _ := batchSeq.LoadOrStore(namespace, new(atomic.Uint64))
	return v.(*atomic.Uint64).Add(1)
}

// claimed records which signals a publishing exporter owns in this process.
// Two instances publishing one signal would share table names and batch ids
// but rotate and seal independently, so the second is refused at start.
var claimed sync.Map

type generation struct {
	id    string
	start time.Time
	ts    *tableSet // nil when store_tables is off
	// tables is where each stored table lives, for manifests.
	tables []ManifestTable

	batches, rows atomic.Uint64
	first, last   atomic.Uint64 // batch id range
	sealedAt      time.Time
	// flushed: the Buffer table's rows are in the stored table, so a
	// retried seal may drop the Buffer. Seals run one at a time per
	// generation (rotation, then sweep or close), so no lock.
	flushed bool
	// retiring: local_retention has passed and a DETACH was attempted, so
	// retries do not wait on the clock again (which may have stepped back).
	retiring bool
}

type signalState struct {
	// mu: pushes hold it shared for the whole write, rotation holds it
	// exclusively, so a generation is quiescent once it is no longer cur.
	mu       sync.RWMutex
	cur      *generation
	closed   bool
	sealed   []*generation // awaiting detach
	unsealed []*generation // rotated, but the seal failed: retried by sweep
}

type publisher struct {
	e       *chdbExporter
	cfg     *Config
	epoch   string
	signals map[string]*signalState
	now     func() time.Time

	stop chan struct{}
	done chan struct{}
	wg   sync.WaitGroup // background seals
}

func newPublisher(e *chdbExporter) (*publisher, error) {
	cfg := e.cfg
	p := &publisher{e: e, cfg: cfg, epoch: cfg.Producer.Epoch, signals: map[string]*signalState{},
		now: time.Now, stop: make(chan struct{}), done: make(chan struct{})}
	if p.epoch == "" {
		p.epoch = processEpoch()
	}
	for _, sig := range e.signals() {
		if _, loaded := claimed.LoadOrStore(sig, e); loaded {
			p.release()
			return nil, fmt.Errorf("another chdb exporter in this process already publishes %s", sig)
		}
		p.signals[sig] = &signalState{}
	}
	compression := cfg.Parquet.Compression
	for _, s := range e.all {
		// Every object a batch writes has a fresh key, but a retried seal
		// rewrites _sealed.json.
		if err := exec(s, fmt.Sprintf("SET s3_truncate_on_insert = 1, engine_file_truncate_on_insert = 1, "+
			"output_format_parquet_compression_method = %s", sqlQuote(compression))); err != nil {
			p.release()
			return nil, err
		}
	}
	if cfg.StoreTables {
		if err := exec(e.all[0], "CREATE DATABASE IF NOT EXISTS "+cfg.Database); err != nil {
			p.release()
			return nil, err
		}
	}
	go p.retentionLoop()
	return p, nil
}

func (p *publisher) release() {
	for sig := range p.signals {
		claimed.CompareAndDelete(sig, p.e)
	}
	for _, sig := range p.e.signals() {
		claimed.CompareAndDelete(sig, p.e)
	}
}

// namespace is the path of this producer incarnation's data for a signal.
func (p *publisher) namespace(signal string) string {
	c := p.cfg.Producer
	return fmt.Sprintf("%s/%s/v%d/%s/%s", c.Region, signal, c.SchemaVersion, c.ID, p.epoch)
}

func joinURL(base string, parts ...string) string {
	return strings.TrimRight(base, "/") + "/" + strings.Join(parts, "/")
}

// manifestRoot is where manifests go: beside the table data when there is
// object storage, else beside the Parquet objects.
func (p *publisher) manifestRoot() string {
	if p.cfg.objectStorage() {
		return p.cfg.ObjectStorage.Endpoint
	}
	return p.cfg.Parquet.URL
}

func genID(t time.Time, d time.Duration) (string, time.Time) {
	start := t.UTC().Truncate(d)
	return "g" + start.Format("20060102T150405"), start
}

// tableSuffix names a generation's tables in the local catalog. It carries
// the epoch as well as the generation: with a persistent chDB path, an
// incarnation restarted within a generation would otherwise find its
// predecessor's tables, and CREATE TABLE IF NOT EXISTS would keep them, on
// the predecessor's endpoint, in a generation the predecessor sealed.
func tableSuffix(epoch, gen string) string {
	b := []byte(epoch)
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			b[i] = '_'
		}
	}
	return gen + "_e" + string(b)
}

// acquire returns the signal's current generation with its lock held
// shared, rotating first if the clock has moved into a later generation.
//
// Generations only move forward. The clock is read under the lock, and a
// reading that falls in an earlier generation than the current one (the
// wall clock stepped back, or this push read it just before a boundary
// that another push has already rotated past) keeps the current
// generation: going back would reopen a sealed one.
func (p *publisher) acquire(signal string) (*generation, func(), error) {
	st := p.signals[signal]
	if st == nil {
		return nil, nil, fmt.Errorf("this exporter does not publish %s", signal)
	}
	st.mu.RLock()
	if g := st.cur; g != nil {
		if _, start := genID(p.now(), p.cfg.generation()); !start.After(g.start) {
			return g, st.mu.RUnlock, nil
		}
	}
	st.mu.RUnlock()

	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return nil, nil, fmt.Errorf("chdb publisher for %s is closed", signal)
	}
	id, start := genID(p.now(), p.cfg.generation())
	if st.cur == nil || start.After(st.cur.start) {
		g, err := p.openGeneration(signal, id, start)
		if err != nil {
			st.mu.Unlock()
			return nil, nil, err
		}
		if old := st.cur; old != nil {
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				err := p.seal(signal, old)
				st.mu.Lock()
				if err != nil {
					p.e.logger.Error("seal generation; will retry", zap.String("signal", signal), zap.String("generation", old.id), zap.Error(err))
					st.unsealed = append(st.unsealed, old)
				} else {
					st.sealed = append(st.sealed, old)
				}
				st.mu.Unlock()
			}()
		}
		st.cur = g
	}
	st.mu.Unlock()
	return p.acquire(signal)
}

func (p *publisher) openGeneration(signal, id string, start time.Time) (*generation, error) {
	g := &generation{id: id, start: start}
	if !p.cfg.StoreTables {
		return g, nil
	}
	disk := localDisk
	if p.cfg.objectStorage() {
		obj := p.cfg.ObjectStorage
		ns := p.namespace(signal)
		disk = func(table string) string {
			// The table's own prefix: its generation, then its unrotated name.
			name := strings.Replace(table, "_"+tableSuffix(p.epoch, id), "", 1)
			ep := joinURL(obj.Endpoint, ns, id, name) + "/"
			g.tables = append(g.tables, ManifestTable{Table: p.cfg.Database + "." + table, Endpoint: ep})
			extra := fmt.Sprintf(", old_parts_lifetime = %d", int(obj.OldPartsLifetime.Seconds()))
			if obj.CompactParts {
				extra += ", min_bytes_for_wide_part = 1099511627776, min_rows_for_wide_part = 1000000000000"
			}
			return fmt.Sprintf(", disk = disk(type = object_storage, object_storage_type = 's3', metadata_type = 'plain_rewritable', "+
				"endpoint = %s, access_key_id = %s, secret_access_key = %s), table_disk = 1%s",
				sqlQuote(ep), sqlQuote(obj.AccessKeyID), sqlQuote(string(obj.SecretAccessKey)), extra)
		}
	}
	ts := buildTableSet(p.cfg, signal, tableSuffix(p.epoch, id), true, disk)
	g.ts = &ts
	err := p.withConn(func(s session) error {
		for _, q := range ts.ddl {
			if err := exec(s, q); err != nil {
				return fmt.Errorf("create %s generation %s: %w", signal, id, err)
			}
		}
		return nil
	})
	return g, err
}

func (p *publisher) withConn(f func(session) error) error {
	s := <-p.e.conns
	defer func() { p.e.conns <- s }()
	return f(s)
}

// push writes one batch: table insert, then Parquet object, then manifest.
func (p *publisher) push(signal string, write writeFunc) error {
	g, unlock, err := p.acquire(signal)
	if err != nil {
		return err
	}
	defer unlock()
	start := p.now()
	env := &envelope{producer: p.cfg.Producer.ID, epoch: p.epoch, batch: nextBatch(p.namespace(signal)),
		received: uint64(start.UnixNano()), schema: p.cfg.Producer.SchemaVersion}
	w := p.e.rbPool.Get().(*rowBinary)
	defer p.e.rbPool.Put(w)
	w.buf = w.buf[:0]
	rows := write(w, env)

	m := batchManifest{
		ProducerID: env.producer, ProducerEpoch: env.epoch, Region: p.cfg.Producer.Region, Signal: signal,
		SchemaVersion: env.schema, Generation: g.id, BatchID: env.batch, Rows: rows, Bytes: len(w.buf),
		ReceivedAt: start.UTC(), MinEventTime: time.Unix(0, int64(env.minTS)).UTC(), MaxEventTime: time.Unix(0, int64(env.maxTS)).UTC(),
		Tables: g.tables,
	}
	err = p.withConn(func(s session) error {
		if g.ts != nil {
			if err := s.Insert(g.ts.insert, "RowBinary", w.buf); err != nil {
				return fmt.Errorf("insert: %w", err)
			}
		}
		if p.cfg.parquet() {
			key, err := p.writeParquet(s, signal, g.id, env.batch, w.buf)
			if err != nil {
				return fmt.Errorf("parquet: %w", err)
			}
			m.Parquet = key
		}
		return p.writeObject(s, p.manifestPath(signal, g.id, fmt.Sprintf("%020d.json", env.batch)), mustJSON(m))
	})
	if err != nil {
		return fmt.Errorf("chdb publish %d %s (batch %d): %w", rows, signal, env.batch, err)
	}
	g.batches.Add(1)
	g.rows.Add(uint64(rows))
	for {
		f := g.first.Load()
		if (f != 0 && f <= env.batch) || g.first.CompareAndSwap(f, env.batch) {
			break
		}
	}
	for {
		l := g.last.Load()
		if env.batch <= l || g.last.CompareAndSwap(l, env.batch) {
			break
		}
	}
	p.e.debug(signal, rows, len(w.buf), 0, start)
	return nil
}

func (p *publisher) manifestPath(signal, gen, name string) string {
	return joinURL(p.manifestRoot(), p.namespace(signal), "manifests", gen, name)
}

// writeParquet streams the batch's RowBinary through chDB into one Parquet
// object and returns its location.
func (p *publisher) writeParquet(s session, signal, gen string, batch uint64, rb []byte) (string, error) {
	loc := joinURL(p.cfg.Parquet.URL, p.namespace(signal), gen, fmt.Sprintf("%020d.parquet", batch))
	st := sqlQuote(structure(signal, true))
	var fn string
	if isHTTP(loc) {
		fn = fmt.Sprintf("s3(%s, %s, %s, 'Parquet', %s)", sqlQuote(loc), sqlQuote(p.cfg.Parquet.AccessKeyID),
			sqlQuote(string(p.cfg.Parquet.SecretAccessKey)), st)
	} else {
		path, err := localPath(loc)
		if err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return "", err
		}
		fn = fmt.Sprintf("file(%s, 'Parquet', %s)", sqlQuote(path), st)
	}
	return loc, s.Insert("INSERT INTO FUNCTION "+fn, "RowBinary", rb)
}

// writeObject stores data verbatim at loc: through chDB's s3() for http(s)
// URLs (RawBLOB, so the bytes are exactly the JSON), or as a local file,
// renamed into place so a reader never sees half of it.
func (p *publisher) writeObject(s session, loc string, data []byte) error {
	if !isHTTP(loc) {
		path, err := localPath(loc)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			return err
		}
		return os.Rename(tmp, path)
	}
	key, secret := p.cfg.ObjectStorage.AccessKeyID, string(p.cfg.ObjectStorage.SecretAccessKey)
	if !p.cfg.objectStorage() {
		key, secret = p.cfg.Parquet.AccessKeyID, string(p.cfg.Parquet.SecretAccessKey)
	}
	return s.Insert(fmt.Sprintf("INSERT INTO FUNCTION s3(%s, %s, %s, 'RawBLOB')", sqlQuote(loc), sqlQuote(key), sqlQuote(secret)),
		"RawBLOB", data)
}

func localPath(fileURL string) (string, error) {
	u, err := url.Parse(fileURL)
	if err != nil || u.Scheme != "file" || !filepath.IsAbs(u.Path) {
		return "", fmt.Errorf("not a file:///absolute URL: %q", fileURL)
	}
	return u.Path, nil
}

// seal finishes a generation that takes no more batches: flush any Buffer,
// optionally merge each table down, drop the process-local helpers, and
// write _sealed.json. The stored tables stay attached (and queryable here)
// until local_retention.
//
// Every step can be repeated, so a seal that failed is retried whole (by
// sweep, and by close): the Buffer is flushed once and only dropped after
// that, DROPs are IF EXISTS, and _sealed.json is rewritten.
func (p *publisher) seal(signal string, g *generation) error {
	var err error
	if g.ts != nil {
		err = p.withConn(func(s session) error {
			if p.cfg.BufferSeconds > 0 && !g.flushed {
				// Before the helpers go: the flush runs the trace-id view.
				// Unflushed, the Buffer must not be dropped with its rows.
				if err := exec(s, fmt.Sprintf("OPTIMIZE TABLE %s.%s_buf", p.cfg.Database, g.ts.base)); err != nil {
					return err
				}
				g.flushed = true
			}
			var err error
			for _, obj := range g.ts.local {
				err = errors.Join(err, exec(s, "DROP TABLE IF EXISTS "+obj))
			}
			if p.cfg.objectStorage() && p.cfg.ObjectStorage.SealOptimize {
				for _, t := range g.ts.stored {
					err = errors.Join(err, exec(s, "OPTIMIZE TABLE "+t+" FINAL"))
				}
			}
			return err
		})
	}
	sealedAt := p.now()
	m := sealManifest{
		ProducerID: p.cfg.Producer.ID, ProducerEpoch: p.epoch, Region: p.cfg.Producer.Region, Signal: signal,
		SchemaVersion: p.cfg.Producer.SchemaVersion, Generation: g.id, GenerationStart: g.start,
		SealedAt: sealedAt.UTC(), Batches: g.batches.Load(), Rows: g.rows.Load(),
		FirstBatch: g.first.Load(), LastBatch: g.last.Load(), Tables: g.tables,
	}
	if p.cfg.parquet() {
		m.ParquetPrefix = joinURL(p.cfg.Parquet.URL, p.namespace(signal), g.id) + "/"
	}
	err = errors.Join(err, p.withConn(func(s session) error {
		return p.writeObject(s, p.manifestPath(signal, g.id, "_sealed.json"), mustJSON(m))
	}))
	if err == nil {
		// local_retention counts from the seal that completed.
		g.sealedAt = sealedAt
	}
	return err
}

// retentionLoop runs sweep periodically.
func (p *publisher) retentionLoop() {
	defer close(p.done)
	every := p.cfg.generation() / 4
	if p.cfg.StoreTables {
		every = p.cfg.ObjectStorage.LocalRetention / 4
	}
	t := time.NewTicker(min(max(every, 100*time.Millisecond), time.Minute))
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.sweep()
		}
	}
}

// sweep is the publisher's housekeeping: it retries the seals that failed,
// then retires sealed generations past local_retention.
func (p *publisher) sweep() {
	p.retrySeals()
	p.detachExpired()
}

// retrySeals seals again every rotated generation whose seal failed. One
// that fails again waits for the next sweep.
func (p *publisher) retrySeals() {
	for signal, st := range p.signals {
		st.mu.Lock()
		pending := st.unsealed
		st.unsealed = nil
		st.mu.Unlock()
		for _, g := range pending {
			err := p.seal(signal, g)
			st.mu.Lock()
			if err != nil {
				p.e.logger.Error("seal generation; will retry", zap.String("signal", signal), zap.String("generation", g.id), zap.Error(err))
				st.unsealed = append(st.unsealed, g)
			} else {
				st.sealed = append(st.sealed, g)
			}
			st.mu.Unlock()
		}
	}
}

// detachExpired retires sealed generations once they are older than
// local_retention. On object storage it DETACHes them, which leaves the
// objects in place: only the central consumer, after acknowledging a
// generation, should delete them. On local disk (tables beside Parquet
// export) the tables are this process's own copy, so they are dropped. A
// generation whose DETACH or DROP failed stays on the list for the next
// sweep.
func (p *publisher) detachExpired() {
	for signal, st := range p.signals {
		st.mu.Lock()
		var keep, expired []*generation
		for _, g := range st.sealed {
			if g.retiring || p.now().Sub(g.sealedAt) >= p.cfg.ObjectStorage.LocalRetention {
				expired = append(expired, g)
			} else {
				keep = append(keep, g)
			}
		}
		st.sealed = keep
		st.mu.Unlock()
		for _, g := range expired {
			if g.ts == nil {
				continue
			}
			verb := "DROP"
			if p.cfg.objectStorage() {
				verb = "DETACH"
			}
			err := p.withConn(func(s session) error {
				var err error
				for _, t := range g.ts.stored {
					err = errors.Join(err, exec(s, verb+" TABLE IF EXISTS "+t))
				}
				return err
			})
			if err != nil {
				p.e.logger.Error("detach generation; will retry", zap.String("signal", signal), zap.String("generation", g.id), zap.Error(err))
				g.retiring = true
				st.mu.Lock()
				st.sealed = append(st.sealed, g)
				st.mu.Unlock()
			}
		}
	}
}

// closeSealAttempts bounds close's retries of seals that fail. A generation
// still unsealed after them stays so: the process is going away, and only
// a successor that fences this epoch could seal it (see model/S3NATIVE.md).
var closeSealAttempts, closeSealBackoff = 3, 200 * time.Millisecond

// close seals every open generation, and retries every seal that failed,
// the background ones included.
func (p *publisher) close() error {
	for _, st := range p.signals {
		st.mu.Lock()
		if g := st.cur; g != nil {
			st.unsealed = append(st.unsealed, g)
		}
		st.cur, st.closed = nil, true
		st.mu.Unlock()
	}
	p.wg.Wait()
	close(p.stop)
	<-p.done
	for i := 0; i < closeSealAttempts; i++ {
		if i > 0 {
			time.Sleep(closeSealBackoff << (i - 1))
		}
		p.retrySeals()
		if p.pendingSeals() == 0 {
			break
		}
	}
	var err error
	for signal, st := range p.signals {
		for _, g := range st.unsealed {
			err = errors.Join(err, fmt.Errorf("seal %s generation %s: gave up after %d attempts", signal, g.id, closeSealAttempts))
		}
	}
	p.release()
	return err
}

func (p *publisher) pendingSeals() int {
	n := 0
	for _, st := range p.signals {
		st.mu.Lock()
		n += len(st.unsealed)
		st.mu.Unlock()
	}
	return n
}

// ManifestTable is one published table: its name in the writer and the
// s3_plain_rewritable endpoint that holds it.
type ManifestTable struct {
	Table    string `json:"table"`
	Endpoint string `json:"endpoint"`
}

type batchManifest struct {
	ProducerID    string          `json:"producer_id"`
	ProducerEpoch string          `json:"producer_epoch"`
	Region        string          `json:"region"`
	Signal        string          `json:"signal"`
	SchemaVersion uint16          `json:"schema_version"`
	Generation    string          `json:"generation"`
	BatchID       uint64          `json:"batch_id"`
	Rows          int             `json:"rows"`
	Bytes         int             `json:"rowbinary_bytes"`
	ReceivedAt    time.Time       `json:"received_at"`
	MinEventTime  time.Time       `json:"min_event_time"`
	MaxEventTime  time.Time       `json:"max_event_time"`
	Tables        []ManifestTable `json:"tables,omitempty"`
	Parquet       string          `json:"parquet,omitempty"`
}

type sealManifest struct {
	ProducerID      string          `json:"producer_id"`
	ProducerEpoch   string          `json:"producer_epoch"`
	Region          string          `json:"region"`
	Signal          string          `json:"signal"`
	SchemaVersion   uint16          `json:"schema_version"`
	Generation      string          `json:"generation"`
	GenerationStart time.Time       `json:"generation_start"`
	SealedAt        time.Time       `json:"sealed_at"`
	Batches         uint64          `json:"batches"`
	Rows            uint64          `json:"rows"`
	FirstBatch      uint64          `json:"first_batch"`
	LastBatch       uint64          `json:"last_batch"`
	Tables          []ManifestTable `json:"tables,omitempty"`
	ParquetPrefix   string          `json:"parquet_prefix,omitempty"`
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

// ReaderDDL returns the statements a read-only consumer runs to attach one
// published generation of a signal's tables into its own database db. The
// definitions are the writer's, on the same endpoints (from the batch or
// seal manifest's "tables"), with the disk read-only and
// refresh_parts_interval set, so the reader picks up new parts as the writer
// publishes them. SHOW CREATE TABLE on the writer is no substitute: it hides
// the credentials.
func ReaderDDL(c *Config, db, signal, gen string, tables []ManifestTable, accessKeyID, secretAccessKey string, refreshSeconds int) []string {
	rc := *c
	rc.Database = db
	rc.BufferSeconds = 0
	rc.StagingTables = false
	// The reader's tables are named {table}_{gen}; the writer's carry its
	// epoch too (tableSuffix), which the manifest's table names give.
	name := c.TracesTableName
	if signal == signalLogs {
		name = c.LogsTableName
	}
	// The main table's is the shortest name with the generation in it.
	rest, found := "", false
	endpoint := map[string]string{}
	for _, t := range tables {
		tn := t.Table[strings.IndexByte(t.Table, '.')+1:]
		endpoint[tn] = t.Endpoint
		if s, ok := strings.CutPrefix(tn, name+"_"+gen); ok && (!found || len(s) < len(rest)) {
			rest, found = s, true
		}
	}
	suffix := gen + rest
	disk := func(table string) string {
		writer := strings.Replace(table, name+"_"+gen, name+"_"+suffix, 1)
		return fmt.Sprintf(", disk = disk(readonly = 1, type = object_storage, object_storage_type = 's3', metadata_type = 'plain_rewritable', "+
			"endpoint = %s, access_key_id = %s, secret_access_key = %s), table_disk = 1, refresh_parts_interval = %d",
			sqlQuote(endpoint[writer]), sqlQuote(accessKeyID), sqlQuote(secretAccessKey), refreshSeconds)
	}
	ts := buildTableSet(&rc, signal, gen, true, disk)
	out := []string{"CREATE DATABASE IF NOT EXISTS " + db, ts.ddl[0]}
	if signal == signalTraces {
		out = append(out, ts.ddl[1]) // the trace-id lookup table; its view is the writer's business
	}
	return out
}
