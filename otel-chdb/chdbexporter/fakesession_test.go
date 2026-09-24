package chdbexporter

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// fakeWorld stands in for everything the publisher writes to: chDB's catalog
// (tables, views, their rows) and S3 (Parquet objects, manifests, seals).
// fakeSession implements session over it, interpreting the handful of
// statement shapes publish.go issues. It decodes every RowBinary payload
// with the structure the statement names, so rows carry their real envelope
// (epoch, batch id, ordinal).
//
// Faults are armed by the state machine before a step and consumed by the
// first statement of a matching kind: "fail" returns an error without
// applying the statement; "ambiguous" applies it and then returns an error,
// like an S3 PUT that lands but whose response is lost.
//
// The world also flags what the design says must never happen, as it happens
// (violations), and can record each write as a quint step (steps) for
// trace validation against ../model/edgePublish.qnt.

type fakeRow struct {
	epoch   string
	batch   uint64
	ordinal uint64
}

type fakeTable struct {
	name     string // fully qualified
	stored   bool   // MergeTree holding data (vs a view, staging or Buffer table)
	state    string // attached, detached, dropped
	endpoint string // s3_plain_rewritable endpoint from its DDL, "" for local
	gen      string // generation suffix of its name
	rows     []fakeRow
	target   string // for a materialized view: the table it writes to
}

type fakeObject struct {
	data     []byte
	rows     []fakeRow // Parquet
	writes   int
	kind, ns string
	manifest *batchManifest
	seal     *sealManifest
}

type fault struct {
	op   string // ddl, insert, parquet, manifest, seal, drop, optimize, detach
	mode string // fail, ambiguous
}

type fakeWorld struct {
	mu      sync.Mutex
	cfg     *Config
	tables  map[string]*fakeTable
	remote  []*fakeTable // stored tables a lost catalog no longer knows
	objects map[string]*fakeObject
	faults  []fault
	fired   []fault
	// violations are design rules broken as they happened.
	violations []string
	statements []string
	rec        *stepRecorder
	cache      *manifestCache
	payload    string // what the current push is exporting, for steps
}

func newFakeWorld(cfg *Config) *fakeWorld {
	return &fakeWorld{cfg: cfg, tables: map[string]*fakeTable{}, objects: map[string]*fakeObject{}}
}

// arm adds a fault for the next statement of kind op.
func (w *fakeWorld) arm(op, mode string) {
	w.mu.Lock()
	w.faults = append(w.faults, fault{op, mode})
	w.mu.Unlock()
}

// disarm drops unconsumed faults and returns the ones that fired.
func (w *fakeWorld) disarm() []fault {
	w.mu.Lock()
	defer w.mu.Unlock()
	f := w.fired
	w.faults, w.fired = nil, nil
	return f
}

// take consumes an armed fault for op.
func (w *fakeWorld) take(op string) string {
	for i, f := range w.faults {
		if f.op == op {
			w.faults = append(w.faults[:i], w.faults[i+1:]...)
			w.fired = append(w.fired, f)
			return f.mode
		}
	}
	return ""
}

var errInjected = errors.New("injected fault")

// apply runs f unless an armed fault for op says otherwise.
func (w *fakeWorld) apply(op string, f func() error) (landed bool, err error) {
	switch w.take(op) {
	case "fail":
		return false, fmt.Errorf("%s: %w (not applied)", op, errInjected)
	case "ambiguous":
		if err := f(); err != nil {
			return false, err
		}
		return true, fmt.Errorf("%s: %w (applied, response lost)", op, errInjected)
	}
	if err := f(); err != nil {
		return false, err
	}
	return true, nil
}

func (w *fakeWorld) violate(format string, args ...any) {
	w.violations = append(w.violations, fmt.Sprintf(format, args...))
}

// ---- keys and names -----------------------------------------------------------

var (
	// {root}/{region}/{signal}/v{schema}/{producer}/{epoch}/manifests/{gen}/{name}
	manifestKeyRe = regexp.MustCompile(`^(.+?)/([^/]+/(traces|logs)/v\d+/[^/]+/([^/]+))/manifests/(g\d{8}T\d{6})/(\d{20}\.json|_sealed\.json)$`)
	// {root}/{region}/{signal}/v{schema}/{producer}/{epoch}/{gen}/{batch}.parquet
	parquetKeyRe = regexp.MustCompile(`^(.+?)/([^/]+/(traces|logs)/v\d+/[^/]+/([^/]+))/(g\d{8}T\d{6})/(\d{20})\.parquet$`)
	genSuffixRe  = regexp.MustCompile(`_(g\d{8}T\d{6})(_|$)`)
)

type objKey struct {
	ns, signal, epoch, gen, name string
}

func parseObjKey(key string) (objKey, string, bool) {
	if m := manifestKeyRe.FindStringSubmatch(key); m != nil {
		kind := "manifest"
		if m[6] == "_sealed.json" {
			kind = "seal"
		}
		return objKey{ns: m[2], signal: m[3], epoch: m[4], gen: m[5], name: m[6]}, kind, true
	}
	if m := parquetKeyRe.FindStringSubmatch(key); m != nil {
		return objKey{ns: m[2], signal: m[3], epoch: m[4], gen: m[5], name: m[6]}, "parquet", true
	}
	return objKey{}, "", false
}

func (w *fakeWorld) sealKey(ns, gen string) string {
	root := w.cfg.ObjectStorage.Endpoint
	if root == "" {
		root = w.cfg.Parquet.URL
	}
	return joinURL(root, ns, "manifests", gen, "_sealed.json")
}

// sealed reports whether a generation of a namespace has a seal object.
func (w *fakeWorld) sealed(ns, gen string) bool {
	_, ok := w.objects[w.sealKey(ns, gen)]
	return ok
}

// sqlStrings returns the single-quoted literals of a statement, unescaped
// the way sqlQuote escapes them.
func sqlStrings(q string) []string {
	var out []string
	for i := 0; i < len(q); i++ {
		if q[i] != '\'' {
			continue
		}
		var b strings.Builder
		for i++; i < len(q) && q[i] != '\''; i++ {
			if q[i] == '\\' && i+1 < len(q) {
				i++
			}
			b.WriteByte(q[i])
		}
		out = append(out, b.String())
	}
	return out
}

func (w *fakeWorld) signalOf(table string) string {
	name := table[strings.IndexByte(table, '.')+1:]
	if strings.HasPrefix(name, w.cfg.TracesTableName) {
		return signalTraces
	}
	return signalLogs
}

// ---- statements ------------------------------------------------------------------

var (
	createTableRe = regexp.MustCompile(`^CREATE TABLE IF NOT EXISTS (\S+)`)
	createViewRe  = regexp.MustCompile(`^CREATE MATERIALIZED VIEW IF NOT EXISTS (\S+)\s+TO (\S+)`)
	dropRe        = regexp.MustCompile(`^DROP TABLE IF EXISTS (\S+)$`)
	detachRe      = regexp.MustCompile(`^DETACH TABLE IF EXISTS (\S+)$`)
	optimizeRe    = regexp.MustCompile(`^OPTIMIZE TABLE (\S+)( FINAL)?$`)
	insertRe      = regexp.MustCompile(`^INSERT INTO (\S+) \(`)
)

func isLocalHelper(name string) bool {
	return strings.HasSuffix(name, "_in") || strings.HasSuffix(name, "_buf") || strings.HasSuffix(name, "_mv")
}

func (w *fakeWorld) query(q string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.statements = append(w.statements, firstLine(q))
	switch {
	case strings.HasPrefix(q, "SET "), strings.HasPrefix(q, "CREATE DATABASE IF NOT EXISTS "):
		return nil
	case createViewRe.MatchString(q):
		m := createViewRe.FindStringSubmatch(q)
		_, err := w.apply("ddl", func() error { return w.create(m[1], false, "", m[2]) })
		return err
	case createTableRe.MatchString(q):
		name := createTableRe.FindStringSubmatch(q)[1]
		endpoint := ""
		if i := strings.Index(q, "endpoint = "); i >= 0 {
			endpoint = sqlStrings(q[i:])[0]
		}
		_, err := w.apply("ddl", func() error { return w.create(name, !isLocalHelper(name), endpoint, "") })
		return err
	case dropRe.MatchString(q):
		name := dropRe.FindStringSubmatch(q)[1]
		_, err := w.apply("drop", func() error {
			if t := w.tables[name]; t != nil && t.state != "dropped" {
				if t.stored && t.endpoint != "" {
					w.violate("DROP TABLE %s deletes objects on %s that a consumer may not have read", name, t.endpoint)
				}
				t.state = "dropped"
			}
			return nil
		})
		return err
	case detachRe.MatchString(q):
		name := detachRe.FindStringSubmatch(q)[1]
		_, err := w.apply("detach", func() error {
			if t := w.tables[name]; t != nil && t.state == "attached" {
				t.state = "detached"
			}
			return nil
		})
		return err
	case optimizeRe.MatchString(q):
		name := optimizeRe.FindStringSubmatch(q)[1]
		_, err := w.apply("optimize", func() error {
			if t := w.tables[name]; t == nil || t.state != "attached" {
				return fmt.Errorf("OPTIMIZE: table %s does not exist", name)
			}
			return nil
		})
		return err
	}
	w.violate("unexpected statement %q", firstLine(q))
	return fmt.Errorf("fake: unexpected statement %q", firstLine(q))
}

func firstLine(q string) string {
	if i := strings.IndexByte(q, '\n'); i >= 0 {
		return q[:i] + " ..."
	}
	return q
}

// create follows CREATE ... IF NOT EXISTS: an existing attached object is
// left as it is (its old disk included); a detached one is an error, as in
// ClickHouse; a dropped one is created afresh.
func (w *fakeWorld) create(name string, stored bool, endpoint, target string) error {
	if t := w.tables[name]; t != nil {
		switch t.state {
		case "attached":
			return nil
		case "detached":
			return fmt.Errorf("table %s already exists (detached)", name)
		}
	}
	gen := ""
	if m := genSuffixRe.FindStringSubmatch(name); m != nil {
		gen = m[1]
	}
	w.tables[name] = &fakeTable{name: name, stored: stored, state: "attached", endpoint: endpoint, gen: gen, target: target}
	return nil
}

func (w *fakeWorld) insert(q, format string, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.statements = append(w.statements, firstLine(q))
	if format == "RawBLOB" && strings.HasPrefix(q, "INSERT INTO FUNCTION s3(") {
		key := sqlStrings(q)[0]
		k, kind, ok := parseObjKey(key)
		if !ok {
			w.violate("object at an unexpected key %s", key)
			return fmt.Errorf("fake: key %s", key)
		}
		return w.putObject(key, k, kind, data, nil)
	}
	if format == "RowBinary" && strings.HasPrefix(q, "INSERT INTO FUNCTION s3(") {
		args := sqlStrings(q)
		key, structure := args[0], args[len(args)-1]
		k, kind, ok := parseObjKey(key)
		if !ok || kind != "parquet" {
			w.violate("parquet at an unexpected key %s", key)
			return fmt.Errorf("fake: key %s", key)
		}
		rows, err := decodeEnvelopeRows(structure, data)
		if err != nil {
			w.violate("parquet %s: %v", key, err)
			return err
		}
		return w.putObject(key, k, kind, data, rows)
	}
	if m := insertRe.FindStringSubmatch(q); m != nil && format == "RowBinary" {
		return w.insertRows(m[1], data)
	}
	w.violate("unexpected insert %q (%s)", firstLine(q), format)
	return fmt.Errorf("fake: unexpected insert %q", firstLine(q))
}

// decodeEnvelopeRows decodes a payload with the structure a statement names
// and returns each row's envelope.
var structureCache sync.Map // structure string -> []rbColumn

func decodeEnvelopeRows(structure string, data []byte) ([]fakeRow, error) {
	var cols []rbColumn
	if c, ok := structureCache.Load(structure); ok {
		cols = c.([]rbColumn)
	} else {
		var err error
		if cols, err = parseStructure(structure); err != nil {
			return nil, err
		}
		structureCache.Store(structure, cols)
	}
	rows, err := decodeRowBinary(cols, data)
	if err != nil {
		return nil, err
	}
	ie, ib, io := colIndex(cols, "producer_epoch"), colIndex(cols, "batch_id"), colIndex(cols, "row_ordinal")
	if ie < 0 || ib < 0 || io < 0 {
		return nil, errors.New("no envelope columns")
	}
	out := make([]fakeRow, len(rows))
	for i, r := range rows {
		out[i] = fakeRow{epoch: r[ie].(string), batch: r[ib].(uint64), ordinal: r[io].(uint64)}
	}
	return out, nil
}

func batchOf(rows []fakeRow) (string, uint64) {
	if len(rows) == 0 {
		return "", 0
	}
	return rows[0].epoch, rows[0].batch
}

// insertRows is a table insert: into the MergeTree table, or into a staging
// table whose view forwards to it. A view that is gone forwards nothing (the
// Null engine swallows the rows); a table that is gone is an error.
func (w *fakeWorld) insertRows(target string, data []byte) error {
	t := w.tables[target]
	if t == nil || t.state != "attached" {
		return fmt.Errorf("table %s does not exist", target)
	}
	signal := w.signalOf(target)
	rows, err := decodeEnvelopeRows(structure(signal, true), data)
	if err != nil {
		w.violate("insert into %s: %v", target, err)
		return err
	}
	dest := t
	if !t.stored {
		dest = nil
		if mv := w.tables[target+"_mv"]; mv != nil && mv.state == "attached" {
			dest = w.tables[mv.target]
		}
	}
	epoch, batch := batchOf(rows)
	w.stepPushStart(signal, epoch, batch, t.gen)
	landed, err := w.apply("insert", func() error {
		if dest == nil {
			return nil
		}
		if dest.state != "attached" {
			return fmt.Errorf("table %s does not exist", dest.name)
		}
		if ns := w.namespaceOfEndpoint(dest.endpoint); ns != "" && w.sealed(ns, dest.gen) {
			w.violate("rows of epoch %s batch %d written into %s after its generation %s was sealed", epoch, batch, dest.name, dest.gen)
		}
		dest.rows = append(dest.rows, rows...)
		return nil
	})
	w.stepWrite("writeTable", signal, epoch, batch, t.gen, landed, err)
	if !w.cfg.parquet() {
		// No Parquet: the model's Parquet phase is a no-op that succeeds.
		if err == nil {
			w.stepWrite("writeParquet", signal, epoch, batch, t.gen, true, nil)
		}
	}
	return err
}

// namespaceOfEndpoint recovers the namespace from a table endpoint
// {root}/{ns}/{gen}/{table}/.
func (w *fakeWorld) namespaceOfEndpoint(ep string) string {
	root := strings.TrimRight(w.cfg.ObjectStorage.Endpoint, "/") + "/"
	if ep == "" || !strings.HasPrefix(ep, root) {
		return ""
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(ep, root), "/"), "/")
	if len(parts) < 7 {
		return ""
	}
	return strings.Join(parts[:5], "/")
}

func (w *fakeWorld) putObject(key string, k objKey, kind string, data []byte, rows []fakeRow) error {
	epoch, batch := batchOf(rows)
	if kind == "manifest" {
		var m batchManifest
		if err := json.Unmarshal(data, &m); err != nil {
			w.violate("manifest %s is not JSON: %v", key, err)
		}
		epoch, batch = m.ProducerEpoch, m.BatchID
	}
	if kind == "parquet" && !w.cfg.StoreTables {
		// No tables: the model's table phase is a no-op that succeeds.
		w.stepPushStart(k.signal, epoch, batch, k.gen)
		w.stepWrite("writeTable", k.signal, epoch, batch, k.gen, true, nil)
	}
	landed, err := w.apply(kind, func() error {
		if w.sealed(k.ns, k.gen) && kind != "seal" {
			w.violate("%s %s written after generation %s of %s was sealed", kind, k.name, k.gen, k.ns)
		}
		o := w.objects[key]
		if o == nil {
			o = &fakeObject{}
			w.objects[key] = o
		} else if kind != "seal" {
			w.violate("%s %s overwritten (write %d)", kind, key, o.writes+1)
		}
		o.data, o.rows, o.kind, o.ns = append([]byte(nil), data...), rows, kind, k.ns
		o.writes++
		switch kind {
		case "manifest":
			o.manifest = new(batchManifest)
			_ = json.Unmarshal(data, o.manifest)
		case "seal":
			o.seal = new(sealManifest)
			_ = json.Unmarshal(data, o.seal)
		}
		w.cache = nil
		return nil
	})
	switch kind {
	case "parquet":
		w.stepWrite("writeParquet", k.signal, epoch, batch, k.gen, landed, err)
	case "manifest":
		w.stepWrite("writeManifest", k.signal, epoch, batch, k.gen, landed, err)
	case "seal":
		if landed {
			var s sealManifest
			_ = json.Unmarshal(data, &s)
			w.stepSeal(k.signal, k.epoch, k.gen, s.Batches)
		}
	}
	return err
}

// ---- reading the world back -----------------------------------------------------------

type fakeManifest struct {
	key string
	batchManifest
}

type fakeSeal struct {
	key string
	ns  string
	sealManifest
}

// manifests returns every batch manifest and seal in S3, in key order. They
// are parsed when written and cached until the next write.
func (w *fakeWorld) manifests() ([]fakeManifest, []fakeSeal) {
	if w.cache != nil {
		return w.cache.ms, w.cache.ss
	}
	var ms []fakeManifest
	var ss []fakeSeal
	keys := make([]string, 0, len(w.objects))
	for k, o := range w.objects {
		if o.kind == "manifest" || o.kind == "seal" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		o := w.objects[key]
		if o.kind == "manifest" {
			ms = append(ms, fakeManifest{key, *o.manifest})
		} else {
			ss = append(ss, fakeSeal{key, o.ns, *o.seal})
		}
	}
	w.cache = &manifestCache{ms, ss}
	return ms, ss
}

type manifestCache struct {
	ms []fakeManifest
	ss []fakeSeal
}

// crashLocal forgets the local catalog, as a restart on a fresh (temporary)
// chDB path does; S3 is untouched.
// The data of tables on object storage stays where it is, in remote.
func (w *fakeWorld) crashLocal() {
	w.mu.Lock()
	for _, t := range w.tables {
		if t.stored && t.endpoint != "" && t.state != "dropped" {
			w.remote = append(w.remote, t)
		}
	}
	w.tables = map[string]*fakeTable{}
	w.mu.Unlock()
}

// storedTables returns every stored table whose data exists: attached or
// detached in the catalog, or left on object storage by a previous
// incarnation's catalog.
func (w *fakeWorld) storedTables() []*fakeTable {
	var out []*fakeTable
	names := make([]string, 0, len(w.tables))
	for n := range w.tables {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if t := w.tables[n]; t.stored && t.state != "dropped" {
			out = append(out, t)
		}
	}
	return append(out, w.remote...)
}

// ---- the session ------------------------------------------------------------------------

type fakeSession struct{ w *fakeWorld }

func (s fakeSession) Query(q string, _ ...string) (result, error) { return nil, s.w.query(q) }
func (s fakeSession) Insert(q, format string, data []byte) error  { return s.w.insert(q, format, data) }
func (s fakeSession) Close()                                      {}
