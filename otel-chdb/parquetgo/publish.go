package parquetgo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// This is the Parquet-only half of ../chdbexporter/publish.go (store_tables:
// false), with the same layout and manifests:
//
//	{url}/{region}/{signal}/v{schema}/{producer}/{epoch}/
//	    {generation}/{batch_id}.parquet           one object per batch
//	    manifests/{generation}/{batch_id}.json    written last: the batch's commit record
//	    manifests/{generation}/_sealed.json       written when the generation takes no more batches
//
// One difference in content: the batch manifest's "rowbinary_bytes" holds
// the Parquet object's size, since no RowBinary is produced here.

// Config mirrors the chdb exporter's producer and parquet settings.
type Config struct {
	// URL is an S3 prefix (http(s)://host/bucket/prefix, path-style) or a
	// local directory (file:///abs/path).
	URL             string
	AccessKeyID     string
	SecretAccessKey string
	// S3Region signs requests; default us-east-1.
	S3Region string

	ProducerID    string
	Region        string
	SchemaVersion uint16
	Epoch         string // empty: a new one per process
	// Generation groups objects; default 1h.
	Generation time.Duration
	Parquet    Options
	// Engine is "arrow" (arrow-go pqarrow, the default) or "parquet-go".
	Engine string

	// Transport, when set, carries every S3 request (for counting them).
	Transport http.RoundTripper
	// Now is the clock; default time.Now.
	Now func() time.Time
}

var processEpoch = sync.OnceValue(func() string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
})

type generation struct {
	id            string
	start         time.Time
	batches, rows atomic.Uint64
	first, last   atomic.Uint64
}

type signalState struct {
	mu  sync.RWMutex
	cur *generation
	seq atomic.Uint64
}

// newArrowEncoder is set by encode.go unless built with -tags noarrow
// (arrow-go adds ~20 MB to a binary; see README).
var newArrowEncoder func(Options) BatchEncoder

// BatchEncoder writes one batch as one Parquet file.
type BatchEncoder interface {
	Traces(dst io.Writer, td ptrace.Traces, env *Envelope) (int, error)
	Logs(dst io.Writer, ld plog.Logs, env *Envelope) (int, error)
}

// Publisher writes batches as Parquet objects plus manifests.
type Publisher struct {
	cfg     Config
	epoch   string
	s3      *s3.Client
	bucket  string
	prefix  string // key prefix inside the bucket, no trailing slash
	local   string // directory for file:// URLs
	signals map[string]*signalState
	encs    sync.Pool
	bufs    sync.Pool
	sealWG  sync.WaitGroup
}

// New validates cfg and connects nothing yet (the S3 client is lazy).
func New(cfg Config) (*Publisher, error) {
	if cfg.Generation <= 0 {
		cfg.Generation = time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.S3Region == "" {
		cfg.S3Region = "us-east-1"
	}
	if cfg.Parquet.Compression == "" && cfg.Parquet.DataPageSize == 0 {
		cfg.Parquet = DefaultOptions()
	}
	p := &Publisher{cfg: cfg, epoch: cfg.Epoch, signals: map[string]*signalState{
		"traces": {}, "logs": {},
	}}
	if p.epoch == "" {
		p.epoch = processEpoch()
	}
	switch cfg.Engine {
	case "", "arrow":
		if newArrowEncoder == nil {
			return nil, errors.New("built with -tags noarrow: use engine parquet-go")
		}
		p.encs.New = func() any { return newArrowEncoder(cfg.Parquet) }
	case "parquet-go":
		p.encs.New = func() any { return BatchEncoder(NewPGEncoder(cfg.Parquet)) }
	default:
		return nil, fmt.Errorf("engine %q: want arrow or parquet-go", cfg.Engine)
	}
	p.bufs.New = func() any { return new(bytes.Buffer) }
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "file":
		if !filepath.IsAbs(u.Path) {
			return nil, fmt.Errorf("not a file:///absolute URL: %q", cfg.URL)
		}
		p.local = u.Path
	case "http", "https":
		parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
		if parts[0] == "" {
			return nil, fmt.Errorf("S3 URL %q names no bucket", cfg.URL)
		}
		p.bucket = parts[0]
		if len(parts) == 2 {
			p.prefix = parts[1]
		}
		var creds aws.CredentialsProvider = aws.AnonymousCredentials{}
		if cfg.AccessKeyID != "" {
			creds = credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")
		}
		opts := s3.Options{
			Region:       cfg.S3Region,
			BaseEndpoint: aws.String(u.Scheme + "://" + u.Host),
			// Path-style, so SeaweedFS, Garage and MinIO work without
			// virtual-host DNS.
			UsePathStyle: true,
			Credentials:  creds,
		}
		if cfg.Transport != nil {
			opts.HTTPClient = &http.Client{Transport: cfg.Transport}
		}
		p.s3 = s3.New(opts)
	default:
		return nil, fmt.Errorf("parquet url %q: want file:// or http(s)://", cfg.URL)
	}
	return p, nil
}

func (p *Publisher) namespace(signal string) string {
	return fmt.Sprintf("%s/%s/v%d/%s/%s", p.cfg.Region, signal, p.cfg.SchemaVersion, p.cfg.ProducerID, p.epoch)
}

func joinURL(base string, parts ...string) string {
	return strings.TrimRight(base, "/") + "/" + strings.Join(parts, "/")
}

func genID(t time.Time, d time.Duration) (string, time.Time) {
	start := t.UTC().Truncate(d)
	return "g" + start.Format("20060102T150405"), start
}

func (p *Publisher) acquire(signal string) (*generation, func()) {
	st := p.signals[signal]
	id, start := genID(p.cfg.Now(), p.cfg.Generation)
	st.mu.RLock()
	if st.cur != nil && st.cur.id == id {
		return st.cur, st.mu.RUnlock
	}
	st.mu.RUnlock()
	st.mu.Lock()
	if st.cur == nil || st.cur.id != id {
		if old := st.cur; old != nil {
			p.sealWG.Add(1)
			go func() {
				defer p.sealWG.Done()
				_ = p.seal(context.Background(), signal, old)
			}()
		}
		st.cur = &generation{id: id, start: start}
	}
	st.mu.Unlock()
	return p.acquire(signal)
}

// PushTraces publishes td as one batch.
func (p *Publisher) PushTraces(ctx context.Context, td ptrace.Traces) error {
	return p.push(ctx, "traces", func(e BatchEncoder, buf *bytes.Buffer, env *Envelope) (int, error) {
		return e.Traces(buf, td, env)
	})
}

// PushLogs publishes ld as one batch.
func (p *Publisher) PushLogs(ctx context.Context, ld plog.Logs) error {
	return p.push(ctx, "logs", func(e BatchEncoder, buf *bytes.Buffer, env *Envelope) (int, error) {
		return e.Logs(buf, ld, env)
	})
}

func (p *Publisher) push(ctx context.Context, signal string, enc func(BatchEncoder, *bytes.Buffer, *Envelope) (int, error)) error {
	g, unlock := p.acquire(signal)
	defer unlock()
	st := p.signals[signal]
	start := p.cfg.Now()
	env := &Envelope{Producer: p.cfg.ProducerID, Epoch: p.epoch, Batch: st.seq.Add(1),
		Received: uint64(start.UnixNano()), Schema: p.cfg.SchemaVersion}
	e := p.encs.Get().(BatchEncoder)
	defer p.encs.Put(e)
	buf := p.bufs.Get().(*bytes.Buffer)
	defer p.bufs.Put(buf)
	buf.Reset()
	rows, err := enc(e, buf, env)
	if err != nil {
		return fmt.Errorf("parquet encode %s batch %d: %w", signal, env.Batch, err)
	}
	ns := p.namespace(signal)
	objRel := ns + "/" + g.id + "/" + fmt.Sprintf("%020d.parquet", env.Batch)
	if err := p.put(ctx, objRel, buf.Bytes(), "application/vnd.apache.parquet"); err != nil {
		return fmt.Errorf("parquet %s batch %d: %w", signal, env.Batch, err)
	}
	m := batchManifest{
		ProducerID: env.Producer, ProducerEpoch: env.Epoch, Region: p.cfg.Region, Signal: signal,
		SchemaVersion: env.Schema, Generation: g.id, BatchID: env.Batch, Rows: rows, Bytes: buf.Len(),
		ReceivedAt: start.UTC(), MinEventTime: time.Unix(0, int64(env.MinTS)).UTC(), MaxEventTime: time.Unix(0, int64(env.MaxTS)).UTC(),
		Parquet: joinURL(p.cfg.URL, objRel),
	}
	if err := p.put(ctx, ns+"/manifests/"+g.id+"/"+fmt.Sprintf("%020d.json", env.Batch), mustJSON(m), "application/json"); err != nil {
		return fmt.Errorf("manifest %s batch %d: %w", signal, env.Batch, err)
	}
	g.batches.Add(1)
	g.rows.Add(uint64(rows))
	for {
		f := g.first.Load()
		if (f != 0 && f <= env.Batch) || g.first.CompareAndSwap(f, env.Batch) {
			break
		}
	}
	for {
		l := g.last.Load()
		if env.Batch <= l || g.last.CompareAndSwap(l, env.Batch) {
			break
		}
	}
	return nil
}

// put stores data at rel (relative to the URL): one PUT on S3 (the SDK's
// standard retryer, 3 attempts with backoff; one batch is far below the 5 GiB
// single-PUT limit, so there is no multipart), or a local write renamed into
// place.
func (p *Publisher) put(ctx context.Context, rel string, data []byte, contentType string) error {
	if p.s3 == nil {
		path := filepath.Join(p.local, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			return err
		}
		return os.Rename(tmp, path)
	}
	key := rel
	if p.prefix != "" {
		key = p.prefix + "/" + rel
	}
	_, err := p.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(p.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String(contentType),
	})
	return err
}

func (p *Publisher) seal(ctx context.Context, signal string, g *generation) error {
	m := sealManifest{
		ProducerID: p.cfg.ProducerID, ProducerEpoch: p.epoch, Region: p.cfg.Region, Signal: signal,
		SchemaVersion: p.cfg.SchemaVersion, Generation: g.id, GenerationStart: g.start,
		SealedAt: p.cfg.Now().UTC(), Batches: g.batches.Load(), Rows: g.rows.Load(),
		FirstBatch: g.first.Load(), LastBatch: g.last.Load(),
		ParquetPrefix: joinURL(p.cfg.URL, p.namespace(signal), g.id) + "/",
	}
	return p.put(ctx, p.namespace(signal)+"/manifests/"+g.id+"/_sealed.json", mustJSON(m), "application/json")
}

// Close seals every open generation.
func (p *Publisher) Close(ctx context.Context) error {
	var err error
	for signal, st := range p.signals {
		st.mu.Lock()
		g := st.cur
		st.cur = nil
		st.mu.Unlock()
		if g != nil {
			err = errors.Join(err, p.seal(ctx, signal, g))
		}
	}
	p.sealWG.Wait()
	return err
}

// Epoch returns this publisher's producer epoch.
func (p *Publisher) Epoch() string { return p.epoch }

// The manifests are ../chdbexporter/publish.go's, field for field.

type manifestTable struct {
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
	Tables        []manifestTable `json:"tables,omitempty"`
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
	Tables          []manifestTable `json:"tables,omitempty"`
	ParquetPrefix   string          `json:"parquet_prefix,omitempty"`
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}
