// Package parquetencoding is a collector encoding extension that marshals
// traces and logs to Parquet in the otel_traces / otel_logs schema the
// ClickHouse exporter uses (ClickStack), with the edge envelope columns, via
// ../../parquetgo (parquet-go). It works two ways:
//
//   - As a plain ptrace/plog Marshaler, usable by the stock awss3exporter
//     (`encoding: parquet_encoding`). The marshaler sees only pdata, so the
//     envelope's epoch is this process's and batch_id is a local counter: it
//     cannot know which object key, attempt or retry the bytes are for.
//   - Through MarshalTracesSlot / MarshalLogsSlot, which the patched exporter
//     (../awss3inline, key_mode: sequence) calls with the epoch and slot the
//     batch is being committed at, and which also return the S3 user
//     metadata that describes the batch.
//
// Either way the Parquet footer carries the batch description as key-value
// metadata (oscope-*), so no separate manifest is needed to know it.
package parquetencoding

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/inline"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

var typ = component.MustNewType("parquet_encoding")

// Config of the extension.
type Config struct {
	ProducerID    string `mapstructure:"producer_id"`
	SchemaVersion uint16 `mapstructure:"schema_version"`
	Compression   string `mapstructure:"compression"`   // zstd (default), snappy, lz4, gzip, none
	BloomFilters  bool   `mapstructure:"bloom_filters"` // default false: 35-60% of the file (../../parquetgo/README.md)
}

func (c *Config) Validate() error {
	if c.ProducerID == "" {
		return errors.New("producer_id is required")
	}
	return nil
}

func NewFactory() extension.Factory {
	return extension.NewFactory(typ,
		func() component.Config { return &Config{SchemaVersion: 1, Compression: "zstd"} },
		func(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
			return New(*cfg.(*Config)), nil
		},
		component.StabilityLevelDevelopment)
}

// Ext is the extension.
type Ext struct {
	cfg   Config
	epoch string
	seq   atomic.Uint64
	pool  sync.Pool
}

func New(cfg Config) *Ext {
	opts := parquetgo.DefaultOptions()
	if cfg.Compression != "" {
		opts.Compression = cfg.Compression
	}
	opts.BloomFilters = cfg.BloomFilters
	var b [3]byte
	_, _ = rand.Read(b[:])
	e := &Ext{cfg: cfg, epoch: time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])}
	e.pool.New = func() any { return parquetgo.NewPGEncoder(opts) }
	return e
}

func (*Ext) Start(context.Context, component.Host) error { return nil }
func (*Ext) Shutdown(context.Context) error              { return nil }

const ContentType = "application/vnd.apache.parquet"

func (e *Ext) encode(signal, epoch string, seq uint64, write func(*parquetgo.PGEncoder, *bytes.Buffer, *parquetgo.Envelope) (int, error)) ([]byte, map[string]string, error) {
	enc := e.pool.Get().(*parquetgo.PGEncoder)
	defer e.pool.Put(enc)
	now := time.Now()
	env := &parquetgo.Envelope{Producer: e.cfg.ProducerID, Epoch: epoch, Batch: seq,
		Received: uint64(now.UnixNano()), Schema: e.cfg.SchemaVersion}
	var buf bytes.Buffer
	rows, err := write(enc, &buf, env)
	if err != nil {
		return nil, nil, err
	}
	meta := map[string]string{
		inline.MetaProducer: e.cfg.ProducerID, inline.MetaSignal: signal,
		inline.MetaSchema: strconv.Itoa(int(e.cfg.SchemaVersion)), inline.MetaRows: strconv.Itoa(rows),
		inline.MetaMinTime: strconv.FormatUint(env.MinTS, 10), inline.MetaMaxTime: strconv.FormatUint(env.MaxTS, 10),
		inline.MetaReceived: strconv.FormatUint(env.Received, 10),
		inline.MetaEpoch:    epoch, inline.MetaSeq: strconv.FormatUint(seq, 10),
	}
	body, err := inline.AddFooterKV(buf.Bytes(), meta)
	return body, meta, err
}

// MarshalTraces implements ptrace.Marshaler (the stock awss3exporter's view).
func (e *Ext) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	b, meta, err := e.MarshalTracesSlot(td, e.epoch, e.seq.Add(1))
	if err != nil {
		return nil, err
	}
	if h, err := inline.ContentHashTraces(td); err == nil {
		// Too late for the key (the stock exporter chose it already), but a
		// consumer can deduplicate on it.
		meta[inline.MetaContent] = h
		return inline.AddFooterKV(b, map[string]string{inline.MetaContent: h})
	}
	return b, nil
}

func (e *Ext) MarshalLogs(ld plog.Logs) ([]byte, error) {
	b, _, err := e.MarshalLogsSlot(ld, e.epoch, e.seq.Add(1))
	if err != nil {
		return nil, err
	}
	if h, err := inline.ContentHashLogs(ld); err == nil {
		return inline.AddFooterKV(b, map[string]string{inline.MetaContent: h})
	}
	return b, nil
}

// MarshalTracesSlot encodes td for slot seq of log epoch and returns the
// object's content type and S3 user metadata. The patched exporter finds it
// by this method signature (no import of this package needed).
func (e *Ext) MarshalTracesSlot(td ptrace.Traces, epoch string, seq uint64) ([]byte, map[string]string, error) {
	return e.encode("traces", epoch, seq, func(enc *parquetgo.PGEncoder, b *bytes.Buffer, env *parquetgo.Envelope) (int, error) {
		return enc.Traces(b, td, env)
	})
}

func (e *Ext) MarshalLogsSlot(ld plog.Logs, epoch string, seq uint64) ([]byte, map[string]string, error) {
	return e.encode("logs", epoch, seq, func(enc *parquetgo.PGEncoder, b *bytes.Buffer, env *parquetgo.Envelope) (int, error) {
		return enc.Logs(b, ld, env)
	})
}

// ObjectContentType is what the patched exporter sends as Content-Type.
func (*Ext) ObjectContentType() string { return ContentType }
