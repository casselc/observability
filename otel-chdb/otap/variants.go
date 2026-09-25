package otap

import (
	"bytes"
	"errors"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/proto"
)

// Variants of publishing one batch.
const (
	// Ref is parquetgo's parquet-go engine straight from pdata: the
	// reference the other variants are checked against.
	Ref = "ref"
	// Star writes each OTAP table as its own Parquet object (option a).
	Star = "star"
	// FlatParquet denormalises OTAP to the ClickStack shape at the edge and
	// writes one Parquet object (option b, Arrow-native).
	FlatParquet = "flat-parquet"
	// FlatArrow is the same record as an Arrow IPC file (option c).
	FlatArrow = "flat-arrow"
	// ViaPdata decodes OTAP to pdata with the Go library's consumer, then
	// parquetgo: what a Go collector with the otelarrow receiver would do
	// (option b, Go collector).
	ViaPdata = "via-pdata"
	// BAR stores the BatchArrowRecords protobuf as received (option d).
	// ClickHouse cannot read it.
	BAR = "bar"
)

// ErrLibraryDroppedBatch: the Go library's consumer returned no data and
// no error. TracesFrom/LogsFrom discard the error from RelatedDataFrom, so a
// batch it cannot decode (for example a map or slice attribute holding
// invalid UTF-8: its CBOR decoder rejects it) comes back empty. The contrib
// otelarrowreceiver then counts 0 items and acknowledges the request.
var ErrLibraryDroppedBatch = errors.New("otap: otel-arrow consumer returned no data and no error (batch dropped)")

// Publisher publishes batches in one of the variants, with manifests.
type Publisher struct {
	Sink          *Sink
	Producer      string
	Epoch         string
	SchemaVersion uint16
	Parquet       parquetgo.Options
	Now           func() time.Time
	// CreateOnly sends If-None-Match: * on every PUT.
	CreateOnly bool
	Mem        memory.Allocator

	seq map[string]uint64
	pg  *parquetgo.PGEncoder
	buf bytes.Buffer
}

func (p *Publisher) next(signal string) uint64 {
	if p.seq == nil {
		p.seq = map[string]uint64{}
	}
	p.seq[signal]++
	return p.seq[signal]
}

func (p *Publisher) envelope(signal string) (*parquetgo.Envelope, time.Time) {
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	t := now()
	return &parquetgo.Envelope{Producer: p.Producer, Epoch: p.Epoch, Batch: p.next(signal),
		Received: uint64(t.UnixNano()), Schema: p.SchemaVersion}, t
}

func (p *Publisher) rel(signal, variant string, batch uint64, name string) string {
	return fmt.Sprintf("%s/%s/%020d/%s", signal, variant, batch, name)
}

// ContentKey hashes the batch's Arrow payloads (type and IPC bytes).
func ContentKey(bar *pb.BatchArrowRecords) string {
	h := sha256.New()
	for _, pl := range bar.ArrowPayloads {
		fmt.Fprintf(h, "%d:%d:", pl.Type, len(pl.Record))
		h.Write(pl.Record)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// PublishRef publishes pdata through parquetgo's parquet-go engine.
func (p *Publisher) PublishRef(ctx context.Context, td *ptrace.Traces, ld *plog.Logs) (Manifest, error) {
	if p.pg == nil {
		p.pg = parquetgo.NewPGEncoder(p.Parquet)
	}
	signal := "logs"
	if td != nil {
		signal = "traces"
	}
	env, t := p.envelope(signal)
	p.buf.Reset()
	var n int
	var err error
	if td != nil {
		n, err = p.pg.Traces(&p.buf, *td, env)
	} else {
		n, err = p.pg.Logs(&p.buf, *ld, env)
	}
	if err != nil {
		return Manifest{}, err
	}
	m := p.manifest(signal, Ref, env, t, n)
	if err := p.put(ctx, &m, signal, Ref, env.Batch, "otel_"+signal, int64(n), p.buf.Bytes(), ".parquet"); err != nil {
		return m, err
	}
	return m, p.commit(ctx, m)
}

// PublishBAR publishes one OTAP batch in the given variant.
func (p *Publisher) PublishBAR(ctx context.Context, variant string, bar *pb.BatchArrowRecords) (Manifest, error) {
	if variant == Raw {
		return p.PublishRaw(ctx, bar)
	}
	signal := "logs"
	for _, pl := range bar.ArrowPayloads {
		if pl.Type == Spans {
			signal = "traces"
		}
	}
	env, t := p.envelope(signal)
	var m Manifest
	switch variant {
	case BAR:
		raw, err := proto.Marshal(bar)
		if err != nil {
			return m, err
		}
		m = p.manifest(signal, variant, env, t, -1)
		if err := p.put(ctx, &m, signal, variant, env.Batch, "bar", -1, raw, ".pb"); err != nil {
			return m, err
		}
	case ViaPdata:
		c := arrow_record.NewConsumer()
		defer c.Close()
		if p.pg == nil {
			p.pg = parquetgo.NewPGEncoder(p.Parquet)
		}
		p.buf.Reset()
		var n int
		if signal == "traces" {
			tds, err := c.TracesFrom(bar)
			if err != nil {
				return m, err
			}
			if len(tds) == 0 {
				return m, ErrLibraryDroppedBatch
			}
			if n, err = p.pg.Traces(&p.buf, tds[0], env); err != nil {
				return m, err
			}
		} else {
			lds, err := c.LogsFrom(bar)
			if err != nil {
				return m, err
			}
			if len(lds) == 0 {
				return m, ErrLibraryDroppedBatch
			}
			if n, err = p.pg.Logs(&p.buf, lds[0], env); err != nil {
				return m, err
			}
		}
		m = p.manifest(signal, variant, env, t, n)
		if err := p.put(ctx, &m, signal, variant, env.Batch, "otel_"+signal, int64(n), p.buf.Bytes(), ".parquet"); err != nil {
			return m, err
		}
	case Star, FlatParquet, FlatArrow:
		b, err := Decode(bar)
		if err != nil {
			return m, err
		}
		defer b.Release()
		m = p.manifest(signal, variant, env, t, rows(b.Root))
		if variant == Star {
			tabs, err := StarTables(b, env, p.Mem)
			if err != nil {
				return m, err
			}
			defer func() {
				for _, r := range tabs {
					r.Release()
				}
			}()
			payloads := LogPayloads
			if b.Traces {
				payloads = TracePayloads
			}
			// Root timestamps for the manifest's event-time range.
			f := &flattener{b: b, env: env}
			f.eventRange()
			for _, pt := range payloads {
				r := tabs[pt]
				if r == nil {
					continue
				}
				p.buf.Reset()
				if err := WriteParquet(&p.buf, r, p.Parquet, p.Mem); err != nil {
					return m, fmt.Errorf("%s: %w", TableName(pt), err)
				}
				if err := p.put(ctx, &m, signal, variant, env.Batch, TableName(pt), r.NumRows(), p.buf.Bytes(), ".parquet"); err != nil {
					return m, err
				}
			}
		} else {
			rec := Flatten(b, env, p.Mem)
			defer rec.Release()
			p.buf.Reset()
			ext := ".parquet"
			if variant == FlatParquet {
				err = WriteParquet(&p.buf, rec, p.Parquet, p.Mem)
			} else {
				ext = ".arrow"
				err = WriteArrowFile(&p.buf, rec, p.Parquet.Compression, p.Mem)
			}
			if err != nil {
				return m, err
			}
			if err := p.put(ctx, &m, signal, variant, env.Batch, "otel_"+signal, rec.NumRows(), p.buf.Bytes(), ext); err != nil {
				return m, err
			}
		}
		m.Rows = rows(b.Root)
	default:
		return m, fmt.Errorf("unknown variant %q", variant)
	}
	m.ContentKey = ContentKey(bar)
	m.MinEventTime = time.Unix(0, int64(env.MinTS)).UTC()
	m.MaxEventTime = time.Unix(0, int64(env.MaxTS)).UTC()
	return m, p.commit(ctx, m)
}

// eventRange fills the envelope's event-time range from the root table
// (Flatten does this as it goes; the star path needs it separately).
func (f *flattener) eventRange() {
	r := f.b.Root
	c := col(r, "start_time_unix_nano")
	if !f.b.Traces {
		c = col(r, "time_unix_nano")
	}
	for i := 0; i < rows(r); i++ {
		ts, _ := uintAt(c, i)
		if f.env.MinTS == 0 || ts < f.env.MinTS {
			f.env.MinTS = ts
		}
		if ts > f.env.MaxTS {
			f.env.MaxTS = ts
		}
	}
}

func (p *Publisher) manifest(signal, variant string, env *parquetgo.Envelope, t time.Time, rows int) Manifest {
	return Manifest{ProducerID: p.Producer, ProducerEpoch: p.Epoch, Signal: signal, SchemaVersion: p.SchemaVersion,
		Layout: variant, BatchID: env.Batch, Rows: rows, ReceivedAt: t.UTC(),
		MinEventTime: time.Unix(0, int64(env.MinTS)).UTC(), MaxEventTime: time.Unix(0, int64(env.MaxTS)).UTC()}
}

func (p *Publisher) put(ctx context.Context, m *Manifest, signal, variant string, batch uint64, table string, n int64, data []byte, ext string) error {
	rel := p.rel(signal, variant, batch, table+ext)
	if err := p.Sink.Put(ctx, rel, data, p.CreateOnly); err != nil {
		return fmt.Errorf("put %s: %w", rel, err)
	}
	m.Objects = append(m.Objects, Object{Table: table, URL: p.Sink.ObjectURL(rel), Rows: n, Bytes: len(data)})
	return nil
}

func (p *Publisher) commit(ctx context.Context, m Manifest) error {
	rel := fmt.Sprintf("%s/%s/manifests/%020d.json", m.Signal, m.Layout, m.BatchID)
	return p.Sink.Put(ctx, rel, m.JSON(), p.CreateOnly)
}
