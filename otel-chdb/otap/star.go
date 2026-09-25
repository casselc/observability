package otap

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// StarTables returns the batch's OTAP tables as they would be stored
// "natively", one table per payload type, the layout of the Rust
// otap-dataflow parquet exporter:
//
//   - transport-optimised ids are decoded to absolute ids (the Rust exporter
//     does the same, decode_transport_optimized_ids);
//   - instead of the Rust exporter's partition-sequence id offsets and
//     `_part_id` partition directory, every table gets a `batch_id` column,
//     so a consumer joins on (batch_id, id) whatever objects it reads together;
//   - struct columns are flattened (`resource.id` becomes `resource_id`) to
//     keep the central SQL readable; the Rust exporter keeps the structs,
//     which ClickHouse reads as named Tuples, at the same cost;
//   - the root table also carries the envelope;
//   - one modification the central side cannot do without: map and slice
//     values (`ser`, CBOR) are re-encoded as the JSON string
//     pcommon.Value.AsString would give, in `ser_json`. ClickHouse has no
//     CBOR decoder, so without this the Map/Body columns cannot be rebuilt.
//
// Dictionary columns of strings are kept (Parquet stores them dictionary-
// encoded anyway); other dictionaries are unpacked, and durations become
// int64, which Parquet has no type for.
func StarTables(b *Batch, env *parquetgo.Envelope, mem memory.Allocator) (map[pb.ArrowPayloadType]arrow.Record, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	out := map[pb.ArrowPayloadType]arrow.Record{}
	payloads := LogPayloads
	if b.Traces {
		payloads = TracePayloads
	}
	for _, t := range payloads {
		r := b.T[t]
		if r == nil {
			continue
		}
		rec, err := starTable(b, t, r, env, mem)
		if err != nil {
			for _, x := range out {
				x.Release()
			}
			return nil, err
		}
		out[t] = rec
	}
	return out, nil
}

type colSet struct {
	fields []arrow.Field
	cols   []arrow.Array
}

func (c *colSet) add(name string, a arrow.Array) {
	c.fields = append(c.fields, arrow.Field{Name: name, Type: a.DataType(), Nullable: true})
	c.cols = append(c.cols, a)
}

func starTable(b *Batch, t pb.ArrowPayloadType, r arrow.Record, env *parquetgo.Envelope, mem memory.Allocator) (arrow.Record, error) {
	n := rows(r)
	var cs colSet
	cs.add("batch_id", constU64(mem, env.Batch, n))
	root := r == b.Root
	for i, f := range r.Schema().Fields() {
		a := r.Column(i)
		switch {
		case f.Name == "id" && root:
			cs.add("id", idsU32(mem, b.RootID))
		case f.Name == "id":
			cs.add("id", idsU32(mem, b.ChildID[t]))
		case f.Name == "parent_id":
			cs.add("parent_id", u32s(mem, b.Parent[t]))
		case f.Name == "ser":
			cs.add("ser_json", serJSON(mem, a))
		case f.Type.ID() == arrow.STRUCT:
			st := f.Type.(*arrow.StructType)
			s := a.(*array.Struct)
			for j, cf := range st.Fields() {
				name := f.Name + "_" + cf.Name
				switch {
				case cf.Name == "id" && f.Name == "resource":
					cs.add(name, u16s(mem, b.ResID))
				case cf.Name == "id" && f.Name == "scope":
					cs.add(name, u16s(mem, b.ScopeID))
				case cf.Name == "ser":
					cs.add(name+"_json", serJSON(mem, s.Field(j)))
				default:
					m, err := materialize(mem, s.Field(j))
					if err != nil {
						return nil, err
					}
					cs.add(name, m)
				}
			}
		default:
			m, err := materialize(mem, a)
			if err != nil {
				return nil, err
			}
			cs.add(f.Name, m)
		}
	}
	if root {
		sb := array.NewStringBuilder(mem)
		eb := array.NewStringBuilder(mem)
		ob := array.NewUint32Builder(mem)
		rb := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
		vb := array.NewUint16Builder(mem)
		for i := 0; i < n; i++ {
			sb.Append(env.Producer)
			eb.Append(env.Epoch)
			ob.Append(uint32(i))
			rb.Append(arrow.Timestamp(env.Received))
			vb.Append(env.Schema)
		}
		cs.add("producer_id", sb.NewArray())
		cs.add("producer_epoch", eb.NewArray())
		cs.add("row_ordinal", ob.NewArray())
		cs.add("received_at", rb.NewArray())
		cs.add("schema_version", vb.NewArray())
	}
	rec := array.NewRecordBatch(arrow.NewSchema(cs.fields, nil), cs.cols, int64(n))
	for _, c := range cs.cols {
		c.Release()
	}
	return rec, nil
}

func constU64(mem memory.Allocator, v uint64, n int) arrow.Array {
	b := array.NewUint64Builder(mem)
	defer b.Release()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		b.UnsafeAppend(v)
	}
	return b.NewArray()
}

func idsU32(mem memory.Allocator, ids []int64) arrow.Array {
	b := array.NewUint32Builder(mem)
	defer b.Release()
	b.Reserve(len(ids))
	for _, id := range ids {
		if id < 0 {
			b.AppendNull()
		} else {
			b.Append(uint32(id))
		}
	}
	return b.NewArray()
}

func u32s(mem memory.Allocator, v []uint32) arrow.Array {
	b := array.NewUint32Builder(mem)
	defer b.Release()
	b.AppendValues(v, nil)
	return b.NewArray()
}

func u16s(mem memory.Allocator, v []uint16) arrow.Array {
	b := array.NewUint16Builder(mem)
	defer b.Release()
	b.AppendValues(v, nil)
	return b.NewArray()
}

func serJSON(mem memory.Allocator, a arrow.Array) arrow.Array {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for i := 0; i < a.Len(); i++ {
		x, ok := bytesAt(a, i)
		if !ok {
			b.AppendNull()
			continue
		}
		v := pcommon.NewValueEmpty()
		if decodeCBOR(x, v) != nil {
			b.AppendNull()
			continue
		}
		b.Append(v.AsString())
	}
	return b.NewArray()
}

// materialize keeps plain columns and string dictionaries, unpacks other
// dictionaries, and turns durations into int64.
func materialize(mem memory.Allocator, a arrow.Array) (arrow.Array, error) {
	if d, ok := a.(*array.Dictionary); ok {
		switch d.Dictionary().DataType().ID() {
		case arrow.STRING, arrow.BINARY:
			a.Retain()
			return a, nil
		}
		ctx := compute.WithAllocator(context.Background(), mem)
		out, err := compute.TakeArray(ctx, d.Dictionary(), d.Indices())
		if err != nil {
			return nil, err
		}
		a = out
	} else {
		a.Retain()
	}
	if a.DataType().ID() == arrow.DURATION {
		data := a.Data()
		nd := array.NewData(arrow.PrimitiveTypes.Int64, data.Len(), data.Buffers(), nil, data.NullN(), data.Offset())
		out := array.MakeFromData(nd)
		nd.Release()
		a.Release()
		return out, nil
	}
	return a, nil
}
