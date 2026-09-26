// Package commit is the Go edge's manifest-less commit protocol: the data
// object is the commit record (../../DECISIONS.md D3, ../../model/s3Inline.qnt).
//
//	{prefix}/{signal}/{epoch}/{seq:020d}.parquet
//
// Each (signal, lane) appends batches to its own epoch's log at consecutive
// slots with PUT If-None-Match: *, and carries the batch description in S3
// user metadata (x-amz-meta-oscope-*) and in the Parquet footer. On a 412 or
// no answer it HEADs the slot: ours → done; free → resend the identical
// bytes; another batch → learn it, next slot; a consumer's tombstone → halt
// and start a new epoch.
//
// It is a port of the Rust exporter's lane (../../otap-rs/src/proto.rs and
// runner.rs), which is itself ../../awss3/inline/log.go's Log.Append as a
// state machine; object keys, epochs, metadata keys and content keys are the
// Rust edge's, byte for byte, so the one consumer (otap-rs `consume`) reads
// both edges' objects.
package commit

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/blake3"
)

// Metadata keys: S3 user metadata `x-amz-meta-<key>`, and the same keys in
// the Parquet footer (../../otap-rs/src/proto.rs).
const (
	MetaKind     = "oscope-kind"
	MetaProducer = "oscope-producer"
	MetaEpoch    = "oscope-epoch"
	MetaSeq      = "oscope-seq"
	MetaContent  = "oscope-content"
	MetaSignal   = "oscope-signal"
	MetaSchema   = "oscope-schema"
	MetaRows     = "oscope-rows"
	MetaMinTime  = "oscope-min-time"
	MetaMaxTime  = "oscope-max-time"
	MetaReceived = "oscope-received"

	KindData = "data"
	KindTomb = "tomb"

	// ParquetContentType is the objects' Content-Type.
	ParquetContentType = "application/vnd.apache.parquet"
)

// SlotKey is the key of slot seq in epoch's log under prefix (which ends in
// the signal). Zero padding makes LIST order slot order.
func SlotKey(prefix, epoch string, seq uint64) string {
	return strings.TrimRight(prefix, "/") + "/" + epoch + "/" + fmt.Sprintf("%020d", seq) + ".parquet"
}

// ParseSlotKey parses a key made by SlotKey.
func ParseSlotKey(prefix, key string) (epoch string, seq uint64, ok bool) {
	rest, found := strings.CutPrefix(key, strings.TrimRight(prefix, "/")+"/")
	if !found {
		return "", 0, false
	}
	epoch, name, found := strings.Cut(rest, "/")
	if !found || !strings.HasSuffix(name, ".parquet") {
		return "", 0, false
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(name, ".parquet"), 10, 64)
	if err != nil {
		return "", 0, false
	}
	return epoch, n, true
}

// NewEpoch names a new log: `YYYYMMDDTHHMMSS.mmmZ-<8 hex>`, sortable by start
// time, unique by a random suffix (the Rust edge's `proto::new_epoch`). The
// consumer's checkpoint compaction assumes names from wall-clock time.
func NewEpoch() string { return EpochAt(time.Now()) }

// EpochAt is NewEpoch at time t.
func EpochAt(t time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return t.UTC().Format("20060102T150405.000Z") + fmt.Sprintf("-%08x", binary.BigEndian.Uint32(b[:]))
}

// ContentHash is the content key of an OTLP request: BLAKE3 over
// "{signal}\0" and the request's protobuf bytes, 128 bits in hex. The Rust
// edge hashes the bytes it received; a Go collector re-marshals the pdata,
// which gives the same bytes for a request a Go sender marshalled (the
// persistent queue stores it the same way), so a request hashes alike in
// either edge and across restarts.
func ContentHash(signal string, request []byte) string {
	h := blake3.New()
	_, _ = h.Write([]byte(signal))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(request)
	var sum [32]byte
	return hex.EncodeToString(h.Sum(sum[:0])[:16])
}

// RowsHasher keys an object by its rows rather than by a request (a layout-B
// series object, whose content depends on the series cache): BLAKE3 over
// "rows:{signal}" and whatever the caller feeds it, 128 bits in hex.
type RowsHasher struct{ h *blake3.Hasher }

func NewRowsHasher(signal string) RowsHasher {
	h := blake3.New()
	_, _ = h.Write([]byte("rows:" + signal))
	return RowsHasher{h}
}

func (r RowsHasher) Write(b []byte) { _, _ = r.h.Write(b) }

func (r RowsHasher) Sum() string {
	var sum [32]byte
	return hex.EncodeToString(r.h.Sum(sum[:0])[:16])
}
