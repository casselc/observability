// Package inline is the manifest-less object format and protocol: the data
// object is the commit record (see ../README.md and ../../model/s3Inline.qnt).
//
//	{prefix}/{epoch}/{seq:020d}.parquet
//
// Each epoch is one log. A writer lane appends batches to its own epoch at
// consecutive sequence numbers with PUT If-None-Match: *, and carries the
// batch description in S3 user metadata (x-amz-meta-oscope-*) and in the
// Parquet footer's key-value metadata. The consumer closes a dead epoch by
// racing a zero-byte create-only tombstone into its first free slot.
package inline

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Metadata keys. S3 stores them as x-amz-meta-<key>, lower case. The same
// keys, with the "oscope." prefix, go into the Parquet footer.
const (
	MetaKind     = "oscope-kind" // "data" or "tomb"
	MetaProducer = "oscope-producer"
	MetaEpoch    = "oscope-epoch"
	MetaSeq      = "oscope-seq"
	MetaContent  = "oscope-content" // content hash of the OTLP request (hex)
	MetaSignal   = "oscope-signal"
	MetaSchema   = "oscope-schema"
	MetaRows     = "oscope-rows"
	MetaMinTime  = "oscope-min-time" // ns since epoch
	MetaMaxTime  = "oscope-max-time"
	MetaReceived = "oscope-received"

	KindData = "data"
	KindTomb = "tomb"
)

// SlotKey is the object key of slot seq in an epoch's log. Zero padding
// makes lexicographic order (LIST order) equal to slot order.
func SlotKey(prefix, epoch string, seq uint64) string {
	return strings.TrimRight(prefix, "/") + "/" + epoch + "/" + fmt.Sprintf("%020d", seq) + ".parquet"
}

// ParseSlotKey returns the epoch and slot of a key made by SlotKey.
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

// ContentHashTraces identifies an OTLP request by content: SHA-256 of its
// protobuf encoding, 128 bits in hex. The persistent queue stores requests
// as protobuf, so a request read back after a restart hashes the same.
func ContentHashTraces(td ptrace.Traces) (string, error) {
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		return "", err
	}
	return hashBytes("traces", b), nil
}

func ContentHashLogs(ld plog.Logs) (string, error) {
	b, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		return "", err
	}
	return hashBytes("logs", b), nil
}

func hashBytes(signal string, b []byte) string {
	h := sha256.New()
	h.Write([]byte(signal))
	h.Write([]byte{0})
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// AddFooterKV appends key-value metadata to a Parquet file's footer. Only the
// footer changes; data pages, page indexes and bloom filters keep their
// offsets, so the rest of the file is untouched.
func AddFooterKV(file []byte, kv map[string]string) ([]byte, error) {
	n := len(file)
	if n < 12 || !bytes.Equal(file[n-4:], []byte("PAR1")) {
		return nil, errors.New("not a Parquet file")
	}
	flen := int(binary.LittleEndian.Uint32(file[n-8 : n-4]))
	if flen <= 0 || flen > n-12 {
		return nil, errors.New("bad Parquet footer length")
	}
	start := n - 8 - flen
	var md format.FileMetaData
	proto := &thrift.CompactProtocol{}
	if err := thrift.Unmarshal(proto, file[start:n-8], &md); err != nil {
		return nil, fmt.Errorf("decode footer: %w", err)
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		md.KeyValueMetadata = append(md.KeyValueMetadata, format.KeyValue{Key: k, Value: kv[k]})
	}
	footer, err := thrift.Marshal(proto, &md)
	if err != nil {
		return nil, fmt.Errorf("encode footer: %w", err)
	}
	out := make([]byte, 0, start+len(footer)+8)
	out = append(out, file[:start]...)
	out = append(out, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	return append(out, "PAR1"...), nil
}

// FooterKV reads a Parquet file's key-value metadata.
func FooterKV(file []byte) (map[string]string, error) {
	n := len(file)
	if n < 12 || !bytes.Equal(file[n-4:], []byte("PAR1")) {
		return nil, errors.New("not a Parquet file")
	}
	flen := int(binary.LittleEndian.Uint32(file[n-8 : n-4]))
	var md format.FileMetaData
	if err := thrift.Unmarshal(&thrift.CompactProtocol{}, file[n-8-flen:n-8], &md); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, e := range md.KeyValueMetadata {
		out[e.Key] = e.Value
	}
	return out, nil
}
