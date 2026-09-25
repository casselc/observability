package probe

// Tests of what the manifest-less design assumes of S3 and of the ClickHouse
// reader, against SeaweedFS and a ClickHouse server. Skipped unless
// INLINE_S3 is set:
//
//	INLINE_S3=http://127.0.0.1:18333 INLINE_CH=http://127.0.0.1:18123 go test -v -count=1 ./probe
//
// Everything is written under otel/s3inline/probe/<run>/ (bucket otel; no
// new buckets). Each test logs what the servers answered.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/inline"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/parquetencoding"
)

const bucket = "otel"

var runID = time.Now().UTC().Format("20060102T150405")

func endpoint(t *testing.T) string {
	e := os.Getenv("INLINE_S3")
	if e == "" {
		t.Skip("INLINE_S3 not set")
	}
	return e
}

func newClient(t *testing.T, rt http.RoundTripper) *s3.Client {
	o := s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(endpoint(t)), UsePathStyle: true,
		Credentials:      credentials.NewStaticCredentialsProvider("otel", "otelsecret", ""),
		RetryMaxAttempts: 1, // the SDK's own retries would hide the outcomes probed here
	}
	if rt != nil {
		o.HTTPClient = &http.Client{Transport: rt}
	}
	return s3.New(o)
}

func store(t *testing.T, rt http.RoundTripper) *inline.Store {
	return &inline.Store{S3: newClient(t, rt), Bucket: bucket, Prefix: "s3inline/probe/" + runID + "/" + t.Name()}
}

func traces(n int, seed int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "probe")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := range n {
		sp := ss.Spans().AppendEmpty()
		sp.SetName(fmt.Sprintf("op-%d-%d", seed, i))
		sp.SetTraceID(pcommon.TraceID([16]byte{byte(seed), byte(i), 1}))
		sp.SetSpanID(pcommon.SpanID([8]byte{byte(seed), byte(i), 2}))
		sp.SetStartTimestamp(pcommon.Timestamp(1_758_800_000_000_000_000 + int64(seed)*1_000_000 + int64(i)))
		sp.SetEndTimestamp(sp.StartTimestamp() + 1000)
		sp.Attributes().PutStr("k", "v")
	}
	return td
}

var ext = parquetencoding.New(parquetencoding.Config{ProducerID: "probe", SchemaVersion: 1, Compression: "zstd"})

func encoder(td ptrace.Traces) inline.Encoder {
	return func(epoch string, seq uint64) (inline.Object, error) {
		b, meta, err := ext.MarshalTracesSlot(td, epoch, seq)
		return inline.Object{Body: b, ContentType: parquetencoding.ContentType, Meta: meta}, err
	}
}

// Create-only PUT carrying user metadata: 200, then 412 for a second create
// with other metadata, and HEAD still returns the first object's metadata
// and content type.
func TestCreateOnlyWithMetadata(t *testing.T) {
	st, ctx := store(t, nil), context.Background()
	k := st.Prefix + "/obj"
	err := st.PutIfAbsent(ctx, k, []byte("first"), "application/vnd.apache.parquet", map[string]string{"oscope-content": "aaa", "oscope-rows": "7"})
	t.Logf("PUT If-None-Match:* + x-amz-meta-*: %v", inline.Classify(err))
	if inline.Classify(err) != inline.OK {
		t.Fatal(err)
	}
	err = st.PutIfAbsent(ctx, k, []byte("second"), "text/plain", map[string]string{"oscope-content": "bbb"})
	t.Logf("second PUT: %v (%v)", inline.Classify(err), err)
	if inline.Classify(err) != inline.Exists {
		t.Fatalf("want 412, got %v", err)
	}
	out, err := st.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("HEAD: metadata %v, content-type %q, length %d, etag %s", out.Metadata, aws.ToString(out.ContentType),
		aws.ToInt64(out.ContentLength), aws.ToString(out.ETag))
	if out.Metadata["oscope-content"] != "aaa" || out.Metadata["oscope-rows"] != "7" || aws.ToString(out.ContentType) != "application/vnd.apache.parquet" {
		t.Fatalf("metadata not the first writer's: %v", out.Metadata)
	}
}

// A tombstone takes a slot: a data PUT afterwards gets 412 and HEAD shows the tombstone.
func TestTombstoneBlocksData(t *testing.T) {
	st, ctx := store(t, nil), context.Background()
	r, err := st.Tombstone(ctx, "e1", 3)
	t.Logf("tombstone e1/3: %v %v", r, err)
	if r != inline.TombWon {
		t.Fatal(r, err)
	}
	err = st.PutIfAbsent(ctx, inline.SlotKey(st.Prefix, "e1", 3), []byte("PAR1late"), "", map[string]string{inline.MetaKind: inline.KindData})
	t.Logf("late data PUT into the tombstoned slot: %v", inline.Classify(err))
	if inline.Classify(err) != inline.Exists {
		t.Fatal(err)
	}
	// And the reverse: data first, then the consumer's tombstone loses.
	if err := st.PutIfAbsent(ctx, inline.SlotKey(st.Prefix, "e1", 4), []byte("PAR1data"), "", map[string]string{inline.MetaKind: inline.KindData}); err != nil {
		t.Fatal(err)
	}
	r, err = st.Tombstone(ctx, "e1", 4)
	t.Logf("tombstone into a slot holding data: %v %v", r, err)
	if r != inline.DataWon {
		t.Fatal(r, err)
	}
}

// LIST with StartAfter returns later slots in slot order; a delimiter LIST
// returns the epochs.
func TestListStartAfter(t *testing.T) {
	st, ctx := store(t, nil), context.Background()
	for _, e := range []string{"e1", "e2"} {
		for _, s := range []uint64{0, 1, 2, 5, 9, 10, 11, 100} {
			if err := st.PutIfAbsent(ctx, inline.SlotKey(st.Prefix, e, s), []byte("x"), "", nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	eps, err := st.Epochs(ctx)
	t.Logf("epochs: %v %v", eps, err)
	if len(eps) != 2 {
		t.Fatal(eps)
	}
	got, err := st.After(ctx, "e1", 3)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	for _, s := range got {
		seqs = append(seqs, s.Seq)
	}
	t.Logf("e1 after slot 2 (StartAfter %s): %v", inline.SlotKey(st.Prefix, "e1", 2), seqs)
	if fmt.Sprint(seqs) != "[5 9 10 11 100]" {
		t.Fatal(seqs)
	}
}

// faultRT forwards every request but can drop the answer of the n-th PUT
// (the write lands, the client sees an error: the ambiguous outcome) or hold
// it back past the client's deadline (it lands late, after the client gave up).
type faultRT struct {
	base     http.RoundTripper
	puts     atomic.Int64
	dropPut  int64 // drop the response of this PUT (1-based)
	delayPut int64 // forward this PUT only after delay
	delay    time.Duration
	landed   chan struct{} // closed when the delayed PUT has been applied
	landOnce sync.Once
}

func (f *faultRT) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodPut {
		return f.base.RoundTrip(r)
	}
	n := f.puts.Add(1)
	switch n {
	case f.dropPut:
		resp, err := f.base.RoundTrip(r)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, errors.New("fault: connection reset after the request was sent")
	case f.delayPut:
		body, _ := io.ReadAll(r.Body)
		late := r.Clone(context.Background())
		late.Body = io.NopCloser(bytes.NewReader(body))
		go func() {
			time.Sleep(f.delay)
			resp, err := f.base.RoundTrip(late)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			f.landOnce.Do(func() { close(f.landed) })
		}()
		<-r.Context().Done()
		return nil, r.Context().Err()
	}
	return f.base.RoundTrip(r)
}

func listAll(t *testing.T, st *inline.Store) []string {
	out, err := st.S3.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(st.Prefix + "/")})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, strings.TrimPrefix(aws.ToString(o.Key), st.Prefix+"/"))
	}
	return keys
}

// F1: the PUT lands, the response is lost. Append HEADs the slot, finds its
// own batch, and returns success: one object, one commit.
func TestAppendAmbiguousLanded(t *testing.T) {
	f := &faultRT{base: http.DefaultTransport, dropPut: 1}
	st := store(t, f)
	lg := inline.NewLog(st, "probe", nil)
	td := traces(50, 1)
	h, _ := inline.ContentHashTraces(td)
	ref, err := lg.Append(context.Background(), h, encoder(td))
	keys := listAll(t, st)
	t.Logf("append: %+v err=%v; stats committed=%d resolvedOwn=%d; objects %v", ref, err,
		lg.Stats.Committed.Load(), lg.Stats.ResolvedOwn.Load(), keys)
	if err != nil || lg.Stats.ResolvedOwn.Load() != 1 || len(keys) != 1 {
		t.Fatal("want one object resolved as ours")
	}
	// The queue's retry of the same request (a new Append call) commits nothing new.
	ref2, err := lg.Append(context.Background(), h, encoder(td))
	t.Logf("retry of the same request: %+v err=%v knownSkipped=%d objects=%d", ref2, err, lg.Stats.KnownSkipped.Load(), len(listAll(t, st)))
	if ref2 != ref || len(listAll(t, st)) != 1 {
		t.Fatal("retry committed again")
	}
}

// F1, late: the PUT is still in flight when the exporter's deadline passes;
// the exporter returns an error and the queue retries. The retry finds the
// slot free, resends; the late copy lands too. If-None-Match lets one win,
// and the loser is resolved by HEAD. One object either way.
func TestAppendLateRequest(t *testing.T) {
	f := &faultRT{base: http.DefaultTransport, delayPut: 1, delay: 1500 * time.Millisecond, landed: make(chan struct{})}
	st := store(t, f)
	lg := inline.NewLog(st, "probe", nil)
	td := traces(50, 2)
	h, _ := inline.ContentHashTraces(td)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, err := lg.Append(ctx, h, encoder(td))
	cancel()
	t.Logf("first attempt: %v (exporter timeout)", err)
	ref, err := lg.Append(context.Background(), h, encoder(td)) // exporterhelper retry
	t.Logf("retry: %+v err=%v", ref, err)
	<-f.landed
	time.Sleep(200 * time.Millisecond)
	keys := listAll(t, st)
	t.Logf("after the late copy was applied: objects %v; committed=%d resolvedOwn=%d resent=%d", keys,
		lg.Stats.Committed.Load(), lg.Stats.ResolvedOwn.Load(), lg.Stats.Resent.Load())
	if err != nil || len(keys) != 1 {
		t.Fatal("want exactly one object")
	}
	// A new batch after that goes to the next slot, not over the late copy.
	td2 := traces(10, 3)
	h2, _ := inline.ContentHashTraces(td2)
	ref2, err := lg.Append(context.Background(), h2, encoder(td2))
	t.Logf("next batch: %+v err=%v objects %v", ref2, err, listAll(t, st))
	if err != nil || ref2.Seq != ref.Seq+1 {
		t.Fatal(ref2, err)
	}
}

// The consumer closes an epoch with a tombstone; the (zombie) writer's next
// append gets 412, sees the tombstone, halts that epoch, and commits in a new one.
func TestTombstoneFencesWriter(t *testing.T) {
	st, ctx := store(t, nil), context.Background()
	lg := inline.NewLog(st, "probe", nil)
	for i := range 2 {
		td := traces(5, 10+i)
		h, _ := inline.ContentHashTraces(td)
		if _, err := lg.Append(ctx, h, encoder(td)); err != nil {
			t.Fatal(err)
		}
	}
	old := lg.Epoch()
	r, err := st.Tombstone(ctx, old, 2)
	t.Logf("consumer tombstones %s/2: %v %v", old, r, err)
	td := traces(5, 20)
	h, _ := inline.ContentHashTraces(td)
	ref, err := lg.Append(ctx, h, encoder(td))
	t.Logf("writer's next append: %+v err=%v halted=%d; objects %v", ref, err, lg.Stats.Halted.Load(), listAll(t, st))
	if err != nil || lg.Stats.Halted.Load() != 1 || ref.Epoch == old || ref.Seq != 0 {
		t.Fatal("want halt and a commit at slot 0 of a new epoch")
	}
}

func ch(t *testing.T, q string) (string, error) {
	u := os.Getenv("INLINE_CH")
	if u == "" {
		t.Skip("INLINE_CH not set")
	}
	resp, err := http.Post(u+"/?"+url.Values{"query": {q}}.Encode(), "text/plain", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return strings.TrimSpace(string(b)), nil
}

// ClickHouse reads a log directly: a glob over the epoch's slots skips the
// zero-byte tombstone (s3_skip_empty_files, default 1 in 26.10), _path and
// _file name the slot, and the ParquetMetadata format returns the footer's
// key-value metadata without reading the data.
func TestClickHouseReadsLog(t *testing.T) {
	st, ctx := store(t, nil), context.Background()
	lg := inline.NewLog(st, "probe", nil)
	for i := range 3 {
		td := traces(100*(i+1), 30+i)
		h, _ := inline.ContentHashTraces(td)
		if _, err := lg.Append(ctx, h, encoder(td)); err != nil {
			t.Fatal(err)
		}
	}
	ep := lg.Epoch()
	if r, err := st.Tombstone(ctx, ep, 3); r != inline.TombWon {
		t.Fatal(r, err)
	}
	glob := fmt.Sprintf("%s/%s/%s/%s/*.parquet", endpoint(t), bucket, st.Prefix, ep)
	src := fmt.Sprintf("s3('%s', 'otel', 'otelsecret', 'Parquet')", glob)
	out, err := ch(t, "SELECT _file, count(), any(producer_epoch), any(batch_id) FROM "+src+" GROUP BY _file ORDER BY _file FORMAT TSV")
	t.Logf("per-slot rows via glob (tombstone included in the glob):\n%s\nerr=%v", out, err)
	if err != nil || strings.Count(out, "\n") != 2 {
		t.Fatal("want 3 data slots")
	}
	out, err = ch(t, "SELECT count() FROM "+src+" SETTINGS s3_skip_empty_files = 0 FORMAT TSV")
	t.Logf("same glob with s3_skip_empty_files = 0: %q err=%v", out, err)
	out, err = ch(t, fmt.Sprintf("DESCRIBE s3('%s', 'otel', 'otelsecret', 'ParquetMetadata') FORMAT TSV", glob))
	var names []string
	for _, l := range strings.Split(out, "\n") {
		if f := strings.SplitN(l, "\t", 2); len(f) == 2 && !strings.HasPrefix(f[0], " ") {
			names = append(names, f[0])
		}
	}
	out = strings.Join(names, " ")
	t.Logf("ParquetMetadata columns (no key_value_metadata among them):\n%s\nerr=%v", out, err)
	// Per-batch stats without reading data: rows and the Timestamp column's
	// min/max from the footer's row-group statistics.
	out, err = ch(t, fmt.Sprintf("SELECT _file, num_rows, "+
		"arrayFirst(c -> c.name = 'Timestamp', row_groups[1].columns).statistics.min AS min_ts, "+
		"arrayFirst(c -> c.name = 'Timestamp', row_groups[1].columns).statistics.max AS max_ts "+
		"FROM s3('%s', 'otel', 'otelsecret', 'ParquetMetadata') ORDER BY _file FORMAT TSV", glob))
	t.Logf("footer stats per slot (ParquetMetadata):\n%s\nerr=%v", out, err)
	// The footer key-value metadata is there for readers that expose it
	// (pyarrow, DuckDB, Spark); read it back here.
	obj, err := st.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(inline.SlotKey(st.Prefix, ep, 1))})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(obj.Body)
	kv, err := inline.FooterKV(b)
	t.Logf("footer key-value metadata of slot 1: %v err=%v", kv, err)
	if kv[inline.MetaSeq] != "1" || kv[inline.MetaRows] != "200" {
		t.Fatal(kv)
	}
	if p := os.Getenv("INLINE_DUMP"); p != "" {
		_ = os.WriteFile(p, b, 0o644)
	}
}

func firstLines(s string, n int) string {
	l := strings.Split(s, "\n")
	if len(l) > n {
		l = l[:n]
	}
	return strings.Join(l, "\n")
}
