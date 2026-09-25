// Package retrytest drives the stock contrib awss3exporter (v0.161.0) and
// the patched one (../awss3inline, key_mode: sequence) through their
// factories, exporterhelper timeout and retry included, with the Parquet
// encoding extension, against SeaweedFS behind a fault-injecting proxy.
// Skipped unless INLINE_S3 is set:
//
//	INLINE_S3=http://127.0.0.1:18333 go test -v -count=1 ./retrytest
package retrytest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	stock "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	patched "github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/awss3inline"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/parquetencoding"
)

var runID = time.Now().UTC().Format("20060102T150405")

// proxy forwards to SeaweedFS (keeping the Host header, so SigV4 still
// verifies) and can hold back the n-th data PUT: it is forwarded after
// `land` and answered after `answer` (past the exporter's timeout, so the
// client has given up by then).
type proxy struct {
	target      *url.URL
	rp          *httputil.ReverseProxy
	puts        atomic.Int64
	holdPut     int64
	land        time.Duration
	answer      time.Duration
	mu          sync.Mutex
	log         []string
	late        sync.WaitGroup
	matchPrefix string
}

func newProxy(t *testing.T, match string) (*proxy, *httptest.Server) {
	target, _ := url.Parse(os.Getenv("INLINE_S3"))
	p := &proxy{target: target, rp: httputil.NewSingleHostReverseProxy(target), matchPrefix: match}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return p, srv
}

func (p *proxy) note(s string) {
	p.mu.Lock()
	p.log = append(p.log, fmt.Sprintf("%s %s", time.Now().Format("15:04:05.000"), s))
	p.mu.Unlock()
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut || !strings.Contains(r.URL.Path, p.matchPrefix) {
		p.rp.ServeHTTP(w, r)
		return
	}
	n := p.puts.Add(1)
	cond := r.Header.Get("If-None-Match")
	p.note(fmt.Sprintf("PUT #%d %s If-None-Match=%q", n, r.URL.Path, cond))
	if n != p.holdPut {
		rec := httptest.NewRecorder()
		p.rp.ServeHTTP(rec, r)
		p.note(fmt.Sprintf("PUT #%d -> %d", n, rec.Code))
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
		return
	}
	body, _ := io.ReadAll(r.Body)
	late := r.Clone(context.Background())
	late.Body = io.NopCloser(bytes.NewReader(body))
	late.ContentLength = int64(len(body))
	p.late.Add(1)
	go func() {
		defer p.late.Done()
		time.Sleep(p.land)
		rec := httptest.NewRecorder()
		p.rp.ServeHTTP(rec, late)
		p.note(fmt.Sprintf("PUT #%d (held back) applied late -> %d", n, rec.Code))
	}()
	select {
	case <-time.After(p.answer):
	case <-r.Context().Done():
	}
	p.note(fmt.Sprintf("PUT #%d: client gave up (%v)", n, r.Context().Err()))
	hj, ok := w.(http.Hijacker)
	if ok {
		if c, _, err := hj.Hijack(); err == nil {
			c.Close()
		}
	}
}

type host struct {
	ext map[component.ID]component.Component
}

func (h host) GetExtensions() map[component.ID]component.Component { return h.ext }

var encID = component.MustNewID("parquet_encoding")

func traces(n, seed int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "retrytest")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := range n {
		sp := ss.Spans().AppendEmpty()
		sp.SetName(fmt.Sprintf("op-%d-%d", seed, i))
		sp.SetTraceID(pcommon.TraceID([16]byte{byte(seed), byte(i), 7}))
		sp.SetSpanID(pcommon.SpanID([8]byte{byte(seed), byte(i), 8}))
		sp.SetStartTimestamp(pcommon.Timestamp(1_758_800_000_000_000_000 + int64(i)))
		sp.SetEndTimestamp(sp.StartTimestamp() + 5)
	}
	return td
}

type exp interface {
	component.Component
	ConsumeTraces(context.Context, ptrace.Traces) error
}

// newExporter builds the stock or the patched exporter with the same
// settings: parquet_encoding, timeout 1 s, exporterhelper retry from 100 ms,
// no sending queue (so ConsumeTraces returns once retries are done).
func newExporter(t *testing.T, patchedMode bool, endpoint, prefix string) exp {
	t.Setenv("AWS_ACCESS_KEY_ID", "otel")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "otelsecret")
	var f exporter.Factory
	var cfg component.Config
	retry := configretry.NewDefaultBackOffConfig()
	retry.InitialInterval, retry.MaxElapsedTime = 100*time.Millisecond, 30*time.Second
	if patchedMode {
		f = patched.NewFactory()
		c := f.CreateDefaultConfig().(*patched.Config)
		c.S3Uploader.Region, c.S3Uploader.S3Bucket, c.S3Uploader.S3Prefix = "us-east-1", "otel", prefix
		c.S3Uploader.Endpoint, c.S3Uploader.S3ForcePathStyle, c.S3Uploader.DisableSSL = endpoint, true, true
		c.S3Uploader.KeyMode, c.S3Uploader.FilePrefix = "sequence", "edge-1"
		c.Encoding, c.EncodingFileExtension = &encID, "parquet"
		c.TimeoutSettings.Timeout, c.BackOffConfig = time.Second, retry
		cfg = c
	} else {
		f = stock.NewFactory()
		c := f.CreateDefaultConfig().(*stock.Config)
		c.S3Uploader.Region, c.S3Uploader.S3Bucket, c.S3Uploader.S3Prefix = "us-east-1", "otel", prefix
		c.S3Uploader.Endpoint, c.S3Uploader.S3ForcePathStyle, c.S3Uploader.DisableSSL = endpoint, true, true
		c.S3Uploader.S3PartitionFormat = "year=%Y/month=%m/day=%d/hour=%H/minute=%M"
		c.Encoding, c.EncodingFileExtension = &encID, "parquet"
		c.TimeoutSettings.Timeout, c.BackOffConfig = time.Second, retry
		cfg = c
	}
	e, err := f.CreateTraces(context.Background(), exportertest.NewNopSettings(f.Type()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	ext := parquetencoding.New(parquetencoding.Config{ProducerID: "edge-1", SchemaVersion: 1, Compression: "zstd"})
	if err := e.Start(context.Background(), host{map[component.ID]component.Component{encID: ext}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Shutdown(context.Background()) })
	return e
}

func s3client() *s3.Client {
	return s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(os.Getenv("INLINE_S3")), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("otel", "otelsecret", "")})
}

type obj struct {
	key  string
	size int64
	meta map[string]string
}

func objects(t *testing.T, prefix string) []obj {
	c := s3client()
	out, err := c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("otel"), Prefix: aws.String(prefix + "/")})
	if err != nil {
		t.Fatal(err)
	}
	var res []obj
	for _, o := range out.Contents {
		h, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("otel"), Key: o.Key})
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, obj{strings.TrimPrefix(aws.ToString(o.Key), prefix+"/"), aws.ToInt64(o.Size), h.Metadata})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].key < res[j].key })
	return res
}

func report(t *testing.T, p *proxy, prefix string, err error, elapsed time.Duration) []obj {
	p.late.Wait()
	objs := objects(t, prefix)
	t.Logf("ConsumeTraces returned %v after %v", err, elapsed.Round(time.Millisecond))
	for _, l := range p.log {
		t.Logf("  proxy: %s", l)
	}
	for _, o := range objs {
		t.Logf("  object %s (%d bytes) meta=%v", o.key, o.size, o.meta)
	}
	return objs
}

// Scenario: the first PUT is slow. The exporter's 1 s timeout fires,
// exporterhelper retries, and the first PUT is applied later anyway.
func TestSlowPutThenRetry(t *testing.T) {
	if os.Getenv("INLINE_S3") == "" {
		t.Skip("INLINE_S3 not set")
	}
	for _, tc := range []struct {
		name    string
		patched bool
		land    time.Duration
	}{
		{"stock/lands-after-retry", false, 1500 * time.Millisecond},
		{"stock/lands-before-retry", false, 500 * time.Millisecond},
		{"patched/lands-after-retry", true, 1500 * time.Millisecond},
		{"patched/lands-before-retry", true, 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := "s3inline/retry/" + runID + "/" + strings.ReplaceAll(tc.name, "/", "-")
			p, srv := newProxy(t, prefix)
			p.holdPut, p.land, p.answer = 1, tc.land, 3*time.Second
			e := newExporter(t, tc.patched, srv.URL, prefix)
			start := time.Now()
			err := e.ConsumeTraces(context.Background(), traces(200, 1))
			objs := report(t, p, prefix, err, time.Since(start))
			want := 1
			if !tc.patched {
				want = 2
			}
			if len(objs) != want {
				t.Errorf("objects: got %d, want %d", len(objs), want)
			}
		})
	}
}

// Scenario: the process dies after the commit but before the queue's ack;
// the persistent queue hands the same request to the next incarnation.
func TestRedeliveryAfterRestart(t *testing.T) {
	if os.Getenv("INLINE_S3") == "" {
		t.Skip("INLINE_S3 not set")
	}
	prefix := "s3inline/retry/" + runID + "/restart"
	_, srv := newProxy(t, prefix)
	td := traces(100, 2)
	for i := range 2 {
		e := newExporter(t, true, srv.URL, prefix)
		if err := e.ConsumeTraces(context.Background(), td); err != nil {
			t.Fatal(err)
		}
		t.Logf("incarnation %d committed the request", i+1)
	}
	objs := objects(t, prefix)
	for _, o := range objs {
		t.Logf("  object %s content=%s", o.key, o.meta["oscope-content"])
	}
	if len(objs) != 2 || objs[0].meta["oscope-content"] != objs[1].meta["oscope-content"] {
		t.Fatal("want one copy per epoch with the same content hash (the consumer's check removes the second)")
	}
}
