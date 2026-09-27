package lane

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// NewS3 builds a client for endpoint ("" = AWS) with the default credential
// chain (env keys for SeaweedFS), path-style for a custom endpoint.
func NewS3(ctx context.Context, endpoint, region string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired))
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

// Writer appends records to one lane. Add buffers; a flush every interval
// writes one delta object. Sync writes pending deltas, then a sync object.
type Writer struct {
	S3     *s3.Client
	Bucket string
	Lane   string // {prefix}/{cluster}/{epochMs}-{instance}
	// PutTimeout bounds each PUT / HEAD attempt (default 20 s). Without it a
	// hung S3 stalled the lane until the connection broke (k8s-sim.md §6); a
	// PUT that timed out but landed is recognised by its ETag on the retry.
	PutTimeout time.Duration

	mu  sync.Mutex
	buf []Record

	putMu sync.Mutex
	seq   uint64

	Objects, Bytes, Records, Retries atomic.Int64
	LastPut                          atomic.Int64 // ms
}

func (w *Writer) Add(recs ...Record) {
	w.mu.Lock()
	w.buf = append(w.buf, recs...)
	w.mu.Unlock()
}

// Buffered returns a copy of the records not flushed yet.
func (w *Writer) Buffered() []Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]Record(nil), w.buf...)
}

func (w *Writer) Pending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buf)
}

// Flush writes the buffered records as one delta object (none if empty).
func (w *Writer) Flush(ctx context.Context) error {
	w.putMu.Lock()
	defer w.putMu.Unlock()
	return w.flushLocked(ctx)
}

func (w *Writer) flushLocked(ctx context.Context) error {
	w.mu.Lock()
	recs := w.buf
	w.buf = nil
	w.mu.Unlock()
	if len(recs) == 0 {
		return nil
	}
	return w.put(ctx, "delta", recs)
}

// Sync flushes pending deltas, then writes the full open state as of syncAt.
func (w *Writer) Sync(ctx context.Context, syncAt Time, recs []Record) error {
	w.putMu.Lock()
	defer w.putMu.Unlock()
	if err := w.flushLocked(ctx); err != nil {
		return err
	}
	return w.put(ctx, fmt.Sprintf("sync.%d", syncAt), recs)
}

// Run flushes every interval until ctx ends, then flushes once more.
func (w *Writer) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_ = w.Flush(c)
			cancel()
			return
		case <-t.C:
			if err := w.Flush(ctx); err != nil && ctx.Err() == nil {
				log.Printf("lane flush: %v", err)
			}
		}
	}
}

// put writes one object create-only, retrying until it is committed. A 412
// on a retry is our own earlier PUT if the stored ETag is our MD5.
func (w *Writer) put(ctx context.Context, kind string, recs []Record) error {
	var raw bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&raw, gzip.BestSpeed)
	enc := json.NewEncoder(zw)
	for i := range recs {
		if err := enc.Encode(&recs[i]); err != nil {
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return err
	}
	body := raw.Bytes()
	sum := md5.Sum(body)
	etag := hex.EncodeToString(sum[:])
	backoff := 200 * time.Millisecond
	for attempt := 0; ; attempt++ {
		key := fmt.Sprintf("%s/%012d.%s.ndjson.gz", w.Lane, w.seq, kind)
		to := w.PutTimeout
		if to <= 0 {
			to = 20 * time.Second
		}
		actx, cancel := context.WithTimeout(ctx, to)
		_, err := w.S3.PutObject(actx, &s3.PutObjectInput{
			Bucket: aws.String(w.Bucket), Key: aws.String(key), Body: bytes.NewReader(body),
			IfNoneMatch: aws.String("*"), ContentType: aws.String("application/x-ndjson"),
			ContentLength: aws.Int64(int64(len(body))),
		})
		cancel()
		if err == nil {
			w.seq++
			w.Objects.Add(1)
			w.Bytes.Add(int64(len(body)))
			w.Records.Add(int64(len(recs)))
			w.LastPut.Store(time.Now().UnixMilli())
			return nil
		}
		var re *smithyhttp.ResponseError
		if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusPreconditionFailed {
			hctx, hcancel := context.WithTimeout(ctx, to)
			h, herr := w.S3.HeadObject(hctx, &s3.HeadObjectInput{Bucket: aws.String(w.Bucket), Key: aws.String(key)})
			hcancel()
			if herr == nil && strings.Trim(aws.ToString(h.ETag), `"`) == etag {
				w.seq++ // our own earlier attempt landed
				w.Objects.Add(1)
				w.Bytes.Add(int64(len(body)))
				w.Records.Add(int64(len(recs)))
				return nil
			}
			log.Printf("lane: slot %s taken by another writer; skipping it", key)
			w.seq++
			continue
		}
		if ctx.Err() != nil {
			// put the records back so a later flush (or shutdown flush) keeps them
			w.mu.Lock()
			w.buf = append(recs, w.buf...)
			w.mu.Unlock()
			return ctx.Err()
		}
		w.Retries.Add(1)
		if attempt%10 == 0 {
			log.Printf("lane put %s (attempt %d): %v", key, attempt, err)
		}
		time.Sleep(backoff)
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}
