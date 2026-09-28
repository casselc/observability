package store

// Ranged reads and conditional writes, for the lake indexer (the service
// itself only reads). Kept apart from store.go's read-only Store.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// RangeGetter reads part of an object.
type RangeGetter interface {
	// GetRange returns n bytes at off (n < 0: to the end); a missing key
	// is an error.
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)
}

// Writer writes create-only and compare-and-swap.
type Writer interface {
	// PutCreate writes body with If-None-Match: *. created is false (and
	// err nil) on 412: the key exists. An error is an unknown outcome.
	PutCreate(ctx context.Context, key string, body []byte, meta map[string]string) (created bool, err error)
	// PutIfMatch writes body with If-Match: etag, or If-None-Match: * when
	// etag is "". ok is false (err nil) on 412 or 409: another write won.
	PutIfMatch(ctx context.Context, key string, body []byte, etag string) (ok bool, err error)
}

// GetRange implements RangeGetter.
func (s *S3) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	rng := fmt.Sprintf("bytes=%d-", off)
	if n >= 0 {
		if n == 0 {
			return []byte{}, nil
		}
		rng = fmt.Sprintf("bytes=%d-%d", off, off+n-1)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key, Range: &rng})
	if err != nil {
		return nil, fmt.Errorf("GET %s %s: %w", key, rng, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(io.LimitReader(out.Body, 256<<20))
	if err != nil {
		return nil, fmt.Errorf("GET %s %s: %w", key, rng, err)
	}
	if n >= 0 && int64(len(b)) != n {
		return nil, fmt.Errorf("GET %s %s: %d bytes", key, rng, len(b))
	}
	return b, nil
}

func conflict(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		c := re.HTTPStatusCode()
		return c == http.StatusPreconditionFailed || c == http.StatusConflict
	}
	return false
}

// PutCreate implements Writer.
func (s *S3) PutCreate(ctx context.Context, key string, body []byte, meta map[string]string) (bool, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*"), Metadata: meta})
	if err != nil {
		if conflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("PUT %s: %w", key, err)
	}
	return true, nil
}

// PutIfMatch implements Writer.
func (s *S3) PutIfMatch(ctx context.Context, key string, body []byte, etag string) (bool, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	in := &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		ContentType: aws.String("application/json")}
	if etag == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(etag)
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		if conflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("PUT %s: %w", key, err)
	}
	return true, nil
}
