package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Small wrappers over the S3 calls the checks use, each under the per-request
// timeout. inm/im are If-None-Match / If-Match ("" = not sent).

type putReq struct {
	Key, Body, CT, INM, IM string
	Bytes                  []byte
	Meta                   map[string]string
}

func (e *Env) put(ctx context.Context, p putReq) (string, error) {
	return e.putWith(ctx, e.S3, p)
}

func (e *Env) putWith(ctx context.Context, c *s3.Client, p putReq) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
	defer cancel()
	b := p.Bytes
	if b == nil {
		b = []byte(p.Body)
	}
	in := &s3.PutObjectInput{Bucket: &e.O.Bucket, Key: aws.String(p.Key), Body: bytes.NewReader(b)}
	if p.INM != "" {
		in.IfNoneMatch = aws.String(p.INM)
	}
	if p.IM != "" {
		in.IfMatch = aws.String(p.IM)
	}
	if p.CT != "" {
		in.ContentType = aws.String(p.CT)
	}
	if p.Meta != nil {
		in.Metadata = p.Meta
	}
	out, err := c.PutObject(ctx, in)
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

func (e *Env) get(ctx context.Context, key string) ([]byte, string, error) {
	return e.getWith(ctx, e.S3, key)
}

func (e *Env) getWith(ctx context.Context, c *s3.Client, key string) ([]byte, string, error) {
	ctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
	defer cancel()
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: &e.O.Bucket, Key: aws.String(key)})
	if err != nil {
		return nil, "", err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	return b, aws.ToString(out.ETag), err
}

func (e *Env) head(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	ctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
	defer cancel()
	return e.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &e.O.Bucket, Key: aws.String(key)})
}

func (e *Env) del(ctx context.Context, key, ifMatch string) error {
	ctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
	defer cancel()
	in := &s3.DeleteObjectInput{Bucket: &e.O.Bucket, Key: aws.String(key)}
	if ifMatch != "" {
		in.IfMatch = aws.String(ifMatch)
	}
	_, err := e.S3.DeleteObject(ctx, in)
	return err
}

func (e *Env) list(ctx context.Context, prefix, startAfter, delim string, max int32, token *string) (*s3.ListObjectsV2Output, error) {
	ctx, cancel := context.WithTimeout(ctx, e.O.Timeout)
	defer cancel()
	in := &s3.ListObjectsV2Input{Bucket: &e.O.Bucket, Prefix: aws.String(prefix), ContinuationToken: token}
	if startAfter != "" {
		in.StartAfter = aws.String(startAfter)
	}
	if delim != "" {
		in.Delimiter = aws.String(delim)
	}
	if max > 0 {
		in.MaxKeys = aws.Int32(max)
	}
	return e.S3.ListObjectsV2(ctx, in)
}

// listKeys lists every key under prefix after startAfter, following pages.
func (e *Env) listKeys(ctx context.Context, prefix, startAfter string, page int32) ([]string, int, error) {
	var keys []string
	var tok *string
	pages := 0
	for {
		out, err := e.list(ctx, prefix, startAfter, "", page, tok)
		if err != nil {
			return keys, pages, err
		}
		pages++
		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, pages, nil
		}
		tok = out.NextContinuationToken
	}
}

func md5ETag(b []byte) string {
	h := md5.Sum(b)
	return `"` + hex.EncodeToString(h[:]) + `"`
}

func sameETag(a, b string) bool { return strings.Trim(a, `"`) == strings.Trim(b, `"`) }
