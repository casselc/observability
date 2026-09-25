package otap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Sink stores objects under a prefix: S3 (path-style, one PutObject per
// object, as parquetgo does) or a local directory.
type Sink struct {
	URL    string
	s3     *s3.Client
	bucket string
	prefix string
	local  string
}

// NewSink takes http(s)://host/bucket/prefix or file:///dir.
func NewSink(rawURL, key, secret string, rt http.RoundTripper) (*Sink, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	s := &Sink{URL: strings.TrimRight(rawURL, "/")}
	if u.Scheme == "file" {
		s.local = u.Path
		return s, nil
	}
	parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
	s.bucket = parts[0]
	if len(parts) == 2 {
		s.prefix = parts[1]
	}
	hc := &http.Client{}
	if rt != nil {
		hc.Transport = rt
	}
	cfg := aws.Config{Region: "us-east-1", HTTPClient: hc}
	if key != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(key, secret, "")
	}
	s.s3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(u.Scheme + "://" + u.Host)
		o.UsePathStyle = true
	})
	return s, nil
}

// Put stores data at rel. ifNoneMatch sends If-None-Match: * (create-only),
// the conditional write the s3cas design commits with.
func (s *Sink) Put(ctx context.Context, rel string, data []byte, ifNoneMatch bool) error {
	if s.s3 == nil {
		p := filepath.Join(s.local, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(p+".tmp", data, 0o644); err != nil {
			return err
		}
		return os.Rename(p+".tmp", p)
	}
	key := rel
	if s.prefix != "" {
		key = s.prefix + "/" + rel
	}
	in := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key),
		Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data)))}
	if ifNoneMatch {
		in.IfNoneMatch = aws.String("*")
	}
	_, err := s.s3.PutObject(ctx, in)
	return err
}

// ObjectURL is rel as a URL ClickHouse's s3() can read.
func (s *Sink) ObjectURL(rel string) string { return s.URL + "/" + rel }

// Manifest is a batch's commit record. With several objects per batch (the
// star layout), the manifest is still one object written last, so "commit
// implies data" holds exactly as with one object: the batch is committed
// iff its manifest exists, and a reader never looks at objects the manifest
// does not name.
type Manifest struct {
	ProducerID    string    `json:"producer_id"`
	ProducerEpoch string    `json:"producer_epoch"`
	Signal        string    `json:"signal"`
	SchemaVersion uint16    `json:"schema_version"`
	Layout        string    `json:"layout"` // star | flat-parquet | flat-arrow | bar
	BatchID       uint64    `json:"batch_id"`
	ContentKey    string    `json:"content_key,omitempty"`
	Rows          int       `json:"rows"`
	ReceivedAt    time.Time `json:"received_at"`
	MinEventTime  time.Time `json:"min_event_time"`
	MaxEventTime  time.Time `json:"max_event_time"`
	Objects       []Object  `json:"objects"`
}

type Object struct {
	Table string `json:"table"`
	URL   string `json:"url"`
	Rows  int64  `json:"rows"`
	Bytes int    `json:"bytes"`
}

func (m Manifest) JSON() []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic(fmt.Sprint("manifest: ", err))
	}
	return b
}
