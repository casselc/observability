// Package store is the service's view of the bucket: GET a control
// document, LIST lanes, HEAD a slot's metadata, presign a GET. The S3
// implementation uses the AWS SDK; Mem is for tests.
package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Object is one LIST entry.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// Store is what the service needs from the bucket.
type Store interface {
	// Get returns (nil, "", nil) when the key does not exist.
	Get(ctx context.Context, key string) ([]byte, string, error)
	// Dirs lists the common prefixes under prefix (which ends in "/"),
	// as names without the prefix or the trailing "/".
	Dirs(ctx context.Context, prefix string) ([]string, error)
	// List lists every object under prefix, in key order, stopping after
	// max entries (then truncated is true).
	List(ctx context.Context, prefix string, max int) (objs []Object, truncated bool, err error)
	// Head returns the object's user metadata (keys lower-case, without
	// the x-amz-meta- prefix).
	Head(ctx context.Context, key string) (map[string]string, error)
	// Presign returns a GET URL valid for ttl.
	Presign(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// S3Config configures S3.
type S3Config struct {
	Endpoint string `json:"endpoint"` // e.g. http://127.0.0.1:18333; empty for AWS
	// PublicEndpoint is the endpoint browsers reach the bucket at, if not
	// Endpoint: presigned URLs are signed for it (SigV4 signs the host).
	PublicEndpoint string `json:"public_endpoint"`
	Region         string `json:"region"`
	Bucket         string `json:"bucket"`
	PathStyle      bool   `json:"path_style"`
	// Static keys, for local stores; otherwise the SDK's default chain
	// (environment, web identity, instance role).
	AccessKey string `json:"-"`
	SecretKey string `json:"-"`
	TimeoutS  int    `json:"timeout_s"`
}

// S3 is a Store on one bucket.
type S3 struct {
	bucket  string
	client  *s3.Client
	presign *s3.PresignClient
	timeout time.Duration
}

// NewS3 builds clients for cfg.
func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("store: bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	opts := []func(*config.LoadOptions) error{config.WithRegion(cfg.Region)}
	if cfg.AccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	mk := func(endpoint string) *s3.Client {
		return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if endpoint != "" {
				o.BaseEndpoint = aws.String(endpoint)
			}
			o.UsePathStyle = cfg.PathStyle || endpoint != ""
			// SeaweedFS and other S3-compatible stores: no CRC headers on
			// requests that do not need them.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
	}
	pub := cfg.PublicEndpoint
	if pub == "" {
		pub = cfg.Endpoint
	}
	t := time.Duration(cfg.TimeoutS) * time.Second
	if t <= 0 {
		t = 10 * time.Second
	}
	return &S3{bucket: cfg.Bucket, client: mk(cfg.Endpoint), presign: s3.NewPresignClient(mk(pub)), timeout: t}, nil
}

func (s *S3) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
}

// Get implements Store.
func (s *S3) Get(ctx context.Context, key string) ([]byte, string, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if notFound(err) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("GET %s: %w", key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(io.LimitReader(out.Body, 64<<20))
	if err != nil {
		return nil, "", fmt.Errorf("GET %s: %w", key, err)
	}
	return b, aws.ToString(out.ETag), nil
}

func notFound(err error) bool {
	var nsk *s3types.NoSuchKey
	var nf *s3types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	return errors.As(err, &ae) && (ae.ErrorCode() == "NoSuchKey" || ae.ErrorCode() == "NotFound")
}

// Dirs implements Store.
func (s *S3) Dirs(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	var token *string
	for {
		c, cancel := s.ctx(ctx)
		page, err := s.client.ListObjectsV2(c, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, Delimiter: aws.String("/"), ContinuationToken: token})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("LIST %s: %w", prefix, err)
		}
		for _, p := range page.CommonPrefixes {
			out = append(out, strings.TrimSuffix(strings.TrimPrefix(aws.ToString(p.Prefix), prefix), "/"))
		}
		if !aws.ToBool(page.IsTruncated) {
			return out, nil
		}
		token = page.NextContinuationToken
	}
}

// List implements Store.
func (s *S3) List(ctx context.Context, prefix string, max int) ([]Object, bool, error) {
	var out []Object
	var token *string
	for {
		c, cancel := s.ctx(ctx)
		page, err := s.client.ListObjectsV2(c, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, ContinuationToken: token})
		cancel()
		if err != nil {
			return nil, false, fmt.Errorf("LIST %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			if len(out) >= max {
				return out, true, nil
			}
			out = append(out, Object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), LastModified: aws.ToTime(o.LastModified), ETag: aws.ToString(o.ETag)})
		}
		if !aws.ToBool(page.IsTruncated) {
			return out, false, nil
		}
		token = page.NextContinuationToken
	}
}

// Head implements Store.
func (s *S3) Head(ctx context.Context, key string) (map[string]string, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("HEAD %s: %w", key, err)
	}
	m := map[string]string{}
	for k, v := range out.Metadata {
		m[strings.ToLower(k)] = v
	}
	return m, nil
}

// Presign implements Store.
func (s *S3) Presign(ctx context.Context, key string, ttl time.Duration) (string, error) {
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	if req.Method != http.MethodGet {
		return "", fmt.Errorf("presign: method %s", req.Method)
	}
	return req.URL, nil
}

// Mem is an in-memory Store for tests.
type Mem struct {
	mu      sync.Mutex
	Objects map[string]MemObject
	// Err, if set, fails every call; GetErr only Get.
	Err, GetErr error
	Heads       int
}

// MemObject is a stored object.
type MemObject struct {
	Body         []byte
	Size         int64
	Meta         map[string]string
	LastModified time.Time
}

// NewMem returns an empty Mem.
func NewMem() *Mem { return &Mem{Objects: map[string]MemObject{}} }

// Put stores an object.
func (m *Mem) Put(key string, body []byte, meta map[string]string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Objects[key] = MemObject{Body: body, Size: int64(len(body)), Meta: meta, LastModified: at}
}

// Get implements Store.
func (m *Mem) Get(_ context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, "", m.Err
	}
	if m.GetErr != nil {
		return nil, "", m.GetErr
	}
	o, ok := m.Objects[key]
	if !ok {
		return nil, "", nil
	}
	return bytes.Clone(o.Body), "etag", nil
}

// Dirs implements Store.
func (m *Mem) Dirs(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	seen := map[string]bool{}
	for k := range m.Objects {
		if rest, ok := strings.CutPrefix(k, prefix); ok {
			if i := strings.IndexByte(rest, '/'); i > 0 {
				seen[rest[:i]] = true
			}
		}
	}
	var out []string
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

// List implements Store.
func (m *Mem) List(_ context.Context, prefix string, max int) ([]Object, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, false, m.Err
	}
	var out []Object
	for k, o := range m.Objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, Object{Key: k, Size: o.Size, LastModified: o.LastModified})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > max {
		return out[:max], true, nil
	}
	return out, false, nil
}

// Head implements Store.
func (m *Mem) Head(_ context.Context, key string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Heads++
	if m.Err != nil {
		return nil, m.Err
	}
	o, ok := m.Objects[key]
	if !ok {
		return nil, fmt.Errorf("HEAD %s: not found", key)
	}
	return o.Meta, nil
}

// Presign implements Store: a fake URL naming the key and expiry.
func (m *Mem) Presign(_ context.Context, key string, ttl time.Duration) (string, error) {
	return fmt.Sprintf("https://mem.invalid/%s?X-Amz-Expires=%d", key, int(ttl.Seconds())), nil
}
