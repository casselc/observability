// Package store keeps each rule's state as one object, replaced only by
// compare-and-swap: If-Match on the ETag the writer read, If-None-Match: *
// to create. It is the same primitive the consumer's leases and checkpoints
// use (DECISIONS D8), and it is what makes two replicas safe: a replica
// whose read is out of date loses the swap and re-reads; it never
// overwrites a newer state.
//
// A write with no answer is ambiguous: it may have landed or not (and a
// copy may still land later). Commit resolves it by reading the object
// back: the state carries a random write id, so "the object is my write" is
// a definite yes. Anything else is treated as "not mine": the caller
// re-reads and starts over from what is there. A late copy of the write can
// only land on the version it was conditioned on, i.e. only if nothing was
// written since, and it is then a valid successor of that version.
package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Errors a Store returns.
var (
	ErrNotFound     = errors.New("not found")
	ErrPrecondition = errors.New("precondition failed") // 412 / 409: someone else wrote first
	ErrAmbiguous    = errors.New("no definite answer")  // a timeout, a 5xx, a reset: the write may or may not have landed
)

// Store is a bucket of small documents with conditional writes.
type Store interface {
	Get(ctx context.Context, key string) (body []byte, etag string, err error)
	// PutIfMatch replaces key if its ETag is etag; PutIfAbsent creates key.
	PutIfMatch(ctx context.Context, key, etag string, body []byte) (string, error)
	PutIfAbsent(ctx context.Context, key string, body []byte) (string, error)
}

// ---- S3 ------------------------------------------------------------------

// S3Config is the bucket the state lives in.
type S3Config struct {
	Endpoint  string `yaml:"endpoint" json:"endpoint"` // empty: AWS
	Region    string `yaml:"region" json:"region"`
	Bucket    string `yaml:"bucket" json:"bucket"`
	Prefix    string `yaml:"prefix" json:"prefix"` // e.g. "alerts/state"
	AccessKey string `yaml:"access_key" json:"access_key"`
	SecretKey string `yaml:"secret_key" json:"secret_key"`
}

// S3 is a Store on an S3 bucket. SDK retries are off: a retry would hide
// exactly the ambiguous outcomes Commit resolves itself.
type S3 struct {
	c      *s3.Client
	bucket string
}

// NewS3 builds the client from the AWS default chain (IRSA, Pod Identity,
// the environment) unless static keys are given.
func NewS3(ctx context.Context, c S3Config) (*S3, error) {
	if c.Bucket == "" {
		return nil, errors.New("state: no bucket")
	}
	opts := []func(*config.LoadOptions) error{config.WithRetryMaxAttempts(1)}
	if c.Region != "" {
		opts = append(opts, config.WithRegion(c.Region))
	} else if c.Endpoint != "" {
		opts = append(opts, config.WithRegion("us-east-1"))
	}
	if c.AccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	cl := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
			o.UsePathStyle = true
		}
	})
	return &S3{c: cl, bucket: c.Bucket}, nil
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	status := 0
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		status = re.HTTPStatusCode()
	}
	code := ""
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code = ae.ErrorCode()
	}
	switch {
	case status == http.StatusPreconditionFailed || code == "PreconditionFailed" || status == http.StatusConflict:
		return fmt.Errorf("%w: %v", ErrPrecondition, err)
	case status == http.StatusNotFound || code == "NoSuchKey" || code == "NotFound":
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case status >= 400 && status < 500:
		return err // rejected before evaluation (403, 400): definite, not applied
	}
	return fmt.Errorf("%w: %v", ErrAmbiguous, err)
}

// Get reads key.
func (s *S3) Get(ctx context.Context, key string) ([]byte, string, error) {
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, "", classify(err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrAmbiguous, err)
	}
	return b, aws.ToString(out.ETag), nil
}

// PutIfMatch replaces key only if its ETag is etag.
func (s *S3) PutIfMatch(ctx context.Context, key, etag string, body []byte) (string, error) {
	out, err := s.c.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(body),
		IfMatch: aws.String(etag), ContentType: aws.String("application/json")})
	if err != nil {
		return "", classify(err)
	}
	return aws.ToString(out.ETag), nil
}

// PutIfAbsent creates key only if it does not exist.
func (s *S3) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	out, err := s.c.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(body),
		IfNoneMatch: aws.String("*"), ContentType: aws.String("application/json")})
	if err != nil {
		return "", classify(err)
	}
	return aws.ToString(out.ETag), nil
}

// ---- in memory, with faults (tests and the simulation) ----------------------

// Fault is what the memory store does with one write.
type Fault int

// Faults.
const (
	NoFault     Fault = iota
	LoseRequest       // not applied, no answer (ErrAmbiguous)
	LoseAnswer        // applied, no answer (ErrAmbiguous)
	DelayApply        // not applied now, no answer; applied at the next Flush if still valid (a late copy)
	FailGet           // reads fail (ErrAmbiguous)
)

type obj struct {
	body []byte
	ver  int
}

type late struct {
	key, etag string
	create    bool
	body      []byte
}

// Mem is an in-memory Store with S3's conditional semantics and injectable
// ambiguity.
type Mem struct {
	mu     sync.Mutex
	objs   map[string]obj
	ver    int
	late   []late
	Faults func(op, key string) Fault // nil: no faults
	Writes int                        // successful writes (tests)
	// OnApply sees every write that lands, late copies included (tests).
	OnApply func(key string, body []byte)
}

// NewMem is an empty store.
func NewMem() *Mem { return &Mem{objs: map[string]obj{}} }

func (m *Mem) fault(op, key string) Fault {
	if m.Faults == nil {
		return NoFault
	}
	return m.Faults(op, key)
}

// Get reads key.
func (m *Mem) Get(_ context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fault("get", key) == FailGet {
		return nil, "", fmt.Errorf("%w: injected", ErrAmbiguous)
	}
	o, ok := m.objs[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return append([]byte(nil), o.body...), fmt.Sprintf(`"v%d"`, o.ver), nil
}

func (m *Mem) apply(key, etag string, create bool, body []byte) (string, error) {
	o, ok := m.objs[key]
	if create && ok {
		return "", ErrPrecondition
	}
	if !create && (!ok || fmt.Sprintf(`"v%d"`, o.ver) != etag) {
		if !ok {
			return "", ErrNotFound
		}
		return "", ErrPrecondition
	}
	m.ver++
	m.objs[key] = obj{body: append([]byte(nil), body...), ver: m.ver}
	m.Writes++
	if m.OnApply != nil {
		m.OnApply(key, body)
	}
	return fmt.Sprintf(`"v%d"`, m.ver), nil
}

func (m *Mem) put(key, etag string, create bool, body []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.fault("put", key) {
	case LoseRequest:
		return "", fmt.Errorf("%w: injected lost request", ErrAmbiguous)
	case LoseAnswer:
		_, _ = m.apply(key, etag, create, body)
		return "", fmt.Errorf("%w: injected lost answer", ErrAmbiguous)
	case DelayApply:
		m.late = append(m.late, late{key, etag, create, append([]byte(nil), body...)})
		return "", fmt.Errorf("%w: injected delayed copy", ErrAmbiguous)
	}
	return m.apply(key, etag, create, body)
}

// PutIfMatch replaces key if its ETag is etag.
func (m *Mem) PutIfMatch(_ context.Context, key, etag string, body []byte) (string, error) {
	return m.put(key, etag, false, body)
}

// PutIfAbsent creates key.
func (m *Mem) PutIfAbsent(_ context.Context, key string, body []byte) (string, error) {
	return m.put(key, "", true, body)
}

// Flush lands every delayed copy whose condition still holds (the others
// fail their precondition, as on S3).
func (m *Mem) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range m.late {
		_, _ = m.apply(l.key, l.etag, l.create, l.body)
	}
	m.late = nil
}

// ---- compare-and-swap of a JSON document ----------------------------------

// Doc is a document that carries its own write id.
type Doc interface{ SetWrite(writer, id string) }

// Outcome of a Commit.
type Outcome string

// Commit outcomes.
const (
	Committed    Outcome = "ok"
	Resolved     Outcome = "ambiguous_landed" // no answer; the read-back shows our write
	Conflict     Outcome = "conflict"         // someone else wrote first: re-read
	Lost         Outcome = "ambiguous_lost"   // no answer; the read-back does not show our write: re-read
	CommitFailed Outcome = "error"            // a definite refusal (403…) or the read-back failed: re-read later
)

// OK says whether the write is known to have landed.
func (o Outcome) OK() bool { return o == Committed || o == Resolved }

// NewID is a random write id.
func NewID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Commit writes doc over the version etag ("" creates it) and resolves an
// unanswered write by reading it back. It returns the new ETag when the
// write is known to have landed.
func Commit(ctx context.Context, s Store, key, etag, writer string, doc Doc, idOf func([]byte) string) (string, Outcome, error) {
	id := NewID()
	doc.SetWrite(writer, id)
	body, err := json.Marshal(doc)
	if err != nil {
		return "", CommitFailed, err
	}
	var e string
	if etag == "" {
		e, err = s.PutIfAbsent(ctx, key, body)
	} else {
		e, err = s.PutIfMatch(ctx, key, etag, body)
	}
	switch {
	case err == nil:
		return e, Committed, nil
	case errors.Is(err, ErrPrecondition), errors.Is(err, ErrNotFound):
		return "", Conflict, err
	case !errors.Is(err, ErrAmbiguous):
		return "", CommitFailed, err
	}
	b, e2, gerr := s.Get(ctx, key)
	if gerr != nil {
		return "", CommitFailed, fmt.Errorf("%v; read-back: %w", err, gerr)
	}
	if idOf(b) == id {
		return e2, Resolved, nil
	}
	return "", Lost, err
}
