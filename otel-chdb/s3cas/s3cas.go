// Package s3cas probes the conditional-write primitives of an S3 endpoint and
// prototypes the control plane of the S3-native publishing variant
// (../model/s3Native.qnt): create-only log slots, epoch fencing through a
// fence entry, and compare-and-swap'd consumer checkpoints.
//
// Everything here talks to S3 with aws-sdk-go-v2 directly: chDB's s3() table
// function and s3_plain_rewritable disks send no conditional headers (see
// ../model/S3NATIVE.md), so the control plane cannot go through chDB.
package s3cas

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Outcome classifies the answer to a conditional request.
type Outcome int

const (
	OK                 Outcome = iota
	PreconditionFailed         // 412: the condition did not hold
	Conflict                   // 409 ConditionalRequestConflict: a concurrent op interfered; retry
	NotFound                   // 404: If-Match on a missing key, or a missing upload
	Unauthorized               // 403 / expired credentials: rejected before evaluation, not applied
	Other                      // anything else, including timeouts: the outcome is unknown
)

func (o Outcome) String() string {
	return [...]string{"OK", "412 PreconditionFailed", "409 Conflict", "404 NotFound", "403 Unauthorized", "other"}[o]
}

// Classify maps an SDK error to an Outcome, and reports the HTTP status and
// error code it saw.
func Classify(err error) (Outcome, int, string) {
	if err == nil {
		return OK, 200, ""
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
	case status == http.StatusPreconditionFailed || code == "PreconditionFailed":
		return PreconditionFailed, status, code
	case status == http.StatusConflict:
		return Conflict, status, code
	case status == http.StatusNotFound || code == "NoSuchKey" || code == "NotFound" || code == "NoSuchUpload":
		return NotFound, status, code
	case status == http.StatusForbidden || code == "ExpiredToken" || code == "InvalidAccessKeyId" ||
		code == "SignatureDoesNotMatch" || code == "RequestTimeTooSkewed" || code == "AccessDenied":
		return Unauthorized, status, code
	}
	return Other, status, code
}

// Config is an S3 bucket and, for S3-compatible stores, an endpoint.
// Credentials come from the AWS default chain unless Key is set: IRSA (web
// identity), EKS Pod Identity (container credentials), IAM Roles Anywhere
// (credential_process with aws_signing_helper in the shared config), the
// environment, or the shared credentials file. Refreshing credentials are
// renewed by the SDK; nothing here caches them.
type Config struct {
	Endpoint string // empty: AWS (or AWS_ENDPOINT_URL_S3); set: path-style S3-compatible store
	Bucket   string
	Region   string // empty: from the default chain
	Key      string // optional static credentials (e.g. Nutanix Objects keys)
	Secret   string
}

// FromEnv reads S3CAS_BUCKET (required), S3CAS_ENDPOINT, S3CAS_REGION and,
// optionally, static S3CAS_KEY / S3CAS_SECRET.
func FromEnv() (Config, bool) {
	c := Config{
		Endpoint: os.Getenv("S3CAS_ENDPOINT"),
		Bucket:   os.Getenv("S3CAS_BUCKET"),
		Region:   os.Getenv("S3CAS_REGION"),
		Key:      os.Getenv("S3CAS_KEY"),
		Secret:   os.Getenv("S3CAS_SECRET"),
	}
	return c, c.Bucket != ""
}

// Client wraps an s3.Client with the operations the protocol needs.
type Client struct {
	S3     *s3.Client
	Bucket string
}

// New builds a client with config.LoadDefaultConfig, so every credential
// source of the deployment targets works, and SDK retries off: a retry
// would hide exactly the outcomes the protocol has to resolve itself.
func New(ctx context.Context, c Config) (*Client, error) {
	opts := []func(*config.LoadOptions) error{config.WithRetryMaxAttempts(1)}
	if c.Region != "" {
		opts = append(opts, config.WithRegion(c.Region))
	} else if c.Endpoint != "" {
		opts = append(opts, config.WithRegion("us-east-1"))
	}
	if c.Key != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.Key, c.Secret, "")))
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
	return &Client{S3: cl, Bucket: c.Bucket}, nil
}

// MD5ETag is the ETag S3 gives a single-part object: the hex MD5 of its body, quoted.
func MD5ETag(b []byte) string {
	h := md5.Sum(b)
	return `"` + hex.EncodeToString(h[:]) + `"`
}

// SameETag compares ETags ignoring quotes.
func SameETag(a, b string) bool { return strings.Trim(a, `"`) == strings.Trim(b, `"`) }

// PutIfAbsent creates key only if it does not exist (If-None-Match: *).
func (c *Client) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	out, err := c.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &c.Bucket, Key: &key, Body: bytes.NewReader(body), IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// PutIfMatch replaces key only if its current ETag is etag.
func (c *Client) PutIfMatch(ctx context.Context, key, etag string, body []byte) (string, error) {
	out, err := c.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &c.Bucket, Key: &key, Body: bytes.NewReader(body), IfMatch: aws.String(etag),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// Put writes key unconditionally.
func (c *Client) Put(ctx context.Context, key string, body []byte) (string, error) {
	out, err := c.S3.PutObject(ctx, &s3.PutObjectInput{Bucket: &c.Bucket, Key: &key, Body: bytes.NewReader(body)})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// Get returns the body and ETag of key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, string, error) {
	out, err := c.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: &c.Bucket, Key: &key})
	if err != nil {
		return nil, "", err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	return b, aws.ToString(out.ETag), err
}

// DeleteIfMatch deletes key only if its ETag is etag ("*": only if it exists).
func (c *Client) DeleteIfMatch(ctx context.Context, key, etag string) error {
	_, err := c.S3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &c.Bucket, Key: &key, IfMatch: aws.String(etag)})
	return err
}

// ---- the log: create-only slots with epoch fencing ---------------------------

// Entry is one slot of a producer's commit log. Kind is "commit", "seal" or
// "fence"; Content is the batch's content hash for a commit.
type Entry struct {
	Kind    string `json:"kind"`
	Epoch   int    `json:"epoch"`
	Gen     int    `json:"gen,omitempty"`
	Content string `json:"content,omitempty"`
}

// ErrFenced: a slot holds an entry of a newer epoch, so this writer must halt.
var ErrFenced = errors.New("s3cas: writer fenced by a newer epoch")

// SlotKey names slot n of a log.
func SlotKey(prefix string, n int) string { return fmt.Sprintf("%s/log/%020d.json", prefix, n) }

// Writer appends to one producer's log. It is not safe for concurrent use:
// one incarnation appends sequentially (group commit would batch payloads
// into one entry, as turbopuffer's WAL does).
type Writer struct {
	C      *Client
	Prefix string
	Epoch  int
	Next   int             // next slot to try
	Known  map[string]bool // content hashes committed in the log, as far as this writer has read
}

// Append puts e into the first free slot at or after w.Next, with If-None-Match.
// On 412 it reads the slot: its own entry (an earlier attempt that landed, for
// example after a timeout) counts as success; a newer epoch means fenced;
// anything else is skipped over. It returns the slot e landed in, or
// alreadyCommitted = true when the log already holds a commit of e.Content.
func (w *Writer) Append(ctx context.Context, e Entry) (slot int, alreadyCommitted bool, err error) {
	body, _ := json.Marshal(e)
	for {
		key := SlotKey(w.Prefix, w.Next)
		_, perr := w.C.PutIfAbsent(ctx, key, body)
		o, _, _ := Classify(perr)
		switch o {
		case OK:
			w.learn(e)
			w.Next++
			return w.Next - 1, false, nil
		case PreconditionFailed, Other, Conflict:
			// Other (a timeout) and 409 are resolved the same way: read the slot.
			got, _, gerr := w.C.Get(ctx, key)
			if gerr != nil {
				if o2, _, _ := Classify(gerr); o2 == NotFound && o != PreconditionFailed {
					continue // our attempt did not land: resend the same slot
				}
				return 0, false, fmt.Errorf("read slot %d: %w (put: %v)", w.Next, gerr, perr)
			}
			var x Entry
			if err := json.Unmarshal(got, &x); err != nil {
				return 0, false, err
			}
			switch {
			case x == e:
				w.learn(e)
				w.Next++
				return w.Next - 1, false, nil
			case x.Epoch > w.Epoch:
				return 0, false, ErrFenced
			case e.Kind == "commit" && x.Kind == "commit" && x.Content == e.Content:
				w.learn(x)
				w.Next++
				return w.Next - 1, true, nil
			default:
				w.learn(x)
				w.Next++
			}
		default:
			// Unauthorized (e.g. credentials expired mid-refresh) was not
			// applied, but an EARLIER attempt at this slot may have been: the
			// caller must retry this same entry, which resolves both cases.
			return 0, false, perr
		}
	}
}

func (w *Writer) learn(e Entry) {
	if w.Known == nil {
		w.Known = map[string]bool{}
	}
	if e.Kind == "commit" {
		w.Known[e.Content] = true
	}
}

// Start begins incarnation epoch: it appends a fence entry at the first free
// slot, which closes the log to every older epoch, then replays the log below
// the fence so Known holds every content hash already committed.
func Start(ctx context.Context, c *Client, prefix string, epoch int) (*Writer, int, error) {
	w := &Writer{C: c, Prefix: prefix, Epoch: epoch}
	fence, _, err := w.Append(ctx, Entry{Kind: "fence", Epoch: epoch})
	if err != nil {
		return nil, 0, err
	}
	for i := 0; i < fence; i++ {
		b, _, err := c.Get(ctx, SlotKey(prefix, i))
		if err != nil {
			return nil, 0, err
		}
		var x Entry
		if err := json.Unmarshal(b, &x); err != nil {
			return nil, 0, err
		}
		w.learn(x)
	}
	return w, fence, nil
}

// Commit publishes content hash h: it is a no-op when h is already known.
func (w *Writer) Commit(ctx context.Context, h string, gen int) (int, bool, error) {
	if w.Known[h] {
		return -1, true, nil
	}
	return w.Append(ctx, Entry{Kind: "commit", Epoch: w.Epoch, Gen: gen, Content: h})
}
