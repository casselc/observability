package inline

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Store is the bucket and key prefix a producer's logs live under.
type Store struct {
	S3     *s3.Client
	Bucket string
	Prefix string // {root}/{region}/{signal}/v{schema}/{producer}
}

// Object is one encoded batch, ready to be put in a slot.
type Object struct {
	Body        []byte
	ContentType string
	Meta        map[string]string // rows, times, ...; the log adds kind, epoch, seq, content
}

// Encoder encodes the batch for a given slot. It is called again only if
// the batch has to move to another slot (another batch or a tombstone took
// the one it was encoded for), so envelope columns can carry epoch and seq.
type Encoder func(epoch string, seq uint64) (Object, error)

// NewEpoch names a new log: sortable by start time, unique by a random suffix.
func NewEpoch() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return time.Now().UTC().Format("20060102T150405.000Z") + "-" + hex.EncodeToString(b[:])
}

// Outcome of a conditional request.
type Outcome int

const (
	OK      Outcome = iota
	Exists          // 412: the key is taken
	Missing         // 404
	Unknown         // anything else, timeouts and cancellations included: may or may not have applied
)

func Classify(err error) Outcome {
	if err == nil {
		return OK
	}
	var re *smithyhttp.ResponseError
	status := 0
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
		return Exists
	case status == http.StatusNotFound || code == "NotFound" || code == "NoSuchKey":
		return Missing
	}
	return Unknown
}

// PutIfAbsent is PUT If-None-Match: * with user metadata.
func (st *Store) PutIfAbsent(ctx context.Context, key string, body []byte, contentType string, meta map[string]string) error {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(st.Bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		IfNoneMatch:   aws.String("*"),
		Metadata:      meta,
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	_, err := st.S3.PutObject(ctx, in)
	return err
}

// Head returns an object's user metadata and size; found is false on 404.
func (st *Store) Head(ctx context.Context, key string) (meta map[string]string, size int64, found bool, err error) {
	out, err := st.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(st.Bucket), Key: aws.String(key)})
	if Classify(err) == Missing {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	return out.Metadata, aws.ToInt64(out.ContentLength), true, nil
}

// Stats counts what the writer saw; every field is an event of the model.
type Stats struct {
	Committed    atomic.Int64 // 200 on the first try
	ResolvedOwn  atomic.Int64 // 412 or unknown outcome, then HEAD found our batch
	Resent       atomic.Int64 // unknown outcome, HEAD found the slot free
	LearnedOther atomic.Int64 // the slot held another batch: committed, appended after it
	Halted       atomic.Int64 // the slot held a tombstone: this epoch is closed; new epoch
	KnownSkipped atomic.Int64 // a retry of a batch this log already committed
}

// Log is one writer lane: a single epoch's log, appended one batch at a time.
type Log struct {
	st       *Store
	producer string
	newEpoch func() string

	mu     sync.Mutex
	epoch  string
	next   uint64
	recent map[string]Ref // content hash -> where it is committed (bounded)
	order  []string
	Stats  *Stats
}

// Ref is a committed batch's place in the logs.
type Ref struct {
	Epoch string
	Seq   uint64
}

func NewLog(st *Store, producer string, stats *Stats) *Log {
	if stats == nil {
		stats = &Stats{}
	}
	return &Log{st: st, producer: producer, newEpoch: NewEpoch, epoch: NewEpoch(), recent: map[string]Ref{}, Stats: stats}
}

// Epoch returns the log's current epoch.
func (l *Log) Epoch() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

const recentCap = 4096

// headTimeout bounds the HEAD that resolves an unknown outcome.
var headTimeout = 2 * time.Second

func (l *Log) remember(content string, r Ref) {
	if _, ok := l.recent[content]; ok {
		return
	}
	l.recent[content] = r
	l.order = append(l.order, content)
	if len(l.order) > recentCap {
		delete(l.recent, l.order[0])
		l.order = l.order[1:]
	}
}

// Append commits the batch with content hash content at the next free slot
// of this log. It returns once the batch is committed (by this call, by an
// earlier attempt whose response was lost, or found already committed), or
// with an error if the outcome is not known yet; calling Append again with
// the same content resolves it without committing twice.
func (l *Log) Append(ctx context.Context, content string, enc Encoder) (Ref, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r, ok := l.recent[content]; ok {
		l.Stats.KnownSkipped.Add(1)
		return r, nil
	}
	var obj Object
	encodedFor := Ref{Seq: ^uint64(0)}
	for {
		if err := ctx.Err(); err != nil {
			return Ref{}, err
		}
		here := Ref{Epoch: l.epoch, Seq: l.next}
		if encodedFor != here {
			var err error
			if obj, err = enc(here.Epoch, here.Seq); err != nil {
				return Ref{}, err
			}
			encodedFor = here
		}
		meta := map[string]string{
			MetaKind: KindData, MetaEpoch: here.Epoch, MetaSeq: strconv.FormatUint(here.Seq, 10),
			MetaContent: content, MetaProducer: l.producer,
		}
		for k, v := range obj.Meta {
			meta[k] = v
		}
		key := SlotKey(l.st.Prefix, here.Epoch, here.Seq)
		err := l.st.PutIfAbsent(ctx, key, obj.Body, obj.ContentType, meta)
		o := Classify(err)
		if o == OK {
			l.Stats.Committed.Add(1)
			l.remember(content, here)
			l.next++
			return here, nil
		}
		if o != Exists && o != Unknown {
			return Ref{}, err
		}
		// 412, or no answer: read the slot. With no answer the request may
		// still be in flight; If-None-Match lets at most one copy land.
		// The exporter's deadline has usually passed when the PUT timed out;
		// HEAD on a short detached deadline so the outcome is still resolved
		// in this call when S3 answers.
		hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), headTimeout)
		m, _, found, herr := l.st.Head(hctx, key)
		cancel()
		if herr != nil {
			return Ref{}, fmt.Errorf("put %s: %v; head: %w", key, err, herr)
		}
		switch {
		case !found:
			if o == Exists { // 412 then 404: deleted in between (GC below a horizon); never reuse it
				l.next++
				continue
			}
			l.Stats.Resent.Add(1)
		case m[MetaKind] == KindTomb:
			// The consumer closed this epoch here: nothing of ours is in it
			// from this slot on. Start a new log and append there.
			l.Stats.Halted.Add(1)
			l.epoch, l.next = l.newEpoch(), 0
		case m[MetaContent] == content && m[MetaEpoch] == here.Epoch:
			l.Stats.ResolvedOwn.Add(1)
			l.remember(content, here)
			l.next++
			return here, nil
		default:
			l.Stats.LearnedOther.Add(1)
			l.remember(m[MetaContent], here)
			l.next++
		}
	}
}

// Lanes spreads batches over n logs by content hash, so a retry of a batch
// always meets the lane (and the unresolved slot) of its first attempt.
type Lanes struct {
	logs  []*Log
	Stats *Stats
}

func NewLanes(st *Store, producer string, n int) *Lanes {
	if n < 1 {
		n = 1
	}
	s := &Stats{}
	ls := &Lanes{Stats: s}
	for range n {
		ls.logs = append(ls.logs, NewLog(st, producer, s))
	}
	return ls
}

func (ls *Lanes) Append(ctx context.Context, content string, enc Encoder) (Ref, error) {
	var h uint64
	for i := 0; i < 16 && i < len(content); i++ {
		h = h*31 + uint64(content[i])
	}
	return ls.logs[h%uint64(len(ls.logs))].Append(ctx, content, enc)
}

func (ls *Lanes) Epochs() []string {
	var out []string
	for _, l := range ls.logs {
		out = append(out, l.Epoch())
	}
	return out
}
