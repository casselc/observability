// Package s3emu is an in-memory S3 that speaks enough of the REST API for
// aws-sdk-go-v2's S3 client (path-style, SigV4 not checked): PUT with
// `If-None-Match: *` or `If-Match`, GET, HEAD, DELETE and LIST v2 (prefix,
// delimiter, start-after, continuation). It is strongly consistent, like AWS
// S3 and SeaweedFS, except where a fault says otherwise. A port of the Rust
// consumer's emulator (../../otap-rs/tests/dst/s3emu.rs) for the Go edge's
// deterministic simulation: served by net/http/httptest.NewTestServer inside
// a testing/synctest bubble, the real SDK stack (signing, retries, XML
// errors, checksums) runs against it in fake time.
//
// Faults are decided per request by Emu.Faults:
//
//   - Refuse: 503 SlowDown, nothing applied;
//   - ErrorAfter: applied, then answered 500 InternalError (the client's
//     retry meets its own write: 412);
//   - DropAfter: applied, then the connection is cut before the answer;
//   - DropBefore: not applied, the connection is cut;
//   - Late: the connection is cut now and the request is applied After
//     later, its condition evaluated when it lands (a late-landing write,
//     STPA CAST 50);
//   - Slow: the request is held After, then applied and answered (the
//     client has usually timed out by then: a lost answer to a write that
//     still lands).
package s3emu

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fault is what happens to one request.
type Fault int

const (
	None Fault = iota
	Refuse
	ErrorAfter
	DropAfter
	DropBefore
	Late
	Slow
)

func (f Fault) String() string {
	return [...]string{"none", "refuse", "error_after", "drop_after", "drop_before", "late", "slow"}[f]
}

// Decision is a fault and, for Late and Slow, its delay.
type Decision struct {
	Fault Fault
	After time.Duration
}

// Op names a request kind for Faults: GET, HEAD, PUT, PUT-CREATE,
// PUT-IFMATCH, LIST, DELETE.
type Faults func(op, key string) Decision

// Obj is a stored object.
type Obj struct {
	Body        []byte
	Meta        map[string]string // lower-case, without x-amz-meta-
	ETag        string            // quoted
	Modified    time.Time
	ContentType string
}

// Change is one applied write or delete, in the order applied.
type Change struct {
	Op   string // PUT, PUT-CREATE, PUT-IFMATCH, DELETE
	Key  string
	Obj  *Obj // nil for DELETE
	Late bool // a Late or Slow request that landed after its connection was gone
	At   time.Time
}

// Mutation is a deliberate store bug, for showing the simulation catches it.
type Mutation int

const (
	NoMutation        Mutation = iota
	IgnoreIfNoneMatch          // create-only PUTs overwrite
	IgnoreIfMatch              // If-Match PUTs replace whatever is there
)

// Emu is one bucket.
type Emu struct {
	Bucket   string
	Faults   Faults
	Mutation Mutation
	// OnChange sees every applied write, under the emulator's lock.
	OnChange func(Change)
	// Log, if set, gets one line per request.
	Log func(string)

	mu      sync.Mutex
	objs    map[string]*Obj
	n       uint64
	pending sync.WaitGroup
	counts  map[string]int
}

// New makes an empty bucket.
func New(bucket string) *Emu {
	return &Emu{Bucket: bucket, objs: map[string]*Obj{}, counts: map[string]int{}}
}

// Wait blocks until every Late and Slow request has landed or failed.
func (e *Emu) Wait() { e.pending.Wait() }

// Get returns a copy of an object.
func (e *Emu) Get(key string) (Obj, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	o, ok := e.objs[key]
	if !ok {
		return Obj{}, false
	}
	c := *o
	c.Meta = maps.Clone(o.Meta)
	return c, true
}

// Keys lists the keys under prefix, sorted.
func (e *Emu) Keys(prefix string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []string
	for k := range e.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Put stores an object directly (a consumer's tombstone), create-only;
// false if the key is taken.
func (e *Emu) Put(key string, body []byte, meta map[string]string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.objs[key]; ok {
		return false
	}
	e.store("PUT-CREATE", key, body, meta, "", false)
	return true
}

// Count is how many requests of op were received (faulted ones included).
func (e *Emu) Count(op string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.counts[op]
}

func (e *Emu) log(format string, args ...any) {
	if e.Log != nil {
		e.Log(fmt.Sprintf(format, args...))
	}
}

// ---- HTTP ----------------------------------------------------------------

type answer struct {
	status int
	header http.Header
	body   []byte
}

func xmlError(status int, code, msg, resource string) answer {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n<Error><Code>")
	xml.EscapeText(&b, []byte(code))
	b.WriteString("</Code><Message>")
	xml.EscapeText(&b, []byte(msg))
	b.WriteString("</Message><Resource>")
	xml.EscapeText(&b, []byte(resource))
	b.WriteString("</Resource><RequestId>sim</RequestId></Error>")
	return answer{status: status, header: http.Header{"Content-Type": {"application/xml"}}, body: b.Bytes()}
}

func precondition(key string) answer {
	return xmlError(http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", key)
}

// request is what the handler keeps of an HTTP request (the body read).
type request struct {
	method string
	key    string
	query  url.Values
	header http.Header
	body   []byte
}

func opOf(r *request) string {
	switch {
	case r.method == http.MethodPut && r.key != "":
		switch {
		case r.header.Get("If-None-Match") != "":
			return "PUT-CREATE"
		case r.header.Get("If-Match") != "":
			return "PUT-IFMATCH"
		}
		return "PUT"
	case r.method == http.MethodGet && r.key != "":
		return "GET"
	case r.method == http.MethodHead && r.key != "":
		return "HEAD"
	case r.method == http.MethodGet:
		return "LIST"
	case r.method == http.MethodDelete && r.key != "":
		return "DELETE"
	}
	return ""
}

func (e *Emu) ServeHTTP(w http.ResponseWriter, hr *http.Request) {
	path := strings.TrimPrefix(hr.URL.EscapedPath(), "/")
	bucket, rawKey, _ := strings.Cut(path, "/")
	key, err := url.PathUnescape(rawKey)
	if err != nil {
		key = rawKey
	}
	body, err := readBody(hr)
	if err != nil {
		write(w, xmlError(http.StatusBadRequest, "IncompleteBody", err.Error(), key))
		return
	}
	r := &request{method: hr.Method, key: key, query: hr.URL.Query(), header: hr.Header, body: body}
	if bucket != e.Bucket {
		write(w, xmlError(http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucket))
		return
	}
	op := opOf(r)
	if op == "" {
		write(w, xmlError(http.StatusNotImplemented, "NotImplemented", "not emulated", hr.URL.Path))
		return
	}
	e.mu.Lock()
	e.counts[op]++
	e.mu.Unlock()
	d := Decision{}
	if e.Faults != nil {
		d = e.Faults(op, key)
	}
	switch d.Fault {
	case Refuse:
		e.log("S3 %s %s -> 503 (injected)", op, key)
		write(w, xmlError(http.StatusServiceUnavailable, "SlowDown", "Please reduce your request rate.", key))
		return
	case DropBefore:
		e.log("S3 %s %s -> dropped before (injected)", op, key)
		panic(http.ErrAbortHandler)
	case Late:
		e.pending.Add(1)
		go func() {
			defer e.pending.Done()
			time.Sleep(d.After)
			a := e.apply(op, r, true)
			e.log("S3 %s %s -> %d, landed %v late (injected)", op, key, a.status, d.After)
		}()
		e.log("S3 %s %s -> dropped, to land in %v (injected)", op, key, d.After)
		panic(http.ErrAbortHandler)
	case Slow:
		e.pending.Add(1)
		defer e.pending.Done()
		time.Sleep(d.After)
		a := e.apply(op, r, hr.Context().Err() != nil)
		e.log("S3 %s %s -> %d after %v (injected)", op, key, a.status, d.After)
		write(w, a)
		return
	}
	a := e.apply(op, r, false)
	switch d.Fault {
	case ErrorAfter:
		e.log("S3 %s %s -> %d, answered 500 (injected)", op, key, a.status)
		write(w, xmlError(http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.", key))
	case DropAfter:
		e.log("S3 %s %s -> %d, dropped (injected)", op, key, a.status)
		panic(http.ErrAbortHandler)
	default:
		e.log("S3 %s %s -> %d", op, key, a.status)
		write(w, a)
	}
}

func write(w http.ResponseWriter, a answer) {
	for k, v := range a.header {
		w.Header()[k] = v
	}
	if a.header.Get("Content-Length") == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(a.body)))
	}
	w.WriteHeader(a.status)
	_, _ = w.Write(a.body)
}

// readBody reads the payload, decoding aws-chunked framing (the SDK's
// streaming checksum trailers) when the request uses it.
func readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		return b, nil
	}
	return decodeChunked(b)
}

func decodeChunked(b []byte) ([]byte, error) {
	var out []byte
	br := bufio.NewReader(bytes.NewReader(b))
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("aws-chunked: %w", err)
		}
		size, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		n, err := strconv.ParseInt(size, 16, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("aws-chunked: bad size %q", size)
		}
		if n == 0 {
			return out, nil // trailers follow; not checked
		}
		chunk := make([]byte, n)
		if _, err := io.ReadFull(br, chunk); err != nil {
			return nil, fmt.Errorf("aws-chunked: %w", err)
		}
		out = append(out, chunk...)
		if _, err := br.ReadString('\n'); err != nil {
			return nil, fmt.Errorf("aws-chunked: %w", err)
		}
	}
}

func (e *Emu) apply(op string, r *request, late bool) answer {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch op {
	case "PUT", "PUT-CREATE", "PUT-IFMATCH":
		return e.put(op, r, late)
	case "GET", "HEAD":
		return e.get(r, op == "HEAD")
	case "LIST":
		return e.list(r.query)
	case "DELETE":
		delete(e.objs, r.key)
		if e.OnChange != nil {
			e.OnChange(Change{Op: op, Key: r.key, Late: late, At: time.Now()})
		}
		return answer{status: http.StatusNoContent}
	}
	panic(op)
}

func (e *Emu) put(op string, r *request, late bool) answer {
	cur, exists := e.objs[r.key]
	if v := r.header.Get("If-None-Match"); v == "*" && exists && e.Mutation != IgnoreIfNoneMatch {
		return precondition(r.key)
	}
	if want := r.header.Get("If-Match"); want != "" && e.Mutation != IgnoreIfMatch {
		if !exists {
			return xmlError(http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", r.key)
		}
		if strings.Trim(cur.ETag, `"`) != strings.Trim(want, `"`) {
			return precondition(r.key)
		}
	}
	meta := map[string]string{}
	for k, v := range r.header {
		if name, ok := strings.CutPrefix(strings.ToLower(k), "x-amz-meta-"); ok && len(v) > 0 {
			meta[name] = v[0]
		}
	}
	ct := r.header.Get("Content-Type")
	if ct == "" {
		ct = "binary/octet-stream"
	}
	o := e.store(op, r.key, r.body, meta, ct, late)
	return answer{status: http.StatusOK, header: http.Header{"Etag": {o.ETag}}}
}

func (e *Emu) store(op, key string, body []byte, meta map[string]string, ct string, late bool) *Obj {
	e.n++
	// The content MD5, as AWS S3 and SeaweedFS give for a single-part PUT
	// (the lane writer and casreg rely on it).
	sum := md5.Sum(body)
	o := &Obj{Body: append([]byte(nil), body...), Meta: maps.Clone(meta), ETag: `"` + hex.EncodeToString(sum[:]) + `"`,
		Modified: time.Now(), ContentType: ct}
	e.objs[key] = o
	if e.OnChange != nil {
		e.OnChange(Change{Op: op, Key: key, Obj: o, Late: late, At: o.Modified})
	}
	return o
}

func (e *Emu) get(r *request, head bool) answer {
	o, ok := e.objs[r.key]
	if !ok {
		if head {
			return answer{status: http.StatusNotFound}
		}
		return xmlError(http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", r.key)
	}
	if want := r.header.Get("If-Match"); want != "" && strings.Trim(want, `"`) != strings.Trim(o.ETag, `"`) {
		return precondition(r.key)
	}
	h := http.Header{
		"Etag":           {o.ETag},
		"Last-Modified":  {o.Modified.UTC().Format(http.TimeFormat)},
		"Content-Length": {strconv.Itoa(len(o.Body))},
		"Content-Type":   {o.ContentType},
		"Accept-Ranges":  {"bytes"},
	}
	for k, v := range o.Meta {
		h.Set("X-Amz-Meta-"+k, v)
	}
	a := answer{status: http.StatusOK, header: h}
	if !head {
		a.body = o.Body
	}
	return a
}

type listResult struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	Contents              []listContent  `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type listContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func (e *Emu) list(q url.Values) answer {
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	after := q.Get("continuation-token")
	if after == "" {
		after = q.Get("start-after")
	}
	max := 1000
	if v, err := strconv.Atoi(q.Get("max-keys")); err == nil && v >= 0 && v < max {
		max = v
	}
	keys := make([]string, 0, len(e.objs))
	for k := range e.objs {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := listResult{Name: e.Bucket, Prefix: prefix, Delimiter: delim, MaxKeys: max}
	last := ""
	for _, k := range keys {
		if len(res.Contents)+len(res.CommonPrefixes) >= max {
			res.IsTruncated = true
			break
		}
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				p := k[:len(prefix)+i+len(delim)]
				if n := len(res.CommonPrefixes); n == 0 || res.CommonPrefixes[n-1].Prefix != p {
					res.CommonPrefixes = append(res.CommonPrefixes, commonPrefix{p})
				}
				last = k
				continue
			}
		}
		o := e.objs[k]
		res.Contents = append(res.Contents, listContent{Key: k, LastModified: o.Modified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag: o.ETag, Size: len(o.Body), StorageClass: "STANDARD"})
		last = k
	}
	res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)
	if res.IsTruncated {
		res.NextContinuationToken = last
	}
	b, err := xml.Marshal(res)
	if err != nil {
		return xmlError(http.StatusInternalServerError, "InternalError", err.Error(), "")
	}
	return answer{status: http.StatusOK, header: http.Header{"Content-Type": {"application/xml"}},
		body: append([]byte(xml.Header), b...)}
}
