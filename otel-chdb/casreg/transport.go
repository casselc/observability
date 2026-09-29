package casreg

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Transport records the object requests an S3 client sends through it
// (path-style: /bucket/key) as a history, whatever the client: an SDK, a
// lane writer, an acceptance run against a real store. A value is the hex
// MD5 of the object's bytes, and an If-Match ETag is taken to name the
// value it is the MD5 of: that holds for single-part PUTs on AWS S3 and
// SeaweedFS (s3accept checks it, "etag-md5"); on a store whose ETags are
// not content MD5s, a history with If-Match is not meaningful.
//
// Answers: 2xx is OK; 412, and 404 to an If-Match, are Failed; anything
// else (5xx, a reset, a timeout) is Unknown. A read's value is the MD5 of
// the GET's body, or the ETag of a HEAD.
type Transport struct {
	Base   http.RoundTripper
	Rec    *Recorder
	Client int
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	in, ok, err := classify(req)
	if err != nil || !ok {
		if k := untracked(req); k != "" {
			t.Rec.Taint(k)
		}
		return base.RoundTrip(req)
	}
	call := t.Rec.Begin(t.Client, in)
	resp, err := base.RoundTrip(req)
	if err != nil {
		call.End(Output{Result: Unknown})
		return resp, err
	}
	out := Output{Result: Unknown}
	switch s := resp.StatusCode; {
	case in.Op == Read && s == http.StatusNotFound:
		out = Output{Result: OK}
	case in.Op == Read && s/100 == 2:
		v := strings.Trim(resp.Header.Get("ETag"), `"`)
		if req.Method == http.MethodGet {
			b, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(b))
			if rerr != nil {
				break // the body was cut: the read tells nothing
			}
			v = md5hex(b)
		}
		out = Output{Result: OK, Found: true, Value: v}
	case s/100 == 2:
		out = Output{Result: OK}
	case s == http.StatusPreconditionFailed, s == http.StatusNotFound && in.Op == Swap:
		out = Output{Result: Failed}
	}
	call.End(out)
	return resp, nil
}

// untracked names the key a request changes outside the model (a
// multipart completion, a copy, a conditional DELETE), or "".
func untracked(req *http.Request) string {
	q := req.URL.Query()
	p, err := url.PathUnescape(req.URL.EscapedPath())
	if err != nil {
		return ""
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(p, "/"), "/")
	switch {
	case key == "":
		return "" // a batch DeleteObjects: its keys are in the body (not recorded; avoid mixing it with modelled writes)
	case req.Method == http.MethodPost && q.Has("uploadId"),
		req.Method == http.MethodPut && req.Header.Get("X-Amz-Copy-Source") != "",
		req.Method == http.MethodDelete && req.Header.Get("If-Match") != "":
		return bucket + "/" + key
	}
	return ""
}

func md5hex(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

// classify maps an object request onto an Input; ok is false for requests
// that are not single-object reads or writes (LIST, multipart, ...), which
// pass through unrecorded. A PUT's body is read (and put back) to name its
// value.
func classify(req *http.Request) (Input, bool, error) {
	q := req.URL.Query()
	if q.Has("uploads") || q.Has("uploadId") || q.Has("list-type") || q.Has("delete") || q.Has("tagging") || q.Has("acl") {
		return Input{}, false, nil
	}
	p, err := url.PathUnescape(req.URL.EscapedPath())
	if err != nil {
		return Input{}, false, err
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(p, "/"), "/")
	if key == "" {
		return Input{}, false, nil
	}
	in := Input{Key: bucket + "/" + key}
	switch req.Method {
	case http.MethodGet, http.MethodHead:
		if req.Header.Get("Range") != "" || req.Header.Get("If-Match") != "" || req.Header.Get("If-None-Match") != "" {
			return Input{}, false, nil // partial or conditional reads: not modelled
		}
		in.Op = Read
		return in, true, nil
	case http.MethodDelete:
		if req.Header.Get("If-Match") != "" {
			return Input{}, false, nil
		}
		in.Op = Delete
		return in, true, nil
	case http.MethodPut:
		if req.Header.Get("X-Amz-Copy-Source") != "" {
			return Input{}, false, nil
		}
	default:
		return Input{}, false, nil
	}
	body, err := payload(req)
	if err != nil {
		return Input{}, false, err
	}
	in.Value = md5hex(body)
	switch {
	case req.Header.Get("If-None-Match") == "*":
		in.Op = Create
	case req.Header.Get("If-Match") != "":
		in.Op, in.Expect = Swap, strings.Trim(req.Header.Get("If-Match"), `"`)
	default:
		in.Op = Write
	}
	return in, true, nil
}

// payload returns the object bytes of a PUT (decoding aws-chunked framing)
// and restores the request body for sending.
func payload(req *http.Request) ([]byte, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	if req.GetBody != nil {
		rc, err := req.GetBody()
		if err == nil {
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err == nil {
				return decode(req, b)
			}
		}
	}
	b, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(b))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	return decode(req, b)
}

func decode(req *http.Request, b []byte) ([]byte, error) {
	if !strings.Contains(req.Header.Get("Content-Encoding"), "aws-chunked") {
		return b, nil
	}
	return DecodeAWSChunked(b)
}

// DecodeAWSChunked decodes the aws-chunked content encoding (SigV4
// streaming payloads and the SDKs' trailing checksums): the chunks'
// bytes, without signatures or trailers.
func DecodeAWSChunked(b []byte) ([]byte, error) {
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
			return nil, errors.New("aws-chunked: bad chunk size " + strconv.Quote(size))
		}
		if n == 0 {
			return out, nil
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
