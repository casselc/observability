package s3cas

// Probes of an S3 endpoint's conditional operations, against the semantics AWS
// documents (see ../model/S3NATIVE.md for sources). Skipped unless
// S3CAS_BUCKET is set. Credentials come from the AWS default chain:
//
//	# SeaweedFS (static keys through the environment)
//	S3CAS_BUCKET=otel S3CAS_ENDPOINT=http://127.0.0.1:18333 \
//	  AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret go test -v -count=1 .
//	# AWS with IRSA / Pod Identity / Roles Anywhere: just the bucket and region
//	S3CAS_BUCKET=my-bucket S3CAS_REGION=eu-west-1 go test -v -count=1 .
//
// It doubles as the acceptance test for a new store (e.g. Nutanix Objects):
// the S3-native mode requires TestPutIfNoneMatch, TestCreateRace,
// TestPutIfMatch and TestCASRace to pass.
//
// Each test logs what the server answered, so a run is the evidence.

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func client(t *testing.T) (*Client, string) {
	t.Helper()
	c, ok := FromEnv()
	if !ok {
		t.Skip("S3CAS_BUCKET not set")
	}
	cl, err := New(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	return cl, fmt.Sprintf("s3cas-probe/%d/%s", time.Now().UnixNano(), t.Name())
}

func expect(t *testing.T, what string, err error, want Outcome) {
	t.Helper()
	got, status, code := Classify(err)
	if got != want {
		t.Errorf("%s: got %v (HTTP %d %s, err %v), want %v", what, got, status, code, err, want)
		return
	}
	t.Logf("%s: %v (HTTP %d %s)", what, got, status, code)
}

func body(t *testing.T, c *Client, key string) string {
	t.Helper()
	b, _, err := c.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return string(b)
}

// If-None-Match: * is create-only; a second create gets 412 and the first body stays.
func TestPutIfNoneMatch(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	k := p + "/obj"
	etag, err := c.PutIfAbsent(ctx, k, []byte("first"))
	expect(t, "first PUT If-None-Match:*", err, OK)
	if !SameETag(etag, MD5ETag([]byte("first"))) {
		t.Errorf("ETag %s is not the MD5 of the body (%s): content-addressed resolution needs it", etag, MD5ETag([]byte("first")))
	} else {
		t.Logf("ETag is the body's MD5: %s", etag)
	}
	_, err = c.PutIfAbsent(ctx, k, []byte("second"))
	expect(t, "second PUT If-None-Match:*", err, PreconditionFailed)
	_, err = c.PutIfAbsent(ctx, k, []byte("first"))
	expect(t, "identical retry PUT If-None-Match:*", err, PreconditionFailed)
	if got := body(t, c, k); got != "first" {
		t.Errorf("body %q, want first", got)
	}
}

// If-Match with the current ETag replaces; a stale ETag gets 412; If-Match on a
// missing key fails (AWS: 404 NoSuchKey).
func TestPutIfMatch(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	k := p + "/obj"
	e1, err := c.Put(ctx, k, []byte("v1"))
	expect(t, "plain PUT v1", err, OK)
	e2, err := c.PutIfMatch(ctx, k, e1, []byte("v2"))
	expect(t, "PUT If-Match current ETag", err, OK)
	_, err = c.PutIfMatch(ctx, k, e1, []byte("v3"))
	expect(t, "PUT If-Match stale ETag", err, PreconditionFailed)
	if got := body(t, c, k); got != "v2" {
		t.Errorf("body %q, want v2", got)
	}
	_, err = c.PutIfMatch(ctx, k, e2, []byte("v2")) // same body: ETag unchanged
	expect(t, "PUT If-Match current ETag, same body", err, OK)
	_, err = c.PutIfMatch(ctx, k, `"`+"00000000000000000000000000000000"+`"`, []byte("x"))
	expect(t, "PUT If-Match wrong ETag", err, PreconditionFailed)
	_, err = c.PutIfMatch(ctx, p+"/missing", e1, []byte("x"))
	got, status, code := Classify(err)
	t.Logf("PUT If-Match on a missing key: %v (HTTP %d %s); AWS documents 404", got, status, code)
	if got == OK {
		t.Errorf("If-Match on a missing key created it")
	}
	// Unquoted ETag, as some clients send it.
	_, cur, _ := c.Get(ctx, k)
	_, err = c.PutIfMatch(ctx, k, cur[1:len(cur)-1], []byte("v4"))
	expect(t, "PUT If-Match current ETag, unquoted", err, OK)
}

func multipart(ctx context.Context, c *Client, key string, parts [][]byte, ifNoneMatch, ifMatch *string) error {
	cr, err := c.S3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &c.Bucket, Key: &key})
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	var done []types.CompletedPart
	for i, b := range parts {
		n := int32(i + 1)
		up, err := c.S3.UploadPart(ctx, &s3.UploadPartInput{Bucket: &c.Bucket, Key: &key, UploadId: cr.UploadId,
			PartNumber: &n, Body: bytes.NewReader(b)})
		if err != nil {
			return fmt.Errorf("part %d: %w", n, err)
		}
		done = append(done, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(n)})
	}
	_, err = c.S3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &c.Bucket, Key: &key,
		UploadId: cr.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: done},
		IfNoneMatch: ifNoneMatch, IfMatch: ifMatch})
	return err
}

// CompleteMultipartUpload honours If-None-Match and If-Match.
func TestMultipartConditional(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	k := p + "/mpu"
	part := bytes.Repeat([]byte("a"), 5<<20)
	star := aws.String("*")
	expect(t, "complete MPU If-None-Match:* on a new key", multipart(ctx, c, k, [][]byte{part, []byte("tail")}, star, nil), OK)
	expect(t, "complete MPU If-None-Match:* on an existing key", multipart(ctx, c, k, [][]byte{part, []byte("x")}, star, nil), PreconditionFailed)
	_, etag, _ := c.Get(ctx, k)
	t.Logf("multipart ETag %s", etag)
	expect(t, "complete MPU If-Match stale", multipart(ctx, c, k, [][]byte{part, []byte("y")}, nil, aws.String(`"0123"`)), PreconditionFailed)
	expect(t, "complete MPU If-Match current", multipart(ctx, c, k, [][]byte{part, []byte("z")}, nil, aws.String(etag)), OK)
	b, _, _ := c.Get(ctx, k)
	if !bytes.HasSuffix(b, []byte("z")) {
		t.Errorf("object does not end with the If-Match upload's tail")
	}
}

// N concurrent multipart uploads race to complete the same key with
// If-None-Match: *. Exactly one may win.
func TestMultipartCreateRace(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	const n, rounds = 8, 5
	part := bytes.Repeat([]byte("b"), 5<<20)
	for r := 0; r < rounds; r++ {
		k := fmt.Sprintf("%s/mpu-race-%d", p, r)
		// Upload all parts first, then complete concurrently: the race is on completion.
		type up struct {
			id    *string
			parts []types.CompletedPart
		}
		ups := make([]up, n)
		for i := range ups {
			cr, err := c.S3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &c.Bucket, Key: &k})
			if err != nil {
				t.Fatal(err)
			}
			var ps []types.CompletedPart
			for j, b := range [][]byte{part, []byte(fmt.Sprintf("writer-%d", i))} {
				pn := int32(j + 1)
				u, err := c.S3.UploadPart(ctx, &s3.UploadPartInput{Bucket: &c.Bucket, Key: &k, UploadId: cr.UploadId, PartNumber: &pn, Body: bytes.NewReader(b)})
				if err != nil {
					t.Fatal(err)
				}
				ps = append(ps, types.CompletedPart{ETag: u.ETag, PartNumber: aws.Int32(pn)})
			}
			ups[i] = up{cr.UploadId, ps}
		}
		var wins, fails, other atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range ups {
			wg.Add(1)
			go func(u up) {
				defer wg.Done()
				<-start
				_, err := c.S3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &c.Bucket, Key: &k,
					UploadId: u.id, MultipartUpload: &types.CompletedMultipartUpload{Parts: u.parts}, IfNoneMatch: aws.String("*")})
				switch o, _, _ := Classify(err); o {
				case OK:
					wins.Add(1)
				case PreconditionFailed, Conflict:
					fails.Add(1)
				default:
					other.Add(1)
					t.Logf("complete: %v", err)
				}
			}(ups[i])
		}
		close(start)
		wg.Wait()
		t.Logf("round %d: %d completions won, %d refused, %d other", r, wins.Load(), fails.Load(), other.Load())
		if wins.Load() != 1 {
			t.Errorf("round %d: %d concurrent If-None-Match completions succeeded, want exactly 1", r, wins.Load())
		}
	}
}

// CopyObject honours If-None-Match on the destination.
func TestCopyIfNoneMatch(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	src, dst := p+"/src", p+"/dst"
	_, _ = c.Put(ctx, src, []byte("src"))
	_, _ = c.Put(ctx, dst, []byte("dst"))
	_, err := c.S3.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &c.Bucket, Key: &dst, CopySource: aws.String(c.Bucket + "/" + src), IfNoneMatch: aws.String("*")})
	expect(t, "CopyObject If-None-Match:* onto an existing key", err, PreconditionFailed)
	if got := body(t, c, dst); got != "dst" {
		t.Errorf("destination overwritten: %q", got)
	}
	_, err = c.S3.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &c.Bucket, Key: aws.String(p + "/new"), CopySource: aws.String(c.Bucket + "/" + src), IfNoneMatch: aws.String("*")})
	expect(t, "CopyObject If-None-Match:* onto a new key", err, OK)
}

// DeleteObject with If-Match: a stale ETag gets 412 and the object stays;
// the current ETag deletes; "*" requires existence.
func TestDeleteIfMatch(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	k := p + "/obj"
	e1, _ := c.Put(ctx, k, []byte("v1"))
	_, _ = c.Put(ctx, k, []byte("v2"))
	expect(t, "DELETE If-Match stale ETag", c.DeleteIfMatch(ctx, k, e1), PreconditionFailed)
	if got := body(t, c, k); got != "v2" {
		t.Errorf("object gone after refused delete: %q", got)
	}
	_, e2, _ := c.Get(ctx, k)
	expect(t, "DELETE If-Match current ETag", c.DeleteIfMatch(ctx, k, e2), OK)
	if _, _, err := c.Get(ctx, k); err == nil {
		t.Errorf("object still there after conditional delete")
	}
	err := c.DeleteIfMatch(ctx, k, "*")
	got, status, code := Classify(err)
	t.Logf("DELETE If-Match:* on a missing key: %v (HTTP %d %s); AWS documents 412 (or 404)", got, status, code)
	if got == OK {
		t.Logf("  note: a 204 here is indistinguishable from a successful delete")
	}
	_, _ = c.Put(ctx, k, []byte("v3"))
	expect(t, "DELETE If-Match:* on an existing key", c.DeleteIfMatch(ctx, k, "*"), OK)

	// DeleteObjects with per-object ETags.
	ka, kb := p+"/a", p+"/b"
	ea, _ := c.Put(ctx, ka, []byte("a"))
	_, _ = c.Put(ctx, kb, []byte("b"))
	out, err := c.S3.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &c.Bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{
		{Key: &ka, ETag: aws.String(ea)}, {Key: &kb, ETag: aws.String(`"0123"`)},
	}}})
	if err != nil {
		t.Errorf("DeleteObjects: %v", err)
		return
	}
	var deleted, errored []string
	for _, d := range out.Deleted {
		deleted = append(deleted, aws.ToString(d.Key))
	}
	for _, e := range out.Errors {
		errored = append(errored, aws.ToString(e.Key)+":"+aws.ToString(e.Code))
	}
	t.Logf("DeleteObjects with ETags: deleted %v, errors %v", deleted, errored)
	if _, _, err := c.Get(ctx, kb); err != nil {
		t.Errorf("DeleteObjects deleted %s despite a wrong ETag", kb)
	}
}

// N goroutines race If-None-Match creates of one key per round: exactly one wins.
func TestCreateRace(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	const n, rounds = 16, 20
	for r := 0; r < rounds; r++ {
		k := fmt.Sprintf("%s/slot-%d", p, r)
		var wins atomic.Int32
		var winner atomic.Value
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				b := []byte(fmt.Sprintf("writer-%d", i))
				_, err := c.PutIfAbsent(ctx, k, b)
				if o, _, _ := Classify(err); o == OK {
					wins.Add(1)
					winner.Store(string(b))
				} else if o != PreconditionFailed && o != Conflict {
					t.Errorf("unexpected: %v", err)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Errorf("round %d: %d winners", r, wins.Load())
		} else if got := body(t, c, k); got != winner.Load() {
			t.Errorf("round %d: stored %q, winner %q", r, got, winner.Load())
		}
	}
	t.Logf("%d rounds x %d concurrent If-None-Match creates: exactly one winner each", rounds, n)
}

// N goroutines increment a counter by read-modify-CAS. Every successful CAS
// is one increment; lost updates would show as a final value below the
// number of successes, and double wins per round as duplicates.
func TestCASRace(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	k := p + "/counter"
	if _, err := c.Put(ctx, k, []byte("0")); err != nil {
		t.Fatal(err)
	}
	const n, perWriter = 8, 25
	var success, refused atomic.Int32
	seen := sync.Map{} // value -> writer that installed it
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for done := 0; done < perWriter; {
				b, etag, err := c.Get(ctx, k)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				var v int
				fmt.Sscan(string(b), &v)
				nb := []byte(fmt.Sprint(v + 1))
				_, err = c.PutIfMatch(ctx, k, etag, nb)
				switch o, _, _ := Classify(err); o {
				case OK:
					if prev, dup := seen.LoadOrStore(v+1, i); dup {
						t.Errorf("value %d installed twice (writers %v and %d)", v+1, prev, i)
					}
					success.Add(1)
					done++
				case PreconditionFailed, Conflict:
					refused.Add(1)
				default:
					t.Errorf("CAS: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	final := body(t, c, k)
	t.Logf("%d writers x %d increments: %d CAS won, %d refused, final value %s", n, perWriter, success.Load(), refused.Load(), final)
	if final != fmt.Sprint(n*perWriter) || int(success.Load()) != n*perWriter {
		t.Errorf("lost update: final %s, successes %d, want %d", final, success.Load(), n*perWriter)
	}
}

// The ambiguous-timeout recovery the design relies on. The client cancels
// each create-only PUT the moment its request is fully written, so it always
// sees an error, although the server has the whole request and may apply it.
// The retry (same body) either creates the object (the first never landed) or
// gets 412, and then a read whose ETag is the MD5 of our body says our first
// attempt landed: committed exactly once either way.
func TestAmbiguousCreateResolution(t *testing.T) {
	c, p := client(t)
	landed, notLanded := 0, 0
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("%s/slot-%d", p, i)
		b := []byte(fmt.Sprintf(`{"kind":"commit","epoch":1,"content":"h%d"}`, i))
		ctx, cancel := context.WithCancel(context.Background())
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { cancel() }})
		_, err := c.PutIfAbsent(ctx, k, b)
		cancel()
		if err == nil {
			t.Logf("attempt %d: the response beat the cancel; not ambiguous", i)
			continue
		}
		time.Sleep(50 * time.Millisecond) // let a request the server has finish
		_, err = c.PutIfAbsent(context.Background(), k, b)
		switch o, _, _ := Classify(err); o {
		case OK:
			notLanded++
		case PreconditionFailed:
			_, etag, _ := c.Get(context.Background(), k)
			if !SameETag(etag, MD5ETag(b)) {
				t.Fatalf("412 but the stored ETag %s is not ours", etag)
			}
			landed++
		default:
			t.Fatalf("retry: %v", err)
		}
	}
	t.Logf("20 PUTs cancelled once written: %d had landed (retry: 412, ETag = our MD5), %d had not (retry created it)", landed, notLanded)
}

// The same for the If-Match CAS: an ambiguous CAS is retried with the old
// ETag; 412 plus a current ETag equal to our body's MD5 means our CAS won.
func TestAmbiguousCASResolution(t *testing.T) {
	c, p := client(t)
	k := p + "/head"
	etag, err := c.Put(context.Background(), k, []byte("v0"))
	if err != nil {
		t.Fatal(err)
	}
	won, lost := 0, 0
	for i := 1; i <= 20; i++ {
		b := []byte(fmt.Sprintf("v%d", i))
		ctx, cancel := context.WithCancel(context.Background())
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { cancel() }})
		_, err := c.PutIfMatch(ctx, k, etag, b)
		cancel()
		if err == nil {
			etag = MD5ETag(b)
			continue
		}
		time.Sleep(50 * time.Millisecond)
		ne, err := c.PutIfMatch(context.Background(), k, etag, b)
		switch o, _, _ := Classify(err); o {
		case OK:
			lost++ // the first attempt had not applied; the retry did
			etag = ne
		case PreconditionFailed:
			_, cur, _ := c.Get(context.Background(), k)
			if !SameETag(cur, MD5ETag(b)) {
				t.Fatalf("412 and the current ETag %s is not ours: someone else won", cur)
			}
			won++
			etag = cur
		default:
			t.Fatalf("retry: %v", err)
		}
	}
	t.Logf("20 CASes cancelled once written: %d had applied (resolved by ETag), %d had not (retry applied)", won, lost)
}

// Concurrent CAS and conditional delete on the same ETag: at most one wins.
func TestCASvsConditionalDelete(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	both := 0
	const rounds = 50
	for r := 0; r < rounds; r++ {
		k := fmt.Sprintf("%s/obj-%d", p, r)
		etag, _ := c.Put(ctx, k, []byte("v0"))
		var putErr, delErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, putErr = c.PutIfMatch(ctx, k, etag, []byte("v1")) }()
		go func() { defer wg.Done(); <-start; delErr = c.DeleteIfMatch(ctx, k, etag) }()
		close(start)
		wg.Wait()
		if putErr == nil && delErr == nil {
			both++
		}
	}
	t.Logf("%d rounds of CAS racing a conditional delete on one ETag: both succeeded in %d", rounds, both)
	if both > 0 {
		t.Errorf("a CAS and a delete conditioned on the same ETag both succeeded %d times", both)
	}
}

// N concurrent If-Match CASes via multipart completion on the same ETag: at most one may win.
func TestMultipartCASRace(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	const n, rounds = 6, 3
	part := bytes.Repeat([]byte("c"), 5<<20)
	for r := 0; r < rounds; r++ {
		k := fmt.Sprintf("%s/mpu-cas-%d", p, r)
		etag, _ := c.Put(ctx, k, []byte("v0"))
		ids := make([]*string, n)
		parts := make([][]types.CompletedPart, n)
		for i := range ids {
			cr, err := c.S3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &c.Bucket, Key: &k})
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = cr.UploadId
			for j, b := range [][]byte{part, []byte(fmt.Sprintf("w%d", i))} {
				pn := int32(j + 1)
				u, err := c.S3.UploadPart(ctx, &s3.UploadPartInput{Bucket: &c.Bucket, Key: &k, UploadId: cr.UploadId, PartNumber: &pn, Body: bytes.NewReader(b)})
				if err != nil {
					t.Fatal(err)
				}
				parts[i] = append(parts[i], types.CompletedPart{ETag: u.ETag, PartNumber: aws.Int32(pn)})
			}
		}
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range ids {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, err := c.S3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &c.Bucket, Key: &k,
					UploadId: ids[i], MultipartUpload: &types.CompletedMultipartUpload{Parts: parts[i]}, IfMatch: aws.String(etag)})
				if err == nil {
					wins.Add(1)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		t.Logf("round %d: %d of %d If-Match completions on one ETag won", r, wins.Load(), n)
		if wins.Load() > 1 {
			t.Errorf("round %d: %d If-Match completions won, want at most 1", r, wins.Load())
		}
	}
}

// Conditional reads, for completeness: GET If-None-Match with the current ETag is 304.
func TestConditionalGet(t *testing.T) {
	c, p := client(t)
	ctx := context.Background()
	k := p + "/obj"
	e, _ := c.Put(ctx, k, []byte("x"))
	_, err := c.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: &c.Bucket, Key: &k, IfNoneMatch: aws.String(e)})
	_, status, _ := Classify(err)
	t.Logf("GET If-None-Match current ETag: HTTP %d", status)
	if status != 304 {
		t.Errorf("want 304, got %d (%v)", status, err)
	}
}
