package dst

import (
	"bytes"
	"context"
	"errors"
	"github.com/casselc/observability/otel-chdb/parquetgo/internal/s3emu"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// client is the real SDK client against the emulator over the in-memory
// network: the only difference from production is the transport.
func emuClient(t *testing.T, e *s3emu.Emu, maxAttempts int) *s3.Client {
	hc := httptest.NewTestServer(t, e).Client() // any host reaches the server
	return s3.New(s3.Options{
		Region:           "us-east-1",
		BaseEndpoint:     aws.String("http://s3.emu.test"),
		UsePathStyle:     true,
		Credentials:      credentials.NewStaticCredentialsProvider("k", "s", ""),
		HTTPClient:       hc,
		RetryMaxAttempts: maxAttempts,
	})
}

func status(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

func put(ctx context.Context, c *s3.Client, key, body string, ifNone, ifMatch *string) (*s3.PutObjectOutput, error) {
	return c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String(key), Body: bytes.NewReader([]byte(body)),
		ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: ifNone, IfMatch: ifMatch,
		Metadata: map[string]string{"oscope-content": body}})
}

// The conditional-write semantics the edge and the alerts store rely on,
// through the real SDK.
func TestConditionalWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := s3emu.New("b")
		c := emuClient(t, e, 1)
		ctx := t.Context()
		out, err := put(ctx, c, "a/1", "one", aws.String("*"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := put(ctx, c, "a/1", "two", aws.String("*"), nil); status(err) != 412 {
			t.Fatalf("second create: %v", err)
		}
		if _, err := put(ctx, c, "a/1", "two", nil, aws.String(`"nope"`)); status(err) != 412 {
			t.Fatalf("stale If-Match: %v", err)
		}
		if _, err := put(ctx, c, "a/2", "x", nil, out.ETag); status(err) != 404 {
			t.Fatalf("If-Match on a missing key: %v", err)
		}
		if _, err := put(ctx, c, "a/1", "two", nil, out.ETag); err != nil {
			t.Fatal(err)
		}
		h, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("b"), Key: aws.String("a/1")})
		if err != nil || h.Metadata["oscope-content"] != "two" {
			t.Fatalf("head: %v %v", h, err)
		}
		g, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("a/1")})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(g.Body)
		_ = g.Body.Close()
		if string(b) != "two" {
			t.Fatalf("body %q", b)
		}
		if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("b"), Key: aws.String("none")}); status(err) != 404 {
			t.Fatalf("head missing: %v", err)
		}
		for _, k := range []string{"a/3", "b/1", "a/x/1", "a/x/2"} {
			if _, err := put(ctx, c, k, k, aws.String("*"), nil); err != nil {
				t.Fatal(err)
			}
		}
		var keys []string
		p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String("b"), Prefix: aws.String("a/"), MaxKeys: aws.Int32(1)})
		for p.HasMorePages() {
			pg, err := p.NextPage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range pg.Contents {
				keys = append(keys, *o.Key)
			}
		}
		if want := "a/1 a/3 a/x/1 a/x/2"; joinKeys(keys) != want {
			t.Fatalf("list %q, want %q", joinKeys(keys), want)
		}
		l, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("b"), Prefix: aws.String("a/"), Delimiter: aws.String("/")})
		if err != nil || len(l.CommonPrefixes) != 1 || *l.CommonPrefixes[0].Prefix != "a/x/" || len(l.Contents) != 2 {
			t.Fatalf("delimited list: %+v %v", l, err)
		}
	})
}

func joinKeys(ks []string) string {
	var b bytes.Buffer
	for i, k := range ks {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(k)
	}
	return b.String()
}

// Each fault, as the SDK sees it, and what the store holds afterwards.
func TestFaults(t *testing.T) {
	for _, c := range []struct {
		fault   s3emu.Fault
		applied bool // after Wait
		answer  func(err error) bool
	}{
		{s3emu.Refuse, false, func(err error) bool { return status(err) == 503 }},
		{s3emu.ErrorAfter, true, func(err error) bool { return status(err) == 500 }},
		{s3emu.DropAfter, true, func(err error) bool { return err != nil && status(err) == 0 }},
		{s3emu.DropBefore, false, func(err error) bool { return err != nil && status(err) == 0 }},
		{s3emu.Late, true, func(err error) bool { return err != nil && status(err) == 0 }},
		{s3emu.Slow, true, func(err error) bool { return errors.Is(err, context.DeadlineExceeded) }},
	} {
		t.Run(c.fault.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := s3emu.New("b")
				first := true
				var late []s3emu.Change
				e.OnChange = func(ch s3emu.Change) {
					if ch.Late {
						late = append(late, ch)
					}
				}
				e.Faults = func(op, key string) s3emu.Decision {
					if op == "PUT-CREATE" && first {
						first = false
						return s3emu.Decision{Fault: c.fault, After: time.Minute}
					}
					return s3emu.Decision{}
				}
				cl := emuClient(t, e, 1)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				start := time.Now()
				_, err := put(ctx, cl, "k", "v", aws.String("*"), nil)
				if !c.answer(err) {
					t.Fatalf("answer: %v", err)
				}
				if c.fault == s3emu.Slow && time.Since(start) != 10*time.Second {
					t.Fatalf("timed out after %v of fake time", time.Since(start))
				}
				e.Wait()
				synctest.Wait()
				if _, ok := e.Get("k"); ok != c.applied {
					t.Fatalf("applied %v", ok)
				}
				if (c.fault == s3emu.Late || c.fault == s3emu.Slow) != (len(late) == 1) {
					t.Fatalf("late changes %v", late)
				}
				// the client's retry meets its own write
				if _, err := put(t.Context(), cl, "k", "v", aws.String("*"), nil); c.applied != (status(err) == 412) {
					t.Fatalf("retry: %v", err)
				}
			})
		})
	}
}

// A late copy is conditioned when it lands: it loses to a write that
// landed first (create-only), and wins over nothing.
func TestLateCopyLosesToAnEarlierWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := s3emu.New("b")
		n := 0
		e.Faults = func(op, key string) s3emu.Decision {
			n++
			if n == 1 {
				return s3emu.Decision{Fault: s3emu.Late, After: time.Minute}
			}
			return s3emu.Decision{}
		}
		c := emuClient(t, e, 1)
		if _, err := put(t.Context(), c, "k", "late", aws.String("*"), nil); err == nil {
			t.Fatal("no error")
		}
		if _, err := put(t.Context(), c, "k", "first", aws.String("*"), nil); err != nil {
			t.Fatal(err)
		}
		e.Wait()
		if o, _ := e.Get("k"); string(o.Body) != "first" {
			t.Fatalf("late copy overwrote: %q", o.Body)
		}
		e.Mutation = s3emu.IgnoreIfNoneMatch
		n = 0
		_, _ = put(t.Context(), c, "k2", "late", aws.String("*"), nil)
		_, _ = put(t.Context(), c, "k2", "first", aws.String("*"), nil)
		e.Wait()
		if o, _ := e.Get("k2"); string(o.Body) != "late" {
			t.Fatalf("mutant: %q", o.Body)
		}
	})
}

// The SDK's default retryer (as the edge configures it: RetryMaxAttempts
// unset) retries a 503 and a 500, and the retry of a write that applied
// meets it: 412.
func TestSDKRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := s3emu.New("b")
		n := 0
		e.Faults = func(op, key string) s3emu.Decision {
			n++
			switch n {
			case 1:
				return s3emu.Decision{Fault: s3emu.Refuse}
			case 2:
				return s3emu.Decision{Fault: s3emu.ErrorAfter}
			}
			return s3emu.Decision{}
		}
		c := emuClient(t, e, 0)
		_, err := put(t.Context(), c, "k", "v", aws.String("*"), nil)
		if status(err) != 412 || e.Count("PUT-CREATE") != 3 {
			t.Fatalf("%v after %d PUTs", err, e.Count("PUT-CREATE"))
		}
		var re *smithyhttp.ResponseError
		_ = errors.As(err, &re)
		if re.Response.StatusCode != http.StatusPreconditionFailed {
			t.Fatal(re.Response.Status)
		}
	})
}
