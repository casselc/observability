package compare

// Cost of one S3 PUT, client side only: a stub server that discards the
// body, so the numbers are signing, checksums, the SDK's middleware stack
// and allocations, not network or storage. Run:
//
//	go test -run x -bench S3Put -benchtime 300x -count 3 .
//	S3PUT_BODY=1200 go test -run x -bench S3Put ...   # manifest-sized
//
// Results: results/s3client.md.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/minio/minio-go/v7"
	mcreds "github.com/minio/minio-go/v7/pkg/credentials"
)

func putBody() []byte {
	n := 1 << 20
	if v := os.Getenv("S3PUT_BODY"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	return bytes.Repeat([]byte("x"), n)
}

func stubS3(b *testing.B, tls bool) *httptest.Server {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"abc"`)
	})
	s := httptest.NewUnstartedServer(h)
	if tls {
		s.StartTLS()
	} else {
		s.Start()
	}
	b.Cleanup(s.Close)
	return s
}

func sdkClient(s *httptest.Server, mut func(*s3.Options)) *s3.Client {
	o := s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(s.URL), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""),
		HTTPClient:  s.Client()}
	if mut != nil {
		mut(&o)
	}
	return s3.New(o)
}

func benchSDK(b *testing.B, c *s3.Client, opt ...func(*s3.Options)) {
	body := putBody()
	ctx := context.Background()
	in := &s3.PutObjectInput{Bucket: aws.String("bkt"), Key: aws.String("k/x.parquet"),
		ContentType: aws.String("application/vnd.apache.parquet")}
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		in.Body = bytes.NewReader(body)
		in.ContentLength = aws.Int64(int64(len(body)))
		if _, err := c.PutObject(ctx, in, opt...); err != nil {
			b.Fatal(err)
		}
	}
}

func benchMinio(b *testing.B, tls bool) {
	s := stubS3(b, tls)
	c, err := minio.New(strings.TrimPrefix(strings.TrimPrefix(s.URL, "https://"), "http://"), &minio.Options{
		Creds: mcreds.NewStaticV4("k", "s", ""), Region: "us-east-1", Secure: tls,
		BucketLookup: minio.BucketLookupPath, Transport: s.Client().Transport})
	if err != nil {
		b.Fatal(err)
	}
	body := putBody()
	ctx := context.Background()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		if _, err := c.PutObject(ctx, "bkt", "k/x.parquet", bytes.NewReader(body), int64(len(body)),
			minio.PutObjectOptions{ContentType: "application/vnd.apache.parquet"}); err != nil {
			b.Fatal(err)
		}
	}
}

// benchRaw: the SDK's SigV4 signer on a plain net/http request, no
// operation middleware stack, UNSIGNED-PAYLOAD. The allocation floor.
func benchRaw(b *testing.B, tls bool) {
	s := stubS3(b, tls)
	signer := v4.NewSigner()
	creds := aws.Credentials{AccessKeyID: "k", SecretAccessKey: "s"}
	hc := s.Client()
	body := putBody()
	ctx := context.Background()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, s.URL+"/bkt/k/x.parquet", bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		req.Header.Set("Content-Type", "application/vnd.apache.parquet")
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		if err := signer.SignHTTP(ctx, creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now()); err != nil {
			b.Fatal(err)
		}
		resp, err := hc.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			b.Fatal(resp.Status)
		}
	}
}

func BenchmarkS3Put(b *testing.B) {
	for _, tls := range []bool{true, false} {
		scheme := map[bool]string{true: "https", false: "http"}[tls]
		b.Run(scheme+"/minio", func(b *testing.B) { benchMinio(b, tls) })
		b.Run(scheme+"/sdk-default", func(b *testing.B) { benchSDK(b, sdkClient(stubS3(b, tls), nil)) })
		b.Run(scheme+"/sdk-unsigned", func(b *testing.B) {
			benchSDK(b, sdkClient(stubS3(b, tls), nil), s3.WithAPIOptions(v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware))
		})
		b.Run(scheme+"/sdk-noretry-nochecksum", func(b *testing.B) {
			benchSDK(b, sdkClient(stubS3(b, tls), func(o *s3.Options) {
				o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
				o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
				o.Retryer = aws.NopRetryer{}
			}))
		})
		b.Run(scheme+"/raw-sigv4", func(b *testing.B) { benchRaw(b, tls) })
	}
}
