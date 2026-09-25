package compare

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"

	"context"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/auth"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// staticEndpoint resolves every request to {base}/{bucket} (path-style),
// skipping the S3 endpoint rules engine.
type staticEndpoint struct{ base url.URL }

func (r staticEndpoint) ResolveEndpoint(_ context.Context, p s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	u := r.base
	if p.Bucket != nil {
		u.Path = "/" + *p.Bucket
	}
	return smithyendpoints.Endpoint{URI: u}, nil
}

// sigv4Only always picks SigV4 for s3 in one region, skipping auth-scheme
// resolution. The option is built once and shared.
type sigv4Only struct{ opts []*auth.Option }

func newSigv4Only(region string) sigv4Only {
	var props smithy.Properties
	smithyhttp.SetSigV4SigningName(&props, "s3")
	smithyhttp.SetSigV4SigningRegion(&props, region)
	return sigv4Only{[]*auth.Option{{SchemeID: auth.SchemeIDSigV4, SignerProperties: props}}}
}

func (a sigv4Only) ResolveAuthSchemes(context.Context, *s3.AuthResolverParameters) ([]*auth.Option, error) {
	return a.opts, nil
}

// lean applies the replaceable pieces: static endpoint, fixed auth scheme,
// no retryer, checksums only when required, and drops per-request
// middlewares that only add headers or telemetry.
func lean(o *s3.Options) {
	u, _ := url.Parse(*o.BaseEndpoint)
	o.EndpointResolverV2 = staticEndpoint{*u}
	o.AuthSchemeResolver = newSigv4Only(o.Region)
	o.Retryer = aws.NopRetryer{}
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
}

func dropMiddlewares(s *middleware.Stack) error {
	for _, id := range []string{"UserAgent", "RecursionDetection", "ClientRequestID", "S3100Continue"} {
		s.Build.Remove(id)
	}
	return nil
}

func BenchmarkS3PutLean(b *testing.B) {
	for _, tls := range []bool{true, false} {
		scheme := map[bool]string{true: "https", false: "http"}[tls]
		b.Run(scheme+"/sdk-lean", func(b *testing.B) { benchSDK(b, sdkClient(stubS3(b, tls), lean)) })
		b.Run(scheme+"/sdk-lean-dropmw", func(b *testing.B) {
			benchSDK(b, sdkClient(stubS3(b, tls), func(o *s3.Options) {
				lean(o)
				o.APIOptions = append(o.APIOptions, dropMiddlewares)
			}))
		})
	}
}

// TestS3LeanAgainstS3: the lean client's requests are accepted by a real S3
// (signature, endpoint, conditional create). Needs CHDB_TEST_S3 as
// http://host/bucket and CHDB_TEST_S3_KEY/_SECRET.
func TestS3LeanAgainstS3(t *testing.T) {
	s, ok := S3FromEnv()
	if !ok {
		t.Skip("needs CHDB_TEST_S3")
	}
	u, err := url.Parse(s.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	bucket := strings.Trim(u.Path, "/")
	c := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(u.Scheme + "://" + u.Host), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(s.Key, s.Secret, "")}, lean,
		func(o *s3.Options) { o.APIOptions = append(o.APIOptions, dropMiddlewares) })
	ctx := context.Background()
	key := "s3lean/" + strconv.FormatInt(time.Now().UnixNano(), 10)
	body := []byte("lean client body")
	put := func() error {
		_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*")})
		return err
	}
	if err := put(); err != nil {
		t.Fatalf("create: %v", err)
	}
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, _ := io.ReadAll(out.Body)
	if !bytes.Equal(got, body) {
		t.Fatalf("got %q", got)
	}
	if err := put(); err == nil || !strings.Contains(err.Error(), "412") {
		t.Fatalf("second create-only PUT: %v, want 412", err)
	}
}
