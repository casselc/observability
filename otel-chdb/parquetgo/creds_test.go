package parquetgo

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The credential-path tests publish a real batch through each way the
// deployments get credentials, against the SeaweedFS in CHDB_TEST_S3
// (http://host:port/bucket; key and secret in CHDB_TEST_S3_KEY/SECRET),
// with local stubs standing in for the AWS side: the EKS Pod Identity
// agent, STS, and Roles Anywhere's aws_signing_helper. Each stub hands out
// the store's own key and secret, so SeaweedFS accepts the signature: what
// is under test is that the SDK found, used and refreshed them. Objects go
// under {bucket}/creds/... and are removed afterwards unless
// CREDS_TEST_KEEP=1.
//
// Temporary AWS credentials always carry a session token, but SeaweedFS
// answers InvalidAccessKeyId to any request with X-Amz-Security-Token that
// its own STS did not issue, and the header is signed, so a proxy cannot
// drop it. So each stub hands out an EMPTY token for the SeaweedFS run
// (e.token == ""), and each test repeats the flow with a token against a
// local fake S3 (e.sessionTokens) to show the token reaches the request.

type credsEnv struct {
	base, bucket, key, secret string
	dir                       string
	token                     string // session token the stubs hand out
}

func newCredsEnv(t *testing.T) *credsEnv {
	base, key, secret := os.Getenv("CHDB_TEST_S3"), os.Getenv("CHDB_TEST_S3_KEY"), os.Getenv("CHDB_TEST_S3_SECRET")
	if base == "" || key == "" {
		t.Skip("CHDB_TEST_S3 / CHDB_TEST_S3_KEY / CHDB_TEST_S3_SECRET not set")
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	e := &credsEnv{base: strings.TrimRight(base, "/"), bucket: strings.Trim(u.Path, "/"), key: key, secret: secret, dir: t.TempDir()}
	// A clean slate: nothing from the developer's machine may satisfy the
	// chain before the provider under test does.
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_DEFAULT_PROFILE", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_SESSION_NAME",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_CA_BUNDLE",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_STS"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(e.dir, "no-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(e.dir, "no-credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("RA_SESSION_TOKEN", "")
	return e
}

func (e *credsEnv) write(t *testing.T, name, content string, mode os.FileMode) string {
	p := filepath.Join(e.dir, name)
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	return p
}

// direct is a static-key client straight to the store, for reading back.
func (e *credsEnv) direct() *s3.Client {
	u, _ := url.Parse(e.base)
	return s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(u.Scheme + "://" + u.Host),
		UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(e.key, e.secret, "")})
}

func credsTraces(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "creds")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		s := ss.Spans().AppendEmpty()
		s.SetName(fmt.Sprintf("op-%d", i%5))
		s.SetTraceID(pcommon.TraceID{1, byte(i)})
		s.SetSpanID(pcommon.SpanID{2, byte(i)})
		s.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(i)))
		s.SetEndTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(i) + 1000))
		s.Attributes().PutInt("i", int64(i))
	}
	return td
}

// publish pushes `batches` batches of 37 spans through cfg (URL defaulting
// to {base}/creds/{test}), then reads each Parquet object back from the store
// and checks its row count. It returns the object keys.
func (e *credsEnv) publish(t *testing.T, cfg Config, batches int) []string {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "-")
	if cfg.URL == "" {
		cfg.URL = e.base + "/creds/" + name
	}
	cfg.ProducerID, cfg.Region, cfg.SchemaVersion, cfg.Engine = "p", "r", 1, "parquet-go"
	cfg.Epoch = fmt.Sprintf("e%d", time.Now().UnixNano())
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	cfg.Now = func() time.Time { return now }
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	u, _ := url.Parse(cfg.URL)
	_, prefix, _ := strings.Cut(strings.Trim(u.Path, "/"), "/")
	t.Cleanup(func() { e.cleanup(t, prefix) })
	var keys []string
	for b := 1; b <= batches; b++ {
		if err := p.PushTraces(ctx, credsTraces(37)); err != nil {
			t.Fatalf("batch %d: %v", b, err)
		}
		keys = append(keys, fmt.Sprintf("%s/r/traces/v1/p/%s/g20260925T120000/%020d.parquet", prefix, cfg.Epoch, b))
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		out, err := e.direct().GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(k)})
		if err != nil {
			t.Fatalf("read back %s: %v", k, err)
		}
		b, _ := io.ReadAll(out.Body)
		out.Body.Close()
		f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
		if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		if f.NumRows() != 37 {
			t.Fatalf("%s: %d rows, want 37", k, f.NumRows())
		}
	}
	return keys
}

// sessionTokens publishes one batch through cfg to a local fake S3 with
// e.token set, and returns the X-Amz-Security-Token of each request.
func (e *credsEnv) sessionTokens(t *testing.T, cfg Config, token string) []string {
	t.Helper()
	e.token = token
	defer func() { e.token = "" }()
	t.Setenv("RA_SESSION_TOKEN", token) // for the credential_process script
	var mu sync.Mutex
	var seen []string
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		seen = append(seen, r.Header.Get("X-Amz-Security-Token"))
		mu.Unlock()
		w.Header().Set("ETag", `"x"`)
	}))
	defer fake.Close()
	cfg.URL = fake.URL + "/bkt/pre"
	cfg.ProducerID, cfg.Region, cfg.SchemaVersion, cfg.Engine = "p", "r", 1, "parquet-go"
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PushTraces(context.Background(), credsTraces(3)); err != nil {
		t.Fatal(err)
	}
	for _, s := range seen {
		if s != token {
			t.Fatalf("X-Amz-Security-Token %q, want %q", s, token)
		}
	}
	return seen
}

func (e *credsEnv) cleanup(t *testing.T, prefix string) {
	if os.Getenv("CREDS_TEST_KEEP") == "1" {
		t.Logf("kept s3://%s/%s", e.bucket, prefix)
		return
	}
	ctx := context.Background()
	c := e.direct()
	pg := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(e.bucket), Prefix: aws.String(prefix + "/")})
	for pg.HasMorePages() {
		out, err := pg.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup list: %v", err)
			return
		}
		for _, o := range out.Contents {
			_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(e.bucket), Key: o.Key})
		}
	}
}

// TestCredsPodIdentity: EKS Pod Identity. The SDK GETs
// AWS_CONTAINER_CREDENTIALS_FULL_URI with the token file's content as the
// Authorization header, caches the result, and refetches before Expiration
// (the container provider refreshes 5 minutes early).
func TestCredsPodIdentity(t *testing.T) {
	e := newCredsEnv(t)
	for _, c := range []struct {
		name    string
		ttl     time.Duration
		refetch bool
	}{
		{"cached", time.Hour, false},
		{"refresh", time.Minute, true}, // inside the 5 min window: stale on every use
	} {
		t.Run(c.name, func(t *testing.T) {
			var hits atomic.Int32
			var mu sync.Mutex
			var auths []string
			agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				mu.Lock()
				auths = append(auths, r.Header.Get("Authorization"))
				mu.Unlock()
				json.NewEncoder(w).Encode(map[string]string{
					"AccessKeyId": e.key, "SecretAccessKey": e.secret, "Token": e.token,
					"AccountId": "123456789012", "Expiration": time.Now().Add(c.ttl).UTC().Format(time.RFC3339),
				})
			}))
			defer agent.Close()
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", agent.URL+"/v1/credentials")
			t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", e.write(t, "eks-pod-identity-token", "pod-token-abc", 0o600))

			e.publish(t, Config{}, 1)
			first := hits.Load()
			if first < 1 {
				t.Fatal("credentials endpoint never called")
			}
			e.publish(t, Config{}, 1)
			second := hits.Load()
			mu.Lock()
			for _, a := range auths {
				if a != "pod-token-abc" {
					t.Errorf("Authorization = %q, want the token file's content", a)
				}
			}
			mu.Unlock()
			// Each publish is a new Publisher (its own cache): so the
			// cached case is 1 fetch per publisher, 2 PUTs + seal each.
			t.Logf("endpoint hits: %d after first publisher, %d after second", first, second)
			if c.refetch && first < 2 {
				t.Errorf("short expiry: %d fetches for 3 requests, want a refetch per request", first)
			}
			if !c.refetch && first != 1 {
				t.Errorf("1h expiry: %d fetches, want 1 (cached)", first)
			}
			t.Logf("with a session token: %d requests carried it", len(e.sessionTokens(t, Config{}, "pod-session-token")))
		})
	}
}

// stsStub answers AssumeRoleWithWebIdentity and AssumeRole with the store's
// key and secret, recording each request's form.
type stsStub struct {
	*httptest.Server
	mu    sync.Mutex
	forms []url.Values
	key   string
	sec   string
	token *string
}

func newSTSStub(e *credsEnv) *stsStub {
	s := &stsStub{key: e.key, sec: e.secret, token: &e.token}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		s.mu.Lock()
		s.forms = append(s.forms, r.PostForm)
		s.mu.Unlock()
		action := r.PostForm.Get("Action")
		exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprintf(w, `<%[1]sResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <%[1]sResult>
    <Credentials>
      <AccessKeyId>%[2]s</AccessKeyId>
      <SecretAccessKey>%[3]s</SecretAccessKey>
      <SessionToken>%[5]s</SessionToken>
      <Expiration>%[4]s</Expiration>
    </Credentials>
    <AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/r/s</Arn><AssumedRoleId>AROA:s</AssumedRoleId></AssumedRoleUser>
  </%[1]sResult>
  <ResponseMetadata><RequestId>stub</RequestId></ResponseMetadata>
</%[1]sResponse>`, action, s.key, s.sec, exp, *s.token)
	}))
	return s
}

// TestCredsIRSA: EKS IAM Roles for Service Accounts. The SDK reads the
// projected token file and calls STS AssumeRoleWithWebIdentity (here a stub
// reached through AWS_ENDPOINT_URL_STS) with it and AWS_ROLE_ARN.
func TestCredsIRSA(t *testing.T) {
	e := newCredsEnv(t)
	sts := newSTSStub(e)
	defer sts.Close()
	const role = "arn:aws:iam::123456789012:role/otel-publisher"
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	t.Setenv("AWS_ROLE_ARN", role)
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", e.write(t, "irsa-token", "eyJhbGciOi.irsa.jwt", 0o600))
	e.publish(t, Config{}, 2)
	if len(sts.forms) != 1 {
		t.Fatalf("%d STS calls, want 1 (cached for both batches)", len(sts.forms))
	}
	f := sts.forms[0]
	if f.Get("Action") != "AssumeRoleWithWebIdentity" || f.Get("RoleArn") != role || f.Get("WebIdentityToken") != "eyJhbGciOi.irsa.jwt" {
		t.Fatalf("STS request: %v", f)
	}
	e.sessionTokens(t, Config{}, "irsa-session-token")
}

// TestCredsRoleARN: Config.RoleARN assumes a role on top of whatever the
// chain found (here static env keys that the store would reject).
func TestCredsRoleARN(t *testing.T) {
	e := newCredsEnv(t)
	sts := newSTSStub(e)
	defer sts.Close()
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIABASE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "base-secret")
	const role = "arn:aws:iam::123456789012:role/cross-account"
	e.publish(t, Config{RoleARN: role}, 1)
	if len(sts.forms) != 1 || sts.forms[0].Get("Action") != "AssumeRole" || sts.forms[0].Get("RoleArn") != role {
		t.Fatalf("STS requests: %v", sts.forms)
	}
	e.sessionTokens(t, Config{RoleARN: role}, "assumed-session-token")
}

// TestCredsRolesAnywhere: IAM Roles Anywhere outside AWS. A shared-config
// profile runs `credential_process = aws_signing_helper credential-process
// ...`; here a shell script under that name prints the same JSON shape.
// The real helper signs CreateSession with the X.509 key; the SDK side,
// which is what this exercises, is identical.
func TestCredsRolesAnywhere(t *testing.T) {
	e := newCredsEnv(t)
	calls := filepath.Join(e.dir, "calls")
	helper := e.write(t, "aws_signing_helper", fmt.Sprintf(`#!/bin/sh
echo "$@" >> %s
cat <<EOF
{"Version": 1, "AccessKeyId": "%s", "SecretAccessKey": "%s", "SessionToken": "$RA_SESSION_TOKEN",
 "Expiration": "$(date -u -d '+1 hour' +%%Y-%%m-%%dT%%H:%%M:%%SZ)"}
EOF
`, calls, e.key, e.secret), 0o755)
	cfgFile := e.write(t, "config", fmt.Sprintf(`[profile rolesanywhere]
region = us-east-1
credential_process = %s credential-process --certificate /etc/ra/cert.pem --private-key /etc/ra/key.pem --trust-anchor-arn arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/ta --profile-arn arn:aws:rolesanywhere:us-east-1:123456789012:profile/pr --role-arn arn:aws:iam::123456789012:role/otel-publisher
`, helper), 0o600)
	t.Setenv("AWS_CONFIG_FILE", cfgFile)
	t.Run("AWS_PROFILE", func(t *testing.T) {
		t.Setenv("AWS_PROFILE", "rolesanywhere")
		e.publish(t, Config{}, 2)
	})
	t.Run("Config.Profile", func(t *testing.T) {
		e.publish(t, Config{Profile: "rolesanywhere"}, 1)
	})
	b, _ := os.ReadFile(calls)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "credential-process --certificate") {
		t.Fatalf("helper calls (want one per publisher): %q", lines)
	}
	e.sessionTokens(t, Config{Profile: "rolesanywhere"}, "ra-session-token")
}

// TestCredsRolesAnywhereServe: the other way to run IAM Roles Anywhere,
// `aws_signing_helper serve`, which emulates IMDSv2 on localhost (default
// :9911) and is found through AWS_EC2_METADATA_SERVICE_ENDPOINT. It is the
// mode that also works for chDB and ClickHouse, which have no
// credential_process provider. The stub below answers IMDSv2's three calls.
func TestCredsRolesAnywhereServe(t *testing.T) {
	e := newCredsEnv(t)
	var mu sync.Mutex
	var calls []string
	imds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.Method+" "+r.URL.Path+" token="+r.Header.Get("X-Aws-Ec2-Metadata-Token"))
		mu.Unlock()
		switch strings.TrimSuffix(r.URL.Path, "/") {
		case "/latest/api/token":
			w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
			fmt.Fprint(w, "imdsv2-token")
		case "/latest/meta-data/iam/security-credentials":
			fmt.Fprint(w, "otel-publisher")
		case "/latest/meta-data/iam/security-credentials/otel-publisher":
			now := time.Now().UTC()
			json.NewEncoder(w).Encode(map[string]string{"Code": "Success", "Type": "AWS-HMAC",
				"AccessKeyId": e.key, "SecretAccessKey": e.secret, "Token": e.token,
				"LastUpdated": now.Format(time.RFC3339), "Expiration": now.Add(time.Hour).Format(time.RFC3339)})
		default:
			http.NotFound(w, r)
		}
	}))
	defer imds.Close()
	t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", imds.URL)
	e.publish(t, Config{}, 2)
	mu.Lock()
	if len(calls) < 2 || !strings.HasPrefix(calls[0], "PUT /latest/api/token") || !strings.HasSuffix(calls[len(calls)-1], "token=imdsv2-token") {
		t.Fatalf("IMDS calls: %q", calls)
	}
	mu.Unlock()
	e.sessionTokens(t, Config{}, "serve-session-token")
}

// TestCredsPrivateCA: Nutanix Objects style. Static keys over https to a
// custom endpoint whose certificate chains to a private CA. A TLS-terminating
// reverse proxy stands in for the store; it forwards the client's Host
// header unchanged (httputil.ReverseProxy keeps req.Host), which is what
// SigV4 signed, so SeaweedFS verifies the signature against the same Host.
func TestCredsPrivateCA(t *testing.T) {
	e := newCredsEnv(t)
	up, _ := url.Parse(e.base)
	up.Path = ""
	proxy := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(up))
	defer proxy.Close()
	ca := e.write(t, "private-ca.pem", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: proxy.Certificate().Raw})), 0o644)
	static := Config{URL: proxy.URL + "/" + e.bucket + "/creds/" + t.Name(), AccessKeyID: e.key, SecretAccessKey: e.secret}

	t.Run("no CA: rejected", func(t *testing.T) {
		p, err := New(static)
		if err != nil {
			t.Fatal(err)
		}
		err = p.put(context.Background(), "x", []byte("x"), "text/plain")
		if err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("want a certificate error, got %v", err)
		}
		t.Logf("without CA: %v", err)
	})
	t.Run("CABundle", func(t *testing.T) {
		var n atomic.Int32
		c := static
		c.URL += "-cabundle"
		c.CABundle = ca
		c.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(r *http.Request) (*http.Response, error) { n.Add(1); return rt.RoundTrip(r) })
		}
		e.publish(t, c, 2)
		if n.Load() != 5 { // 2 × (object + manifest) + seal
			t.Errorf("%d requests through WrapTransport, want 5", n.Load())
		}
	})
	t.Run("AWS_CA_BUNDLE", func(t *testing.T) {
		t.Setenv("AWS_CA_BUNDLE", ca)
		c := static
		c.URL += "-env"
		e.publish(t, c, 1)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestS3URLAddressing: s3:// goes to the SDK's AWS endpoint, virtual-hosted,
// with the chain's region; http(s):// is a path-style custom endpoint; and
// PathStyle overrides either. No network: WrapTransport answers.
func TestS3URLAddressing(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDENV")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	yes, no := true, false
	for _, c := range []struct {
		url, region string
		pathStyle   *bool
		want        string // scheme://host/path of the PUT
	}{
		{"s3://my-bucket/otel/edge", "eu-west-1", nil, "https://my-bucket.s3.eu-west-1.amazonaws.com/otel/edge/k"},
		{"s3://my-bucket", "us-east-2", &yes, "https://s3.us-east-2.amazonaws.com/my-bucket/k"},
		{"https://objects.nutanix.local:9440/bkt/pre", "", nil, "https://objects.nutanix.local:9440/bkt/pre/k"},
		{"https://s3.example.com/bkt/pre", "", &no, "https://bkt.s3.example.com/pre/k"},
	} {
		t.Setenv("AWS_REGION", c.region)
		var got string
		p, err := New(Config{URL: c.url, PathStyle: c.pathStyle, Engine: "parquet-go",
			WrapTransport: func(http.RoundTripper) http.RoundTripper {
				return roundTripFunc(func(r *http.Request) (*http.Response, error) {
					io.Copy(io.Discard, r.Body)
					got = r.URL.Scheme + "://" + r.URL.Host + r.URL.Path
					return &http.Response{StatusCode: 200, Header: http.Header{"Etag": {`"x"`}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
				})
			}})
		if err != nil {
			t.Fatalf("%s: %v", c.url, err)
		}
		if err := p.put(context.Background(), "k", []byte("x"), "text/plain"); err != nil {
			t.Fatalf("%s: %v", c.url, err)
		}
		if got != c.want {
			t.Errorf("%s: PUT %s, want %s", c.url, got, c.want)
		}
	}
	t.Setenv("AWS_REGION", "")
	if _, err := New(Config{URL: "s3://my-bucket/x", Engine: "parquet-go"}); err == nil || !strings.Contains(err.Error(), "region") {
		t.Errorf("s3:// without a region: %v", err)
	}
}
