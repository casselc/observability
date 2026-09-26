package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Opts are the connection settings. They mirror the exporters' settings
// (parquetgo.Config, otap-rs `s3:`): a URL or bucket+prefix, an optional
// endpoint (then path-style by default), region, profile, CA bundle, static
// keys as an override, and a role to assume on top.
type Opts struct {
	URL          string
	Bucket       string
	Prefix       string
	Endpoint     string
	Region       string
	Profile      string
	CABundle     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	RoleARN      string
	STSEndpoint  string
	PathStyle    string // auto | true | false
	Store        string // aws | nutanix | seaweedfs | other; only changes the advice
	Timeout      time.Duration
	// RaceEndpoints are extra endpoints (same store) for the race checks.
	RaceEndpoints []string
}

// resolve fills Bucket/Prefix/Endpoint from URL (s3://b/p or https://host/b/p),
// and Store from the endpoint when not given.
func (o *Opts) resolve() error {
	if o.URL != "" {
		u, err := url.Parse(o.URL)
		if err != nil {
			return fmt.Errorf("--url: %w", err)
		}
		switch u.Scheme {
		case "s3":
			o.Bucket = u.Host
			o.Prefix = strings.Trim(u.Path, "/")
		case "http", "https":
			o.Endpoint = u.Scheme + "://" + u.Host
			p := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
			o.Bucket = p[0]
			if len(p) == 2 {
				o.Prefix = p[1]
			}
		default:
			return fmt.Errorf("--url: scheme %q (want s3, http or https)", u.Scheme)
		}
	}
	if o.Bucket == "" {
		return errors.New("no bucket: pass --url or --bucket")
	}
	o.Prefix = strings.Trim(o.Prefix, "/")
	if o.Prefix == "" {
		o.Prefix = "otel-accept"
	}
	if o.Store == "" {
		switch {
		case o.Endpoint == "" && os.Getenv("AWS_ENDPOINT_URL_S3") == "":
			o.Store = "aws"
		case strings.Contains(o.Endpoint, "127.0.0.1") || strings.Contains(o.Endpoint, "localhost"):
			o.Store = "local"
		default:
			o.Store = "other"
		}
	}
	if o.Timeout == 0 {
		o.Timeout = 30 * time.Second
	}
	return nil
}

func (o *Opts) pathStyle() bool {
	switch o.PathStyle {
	case "true":
		return true
	case "false":
		return false
	}
	return o.Endpoint != ""
}

// Recorder wraps the HTTP transport: it counts requests by method and status,
// tracks the server clock (the Date header) against ours, and can capture
// the headers of the next matching request.
type Recorder struct {
	base http.RoundTripper

	mu       sync.Mutex
	counts   map[string]int
	maxSkew  time.Duration
	skewSeen bool
	capture  func(*http.Request) bool
	captured http.Header

	noChecksum atomic.Int64 // GET responses without a checksum the SDK could validate
}

func (r *Recorder) RoundTrip(req *http.Request) (*http.Response, error) { return r.via(r.base, req) }

// via records a request sent through base (a racer's own transport).
func (r *Recorder) via(base http.RoundTripper, req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	if r.capture != nil && r.capture(req) {
		r.captured = req.Header.Clone()
		r.capture = nil
	}
	r.mu.Unlock()
	t0 := time.Now()
	resp, err := base.RoundTrip(req)
	key := req.Method + " "
	if err != nil {
		key += "error"
	} else {
		key += fmt.Sprint(resp.StatusCode)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = map[string]int{}
	}
	r.counts[key]++
	if err == nil {
		if d, perr := http.ParseTime(resp.Header.Get("Date")); perr == nil {
			// Date has 1 s resolution; compare against the request's midpoint.
			mid := t0.Add(time.Since(t0) / 2)
			skew := d.Sub(mid)
			if skew < 0 {
				skew = -skew
			}
			if skew > r.maxSkew {
				r.maxSkew = skew
			}
			r.skewSeen = true
		}
	}
	return resp, err
}

// Capture records the headers of the next request for which match is true.
func (r *Recorder) Capture(match func(*http.Request) bool) {
	r.mu.Lock()
	r.capture, r.captured = match, nil
	r.mu.Unlock()
}

func (r *Recorder) Captured() http.Header {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.captured
}

func (r *Recorder) Counts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for k, v := range r.counts {
		out[k] = v
	}
	return out
}

// transport builds the base transport, trusting the CA bundle (appended to
// the system roots) when one is given, as the exporters do.
func transport(o *Opts) (*http.Transport, []byte, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 256
	tr.MaxIdleConnsPerHost = 128
	ca := o.CABundle
	if ca == "" {
		ca = os.Getenv("AWS_CA_BUNDLE")
	}
	if ca == "" {
		return tr, nil, nil
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, nil, fmt.Errorf("CA bundle: %w", err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, nil, fmt.Errorf("CA bundle %s: no PEM certificates", ca)
	}
	tr.TLSClientConfig = &tls.Config{RootCAs: roots}
	return tr, pem, nil
}

// Env is what a check runs against.
type Env struct {
	O       *Opts
	AWS     aws.Config
	S3      *s3.Client
	Rec     *Recorder
	Base    http.RoundTripper
	Run     string // run id
	Root    string // {prefix}/{run}
	Verbose bool
	Params  Params
	// Racers are the clients the race checks spread their writers over: the
	// main client, plus one per --race-endpoints entry (for example each
	// client-facing IP of a Nutanix object store, so that concurrent writers
	// land on different gateways).
	Racers []*s3.Client
}

// racer returns the client for writer i.
func (e *Env) racer(i int) *s3.Client {
	if len(e.Racers) == 0 {
		return e.S3
	}
	return e.Racers[i%len(e.Racers)]
}

// Key names an object of this run.
func (e *Env) Key(parts ...string) string { return e.Root + "/" + strings.Join(parts, "/") }

// newEnv loads the AWS config (the default chain unless static keys are
// given), wraps the transport with the Recorder, and builds the S3 client
// with SDK retries off: a retry would hide exactly the outcomes probed here.
func newEnv(ctx context.Context, o *Opts) (*Env, error) {
	tr, pem, err := transport(o)
	if err != nil {
		return nil, err
	}
	rec := &Recorder{base: tr}
	cfg, err := loadAWS(ctx, o, rec, pem)
	if err != nil {
		return nil, err
	}
	e := &Env{O: o, AWS: cfg, Rec: rec, Base: tr}
	e.S3 = e.client(nil)
	e.Racers = []*s3.Client{e.S3}
	for _, spec := range o.RaceEndpoints {
		ep, ip, _ := strings.Cut(spec, "=")
		var hc *http.Client
		if ip != "" {
			// URL=IP: connect to IP but keep the URL's host for TLS (SNI,
			// certificate check) and SigV4, like curl --resolve. This reaches
			// one gateway of a store behind round-robin DNS: Go dials the
			// first address that answers, so DNS alone would not spread writers.
			tr2 := tr.Clone()
			var d net.Dialer
			tr2.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				_, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				return d.DialContext(ctx, network, net.JoinHostPort(ip, port))
			}
			hc = &http.Client{Transport: rtFunc(func(q *http.Request) (*http.Response, error) { return rec.via(tr2, q) })}
		}
		e.Racers = append(e.Racers, e.client(func(so *s3.Options) {
			so.BaseEndpoint = aws.String(ep)
			if hc != nil {
				so.HTTPClient = hc
			}
		}))
	}
	return e, nil
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(q *http.Request) (*http.Response, error) { return f(q) }

func loadAWS(ctx context.Context, o *Opts, rt *Recorder, pem []byte) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRetryMaxAttempts(1),
		config.WithHTTPClient(&http.Client{Transport: rt}),
		// The SDK logs "Response has no supported checksum" for every GET of an
		// object stored without one (most S3-compatible stores): count it
		// instead, and pass everything else through.
		config.WithLogger(logging.LoggerFunc(func(c logging.Classification, f string, v ...any) {
			msg := fmt.Sprintf(f, v...)
			if strings.Contains(msg, "no supported checksum") {
				rt.noChecksum.Add(1)
				return
			}
			fmt.Fprintf(os.Stderr, "SDK %s %s\n", c, msg)
		})),
	}
	if o.Region != "" {
		opts = append(opts, config.WithRegion(o.Region))
	}
	if o.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(o.Profile))
	}
	// The CA bundle (flag or AWS_CA_BUNDLE) is already in rt's TLS roots, and
	// every client built from this config (S3, STS, SSO) shares rt. The SDK's
	// own AWS_CA_BUNDLE handling needs a client it can rebuild, so hide the
	// variable from it while loading.
	_ = pem
	if v, ok := os.LookupEnv("AWS_CA_BUNDLE"); ok {
		os.Unsetenv("AWS_CA_BUNDLE")
		defer os.Setenv("AWS_CA_BUNDLE", v)
	}
	if o.AccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(o.AccessKey, o.SecretKey, o.SessionToken)))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return cfg, fmt.Errorf("AWS config: %w", err)
	}
	if cfg.Region == "" {
		if o.Endpoint == "" {
			return cfg, errors.New("no region: pass --region or set AWS_REGION")
		}
		cfg.Region = "us-east-1"
	}
	if o.RoleARN != "" {
		cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient(cfg, o), o.RoleARN))
	}
	return cfg, nil
}

func stsClient(cfg aws.Config, o *Opts) *sts.Client {
	return sts.NewFromConfig(cfg, func(so *sts.Options) {
		if o.STSEndpoint != "" {
			so.BaseEndpoint = aws.String(o.STSEndpoint)
		}
	})
}

// client builds an S3 client on e's config; mod adjusts the options (for
// example the checksum mode).
func (e *Env) client(mod func(*s3.Options)) *s3.Client {
	return s3.NewFromConfig(e.AWS, func(so *s3.Options) {
		if e.O.Endpoint != "" {
			so.BaseEndpoint = aws.String(e.O.Endpoint)
		}
		so.UsePathStyle = e.O.pathStyle()
		if mod != nil {
			mod(so)
		}
	})
}

// ---- outcome classification (as ../../s3cas.Classify) ----

type Outcome int

const (
	OK Outcome = iota
	PreconditionFailed
	Conflict
	NotFound
	Forbidden
	NotImplemented
	Other
)

func (o Outcome) String() string {
	return [...]string{"200/204", "412", "409", "404", "403", "501/400 unsupported", "other"}[o]
}

func classify(err error) (Outcome, int, string) {
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
	case status == 412 || code == "PreconditionFailed":
		return PreconditionFailed, status, code
	case status == 409:
		return Conflict, status, code
	case status == 404 || code == "NoSuchKey" || code == "NotFound" || code == "NoSuchUpload":
		return NotFound, status, code
	case status == 403 || code == "AccessDenied" || code == "ExpiredToken" || code == "InvalidAccessKeyId" ||
		code == "SignatureDoesNotMatch" || code == "RequestTimeTooSkewed":
		return Forbidden, status, code
	case status == 501 || code == "NotImplemented":
		return NotImplemented, status, code
	}
	return Other, status, code
}

// describe renders an error compactly for evidence lines.
func describe(err error) string {
	if err == nil {
		return "OK"
	}
	o, st, code := classify(err)
	if st != 0 {
		return fmt.Sprintf("HTTP %d %s (%v)", st, code, o)
	}
	// No HTTP answer (TLS, DNS, proxy, timeout): the innermost cause is at the end.
	s := err.Error()
	if i := strings.Index(s, "x509:"); i >= 0 {
		return "TLS: " + s[i:]
	}
	if len(s) > 160 {
		s = "…" + s[len(s)-160:]
	}
	return s
}
