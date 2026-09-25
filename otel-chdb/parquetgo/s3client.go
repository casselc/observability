package parquetgo

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// newS3Client builds the S3 client for an s3:// or http(s):// URL.
//
// Credentials: static keys when AccessKeyID is set (Nutanix Objects and
// other S3-compatible stores), unsigned when Anonymous, and otherwise the
// AWS default chain from config.LoadDefaultConfig: env vars, shared
// config/credentials files (profiles, credential_process as used by IAM
// Roles Anywhere's aws_signing_helper, SSO), web identity (EKS IRSA:
// AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE), container credentials (EKS
// Pod Identity: AWS_CONTAINER_CREDENTIALS_FULL_URI +
// AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE) and IMDS. The chain's providers
// sit behind aws.CredentialsCache, which refreshes before expiry. Nothing
// is fetched here: the first request resolves credentials.
func newS3Client(cfg Config, u *url.URL) (*s3.Client, error) {
	custom := u.Scheme != "s3"
	pathStyle := custom
	if cfg.PathStyle != nil {
		pathStyle = *cfg.PathStyle
	}
	ca := cfg.CABundle
	if ca == "" {
		ca = os.Getenv("AWS_CA_BUNDLE")
	}
	var rt http.RoundTripper = http.DefaultTransport
	var pem []byte
	if ca != "" {
		var err error
		pem, err = os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("CA bundle: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA bundle %s: no PEM certificates", ca)
		}
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = &tls.Config{RootCAs: roots}
		rt = tr
	}
	if cfg.WrapTransport != nil {
		rt = cfg.WrapTransport(rt)
	}
	if cfg.Transport != nil {
		rt = cfg.Transport
	}

	var awsCfg aws.Config
	switch {
	case cfg.AccessKeyID != "":
		awsCfg = aws.Config{Region: cfg.S3Region, Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)}
	case cfg.Anonymous:
		awsCfg = aws.Config{Region: cfg.S3Region, Credentials: aws.AnonymousCredentials{}}
	default:
		var opts []func(*config.LoadOptions) error
		if cfg.S3Region != "" {
			opts = append(opts, config.WithRegion(cfg.S3Region))
		}
		if cfg.Profile != "" {
			opts = append(opts, config.WithSharedConfigProfile(cfg.Profile))
		}
		if cfg.CABundle != "" {
			// STS (IRSA, AssumeRole) and SSO calls trust the same CA;
			// AWS_CA_BUNDLE already reaches them through the env config.
			opts = append(opts, config.WithCustomCABundle(bytes.NewReader(pem)))
		}
		var err error
		if awsCfg, err = config.LoadDefaultConfig(context.Background(), opts...); err != nil {
			return nil, fmt.Errorf("AWS config: %w", err)
		}
	}
	if cfg.RoleARN != "" {
		if awsCfg.Credentials == nil {
			return nil, errors.New("role_arn needs base credentials")
		}
		awsCfg.Credentials = aws.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(sts.NewFromConfig(awsCfg), cfg.RoleARN))
	}
	if awsCfg.Region == "" {
		if !custom {
			return nil, fmt.Errorf("S3 URL %q: no region (set s3_region or AWS_REGION)", u)
		}
		awsCfg.Region = "us-east-1"
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if custom {
			o.BaseEndpoint = aws.String(u.Scheme + "://" + u.Host)
		}
		o.UsePathStyle = pathStyle
		o.HTTPClient = &http.Client{Transport: rt}
	}), nil
}
