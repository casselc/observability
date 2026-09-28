// Package app loads the service's configuration and wires its parts; the
// command and the integration test share it.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/catalog"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/lake"
	"github.com/casselc/observability/otel-chdb/query/internal/server"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// Config is the file's shape.
type Config struct {
	Listen string          `json:"listen"`
	OIDC   auth.OIDCConfig `json:"oidc"`
	Claims auth.Mapping    `json:"claims"`
	Audit  struct {
		Path  string `json:"path"`
		Fsync bool   `json:"fsync"`
	} `json:"audit"`
	Central struct {
		central.Config
		PasswordEnv string            `json:"password_env"`
		Tables      []*sqlscope.Table `json:"tables"`
		MaxSQLBytes int               `json:"max_sql_bytes"`
	} `json:"central"`
	Limits    server.Limits  `json:"limits"`
	Catalog   catalog.Config `json:"catalog"`
	S3        store.S3Config `json:"s3"`
	Lake      lake.Config    `json:"lake"`
	Watermark struct {
		CacheS  int `json:"cache_s"`
		MaxAgeS int `json:"max_age_s"`
		// MaxLatenessS bridges custody time (complete_through) and event
		// time (windows): a window is complete once complete_through ≥ its
		// end + this (STPA CAST row 26). Default 60; 0 is allowed and
		// claims the two clocks are one.
		MaxLatenessS *float64 `json:"max_lateness_s"`
		// CountLate: a windowed /v1/query counts rows later than
		// MaxLatenessS (default true).
		CountLate *bool `json:"count_late"`
	} `json:"watermark"`
	// Basis (D30): the keys basis tokens are minted and verified with, and
	// how old a basis may be.
	Basis struct {
		// KeysEnv names the variable holding "kid:base64,kid:base64" (at
		// least 32 bytes each; default QS_BASIS_KEYS). Every replica needs
		// the same keys. None: a random key per process (bases then do not
		// survive a restart and are refused by another replica).
		KeysEnv string `json:"keys_env"`
		// Current is the kid new bases are minted with (default: the first).
		Current string `json:"current"`
		// RetentionS: a basis older than this, or a window starting before
		// it, is basis_expired (default 90 days: central's custody-age
		// retention, D19). SkewS: how early a row may be received before
		// its event time (default the lake's skew_s, 300).
		RetentionS int `json:"retention_s"`
		SkewS      int `json:"skew_s"`
		// Signer (QS_BASIS_SIGNER): "static" (default: the keys above) or
		// "kms" (D30 amendment 2026-09-28): KMSKeys mint and verify, and
		// the static keys, if any, stay verify-only (a migration).
		Signer string `json:"signer"`
		// KMSKeys (QS_BASIS_KMS_KEYS "id=arn-or-alias[*],…", * marks the
		// current one): the only KMS keys a token may name.
		KMSKeys []basis.KMSKey `json:"kms_keys"`
		// KMSRegion (QS_BASIS_KMS_REGION): default the region of the keys'
		// ARNs, else s3.region, else the SDK's. KMSEndpoint
		// (QS_BASIS_KMS_ENDPOINT): an emulator; empty for AWS.
		KMSRegion   string `json:"kms_region"`
		KMSEndpoint string `json:"kms_endpoint"`
		// KMSTimeoutS bounds each KMS call (default 2).
		KMSTimeoutS float64 `json:"kms_timeout_s"`
		// StartWithoutSigner (QS_BASIS_START_WITHOUT_SIGNER): start even
		// when a KMS key cannot be described or is not an enabled
		// HMAC_SHA_256 key; otherwise that fails startup.
		StartWithoutSigner bool `json:"start_without_signer"`
		// VerifyCacheS: how long a verified token is not re-verified
		// (default 600); MintReuseS: how long an equal basis's token is
		// reused (default 5 with kms, 0 with static keys).
		VerifyCacheS *int `json:"verify_cache_s"`
		MintReuseS   *int `json:"mint_reuse_s"`
	} `json:"basis"`
	CORSOrigins  []string `json:"cors_origins"`
	MaxBodyBytes int64    `json:"max_body_bytes"`
	// LakeEnabled turns /v1/plan on.
	LakeEnabled bool `json:"lake_enabled"`
}

func env(dst *string, k string) {
	if v := os.Getenv(k); v != "" {
		*dst = v
	}
}

// Load reads path (optional) and applies the environment.
func Load(path string) (*Config, error) {
	c := &Config{Listen: ":18190"}
	c.Limits.Default = central.DefaultLimits
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(c); err != nil {
			return nil, err
		}
	}
	env(&c.Listen, "QS_LISTEN")
	env(&c.OIDC.Issuer, "QS_OIDC_ISSUER")
	env(&c.OIDC.Audience, "QS_OIDC_AUDIENCE")
	env(&c.OIDC.JWKSURL, "QS_OIDC_JWKS_URL")
	env(&c.Audit.Path, "QS_AUDIT_PATH")
	env(&c.Central.URL, "QS_CH_URL")
	env(&c.Central.User, "QS_CH_USER")
	env(&c.Central.Database, "QS_CH_DATABASE")
	pwEnv := c.Central.PasswordEnv
	if pwEnv == "" {
		pwEnv = "QS_CH_PASSWORD"
	}
	c.Central.Password = os.Getenv(pwEnv)
	env(&c.S3.Endpoint, "QS_S3_ENDPOINT")
	env(&c.S3.PublicEndpoint, "QS_S3_PUBLIC_ENDPOINT")
	env(&c.S3.Bucket, "QS_S3_BUCKET")
	env(&c.S3.Region, "QS_S3_REGION")
	env(&c.S3.AccessKey, "QS_S3_KEY")
	env(&c.S3.SecretKey, "QS_S3_SECRET")
	env(&c.Lake.Root, "QS_LAKE_ROOT")
	env(&c.Lake.Ctl, "QS_LAKE_CTL")
	env(&c.Catalog.Database, "QS_CATALOG_DB")
	if v := os.Getenv("QS_LAKE_ENABLED"); v != "" {
		c.LakeEnabled, _ = strconv.ParseBool(v)
	}
	if v := os.Getenv("QS_MAX_LATENESS_S"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("QS_MAX_LATENESS_S: %w", err)
		}
		c.Watermark.MaxLatenessS = &f
	}
	if v := os.Getenv("QS_COUNT_LATE"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("QS_COUNT_LATE: %w", err)
		}
		c.Watermark.CountLate = &b
	}
	env(&c.Basis.Signer, "QS_BASIS_SIGNER")
	env(&c.Basis.KMSRegion, "QS_BASIS_KMS_REGION")
	env(&c.Basis.KMSEndpoint, "QS_BASIS_KMS_ENDPOINT")
	if v := os.Getenv("QS_BASIS_KMS_KEYS"); v != "" {
		ks, err := ParseKMSKeys(v)
		if err != nil {
			return nil, fmt.Errorf("QS_BASIS_KMS_KEYS: %w", err)
		}
		c.Basis.KMSKeys = ks
	}
	if v := os.Getenv("QS_BASIS_START_WITHOUT_SIGNER"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("QS_BASIS_START_WITHOUT_SIGNER: %w", err)
		}
		c.Basis.StartWithoutSigner = b
	}
	switch c.Basis.Signer {
	case "":
		c.Basis.Signer = "static"
	case "static", "kms":
	default:
		return nil, fmt.Errorf("basis.signer %q: want static or kms", c.Basis.Signer)
	}
	if c.Basis.Signer == "kms" && len(c.Basis.KMSKeys) == 0 {
		return nil, errors.New("basis.signer is kms but basis.kms_keys (QS_BASIS_KMS_KEYS) is empty")
	}
	if c.Watermark.MaxLatenessS == nil {
		d := completeness.DefaultMaxLateness.Seconds()
		c.Watermark.MaxLatenessS = &d
	}
	if l := *c.Watermark.MaxLatenessS; l < 0 || l > 86400 || l != l {
		return nil, fmt.Errorf("watermark.max_lateness_s %v: want 0 to 86400", l)
	}
	if c.Audit.Path == "" {
		return nil, errors.New("audit.path is required: every decision is recorded")
	}
	if c.S3.Bucket == "" {
		return nil, errors.New("s3.bucket is required: complete_through is read from {ctl}/watermark.json")
	}
	if c.Watermark.CacheS <= 0 {
		c.Watermark.CacheS = 15
	}
	if c.Watermark.MaxAgeS <= 0 {
		c.Watermark.MaxAgeS = 300
	}
	return c, nil
}

// Build wires a server from c.
func Build(ctx context.Context, c *Config, verifier server.TokenVerifier, sink audit.Sink) (*server.Server, error) {
	policy, err := sqlscope.NewPolicy(c.Central.Database, c.Central.Tables, c.Central.MaxSQLBytes)
	if err != nil {
		return nil, err
	}
	ch := central.New(c.Central.Config)
	cat, err := catalog.New(c.Catalog, ch)
	if err != nil {
		return nil, err
	}
	st, err := store.NewS3(ctx, c.S3)
	if err != nil {
		return nil, err
	}
	lc := c.Lake
	planner := lake.New(lc, st, nil)
	ctl := planner.Config().Ctl
	wm := completeness.NewReader(func(ctx context.Context, key string) ([]byte, error) {
		b, _, err := st.Get(ctx, key)
		return b, err
	}, ctl+"/watermark.json", time.Duration(c.Watermark.CacheS)*time.Second, time.Duration(c.Watermark.MaxAgeS)*time.Second)
	if c.Watermark.MaxLatenessS != nil {
		wm.SetMaxLateness(time.Duration(*c.Watermark.MaxLatenessS * float64(time.Second)))
	}
	var s *server.Server
	bases, err := Bases(ctx, c, nil, func(op, result string) {
		if s != nil {
			s.ObserveSigner(op, result)
		}
	})
	if err != nil {
		return nil, err
	}
	s = &server.Server{Verifier: verifier, Mapping: &c.Claims, Audit: sink, Policy: policy, Central: ch, Catalog: cat, Bases: bases,
		Retention: time.Duration(c.Basis.RetentionS) * time.Second, BasisSkew: time.Duration(c.Basis.SkewS) * time.Second,
		Watermark: wm, Limits: c.Limits, Origins: c.CORSOrigins, MaxBody: c.MaxBodyBytes,
		NoLateCount: c.Watermark.CountLate != nil && !*c.Watermark.CountLate}
	if c.LakeEnabled {
		s.Planner = lake.New(lc, st, wm)
	}
	s.Init()
	return s, nil
}

// keyring reads the static basis keys from the environment (Config.Basis);
// nil when none are set.
func keyring(c *Config) (*basis.Keyring, string, error) {
	name := c.Basis.KeysEnv
	if name == "" {
		name = "QS_BASIS_KEYS"
	}
	spec := os.Getenv(name)
	if spec == "" {
		return nil, name, nil
	}
	cur := c.Basis.Current
	if v := os.Getenv("QS_BASIS_KEY_CURRENT"); v != "" {
		cur = v
	}
	kr, err := basis.ParseKeys(spec, cur)
	return kr, name, err
}

// ParseKMSKeys reads QS_BASIS_KMS_KEYS: "id=arn-or-alias[*],…"; a
// trailing * marks the current key (default: the first).
func ParseKMSKeys(spec string) ([]basis.KMSKey, error) {
	var out []basis.KMSKey
	cur := false
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, key, ok := strings.Cut(part, "=")
		if !ok {
			return nil, errors.New("want id=arn-or-alias[*][,…]")
		}
		k := basis.KMSKey{ID: strings.TrimSpace(id), Key: strings.TrimSpace(key)}
		if strings.HasSuffix(k.Key, "*") {
			k.Key, k.Current = strings.TrimSuffix(k.Key, "*"), true
			cur = true
		}
		out = append(out, k)
	}
	if len(out) > 0 && !cur {
		out[0].Current = true
	}
	return out, nil
}

// kmsRegion: basis.kms_region, else the region every configured key ARN
// names (they must agree: one client), else s3.region, else the SDK's.
func kmsRegion(c *Config) (string, error) {
	region := ""
	for _, k := range c.Basis.KMSKeys {
		if parts := strings.Split(k.Key, ":"); strings.HasPrefix(k.Key, "arn:") && len(parts) > 3 {
			if region != "" && parts[3] != region {
				return "", fmt.Errorf("basis.kms_keys name keys in regions %s and %s: one region per service", region, parts[3])
			}
			region = parts[3]
		}
	}
	if c.Basis.KMSRegion != "" {
		if region != "" && region != c.Basis.KMSRegion {
			return "", fmt.Errorf("basis.kms_region %s, but the key ARNs are in %s", c.Basis.KMSRegion, region)
		}
		return c.Basis.KMSRegion, nil
	}
	if region != "" {
		return region, nil
	}
	return c.S3.Region, nil
}

// Bases builds the basis signer set (D30 and its 2026-09-28 amendment).
// static: the keys of QS_BASIS_KEYS, or a random per-process key when there
// are none (logged: bases then die with the process). kms: basis.kms_keys
// mint and verify (each checked with DescribeKey, or startup fails unless
// basis.start_without_signer), and the static keys, if any, only verify.
// api overrides the KMS client (tests); nil builds one from the service's
// AWS chain.
func Bases(ctx context.Context, c *Config, api basis.KMSAPI, observe func(op, result string)) (*basis.Bases, error) {
	kr, name, err := keyring(c)
	if err != nil {
		return nil, err
	}
	o := basis.Options{Observe: observe}
	if c.Basis.VerifyCacheS != nil {
		o.VerifyTTL = time.Duration(*c.Basis.VerifyCacheS) * time.Second
		if *c.Basis.VerifyCacheS == 0 {
			o.VerifyTTL = -1
		}
	}
	if c.Basis.Signer != "kms" {
		if kr == nil {
			log.Printf("basis: no keys in %s: minting with a random per-process key (bases will not survive a restart or reach another replica)", name)
			kr = basis.Ephemeral()
		}
		if c.Basis.MintReuseS != nil {
			o.MintReuse = time.Duration(*c.Basis.MintReuseS) * time.Second
		}
		return basis.New(kr, nil, o)
	}
	if api == nil {
		region, err := kmsRegion(c)
		if err != nil {
			return nil, err
		}
		awsCfg, err := store.AWSConfig(ctx, region, "", "")
		if err != nil {
			return nil, fmt.Errorf("basis kms: AWS config: %w", err)
		}
		api = kms.NewFromConfig(awsCfg, func(o *kms.Options) {
			if c.Basis.KMSEndpoint != "" {
				o.BaseEndpoint = aws.String(c.Basis.KMSEndpoint)
			}
		})
	}
	ks, err := basis.NewKMSSigner(api, c.Basis.KMSKeys, time.Duration(c.Basis.KMSTimeoutS*float64(time.Second)))
	if err != nil {
		return nil, err
	}
	if ks.Current() == "" {
		return nil, errors.New("basis kms: no key in basis.kms_keys is current")
	}
	if err := ks.Check(ctx); err != nil {
		if !c.Basis.StartWithoutSigner {
			return nil, fmt.Errorf("%w (basis.start_without_signer starts anyway: requests needing a basis then fail with 503 basis_signer_unavailable)", err)
		}
		log.Printf("basis: starting without a working KMS signer: %v", err)
	}
	for _, k := range c.Basis.KMSKeys {
		if !strings.HasPrefix(k.Key, "arn:") || strings.Contains(k.Key, ":alias/") {
			log.Printf("basis: KMS key %q is named by alias %s: resolved to its key at startup; re-pointing the alias makes restarted replicas refuse the old key's bases under this id (name keys by key ARN)", k.ID, k.Key)
		}
	}
	o.MintReuse = 5 * time.Second
	if c.Basis.MintReuseS != nil {
		o.MintReuse = time.Duration(*c.Basis.MintReuseS) * time.Second
	}
	var verifyOnly []basis.Signer
	if kr != nil {
		verifyOnly = append(verifyOnly, kr)
		log.Printf("basis: minting with KMS key %s; static keys %s verify only", ks.Current(), name)
	}
	return basis.New(ks, verifyOnly, o)
}
