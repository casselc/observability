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
	bases, err := keyring(c)
	if err != nil {
		return nil, err
	}
	s := &server.Server{Verifier: verifier, Mapping: &c.Claims, Audit: sink, Policy: policy, Central: ch, Catalog: cat, Bases: bases,
		Retention: time.Duration(c.Basis.RetentionS) * time.Second, BasisSkew: time.Duration(c.Basis.SkewS) * time.Second,
		Watermark: wm, Limits: c.Limits, Origins: c.CORSOrigins, MaxBody: c.MaxBodyBytes,
		NoLateCount: c.Watermark.CountLate != nil && !*c.Watermark.CountLate}
	if c.LakeEnabled {
		s.Planner = lake.New(lc, st, wm)
	}
	s.Init()
	return s, nil
}

// keyring reads the basis keys from the environment (Config.Basis).
func keyring(c *Config) (*basis.Keyring, error) {
	name := c.Basis.KeysEnv
	if name == "" {
		name = "QS_BASIS_KEYS"
	}
	spec := os.Getenv(name)
	if spec == "" {
		log.Printf("basis: no keys in %s: minting with a random per-process key (bases will not survive a restart or reach another replica)", name)
		return basis.Ephemeral(), nil
	}
	cur := c.Basis.Current
	if v := os.Getenv("QS_BASIS_KEY_CURRENT"); v != "" {
		cur = v
	}
	return basis.ParseKeys(spec, cur)
}
