// Package auth verifies OIDC bearer tokens against an issuer's JWKS and maps
// their claims to the attributes the service scopes by: clusters,
// namespaces and roles. Everything not granted is denied.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// OIDCConfig is the issuer the service trusts.
type OIDCConfig struct {
	Issuer   string `json:"issuer"`
	Audience string `json:"audience"`
	// JWKSURL, if empty, is read from {issuer}/.well-known/openid-configuration.
	JWKSURL string `json:"jwks_url"`
	// Algorithms accepted; default RS256 only.
	Algorithms []string `json:"algorithms"`
	LeewayS    int      `json:"leeway_s"`
	// RefreshS: how often the key set is re-read; an unknown kid triggers a
	// re-read too, at most once per MinRefreshS.
	RefreshS    int `json:"jwks_refresh_s"`
	MinRefreshS int `json:"jwks_min_refresh_s"`
}

// Verifier checks tokens.
type Verifier struct {
	cfg    OIDCConfig
	client *http.Client
	now    func() time.Time

	mu        sync.Mutex
	jwksURL   string
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
	lastTry   time.Time
}

// NewVerifier returns a verifier; keys are fetched on first use.
func NewVerifier(cfg OIDCConfig, client *http.Client) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("oidc: issuer and audience are required")
	}
	if len(cfg.Algorithms) == 0 {
		cfg.Algorithms = []string{"RS256"}
	}
	for _, a := range cfg.Algorithms {
		if a != "RS256" && a != "RS384" && a != "RS512" {
			return nil, fmt.Errorf("oidc: algorithm %s is not supported (RSA only)", a)
		}
	}
	if cfg.RefreshS <= 0 {
		cfg.RefreshS = 300
	}
	if cfg.MinRefreshS <= 0 {
		cfg.MinRefreshS = 30
	}
	if cfg.LeewayS < 0 {
		cfg.LeewayS = 0
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Verifier{cfg: cfg, client: client, now: time.Now, jwksURL: cfg.JWKSURL}, nil
}

// ErrUnauthenticated wraps every token failure (HTTP 401).
var ErrUnauthenticated = errors.New("unauthenticated")

// Verify checks the token's signature, issuer, audience and times, and
// returns its claims.
func (v *Verifier) Verify(ctx context.Context, raw string) (jwt.MapClaims, error) {
	claims := jwt.MapClaims{}
	p := jwt.NewParser(
		jwt.WithValidMethods(v.cfg.Algorithms),
		jwt.WithIssuer(v.cfg.Issuer),
		jwt.WithAudience(v.cfg.Audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(time.Duration(v.cfg.LeewayS)*time.Second),
		jwt.WithTimeFunc(v.now),
	)
	_, err := p.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		return v.key(ctx, kid)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	return claims, nil
}

func (v *Verifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.now()
	minGap := time.Duration(v.cfg.MinRefreshS) * time.Second
	refresh := func() error {
		v.lastTry = now
		keys, err := v.fetch(ctx)
		if err != nil {
			return err // the previous set, if any, stays in use
		}
		v.keys, v.fetchedAt = keys, now
		return nil
	}
	stale := now.Sub(v.fetchedAt) > time.Duration(v.cfg.RefreshS)*time.Second
	if v.keys == nil || (stale && now.Sub(v.lastTry) >= minGap) {
		if err := refresh(); err != nil && v.keys == nil {
			return nil, err
		}
	}
	if k := v.lookup(kid); k != nil {
		return k, nil
	}
	// an unknown kid: the issuer may have rotated; re-read, rate-limited
	if now.Sub(v.lastTry) >= minGap {
		_ = refresh()
		if k := v.lookup(kid); k != nil {
			return k, nil
		}
	}
	return nil, fmt.Errorf("no key %q in the issuer's key set", kid)
}

func (v *Verifier) lookup(kid string) *rsa.PublicKey {
	if k, ok := v.keys[kid]; ok {
		return k
	}
	if kid == "" && len(v.keys) == 1 {
		for _, only := range v.keys {
			return only
		}
	}
	return nil
}

func (v *Verifier) fetch(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	if v.jwksURL == "" {
		var disc struct {
			Issuer  string `json:"issuer"`
			JWKSURI string `json:"jwks_uri"`
		}
		if err := v.getJSON(ctx, strings.TrimSuffix(v.cfg.Issuer, "/")+"/.well-known/openid-configuration", &disc); err != nil {
			return nil, err
		}
		if disc.Issuer != v.cfg.Issuer {
			return nil, fmt.Errorf("oidc discovery: issuer %q, configured %q", disc.Issuer, v.cfg.Issuer)
		}
		if disc.JWKSURI == "" {
			return nil, errors.New("oidc discovery: no jwks_uri")
		}
		v.jwksURL = disc.JWKSURI
	}
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := v.getJSON(ctx, v.jwksURL, &set); err != nil {
		return nil, err
	}
	out := map[string]*rsa.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "RSA" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		n, err1 := base64.RawURLEncoding.DecodeString(k.N)
		e, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if err1 != nil || err2 != nil || len(e) == 0 || len(e) > 4 {
			continue
		}
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		if pub.N.BitLen() < 2048 {
			continue // refuse short keys
		}
		out[k.Kid] = pub
	}
	if len(out) == 0 {
		return nil, errors.New("jwks: no usable RSA signing key")
	}
	return out, nil
}

func (v *Verifier) getJSON(ctx context.Context, url string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 1<<20)).Decode(into)
}

// Bearer extracts the token from an Authorization header.
func Bearer(h string) (string, bool) {
	const p = "bearer "
	if len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) {
		return "", false
	}
	return strings.TrimSpace(h[len(p):]), true
}
