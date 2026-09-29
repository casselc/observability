// Package ingress is the authenticated OTLP/HTTP ingress for producers
// outside Kubernetes (research/entra-ingress.md, DECISIONS.md D37): it
// verifies a Microsoft Entra access token, maps the identity to a tenant
// scope (cluster, namespace) by policy, stamps that scope and the caller's
// attribution onto the request (overwriting whatever the producer claimed,
// R-L1), and hands the request to the Go edge (../parquetgo/edge), which
// commits it to its lanes exactly as an in-cluster edge does.
//
// Nothing here trusts a value the producer chose: not a header, not an
// attribute, not a Langfuse key. The only inputs to the tenant are the
// token's verified claims and the operator's policy.
package ingress

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// EntraConfig is what the ingress trusts. Every list is an allow-list;
// empty means nothing is allowed (fail closed), never "anything".
type EntraConfig struct {
	// Authority is the login host, without a tenant
	// ("https://login.microsoftonline.com"; a fake issuer's URL in tests).
	// The expected issuer of a token is {Authority}/{tid}/v2.0.
	Authority string `json:"authority"`
	// JWKSURL is the key set (default {Authority}/common/discovery/v2.0/keys).
	// Keys may carry an "issuer" property, exact or templated with
	// {tenantid}; a key bound to one tenant signs for that tenant only.
	JWKSURL string `json:"jwks_url"`
	// Tenants are the Entra tenant ids (GUIDs) whose tokens are accepted.
	Tenants []string `json:"tenants"`
	// Audiences: the ingress API app registration's client id (v2 tokens
	// carry the GUID) and, if used, its App ID URI.
	Audiences []string `json:"audiences"`
	// ClientApps are the client ids (azp) allowed to present tokens: the
	// device forwarder's public client registration, CI workload apps.
	// A token for our API minted to any other client is refused.
	ClientApps []string `json:"client_apps"`
	// UserScope is the delegated scope a user token must carry
	// (e.g. "Telemetry.Write"); AppRoleClaim is the app role an app-only
	// token (CI, serverless) must carry (e.g. "Telemetry.Write.App").
	UserScope string `json:"user_scope"`
	AppRole   string `json:"app_role"`
	// LeewayS tolerates this much clock difference on exp/nbf/iat
	// (default 60; the ingress's clock, not the device's, is the judge).
	LeewayS int `json:"leeway_s"`
	// RefreshS: how often the key set is re-read (default 3600); an unknown
	// kid re-reads it at most once per MinRefreshS (default 30).
	RefreshS    int `json:"jwks_refresh_s"`
	MinRefreshS int `json:"jwks_min_refresh_s"`
}

// PrincipalType is who the token speaks for.
type PrincipalType string

const (
	PrincipalUser PrincipalType = "user" // delegated: a person, through the forwarder
	PrincipalApp  PrincipalType = "app"  // app-only: a workload (CI, serverless)
)

// Identity is a verified token's facts, the only input to the tenant
// mapping besides policy.
type Identity struct {
	TenantID  string        // tid (GUID, bound to the issuer)
	ObjectID  string        // oid: the user or service principal (GUID)
	Type      PrincipalType // user or app
	ClientApp string        // azp: the client the token was issued to
	Roles     []string      // app roles (assigned to the user, a group, or the app)
	Groups    []string      // group object ids, if the groups claim is emitted
	Overage   bool          // the groups claim overflowed (_claim_names.groups)
	Scopes    []string      // scp (delegated only)
	Expires   time.Time
}

// ErrUnauthenticated wraps every token failure: HTTP 401. The detail is
// logged, never sent back (an attacker learns nothing about which check
// failed beyond "the token").
var ErrUnauthenticated = errors.New("unauthenticated")

var guidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// IsGUID reports whether s is a lower-case GUID (Entra's tid and oid form).
func IsGUID(s string) bool { return guidRE.MatchString(s) }

type jwk struct {
	pub    *rsa.PublicKey
	issuer string // "" (unbound), exact, or templated with {tenantid}
}

// EntraVerifier checks Entra v2.0 access tokens for the ingress API.
type EntraVerifier struct {
	cfg    EntraConfig
	client *http.Client
	now    func() time.Time

	mu        sync.Mutex
	keys      map[string]jwk
	fetchedAt time.Time
	lastTry   time.Time
}

// NewEntraVerifier validates cfg; keys are fetched on first use.
func NewEntraVerifier(cfg EntraConfig, client *http.Client) (*EntraVerifier, error) {
	cfg.Authority = strings.TrimSuffix(cfg.Authority, "/")
	if cfg.Authority == "" {
		return nil, errors.New("entra: authority is required")
	}
	if len(cfg.Tenants) == 0 || len(cfg.Audiences) == 0 || len(cfg.ClientApps) == 0 {
		return nil, errors.New("entra: tenants, audiences and client_apps are required (allow-lists; empty is not 'any')")
	}
	for _, t := range cfg.Tenants {
		if !IsGUID(t) {
			return nil, fmt.Errorf("entra: tenant %q is not a GUID (names like 'common' or a domain are not trust boundaries)", t)
		}
	}
	if cfg.UserScope == "" && cfg.AppRole == "" {
		return nil, errors.New("entra: user_scope or app_role is required")
	}
	if cfg.JWKSURL == "" {
		cfg.JWKSURL = cfg.Authority + "/common/discovery/v2.0/keys"
	}
	if cfg.LeewayS <= 0 {
		cfg.LeewayS = 60
	}
	if cfg.RefreshS <= 0 {
		cfg.RefreshS = 3600
	}
	if cfg.MinRefreshS <= 0 {
		cfg.MinRefreshS = 30
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &EntraVerifier{cfg: cfg, client: client, now: time.Now}, nil
}

// SetClock replaces the verifier's clock (tests).
func (v *EntraVerifier) SetClock(now func() time.Time) { v.now = now }

func fail(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrUnauthenticated, fmt.Sprintf(format, a...))
}

// Verify checks signature (RS256, a key from the set, bound to the token's
// tenant if the key says so), issuer = {Authority}/{tid}/v2.0 with tid an
// allowed GUID, audience, client app, times, version and scope or role,
// and returns the identity.
func (v *EntraVerifier) Verify(ctx context.Context, raw string) (*Identity, error) {
	claims := jwt.MapClaims{}
	var signer jwk
	p := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(time.Duration(v.cfg.LeewayS)*time.Second),
		jwt.WithTimeFunc(v.now),
	)
	_, err := p.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		k, err := v.key(ctx, kid)
		if err != nil {
			return nil, err
		}
		signer = k
		return k.pub, nil
	})
	if err != nil {
		return nil, fail("%v", err)
	}
	str := func(name string) string { s, _ := claims[name].(string); return s }
	tid := str("tid")
	if !IsGUID(tid) || !slices.Contains(v.cfg.Tenants, tid) {
		return nil, fail("tenant %q not accepted", tid)
	}
	iss := v.cfg.Authority + "/" + tid + "/v2.0"
	if str("iss") != iss {
		return nil, fail("issuer %q is not %q (issuer and tid must agree)", str("iss"), iss)
	}
	if signer.issuer != "" && strings.ReplaceAll(signer.issuer, "{tenantid}", tid) != iss {
		return nil, fail("signing key is bound to issuer %q", signer.issuer)
	}
	if str("ver") != "2.0" {
		return nil, fail("token version %q (v2.0 only: set requestedAccessTokenVersion 2)", str("ver"))
	}
	aud, _ := claims.GetAudience()
	if !slices.ContainsFunc(aud, func(a string) bool { return slices.Contains(v.cfg.Audiences, a) }) {
		return nil, fail("audience %v is not this ingress (a token for another API is never accepted: confused deputy)", aud)
	}
	azp := str("azp")
	if !slices.Contains(v.cfg.ClientApps, azp) {
		return nil, fail("client app %q not allowed", azp)
	}
	oid := str("oid")
	if !IsGUID(oid) {
		return nil, fail("no oid")
	}
	exp, _ := claims.GetExpirationTime()
	id := &Identity{TenantID: tid, ObjectID: oid, ClientApp: azp, Roles: strList(claims["roles"]),
		Groups: strList(claims["groups"]), Scopes: strings.Fields(str("scp")), Expires: exp.Time}
	if names, ok := claims["_claim_names"].(map[string]any); ok {
		_, id.Overage = names["groups"]
	}
	// idtyp is an optional claim the API registration must emit (DECISIONS
	// D37): without it a token with no scp is an app token.
	switch str("idtyp") {
	case "app":
		id.Type = PrincipalApp
	case "user":
		id.Type = PrincipalUser
	case "":
		if str("scp") != "" {
			id.Type = PrincipalUser
		} else {
			id.Type = PrincipalApp
		}
	default:
		return nil, fail("idtyp %q", str("idtyp"))
	}
	switch id.Type {
	case PrincipalUser:
		if v.cfg.UserScope == "" || !slices.Contains(id.Scopes, v.cfg.UserScope) {
			return nil, fail("user token without scope %q", v.cfg.UserScope)
		}
	case PrincipalApp:
		if str("scp") != "" {
			return nil, fail("app token with a delegated scope")
		}
		if v.cfg.AppRole == "" || !slices.Contains(id.Roles, v.cfg.AppRole) {
			return nil, fail("app token without role %q", v.cfg.AppRole)
		}
	}
	return id, nil
}

func strList(v any) []string {
	xs, _ := v.([]any)
	var out []string
	for _, x := range xs {
		if s, ok := x.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (v *EntraVerifier) key(ctx context.Context, kid string) (jwk, error) {
	if kid == "" {
		return jwk{}, errors.New("no kid")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.now()
	minGap := time.Duration(v.cfg.MinRefreshS) * time.Second
	refresh := func() error {
		v.lastTry = now
		keys, err := v.fetch(ctx)
		if err != nil {
			return err // the previous set stays in use
		}
		v.keys, v.fetchedAt = keys, now
		return nil
	}
	stale := now.Sub(v.fetchedAt) > time.Duration(v.cfg.RefreshS)*time.Second
	if v.keys == nil || (stale && now.Sub(v.lastTry) >= minGap) {
		if err := refresh(); err != nil && v.keys == nil {
			return jwk{}, err
		}
	}
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	if now.Sub(v.lastTry) >= minGap { // a rotation: re-read, rate-limited
		_ = refresh()
		if k, ok := v.keys[kid]; ok {
			return k, nil
		}
	}
	return jwk{}, fmt.Errorf("no key %q in the key set", kid)
}

func (v *EntraVerifier) fetch(ctx context.Context) (map[string]jwk, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", v.cfg.JWKSURL, resp.Status)
	}
	var set struct {
		Keys []struct {
			Kty, Kid, Use, N, E, Issuer string
		} `json:"keys"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 1<<20)).Decode(&set); err != nil {
		return nil, err
	}
	out := map[string]jwk{}
	for _, k := range set.Keys {
		if k.Kty != "RSA" || (k.Use != "" && k.Use != "sig") || k.Kid == "" {
			continue
		}
		n, err1 := base64.RawURLEncoding.DecodeString(k.N)
		e, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if err1 != nil || err2 != nil || len(e) == 0 || len(e) > 4 {
			continue
		}
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		if pub.N.BitLen() < 2048 {
			continue
		}
		out[k.Kid] = jwk{pub: pub, issuer: k.Issuer}
	}
	if len(out) == 0 {
		return nil, errors.New("jwks: no usable RSA signing key")
	}
	return out, nil
}

// Bearer extracts the token from an Authorization header.
func Bearer(h string) (string, bool) {
	const p = "bearer "
	if len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) {
		return "", false
	}
	return strings.TrimSpace(h[len(p):]), true
}
