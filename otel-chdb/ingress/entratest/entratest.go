// Package entratest is a local stand-in for Microsoft Entra's token
// issuer, for tests: the tenant-independent key set
// ({url}/common/discovery/v2.0/keys, each key with an "issuer" property,
// templated or bound to one tenant, as Entra publishes it) and a minter of
// v2.0 access tokens. The pattern is the query service's authtest issuer
// (../../query/internal/auth/authtest), with Entra's multi-tenant shape.
package entratest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Issuer serves the key set.
type Issuer struct {
	Server *httptest.Server
	URL    string

	mu       sync.Mutex
	keys     map[string]*rsa.PrivateKey
	issuers  map[string]string // kid -> the key's issuer property
	JWKSHits int
}

// New starts an issuer with one templated key, "k1".
func New() *Issuer {
	is := &Issuer{keys: map[string]*rsa.PrivateKey{}, issuers: map[string]string{}}
	mux := http.NewServeMux()
	is.Server = httptest.NewServer(mux)
	is.URL = is.Server.URL
	is.AddKey("k1", "")
	mux.HandleFunc("/common/discovery/v2.0/keys", func(w http.ResponseWriter, r *http.Request) {
		is.mu.Lock()
		defer is.mu.Unlock()
		is.JWKSHits++
		var keys []map[string]string
		for kid, k := range is.keys {
			keys = append(keys, map[string]string{
				"kty": "RSA", "kid": kid, "use": "sig", "issuer": is.issuers[kid],
				"n": base64.RawURLEncoding.EncodeToString(k.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(k.E)).Bytes()),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	})
	return is
}

// Close stops the server.
func (is *Issuer) Close() { is.Server.Close() }

// AddKey adds a signing key. tenant "" publishes it with the templated
// issuer ({url}/{tenantid}/v2.0); a tenant id binds it to that tenant.
func (is *Issuer) AddKey(kid, tenant string) {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	is.mu.Lock()
	defer is.mu.Unlock()
	is.keys[kid] = k
	if tenant == "" {
		is.issuers[kid] = is.URL + "/{tenantid}/v2.0"
	} else {
		is.issuers[kid] = is.URL + "/" + tenant + "/v2.0"
	}
}

// Token describes a v2.0 access token; zero fields take the defaults
// below, and Extra overrides or (with nil) deletes any claim.
type Token struct {
	Tenant, OID, Audience, ClientApp, Scope string
	Roles, Groups                           []string
	Kid                                     string
	Lifetime                                time.Duration
	Extra                                   jwt.MapClaims
}

// Mint signs t with the issuer's key t.Kid (default "k1").
func (is *Issuer) Mint(t Token) string {
	is.mu.Lock()
	kid := t.Kid
	if kid == "" {
		kid = "k1"
	}
	k := is.keys[kid]
	is.mu.Unlock()
	if t.Lifetime == 0 {
		t.Lifetime = 75 * time.Minute
	}
	now := time.Now()
	c := jwt.MapClaims{
		"iss": is.URL + "/" + t.Tenant + "/v2.0", "tid": t.Tenant, "oid": t.OID, "sub": "pairwise-" + t.OID,
		"aud": t.Audience, "azp": t.ClientApp, "ver": "2.0",
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(t.Lifetime).Unix(),
		"uti": base64.RawURLEncoding.EncodeToString(big.NewInt(now.UnixNano()).Bytes()),
	}
	if t.Scope != "" {
		c["scp"] = t.Scope
		c["idtyp"] = "user"
	} else {
		c["idtyp"] = "app"
	}
	if t.Roles != nil {
		c["roles"] = toAny(t.Roles)
	}
	if t.Groups != nil {
		c["groups"] = toAny(t.Groups)
	}
	for key, v := range t.Extra {
		if v == nil {
			delete(c, key)
		} else {
			c[key] = v
		}
	}
	return Sign(k, kid, jwt.SigningMethodRS256, c)
}

// Sign signs claims with an arbitrary key (a forger's).
func Sign(k any, kid string, m jwt.SigningMethod, c jwt.MapClaims) string {
	tok := jwt.NewWithClaims(m, c)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(k)
	if err != nil {
		panic(err)
	}
	return s
}

func toAny(xs []string) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
