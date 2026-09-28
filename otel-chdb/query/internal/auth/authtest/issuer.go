// Package authtest is a local OIDC issuer for tests: RS256 keys, a discovery
// document and a JWKS over httptest, and a token minter.
package authtest

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

// Issuer serves /.well-known/openid-configuration and /jwks.
type Issuer struct {
	Server *httptest.Server
	URL    string

	mu   sync.Mutex
	keys map[string]*rsa.PrivateKey
	kid  string
	// JWKSHits counts key-set fetches.
	JWKSHits int
}

// New starts an issuer with one 2048-bit key, "k1".
func New() *Issuer {
	is := &Issuer{keys: map[string]*rsa.PrivateKey{}}
	is.Rotate("k1")
	mux := http.NewServeMux()
	is.Server = httptest.NewServer(mux)
	is.URL = is.Server.URL
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": is.URL, "jwks_uri": is.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		is.mu.Lock()
		defer is.mu.Unlock()
		is.JWKSHits++
		var keys []map[string]string
		for kid, k := range is.keys {
			keys = append(keys, map[string]string{
				"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
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

// Rotate adds a key and signs with it from now on.
func (is *Issuer) Rotate(kid string) {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	is.mu.Lock()
	defer is.mu.Unlock()
	is.keys[kid] = k
	is.kid = kid
}

// Mint signs claims with the current key; iss, exp and iat default to the
// issuer, now + 5 min and now.
func (is *Issuer) Mint(claims jwt.MapClaims) string {
	is.mu.Lock()
	k, kid := is.keys[is.kid], is.kid
	is.mu.Unlock()
	c := jwt.MapClaims{"iss": is.URL, "exp": time.Now().Add(5 * time.Minute).Unix(), "iat": time.Now().Unix()}
	for key, v := range claims {
		if v == nil {
			delete(c, key)
			continue
		}
		c[key] = v
	}
	t := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	t.Header["kid"] = kid
	s, err := t.SignedString(k)
	if err != nil {
		panic(err)
	}
	return s
}

// MintWith signs with an arbitrary key (a forger's).
func MintWith(k *rsa.PrivateKey, kid string, method jwt.SigningMethod, claims jwt.MapClaims) string {
	t := jwt.NewWithClaims(method, claims)
	t.Header["kid"] = kid
	s, err := t.SignedString(k)
	if err != nil {
		panic(err)
	}
	return s
}
