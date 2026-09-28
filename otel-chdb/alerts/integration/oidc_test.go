package integration

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"time"
)

// issuer is a minimal OIDC provider: a JWKS and an OAuth 2.0 client
// credentials token endpoint issuing RS256 tokens (the query service's own
// test issuer is internal to its module). clients maps a client id to the
// claims its tokens carry; each client's secret is ALR_IT_SECRET_* as the
// test sets it ("fleet-secret", "aa-secret").
type issuer struct {
	*httptest.Server
	key *rsa.PrivateKey
}

func newIssuer(clients map[string]map[string]any) *issuer {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	is := &issuer{key: key}
	secrets := map[string]string{"alertd-fleet": "fleet-secret", "alertd-aa": "aa-secret"}
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		b64 := base64.RawURLEncoding.EncodeToString
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
			"n": b64(key.N.Bytes()), "e": b64(big.NewInt(int64(key.E)).Bytes())}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		id, sec, ok := r.BasicAuth()
		_ = r.ParseForm()
		claims, known := clients[id]
		if !ok || !known || secrets[id] != sec || r.Form.Get("grant_type") != "client_credentials" {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
			return
		}
		c := map[string]any{"iss": is.URL, "aud": r.Form.Get("audience"), "sub": id, "iat": time.Now().Unix(), "exp": time.Now().Add(5 * time.Minute).Unix()}
		for k, v := range claims {
			c[k] = v
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": is.sign(c), "token_type": "Bearer", "expires_in": 300})
	})
	is.Server = httptest.NewServer(mux)
	return is
}

func (is *issuer) sign(claims map[string]any) string {
	enc := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
	in := enc(map[string]string{"alg": "RS256", "typ": "JWT", "kid": "k1"}) + "." + enc(claims)
	h := sha256.Sum256([]byte(in))
	sig, err := rsa.SignPKCS1v15(rand.Reader, is.key, crypto.SHA256, h[:])
	if err != nil {
		panic(err)
	}
	return in + "." + base64.RawURLEncoding.EncodeToString(sig)
}
