package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/auth/authtest"
	"github.com/golang-jwt/jwt/v5"
)

func verifier(t *testing.T, is *authtest.Issuer) *auth.Verifier {
	t.Helper()
	v, err := auth.NewVerifier(auth.OIDCConfig{Issuer: is.URL, Audience: "otel-query"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestVerify(t *testing.T) {
	is := authtest.New()
	defer is.Close()
	v := verifier(t, is)
	ctx := context.Background()
	good := jwt.MapClaims{"sub": "alice", "aud": "otel-query"}
	if _, err := v.Verify(ctx, is.Mint(good)); err != nil {
		t.Fatalf("good token: %v", err)
	}
	forger, _ := rsa.GenerateKey(rand.Reader, 2048)
	bad := map[string]string{
		"wrong audience":  is.Mint(jwt.MapClaims{"sub": "alice", "aud": "other"}),
		"wrong issuer":    is.Mint(jwt.MapClaims{"sub": "alice", "aud": "otel-query", "iss": "https://evil"}),
		"expired":         is.Mint(jwt.MapClaims{"sub": "alice", "aud": "otel-query", "exp": time.Now().Add(-time.Hour).Unix()}),
		"no exp":          is.Mint(jwt.MapClaims{"sub": "alice", "aud": "otel-query", "exp": nil}),
		"not yet valid":   is.Mint(jwt.MapClaims{"sub": "alice", "aud": "otel-query", "nbf": time.Now().Add(time.Hour).Unix()}),
		"forged, our kid": authtest.MintWith(forger, "k1", jwt.SigningMethodRS256, jwt.MapClaims{"sub": "alice", "aud": "otel-query", "iss": is.URL, "exp": time.Now().Add(time.Minute).Unix()}),
		"HS256 with the public key as secret": func() string {
			tk := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "alice", "aud": "otel-query", "iss": is.URL, "exp": time.Now().Add(time.Minute).Unix()})
			tk.Header["kid"] = "k1"
			s, _ := tk.SignedString([]byte("k1"))
			return s
		}(),
		"alg none": "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJhbGljZSJ9.",
		"garbage":  "not.a.token",
	}
	for name, tok := range bad {
		if _, err := v.Verify(ctx, tok); !errors.Is(err, auth.ErrUnauthenticated) {
			t.Errorf("%s: want ErrUnauthenticated, got %v", name, err)
		}
	}
}

func TestKeyRotationRefetchesOnUnknownKid(t *testing.T) {
	is := authtest.New()
	defer is.Close()
	v, _ := auth.NewVerifier(auth.OIDCConfig{Issuer: is.URL, Audience: "a", MinRefreshS: 1}, nil)
	ctx := context.Background()
	if _, err := v.Verify(ctx, is.Mint(jwt.MapClaims{"sub": "s", "aud": "a"})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	is.Rotate("k2")
	if _, err := v.Verify(ctx, is.Mint(jwt.MapClaims{"sub": "s", "aud": "a"})); err != nil {
		t.Fatalf("after rotation: %v", err)
	}
	if is.JWKSHits != 2 {
		t.Fatalf("want 2 key-set fetches, got %d", is.JWKSHits)
	}
}

func TestMappingDeniesByDefault(t *testing.T) {
	m := auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups",
		Groups: map[string]auth.Grant{
			"sre":    {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{"query", "plan"}},
			"team-a": {Clusters: []string{"prod-a"}, Namespaces: []string{"shop"}, Roles: []string{"query"}},
		}}
	p, err := m.Principal(jwt.MapClaims{"sub": "bob"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Roles) != 0 || len(p.Clusters) != 0 || p.AllClusters || p.AllNamespaces {
		t.Fatalf("no claims must grant nothing: %+v", p)
	}
	p, _ = m.Principal(jwt.MapClaims{"sub": "carol", "groups": []any{"team-a", "unknown"}, "roles": "admin,plan"})
	if strings.Join(p.Roles, ",") != "plan,query" || strings.Join(p.Clusters, ",") != "prod-a" || strings.Join(p.Namespaces, ",") != "shop" {
		t.Fatalf("team-a: %+v", p)
	}
	if p.MayCluster("prod-b") || !p.MayCluster("prod-a") {
		t.Fatal("MayCluster")
	}
	p, _ = m.Principal(jwt.MapClaims{"sub": "sam", "groups": "sre"})
	if !p.AllClusters || !p.AllNamespaces || !p.Has("plan") {
		t.Fatalf("sre: %+v", p)
	}
	if _, err := m.Principal(jwt.MapClaims{"groups": "sre"}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("a token without a subject must be refused")
	}
}

func TestBearer(t *testing.T) {
	if tok, ok := auth.Bearer("Bearer abc"); !ok || tok != "abc" {
		t.Fatal(tok)
	}
	for _, h := range []string{"", "Basic abc", "Bearer", "bearer "} {
		if _, ok := auth.Bearer(h); ok {
			t.Errorf("%q accepted", h)
		}
	}
}
