package qclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// TokenSource hands out the evaluator's bearer token for one identity.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
	// Invalidate drops a cached token (after a 401).
	Invalidate()
}

// Identity configures one service identity. Exactly one of TokenFile and
// TokenURL is set.
type Identity struct {
	// TokenFile is re-read when it changes (a projected Kubernetes service
	// account token, or one a sidecar refreshes).
	TokenFile string `yaml:"token_file" json:"token_file"`
	// TokenURL: OAuth 2.0 client credentials (RFC 6749 §4.4) at the IdP.
	TokenURL        string `yaml:"token_url" json:"token_url"`
	ClientID        string `yaml:"client_id" json:"client_id"`
	ClientSecretEnv string `yaml:"client_secret_env" json:"client_secret_env"` // the secret's environment variable
	Scope           string `yaml:"scope" json:"scope"`
	Audience        string `yaml:"audience" json:"audience"`
}

// NewTokenSource builds the source an identity describes.
func NewTokenSource(id Identity, hc *http.Client) (TokenSource, error) {
	switch {
	case id.TokenFile != "" && id.TokenURL == "":
		return &fileToken{path: id.TokenFile}, nil
	case id.TokenURL != "" && id.TokenFile == "":
		sec := os.Getenv(id.ClientSecretEnv)
		if id.ClientID == "" || sec == "" {
			return nil, fmt.Errorf("identity with token_url needs client_id and a secret in $%s", id.ClientSecretEnv)
		}
		if hc == nil {
			hc = &http.Client{Timeout: 10 * time.Second}
		}
		return &clientCreds{id: id, secret: sec, hc: hc, now: time.Now}, nil
	}
	return nil, errors.New("identity: set exactly one of token_file and token_url")
}

type fileToken struct {
	path string
	mu   sync.Mutex
	tok  string
	mod  time.Time
	size int64
}

func (f *fileToken) Token(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, err := os.Stat(f.path)
	if err != nil {
		return "", err
	}
	if f.tok != "" && st.ModTime().Equal(f.mod) && st.Size() == f.size {
		return f.tok, nil
	}
	b, err := os.ReadFile(f.path)
	if err != nil {
		return "", err
	}
	t := strings.TrimSpace(string(b))
	if t == "" {
		return "", fmt.Errorf("%s is empty", f.path)
	}
	f.tok, f.mod, f.size = t, st.ModTime(), st.Size()
	return t, nil
}

func (f *fileToken) Invalidate() {
	f.mu.Lock()
	f.tok = ""
	f.mu.Unlock()
}

type clientCreds struct {
	id     Identity
	secret string
	hc     *http.Client
	now    func() time.Time

	mu  sync.Mutex
	tok string
	exp time.Time
}

func (c *clientCreds) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tok != "" && c.now().Before(c.exp) {
		return c.tok, nil
	}
	form := url.Values{"grant_type": {"client_credentials"}}
	if c.id.Scope != "" {
		form.Set("scope", c.id.Scope)
	}
	if c.id.Audience != "" {
		form.Set("audience", c.id.Audience)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.id.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(url.QueryEscape(c.id.ClientID), url.QueryEscape(c.secret))
	resp, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("token endpoint: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint: HTTP %d: %.200s", resp.StatusCode, b)
	}
	var out struct {
		AccessToken string  `json:"access_token"`
		ExpiresIn   float64 `json:"expires_in"`
	}
	if err := json.Unmarshal(b, &out); err != nil || out.AccessToken == "" {
		return "", fmt.Errorf("token endpoint: no access_token (%v)", err)
	}
	life := time.Duration(out.ExpiresIn * float64(time.Second))
	if exp, ok := jwtExp(out.AccessToken); ok && (life <= 0 || time.Until(exp) < life) {
		life = exp.Sub(c.now())
	}
	if life <= 0 {
		life = time.Minute
	}
	// renew at 80% of the lifetime, so a token never expires mid-flight
	c.tok, c.exp = out.AccessToken, c.now().Add(life*4/5)
	return c.tok, nil
}

func (c *clientCreds) Invalidate() {
	c.mu.Lock()
	c.tok = ""
	c.mu.Unlock()
}

// jwtExp reads exp from a JWT's payload without verifying it (the query
// service verifies; this only schedules renewal).
func jwtExp(tok string) (time.Time, bool) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var c struct {
		Exp float64 `json:"exp"`
	}
	if json.Unmarshal(b, &c) != nil || c.Exp == 0 {
		return time.Time{}, false
	}
	return time.Unix(int64(c.Exp), 0), true
}
