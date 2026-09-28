package hdxadapter

// The basis (D30) for HyperDX: every panel of one dashboard load reads at
// one basis, so the panels agree with each other and a refresh is the only
// thing that moves them.
//
// The fork sends one of two request headers:
//
//   - X-Otel-Basis: a basis token (or "latest"), passed to the service as
//     the statement's basis;
//   - X-Otel-Basis-Group: an id the fork makes per dashboard load or
//     refresh. The first statement of a group asks the service for a basis
//     (POST /v1/basis, the caller's whole scope, every signal) and the
//     adapter keeps it for the group, keyed by the caller's token too, so
//     every statement of the refresh (they run concurrently) reads at that
//     one basis. A basis the service stops accepting (a restart with a
//     per-process key, retention) is re-minted once.
//
// Every answer says which basis it was computed at: X-Otel-Basis (the
// token), X-Otel-At-Basis (true/false) and X-Otel-Basis-Info (per cluster:
// "cluster<RFC 3339" — rows received before it).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	basisTokenRE = regexp.MustCompile(`^(latest|b1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{1,100})$`) // and at most 16 KiB (basis.MaxTokenBytes)
	basisGroupRE = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)
)

// basisGroups caches one basis per (caller token, group).
type basisGroups struct {
	mu      sync.Mutex
	entries map[string]*groupEntry
	ttl     time.Duration
	max     int
	now     func() time.Time
}

type groupEntry struct {
	once  chan struct{} // closed when minted
	token string
	err   error
	at    time.Time
}

func (g *basisGroups) key(token, group string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:16]) + "/" + group
}

// get returns the group's basis, minting it once (concurrent callers of one
// group wait for the same mint).
func (g *basisGroups) get(ctx context.Context, token, group string, mint func(context.Context) (string, error)) (string, error) {
	k := g.key(token, group)
	g.mu.Lock()
	now := g.now()
	e := g.entries[k]
	if e != nil && now.Sub(e.at) > g.ttl {
		delete(g.entries, k)
		e = nil
	}
	if e == nil {
		if len(g.entries) >= g.max {
			for kk, x := range g.entries { // the expired
				if now.Sub(x.at) > g.ttl {
					delete(g.entries, kk)
				}
			}
			for kk := range g.entries { // still full: any
				if len(g.entries) < g.max {
					break
				}
				delete(g.entries, kk)
			}
		}
		e = &groupEntry{once: make(chan struct{}), at: now}
		g.entries[k] = e
		g.mu.Unlock()
		e.token, e.err = mint(ctx)
		if e.err != nil {
			g.mu.Lock()
			delete(g.entries, k) // a failed mint is not kept
			g.mu.Unlock()
		}
		close(e.once)
		return e.token, e.err
	}
	g.mu.Unlock()
	select {
	case <-e.once:
		return e.token, e.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// drop forgets a group's basis (the service refused it).
func (g *basisGroups) drop(token, group string) {
	g.mu.Lock()
	delete(g.entries, g.key(token, group))
	g.mu.Unlock()
}

// basisURL is the service's /v1/basis beside its /v1/query.
func (a *Adapter) basisURL() string {
	if a.cfg.BasisURL != "" {
		return a.cfg.BasisURL
	}
	return strings.TrimSuffix(a.cfg.QueryURL, "/v1/query") + "/v1/basis"
}

// mintBasis asks the service for a basis for the caller's whole scope and
// every signal (valid for any panel's tables).
func (a *Adapter) mintBasis(ctx context.Context, token string) (string, error) {
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, a.basisURL(), bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return "", refuse(502, 210, "NETWORK_ERROR", "service", "%v", err)
	}
	hr.Header.Set("Authorization", "Bearer "+token)
	hr.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(hr)
	if err != nil {
		return "", refuse(502, 210, "NETWORK_ERROR", "service_unreachable", "the query service did not answer: %v", err)
	}
	defer resp.Body.Close()
	var ans serviceAnswer
	if err := json.NewDecoder(resp.Body).Decode(&ans); err != nil {
		return "", refuse(502, 210, "NETWORK_ERROR", "bad_answer", "the query service's basis answer (HTTP %d) is not JSON: %v", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", serviceError(resp.StatusCode, ans)
	}
	if ans.Basis == nil || !basisTokenRE.MatchString(*ans.Basis) {
		return "", refuse(502, 210, "NETWORK_ERROR", "bad_answer", "the query service answered no basis")
	}
	return *ans.Basis, nil
}

// requestBasis is the basis a request asks for: an explicit token, a
// group's, or none. group is set when it came from a group (so a refusal
// can re-mint it).
func (a *Adapter) requestBasis(ctx context.Context, r *http.Request, token string) (basis, group string, err error) {
	if v := strings.TrimSpace(r.Header.Get("X-Otel-Basis")); v != "" {
		if len(v) > 16<<10 || !basisTokenRE.MatchString(v) {
			return "", "", refuse(400, 36, "BAD_ARGUMENTS", "basis_invalid", "X-Otel-Basis is not a basis token")
		}
		return v, "", nil
	}
	g := strings.TrimSpace(r.Header.Get("X-Otel-Basis-Group"))
	if g == "" {
		return "", "", nil
	}
	if !basisGroupRE.MatchString(g) {
		return "", "", refuse(400, 36, "BAD_ARGUMENTS", "basis_group", "X-Otel-Basis-Group must match %s", basisGroupRE)
	}
	b, err := a.groups.get(ctx, token, g, func(ctx context.Context) (string, error) { return a.mintBasis(ctx, token) })
	if err != nil {
		return "", "", err
	}
	return b, g, nil
}

// retryableBasis: a group's basis the service refused for a reason a new
// basis fixes.
func retryableBasis(err error) bool {
	e, ok := err.(*Error)
	return ok && (e.Reason == "basis_invalid" || e.Reason == "basis_expired")
}

// basisHeaders writes which basis an answer was computed at.
func basisHeaders(h http.Header, resp *serviceAnswer) {
	if resp.Basis == nil {
		h.Set("X-Otel-At-Basis", "false")
		return
	}
	h.Set("X-Otel-Basis", *resp.Basis)
	h.Set("X-Otel-At-Basis", map[bool]string{true: "true", false: "false"}[resp.AtBasis])
	if resp.BasisInfo != nil {
		var parts []string
		for _, c := range resp.BasisInfo.Clusters {
			parts = append(parts, c.Cluster+"<"+c.ReceivedBefore)
		}
		h.Set("X-Otel-Basis-Info", headerSafe(strings.Join(parts, "; ")))
	}
}
