// Package basis is D30's basis token: a named custody time at which an
// answer is computed (research/bitemporal.md §3).
//
// A basis is a vector {cluster: C_cluster} (or {"*": C} for the fleet),
// the signals it was taken for, and the max_lateness policy in force when
// it was issued. Each C_cluster is at or below the cluster's
// complete_through for those signals when the basis was issued (D29), so
// every row with received_at < C_cluster was in central then, and the rows
// of any statement restricted to received_at < C_cluster never change
// afterwards (until retention removes them). The comparison is STRICT: the
// consumer's promise is "every request with received_at < wm is ingested"
// (FORMAT.md §3); a pending object may carry received_at == wm exactly.
//
// The token is opaque to clients, versioned, and integrity-protected with
// an HMAC-SHA256 under a service key (kid-tagged, so keys rotate): a static
// shared key (Keyring) or an AWS KMS HMAC key (KMSSigner), both behind
// Signer and Bases (signer.go, kms.go; D30 amendment 2026-09-28). Why an
// HMAC and not a signature: only the query service mints and verifies
// bases (every replica holds the key); nobody else needs to verify one
// without asking the service, and the readable form (View) travels beside
// the token. Why protect it at all, when every use is re-checked against
// the caller's scope and the current watermark anyway (a forged basis can
// neither widen scope nor name an unsettled C)? So that a basis in an audit
// record, an alert's state or a dashboard URL is known to be one the
// service issued, with the max_lateness it was issued under: the label of
// an answer at a basis is reproducible, not the caller's choice. The key
// is therefore not a scope boundary; losing it lets a caller pick an
// arbitrary SETTLED C, which is an as-of read it could approximate anyway.
package basis

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

// Version is the token format this package writes.
const Version = 1

// Fleet is the cluster key of a fleet basis: one C for every cluster.
const Fleet = "*"

// prefix starts every version-1 token.
const prefix = "b1."

// MaxTokenBytes bounds a token a request may carry.
const MaxTokenBytes = 16 << 10

// Basis is one decoded (and verified) basis.
type Basis struct {
	Version  int
	Kid      string
	IssuedNs int64
	// Clusters: {cluster: C} (custody time, ns; rows with received_at < C),
	// or {Fleet: C}.
	Clusters map[string]uint64
	// Signals it was taken for; nil means every signal.
	Signals       []string
	MaxLatenessNs int64
}

// wire is the token's payload. Field names are short; encoding/json writes
// map keys sorted, so equal bases encode to equal tokens.
type wire struct {
	V int               `json:"v"`
	K string            `json:"k"`
	T int64             `json:"t"`
	C map[string]uint64 `json:"c"`
	S []string          `json:"s"`
	L int64             `json:"l"`
}

// IsFleet: one C for every cluster.
func (b *Basis) IsFleet() bool { _, ok := b.Clusters[Fleet]; return ok }

// C is cluster c's bound; ok false when the basis does not cover c.
func (b *Basis) C(c string) (uint64, bool) {
	if v, ok := b.Clusters[Fleet]; ok {
		return v, true
	}
	v, ok := b.Clusters[c]
	return v, ok
}

// ClusterNames are the named clusters, sorted (nil for a fleet basis).
func (b *Basis) ClusterNames() []string {
	if b.IsFleet() {
		return nil
	}
	out := make([]string, 0, len(b.Clusters))
	for c := range b.Clusters {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// MinFor is the lowest bound over clusters (nil: every cluster the basis
// names, or the fleet's).
func (b *Basis) MinFor(clusters []string) uint64 {
	m := uint64(math.MaxUint64)
	if clusters == nil || b.IsFleet() {
		for _, v := range b.Clusters {
			m = min(m, v)
		}
		return m
	}
	for _, c := range clusters {
		if v, ok := b.C(c); ok {
			m = min(m, v)
		}
	}
	return m
}

// CoversSignals: the basis was taken for every signal in sigs (nil sigs:
// every signal, which only an every-signal basis covers). A per-signal
// value is sound only for its signal's lanes; a basis taken for logs says
// nothing about traces.
func (b *Basis) CoversSignals(sigs []string) bool {
	if b.Signals == nil {
		return true
	}
	if sigs == nil {
		return false
	}
	for _, s := range sigs {
		if !slices.Contains(b.Signals, s) {
			return false
		}
	}
	return true
}

// Keyring holds the service's basis keys by kid; Current mints.
type Keyring struct {
	keys    map[string][]byte
	current string
}

// NewKeyring checks keys (at least 32 bytes each; kids are short names).
func NewKeyring(keys map[string][]byte, current string) (*Keyring, error) {
	if _, ok := keys[current]; !ok {
		return nil, fmt.Errorf("basis: current key %q is not in the keyring", current)
	}
	for k, v := range keys {
		if !kidRE.MatchString(k) {
			return nil, fmt.Errorf("basis: key id %q: want %s", k, kidRE)
		}
		if len(v) < 32 {
			return nil, fmt.Errorf("basis: key %q has %d bytes, want at least 32", k, len(v))
		}
	}
	return &Keyring{keys: keys, current: current}, nil
}

// Ephemeral is a keyring with one random key: bases it issues are refused
// by another replica and after a restart (basis_invalid). For tests and
// single-replica deployments without a configured key.
func Ephemeral() *Keyring {
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	id := "eph-" + hex.EncodeToString(k[:4])
	kr, _ := NewKeyring(map[string][]byte{id: k}, id)
	return kr
}

// ParseKeys reads "kid:base64,kid:base64" (the first is current unless
// current names another).
func ParseKeys(spec, current string) (*Keyring, error) {
	keys := map[string][]byte{}
	first := ""
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kid, b64, ok := strings.Cut(part, ":")
		if !ok {
			return nil, errors.New("basis keys: want kid:base64[,kid:base64…]")
		}
		v, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("basis key %q: %w", kid, err)
		}
		keys[kid] = v
		if first == "" {
			first = kid
		}
	}
	if current == "" {
		current = first
	}
	return NewKeyring(keys, current)
}

var (
	kidRE     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,32}$`)
	clusterRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)
	signalRE  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
)

// Encode mints a token for b under the current key (b.Kid and b.Version
// are set here).
func (k *Keyring) Encode(b Basis) (string, error) {
	if err := check(&b); err != nil {
		return "", err
	}
	p, err := payload(k.current, b)
	if err != nil {
		return "", err
	}
	return prefix + p + "." + base64.RawURLEncoding.EncodeToString(k.mac(k.current, []byte(prefix+p))), nil
}

// mac is HMAC-SHA256(key, msg); msg is "b1." + payload.
func (k *Keyring) mac(kid string, msg []byte) []byte {
	m := hmac.New(sha256.New, k.keys[kid])
	m.Write(msg)
	return m.Sum(nil)
}

func hmacEqual(a, b []byte) bool { return hmac.Equal(a, b) }

// ErrInvalid is every reason a token is not a basis this service issued.
var ErrInvalid = errors.New("basis_invalid")

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

// Decode verifies tok's MAC under the key it names and returns the basis.
func (k *Keyring) Decode(tok string) (*Basis, error) {
	// the key id is inside the payload: parse, then verify, then trust
	pl, mac, b, err := parse(tok)
	if err != nil {
		return nil, err
	}
	if !k.Holds(b.Kid) {
		return nil, invalid("unknown key %q (rotated out, or another deployment's)", b.Kid)
	}
	if !hmac.Equal(mac, k.mac(b.Kid, []byte(prefix+pl))) {
		return nil, invalid("integrity check failed")
	}
	return b, nil
}

// check is what every basis must be, minted or decoded.
func check(b *Basis) error {
	if len(b.Clusters) == 0 {
		return errors.New("no cluster")
	}
	if _, ok := b.Clusters[Fleet]; ok && len(b.Clusters) != 1 {
		return errors.New("a fleet basis names no other cluster")
	}
	for c, v := range b.Clusters {
		if c != Fleet && !clusterRE.MatchString(c) {
			return fmt.Errorf("cluster %q is not a cluster name", c)
		}
		if v > math.MaxInt64 {
			return fmt.Errorf("cluster %q: bound out of range", c)
		}
	}
	if b.Signals != nil {
		if len(b.Signals) == 0 {
			return errors.New("empty signal list")
		}
		for _, s := range b.Signals {
			if !signalRE.MatchString(s) {
				return fmt.Errorf("signal %q is not a lane namespace", s)
			}
		}
		if !slices.IsSorted(b.Signals) {
			return errors.New("signals not sorted")
		}
	}
	if b.MaxLatenessNs < 0 || b.IssuedNs < 0 {
		return errors.New("negative time")
	}
	return nil
}

// ---- the readable form -------------------------------------------------------

// View is what an answer shows beside the token.
type View struct {
	Version      int            `json:"version"`
	IssuedAt     string         `json:"issued_at"`
	IssuedNs     int64          `json:"issued_ns"`
	Clusters     []ClusterBound `json:"clusters"`
	Signals      []string       `json:"signals"` // ["*"]: every signal
	MaxLatenessS float64        `json:"max_lateness_s"`
	// Rule: what the basis means, in words.
	Rule string `json:"rule"`
}

// ClusterBound is one cluster's C.
type ClusterBound struct {
	Cluster          string `json:"cluster"` // "*": every cluster
	ReceivedBefore   string `json:"received_before"`
	ReceivedBeforeNs uint64 `json:"received_before_ns"`
}

// ViewRule is View.Rule.
const ViewRule = "An answer at this basis reads, per cluster, only rows with received_at < received_before; " +
	"each bound was at or below the cluster's complete_through when the basis was issued, so the answer " +
	"does not change while new data arrives (until retention removes rows)."

// View is b's readable form.
func (b *Basis) View() *View {
	v := &View{Version: b.Version, IssuedNs: b.IssuedNs, IssuedAt: fmtNs(b.IssuedNs), Signals: b.Signals,
		MaxLatenessS: time.Duration(b.MaxLatenessNs).Seconds(), Rule: ViewRule}
	if v.Signals == nil {
		v.Signals = []string{"*"}
	}
	names := make([]string, 0, len(b.Clusters))
	for c := range b.Clusters {
		names = append(names, c)
	}
	sort.Strings(names)
	for _, c := range names {
		v.Clusters = append(v.Clusters, ClusterBound{Cluster: c, ReceivedBeforeNs: b.Clusters[c], ReceivedBefore: fmtNs(int64(b.Clusters[c]))})
	}
	return v
}

func fmtNs(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }

// Answer is the basis block every /v1/query and /v1/plan answer carries.
type Answer struct {
	// Basis is the token: with AtBasis, the one the answer was computed at;
	// without, the current basis of the answer's scope, to pin later
	// requests to (this answer itself read up to now). Null when the
	// watermark is unknown.
	Basis     *string `json:"basis"`
	AtBasis   bool    `json:"at_basis"`
	BasisInfo *View   `json:"basis_info,omitempty"`
	// BasisFrom: a delta answer's lower bound (rows with basis_from's
	// C <= received_at < basis's C).
	BasisFrom     *string `json:"basis_from,omitempty"`
	BasisFromInfo *View   `json:"basis_from_info,omitempty"`
	// BasisUnavailable: why a plain answer names no basis although the
	// watermark is known (basis_signer_unavailable: the signer, e.g. KMS,
	// could not mint one). The answer itself is unaffected.
	BasisUnavailable string `json:"basis_unavailable,omitempty"`
}
