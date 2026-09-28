package basis

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Signer computes and checks basis MACs under the key ids it holds (D30
// amendment of 2026-09-28). Two exist: the static Keyring (a shared secret:
// on-prem, Nutanix, tests) and KMSSigner (an AWS KMS HMAC_SHA_256 key the
// service's role may use and nobody can read). A token names its key id in
// its payload; the kid spaces are disjoint (a static kid never contains
// ':', a KMS kid is always "k:" + a configured id), so both kinds coexist
// in one deployment during a migration.
type Signer interface {
	// Kind is "static" or "kms" (logs, metrics).
	Kind() string
	// Current is the kid this signer mints under ("" when it only verifies).
	Current() string
	// Holds: kid names one of this signer's configured keys. No I/O: a
	// token naming any other kid is refused before any call is made.
	Holds(kid string) bool
	// MAC is msg's MAC under kid.
	MAC(ctx context.Context, kid string, msg []byte) ([]byte, error)
	// Verify returns nil when mac is msg's MAC under kid, ErrMismatch when
	// it is not, and any other error when the signer could not decide.
	Verify(ctx context.Context, kid string, msg, mac []byte) error
}

// ErrMismatch: the MAC is not the message's under the key (a Signer's
// Verify; never cached for long).
var ErrMismatch = errors.New("basis: integrity check failed")

// ErrUnavailable is every reason a signer could not mint or decide
// (network, throttling, a disabled key, a denied permission): the request
// that needed it is refused with 503 basis_signer_unavailable, and a token
// that could not be checked is never treated as valid.
var ErrUnavailable = errors.New("basis_signer_unavailable")

// ReasonSignerUnavailable is ErrUnavailable's refusal reason (503).
const ReasonSignerUnavailable = "basis_signer_unavailable"

// ---- the static keyring as a Signer ------------------------------------------

// Kind implements Signer.
func (k *Keyring) Kind() string { return "static" }

// Current implements Signer.
func (k *Keyring) Current() string { return k.current }

// Holds implements Signer.
func (k *Keyring) Holds(kid string) bool { _, ok := k.keys[kid]; return ok }

// MAC implements Signer.
func (k *Keyring) MAC(_ context.Context, kid string, msg []byte) ([]byte, error) {
	if !k.Holds(kid) {
		return nil, fmt.Errorf("basis: no static key %q", kid)
	}
	return k.mac(kid, msg), nil
}

// Verify implements Signer (constant time).
func (k *Keyring) Verify(_ context.Context, kid string, msg, mac []byte) error {
	if !k.Holds(kid) {
		return ErrMismatch
	}
	if !hmacEqual(mac, k.mac(kid, msg)) {
		return ErrMismatch
	}
	return nil
}

// Mint is Encode (a static key never fails); with Bases' signature.
func (k *Keyring) Mint(_ context.Context, b *Basis) (string, error) {
	tok, err := k.Encode(*b)
	if err == nil {
		b.Version, b.Kid = Version, k.current
	}
	return tok, err
}

// Open is Decode, with Bases' signature.
func (k *Keyring) Open(_ context.Context, tok string) (*Basis, error) { return k.Decode(tok) }

// ---- Bases: minting and verifying over signers, with caches -------------------

// Options are Bases' caches.
type Options struct {
	// VerifyTTL: a token whose MAC verified is not re-verified for this
	// long (tokens are immutable, so a verified MAC stays verified; the TTL
	// only bounds how long a key taken out of the configuration keeps
	// working in a running process). Default 10 min; negative disables.
	VerifyTTL time.Duration
	// MismatchTTL: a token whose MAC failed is refused without a signer
	// call for this long (brief: it only blunts a client retrying one bad
	// token). Default 10 s; negative disables. A signer that could not
	// decide is never cached.
	MismatchTTL time.Duration
	// CacheSize bounds each cache (entries; default 10,000).
	CacheSize int
	// MintReuse: a mint whose bounds, signals and max_lateness equal one
	// minted less than this long ago returns that token (and its
	// issued_ns). Every plain answer names its scope's current basis, so
	// without it a KMS signer is called once per answer. 0 disables.
	MintReuse time.Duration
	// Observe, if set, is told each signer call: op "mint" or "verify",
	// result "ok", "mismatch" or "error".
	Observe func(op, result string)
	// Now is the clock (default time.Now).
	Now func() time.Time
}

// Bases mints with one signer's current key and verifies with every
// configured signer, caching verified tokens.
type Bases struct {
	mint    Signer
	signers []Signer
	o       Options
	ok      *ttlLRU[struct{}]
	bad     *ttlLRU[struct{}]
	minted  *ttlLRU[minted]
}

type minted struct {
	tok      string
	issuedNs int64
}

// New: mint with mint's current key; verify with mint and verifyOnly.
// Kids must not be held by two signers.
func New(mint Signer, verifyOnly []Signer, o Options) (*Bases, error) {
	if mint == nil || mint.Current() == "" {
		return nil, errors.New("basis: no signer to mint with")
	}
	if o.VerifyTTL == 0 {
		o.VerifyTTL = 10 * time.Minute
	}
	if o.MismatchTTL == 0 {
		o.MismatchTTL = 10 * time.Second
	}
	if o.CacheSize <= 0 {
		o.CacheSize = 10000
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	b := &Bases{mint: mint, signers: append([]Signer{mint}, verifyOnly...), o: o,
		ok: newTTLLRU[struct{}](o.CacheSize), bad: newTTLLRU[struct{}](o.CacheSize), minted: newTTLLRU[minted](o.CacheSize)}
	for i, s := range b.signers {
		if s == nil {
			return nil, errors.New("basis: nil signer")
		}
		for _, t := range b.signers[:i] {
			if t == s {
				return nil, errors.New("basis: a signer is configured twice")
			}
		}
	}
	return b, nil
}

// Current is the kid new bases are minted under.
func (bs *Bases) Current() string { return bs.mint.Current() }

// MintSigner is the minting signer's kind.
func (bs *Bases) MintSigner() string { return bs.mint.Kind() }

func (bs *Bases) observe(op, result string) {
	if bs.o.Observe != nil {
		bs.o.Observe(op, result)
	}
}

// Mint mints a token for *b under the current key and sets b's Version,
// Kid (and IssuedNs, when a recent equal mint is reused). A signer that
// cannot mint is ErrUnavailable.
func (bs *Bases) Mint(ctx context.Context, b *Basis) (string, error) {
	if err := check(b); err != nil {
		return "", err
	}
	kid := bs.mint.Current()
	now := bs.o.Now()
	var reuseKey string
	if bs.o.MintReuse > 0 {
		k, err := payload(kid, Basis{Clusters: b.Clusters, Signals: b.Signals, MaxLatenessNs: b.MaxLatenessNs})
		if err != nil {
			return "", err
		}
		reuseKey = k
		if m, ok := bs.minted.get(reuseKey, now); ok {
			b.Version, b.Kid, b.IssuedNs = Version, kid, m.issuedNs
			return m.tok, nil
		}
	}
	p, err := payload(kid, *b)
	if err != nil {
		return "", err
	}
	mac, err := bs.mint.MAC(ctx, kid, []byte(prefix+p))
	if err != nil {
		bs.observe("mint", "error")
		return "", fmt.Errorf("%w: minting under %s: %v", ErrUnavailable, kid, err)
	}
	bs.observe("mint", "ok")
	tok := prefix + p + "." + base64.RawURLEncoding.EncodeToString(mac)
	b.Version, b.Kid = Version, kid
	if bs.o.VerifyTTL > 0 {
		// minted here: known good when it comes back (a dashboard's panels)
		sum := sha256.Sum256([]byte(tok))
		bs.ok.put(string(sum[:]), struct{}{}, now.Add(bs.o.VerifyTTL))
	}
	if reuseKey != "" {
		bs.minted.put(reuseKey, minted{tok: tok, issuedNs: b.IssuedNs}, now.Add(bs.o.MintReuse))
	}
	return tok, nil
}

// Open verifies tok's MAC under the key it names (a configured one, or it
// is refused without any signer call) and returns the basis. ErrInvalid:
// not a token this service issued; ErrUnavailable: the signer could not
// decide (never treated as valid).
func (bs *Bases) Open(ctx context.Context, tok string) (*Basis, error) {
	pl, mac, b, err := parse(tok)
	if err != nil {
		return nil, err
	}
	var s Signer
	for _, x := range bs.signers {
		if x.Holds(b.Kid) {
			s = x
			break
		}
	}
	if s == nil {
		if strings.HasPrefix(b.Kid, kmsKidPrefix) {
			return nil, invalid("key %q is not a configured KMS key (a token never chooses the key it is checked with)", b.Kid)
		}
		return nil, invalid("unknown key %q (rotated out, or another deployment's)", b.Kid)
	}
	sum := sha256.Sum256([]byte(tok))
	h := string(sum[:])
	now := bs.o.Now()
	if _, ok := bs.ok.get(h, now); ok {
		return b, nil
	}
	if _, ok := bs.bad.get(h, now); ok {
		return nil, invalid("integrity check failed")
	}
	switch err := s.Verify(ctx, b.Kid, []byte(prefix+pl), mac); {
	case err == nil:
		bs.observe("verify", "ok")
		if bs.o.VerifyTTL > 0 {
			bs.ok.put(h, struct{}{}, now.Add(bs.o.VerifyTTL))
		}
		return b, nil
	case errors.Is(err, ErrMismatch):
		bs.observe("verify", "mismatch")
		if bs.o.MismatchTTL > 0 {
			bs.bad.put(h, struct{}{}, now.Add(bs.o.MismatchTTL))
		}
		return nil, invalid("integrity check failed")
	default:
		bs.observe("verify", "error")
		return nil, fmt.Errorf("%w: verifying under %s: %v", ErrUnavailable, b.Kid, err)
	}
}

// payload is b's wire payload (base64url JSON) under kid.
func payload(kid string, b Basis) (string, error) {
	w := wire{V: Version, K: kid, T: b.IssuedNs, C: b.Clusters, S: b.Signals, L: b.MaxLatenessNs}
	if w.S == nil {
		w.S = []string{"*"}
	}
	body, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(body), nil
}

// parse splits and decodes tok without checking its MAC: the payload, the
// MAC, and the basis it claims (to be trusted only once the MAC verifies).
// Everything checkable without a key is checked here, so garbage never
// costs a signer call.
func parse(tok string) (string, []byte, *Basis, error) {
	if len(tok) > MaxTokenBytes {
		return "", nil, nil, invalid("the token is longer than %d bytes", MaxTokenBytes)
	}
	if !strings.HasPrefix(tok, prefix) {
		return "", nil, nil, invalid("not a version-%d basis token", Version)
	}
	pl, sig, ok := strings.Cut(tok[len(prefix):], ".")
	if !ok || strings.Contains(sig, ".") {
		return "", nil, nil, invalid("malformed token")
	}
	body, err1 := base64.RawURLEncoding.DecodeString(pl)
	mac, err2 := base64.RawURLEncoding.DecodeString(sig)
	if err1 != nil || err2 != nil || len(mac) == 0 {
		return "", nil, nil, invalid("malformed token encoding")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var w wire
	if err := dec.Decode(&w); err != nil {
		return "", nil, nil, invalid("payload: %v", err)
	}
	if w.V != Version {
		return "", nil, nil, invalid("version %d", w.V)
	}
	if w.K == "" {
		return "", nil, nil, invalid("no key id")
	}
	b := &Basis{Version: w.V, Kid: w.K, IssuedNs: w.T, Clusters: w.C, Signals: w.S, MaxLatenessNs: w.L}
	if len(b.Signals) == 1 && b.Signals[0] == "*" {
		b.Signals = nil
	}
	if err := check(b); err != nil {
		return "", nil, nil, invalid("%v", err)
	}
	return pl, mac, b, nil
}

// ---- a bounded LRU with per-entry expiry ---------------------------------------

type ttlLRU[V any] struct {
	mu  sync.Mutex
	max int
	ll  *list.List
	m   map[string]*list.Element
}

type ttlEntry[V any] struct {
	k   string
	v   V
	exp time.Time
}

func newTTLLRU[V any](max int) *ttlLRU[V] {
	return &ttlLRU[V]{max: max, ll: list.New(), m: map[string]*list.Element{}}
}

func (c *ttlLRU[V]) get(k string, now time.Time) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	e, ok := c.m[k]
	if !ok {
		return zero, false
	}
	en := e.Value.(*ttlEntry[V])
	if !now.Before(en.exp) {
		c.ll.Remove(e)
		delete(c.m, k)
		return zero, false
	}
	c.ll.MoveToFront(e)
	return en.v, true
}

func (c *ttlLRU[V]) put(k string, v V, exp time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[k]; ok {
		e.Value = &ttlEntry[V]{k: k, v: v, exp: exp}
		c.ll.MoveToFront(e)
		return
	}
	c.m[k] = c.ll.PushFront(&ttlEntry[V]{k: k, v: v, exp: exp})
	for c.ll.Len() > c.max {
		old := c.ll.Back()
		c.ll.Remove(old)
		delete(c.m, old.Value.(*ttlEntry[V]).k)
	}
}

func (c *ttlLRU[V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
