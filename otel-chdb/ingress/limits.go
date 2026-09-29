package ingress

import (
	"errors"
	"math"
	"sync"
	"time"
)

// Limits are per principal (tenant id + object id), never per group or
// namespace: a team's budget is not a shared pool one member can drain for
// the others (CAST 38), and the key is not the one the grants use (CAST 36).
type Limits struct {
	// MaxBodyBytes caps the request body as sent (compressed); a larger
	// body is refused with 413 before it is read.
	MaxBodyBytes int64 `json:"max_body_bytes"`
	// MaxDecodedBytes caps the body after decompression (a gzip bomb
	// stops here), and must be at least MaxBodyBytes.
	MaxDecodedBytes int64 `json:"max_decoded_bytes"`
	// MaxItems caps spans or log records per request.
	MaxItems int `json:"max_items"`
	// RequestsPerMinute and DecodedBytesPerMinute are token buckets per
	// principal, with a burst of one minute's worth.
	RequestsPerMinute     float64 `json:"requests_per_minute"`
	DecodedBytesPerMinute float64 `json:"decoded_bytes_per_minute"`
	// MaxPrincipals bounds the table (idle full buckets are dropped first).
	MaxPrincipals int `json:"max_principals"`
}

// DefaultLimits: a developer's agent session sends a few requests a
// minute; the offload cap (D36) is 8 MiB per value.
func DefaultLimits() Limits {
	return Limits{MaxBodyBytes: 16 << 20, MaxDecodedBytes: 64 << 20, MaxItems: 10_000,
		RequestsPerMinute: 120, DecodedBytesPerMinute: 256 << 20, MaxPrincipals: 100_000}
}

// Validate checks the limits together.
func (l Limits) Validate() error {
	if l.MaxBodyBytes <= 0 || l.MaxDecodedBytes < l.MaxBodyBytes || l.MaxItems <= 0 ||
		l.RequestsPerMinute <= 0 || l.DecodedBytesPerMinute < float64(l.MaxDecodedBytes) || l.MaxPrincipals <= 0 {
		return errors.New("limits: want max_body_bytes > 0, max_decoded_bytes >= max_body_bytes, max_items > 0, " +
			"requests_per_minute > 0, decoded_bytes_per_minute >= max_decoded_bytes (else one full request can never pass), max_principals > 0")
	}
	return nil
}

type bucket struct {
	reqs, bytes float64
	at          time.Time
}

// Limiter holds the buckets.
type Limiter struct {
	l   Limits
	now func() time.Time
	mu  sync.Mutex
	b   map[string]*bucket
}

// NewLimiter returns a limiter for l (validated by the caller).
func NewLimiter(l Limits, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{l: l, now: now, b: map[string]*bucket{}}
}

func (lm *Limiter) refill(b *bucket, now time.Time) {
	dt := now.Sub(b.at).Minutes()
	if dt > 0 {
		b.reqs = math.Min(lm.l.RequestsPerMinute, b.reqs+dt*lm.l.RequestsPerMinute)
		b.bytes = math.Min(lm.l.DecodedBytesPerMinute, b.bytes+dt*lm.l.DecodedBytesPerMinute)
		b.at = now
	}
}

func (lm *Limiter) get(key string, now time.Time) *bucket {
	b, ok := lm.b[key]
	if ok {
		lm.refill(b, now)
		return b
	}
	if len(lm.b) >= lm.l.MaxPrincipals {
		for k, x := range lm.b {
			lm.refill(x, now)
			if x.reqs >= lm.l.RequestsPerMinute && x.bytes >= lm.l.DecodedBytesPerMinute {
				delete(lm.b, k)
			}
		}
	}
	b = &bucket{reqs: lm.l.RequestsPerMinute, bytes: lm.l.DecodedBytesPerMinute, at: now}
	lm.b[key] = b
	return b
}

// Admit takes one request; it returns the wait before a retry can pass
// (0: admitted).
func (lm *Limiter) Admit(key string) time.Duration {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	b := lm.get(key, lm.now())
	if b.reqs >= 1 {
		b.reqs--
		return 0
	}
	return time.Duration((1 - b.reqs) / lm.l.RequestsPerMinute * float64(time.Minute))
}

// Charge takes n decoded bytes; it returns the wait (0: admitted). A
// request is charged after decoding, so its real size counts.
func (lm *Limiter) Charge(key string, n int64) time.Duration {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	b := lm.get(key, lm.now())
	if b.bytes >= float64(n) {
		b.bytes -= float64(n)
		return 0
	}
	return time.Duration((float64(n) - b.bytes) / lm.l.DecodedBytesPerMinute * float64(time.Minute))
}
