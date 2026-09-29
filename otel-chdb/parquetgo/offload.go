package parquetgo

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/zeebo/blake3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Content by reference: the edge's payload offloader (DECISIONS.md D36,
// ../FORMAT.md §2.3, ../research/langfuse.md §6.2), byte for byte the Rust
// edge's otap-rs/src/offload.rs; ../langfuse/testdata/offload_vectors.json
// holds the vectors both are tested against (offload_test.go).
//
// Every span attribute, span event attribute, log attribute and log body
// goes through Offloader.Value, which
//
//  1. redacts a value whose key is in RedactKeys (replaced by "", with a
//     redacted_from marker) before anything else;
//  2. offloads a value longer than Threshold, or non-empty under a key in
//     Keys: the value becomes a reference document, a JSON array of
//     "h:<32 hex>", and its content goes to the object's payload part
//     (payloads, on the first row that references it);
//  3. splits a value under a key in SplitKeys that is a JSON array (a
//     bounded, iterative scan) into one payload per element;
//  4. caps a value at MaxValue bytes (truncated on a UTF-8 boundary, marked).
//
// Markers follow the map's entries, in the order the values were met:
// otel.payload.<key>.bytes, .truncated_from, .elements, .redacted_from. A
// log body's key is "@body" and its markers go in the log's attributes.
//
// Hash (R-L1, R-L2, SEC-L5), BLAKE3-128 under a per-tenant, per-day key:
//
//	key  = BLAKE3.derive_key("otel-chdb payload v1", le64(len cluster) ‖ cluster ‖ le64(len ns) ‖ ns ‖ decimal(received_ns / 86400e9))
//	hash = BLAKE3.keyed(key, content)[:16]

// PayloadHashContext is the derive-key context (never change it).
const PayloadHashContext = "otel-chdb payload v1"

// BodyKey is a log body's key, for its markers; MarkerPrefix the markers'.
const (
	BodyKey      = "@body"
	MarkerPrefix = "otel.payload."
	nsPerDay     = 86_400_000_000_000
	// MaxRequestCap is the largest MaxRequestBytes (32-bit column offsets).
	MaxRequestCap = 1 << 30
)

// DefaultOffloadKeys: the GenAI content keys, offloaded whatever their size.
var DefaultOffloadKeys = []string{
	"gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.system_instructions", "gen_ai.tool.definitions",
	"gen_ai.tool.call.arguments", "gen_ai.tool.call.result", "gen_ai.prompt", "gen_ai.completion",
	"langfuse.observation.input", "langfuse.observation.output", "langfuse.trace.input", "langfuse.trace.output",
	"input.value", "output.value",
}

// DefaultSplitKeys: the keys whose JSON arrays are split per element.
var DefaultSplitKeys = []string{
	"gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.system_instructions",
	"langfuse.observation.input", "langfuse.observation.output",
}

// OffloadOptions is the offloader's policy (the Rust edge's `offload:`).
type OffloadOptions struct {
	Enabled          bool     `json:"enabled"`
	Threshold        int      `json:"threshold"`
	MaxValue         int      `json:"max_value"`
	MaxRequestBytes  int      `json:"max_request_bytes"`
	Keys             []string `json:"keys"`
	SplitKeys        []string `json:"split_keys"`
	RedactKeys       []string `json:"redact_keys"`
	SplitMaxDepth    int      `json:"split_max_depth"`
	SplitMaxElements int      `json:"split_max_elements"`
	CacheSize        int      `json:"cache_size"`
}

// DefaultOffloadOptions is the owner's policy: on, 2 KiB, 8 MiB; a 128 MiB request cap (the receivers' body limit).
func DefaultOffloadOptions() OffloadOptions {
	return OffloadOptions{
		Enabled: true, Threshold: 2 << 10, MaxValue: 8 << 20, MaxRequestBytes: 128 << 20,
		Keys: slices.Clone(DefaultOffloadKeys), SplitKeys: slices.Clone(DefaultSplitKeys), RedactKeys: []string{},
		SplitMaxDepth: 64, SplitMaxElements: 4096, CacheSize: 65536,
	}
}

// Validate checks the policy's values together (CAST 25).
func (o OffloadOptions) Validate() error {
	if !o.Enabled {
		return nil
	}
	var errs []string
	if o.Threshold <= 0 {
		errs = append(errs, "threshold must be > 0")
	}
	if o.Threshold >= o.MaxValue {
		errs = append(errs, fmt.Sprintf("threshold %d must be below max_value %d", o.Threshold, o.MaxValue))
	}
	if o.MaxValue > o.MaxRequestBytes {
		errs = append(errs, fmt.Sprintf("max_value %d must not exceed max_request_bytes %d", o.MaxValue, o.MaxRequestBytes))
	}
	if o.MaxRequestBytes > MaxRequestCap {
		errs = append(errs, fmt.Sprintf("max_request_bytes %d exceeds %d (32-bit column offsets)", o.MaxRequestBytes, MaxRequestCap))
	}
	if o.SplitMaxDepth <= 0 || o.SplitMaxDepth > 1024 {
		errs = append(errs, fmt.Sprintf("split_max_depth %d must be in 1..=1024", o.SplitMaxDepth))
	}
	if o.SplitMaxElements <= 0 {
		errs = append(errs, "split_max_elements must be > 0")
	}
	if o.CacheSize <= 0 {
		errs = append(errs, "cache_size must be > 0")
	}
	for _, k := range o.SplitKeys {
		if !slices.Contains(o.Keys, k) {
			errs = append(errs, fmt.Sprintf("split key %q is not in keys (a split applies to offloaded values only)", k))
		}
	}
	for _, k := range slices.Concat(o.Keys, o.SplitKeys, o.RedactKeys) {
		if k == "" || strings.HasPrefix(k, MarkerPrefix) {
			errs = append(errs, fmt.Sprintf("key %q: empty or a marker key", k))
		}
	}
	if len(errs) > 0 {
		return errors.New("offload: " + strings.Join(errs, "; "))
	}
	return nil
}

// TenantKey is the per-tenant, per-day hash key.
func TenantKey(cluster string, namespace []byte, receivedNs uint64) [32]byte {
	day := strconv.FormatUint(receivedNs/nsPerDay, 10)
	m := make([]byte, 0, len(cluster)+len(namespace)+len(day)+16)
	m = binary.LittleEndian.AppendUint64(m, uint64(len(cluster)))
	m = append(m, cluster...)
	m = binary.LittleEndian.AppendUint64(m, uint64(len(namespace)))
	m = append(m, namespace...)
	m = append(m, day...)
	var k [32]byte
	blake3.DeriveKey(PayloadHashContext, m, k[:])
	return k
}

// PayloadHash is a payload's hash under a tenant key.
func PayloadHash(key *[32]byte, content []byte) [16]byte {
	h, err := blake3.NewKeyed(key[:])
	if err != nil {
		panic(err) // a 32-byte key never fails
	}
	_, _ = h.Write(content)
	var out [16]byte
	copy(out[:], h.Sum(nil))
	return out
}

// TruncateAt is where a value over n bytes is cut: at n, backed off (at
// most three bytes) to the start of a UTF-8 sequence.
func TruncateAt(v []byte, n int) int {
	if len(v) <= n {
		return len(v)
	}
	cut := n
	for cut > 0 && cut+3 > n && v[cut]&0xC0 == 0x80 {
		cut--
	}
	return cut
}

func jsWS(v []byte, i int) int {
	for i < len(v) && (v[i] == ' ' || v[i] == '\t' || v[i] == '\n' || v[i] == '\r') {
		i++
	}
	return i
}

func isHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

func jsString(v []byte, i int) (int, bool) {
	i++
	for i < len(v) {
		switch c := v[i]; {
		case c == '"':
			return i + 1, true
		case c == '\\':
			if i+1 >= len(v) {
				return 0, false
			}
			switch v[i+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(v) {
					return 0, false
				}
				for _, h := range v[i+2 : i+6] {
					if !isHex(h) {
						return 0, false
					}
				}
				i += 6
			default:
				return 0, false
			}
		case c < 0x20:
			return 0, false
		default:
			i++
		}
	}
	return 0, false
}

func jsNumber(v []byte, i int) (int, bool) {
	digits := func(i int) (int, bool) {
		s := i
		for i < len(v) && v[i] >= '0' && v[i] <= '9' {
			i++
		}
		return i, i > s
	}
	var ok bool
	if i < len(v) && v[i] == '-' {
		i++
	}
	switch {
	case i < len(v) && v[i] == '0':
		i++
	case i < len(v) && v[i] >= '1' && v[i] <= '9':
		i, _ = digits(i)
	default:
		return 0, false
	}
	if i < len(v) && v[i] == '.' {
		if i, ok = digits(i + 1); !ok {
			return 0, false
		}
	}
	if i < len(v) && (v[i] == 'e' || v[i] == 'E') {
		i++
		if i < len(v) && (v[i] == '+' || v[i] == '-') {
			i++
		}
		if i, ok = digits(i); !ok {
			return 0, false
		}
	}
	return i, true
}

type jsState int

const (
	jsValue jsState = iota
	jsValueOrClose
	jsKeyOrClose
	jsKey
	jsColon
	jsAfter
)

// JSONArrayElements is the top-level elements of v when it is one JSON
// array (RFC 8259 over bytes; string contents are not checked for UTF-8), as
// byte ranges without surrounding whitespace; ok false when it is not an
// array, not valid, nested deeper than maxDepth containers, or has more than
// maxElements elements. Iterative: no recursion, whatever the input.
func JSONArrayElements(v []byte, maxDepth, maxElements int) (elems [][2]int, ok bool) {
	i := jsWS(v, 0)
	if i >= len(v) || v[i] != '[' {
		return nil, false
	}
	var stack []byte
	elems = [][2]int{}
	start := 0
	s := jsValue
	push := func(end int) bool {
		if len(elems) >= maxElements {
			return false
		}
		elems = append(elems, [2]int{start, end})
		return true
	}
	for {
		i = jsWS(v, i)
		switch s {
		case jsValue, jsValueOrClose:
			if i >= len(v) {
				return nil, false
			}
			c := v[i]
			if s == jsValueOrClose && c == ']' {
				stack = stack[:len(stack)-1]
				i++
				if len(stack) == 1 && !push(i) {
					return nil, false
				}
				s = jsAfter
				continue
			}
			if len(stack) == 1 {
				start = i
			}
			switch {
			case c == '[' || c == '{':
				if len(stack) >= maxDepth {
					return nil, false
				}
				stack = append(stack, c)
				i++
				if c == '[' {
					s = jsValueOrClose
				} else {
					s = jsKeyOrClose
				}
				continue
			case c == '"':
				if i, ok = jsString(v, i); !ok {
					return nil, false
				}
			case c == '-' || c >= '0' && c <= '9':
				if i, ok = jsNumber(v, i); !ok {
					return nil, false
				}
			case c == 't' || c == 'f' || c == 'n':
				lit := "null"
				if c == 't' {
					lit = "true"
				} else if c == 'f' {
					lit = "false"
				}
				if i+len(lit) > len(v) || string(v[i:i+len(lit)]) != lit {
					return nil, false
				}
				i += len(lit)
			default:
				return nil, false
			}
			if len(stack) == 1 && !push(i) {
				return nil, false
			}
			s = jsAfter
		case jsKeyOrClose, jsKey:
			if i >= len(v) {
				return nil, false
			}
			c := v[i]
			if s == jsKeyOrClose && c == '}' {
				stack = stack[:len(stack)-1]
				i++
				if len(stack) == 1 && !push(i) {
					return nil, false
				}
				s = jsAfter
				continue
			}
			if c != '"' {
				return nil, false
			}
			if i, ok = jsString(v, i); !ok {
				return nil, false
			}
			s = jsColon
		case jsColon:
			if i >= len(v) || v[i] != ':' {
				return nil, false
			}
			i++
			s = jsValue
		case jsAfter:
			if len(stack) == 0 {
				if i == len(v) {
					return elems, true
				}
				return nil, false
			}
			if i >= len(v) {
				return nil, false
			}
			c, top := v[i], stack[len(stack)-1]
			i++
			switch {
			case c == ',' && top == '[':
				s = jsValue
			case c == ',' && top == '{':
				s = jsKey
			case c == ']' && top == '[' || c == '}' && top == '{':
				stack = stack[:len(stack)-1]
				if len(stack) == 1 && !push(i) {
					return nil, false
				}
			default:
				return nil, false
			}
		}
	}
}

// OffloadStats are one offloader's (or the edge's) counters.
type OffloadStats struct {
	Offloaded, OffloadedBytes, Split, Truncated, Redacted, Refused, Carried, Dedup uint64
}

// Add sums o into s.
func (s *OffloadStats) Add(o OffloadStats) {
	s.Offloaded += o.Offloaded
	s.OffloadedBytes += o.OffloadedBytes
	s.Split += o.Split
	s.Truncated += o.Truncated
	s.Redacted += o.Redacted
	s.Refused += o.Refused
	s.Carried += o.Carried
	s.Dedup += o.Dedup
}

// Payload is one payload of an object: its hash, content and the first row
// (walk order) that references it.
type Payload struct {
	Hash     [16]byte
	Content  []byte
	FirstRow uint32
}

// OffloadPolicy is OffloadOptions compiled.
type OffloadPolicy struct {
	Opts                OffloadOptions
	keys, split, redact map[string]bool
}

// NewOffloadPolicy compiles opts.
func NewOffloadPolicy(opts OffloadOptions) *OffloadPolicy {
	set := func(v []string) map[string]bool {
		m := make(map[string]bool, len(v))
		for _, k := range v {
			m[k] = true
		}
		return m
	}
	return &OffloadPolicy{Opts: opts, keys: set(opts.Keys), split: set(opts.SplitKeys), redact: set(opts.RedactKeys)}
}

// Offloader is one object's offloading state: the tenant key of the
// resource being walked, the payloads and the current row's references.
type Offloader struct {
	Policy   *OffloadPolicy
	cluster  string
	received uint64
	key      [32]byte
	keysByNS map[string][32]byte
	// Payloads: the distinct payloads, in the order first referenced.
	Payloads []Payload
	index    map[[16]byte]int
	rowRefs  [][16]byte
	rowSeen  map[[16]byte]bool
	// Markers of the map being written.
	Markers [][2]string
	Stats   OffloadStats
	out     []byte
}

// NewOffloader starts an object's offloading for an edge of cluster and a
// request received at received (ns).
func NewOffloader(p *OffloadPolicy, cluster string, received uint64) *Offloader {
	return &Offloader{Policy: p, cluster: cluster, received: received, key: TenantKey(cluster, nil, received),
		keysByNS: map[string][32]byte{}, index: map[[16]byte]int{}, rowSeen: map[[16]byte]bool{}}
}

// Tenant sets the resource being walked: its covered k8s.namespace.name.
func (o *Offloader) Tenant(namespace string) {
	if k, ok := o.keysByNS[namespace]; ok {
		o.key = k
		return
	}
	k := TenantKey(o.cluster, []byte(namespace), o.received)
	o.keysByNS[namespace] = k
	o.key = k
}

// Candidate reports whether a value needs Value's slow path.
func (o *Offloader) Candidate(key string, n int) bool {
	return n > o.Policy.Opts.Threshold || o.Policy.keys[key] || o.Policy.redact[key]
}

// reset starts a new object (the walkers call it): no payloads, no
// references, no markers, zero counters; the tenant keys are kept.
func (o *Offloader) reset() {
	o.Payloads = o.Payloads[:0]
	clear(o.index)
	o.rowRefs = nil
	clear(o.rowSeen)
	o.Markers = o.Markers[:0]
	o.Stats = OffloadStats{}
}

// anyCandidate reports whether a map has a value for Value's slow path.
func (o *Offloader) anyCandidate(m pcommon.Map) bool {
	found := false
	m.Range(func(k string, v pcommon.Value) bool {
		n := 0
		if v.Type() == pcommon.ValueTypeStr {
			n = len(v.Str())
		} else {
			o.out = valueString(o.out[:0], v)
			n = len(o.out)
		}
		found = o.Candidate(k, n)
		return !found
	})
	return found
}

// Pending reports whether markers wait for the next map.
func (o *Offloader) Pending() bool { return len(o.Markers) > 0 }

func (o *Offloader) reference(content []byte, row uint32) [16]byte {
	h := PayloadHash(&o.key, content)
	if _, ok := o.index[h]; !ok {
		o.index[h] = len(o.Payloads)
		o.Payloads = append(o.Payloads, Payload{Hash: h, Content: slices.Clone(content), FirstRow: row})
	}
	if !o.rowSeen[h] {
		o.rowSeen[h] = true
		o.rowRefs = append(o.rowRefs, h)
	}
	return h
}

func (o *Offloader) marker(key, what string, n int) {
	o.Markers = append(o.Markers, [2]string{MarkerPrefix + key + "." + what, strconv.Itoa(n)})
}

// Value offloads one value of row under key: replaced false means store v
// as it is; otherwise store out (valid until the next call).
func (o *Offloader) Value(key string, v []byte, row uint32) (out []byte, replaced bool) {
	p := o.Policy
	if p.redact[key] {
		if len(v) == 0 {
			return nil, false
		}
		o.Stats.Redacted++
		o.marker(key, "redacted_from", len(v))
		return []byte{}, true
	}
	if !(len(v) > p.Opts.Threshold || (p.keys[key] && len(v) > 0)) {
		return nil, false
	}
	cut := TruncateAt(v, p.Opts.MaxValue)
	val := v[:cut]
	var elems [][2]int
	split := false
	if p.split[key] {
		elems, split = JSONArrayElements(val, p.Opts.SplitMaxDepth, p.Opts.SplitMaxElements)
	}
	sc := append(o.out[:0], '[')
	add := func(part []byte) {
		h := o.reference(part, row)
		if len(sc) > 1 {
			sc = append(sc, ',')
		}
		sc = append(sc, `"h:`...)
		sc = hex.AppendEncode(sc, h[:])
		sc = append(sc, '"')
	}
	if split {
		for _, e := range elems {
			add(val[e[0]:e[1]])
		}
	} else {
		add(val)
	}
	sc = append(sc, ']')
	o.out = sc
	o.Stats.Offloaded++
	o.Stats.OffloadedBytes += uint64(cut)
	o.marker(key, "bytes", cut)
	if cut < len(v) {
		o.Stats.Truncated++
		o.marker(key, "truncated_from", len(v))
	}
	if split {
		o.Stats.Split++
		o.marker(key, "elements", len(elems))
	}
	return sc, true
}

// EndRow ends the current row: its distinct references, in order (reset).
func (o *Offloader) EndRow() [][16]byte {
	r := o.rowRefs
	o.rowRefs = nil
	clear(o.rowSeen)
	return r
}

// PayloadCache is one writer lane's sent payloads for its current epoch
// (otap-rs offload.rs PayloadCache): a hash counts as sent once an object
// carrying it has committed; a new epoch starts empty; bounded (the least
// recently sent eighth goes first).
type PayloadCache struct {
	epoch   string
	m       map[[16]byte]uint64
	tick    uint64
	Evicted uint64
}

// Wants reports whether an object in epoch must carry payload h.
func (c *PayloadCache) Wants(epoch string, h [16]byte) bool {
	if c.epoch != epoch {
		return true
	}
	_, ok := c.m[h]
	return !ok
}

// Sent marks payloads sent in epoch (once the object carrying them committed).
func (c *PayloadCache) Sent(epoch string, hs [][16]byte, size int) {
	if c.epoch != epoch || c.m == nil {
		c.m = map[[16]byte]uint64{}
		c.epoch = epoch
	}
	for _, h := range hs {
		c.tick++
		c.m[h] = c.tick
	}
	size = max(size, 1)
	if len(c.m) > size {
		keep := size - size/8
		type u struct {
			tick uint64
			h    [16]byte
		}
		v := make([]u, 0, len(c.m))
		for h, t := range c.m {
			v = append(v, u{t, h})
		}
		sort.Slice(v, func(i, j int) bool { return v[i].tick < v[j].tick })
		for _, x := range v[:len(v)-keep] {
			delete(c.m, x.h)
			c.Evicted++
		}
	}
}

// Len is the number of payloads remembered.
func (c *PayloadCache) Len() int { return len(c.m) }
