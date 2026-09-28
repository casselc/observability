package basis

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"pgregory.net/rapid"
)

// fakeKMS is KMS's HMAC API over in-memory keys: real HMAC-SHA256, the
// 4,096-byte message limit, KMSInvalidMacException on a mismatch, aliases,
// and an outage switch. It records every key it was asked to use.
type fakeKMS struct {
	mu      sync.Mutex
	secrets map[string][]byte // key ARN -> material
	aliases map[string]string // alias name or ARN -> key ARN
	spec    map[string]types.KeySpec
	down    bool
	calls   map[string]int // op -> count
	used    []string       // KeyIds passed
}

const (
	arnA = "arn:aws:kms:eu-west-1:111122223333:key/aaaaaaaa-1111-2222-3333-444444444444"
	arnB = "arn:aws:kms:eu-west-1:111122223333:key/bbbbbbbb-1111-2222-3333-444444444444"
	arnE = "arn:aws:kms:eu-west-1:111122223333:key/eeeeeeee-1111-2222-3333-444444444444" // exists, never configured
)

func newFakeKMS() *fakeKMS {
	f := &fakeKMS{secrets: map[string][]byte{}, aliases: map[string]string{}, spec: map[string]types.KeySpec{}, calls: map[string]int{}}
	for i, a := range []string{arnA, arnB, arnE} {
		f.secrets[a] = []byte(strings.Repeat(string(rune('p'+i)), 32))
		f.spec[a] = types.KeySpecHmac256
	}
	f.aliases["alias/qs-basis"] = arnA
	return f
}

func (f *fakeKMS) resolve(op string, id *string) (string, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[op]++
	f.used = append(f.used, aws.ToString(id))
	if f.down {
		return "", nil, &types.KMSInternalException{Message: aws.String("the fake is down")}
	}
	k := aws.ToString(id)
	if a, ok := f.aliases[k]; ok {
		k = a
	}
	s, ok := f.secrets[k]
	if !ok {
		return "", nil, &types.NotFoundException{Message: aws.String(k)}
	}
	return k, s, nil
}

func (f *fakeKMS) n(op string) int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls[op] }

func (f *fakeKMS) setDown(v bool) { f.mu.Lock(); f.down = v; f.mu.Unlock() }

func fmac(key, msg []byte) []byte { m := hmac.New(sha256.New, key); m.Write(msg); return m.Sum(nil) }

func (f *fakeKMS) GenerateMac(_ context.Context, in *kms.GenerateMacInput, _ ...func(*kms.Options)) (*kms.GenerateMacOutput, error) {
	_, s, err := f.resolve("GenerateMac", in.KeyId)
	if err != nil {
		return nil, err
	}
	if len(in.Message) == 0 || len(in.Message) > 4096 || in.MacAlgorithm != types.MacAlgorithmSpecHmacSha256 {
		return nil, errors.New("ValidationException: message or algorithm")
	}
	return &kms.GenerateMacOutput{Mac: fmac(s, in.Message), KeyId: in.KeyId, MacAlgorithm: in.MacAlgorithm}, nil
}

func (f *fakeKMS) VerifyMac(_ context.Context, in *kms.VerifyMacInput, _ ...func(*kms.Options)) (*kms.VerifyMacOutput, error) {
	_, s, err := f.resolve("VerifyMac", in.KeyId)
	if err != nil {
		return nil, err
	}
	if len(in.Message) == 0 || len(in.Message) > 4096 {
		return nil, errors.New("ValidationException: message")
	}
	if !hmac.Equal(in.Mac, fmac(s, in.Message)) {
		return nil, &types.KMSInvalidMacException{Message: aws.String("invalid mac")}
	}
	return &kms.VerifyMacOutput{MacValid: true, KeyId: in.KeyId}, nil
}

func (f *fakeKMS) DescribeKey(_ context.Context, in *kms.DescribeKeyInput, _ ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	arn, _, err := f.resolve("DescribeKey", in.KeyId)
	if err != nil {
		return nil, err
	}
	m := &types.KeyMetadata{Arn: aws.String(arn), KeySpec: f.spec[arn], Enabled: true, KeyState: types.KeyStateEnabled,
		KeyUsage: types.KeyUsageTypeGenerateVerifyMac, MacAlgorithms: []types.MacAlgorithmSpec{types.MacAlgorithmSpecHmacSha256}}
	if f.spec[arn] != types.KeySpecHmac256 {
		m.KeyUsage, m.MacAlgorithms = types.KeyUsageTypeEncryptDecrypt, nil
	}
	return &kms.DescribeKeyOutput{KeyMetadata: m}, nil
}

// clock is a settable test clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// replica is one service process: its own signer and caches over the
// shared fake KMS.
func replica(t testing.TB, f *fakeKMS, c *clock, keys []KMSKey, verifyOnly ...Signer) *Bases {
	t.Helper()
	ks, err := NewKMSSigner(f, keys, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := ks.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	o := Options{Now: c.now}
	bs, err := New(ks, verifyOnly, o)
	if err != nil {
		t.Fatal(err)
	}
	return bs
}

var twoKeys = []KMSKey{{ID: "b", Key: arnB, Current: true}, {ID: "a", Key: "alias/qs-basis"}}

// Tokens minted by one replica verify on another that shares the KMS key
// (and not its caches); any change to a token is refused, never read as
// another basis.
func TestKMSCrossReplicaAndTamper(t *testing.T) {
	f := newFakeKMS()
	c := &clock{t: t0}
	a, b := replica(t, f, c, twoKeys), replica(t, f, c, twoKeys)
	rapid.Check(t, func(rt *rapid.T) {
		bb := genBasis(rt)
		tok, err := a.Mint(context.Background(), &bb)
		if err != nil {
			rt.Fatal(err)
		}
		if !strings.HasPrefix(tok, "b1.") || bb.Kid != "k:b" {
			rt.Fatalf("%q %q", tok, bb.Kid)
		}
		got, err := b.Open(context.Background(), tok)
		if err != nil {
			rt.Fatal(err)
		}
		again, _ := b.Mint(context.Background(), &Basis{IssuedNs: got.IssuedNs, Clusters: got.Clusters, Signals: got.Signals, MaxLatenessNs: got.MaxLatenessNs})
		if again != tok {
			rt.Fatal("a token must decode to the basis it was minted for")
		}
		i := rapid.IntRange(0, len(tok)-1).Draw(rt, "i")
		ch := rapid.Byte().Draw(rt, "c")
		if tok[i] == ch {
			return
		}
		mut := tok[:i] + string([]byte{ch}) + tok[i+1:]
		got, err = b.Open(context.Background(), mut)
		if err == nil {
			again, _ := b.Mint(context.Background(), &Basis{IssuedNs: got.IssuedNs, Clusters: got.Clusters, Signals: got.Signals, MaxLatenessNs: got.MaxLatenessNs})
			if again != tok {
				rt.Fatalf("a changed token decoded to another basis: %q", mut)
			}
		} else if !errors.Is(err, ErrInvalid) {
			rt.Fatalf("a changed token: %v, want basis_invalid", err)
		}
	})
}

// forge builds a token claiming kid, with a random MAC.
func forge(kid string) string {
	body := fmt.Sprintf(`{"v":1,"k":%q,"t":0,"c":{"prod-a":1},"s":["*"],"l":0}`, kid)
	return "b1." + base64.RawURLEncoding.EncodeToString([]byte(body)) + "." + base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))
}

// A token never chooses the key it is checked with: an unconfigured kid,
// an ARN or alias in the kid, another static kid, are refused with no KMS
// call at all.
func TestKMSUnconfiguredKidNoCall(t *testing.T) {
	f := newFakeKMS()
	c := &clock{t: t0}
	bs := replica(t, f, c, twoKeys)
	before := f.n("VerifyMac") + f.n("GenerateMac") + f.n("DescribeKey")
	for _, kid := range []string{"k:e", "k:" + arnE, arnE, "k:alias/qs-basis", "alias/qs-basis", "k:", "k:a ", "k1", "eph-1234", "k:B"} {
		_, err := bs.Open(context.Background(), forge(kid))
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("kid %q: %v", kid, err)
		}
	}
	if after := f.n("VerifyMac") + f.n("GenerateMac") + f.n("DescribeKey"); after != before {
		t.Fatalf("%d KMS calls for unconfigured kids", after-before)
	}
	for _, u := range f.used {
		if u == arnE {
			t.Fatal("the service called KMS with a key nobody configured")
		}
	}
	// garbage never costs a call either
	for _, g := range []string{"", "b1.x", "b1.@@.@@", "b2." + forge("k:b")[3:], forge("k:b")[:20]} {
		if _, err := bs.Open(context.Background(), g); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: %v", g, err)
		}
	}
	if f.n("VerifyMac") != 0 {
		t.Fatal("malformed tokens reached KMS")
	}
	// a configured kid with a forged MAC: one call, refused, then briefly cached
	for range 3 {
		if _, err := bs.Open(context.Background(), forge("k:b")); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if f.n("VerifyMac") != 1 {
		t.Fatalf("%d VerifyMac calls for one forged token, want 1 (mismatch cached briefly)", f.n("VerifyMac"))
	}
	c.add(11 * time.Second)
	bs.Open(context.Background(), forge("k:b"))
	if f.n("VerifyMac") != 2 {
		t.Fatal("a mismatch must not be cached longer than MismatchTTL")
	}
}

// A KMS outage: minting fails visibly (ErrUnavailable); a token verified
// before keeps working (cached); an uncached one is ErrUnavailable, never
// valid and never invalid; nothing about the outage is cached.
func TestKMSOutage(t *testing.T) {
	f := newFakeKMS()
	c := &clock{t: t0}
	bs := replica(t, f, c, twoKeys)
	b1 := sample()
	seen, err := bs.Mint(context.Background(), &b1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bs.Open(context.Background(), seen); err != nil {
		t.Fatal(err)
	}
	other := replica(t, f, c, twoKeys) // mints a token this replica never saw
	b2 := sample()
	b2.MaxLatenessNs++
	unseen, _ := other.Mint(context.Background(), &b2)

	f.setDown(true)
	b3 := sample()
	b3.MaxLatenessNs += 2
	if _, err := bs.Mint(context.Background(), &b3); !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrInvalid) {
		t.Fatalf("mint in an outage: %v", err)
	}
	if _, err := bs.Open(context.Background(), seen); err != nil {
		t.Fatalf("a cached token in an outage: %v", err)
	}
	for range 2 {
		got, err := bs.Open(context.Background(), unseen)
		if got != nil || !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrInvalid) {
			t.Fatalf("an uncached token in an outage: %v %v", got, err)
		}
	}
	n := f.n("VerifyMac")
	f.setDown(false)
	if _, err := bs.Open(context.Background(), unseen); err != nil {
		t.Fatalf("after the outage: %v", err)
	}
	if f.n("VerifyMac") != n+1 {
		t.Fatal("the outage's failure was cached")
	}
	// the verified cache expires: a key taken out of the configuration
	// stops working within VerifyTTL
	c.add(11 * time.Minute)
	f.setDown(true)
	if _, err := bs.Open(context.Background(), seen); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("after VerifyTTL the token is checked again: %v", err)
	}
}

// Rotation and migration: tokens under a verify-only KMS key and under the
// static keys verify; a replica without them refuses them; the current
// key mints.
func TestKMSRotationAndMigration(t *testing.T) {
	f := newFakeKMS()
	c := &clock{t: t0}
	static := ring(t)
	oldTok, _ := static.Encode(sample())
	// before: minting under key a
	before := replica(t, f, c, []KMSKey{{ID: "a", Key: arnA, Current: true}})
	s := sample()
	tokA, _ := before.Mint(context.Background(), &s)
	// after: b is current, a verifies, the static keys verify
	after := replica(t, f, c, twoKeys, static)
	if after.Current() != "k:b" || after.MintSigner() != "kms" {
		t.Fatal(after.Current())
	}
	for name, tok := range map[string]string{"old kms key": tokA, "static": oldTok} {
		if _, err := after.Open(context.Background(), tok); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	s2 := sample()
	tokB, _ := after.Mint(context.Background(), &s2)
	if s2.Kid != "k:b" {
		t.Fatal(s2.Kid)
	}
	// a static-only deployment refuses KMS tokens as unknown keys, without a call
	if _, err := static.Decode(tokB); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// key a retired: its tokens are refused
	retired := replica(t, f, c, []KMSKey{{ID: "b", Key: arnB, Current: true}})
	if _, err := retired.Open(context.Background(), tokA); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// the same id pointing at another key: refused (a mismatch, not an outage)
	swapped := replica(t, f, c, []KMSKey{{ID: "b", Key: arnA, Current: true}})
	if _, err := swapped.Open(context.Background(), tokB); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

// Mint reuse: an equal basis within MintReuse is the same token (one
// GenerateMac); a different one, or after the window, is minted.
func TestKMSMintReuse(t *testing.T) {
	f := newFakeKMS()
	c := &clock{t: t0}
	ks, _ := NewKMSSigner(f, twoKeys, 0)
	bs, _ := New(ks, nil, Options{Now: c.now, MintReuse: 5 * time.Second})
	b1 := sample()
	t1, _ := bs.Mint(context.Background(), &b1)
	c.add(2 * time.Second)
	b2 := sample()
	b2.IssuedNs += int64(2 * time.Second)
	t2, _ := bs.Mint(context.Background(), &b2)
	if t1 != t2 || b2.IssuedNs != b1.IssuedNs || f.n("GenerateMac") != 1 {
		t.Fatalf("reuse: %v %d %d", t1 == t2, b2.IssuedNs-b1.IssuedNs, f.n("GenerateMac"))
	}
	if got, err := bs.Open(context.Background(), t2); err != nil || got.View().IssuedNs != b2.View().IssuedNs {
		t.Fatal("the answer's basis_info must describe the token it names", err)
	}
	b3 := sample()
	b3.Clusters = map[string]uint64{"prod-a": 7}
	t3, _ := bs.Mint(context.Background(), &b3)
	if t3 == t1 || f.n("GenerateMac") != 2 {
		t.Fatal("another basis reused a token")
	}
	c.add(4 * time.Second)
	b4 := sample()
	b4.IssuedNs += int64(6 * time.Second)
	if t4, _ := bs.Mint(context.Background(), &b4); t4 == t1 || f.n("GenerateMac") != 3 {
		t.Fatal("reused past the window")
	}
}

// A token bigger than KMS's 4,096-byte message limit still mints and
// verifies: KMS MACs a digest.
func TestKMSLargeToken(t *testing.T) {
	f := newFakeKMS()
	c := &clock{t: t0}
	bs := replica(t, f, c, twoKeys)
	b := Basis{IssuedNs: 1, Clusters: map[string]uint64{}}
	for i := range 300 {
		b.Clusters[fmt.Sprintf("cluster-%03d", i)] = uint64(i + 1)
	}
	tok, err := bs.Mint(context.Background(), &b)
	if err != nil || len(tok) < 5000 {
		t.Fatal(err, len(tok))
	}
	if _, err := replica(t, f, c, twoKeys).Open(context.Background(), tok); err != nil {
		t.Fatal(err)
	}
}

func TestKMSConfigChecks(t *testing.T) {
	f := newFakeKMS()
	bad := [][]KMSKey{
		nil,
		{{ID: "a", Key: arnA, Current: true}, {ID: "a", Key: arnB}},
		{{ID: "a", Key: arnA, Current: true}, {ID: "b", Key: arnB, Current: true}},
		{{ID: "a:b", Key: arnA}},
		{{ID: "a", Key: "aaaaaaaa-1111-2222-3333-444444444444"}},
		{{ID: "a", Key: "arn:aws:s3:::bucket"}},
		{{ID: strings.Repeat("x", 31), Key: arnA}},
	}
	for _, k := range bad {
		if _, err := NewKMSSigner(f, k, 0); err == nil {
			t.Errorf("accepted %+v", k)
		}
	}
	// DescribeKey: not an HMAC key, an unknown key, KMS down
	f.spec[arnB] = types.KeySpecSymmetricDefault
	ks, _ := NewKMSSigner(f, twoKeys, 0)
	if err := ks.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "HMAC_256") {
		t.Fatal(err)
	}
	f.spec[arnB] = types.KeySpecHmac256
	ks, _ = NewKMSSigner(f, []KMSKey{{ID: "z", Key: "alias/nope", Current: true}}, 0)
	if err := ks.Check(context.Background()); err == nil {
		t.Fatal("an unknown key passed the check")
	}
	f.setDown(true)
	ks, _ = NewKMSSigner(f, twoKeys, 0)
	if err := ks.Check(context.Background()); err == nil {
		t.Fatal("an unreachable KMS passed the check")
	}
	f.setDown(false)
	// an alias is resolved to its key's ARN at startup and used from then on
	ks, _ = NewKMSSigner(f, twoKeys, 0)
	if err := ks.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.aliases["alias/qs-basis"] = arnE
	f.used = nil
	ks.MAC(context.Background(), "k:a", []byte("m"))
	if f.used[0] != arnA {
		t.Fatalf("used %v after the alias moved", f.used)
	}
	// no minting signer, a signer twice
	if _, err := New(nil, nil, Options{}); err == nil {
		t.Fatal("no signer")
	}
	vo, _ := NewKMSSigner(f, []KMSKey{{ID: "a", Key: arnA}}, 0)
	if _, err := New(vo, nil, Options{}); err == nil {
		t.Fatal("a signer with no current key cannot mint")
	}
	if _, err := New(ks, []Signer{ks}, Options{}); err == nil {
		t.Fatal("a signer twice")
	}
}

// The caches are bounded.
func TestCacheBound(t *testing.T) {
	c := newTTLLRU[struct{}](3)
	for i := range 10 {
		c.put(fmt.Sprint(i), struct{}{}, t0.Add(time.Hour))
	}
	if c.len() != 3 {
		t.Fatal(c.len())
	}
	if _, ok := c.get("9", t0); !ok {
		t.Fatal("the newest entry was evicted")
	}
	if _, ok := c.get("0", t0); ok {
		t.Fatal("the oldest entry survived")
	}
	if _, ok := c.get("9", t0.Add(time.Hour)); ok {
		t.Fatal("an expired entry was returned")
	}
}

// BenchmarkOpenCached: the cost of a verified token on its next use.
func BenchmarkOpenCached(b *testing.B) {
	f := newFakeKMS()
	c := &clock{t: t0}
	bs := replica(b, f, c, twoKeys)
	s := sample()
	tok, _ := bs.Mint(context.Background(), &s)
	bs.Open(context.Background(), tok)
	b.ResetTimer()
	for range b.N {
		if _, err := bs.Open(context.Background(), tok); err != nil {
			b.Fatal(err)
		}
	}
}
